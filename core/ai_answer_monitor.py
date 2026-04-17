"""
AI Answer Monitor — multi-engine citation tracking.
Queries Perplexity API, Grok (via AION Brain), and Claude CLI
to track which AI engines cite your content vs competitors.
Uses Firecrawl/Playwright to scrape live AI search result pages.
"""
import hashlib
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional

import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
DB_PATH = "data/storage/seo_engine.db"
PERPLEXITY_API = "https://api.perplexity.ai/chat/completions"

ENGINES = ["perplexity", "grok", "claude"]


def _conn():
    c = sqlite3.connect(DB_PATH)
    c.executescript("""
        CREATE TABLE IF NOT EXISTS ai_answer_log (
            id TEXT PRIMARY KEY,
            business_id TEXT NOT NULL,
            keyword TEXT NOT NULL,
            engine TEXT DEFAULT 'perplexity',
            answer_text TEXT,
            cited_urls TEXT DEFAULT '[]',
            your_urls_cited TEXT DEFAULT '[]',
            competitor_urls_cited TEXT DEFAULT '[]',
            queried_at TEXT DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_aal_biz ON ai_answer_log(business_id, keyword);
    """)
    c.commit()
    return c


def _cache_key(engine: str, keyword: str) -> str:
    h = hashlib.sha256(keyword.encode()).hexdigest()[:16]
    return f"ai_monitor:{engine}:{h}"


def _extract_urls_from_text(text: str) -> List[str]:
    """Extract URLs from markdown/plain text responses."""
    return re.findall(r'https?://[^\s\)\]\'"<>]+', text)


# ---------------------------------------------------------------------------
# Engine query functions
# ---------------------------------------------------------------------------

def query_perplexity(keyword: str) -> Dict:
    """Query Perplexity API with citation return."""
    import requests
    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if not api_key:
        log.warning("ai_answer_monitor: PERPLEXITY_API_KEY not set")
        return {"answer": "", "citations": [], "engine": "perplexity"}

    cache_key = _cache_key("perplexity", keyword)
    cached = _redis.get(cache_key)
    if cached:
        return json.loads(cached)

    try:
        payload = {
            "model": "llama-3.1-sonar-large-128k-online",
            "messages": [
                {"role": "system", "content": "Answer concisely and accurately with sources."},
                {"role": "user", "content": keyword}
            ],
            "max_tokens": 600,
            "return_citations": True,
            "return_related_questions": False,
        }
        resp = requests.post(
            PERPLEXITY_API,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        answer = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        citations = data.get("citations", [])
        result = {"answer": answer, "citations": citations, "engine": "perplexity"}
        _redis.setex(cache_key, 86400 * 3, json.dumps(result))
        log.info("ai_monitor.perplexity  keyword=%s  citations=%d", keyword, len(citations))
        return result
    except Exception:
        log.exception("ai_monitor.perplexity_error  keyword=%s", keyword)
        return {"answer": "", "citations": [], "engine": "perplexity"}


def query_grok(keyword: str) -> Dict:
    """Query Grok via AION Brain OpenAI-compatible router."""
    cache_key = _cache_key("grok", keyword)
    cached = _redis.get(cache_key)
    if cached:
        return json.loads(cached)

    try:
        from core.aion_bridge import aion
        prompt = (
            f"Answer this question and list your top sources as URLs:\n\n{keyword}\n\n"
            f"Format: Answer first, then 'Sources:' followed by one URL per line."
        )
        answer = aion.brain_complete(prompt, model="groq", max_tokens=600)
        citations = _extract_urls_from_text(answer)
        result = {"answer": answer, "citations": citations, "engine": "grok"}
        _redis.setex(cache_key, 86400 * 3, json.dumps(result))
        log.info("ai_monitor.grok  keyword=%s  urls_found=%d", keyword, len(citations))
        return result
    except Exception:
        log.exception("ai_monitor.grok_error  keyword=%s", keyword)
        return {"answer": "", "citations": [], "engine": "grok"}


def query_claude(keyword: str) -> Dict:
    """Query Claude CLI and extract any cited sources."""
    cache_key = _cache_key("claude", keyword)
    cached = _redis.get(cache_key)
    if cached:
        return json.loads(cached)

    try:
        from core.claude import call_claude
        prompt = (
            f"Answer this question accurately. If you reference specific websites or sources, "
            f"include their URLs.\n\nQuestion: {keyword}\n\n"
            f"End your answer with 'Sources:' and list any URLs you referenced."
        )
        answer = call_claude(prompt, max_tokens=600)
        citations = _extract_urls_from_text(answer)
        result = {"answer": answer, "citations": citations, "engine": "claude"}
        _redis.setex(cache_key, 86400 * 3, json.dumps(result))
        log.info("ai_monitor.claude  keyword=%s  urls_found=%d", keyword, len(citations))
        return result
    except Exception:
        log.exception("ai_monitor.claude_error  keyword=%s", keyword)
        return {"answer": "", "citations": [], "engine": "claude"}


def scrape_perplexity_live(keyword: str) -> Dict:
    """
    Use Firecrawl/Playwright to scrape Perplexity.ai search results page
    and extract citations from the rendered DOM.
    More reliable than API for catching actual citation UI behavior.
    """
    cache_key = _cache_key("perplexity_live", keyword)
    cached = _redis.get(cache_key)
    if cached:
        return json.loads(cached)

    try:
        from core.aion_bridge import aion
        import urllib.parse
        query = urllib.parse.quote_plus(keyword)
        url = f"https://www.perplexity.ai/search?q={query}"
        md = aion.firecrawl_scrape(url)
        if not md:
            return {"answer": "", "citations": [], "engine": "perplexity_live"}
        citations = _extract_urls_from_text(md)
        # Filter out Perplexity's own URLs
        citations = [u for u in citations if "perplexity.ai" not in u]
        result = {"answer": md[:1000], "citations": citations[:20], "engine": "perplexity_live"}
        _redis.setex(cache_key, 86400, json.dumps(result))
        log.info("ai_monitor.perplexity_live  keyword=%s  citations=%d", keyword, len(citations))
        return result
    except Exception:
        log.exception("ai_monitor.perplexity_live_error  keyword=%s", keyword)
        return {"answer": "", "citations": [], "engine": "perplexity_live"}


# ---------------------------------------------------------------------------
# Citation analysis
# ---------------------------------------------------------------------------

def check_citation(cited_urls: List[str], your_domain: str, competitor_domains: List[str]) -> Dict:
    your_clean = your_domain.replace("https://", "").replace("http://", "").rstrip("/").lower()
    your_urls = [u for u in cited_urls if your_clean in u.lower()]
    comp_urls = []
    for comp in competitor_domains:
        comp_clean = comp.replace("https://", "").replace("http://", "").rstrip("/").lower()
        comp_urls.extend([u for u in cited_urls if comp_clean in u.lower()])
    return {
        "you_cited": len(your_urls) > 0,
        "your_cited_urls": your_urls,
        "competitor_cited": len(comp_urls) > 0,
        "competitor_cited_urls": comp_urls,
        "citation_gap": len(comp_urls) > 0 and len(your_urls) == 0,
        "total_citations": len(cited_urls),
    }


def _save_result(business_id: str, keyword: str, engine: str, result: Dict, citation_check: Dict) -> None:
    uid = hashlib.sha256(
        f"{business_id}:{keyword}:{engine}:{datetime.now(timezone.utc).date().isoformat()}".encode()
    ).hexdigest()[:16]
    db = _conn()
    db.execute("""
        INSERT OR REPLACE INTO ai_answer_log
            (id, business_id, keyword, engine, answer_text, cited_urls, your_urls_cited, competitor_urls_cited)
        VALUES (?,?,?,?,?,?,?,?)
    """, [uid, business_id, keyword, engine,
          result.get("answer", "")[:1000],
          json.dumps(result.get("citations", [])),
          json.dumps(citation_check["your_cited_urls"]),
          json.dumps(citation_check["competitor_cited_urls"])])
    db.commit()
    db.close()


# ---------------------------------------------------------------------------
# Main sweep
# ---------------------------------------------------------------------------

def run_keyword_monitor(
    business_id: str,
    max_keywords: int = 20,
    engines: List[str] | None = None,
    use_playwright: bool = True,
) -> Dict:
    """
    Query all configured engines for ranked keywords and track citations.
    engines: list from ["perplexity", "grok", "claude"] — defaults to all.
    """
    if engines is None:
        engines = ENGINES

    conn = sqlite3.connect(DB_PATH)
    keywords = [r[0] for r in conn.execute("""
        SELECT DISTINCT keyword FROM ranking_history
        WHERE business_id=? AND position <= 30
        ORDER BY position ASC LIMIT ?
    """, [business_id, max_keywords]).fetchall()]
    conn.close()

    biz = {}
    try:
        from pathlib import Path
        all_biz = json.loads(Path("data/storage/businesses.json").read_text())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
        biz = next((b for b in biz_list
                    if b.get("id") == business_id or b.get("business_id") == business_id), {})
    except Exception:
        pass

    your_domain = biz.get("domain", biz.get("wp_site_url", ""))
    competitor_domains = biz.get("competitors", [])

    engine_funcs = {
        "perplexity": query_perplexity,
        "grok": query_grok,
        "claude": query_claude,
    }

    summary: Dict[str, Dict] = {}

    for engine in engines:
        query_fn = engine_funcs.get(engine)
        if not query_fn:
            continue
        cited = gaps = checked = 0
        for keyword in keywords:
            result = query_fn(keyword)
            if not result.get("answer"):
                continue
            checked += 1
            citation_check = check_citation(result["citations"], your_domain, competitor_domains)
            _save_result(business_id, keyword, engine, result, citation_check)
            if citation_check["you_cited"]:
                cited += 1
            if citation_check["citation_gap"]:
                gaps += 1
        summary[engine] = {"checked": checked, "you_cited": cited, "citation_gaps": gaps}

    # Playwright live scrape for Perplexity DOM citations
    if use_playwright and "perplexity" in engines:
        cited = gaps = checked = 0
        for keyword in keywords[:10]:  # limit — scraping is slow
            result = scrape_perplexity_live(keyword)
            if not result.get("citations"):
                continue
            checked += 1
            citation_check = check_citation(result["citations"], your_domain, competitor_domains)
            _save_result(business_id, keyword, "perplexity_live", result, citation_check)
            if citation_check["you_cited"]:
                cited += 1
            if citation_check["citation_gap"]:
                gaps += 1
        summary["perplexity_live"] = {"checked": checked, "you_cited": cited, "citation_gaps": gaps}

    log.info("ai_monitor.sweep  biz=%s  keywords=%d  engines=%s", business_id, len(keywords), list(summary.keys()))
    return {
        "business_id": business_id,
        "keywords_checked": len(keywords),
        "engines": summary,
    }


def get_citation_gaps(business_id: str, limit: int = 20, engine: str = "") -> List[Dict]:
    conn = _conn()
    engine_filter = "AND engine=?" if engine else ""
    params = [business_id, limit] if not engine else [business_id, engine, limit]
    rows = conn.execute(f"""
        SELECT keyword, engine, cited_urls, competitor_urls_cited, queried_at
        FROM ai_answer_log
        WHERE business_id=? {engine_filter} AND your_urls_cited='[]' AND competitor_urls_cited != '[]'
        ORDER BY queried_at DESC LIMIT ?
    """, params).fetchall()
    conn.close()
    return [{"keyword": r[0], "engine": r[1], "all_citations": json.loads(r[2]),
             "competitor_citations": json.loads(r[3]), "checked_at": r[4]} for r in rows]


def get_citation_wins(business_id: str, engine: str = "") -> List[Dict]:
    conn = _conn()
    engine_filter = "AND engine=?" if engine else ""
    params = [business_id, engine, 50] if engine else [business_id, 50]
    rows = conn.execute(f"""
        SELECT keyword, engine, your_urls_cited, queried_at
        FROM ai_answer_log
        WHERE business_id=? {engine_filter} AND your_urls_cited != '[]'
        ORDER BY queried_at DESC LIMIT ?
    """, params).fetchall()
    conn.close()
    return [{"keyword": r[0], "engine": r[1], "your_cited_urls": json.loads(r[2]),
             "checked_at": r[3]} for r in rows]


def get_engine_comparison(business_id: str, keyword: str) -> List[Dict]:
    """Show how all engines respond to one keyword — useful for gap diagnosis."""
    conn = _conn()
    rows = conn.execute("""
        SELECT engine, answer_text, your_urls_cited, competitor_urls_cited, queried_at
        FROM ai_answer_log
        WHERE business_id=? AND keyword=?
        ORDER BY queried_at DESC LIMIT 10
    """, [business_id, keyword]).fetchall()
    conn.close()
    return [{"engine": r[0], "answer_preview": (r[1] or "")[:200],
             "you_cited": json.loads(r[2]), "competitors_cited": json.loads(r[3]),
             "checked_at": r[4]} for r in rows]
