import hashlib
import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional
import redis
import requests

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
DB_PATH = "data/storage/seo_engine.db"
PERPLEXITY_API = "https://api.perplexity.ai/chat/completions"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS ai_answer_log (
        id TEXT PRIMARY KEY, business_id TEXT NOT NULL,
        keyword TEXT NOT NULL, engine TEXT DEFAULT 'perplexity',
        answer_text TEXT, cited_urls TEXT DEFAULT '[]',
        your_urls_cited TEXT DEFAULT '[]',
        competitor_urls_cited TEXT DEFAULT '[]',
        queried_at TEXT DEFAULT (datetime('now'))
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_aal_biz ON ai_answer_log(business_id, keyword)")
    c.commit()
    return c

def query_perplexity(keyword: str) -> Dict:
    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if not api_key:
        log.warning("ai_answer_monitor: PERPLEXITY_API_KEY not set")
        return {"answer": "", "citations": []}

    cache_key = f"perplexity:{hashlib.sha256(keyword.encode()).hexdigest()[:16]}"
    cached = _redis.get(cache_key)
    if cached:
        return json.loads(cached)

    try:
        payload = {
            "model": "llama-3.1-sonar-small-128k-online",
            "messages": [
                {"role": "system", "content": "You are a helpful assistant. Answer the question concisely and accurately."},
                {"role": "user", "content": keyword}
            ],
            "max_tokens": 500,
            "return_citations": True,
            "return_related_questions": False,
        }
        resp = requests.post(PERPLEXITY_API,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        answer = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        citations = data.get("citations", [])
        result = {"answer": answer, "citations": citations}
        _redis.setex(cache_key, 86400 * 3, json.dumps(result))
        log.info("ai_answer_monitor.perplexity  keyword=%s  citations=%d", keyword, len(citations))
        return result
    except Exception as exc:
        log.exception("ai_answer_monitor.perplexity_error  keyword=%s", keyword)
        return {"answer": "", "citations": []}

def check_citation(cited_urls: List[str], your_domain: str, competitor_domains: List[str]) -> Dict:
    your_domain_clean = your_domain.replace("https://", "").replace("http://", "").rstrip("/").lower()
    your_urls = [u for u in cited_urls if your_domain_clean in u.lower()]
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

def run_keyword_monitor(business_id: str, max_keywords: int = 20) -> Dict:
    conn = sqlite3.connect(DB_PATH)
    keywords = [r[0] for r in conn.execute("""
        SELECT DISTINCT keyword FROM ranking_history
        WHERE business_id=? AND position <= 30
        ORDER BY position ASC LIMIT ?
    """, [business_id, max_keywords]).fetchall()]
    conn.close()

    biz = {}
    try:
        import json as j
        from pathlib import Path
        all_biz = j.loads(Path("data/storage/businesses.json").read_text())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
        biz = next((b for b in biz_list if b.get("id") == business_id or b.get("business_id") == business_id), {})
    except Exception:
        pass

    your_domain = biz.get("domain", biz.get("wp_site_url", ""))
    competitor_domains = biz.get("competitors", [])

    results = []
    cited_count = 0
    gap_count = 0

    for keyword in keywords:
        result = query_perplexity(keyword)
        if not result["answer"]:
            continue
        citation_check = check_citation(result["citations"], your_domain, competitor_domains)

        uid = hashlib.sha256(f"{business_id}:{keyword}:{datetime.utcnow().date().isoformat()}".encode()).hexdigest()[:16]
        db = _conn()
        db.execute("""INSERT OR REPLACE INTO ai_answer_log
            (id, business_id, keyword, engine, answer_text, cited_urls, your_urls_cited, competitor_urls_cited)
            VALUES (?,?,?,?,?,?,?,?)""",
            [uid, business_id, keyword, "perplexity",
             result["answer"][:1000],
             json.dumps(result["citations"]),
             json.dumps(citation_check["your_cited_urls"]),
             json.dumps(citation_check["competitor_cited_urls"])])
        db.commit()
        db.close()

        if citation_check["you_cited"]:
            cited_count += 1
        if citation_check["citation_gap"]:
            gap_count += 1
        results.append({"keyword": keyword, **citation_check})

    log.info("ai_answer_monitor.sweep  biz=%s  keywords=%d  cited=%d  gaps=%d", business_id, len(keywords), cited_count, gap_count)
    return {"business_id": business_id, "keywords_checked": len(keywords), "you_cited": cited_count, "citation_gaps": gap_count, "results": results}

def get_citation_gaps(business_id: str, limit: int = 20) -> List[Dict]:
    conn = _conn()
    rows = conn.execute("""
        SELECT keyword, cited_urls, competitor_urls_cited, queried_at
        FROM ai_answer_log
        WHERE business_id=? AND your_urls_cited='[]' AND competitor_urls_cited != '[]'
        ORDER BY queried_at DESC LIMIT ?
    """, [business_id, limit]).fetchall()
    conn.close()
    return [{"keyword": r[0], "all_citations": json.loads(r[1]), "competitor_citations": json.loads(r[2]), "checked_at": r[3]} for r in rows]

def get_citation_wins(business_id: str) -> List[Dict]:
    conn = _conn()
    rows = conn.execute("""
        SELECT keyword, your_urls_cited, queried_at
        FROM ai_answer_log
        WHERE business_id=? AND your_urls_cited != '[]'
        ORDER BY queried_at DESC LIMIT 50
    """, [business_id]).fetchall()
    conn.close()
    return [{"keyword": r[0], "your_cited_urls": json.loads(r[1]), "checked_at": r[2]} for r in rows]
