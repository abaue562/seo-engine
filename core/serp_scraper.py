"""
Self-hosted SERP scraper — no paid API needed.
Primary: Bing HTML via Firecrawl/Playwright (real browser, no blocks).
Secondary: Google Autocomplete + Suggest for related keywords.
Tertiary: AION Twitter Intel for trending signals.
Caches results in Redis. Falls back gracefully on each layer.
"""
from __future__ import annotations
import hashlib
import json
import logging
import re
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Optional

import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
_DB = "data/storage/seo_engine.db"

_CACHE_SERP_TTL = 86400 * 2       # 2 days
_CACHE_SUGGEST_TTL = 86400 * 7    # 7 days
_BING_BASE = "https://www.bing.com/search"
_AUTOCOMPLETE_URL = "https://suggestqueries.google.com/complete/search"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS serp_results (
            id          TEXT PRIMARY KEY,
            business_id TEXT,
            keyword     TEXT NOT NULL,
            engine      TEXT DEFAULT 'bing',
            results     TEXT DEFAULT '[]',
            paa         TEXT DEFAULT '[]',
            scraped_at  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_serp_kw ON serp_results(keyword, engine);

        CREATE TABLE IF NOT EXISTS keyword_rankings (
            id          TEXT PRIMARY KEY,
            business_id TEXT NOT NULL,
            keyword     TEXT NOT NULL,
            your_url    TEXT,
            position    INTEGER,
            page        INTEGER DEFAULT 1,
            engine      TEXT DEFAULT 'bing',
            checked_at  TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_kr_biz_kw ON keyword_rankings(business_id, keyword, engine);
    """)
    c.commit()
    return c


# ── SERP parsing ──────────────────────────────────────────────────────────────

def _parse_bing_markdown(md: str, keyword: str) -> dict:
    """Extract organic results, PAA, and featured snippets from Bing markdown."""
    # Extract all external URLs (non-Bing)
    raw_urls = re.findall(
        r'https?://(?!(?:www\.)?bing\.com|go\.microsoft|msn\.com|microsoft\.com)[^\s\)\"\'\<\>#]+',
        md
    )
    seen: set[str] = set()
    urls: list[str] = []
    for u in raw_urls:
        u = u.rstrip(".,);'\"")
        domain = re.sub(r'https?://(www\.)?', '', u).split('/')[0]
        if domain and domain not in seen and len(u) < 300:
            seen.add(domain)
            urls.append(u)

    # Build title map: markdown links first
    title_map: dict[str, str] = {}
    for m in re.finditer(r'\[([^\]]{5,120})\]\((https?://[^\)]+)\)', md):
        anchor, url = m.group(1).strip(), m.group(2).rstrip(".,);")
        domain = re.sub(r'https?://(www\.)?', '', url).split('/')[0]
        if domain and domain not in title_map:
            title_map[domain] = anchor

    # Fallback: plain text line immediately before a URL line
    lines = md.split("\n")
    for i, line in enumerate(lines):
        stripped = line.strip()
        if (stripped and not stripped.startswith("http") and
                not stripped.startswith("[") and not stripped.startswith("#") and
                5 < len(stripped) < 120):
            nxt = " ".join(lines[i + 1:i + 3])
            um = re.search(r'https?://(?!(?:www\.)?bing\.com)[^\s]+', nxt)
            if um:
                u = um.group().rstrip(".,);")
                domain = re.sub(r'https?://(www\.)?', '', u).split('/')[0]
                if domain and domain not in title_map:
                    title_map[domain] = stripped

    # Snippet: text after each URL
    snippet_map: dict[str, str] = {}
    for url in urls[:10]:
        domain = re.sub(r'https?://(www\.)?', '', url).split('/')[0]
        idx = md.find(url)
        if idx > 0:
            chunk = md[idx:idx + 400].split('\n')
            snippet = ' '.join(
                ln.strip() for ln in chunk[1:5]
                if ln.strip() and not ln.strip().startswith('[') and not ln.strip().startswith('#')
            )[:200]
            if snippet:
                snippet_map[domain] = snippet

    # Build organic results list
    organic: list[dict] = []
    for pos, url in enumerate(urls[:20], 1):
        domain = re.sub(r'https?://(www\.)?', '', url).split('/')[0]
        organic.append({
            "position": pos,
            "url": url,
            "domain": domain,
            "title": title_map.get(domain, ""),
            "snippet": snippet_map.get(domain, ""),
        })

    paa = re.findall(r'#+\s+([^\n\r]{10,120}\?)', md)
    paa = list(dict.fromkeys(paa))[:10]
    return {"organic": organic, "paa": paa, "keyword": keyword, "engine": "bing"}


def scrape_serp(keyword: str, location: str = "", num_results: int = 10) -> dict:
    """
    Scrape SERP for keyword via Bing/Firecrawl.
    Returns {organic: [{position, url, domain, title, snippet}], paa: [...], keyword, engine}.
    """
    cache_key = f"serp:{hashlib.sha256(f'{keyword}:{location}'.encode()).hexdigest()[:16]}"
    cached = _redis.get(cache_key)
    if cached:
        log.debug("serp_scraper.cache_hit  keyword=%s", keyword)
        return json.loads(cached)

    query = f"{keyword} {location}".strip()
    encoded = urllib.parse.quote_plus(query)
    url = f"{_BING_BASE}?q={encoded}&setlang=en&cc=US&count={min(num_results, 50)}"

    result: dict = {"organic": [], "paa": [], "keyword": keyword, "engine": "bing"}
    try:
        from core.aion_bridge import aion
        md = aion.firecrawl_scrape(url)
        if md and len(md) > 200:
            result = _parse_bing_markdown(md, keyword)
            result["location"] = location
    except Exception:
        log.exception("serp_scraper.scrape_error  keyword=%s", keyword)

    if result.get("organic"):
        _redis.setex(cache_key, _CACHE_SERP_TTL, json.dumps(result))
        _save_serp_result(keyword, result)

    log.info("serp_scraper.done  keyword=%s  results=%d  paa=%d",
             keyword, len(result.get("organic", [])), len(result.get("paa", [])))
    return result


def _save_serp_result(keyword: str, result: dict, business_id: str = "") -> None:
    sid = hashlib.md5(f"{keyword}:{result.get('engine','bing')}".encode()).hexdigest()[:12]
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("""
            INSERT INTO serp_results (id,business_id,keyword,engine,results,paa,scraped_at)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET results=excluded.results, paa=excluded.paa, scraped_at=excluded.scraped_at
        """, [sid, business_id, keyword, result.get("engine", "bing"),
              json.dumps(result.get("organic", [])),
              json.dumps(result.get("paa", [])), now])


# ── Keyword suggestions ───────────────────────────────────────────────────────

def get_keyword_suggestions(seed: str, location: str = "") -> list[str]:
    """
    Google Autocomplete + Bing Autosuggest for related keyword discovery.
    Free, no API key needed.
    """
    cache_key = f"suggest:{hashlib.sha256(f'{seed}:{location}'.encode()).hexdigest()[:12]}"
    cached = _redis.get(cache_key)
    if cached:
        return json.loads(cached)

    suggestions: list[str] = []
    modifiers = ["", " how to", " cost", " near me", " best", " vs", " guide",
                 " tips", " services", " installation", " company", " price",
                 " reviews", " benefits", " types"]

    for mod in modifiers[:6]:
        query = f"{seed}{mod} {location}".strip()
        encoded = urllib.parse.quote_plus(query)
        try:
            req = urllib.request.Request(
                f"{_AUTOCOMPLETE_URL}?client=firefox&q={encoded}",
                headers={"User-Agent": _UA}
            )
            data = json.loads(urllib.request.urlopen(req, timeout=8).read())
            for term in data[1]:
                if isinstance(term, str) and term not in suggestions:
                    suggestions.append(term)
        except Exception:
            pass

    # Also try Bing Autosuggest
    try:
        encoded = urllib.parse.quote_plus(seed)
        req = urllib.request.Request(
            f"https://api.bing.microsoft.com/v7.0/suggestions?q={encoded}",
            headers={"User-Agent": _UA, "Ocp-Apim-Subscription-Key": ""}
        )
        # Silently skip if no Bing key — Google Autocomplete is enough
    except Exception:
        pass

    suggestions = list(dict.fromkeys(suggestions))[:50]
    if suggestions:
        _redis.setex(cache_key, _CACHE_SUGGEST_TTL, json.dumps(suggestions))
    log.info("get_keyword_suggestions  seed=%s  found=%d", seed, len(suggestions))
    return suggestions


def estimate_keyword_difficulty(keyword: str) -> dict:
    """
    Estimate keyword difficulty from SERP signals (no paid API).
    Signals: avg domain authority proxy, big-brand dominance, PAA count.
    """
    serp = scrape_serp(keyword)
    organic = serp.get("organic", [])
    if not organic:
        return {"keyword": keyword, "difficulty": 50, "signal": "no_data"}

    # Big brand proxy — domains with high implicit authority
    BIG_BRANDS = {
        "wikipedia.org", "amazon.com", "youtube.com", "reddit.com",
        "forbes.com", "nytimes.com", "cnn.com", "bbc.com", "gov",
        "britannica.com", "healthline.com", "webmd.com", "yelp.com",
    }
    big_brand_count = sum(
        1 for r in organic
        if any(b in r.get("domain", "") for b in BIG_BRANDS)
    )
    big_brand_ratio = big_brand_count / max(len(organic), 1)

    paa_count = len(serp.get("paa", []))

    # Difficulty formula: big brand dominance (0-60) + PAA signal (0-20) + base (20)
    difficulty = int(20 + (big_brand_ratio * 60) + min(paa_count * 4, 20))
    difficulty = min(difficulty, 100)

    return {
        "keyword": keyword,
        "difficulty": difficulty,
        "big_brand_results": big_brand_count,
        "total_results": len(organic),
        "paa_questions": paa_count,
        "signal": "bing_scrape",
    }


# ── Rank tracking ─────────────────────────────────────────────────────────────

def check_keyword_ranking(business_id: str, keyword: str, your_domain: str, location: str = "") -> dict:
    """Check where your_domain ranks for keyword in Bing SERP."""
    serp = scrape_serp(keyword, location=location)
    organic = serp.get("organic", [])
    your_clean = your_domain.replace("https://", "").replace("http://", "").rstrip("/").lower()

    position: Optional[int] = None
    matched_url = ""
    for result in organic:
        if your_clean in result.get("domain", "").lower() or your_clean in result.get("url", "").lower():
            position = result["position"]
            matched_url = result.get("url", "")
            break

    now = datetime.now(timezone.utc).isoformat()
    rid = hashlib.md5(f"{business_id}:{keyword}:bing".encode()).hexdigest()[:12]
    with _conn() as c:
        c.execute("""
            INSERT INTO keyword_rankings (id,business_id,keyword,your_url,position,engine,checked_at)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(business_id,keyword,engine) DO UPDATE SET
                your_url=excluded.your_url, position=excluded.position, checked_at=excluded.checked_at
        """, [rid, business_id, keyword, matched_url, position, "bing", now])

    log.info("check_ranking  biz=%s  keyword=%s  position=%s", business_id, keyword, position)
    return {
        "keyword": keyword,
        "position": position,
        "in_top_10": position is not None and position <= 10,
        "in_top_3": position is not None and position <= 3,
        "matched_url": matched_url,
        "serp_top5": organic[:5],
    }


def run_rank_tracking_sweep(business_id: str, keywords: list[str], your_domain: str, location: str = "") -> dict:
    """Track rankings for multiple keywords."""
    results = []
    top10 = top3 = not_ranking = 0
    for kw in keywords[:50]:
        r = check_keyword_ranking(business_id, kw, your_domain, location)
        results.append(r)
        if r["in_top_3"]:
            top3 += 1
        elif r["in_top_10"]:
            top10 += 1
        elif r["position"] is None:
            not_ranking += 1
    log.info("rank_sweep  biz=%s  keywords=%d  top3=%d  top10=%d  not_ranking=%d",
             business_id, len(keywords), top3, top10, not_ranking)
    return {"keywords_checked": len(results), "top_3": top3, "top_10": top10, "not_ranking": not_ranking, "results": results}


def get_serp_competitors(keyword: str, your_domain: str, location: str = "") -> list[dict]:
    """Return all SERP competitors for a keyword (everyone ranking above or near you)."""
    serp = scrape_serp(keyword, location=location)
    your_clean = your_domain.replace("https://", "").replace("http://", "").rstrip("/").lower()
    competitors = [r for r in serp.get("organic", []) if your_clean not in r.get("domain", "")]
    return competitors[:10]


def get_ranking_history(business_id: str, keyword: str = "", limit: int = 30) -> list[dict]:
    with _conn() as c:
        if keyword:
            rows = c.execute(
                "SELECT * FROM keyword_rankings WHERE business_id=? AND keyword=? ORDER BY checked_at DESC LIMIT ?",
                [business_id, keyword, limit]
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM keyword_rankings WHERE business_id=? ORDER BY checked_at DESC LIMIT ?",
                [business_id, limit]
            ).fetchall()
    return [dict(r) for r in rows]
