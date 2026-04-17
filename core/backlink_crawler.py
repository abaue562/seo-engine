"""
Self-hosted backlink crawler.
Crawls competitor sites via Firecrawl to extract outbound links,
computes a link graph, discovers who links to competitors (gap analysis),
and estimates domain authority from crawl data — no Ahrefs needed.
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

_CACHE_TTL = 86400 * 3     # 3 days
_MAX_PAGES_PER_DOMAIN = 15
_UA = "SEOEngine-Crawler/1.0 (+https://gethubed.com/bot)"


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS crawled_pages (
            id          TEXT PRIMARY KEY,
            domain      TEXT NOT NULL,
            url         TEXT NOT NULL UNIQUE,
            outbound_links TEXT DEFAULT '[]',
            inbound_count INTEGER DEFAULT 0,
            word_count  INTEGER DEFAULT 0,
            crawled_at  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_cp_domain ON crawled_pages(domain);

        CREATE TABLE IF NOT EXISTS domain_authority_cache (
            domain      TEXT PRIMARY KEY,
            da_score    INTEGER DEFAULT 0,
            page_count  INTEGER DEFAULT 0,
            outbound_count INTEGER DEFAULT 0,
            inbound_proxy INTEGER DEFAULT 0,
            computed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS link_graph (
            id          TEXT PRIMARY KEY,
            source_domain TEXT NOT NULL,
            target_domain TEXT NOT NULL,
            source_url  TEXT,
            anchor_text TEXT,
            discovered_at TEXT,
            UNIQUE(source_domain, target_domain, source_url)
        );
        CREATE INDEX IF NOT EXISTS idx_lg_target ON link_graph(target_domain);
    """)
    c.commit()
    return c


def _extract_domain(url: str) -> str:
    m = re.search(r'https?://(?:www\.)?([^/]+)', url)
    return m.group(1) if m else url


def _extract_links(md: str, base_url: str) -> list[dict]:
    """Extract all hyperlinks from markdown content."""
    base_domain = _extract_domain(base_url)
    links = []
    # Markdown links [text](url)
    for m in re.finditer(r'\[([^\]]{1,200})\]\((https?://[^\)]+)\)', md):
        anchor = m.group(1).strip()
        url = m.group(2).strip().rstrip(".,);")
        domain = _extract_domain(url)
        links.append({"url": url, "anchor": anchor, "domain": domain, "is_external": domain != base_domain})
    # Bare URLs
    for url in re.findall(r'https?://[^\s\)\"\'\<\>]{10,300}', md):
        url = url.rstrip(".,);")
        domain = _extract_domain(url)
        if not any(l["url"] == url for l in links):
            links.append({"url": url, "anchor": "", "domain": domain, "is_external": domain != base_domain})
    return links[:200]


def crawl_domain(domain: str, max_pages: int = _MAX_PAGES_PER_DOMAIN) -> dict:
    """
    Crawl a domain via Firecrawl to build its link graph.
    Returns {domain, pages_crawled, outbound_domains, internal_pages}.
    """
    cache_key = f"crawl:{hashlib.md5(domain.encode()).hexdigest()[:12]}"
    cached = _redis.get(cache_key)
    if cached:
        log.debug("backlink_crawler.cache_hit  domain=%s", domain)
        return json.loads(cached)

    try:
        from core.aion_bridge import aion
    except Exception:
        log.warning("backlink_crawler: aion_bridge unavailable")
        return {"domain": domain, "pages_crawled": 0, "outbound_domains": [], "error": "aion_unavailable"}

    pages_crawled = 0
    outbound_domains: set[str] = set()
    internal_urls: list[str] = []
    all_links: list[dict] = []

    # Start from homepage
    start_url = f"https://{domain}"
    try:
        md = aion.firecrawl_scrape(start_url)
        if not md:
            return {"domain": domain, "pages_crawled": 0, "outbound_domains": [], "error": "scrape_failed"}

        links = _extract_links(md, start_url)
        all_links.extend(links)
        word_count = len(md.split())

        _store_page(domain, start_url, links, word_count)
        pages_crawled += 1

        # Queue internal pages
        for lnk in links:
            if not lnk["is_external"] and lnk["url"] not in internal_urls:
                internal_urls.append(lnk["url"])
            elif lnk["is_external"]:
                outbound_domains.add(lnk["domain"])
    except Exception:
        log.exception("backlink_crawler.start_page  domain=%s", domain)

    # Crawl up to max_pages internal pages
    for url in internal_urls[:max_pages - 1]:
        try:
            md = aion.firecrawl_scrape(url)
            if not md:
                continue
            links = _extract_links(md, url)
            all_links.extend(links)
            word_count = len(md.split())
            _store_page(domain, url, links, word_count)
            pages_crawled += 1
            for lnk in links:
                if lnk["is_external"]:
                    outbound_domains.add(lnk["domain"])
        except Exception:
            log.exception("backlink_crawler.page  url=%s", url)

    # Build link graph edges
    _update_link_graph(domain, all_links)

    result = {
        "domain": domain,
        "pages_crawled": pages_crawled,
        "outbound_domains": list(outbound_domains)[:50],
        "unique_outbound": len(outbound_domains),
        "total_links_found": len(all_links),
    }
    _redis.setex(cache_key, _CACHE_TTL, json.dumps(result))
    log.info("crawl_domain.done  domain=%s  pages=%d  outbound=%d", domain, pages_crawled, len(outbound_domains))
    return result


def _store_page(domain: str, url: str, links: list[dict], word_count: int) -> None:
    pid = hashlib.md5(url.encode()).hexdigest()[:12]
    now = datetime.now(timezone.utc).isoformat()
    ext_links = [l["url"] for l in links if l["is_external"]]
    with _conn() as c:
        c.execute("""
            INSERT INTO crawled_pages (id,domain,url,outbound_links,word_count,crawled_at)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(url) DO UPDATE SET
                outbound_links=excluded.outbound_links,
                word_count=excluded.word_count, crawled_at=excluded.crawled_at
        """, [pid, domain, url, json.dumps(ext_links[:100]), word_count, now])


def _update_link_graph(source_domain: str, links: list[dict]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        for lnk in links:
            if not lnk["is_external"] or not lnk["domain"]:
                continue
            lid = hashlib.md5(f"{source_domain}:{lnk['domain']}:{lnk['url']}".encode()).hexdigest()[:16]
            c.execute("""
                INSERT OR IGNORE INTO link_graph
                    (id,source_domain,target_domain,source_url,anchor_text,discovered_at)
                VALUES (?,?,?,?,?,?)
            """, [lid, source_domain, lnk["domain"], lnk["url"], lnk["anchor"][:100], now])


def compute_domain_authority(domain: str) -> dict:
    """
    Compute a domain authority proxy score (0-100) from crawl data.
    Formula: page_count(30) + outbound_diversity(20) + inbound_links_proxy(30) + content_depth(20)
    """
    with _conn() as c:
        page_count = c.execute("SELECT COUNT(*) FROM crawled_pages WHERE domain=?", [domain]).fetchone()[0]
        avg_words = c.execute("SELECT AVG(word_count) FROM crawled_pages WHERE domain=?", [domain]).fetchone()[0] or 0
        outbound_domains = c.execute(
            "SELECT COUNT(DISTINCT target_domain) FROM link_graph WHERE source_domain=?", [domain]
        ).fetchone()[0]
        inbound_count = c.execute(
            "SELECT COUNT(DISTINCT source_domain) FROM link_graph WHERE target_domain=?", [domain]
        ).fetchone()[0]

    # Score components
    page_score = min(page_count / 5 * 30, 30)                       # 0-30 pts (5 pages = full)
    content_score = min(avg_words / 800 * 20, 20)                   # 0-20 pts (800 avg words = full)
    outbound_score = min(outbound_domains / 10 * 20, 20)            # 0-20 pts (10 unique domains = full)
    inbound_score = min(inbound_count / 3 * 30, 30)                 # 0-30 pts (3 linking domains = full)

    da_score = int(page_score + content_score + outbound_score + inbound_score)

    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("""
            INSERT INTO domain_authority_cache
                (domain,da_score,page_count,outbound_count,inbound_proxy,computed_at)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(domain) DO UPDATE SET
                da_score=excluded.da_score, page_count=excluded.page_count,
                outbound_count=excluded.outbound_count, inbound_proxy=excluded.inbound_proxy,
                computed_at=excluded.computed_at
        """, [domain, da_score, page_count, outbound_domains, inbound_count, now])

    log.info("compute_da  domain=%s  score=%d  pages=%d  inbound=%d", domain, da_score, page_count, inbound_count)
    return {
        "domain": domain,
        "da_score": da_score,
        "page_count": page_count,
        "avg_word_count": int(avg_words),
        "outbound_domains": outbound_domains,
        "inbound_linking_domains": inbound_count,
    }


def get_domain_authority(domain: str, refresh: bool = False) -> int:
    """Get cached DA score or compute it."""
    if not refresh:
        with _conn() as c:
            row = c.execute("SELECT da_score FROM domain_authority_cache WHERE domain=?", [domain]).fetchone()
        if row:
            return row[0]
    result = compute_domain_authority(domain)
    return result["da_score"]


def find_competitor_backlink_gaps(your_domain: str, competitor_domains: list[str], business_id: str = "") -> list[dict]:
    """
    Find domains that link to competitors but not to you.
    Uses self-crawled link graph — no Ahrefs/DataForSEO needed.
    """
    from core.backlink_prospector import add_prospect

    # Get domains linking to your site
    with _conn() as c:
        your_clean = your_domain.replace("https://", "").replace("http://", "").rstrip("/")
        your_linkers = set(
            r[0] for r in c.execute(
                "SELECT DISTINCT source_domain FROM link_graph WHERE target_domain=?", [your_clean]
            ).fetchall()
        )

    gaps = []
    for comp in competitor_domains[:5]:
        comp_clean = comp.replace("https://", "").replace("http://", "").rstrip("/")
        with _conn() as c:
            comp_linkers = c.execute(
                "SELECT DISTINCT source_domain, source_url FROM link_graph WHERE target_domain=?",
                [comp_clean]
            ).fetchall()

        for row in comp_linkers:
            source_domain = row[0]
            source_url = row[1]
            if source_domain in your_linkers or source_domain == your_clean:
                continue
            da = get_domain_authority(source_domain)
            gap = {"source_domain": source_domain, "source_url": source_url,
                   "links_to_competitor": comp_clean, "da_score": da}
            gaps.append(gap)

            if business_id:
                add_prospect(
                    business_id=business_id,
                    opportunity_type="competitor_gap",
                    target_url=source_url or f"https://{source_domain}",
                    domain_rating=da,
                    anchor_context=f"Links to {comp_clean} — gap opportunity",
                    pitch_angle=f"They link to {comp_clean}, offer your content as better/complementary resource",
                )

    log.info("find_competitor_backlink_gaps  your=%s  gaps=%d", your_domain, len(gaps))
    return gaps[:30]


def crawl_competitor_suite(business_id: str) -> dict:
    """Crawl all competitor domains for a business and build link graph."""
    try:
        all_biz = json.loads(open("data/storage/businesses.json").read())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
        biz = next((b for b in biz_list
                    if b.get("id") == business_id or b.get("business_id") == business_id), {})
    except Exception:
        biz = {}

    competitors = biz.get("competitors", [])
    your_domain = biz.get("domain", "").replace("https://", "").replace("http://", "").rstrip("/")
    results = {}

    for comp in competitors[:5]:
        domain = comp.replace("https://", "").replace("http://", "").rstrip("/")
        result = crawl_domain(domain, max_pages=5)
        results[domain] = result

    gaps = []
    if your_domain and competitors:
        gaps = find_competitor_backlink_gaps(your_domain, competitors, business_id)

    log.info("crawl_competitor_suite  biz=%s  comps=%d  gaps=%d", business_id, len(results), len(gaps))
    return {"competitors_crawled": len(results), "backlink_gaps_found": len(gaps), "results": results}
