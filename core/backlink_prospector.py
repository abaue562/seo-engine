"""
Backlink prospector: finds link-building opportunities using DataForSEO
backlink data, competitor gap analysis, and resource page discovery.
Stores prospects in SQLite for outreach queue.
"""
from __future__ import annotations
import json
import sqlite3
import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

_DB = "data/storage/seo_engine.db"

OPPORTUNITY_TYPES = [
    "competitor_gap",   # links competitors have that you don't
    "resource_page",    # "best of" / "resources" pages in niche
    "broken_link",      # broken links on authoritative pages pointing to dead content
    "unlinked_mention", # brand mentioned without a link
    "guest_post",       # sites that accept guest contributions
    "local_citation",   # local directories and citation sources
    "skyscraper",       # top-ranking content you can outdo
]


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS backlink_prospects (
            id              TEXT PRIMARY KEY,
            business_id     TEXT NOT NULL,
            opportunity_type TEXT NOT NULL,
            target_url      TEXT NOT NULL,
            target_domain   TEXT NOT NULL,
            domain_rating   INTEGER DEFAULT 0,
            page_title      TEXT,
            contact_email   TEXT,
            contact_name    TEXT,
            anchor_context  TEXT,
            your_page_to_link TEXT,
            pitch_angle     TEXT,
            status          TEXT DEFAULT 'new',
            priority_score  INTEGER DEFAULT 0,
            discovered_at   TEXT,
            updated_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_bp_biz ON backlink_prospects(business_id);
        CREATE INDEX IF NOT EXISTS idx_bp_status ON backlink_prospects(business_id, status);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bp_url ON backlink_prospects(business_id, target_url);

        CREATE TABLE IF NOT EXISTS backlink_acquired (
            id              TEXT PRIMARY KEY,
            business_id     TEXT NOT NULL,
            source_url      TEXT NOT NULL,
            source_domain   TEXT NOT NULL,
            target_url      TEXT NOT NULL,
            anchor_text     TEXT,
            domain_rating   INTEGER DEFAULT 0,
            is_dofollow     INTEGER DEFAULT 1,
            first_seen      TEXT,
            last_checked    TEXT,
            is_live         INTEGER DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_ba_biz ON backlink_acquired(business_id);
    """)
    c.commit()
    return c


def _prospect_id(business_id: str, target_url: str) -> str:
    return hashlib.md5(f"{business_id}:{target_url}".encode()).hexdigest()[:12]


def _extract_domain(url: str) -> str:
    m = re.search(r'https?://([^/]+)', url)
    return m.group(1) if m else url


def _priority_score(dr: int, opportunity_type: str) -> int:
    base = min(dr, 90)
    type_bonus = {
        "competitor_gap": 20,
        "unlinked_mention": 25,
        "resource_page": 15,
        "broken_link": 18,
        "guest_post": 10,
        "local_citation": 12,
        "skyscraper": 8,
    }.get(opportunity_type, 0)
    return base + type_bonus


def add_prospect(
    business_id: str,
    opportunity_type: str,
    target_url: str,
    domain_rating: int = 0,
    page_title: str = "",
    contact_email: str = "",
    contact_name: str = "",
    anchor_context: str = "",
    your_page_to_link: str = "",
    pitch_angle: str = "",
) -> dict:
    pid = _prospect_id(business_id, target_url)
    domain = _extract_domain(target_url)
    priority = _priority_score(domain_rating, opportunity_type)
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("""
            INSERT INTO backlink_prospects
                (id,business_id,opportunity_type,target_url,target_domain,domain_rating,
                 page_title,contact_email,contact_name,anchor_context,your_page_to_link,
                 pitch_angle,status,priority_score,discovered_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'new',?,?,?)
            ON CONFLICT(business_id,target_url) DO UPDATE SET
                domain_rating=excluded.domain_rating,
                page_title=excluded.page_title,
                contact_email=excluded.contact_email,
                contact_name=excluded.contact_name,
                pitch_angle=excluded.pitch_angle,
                priority_score=excluded.priority_score,
                updated_at=excluded.updated_at
        """, [pid, business_id, opportunity_type, target_url, domain, domain_rating,
              page_title, contact_email, contact_name, anchor_context, your_page_to_link,
              pitch_angle, priority, now, now])
    return get_prospect(pid)


def get_prospect(prospect_id: str) -> dict:
    with _conn() as c:
        row = c.execute("SELECT * FROM backlink_prospects WHERE id=?", [prospect_id]).fetchone()
    return dict(row) if row else {}


def get_prospects(business_id: str, status: str = "new", limit: int = 50) -> list[dict]:
    with _conn() as c:
        rows = c.execute("""
            SELECT * FROM backlink_prospects
            WHERE business_id=? AND status=?
            ORDER BY priority_score DESC LIMIT ?
        """, [business_id, status, limit]).fetchall()
    return [dict(r) for r in rows]


def update_prospect_status(prospect_id: str, status: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("UPDATE backlink_prospects SET status=?, updated_at=? WHERE id=?",
                  [status, now, prospect_id])


def find_competitor_gap_prospects(business_id: str, your_domain: str, competitors: list[str], limit: int = 30) -> list[dict]:
    """Use DataForSEO to find backlinks competitors have that you don't."""
    try:
        from data.connectors.dataforseo import DataForSEOClient
        client = DataForSEOClient(business_id)
    except Exception:
        log.warning("find_competitor_gap_prospects: DataForSEO unavailable")
        return []

    your_backlinks: set[str] = set()
    try:
        your_data = client.get_backlink_summary(your_domain)
        for item in your_data.get("items", []):
            your_backlinks.add(_extract_domain(item.get("url_from", "")))
    except Exception:
        log.exception("competitor_gap: failed to fetch own backlinks")

    prospects = []
    for comp in competitors[:3]:
        try:
            data = client.get_backlink_summary(comp)
            for item in data.get("items", []):
                source = item.get("url_from", "")
                source_domain = _extract_domain(source)
                if not source_domain or source_domain in your_backlinks:
                    continue
                dr = item.get("domain_from_rank", 0)
                if dr < 20:
                    continue
                p = add_prospect(
                    business_id=business_id,
                    opportunity_type="competitor_gap",
                    target_url=source,
                    domain_rating=dr,
                    page_title=item.get("title", ""),
                    anchor_context=f"Competitor {comp} has a link from this page",
                    pitch_angle=f"They link to {comp} — offer your content as a better/complementary resource",
                )
                prospects.append(p)
                if len(prospects) >= limit:
                    break
        except Exception:
            log.exception("competitor_gap: failed for %s", comp)
        if len(prospects) >= limit:
            break

    log.info("find_competitor_gap_prospects  biz=%s  found=%d", business_id, len(prospects))
    return prospects


def find_unlinked_mentions(business_id: str, brand_name: str, your_domain: str) -> list[dict]:
    """Use DataForSEO Content Analysis to find brand mentions without links."""
    try:
        from data.connectors.dataforseo import DataForSEOClient
        client = DataForSEOClient(business_id)
    except Exception:
        log.warning("find_unlinked_mentions: DataForSEO unavailable")
        return []

    prospects = []
    try:
        data = client._post("/content_analysis/search/live", [{
            "keyword": brand_name,
            "type": "organic",
            "filters": ["domain_rank", ">", 20],
            "limit": 50,
        }])
        for item in (data or []):
            url = item.get("url", "")
            if your_domain in url:
                continue
            dr = item.get("domain_rank", 0)
            p = add_prospect(
                business_id=business_id,
                opportunity_type="unlinked_mention",
                target_url=url,
                domain_rating=dr,
                page_title=item.get("title", ""),
                anchor_context=f"Mentions '{brand_name}' without a link",
                pitch_angle="Already mentioned your brand — just ask them to add the link",
            )
            prospects.append(p)
    except Exception:
        log.exception("find_unlinked_mentions  biz=%s", business_id)

    log.info("find_unlinked_mentions  biz=%s  found=%d", business_id, len(prospects))
    return prospects


def find_local_citation_opportunities(business_id: str, niche: str, location: str) -> list[dict]:
    """Add high-value local citation sources as prospects."""
    CITATION_SOURCES = [
        {"url": "https://www.yelp.com/biz/", "dr": 94, "title": "Yelp Business Listing"},
        {"url": "https://www.yellowpages.com/", "dr": 86, "title": "Yellow Pages"},
        {"url": "https://www.bbb.org/", "dr": 91, "title": "Better Business Bureau"},
        {"url": "https://www.houzz.com/", "dr": 90, "title": "Houzz Pro"},
        {"url": "https://www.angi.com/", "dr": 88, "title": "Angi (Angie's List)"},
        {"url": "https://www.thumbtack.com/", "dr": 87, "title": "Thumbtack"},
        {"url": "https://www.homeadvisor.com/", "dr": 83, "title": "HomeAdvisor"},
        {"url": "https://nextdoor.com/", "dr": 85, "title": "Nextdoor Business"},
        {"url": "https://maps.google.com/", "dr": 99, "title": "Google Business Profile"},
        {"url": "https://www.facebook.com/business/", "dr": 99, "title": "Facebook Business Page"},
    ]
    prospects = []
    for source in CITATION_SOURCES:
        p = add_prospect(
            business_id=business_id,
            opportunity_type="local_citation",
            target_url=source["url"],
            domain_rating=source["dr"],
            page_title=source["title"],
            pitch_angle=f"Create/claim listing for {niche} in {location}",
            your_page_to_link="homepage",
        )
        prospects.append(p)
    log.info("find_local_citation_opportunities  biz=%s  sources=%d", business_id, len(prospects))
    return prospects


def run_prospect_sweep(business_id: str) -> dict:
    """Full prospecting run: competitor gaps + unlinked mentions + local citations."""
    import json as _json
    from pathlib import Path

    try:
        all_biz = _json.loads(Path("data/storage/businesses.json").read_text())
        biz = next(
            (b for b in (all_biz if isinstance(all_biz, list) else all_biz.values())
             if b.get("id") == business_id or b.get("business_id") == business_id),
            {}
        )
    except Exception:
        biz = {}

    domain = biz.get("domain", "").replace("https://", "").replace("http://", "").rstrip("/")
    brand = biz.get("business_name", biz.get("name", ""))
    competitors = biz.get("competitors", [])
    niche = biz.get("niche", biz.get("service_type", "home services"))
    location = biz.get("city", biz.get("location", ""))

    results: dict[str, int] = {}

    if competitors and domain:
        gaps = find_competitor_gap_prospects(business_id, domain, competitors)
        results["competitor_gaps"] = len(gaps)

    if brand and domain:
        mentions = find_unlinked_mentions(business_id, brand, domain)
        results["unlinked_mentions"] = len(mentions)

    if niche and location:
        citations = find_local_citation_opportunities(business_id, niche, location)
        results["local_citations"] = len(citations)

    total = sum(results.values())
    log.info("run_prospect_sweep  biz=%s  total=%d  breakdown=%s", business_id, total, results)
    return {"total_found": total, "breakdown": results}


def record_acquired_backlink(
    business_id: str,
    source_url: str,
    target_url: str,
    anchor_text: str = "",
    domain_rating: int = 0,
    is_dofollow: bool = True,
) -> None:
    import hashlib as _h
    bid = _h.md5(f"{business_id}:{source_url}:{target_url}".encode()).hexdigest()[:12]
    source_domain = _extract_domain(source_url)
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("""
            INSERT INTO backlink_acquired
                (id,business_id,source_url,source_domain,target_url,anchor_text,
                 domain_rating,is_dofollow,first_seen,last_checked,is_live)
            VALUES (?,?,?,?,?,?,?,?,?,?,1)
            ON CONFLICT DO NOTHING
        """, [bid, business_id, source_url, source_domain, target_url,
              anchor_text, domain_rating, int(is_dofollow), now, now])
    # Mark prospect as won if it exists
    with _conn() as c:
        pid = _prospect_id(business_id, source_url)
        c.execute("UPDATE backlink_prospects SET status='won', updated_at=? WHERE id=?", [now, pid])


def check_backlink_health(business_id: str) -> dict:
    """Check if acquired backlinks are still live."""
    import urllib.request
    import urllib.error
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM backlink_acquired WHERE business_id=? AND is_live=1",
            [business_id]
        ).fetchall()

    live, dead = 0, 0
    now = datetime.now(timezone.utc).isoformat()
    for row in rows:
        url = row["source_url"]
        still_live = False
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "SEOEngine-LinkChecker/1.0"})
            resp = urllib.request.urlopen(req, timeout=8)
            still_live = resp.status < 400
        except Exception:
            pass

        with _conn() as c:
            c.execute(
                "UPDATE backlink_acquired SET is_live=?, last_checked=? WHERE business_id=? AND source_url=?",
                [int(still_live), now, business_id, url]
            )
        if still_live:
            live += 1
        else:
            dead += 1

    log.info("check_backlink_health  biz=%s  live=%d  dead=%d", business_id, live, dead)
    return {"live": live, "dead": dead, "total_checked": live + dead}


def get_backlink_stats(business_id: str) -> dict:
    with _conn() as c:
        total = c.execute("SELECT COUNT(*) FROM backlink_acquired WHERE business_id=? AND is_live=1", [business_id]).fetchone()[0]
        dofollow = c.execute("SELECT COUNT(*) FROM backlink_acquired WHERE business_id=? AND is_live=1 AND is_dofollow=1", [business_id]).fetchone()[0]
        avg_dr = c.execute("SELECT AVG(domain_rating) FROM backlink_acquired WHERE business_id=? AND is_live=1", [business_id]).fetchone()[0] or 0
        prospects_new = c.execute("SELECT COUNT(*) FROM backlink_prospects WHERE business_id=? AND status='new'", [business_id]).fetchone()[0]
        prospects_contacted = c.execute("SELECT COUNT(*) FROM backlink_prospects WHERE business_id=? AND status='contacted'", [business_id]).fetchone()[0]
        won = c.execute("SELECT COUNT(*) FROM backlink_prospects WHERE business_id=? AND status='won'", [business_id]).fetchone()[0]
    return {
        "live_backlinks": total,
        "dofollow": dofollow,
        "avg_domain_rating": round(avg_dr, 1),
        "prospects_new": prospects_new,
        "prospects_in_outreach": prospects_contacted,
        "won": won,
    }


def find_resource_pages_playwright(business_id: str, niche: str, your_domain: str, limit: int = 20) -> list:
    """
    Use Firecrawl/Playwright to find resource pages and broken-link opportunities.
    Searches Google for 'niche + "useful resources"' via AION GPT-Researcher,
    then scrapes each result for outbound links.
    """
    try:
        from core.aion_bridge import aion
    except Exception:
        log.warning("find_resource_pages_playwright: aion_bridge unavailable")
        return []

    prospects = []
    queries = [
        f"{niche} useful resources links",
        f"{niche} recommended sites",
        f"best {niche} websites resources",
    ]

    for query in queries[:2]:
        try:
            report = aion.gpt_research(query)
            if not report:
                continue
            import re
            urls = re.findall(r'https?://[^\s\)\]\'"<>]+', report)
            urls = [u for u in urls if your_domain not in u and len(u) < 200][:10]

            for url in urls:
                md = aion.firecrawl_scrape(url)
                if not md:
                    continue
                # Check if this page links out a lot (resource page signal)
                outbound = re.findall(r'https?://[^\s\)\]\'"<>]+', md)
                if len(outbound) < 5:
                    continue
                p = add_prospect(
                    business_id=business_id,
                    opportunity_type="resource_page",
                    target_url=url,
                    domain_rating=0,
                    page_title=md[:80].strip(),
                    pitch_angle=f"Resource page with {len(outbound)} outbound links — request inclusion",
                    anchor_context=f"Found via '{query}' research",
                )
                prospects.append(p)
                if len(prospects) >= limit:
                    break
        except Exception:
            log.exception("find_resource_pages_playwright: query=%s", query)

        if len(prospects) >= limit:
            break

    log.info("find_resource_pages_playwright  biz=%s  found=%d", business_id, len(prospects))
    return prospects


def find_broken_link_opportunities(business_id: str, niche: str, limit: int = 15) -> list:
    """
    Use Firecrawl to scrape high-authority pages in the niche and
    identify broken outbound links (404s) as replacement opportunities.
    """
    try:
        from core.aion_bridge import aion
        import urllib.request, urllib.error, re
    except Exception:
        log.warning("find_broken_link_opportunities: dependencies unavailable")
        return []

    prospects = []
    try:
        report = aion.gpt_research(f"top {niche} guide articles authoritative 2024 2025")
        if not report:
            return []
        urls = re.findall(r'https?://[^\s\)\]\'"<>]+', report)[:8]

        for page_url in urls:
            try:
                md = aion.firecrawl_scrape(page_url)
                if not md:
                    continue
                outbound = list(set(re.findall(r'https?://[^\s\)\]\'"<>]{10,200}', md)))
                for link in outbound[:30]:
                    try:
                        req = urllib.request.Request(
                            link, headers={"User-Agent": "SEOEngine-LinkChecker/1.0"},
                            method="HEAD"
                        )
                        urllib.request.urlopen(req, timeout=5)
                    except urllib.error.HTTPError as e:
                        if e.code in (404, 410, 403):
                            p = add_prospect(
                                business_id=business_id,
                                opportunity_type="broken_link",
                                target_url=page_url,
                                domain_rating=0,
                                page_title=md[:80].strip(),
                                anchor_context=f"Broken link to {link} (HTTP {e.code})",
                                pitch_angle=f"Found a broken link on this page — offer your content as replacement",
                            )
                            prospects.append(p)
                            if len(prospects) >= limit:
                                break
                    except Exception:
                        pass
                if len(prospects) >= limit:
                    break
            except Exception:
                log.exception("broken_link: failed scraping %s", page_url)
    except Exception:
        log.exception("find_broken_link_opportunities  biz=%s", business_id)

    log.info("find_broken_link_opportunities  biz=%s  found=%d", business_id, len(prospects))
    return prospects
