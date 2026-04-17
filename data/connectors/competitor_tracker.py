"""Ongoing competitor tracking — weekly crawl + diff + gap alerts.

Crawls top pages of each competitor, diffs against last week,
alerts on new pages in target clusters.
"""
from __future__ import annotations
import hashlib, json, logging, os, time
from datetime import datetime, timezone
from pathlib import Path
import requests

log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "SEOEngineBot/1.0 (+https://gethubed.com/bot)"}
_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_SNAPSHOT_DIR = Path("data/storage/competitor_snapshots")


def _redis():
    try:
        import redis as _r
        r = _r.from_url(_REDIS_URL, decode_responses=True, socket_timeout=2)
        r.ping()
        return r
    except Exception:
        return None


def _snapshot_key(competitor_domain: str, business_id: str) -> str:
    return f"competitor:snapshot:{business_id}:{competitor_domain}"


def crawl_competitor_top_pages(domain: str, limit: int = 10) -> list[dict]:
    """Fetch the competitor's sitemap or homepage to get their top pages."""
    pages = []
    # Try sitemap first
    try:
        resp = requests.get(f"https://{domain}/sitemap.xml", headers=HEADERS, timeout=10)
        if resp.ok and "<url>" in resp.text:
            from xml.etree import ElementTree as ET
            root = ET.fromstring(resp.text)
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for url_el in root.findall(".//sm:url", ns)[:limit]:
                loc = url_el.find("sm:loc", ns)
                lastmod = url_el.find("sm:lastmod", ns)
                if loc is not None:
                    pages.append({"url": loc.text, "lastmod": lastmod.text if lastmod is not None else None})
            return pages
    except Exception:
        pass
    # Fallback: just record the homepage
    pages.append({"url": f"https://{domain}/", "lastmod": None})
    return pages


def _page_fingerprint(page: dict) -> str:
    raw = json.dumps(page, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()


def diff_competitor(business_id: str, competitor_domain: str) -> dict:
    """Crawl competitor, diff against last snapshot, return changes."""
    current_pages = crawl_competitor_top_pages(competitor_domain)
    current_map = {p["url"]: p for p in current_pages}

    r = _redis()
    prev_json = r.get(_snapshot_key(competitor_domain, business_id)) if r else None
    prev_map: dict = json.loads(prev_json) if prev_json else {}

    new_pages = [p for url, p in current_map.items() if url not in prev_map]
    removed_pages = [p for url, p in prev_map.items() if url not in current_map]

    # Store new snapshot
    if r:
        try:
            r.setex(_snapshot_key(competitor_domain, business_id), 86400 * 8, json.dumps(current_map))
        except Exception:
            pass

    result = {
        "business_id": business_id,
        "competitor": competitor_domain,
        "checked_at": datetime.now(tz=timezone.utc).isoformat(),
        "total_pages": len(current_pages),
        "new_pages": new_pages,
        "removed_pages": removed_pages,
        "has_changes": bool(new_pages or removed_pages),
    }

    if new_pages:
        log.info("competitor_tracker.new_pages  biz=%s  competitor=%s  count=%d",
                 business_id, competitor_domain, len(new_pages))

    return result


def run_competitor_tracking(business_id: str, competitor_domains: list[str]) -> list[dict]:
    """Run tracking for all competitors of a business. Called by beat task."""
    results = []
    for domain in competitor_domains:
        try:
            result = diff_competitor(business_id, domain)
            results.append(result)
            time.sleep(2)  # polite crawl delay between competitors
        except Exception as e:
            log.warning("competitor_tracker.fail  biz=%s  domain=%s  err=%s", business_id, domain, e)
    return results
