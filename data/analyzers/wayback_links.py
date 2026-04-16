"""Wayback Machine Broken Link Mapper / Competitor Opportunity Finder.

Based on searchsolved/search-solved-public-seo (MIT license).

Two complementary use cases:
  1. Own domain: find URLs that used to exist but are now 404 → add redirects
  2. Competitor domain: find dead pages others linked to → broken link building

Usage:
    from data.analyzers.wayback_links import WaybackLinkFinder, find_broken_links

    # Class API (broken link building):
    finder = WaybackLinkFinder()
    opportunities = finder.find_opportunities("competitor.com")

    # Legacy functional API (own-site redirects):
    dead_urls = find_broken_links("blendbrightlights.com", current_urls=["/about"])
    redirects = suggest_redirects(dead_urls, current_pages)
"""

from __future__ import annotations

import asyncio
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

WAYBACK_CDX_API = "http://web.archive.org/cdx/search/cdx"
STATIC_EXTENSIONS = frozenset({
    ".css", ".js", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".pdf",
    ".ico", ".woff", ".woff2", ".ttf", ".eot", ".mp4", ".webm", ".zip",
    ".gz", ".xml", ".json",
})
_SKIP_PATH_SEGS = frozenset([
    "feed", "sitemap", "robots.txt", "favicon", "wp-login", "wp-admin",
    "xmlrpc", ".well-known",
])


# ── WaybackLinkFinder class API ───────────────────────────────────────────────

class WaybackLinkFinder:
    """Finds competitor backlink opportunities via Wayback Machine CDX API."""

    WAYBACK_CDX_URL = WAYBACK_CDX_API

    def get_archived_pages(self, domain: str, limit: int = 100) -> list[dict]:
        """Get pages from domain that Wayback Machine has archived.

        Uses CDX API with fields: original, statuscode, timestamp
        Returns list of {url, status_code, timestamp}
        """
        params = {
            "url": f"{domain}/*",
            "output": "json",
            "fl": "original,statuscode,timestamp",
            "limit": limit * 3,          # over-fetch; we'll filter
            "filter": "mimetype:text/html",
            "collapse": "urlkey",
        }
        try:
            resp = requests.get(self.WAYBACK_CDX_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error("wayback.cdx_fail  domain=%s  err=%s", domain, e)
            return []

        if len(data) < 2:
            return []

        headers = data[0]
        rows = data[1:]

        results = []
        seen: set[str] = set()

        for row in rows:
            if len(row) < 3:
                continue
            url, status_code, timestamp = row[0], row[1], row[2]

            parsed = urlparse(url)
            path = parsed.path.rstrip("/").lower()

            # Skip static assets and system paths
            if any(path.endswith(ext) for ext in STATIC_EXTENSIONS):
                continue
            if any(seg in path for seg in _SKIP_PATH_SEGS):
                continue
            if parsed.query or parsed.fragment:
                continue
            if path in seen:
                continue
            seen.add(path)

            results.append({
                "url": url,
                "status_code": status_code,
                "timestamp": timestamp,
            })

            if len(results) >= limit:
                break

        log.info("wayback.archived_pages  domain=%s  count=%d", domain, len(results))
        return results

    def check_live_status(self, urls: list[str]) -> dict[str, int]:
        """Check which URLs are currently live (200) or dead (404/other).

        Uses a thread pool for speed. Returns {url: status_code}.
        """
        results: dict[str, int] = {}

        def _check(url: str) -> tuple[str, int]:
            try:
                resp = requests.head(
                    url, timeout=10, allow_redirects=True,
                    headers={"User-Agent": "Mozilla/5.0 (compatible; SEOEngine/1.0)"},
                )
                return url, resp.status_code
            except Exception:
                return url, 0  # 0 = connection error / unreachable

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(_check, u): u for u in urls}
            for future in as_completed(futures):
                url, code = future.result()
                results[url] = code

        return results

    def find_opportunities(
        self,
        competitor_domain: str,
        min_archive_age_days: int = 180,
    ) -> list[dict]:
        """Find broken link building opportunities on a competitor domain.

        Steps:
          1. Get archived pages for competitor
          2. Filter to pages that existed 6+ months ago
          3. Check current live status — find 404s
          4. Estimate topic from URL slug
          5. Score each opportunity

        Returns list of {dead_url, last_archived, topic_guess, opportunity_score}
        sorted by score descending.
        """
        archived = self.get_archived_pages(competitor_domain, limit=200)

        if not archived:
            log.warning("wayback.no_archived_pages  domain=%s", competitor_domain)
            return []

        # Filter to pages old enough (min_archive_age_days since last known archive)
        cutoff_ts = (datetime.utcnow() - timedelta(days=min_archive_age_days)).strftime("%Y%m%d")
        old_pages = [p for p in archived if p["timestamp"] <= cutoff_ts]

        if not old_pages:
            log.info("wayback.no_old_pages  domain=%s  cutoff=%s", competitor_domain, cutoff_ts)
            old_pages = archived  # fall back to all archived pages

        # Build full URLs to check
        urls_to_check = [p["url"] for p in old_pages]

        log.info("wayback.checking_live  domain=%s  urls=%d", competitor_domain, len(urls_to_check))
        live_status = self.check_live_status(urls_to_check)

        opportunities = []
        for page in old_pages:
            url = page["url"]
            status = live_status.get(url, 0)

            # 404, 410 Gone, or unreachable (0) = dead link opportunity
            if status not in (404, 410, 0):
                continue

            topic = self.estimate_topic(url)
            if not topic:
                continue   # Skip if we can't guess the topic (e.g., bare domain root)

            # Opportunity score: prefer longer slugs (more specific topic) + older archives
            slug_depth = url.rstrip("/").count("/") - 2  # depth beyond domain
            age_score = max(0, int(cutoff_ts) - int(page["timestamp"][:8])) // 10000
            opp_score = min(10, slug_depth * 2 + min(age_score, 5))

            opportunities.append({
                "dead_url": url,
                "http_status": status,
                "last_archived": page["timestamp"],
                "topic_guess": topic,
                "opportunity_score": opp_score,
            })

        # Sort by score descending
        opportunities.sort(key=lambda x: x["opportunity_score"], reverse=True)

        log.info("wayback.opportunities  domain=%s  found=%d", competitor_domain, len(opportunities))
        return opportunities

    def estimate_topic(self, url: str) -> str:
        """Guess the topic of a dead URL from its path/slug.

        Parses URL path, removes common suffixes (.html, .php),
        replaces hyphens/underscores with spaces, strips numeric IDs.
        Returns a cleaned human-readable topic string.
        """
        parsed = urlparse(url)
        path = parsed.path.strip("/")
        if not path:
            return ""

        # Take last meaningful path segment
        segments = [s for s in path.split("/") if s]
        if not segments:
            return ""

        slug = segments[-1]

        # Remove file extensions
        slug = re.sub(r"\.(html?|php|asp|aspx|jsp|cfm)$", "", slug, flags=re.IGNORECASE)

        # Remove trailing numeric IDs (e.g., "post-1234")
        slug = re.sub(r"[-_]\d{3,}$", "", slug)

        # Replace separators with spaces
        slug = re.sub(r"[-_]+", " ", slug).strip()

        return slug.lower() if len(slug) > 2 else ""


# ── Legacy functional API ─────────────────────────────────────────────────────

def find_broken_links(
    domain: str,
    current_urls: list[str] | None = None,
    max_results: int = 200,
) -> list[dict]:
    """Find URLs from Wayback Machine that no longer exist on the domain.

    Args:
        domain: Domain to check (e.g., "blendbrightlights.com")
        current_urls: List of currently live URL paths (e.g., ["/about", "/services"])
        max_results: Max archived URLs to check

    Returns:
        List of dead URLs with their archived versions
    """
    params = {
        "url": f"{domain}/*",
        "output": "json",
        "limit": max_results * 2,
        "fl": "original,timestamp,statuscode,mimetype",
        "filter": "mimetype:text/html",
        "collapse": "urlkey",
    }

    try:
        resp = requests.get(WAYBACK_CDX_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error("wayback.cdx_fail  domain=%s  err=%s", domain, e)
        return []

    if len(data) < 2:
        return []

    rows = data[1:]

    current_set: set[str] = set()
    if current_urls:
        for u in current_urls:
            current_set.add(u.rstrip("/").lower())

    dead_urls = []
    seen: set[str] = set()

    for row in rows:
        url = row[0] if len(row) > 0 else ""
        timestamp = row[1] if len(row) > 1 else ""

        if not url:
            continue

        parsed = urlparse(url)
        path = parsed.path.rstrip("/").lower()

        if any(path.endswith(ext) for ext in STATIC_EXTENSIONS):
            continue
        if path.startswith("/.well-known") or path.startswith("/wp-") or path.startswith("/xmlrpc"):
            continue
        if any(seg in path for seg in ["/feed", "/sitemap", "/robots.txt", "/favicon"]):
            continue
        if path in seen or path in current_set:
            continue
        seen.add(path)
        if parsed.query or parsed.fragment:
            continue

        dead_urls.append({
            "url": url,
            "path": path,
            "timestamp": timestamp,
            "archive_url": f"https://web.archive.org/web/{timestamp}/{url}",
        })

        if len(dead_urls) >= max_results:
            break

    log.info("wayback.found  domain=%s  archived=%d  dead=%d", domain, len(rows), len(dead_urls))
    return dead_urls


def _fetch_archived_h1(archive_url: str, timeout: int = 10) -> str:
    """Fetch H1 from a Wayback Machine archived page."""
    try:
        resp = requests.get(
            archive_url, timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; SEOEngine/1.0)"},
        )
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            h1 = soup.find("h1")
            if h1:
                return h1.get_text(strip=True)
    except Exception:
        pass
    return ""


def suggest_redirects(
    dead_urls: list[dict],
    current_pages: list[dict],
    min_similarity: float = 0.4,
    max_workers: int = 10,
) -> list[dict]:
    """Suggest redirect targets for dead URLs using fuzzy H1 matching.

    Args:
        dead_urls: Output from find_broken_links()
        current_pages: List of dicts with: url, title (or h1)
        min_similarity: Minimum fuzzy match score (0-1)
        max_workers: Concurrent threads for fetching archived pages

    Returns:
        List of redirect suggestions sorted by similarity descending.
    """
    if not dead_urls or not current_pages:
        return []

    archive_h1s: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_fetch_archived_h1, u["archive_url"]): u["path"]
            for u in dead_urls[:50]
        }
        for future in as_completed(futures):
            path = futures[future]
            h1 = future.result()
            if h1:
                archive_h1s[path] = h1

    if not archive_h1s:
        return []

    try:
        from rapidfuzz import fuzz
    except ImportError:
        log.warning("wayback.missing_dep  pip install rapidfuzz")
        return []

    current_titles = {
        p.get("title", p.get("h1", "")): p["url"]
        for p in current_pages
        if p.get("title") or p.get("h1")
    }

    suggestions = []
    for dead_path, archive_h1 in archive_h1s.items():
        best_match = ""
        best_score = 0.0
        best_url = ""

        for title, url in current_titles.items():
            score = fuzz.token_sort_ratio(archive_h1.lower(), title.lower()) / 100
            if score > best_score:
                best_score = score
                best_match = title
                best_url = url

        if best_score >= min_similarity:
            dead_url_obj = next((u for u in dead_urls if u["path"] == dead_path), {})
            suggestions.append({
                "dead_url": dead_url_obj.get("url", dead_path),
                "dead_path": dead_path,
                "archive_h1": archive_h1,
                "archive_url": dead_url_obj.get("archive_url", ""),
                "matched_url": best_url,
                "matched_title": best_match,
                "similarity": round(best_score, 3),
            })

    suggestions.sort(key=lambda x: x["similarity"], reverse=True)
    log.info("wayback.redirects  dead=%d  matched=%d", len(archive_h1s), len(suggestions))
    return suggestions
