"""Wayback Machine Broken Link Mapper — recovers lost backlinks from archived pages.

Based on searchsolved/search-solved-public-seo (MIT license).
Finds URLs that existed on your domain but are now dead (404),
checks the Wayback Machine for archived versions, and suggests
redirect targets using fuzzy matching on H1 tags.

Usage:
    from data.analyzers.wayback_links import find_broken_links, suggest_redirects

    dead_urls = find_broken_links("blendbrightlights.com", current_urls=["/about", "/services"])
    redirects = suggest_redirects(dead_urls, current_pages)
"""

from __future__ import annotations

import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

WAYBACK_CDX_API = "http://web.archive.org/cdx/search/cdx"
STATIC_EXTENSIONS = {".css", ".js", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".pdf",
                     ".ico", ".woff", ".woff2", ".ttf", ".eot", ".mp4", ".webm", ".zip"}


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
    # Query Wayback Machine CDX API
    params = {
        "url": f"{domain}/*",
        "output": "json",
        "limit": max_results * 2,  # Over-fetch to account for filtering
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

    if len(data) < 2:  # First row is header
        return []

    headers = data[0]
    rows = data[1:]

    # Normalize current URLs for comparison
    current_set = set()
    if current_urls:
        for u in current_urls:
            current_set.add(u.rstrip("/").lower())

    dead_urls = []
    seen = set()

    for row in rows:
        url = row[0] if len(row) > 0 else ""
        timestamp = row[1] if len(row) > 1 else ""

        if not url:
            continue

        # Clean URL
        from urllib.parse import urlparse
        parsed = urlparse(url)
        path = parsed.path.rstrip("/").lower()

        # Skip static files
        if any(path.endswith(ext) for ext in STATIC_EXTENSIONS):
            continue

        # Skip system/technical paths that have no SEO value
        if path.startswith("/.well-known") or path.startswith("/wp-") or path.startswith("/xmlrpc"):
            continue
        if any(seg in path for seg in ["/feed", "/sitemap", "/robots.txt", "/favicon"]):
            continue

        # Skip if already seen or currently live
        if path in seen or path in current_set:
            continue
        seen.add(path)

        # Skip URLs with query params or fragments
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
        resp = requests.get(archive_url, timeout=timeout, headers={
            "User-Agent": "Mozilla/5.0 (compatible; SEOEngine/1.0)"
        })
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
        List of redirect suggestions: dead_url, archive_h1, matched_url, matched_title, similarity
    """
    if not dead_urls or not current_pages:
        return []

    # Fetch H1s from archived pages concurrently
    archive_h1s = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_fetch_archived_h1, u["archive_url"]): u["path"]
            for u in dead_urls[:50]  # Limit to 50 to be respectful
        }
        for future in as_completed(futures):
            path = futures[future]
            h1 = future.result()
            if h1:
                archive_h1s[path] = h1

    if not archive_h1s:
        return []

    # Fuzzy match archived H1s to current page titles
    try:
        from rapidfuzz import fuzz
    except ImportError:
        log.warning("wayback.missing_dep  pip install rapidfuzz")
        return []

    current_titles = {p.get("title", p.get("h1", "")): p["url"] for p in current_pages if p.get("title") or p.get("h1")}

    suggestions = []
    for dead_path, archive_h1 in archive_h1s.items():
        best_match = ""
        best_score = 0
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
