"""SERP Scraper — real Google search results extraction.

Scrapes actual Google SERPs to get:
  - Organic results (title, URL, description, position)
  - People Also Ask questions
  - Featured snippets
  - AI Overview presence
  - Competitor rankings

Uses Scrapling (adaptive scraper) for resilient extraction.

Usage:
    from core.serp.scraper import scrape_serp, get_real_rankings

    results = scrape_serp("permanent lights kelowna")
    rankings = get_real_rankings(["permanent lights kelowna", "landscape lighting kelowna"])
"""

from __future__ import annotations

import re
import logging
import time

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

GOOGLE_URL = "https://www.google.com/search"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-CA,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
}


def scrape_serp(query: str, num: int = 10, country: str = "ca", delay: float = 2.0) -> dict:
    """Scrape Google SERP for a query.

    Args:
        query: Search query
        num: Number of results to request
        country: Country code (ca, us, etc.)
        delay: Pre-request delay (rate limiting)

    Returns:
        Dict with: organic_results, paa_questions, has_ai_overview, has_featured_snippet
    """
    time.sleep(delay)

    params = {"q": query, "num": num, "gl": country, "hl": "en"}

    try:
        resp = requests.get(GOOGLE_URL, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        log.error("serp.fetch_fail  query=%s  err=%s", query, e)
        return {"error": str(e), "organic_results": [], "paa_questions": []}

    soup = BeautifulSoup(html, "html.parser")

    # Extract organic results
    organic = []
    for i, div in enumerate(soup.select("div.g, div[data-sokoban-container]")):
        try:
            link = div.find("a", href=True)
            title_el = div.find("h3")
            snippet_el = div.find("div", class_=re.compile("VwiC3b|IsZvec|s3v9rd"))

            if not link or not title_el:
                continue

            url = link["href"]
            if not url.startswith("http"):
                continue

            title = title_el.get_text(strip=True)
            snippet = snippet_el.get_text(strip=True) if snippet_el else ""

            organic.append({
                "position": len(organic) + 1,
                "title": title,
                "url": url,
                "domain": re.sub(r"https?://(?:www\.)?", "", url).split("/")[0],
                "snippet": snippet[:300],
            })
        except Exception:
            continue

    # Extract PAA questions
    paa = []
    for el in soup.find_all(attrs={"data-q": True}):
        q = el.get("data-q", "").strip()
        if q:
            paa.append(q)
    # Fallback: look for question-like text in expandable elements
    if not paa:
        for el in soup.find_all(["div", "span"]):
            text = el.get_text(strip=True)
            if text.endswith("?") and 10 < len(text) < 150:
                if text not in paa:
                    paa.append(text)

    # Detect AI Overview
    has_ai_overview = bool(soup.find(attrs={"data-sgrd": True}) or
                          soup.find(string=re.compile("AI Overview", re.IGNORECASE)))

    # Detect Featured Snippet
    has_featured_snippet = bool(soup.find("div", class_=re.compile("kp-blk|featured-snippet|xpdopen")))

    log.info("serp.scraped  query=%s  results=%d  paa=%d  ai_overview=%s",
             query, len(organic), len(paa), has_ai_overview)

    return {
        "query": query,
        "organic_results": organic[:num],
        "paa_questions": paa[:10],
        "has_ai_overview": has_ai_overview,
        "has_featured_snippet": has_featured_snippet,
        "result_count": len(organic),
    }


def get_real_rankings(
    keywords: list[str],
    target_domain: str = "",
    country: str = "ca",
    delay: float = 3.0,
) -> dict[str, dict]:
    """Get real Google rankings for multiple keywords.

    Args:
        keywords: List of keywords to check
        target_domain: Your domain to find in results
        country: Country code
        delay: Delay between requests (respect rate limits)

    Returns:
        Dict mapping keyword → {position, url, title, competitors}
    """
    rankings = {}

    for kw in keywords:
        serp = scrape_serp(kw, num=20, country=country, delay=delay)

        our_position = None
        our_url = ""
        competitors = []

        for result in serp.get("organic_results", []):
            domain = result.get("domain", "")
            if target_domain and target_domain.lower() in domain.lower():
                our_position = result["position"]
                our_url = result["url"]
            else:
                competitors.append({
                    "position": result["position"],
                    "domain": domain,
                    "title": result["title"],
                })

        rankings[kw] = {
            "position": our_position,
            "url": our_url,
            "competitors": competitors[:5],
            "has_ai_overview": serp.get("has_ai_overview", False),
            "paa_questions": serp.get("paa_questions", []),
            "total_results": serp.get("result_count", 0),
        }

        log.info("ranking.checked  keyword=%s  position=%s  competitors=%d",
                 kw, our_position or "not found", len(competitors))

    return rankings


def find_competitor_rankings(
    keywords: list[str],
    competitors: list[str],
    country: str = "ca",
    delay: float = 3.0,
) -> dict:
    """Find where competitors rank for your keywords.

    Returns dict: keyword → {competitor_name: position}
    """
    results = {}

    for kw in keywords:
        serp = scrape_serp(kw, num=20, country=country, delay=delay)
        kw_results = {}

        for result in serp.get("organic_results", []):
            domain = result.get("domain", "").lower()
            for comp in competitors:
                if comp.lower().replace(" ", "") in domain or domain in comp.lower():
                    kw_results[comp] = result["position"]

        results[kw] = kw_results

    return results
