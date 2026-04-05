"""SERP Reverse Engineering — analyzes top-ranking pages to extract winning patterns.

Crawls the top 3 pages for a keyword, extracts content signals,
and identifies weaknesses to exploit.
"""

from __future__ import annotations

import logging
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

from prediction.models import SERPProfile

log = logging.getLogger(__name__)


async def analyze_serp(keyword: str, city: str = "", top_n: int = 3) -> SERPProfile:
    """Fetch and analyze top-ranking pages for a keyword."""
    profile = SERPProfile(keyword=keyword)
    query = f"{keyword} {city}".strip()

    log.info("serp.analyze  keyword=%s  city=%s", keyword, city)

    try:
        # Fetch SERP
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://www.google.com/search",
                params={"q": query, "num": top_n + 2},
                headers={"User-Agent": "Mozilla/5.0 (compatible; SEOEngine/1.0)"},
            )

            if resp.status_code != 200:
                log.warning("serp.blocked  status=%d", resp.status_code)
                return profile

            soup = BeautifulSoup(resp.text, "lxml")

        # Extract organic result URLs
        urls = []
        for result in soup.select("div.g"):
            link = result.select_one("a[href]")
            if link:
                href = link.get("href", "")
                if href.startswith("http"):
                    urls.append(href)
                    if len(urls) >= top_n:
                        break

        # Analyze each top page
        word_counts = []
        all_h2s = []
        all_keywords = []

        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            for url in urls:
                try:
                    page_resp = await client.get(url)
                    page_soup = BeautifulSoup(page_resp.text, "lxml")

                    # Word count
                    body_text = page_soup.get_text(separator=" ", strip=True)
                    words = len(body_text.split())
                    word_counts.append(words)

                    # H2 headings
                    h2s = [h.get_text(strip=True) for h in page_soup.find_all("h2")]
                    all_h2s.extend(h2s)

                    # Extract keyword from title
                    title = page_soup.title.string if page_soup.title else ""
                    if title:
                        title_words = title.lower().split()
                        all_keywords.extend(title_words)

                except Exception as e:
                    log.debug("serp.page_fail  url=%s  err=%s", url, e)

        # Aggregate
        if word_counts:
            profile.avg_word_count = round(sum(word_counts) / len(word_counts))

        # Find common headings (appearing in 2+ pages)
        h2_counts: dict[str, int] = {}
        for h in all_h2s:
            h_lower = h.lower().strip()
            h2_counts[h_lower] = h2_counts.get(h_lower, 0) + 1
        profile.content_structure = [h for h, c in sorted(h2_counts.items(), key=lambda x: -x[1]) if c >= 2][:10]

        # Common title keywords
        kw_counts: dict[str, int] = {}
        for w in all_keywords:
            if len(w) > 3:
                kw_counts[w] = kw_counts.get(w, 0) + 1
        profile.common_keywords = [w for w, c in sorted(kw_counts.items(), key=lambda x: -x[1])][:15]

        # Identify weaknesses (short content, missing structure)
        weaknesses = []
        if profile.avg_word_count < 800:
            weaknesses.append("Top pages have thin content — opportunity to outwrite them")
        if len(profile.content_structure) < 3:
            weaknesses.append("Top pages lack structured headings — add comprehensive H2 structure")
        if not profile.common_keywords:
            weaknesses.append("No strong keyword patterns in titles — opportunity for better targeting")
        profile.weaknesses = weaknesses

        profile.analyzed_at = datetime.utcnow()
        log.info("serp.done  keyword=%s  avg_words=%d  structures=%d  weaknesses=%d",
                 keyword, profile.avg_word_count, len(profile.content_structure), len(weaknesses))

    except Exception as e:
        log.error("serp.fail  keyword=%s  err=%s", keyword, e)

    return profile


def serp_to_prompt_block(profile: SERPProfile) -> str:
    """Render SERP analysis as agent context."""
    lines = [
        f"SERP ANALYSIS for '{profile.keyword}':",
        f"  Average word count of top pages: {profile.avg_word_count}",
        f"  Common H2 patterns: {', '.join(profile.content_structure[:5]) or 'none detected'}",
        f"  Common keywords: {', '.join(profile.common_keywords[:10]) or 'none detected'}",
    ]
    if profile.weaknesses:
        lines.append("  Weaknesses to exploit:")
        for w in profile.weaknesses:
            lines.append(f"    - {w}")
    return "\n".join(lines)
