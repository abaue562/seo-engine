"""Competitor extraction — finds and profiles competing businesses.

Searches Google Maps + SERPs to discover who ranks for your keywords,
then extracts their key SEO signals.
"""

from __future__ import annotations

import logging
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel

log = logging.getLogger(__name__)


class CompetitorProfile(BaseModel):
    name: str
    website: str = ""
    gbp_url: str = ""
    rating: float = 0.0
    review_count: int = 0
    categories: list[str] = []
    strengths: list[str] = []
    weaknesses: list[str] = []
    fetched_at: datetime = datetime.utcnow()


class CompetitorData(BaseModel):
    query: str
    city: str
    competitors: list[CompetitorProfile] = []
    fetched_at: datetime = datetime.utcnow()


async def discover_serp_competitors(
    keywords: list[str],
    city: str,
    max_per_keyword: int = 5,
) -> CompetitorData:
    """Discover competitors from SERP results for target keywords.

    Uses httpx to fetch search results and BeautifulSoup to parse.
    For production, use a proper SERP API (SerpAPI, DataForSEO, etc).
    """
    all_competitors: dict[str, CompetitorProfile] = {}
    query_str = ", ".join(keywords[:3])

    for kw in keywords[:5]:  # Limit to avoid rate limits
        search_query = f"{kw} {city}"
        log.info("competitors.serp  query=%s", search_query)

        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    "https://www.google.com/search",
                    params={"q": search_query, "num": max_per_keyword},
                    headers={"User-Agent": "Mozilla/5.0 (compatible; SEOEngine/1.0)"},
                )

                if resp.status_code != 200:
                    log.warning("competitors.serp_blocked  status=%d", resp.status_code)
                    continue

                soup = BeautifulSoup(resp.text, "lxml")

                for result in soup.select("div.g"):
                    link = result.select_one("a[href]")
                    title = result.select_one("h3")
                    if not link or not title:
                        continue

                    url = link.get("href", "")
                    name = title.get_text(strip=True)

                    if name and url and name not in all_competitors:
                        all_competitors[name] = CompetitorProfile(
                            name=name,
                            website=url,
                            fetched_at=datetime.utcnow(),
                        )

        except Exception as e:
            log.warning("competitors.serp_fail  query=%s  err=%s", search_query, e)

    log.info("competitors.discovered  total=%d", len(all_competitors))

    return CompetitorData(
        query=query_str,
        city=city,
        competitors=list(all_competitors.values())[:10],
        fetched_at=datetime.utcnow(),
    )


def competitors_to_prompt_block(data: CompetitorData) -> str:
    """Render competitor data as agent context."""
    lines = [
        f"COMPETITOR ANALYSIS ({data.query} in {data.city}):",
        f"Competitors found: {len(data.competitors)}",
        "",
    ]
    for c in data.competitors:
        lines.append(f"  {c.name}")
        lines.append(f"    website: {c.website}")
        if c.rating:
            lines.append(f"    rating: {c.rating} ({c.review_count} reviews)")
        if c.strengths:
            lines.append(f"    strengths: {', '.join(c.strengths)}")
        if c.weaknesses:
            lines.append(f"    weaknesses: {', '.join(c.weaknesses)}")

    return "\n".join(lines)
