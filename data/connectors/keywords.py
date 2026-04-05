"""Keyword discovery — Google Autocomplete + related searches.

Free tier: scrapes Google's suggestion API.
Paid tier: plug in SEMrush/Ahrefs API key for volume + difficulty.
"""

from __future__ import annotations

import logging
from datetime import datetime

import httpx
from pydantic import BaseModel

log = logging.getLogger(__name__)


class KeywordSuggestion(BaseModel):
    keyword: str
    source: str = "autocomplete"  # autocomplete / related / semrush / ahrefs
    volume: int = 0               # 0 = unknown (free tier)
    difficulty: int = 0           # 0 = unknown


class KeywordData(BaseModel):
    seed_query: str
    suggestions: list[KeywordSuggestion] = []
    fetched_at: datetime = datetime.utcnow()


async def fetch_autocomplete(query: str) -> list[str]:
    """Pull Google autocomplete suggestions for a query."""
    url = "https://suggestqueries.google.com/complete/search"
    params = {"client": "firefox", "q": query}

    async with httpx.AsyncClient() as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

    # Response format: [query, [suggestions]]
    suggestions = data[1] if len(data) > 1 else []
    log.info("keywords.autocomplete  query=%s  results=%d", query, len(suggestions))
    return suggestions


async def discover_keywords(
    seed_queries: list[str],
    expand: bool = True,
) -> KeywordData:
    """Discover keywords from multiple seed queries.

    If expand=True, also runs autocomplete on each suggestion (2-deep).
    """
    all_suggestions: list[KeywordSuggestion] = []
    seen: set[str] = set()

    for seed in seed_queries:
        results = await fetch_autocomplete(seed)
        for kw in results:
            kw_lower = kw.lower().strip()
            if kw_lower not in seen:
                seen.add(kw_lower)
                all_suggestions.append(KeywordSuggestion(keyword=kw))

        # 2nd level expansion
        if expand:
            for kw in results[:3]:  # top 3 only to avoid rate limits
                sub_results = await fetch_autocomplete(kw)
                for sub_kw in sub_results:
                    sub_lower = sub_kw.lower().strip()
                    if sub_lower not in seen:
                        seen.add(sub_lower)
                        all_suggestions.append(KeywordSuggestion(keyword=sub_kw))

    log.info("keywords.discovered  seeds=%d  total=%d", len(seed_queries), len(all_suggestions))

    return KeywordData(
        seed_query=", ".join(seed_queries),
        suggestions=all_suggestions,
        fetched_at=datetime.utcnow(),
    )


def keywords_to_prompt_block(data: KeywordData) -> str:
    """Render discovered keywords as agent context."""
    lines = [
        f"KEYWORD DISCOVERY (from: {data.seed_query}):",
        f"Total keywords found: {len(data.suggestions)}",
        "",
    ]
    for s in data.suggestions[:30]:
        vol = f" vol={s.volume}" if s.volume else ""
        diff = f" diff={s.difficulty}" if s.difficulty else ""
        lines.append(f"  - {s.keyword}{vol}{diff}")

    return "\n".join(lines)
