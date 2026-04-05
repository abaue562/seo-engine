"""Google Autocomplete Keyword Expander — zero-dependency keyword discovery.

Based on hassancs91/Keyword-Research-tool-python.
Uses Google's public autocomplete API endpoint directly.
No Selenium, no API key, no scraping — just XML API calls.

Usage:
    from data.analyzers.autocomplete import get_suggestions, expand_keywords

    suggestions = get_suggestions("permanent lights")
    all_keywords = expand_keywords("permanent lights kelowna", depth=2)
"""

from __future__ import annotations

import logging
import time
from xml.etree import ElementTree

import requests

log = logging.getLogger(__name__)

AUTOCOMPLETE_URL = "https://suggestqueries.google.com/complete/search"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def get_suggestions(
    query: str,
    country: str = "ca",
    language: str = "en",
) -> list[str]:
    """Get Google autocomplete suggestions for a query.

    Uses Google's public toolbar API — no key needed.

    Args:
        query: Search query to get suggestions for
        country: Country code (ca, us, gb, etc.)
        language: Language code (en, fr, etc.)

    Returns:
        List of suggestion strings
    """
    params = {
        "output": "toolbar",
        "q": query,
        "gl": country,
        "hl": language,
    }

    try:
        resp = requests.get(AUTOCOMPLETE_URL, params=params, headers=HEADERS, timeout=10)
        resp.raise_for_status()

        # Parse XML response
        root = ElementTree.fromstring(resp.content)
        suggestions = []
        for suggestion in root.iter("suggestion"):
            data = suggestion.get("data", "").strip()
            if data and data != query:
                suggestions.append(data)

        log.debug("autocomplete.found  query=%s  count=%d", query, len(suggestions))
        return suggestions

    except Exception as e:
        log.error("autocomplete.fail  query=%s  err=%s", query, e)
        return []


def get_suggestions_json(query: str, country: str = "ca") -> list[str]:
    """Alternative: JSON endpoint (Firefox format)."""
    params = {
        "client": "firefox",
        "q": query,
        "gl": country,
    }
    try:
        resp = requests.get(
            "https://suggestqueries.google.com/complete/search",
            params=params, headers=HEADERS, timeout=10,
        )
        data = resp.json()
        if isinstance(data, list) and len(data) > 1:
            return [s for s in data[1] if s != query]
    except Exception:
        pass
    return []


def expand_keywords(
    seed: str,
    depth: int = 2,
    delay: float = 1.0,
    country: str = "ca",
    max_per_level: int = 8,
) -> list[str]:
    """Recursively expand a seed keyword using autocomplete suggestions.

    Depth 1 = ~8 suggestions
    Depth 2 = ~64 suggestions (8 x 8)
    Depth 3 = ~512 suggestions

    Args:
        seed: Starting keyword
        depth: Recursion depth (1-3 recommended)
        delay: Seconds between API calls
        country: Country code
        max_per_level: Max suggestions per expansion

    Returns:
        Deduplicated list of all discovered keywords
    """
    all_keywords = set()
    all_keywords.add(seed)

    def _expand(query: str, current_depth: int):
        if current_depth <= 0:
            return

        time.sleep(delay)
        suggestions = get_suggestions(query, country=country)

        for s in suggestions[:max_per_level]:
            if s not in all_keywords:
                all_keywords.add(s)
                _expand(s, current_depth - 1)

    _expand(seed, depth)

    result = sorted(all_keywords)
    log.info("autocomplete.expanded  seed=%s  depth=%d  total=%d", seed, depth, len(result))
    return result


def alphabet_expand(seed: str, country: str = "ca", delay: float = 0.5) -> list[str]:
    """Expand keyword with every letter of the alphabet appended.

    "permanent lights" → "permanent lights a", "permanent lights b", etc.
    Returns all unique suggestions.
    """
    all_suggestions = set()

    for letter in "abcdefghijklmnopqrstuvwxyz":
        query = f"{seed} {letter}"
        time.sleep(delay)
        suggestions = get_suggestions(query, country=country)
        all_suggestions.update(suggestions)

    result = sorted(all_suggestions)
    log.info("autocomplete.alphabet  seed=%s  total=%d", seed, len(result))
    return result
