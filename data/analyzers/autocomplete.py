"""Google Autocomplete Keyword Expander — zero-dependency keyword discovery.

Based on hassancs91/Keyword-Research-tool-python.
Uses Google's public autocomplete API endpoint directly.
No Selenium, no API key, no scraping — just XML/JSON API calls.

Usage:
    from data.analyzers.autocomplete import AutocompleteExpander, get_suggestions, expand_keywords

    # Class API:
    expander = AutocompleteExpander()
    keywords = expander.full_expansion(seed="plumber", location="NYC")

    # Legacy functional API:
    suggestions = get_suggestions("permanent lights")
    all_keywords = expand_keywords("permanent lights kelowna", depth=2)
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from xml.etree import ElementTree

import requests

log = logging.getLogger(__name__)

AUTOCOMPLETE_URL = "https://suggestqueries.google.com/complete/search"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

# ── Intent classification signals ────────────────────────────────────────────

_INFORMATIONAL = frozenset([
    "how", "what", "why", "when", "where", "who", "which", "can", "does",
    "guide", "tips", "tutorial", "learn", "understand", "explain", "definition",
    "vs", "difference",
])
_TRANSACTIONAL = frozenset([
    "buy", "hire", "book", "get", "order", "purchase", "cost", "price",
    "quote", "estimate", "pay", "cheap", "affordable", "emergency", "$",
])
_COMMERCIAL = frozenset([
    "best", "top", "review", "reviews", "compare", "comparison", "versus",
    "rated", "ranking", "recommended",
])


# ── AutocompleteExpander class API ────────────────────────────────────────────

class AutocompleteExpander:
    """Full keyword expansion via Google Autocomplete with clustering."""

    GOOGLE_AC_URL = AUTOCOMPLETE_URL
    MODIFIERS = ["best", "near me", "cost", "how to", "reviews", "vs", "affordable", "emergency"]
    QUESTION_PREFIXES = ["how to", "what is", "why", "when", "where", "who", "which", "can"]

    def get_suggestions(self, query: str, lang: str = "en", country: str = "us") -> list[str]:
        """Get autocomplete suggestions for a query via Firefox JSON endpoint.

        Uses: ?client=firefox&q={query}&hl={lang}&gl={country}
        Returns list of suggestion strings.
        """
        params = {
            "client": "firefox",
            "q": query,
            "hl": lang,
            "gl": country,
        }
        try:
            resp = requests.get(self.GOOGLE_AC_URL, params=params, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list) and len(data) > 1:
                return [s for s in data[1] if s != query]
        except Exception as e:
            log.error("autocomplete.class.fail  query=%s  err=%s", query, e)
        return []

    def expand_with_modifiers(self, seed: str, location: str = "") -> list[str]:
        """Expand seed keyword with common modifiers.

        Tries:
          - "{seed} {modifier}"
          - "{modifier} {seed}"
          - "{seed} in {location}" (if location given)
          - "{seed} {location}" (if location given)

        Returns a deduplicated list of keyword variants.
        """
        collected: set[str] = {seed}

        # Modifier variants
        for mod in self.MODIFIERS:
            for variant in [f"{seed} {mod}", f"{mod} {seed}"]:
                suggestions = self.get_suggestions(variant)
                collected.update(suggestions)
                time.sleep(0.3)

        # Location variants
        if location:
            for loc_variant in [f"{seed} in {location}", f"{seed} {location}", f"{location} {seed}"]:
                suggestions = self.get_suggestions(loc_variant)
                collected.update(suggestions)
                time.sleep(0.3)

        result = sorted(collected)
        log.info("autocomplete.modifiers  seed=%s  total=%d", seed, len(result))
        return result

    def expand_questions(self, seed: str) -> list[str]:
        """Generate question variants for a seed keyword.

        Tries each QUESTION_PREFIXES combined with the seed.
        """
        collected: set[str] = set()

        for prefix in self.QUESTION_PREFIXES:
            query = f"{prefix} {seed}"
            suggestions = self.get_suggestions(query)
            collected.update(suggestions)
            time.sleep(0.3)

        result = sorted(collected)
        log.info("autocomplete.questions  seed=%s  total=%d", seed, len(result))
        return result

    def full_expansion(self, seed: str, location: str = "", depth: int = 2) -> dict:
        """Complete keyword expansion: seed + modifiers + questions + N-level AC expansion.

        Returns:
            {
              seed,
              total_keywords,
              keywords: list[str],
              clusters: {informational: [...], transactional: [...], commercial: [...], navigational: [...]}
            }
        """
        all_keywords: set[str] = {seed}

        # Level 1: modifier + question expansion
        all_keywords.update(self.expand_with_modifiers(seed, location=location))
        all_keywords.update(self.expand_questions(seed))

        # Level 2+: recursive AC expansion from discovered keywords
        if depth >= 2:
            # Take the most interesting seeds (first 20 to avoid excessive API calls)
            second_level_seeds = [kw for kw in sorted(all_keywords) if kw != seed][:20]
            for kw in second_level_seeds:
                suggestions = self.get_suggestions(kw)
                all_keywords.update(suggestions)
                time.sleep(0.4)

        if depth >= 3:
            third_level_seeds = [kw for kw in sorted(all_keywords) if kw not in second_level_seeds][:30]
            for kw in third_level_seeds:
                suggestions = self.get_suggestions(kw)
                all_keywords.update(suggestions)
                time.sleep(0.4)

        keywords = sorted(all_keywords)
        clusters = self.cluster_keywords(keywords)

        log.info("autocomplete.full_expansion  seed=%s  location=%s  depth=%d  total=%d",
                 seed, location, depth, len(keywords))

        return {
            "seed": seed,
            "total_keywords": len(keywords),
            "keywords": keywords,
            "clusters": clusters,
        }

    def cluster_keywords(self, keywords: list[str]) -> dict:
        """Group keywords by intent: informational, navigational, transactional, commercial.

        Heuristic clustering:
        - informational: starts with question word or contains 'how', 'what', 'guide', 'tips'
        - transactional: contains 'buy', 'hire', 'book', 'get', 'cost', 'price', '$'
        - commercial: contains 'best', 'top', 'review', 'vs', 'compare'
        - navigational: everything else (brand/specific queries)
        """
        clusters: dict[str, list[str]] = {
            "informational": [],
            "transactional": [],
            "commercial": [],
            "navigational": [],
        }

        for kw in keywords:
            tokens = set(kw.lower().split())
            if tokens & _TRANSACTIONAL:
                clusters["transactional"].append(kw)
            elif tokens & _COMMERCIAL:
                clusters["commercial"].append(kw)
            elif tokens & _INFORMATIONAL or kw.lower().endswith("?"):
                clusters["informational"].append(kw)
            else:
                clusters["navigational"].append(kw)

        return clusters


# ── Legacy functional API ─────────────────────────────────────────────────────

def get_suggestions(
    query: str,
    country: str = "ca",
    language: str = "en",
) -> list[str]:
    """Get Google autocomplete suggestions for a query (XML toolbar endpoint).

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
    """Alternative: JSON endpoint (Firefox client format)."""
    params = {"client": "firefox", "q": query, "gl": country}
    try:
        resp = requests.get(AUTOCOMPLETE_URL, params=params, headers=HEADERS, timeout=10)
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
    all_keywords: set[str] = {seed}

    def _expand(query: str, current_depth: int) -> None:
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

    "permanent lights" → queries "permanent lights a", "permanent lights b", etc.
    Returns all unique suggestions across all letter expansions.
    """
    all_suggestions: set[str] = set()

    for letter in "abcdefghijklmnopqrstuvwxyz":
        query = f"{seed} {letter}"
        time.sleep(delay)
        suggestions = get_suggestions(query, country=country)
        all_suggestions.update(suggestions)

    result = sorted(all_suggestions)
    log.info("autocomplete.alphabet  seed=%s  total=%d", seed, len(result))
    return result
