"""People Also Ask Tree Builder — recursively expands PAA questions into unlimited trees.

Based on lagranges/people_also_ask (MIT license).
Takes a seed query, finds PAA questions from Google, then recursively
expands each question to build a complete question tree.

Usage:
    from data.analyzers.paa_tree import get_paa_questions, build_paa_tree

    questions = get_paa_questions("permanent lights kelowna")
    tree = build_paa_tree("permanent lights kelowna", depth=2)
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
    "Accept-Language": "en-US,en;q=0.9",
}


def get_paa_questions(query: str, num_results: int = 10) -> list[str]:
    """Get People Also Ask questions for a query from Google SERP.

    Args:
        query: Search query
        num_results: Number of results to request (more = more PAA)

    Returns:
        List of PAA question strings
    """
    params = {"q": query, "num": num_results, "hl": "en"}

    try:
        resp = requests.get(GOOGLE_URL, params=params, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        questions = []

        # Method 1: Find PAA div elements (data-q attribute)
        for el in soup.find_all(attrs={"data-q": True}):
            q = el.get("data-q", "").strip()
            if q and q not in questions:
                questions.append(q)

        # Method 2: Find elements with "related questions" pattern
        for el in soup.find_all(["div", "span"]):
            text = el.get_text(strip=True)
            if text and text.endswith("?") and 10 < len(text) < 200:
                if text not in questions and query.lower() not in text.lower():
                    questions.append(text)

        # Method 3: aria-label on expandable elements
        for el in soup.find_all(attrs={"aria-label": True}):
            label = el.get("aria-label", "")
            if label.endswith("?") and 10 < len(label) < 200:
                if label not in questions:
                    questions.append(label)

        log.info("paa.found  query=%s  questions=%d", query, len(questions))
        return questions[:20]  # Cap at 20

    except Exception as e:
        log.error("paa.fail  query=%s  err=%s", query, e)
        return []


def build_paa_tree(
    seed_query: str,
    depth: int = 2,
    delay: float = 2.0,
    max_questions_per_level: int = 5,
) -> dict:
    """Build a recursive PAA question tree.

    Starting from a seed query, fetches PAA questions, then recursively
    fetches PAA for each question to build a tree.

    Args:
        seed_query: Starting search query
        depth: How many levels deep to recurse (1 = just first level)
        delay: Seconds between requests (respect rate limits)
        max_questions_per_level: Max questions to expand per level

    Returns:
        Tree dict: query, questions (list of {question, children})
    """
    seen = set()

    def _expand(query: str, current_depth: int) -> list[dict]:
        if current_depth <= 0 or query in seen:
            return []

        seen.add(query)
        time.sleep(delay)

        questions = get_paa_questions(query)
        results = []

        for q in questions[:max_questions_per_level]:
            if q in seen:
                continue
            children = _expand(q, current_depth - 1)
            results.append({
                "question": q,
                "children": children,
                "child_count": len(children),
            })

        return results

    tree = {
        "seed_query": seed_query,
        "questions": _expand(seed_query, depth),
        "total_questions": len(seen) - 1,  # Exclude seed
        "depth": depth,
    }

    log.info("paa_tree.built  seed=%s  total=%d  depth=%d", seed_query, tree["total_questions"], depth)
    return tree


def flatten_paa_tree(tree: dict) -> list[str]:
    """Flatten a PAA tree into a unique list of all questions."""
    questions = []

    def _collect(nodes):
        for node in nodes:
            q = node.get("question", "")
            if q and q not in questions:
                questions.append(q)
            _collect(node.get("children", []))

    _collect(tree.get("questions", []))
    return questions
