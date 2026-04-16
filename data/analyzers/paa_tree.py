"""People Also Ask Tree Builder — recursively expands PAA questions into unlimited trees.

Based on lagranges/people_also_ask (MIT license).
Takes a seed query, finds PAA questions from Google SERP, then recursively
expands each question to build a complete question tree.

Usage:
    from data.analyzers.paa_tree import PAATree, get_paa_questions, build_paa_tree

    # Class API:
    paa = PAATree()
    questions = paa.get_questions("plumber NYC")
    tree = paa.build_tree("plumber NYC", depth=2)

    # Legacy functional API:
    questions = get_paa_questions("permanent lights kelowna")
    tree = build_paa_tree("permanent lights kelowna", depth=2)
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from pathlib import Path
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

GOOGLE_URL = "https://www.google.com/search"
CACHE_TTL_SECONDS = 24 * 3600  # 24 hours

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# ── PAATree class API ─────────────────────────────────────────────────────────

class PAATree:
    """PAA question tree extractor with caching and recursive expansion."""

    CACHE_PATH = Path("data/storage/paa_cache")

    def __init__(self) -> None:
        self.cache_path = self.CACHE_PATH
        self.cache_path.mkdir(parents=True, exist_ok=True)

    # ── Public methods ────────────────────────────────────────────────────────

    def get_questions(self, keyword: str, use_cache: bool = True) -> list[str]:
        """Get PAA questions for a keyword from Google SERP.

        Returns a list of question strings.
        Caches results for 24 h to avoid hammering Google.
        """
        if use_cache:
            cached = self.get_cached(keyword)
            if cached:
                return cached

        # Random 1-3s delay to reduce rate-limiting risk
        time.sleep(random.uniform(1.0, 3.0))

        questions = get_paa_questions(keyword)

        if questions:
            self._save_cache(keyword, questions)

        return questions

    def build_tree(self, keyword: str, depth: int = 2) -> dict:
        """Build PAA tree by recursively expanding questions.

        Returns:
            {
              keyword,
              questions: [
                {question, sub_questions: [...]},
                ...
              ]
            }
        Depth 1 = top-level questions only.
        Depth 2 = each question expanded once.
        Max depth capped at 3 to respect rate limits.
        """
        depth = min(depth, 3)
        seen: set[str] = {keyword.lower()}

        def _expand(query: str, remaining_depth: int) -> list[dict]:
            if remaining_depth <= 0:
                return []
            questions = self.get_questions(query)
            nodes = []
            for q in questions:
                if q.lower() in seen:
                    continue
                seen.add(q.lower())
                sub = _expand(q, remaining_depth - 1)
                nodes.append({"question": q, "sub_questions": sub})
            return nodes

        tree_questions = _expand(keyword, depth)

        return {
            "keyword": keyword,
            "depth": depth,
            "total_questions": len(seen) - 1,  # exclude seed
            "questions": tree_questions,
        }

    def format_for_content(self, tree: dict) -> list[dict]:
        """Format PAA tree as FAQ content blocks.

        Returns list of {question, context_for_answer} suitable for Claude content generation.
        """
        blocks: list[dict] = []
        keyword = tree.get("keyword", "")

        def _collect(nodes: list[dict], parent: str = "") -> None:
            for node in nodes:
                q = node.get("question", "")
                if not q:
                    continue
                context = (
                    f"Answer in 40-80 words. Context: this question relates to '{keyword}'."
                )
                if parent:
                    context += f" It is a follow-up to: '{parent}'."
                blocks.append({"question": q, "context_for_answer": context})
                _collect(node.get("sub_questions", []), parent=q)

        _collect(tree.get("questions", []))
        return blocks

    def get_cached(self, keyword: str) -> list[str]:
        """Return cached questions for keyword, or empty list if not cached / stale."""
        cache_file = self._cache_file(keyword)
        if not cache_file.exists():
            return []
        try:
            with cache_file.open() as f:
                data = json.load(f)
            cached_at = data.get("cached_at", 0)
            if (time.time() - cached_at) > CACHE_TTL_SECONDS:
                return []
            return data.get("questions", [])
        except Exception:
            return []

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _save_cache(self, keyword: str, questions: list[str]) -> None:
        cache_file = self._cache_file(keyword)
        try:
            with cache_file.open("w") as f:
                json.dump({"keyword": keyword, "cached_at": time.time(), "questions": questions}, f)
        except Exception as e:
            log.warning("paa.cache_save_fail  keyword=%s  err=%s", keyword, e)

    def _cache_file(self, keyword: str) -> Path:
        import hashlib
        key = hashlib.md5(keyword.lower().encode()).hexdigest()
        return self.cache_path / f"{key}.json"


# ── Legacy functional API ─────────────────────────────────────────────────────

def get_paa_questions(query: str, num_results: int = 10) -> list[str]:
    """Get People Also Ask questions for a query from Google SERP.

    Args:
        query: Search query
        num_results: Number of results to request (more = more PAA)

    Returns:
        List of PAA question strings (up to 20)
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    params = {"q": query, "num": num_results, "hl": "en", "gl": "us"}

    try:
        resp = requests.get(GOOGLE_URL, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        questions: list[str] = []

        # Method 1: data-q attribute (most reliable when Google serves it)
        for el in soup.find_all(attrs={"data-q": True}):
            q = el.get("data-q", "").strip()
            if q and q not in questions:
                questions.append(q)

        # Method 2: jsname attribute PAA containers
        for el in soup.find_all(attrs={"jsname": True}):
            text = el.get_text(separator=" ", strip=True)
            if text.endswith("?") and 10 < len(text) < 200 and text not in questions:
                questions.append(text)

        # Method 3: aria-expanded elements (PAA accordion buttons)
        for el in soup.find_all(attrs={"aria-expanded": True}):
            text = el.get_text(strip=True)
            if text.endswith("?") and 10 < len(text) < 200 and text not in questions:
                questions.append(text)

        # Method 4: broad sweep — any short question-like text in divs/spans
        if len(questions) < 3:
            for el in soup.find_all(["div", "span"]):
                text = el.get_text(strip=True)
                if (text.endswith("?") and 10 < len(text) < 200
                        and text not in questions
                        and query.lower() not in text.lower()):
                    questions.append(text)

        log.info("paa.found  query=%s  questions=%d", query, len(questions))
        return questions[:20]

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

    Args:
        seed_query: Starting search query
        depth: How many levels deep to recurse (1 = just first level)
        delay: Seconds between requests (respect rate limits)
        max_questions_per_level: Max questions to expand per level

    Returns:
        Tree dict: seed_query, questions (list of {question, children, child_count})
    """
    seen: set[str] = set()

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
        "total_questions": len(seen) - 1,
        "depth": depth,
    }

    log.info("paa_tree.built  seed=%s  total=%d  depth=%d",
             seed_query, tree["total_questions"], depth)
    return tree


def flatten_paa_tree(tree: dict) -> list[str]:
    """Flatten a PAA tree into a unique list of all questions."""
    questions: list[str] = []

    def _collect(nodes: list[dict]) -> None:
        for node in nodes:
            q = node.get("question", "")
            if q and q not in questions:
                questions.append(q)
            _collect(node.get("children", node.get("sub_questions", [])))

    _collect(tree.get("questions", []))
    return questions
