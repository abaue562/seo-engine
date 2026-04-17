"""Self-hosted rank checker — replaces DataForSEO rank tracking.

Uses core/serp_scraper.py (Bing via Firecrawl at :3002) to find where a
domain ranks for a given keyword. Zero cost, no API key required.

Drop-in compatible with DataForSEO-based rank_tracker.check_rankings():
    checker = RankChecker()
    results = checker.check_rankings("mysite.com", ["plumber NYC", ...])
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)

_CACHE_TTL = 86400       # 24h
_RATE_DELAY = 2.5        # seconds between SERP requests


def _normalise_domain(raw: str) -> str:
    d = re.sub(r'^https?://(www\.)?', '', raw.lower())
    return d.split('/')[0].rstrip('.')


class RankChecker:
    """Self-hosted rank checker using Bing SERP via Firecrawl. No paid API."""

    STORAGE_PATH = Path("data/storage/rank_history")

    def __init__(self):
        self.STORAGE_PATH.mkdir(parents=True, exist_ok=True)

    def check_position(self, domain: str, keyword: str, location: str = "",
                       num_results: int = 50) -> dict:
        """Return rank of domain for keyword from Bing SERP.

        Returns:
            {keyword, rank: int|None, url, title, featured_snippet, paa_count, checked_at}
        """
        ck = "rank_check:" + hashlib.sha256((domain + ":" + keyword + ":" + location).encode()).hexdigest()[:16]
        cached = _redis.get(ck)
        if cached:
            log.debug("rank_checker.cache_hit  domain=%s  keyword=%s", domain, keyword)
            return json.loads(cached)

        from core.serp_scraper import scrape_serp
        serp = scrape_serp(keyword, location=location, num_results=num_results)

        target = _normalise_domain(domain)
        result = {
            "keyword": keyword,
            "rank": None,
            "url": None,
            "title": None,
            "featured_snippet": False,
            "paa_count": len(serp.get("paa", [])),
            "checked_at": datetime.utcnow().isoformat(),
        }

        for item in serp.get("organic", []):
            item_domain = _normalise_domain(item.get("url", ""))
            if item_domain and (item_domain == target or item_domain.endswith("." + target)):
                result["rank"] = item.get("position")
                result["url"] = item.get("url")
                result["title"] = item.get("title", "")
                if item.get("position") == 1 and len(item.get("snippet", "")) > 60:
                    result["featured_snippet"] = True
                break

        _redis.setex(ck, _CACHE_TTL, json.dumps(result))
        log.info("rank_checker.check  domain=%s  keyword=%s  rank=%s",
                 domain, keyword, result["rank"])
        return result

    def check_rankings(self, domain: str, keywords: list, location: str = "") -> list:
        """Batch check — same interface as DataForSEO-based rank_tracker."""
        results = []
        for i, keyword in enumerate(keywords):
            if i > 0:
                time.sleep(_RATE_DELAY)
            results.append(self.check_position(domain, keyword, location=location))
        self._save(domain, results)
        return results

    def get_rank_delta(self, domain: str, keyword: str, days: int = 7) -> dict:
        """Compare today's rank vs N days ago from stored files."""
        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        past_str = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        domain_dir = self.STORAGE_PATH / re.sub(r'[^\w.-]', '_', domain)

        def _load(date_str):
            f = domain_dir / (date_str + ".json")
            if not f.exists():
                return None
            try:
                for r in json.loads(f.read_text()):
                    if r.get("keyword") == keyword:
                        return r
            except Exception:
                pass
            return None

        current = _load(today_str)
        previous = _load(past_str)
        curr_rank = current["rank"] if current else None
        prev_rank = previous["rank"] if previous else None

        if curr_rank is None and prev_rank is None:
            trend = "not_ranking"
        elif curr_rank is None:
            trend = "lost"
        elif prev_rank is None:
            trend = "new"
        elif curr_rank < prev_rank:
            trend = "up"
        elif curr_rank > prev_rank:
            trend = "down"
        else:
            trend = "stable"

        return {
            "keyword": keyword,
            "current_rank": curr_rank,
            "previous_rank": prev_rank,
            "delta": (prev_rank - curr_rank) if (curr_rank and prev_rank) else None,
            "trend": trend,
        }

    def _save(self, domain: str, results: list) -> None:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        domain_dir = self.STORAGE_PATH / re.sub(r'[^\w.-]', '_', domain)
        domain_dir.mkdir(parents=True, exist_ok=True)
        out = domain_dir / (today + ".json")
        existing = {}
        if out.exists():
            try:
                existing = {r["keyword"]: r for r in json.loads(out.read_text())}
            except Exception:
                pass
        for r in results:
            existing[r["keyword"]] = r
        out.write_text(json.dumps(list(existing.values()), indent=2))
        log.info("rank_checker.saved  domain=%s  count=%d", domain, len(results))
