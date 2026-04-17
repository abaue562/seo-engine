"""Rank tracking connector.

Uses the DataForSEO SERP API to track keyword rankings over time.
Results are stored as JSON files under data/storage/rank_history/{domain}/
one file per day, so trends can be computed across any date range.

Usage:
    tracker = RankTracker()
    results = tracker.check_rankings(domain="mysite.com", keywords=["plumber NYC", ...])
    delta   = tracker.get_rank_delta(domain="mysite.com", keyword="plumber NYC", days=7)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


class RankTracker:
    """Track keyword rankings for a domain using DataForSEO SERP data.

    Storage layout:
        data/storage/rank_history/
            {domain}/
                2025-01-15.json   ← list of rank result dicts for that day
                2025-01-22.json
                ...

    Each daily file is a JSON array of:
        {keyword, rank, url, title, checked_at}
    where rank is null when the domain was not found in the top-100 results.
    """

    STORAGE_PATH = Path("data/storage/rank_history")
    REGISTRY_PATH = Path("data/storage/rank_registry.json")
    BUSINESSES_PATH = Path("data/storage/businesses.json")

    def __init__(self, storage_path: Optional[Path] = None):
        self.storage = storage_path or self.STORAGE_PATH
        self.storage.mkdir(parents=True, exist_ok=True)
        self._dfs_client = None   # lazy-initialised

    # ── DataForSEO client (lazy) ───────────────────────────────────────────────

    def _dfs(self):
        if self._dfs_client is None:
            from data.connectors.dataforseo import DataForSEOClient
            self._dfs_client = DataForSEOClient()
        return self._dfs_client

    def _rank_checker(self):
        from data.connectors.rank_checker import RankChecker
        return RankChecker()

    # ── Registration (keyword + URL tracking) ─────────────────────────────────

    async def register(self, keyword: str, url: str) -> None:
        """Register a keyword + URL pair for ongoing rank tracking.

        Called by the content pipeline after a new page is published.
        Stored in data/storage/rank_registry.json as {keyword: url}.
        """
        registry: dict = {}
        if self.REGISTRY_PATH.exists():
            try:
                registry = json.loads(self.REGISTRY_PATH.read_text())
            except Exception:
                registry = {}

        registry[keyword] = url
        self.REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
        self.REGISTRY_PATH.write_text(json.dumps(registry, indent=2))
        log.info("rank_tracker.registered  keyword=%s  url=%s", keyword, url)

    def _load_registry(self) -> dict:
        """Return {keyword: url} registry dict."""
        if not self.REGISTRY_PATH.exists():
            return {}
        try:
            return json.loads(self.REGISTRY_PATH.read_text())
        except Exception:
            return {}

    def _load_business(self, business_id: str) -> dict:
        """Load a business record from businesses.json by id."""
        if not self.BUSINESSES_PATH.exists():
            return {}
        try:
            businesses = json.loads(self.BUSINESSES_PATH.read_text())
            for biz in businesses:
                if (biz.get("id") or biz.get("business_id")) == business_id:
                    return biz
        except Exception:
            pass
        return {}

    def get_summary_by_id(self, business_id: str) -> dict:
        """Convenience wrapper: look up domain + keywords from businesses.json
        and call get_summary_report().

        Args:
            business_id: The id field from data/storage/businesses.json.

        Returns:
            Summary report dict, or {"error": ...} if business not found.
        """
        biz = self._load_business(business_id)
        if not biz:
            log.warning("rank_tracker.business_not_found  id=%s", business_id)
            return {"error": f"business_id {business_id!r} not found in businesses.json"}

        domain = (
            biz.get("website", "")
            .replace("https://", "")
            .replace("http://", "")
            .rstrip("/")
        )
        keywords = biz.get("primary_keywords", [])

        # Also include any keywords registered via register()
        registry_keywords = list(self._load_registry().keys())
        all_keywords = list(dict.fromkeys(keywords + registry_keywords))  # dedupe, preserve order

        if not domain or not all_keywords:
            return {
                "business_id": business_id,
                "domain": domain,
                "keywords": all_keywords,
                "note": "No domain or keywords configured — add to businesses.json",
            }

        return self.get_summary_report(domain, all_keywords)

    # ── Core ranking check ────────────────────────────────────────────────────

    def check_rankings(
        self,
        domain: str,
        keywords: list[str],
        location_code: int = 2840,
    ) -> list[dict]:
        """Check current rankings for all keywords.

        For each keyword calls DataForSEO SERP live/advanced and scans the
        top-100 organic results for the first URL whose host matches *domain*.

        Returns list of:
            {keyword, rank: int|None, url: str|None, title: str|None, checked_at: ISO}

        Results are saved to storage automatically.
        """
        client = self._dfs()
        results: list[dict] = []
        checked_at = datetime.utcnow().isoformat()

        for keyword in keywords:
            serp = client.serp_live(keyword, location_code=location_code)
            rank_result: dict = {
                "keyword":    keyword,
                "rank":       None,
                "url":        None,
                "title":      None,
                "checked_at": checked_at,
            }

            for item in serp.get("items", []):
                if item.get("type") != "organic":
                    continue
                item_url = item.get("url", "")
                # Normalise domain for comparison (strip www.)
                item_host = _extract_host(item_url)
                target_host = domain.lower().lstrip("www.").lstrip("https://").lstrip("http://")
                target_host = target_host.rstrip("/")
                if item_host and (item_host == target_host or item_host.endswith("." + target_host)):
                    rank_result["rank"]  = item.get("rank_absolute")
                    rank_result["url"]   = item_url
                    rank_result["title"] = item.get("title", "")
                    break

            results.append(rank_result)
            log.info(
                "rank_tracker.check  domain=%s  keyword=%s  rank=%s",
                domain, keyword, rank_result["rank"],
            )

        self._save_rank_check(domain, results)
        return results

    # ── Delta / trend helpers ─────────────────────────────────────────────────

    def get_rank_delta(self, domain: str, keyword: str, days: int = 7) -> dict:
        """Compare current rank vs rank N days ago.

        Returns:
            {keyword, current_rank, previous_rank, delta: int,
             trend: 'up'|'down'|'stable'|'new'|'lost'}

        delta is positive when rank improved (moved up), negative when it dropped.
        """
        history = self._load_rank_history(domain)
        cutoff = datetime.utcnow() - timedelta(days=days)

        # Separate entries into "current window" and "previous window"
        keyword_entries = [
            e for e in history if e.get("keyword") == keyword
        ]
        keyword_entries.sort(key=lambda e: e.get("checked_at", ""))

        recent   = [e for e in keyword_entries if _parse_ts(e.get("checked_at")) >= cutoff]
        previous = [e for e in keyword_entries if _parse_ts(e.get("checked_at")) < cutoff]

        current_rank  = recent[-1]["rank"]  if recent   else None
        previous_rank = previous[-1]["rank"] if previous else None

        trend = _compute_trend(current_rank, previous_rank)
        delta = _rank_delta(current_rank, previous_rank)

        return {
            "keyword":       keyword,
            "current_rank":  current_rank,
            "previous_rank": previous_rank,
            "delta":         delta,
            "trend":         trend,
        }

    def get_declining_keywords(
        self,
        domain: str,
        threshold: int = 3,
        days: int = 14,
    ) -> list[dict]:
        """Keywords that dropped more than *threshold* positions in the last *days* days.

        Returns list of rank-delta dicts sorted by biggest drop first (worst first).
        """
        all_keywords = self._all_tracked_keywords(domain)
        deltas = [self.get_rank_delta(domain, kw, days=days) for kw in all_keywords]
        declining = [d for d in deltas if d["delta"] is not None and d["delta"] < -threshold]
        return sorted(declining, key=lambda d: d["delta"])   # most negative first

    def get_improving_keywords(
        self,
        domain: str,
        threshold: int = 2,
        days: int = 14,
    ) -> list[dict]:
        """Keywords that improved more than *threshold* positions in the last *days* days.

        Returns list of rank-delta dicts sorted by biggest gain first.
        """
        all_keywords = self._all_tracked_keywords(domain)
        deltas = [self.get_rank_delta(domain, kw, days=days) for kw in all_keywords]
        improving = [d for d in deltas if d["delta"] is not None and d["delta"] > threshold]
        return sorted(improving, key=lambda d: d["delta"], reverse=True)

    def get_rank_history(
        self,
        domain: str,
        keyword: str,
        days: int = 90,
    ) -> list[dict]:
        """Full rank history for a single keyword over the last *days* days.

        Returns list of {date, rank, url} sorted oldest first.
        """
        cutoff  = datetime.utcnow() - timedelta(days=days)
        history = self._load_rank_history(domain)

        entries = [
            e for e in history
            if e.get("keyword") == keyword and _parse_ts(e.get("checked_at")) >= cutoff
        ]
        entries.sort(key=lambda e: e.get("checked_at", ""))

        return [
            {
                "date": e.get("checked_at", "")[:10],
                "rank": e.get("rank"),
                "url":  e.get("url"),
            }
            for e in entries
        ]

    # ── Summary report ────────────────────────────────────────────────────────

    def get_summary_report(self, domain: str, keywords: list[str]) -> dict:
        """Full ranking summary: current positions, weekly delta, top movers, opportunities.

        Checks current rankings live (costs DataForSEO credits), then compares
        against stored history for the delta.

        Returns:
            {
                domain,
                generated_at,
                total_keywords,
                ranked_count,      # how many are in top 100
                avg_rank,
                top_10_count,
                top_3_count,
                results: [...],    # full per-keyword list with delta
                top_improvers: [...],
                top_decliners: [...],
                opportunities: [...],  # ranked 11-30 — close to page-1 gold
            }
        """
        current = self.check_rankings(domain, keywords)

        per_keyword: list[dict] = []
        for r in current:
            kw    = r["keyword"]
            delta = self.get_rank_delta(domain, kw, days=7)
            per_keyword.append({
                "keyword":       kw,
                "rank":          r["rank"],
                "url":           r["url"],
                "title":         r["title"],
                "checked_at":    r["checked_at"],
                "previous_rank": delta["previous_rank"],
                "delta":         delta["delta"],
                "trend":         delta["trend"],
            })

        ranked   = [r for r in per_keyword if r["rank"] is not None]
        avg_rank = round(sum(r["rank"] for r in ranked) / len(ranked), 1) if ranked else None

        top_improvers = sorted(
            [r for r in per_keyword if r["delta"] and r["delta"] > 0],
            key=lambda r: r["delta"],
            reverse=True,
        )[:5]

        top_decliners = sorted(
            [r for r in per_keyword if r["delta"] and r["delta"] < 0],
            key=lambda r: r["delta"],
        )[:5]

        opportunities = [
            r for r in ranked if r["rank"] and 11 <= r["rank"] <= 30
        ]

        return {
            "domain":         domain,
            "generated_at":   datetime.utcnow().isoformat(),
            "total_keywords": len(keywords),
            "ranked_count":   len(ranked),
            "avg_rank":       avg_rank,
            "top_10_count":   sum(1 for r in ranked if r["rank"] and r["rank"] <= 10),
            "top_3_count":    sum(1 for r in ranked if r["rank"] and r["rank"] <= 3),
            "results":        per_keyword,
            "top_improvers":  top_improvers,
            "top_decliners":  top_decliners,
            "opportunities":  opportunities,
        }

    # ── Storage helpers ───────────────────────────────────────────────────────

    def _save_rank_check(self, domain: str, results: list[dict]) -> None:
        """Save rank check results to data/storage/rank_history/{domain}/{YYYY-MM-DD}.json.

        If a file for today already exists, the new results are merged in by
        keyword so re-running on the same day does not duplicate entries.
        """
        domain_dir = self.storage / _sanitise_domain(domain)
        domain_dir.mkdir(parents=True, exist_ok=True)

        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        file_path = domain_dir / f"{date_str}.json"

        # Load existing entries for today (if any) and merge
        existing: list[dict] = []
        if file_path.exists():
            try:
                existing = json.loads(file_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                existing = []

        # Index by keyword for merge
        by_keyword: dict[str, dict] = {e["keyword"]: e for e in existing}
        for r in results:
            by_keyword[r["keyword"]] = r

        file_path.write_text(
            json.dumps(list(by_keyword.values()), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        log.info("rank_tracker.saved  domain=%s  date=%s  keywords=%d",
                 domain, date_str, len(by_keyword))

    def _load_rank_history(self, domain: str) -> list[dict]:
        """Load all stored rank check entries for a domain (all dates)."""
        domain_dir = self.storage / _sanitise_domain(domain)
        if not domain_dir.exists():
            return []

        all_entries: list[dict] = []
        for json_file in sorted(domain_dir.glob("*.json")):
            try:
                entries = json.loads(json_file.read_text(encoding="utf-8"))
                if isinstance(entries, list):
                    all_entries.extend(entries)
            except (json.JSONDecodeError, OSError) as exc:
                log.warning("rank_tracker.load_error  file=%s  err=%s", json_file, exc)

        return all_entries

    def _all_tracked_keywords(self, domain: str) -> list[str]:
        """Return all unique keywords ever tracked for this domain."""
        history = self._load_rank_history(domain)
        return list({e["keyword"] for e in history if e.get("keyword")})


# ── Module-level helpers ───────────────────────────────────────────────────────

def _extract_host(url: str) -> str:
    """Extract hostname from a URL, stripping www. prefix."""
    if not url:
        return ""
    # Strip scheme
    host = url.lower()
    for scheme in ("https://", "http://"):
        if host.startswith(scheme):
            host = host[len(scheme):]
            break
    # Strip path
    host = host.split("/")[0].split("?")[0].split("#")[0]
    # Strip port
    host = host.split(":")[0]
    # Strip www.
    if host.startswith("www."):
        host = host[4:]
    return host


def _sanitise_domain(domain: str) -> str:
    """Convert a domain to a safe directory name."""
    return (
        domain.lower()
        .lstrip("https://").lstrip("http://")
        .rstrip("/")
        .replace("/", "_")
        .replace(":", "_")
    )


def _parse_ts(ts: Optional[str]) -> datetime:
    if not ts:
        return datetime.min
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return datetime.min


def _rank_delta(current: Optional[int], previous: Optional[int]) -> Optional[int]:
    """Delta > 0 means improvement (rank number went down), < 0 means drop."""
    if current is None or previous is None:
        return None
    return previous - current   # e.g. previous=15, current=10 → delta=+5 (improved)


def _compute_trend(current: Optional[int], previous: Optional[int]) -> str:
    if current is None and previous is not None:
        return "lost"
    if current is not None and previous is None:
        return "new"
    if current is None and previous is None:
        return "stable"
    delta = _rank_delta(current, previous)
    if delta is None:
        return "stable"
    if delta > 0:
        return "up"
    if delta < 0:
        return "down"
    return "stable"
