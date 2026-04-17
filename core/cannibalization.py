"""Keyword cannibalization detection and resolution workflow.

Two pages targeting the same keyword hurt each other's rankings.
Checks:
  1. Pre-publish: new page targets keyword already assigned to existing page.
  2. Post-rank-check: multiple URLs ranking on same SERP for same keyword.
"""
from __future__ import annotations
import logging, sqlite3
from pathlib import Path

log = logging.getLogger(__name__)


def _db():
    conn = sqlite3.connect(Path("data/storage/seo_engine.db"))
    conn.row_factory = sqlite3.Row
    return conn


def check_pre_publish(business_id: str, keyword: str, new_url: str) -> dict:
    """Check if the keyword is already targeted by another live page.

    Returns: {"conflict": bool, "existing_url": str|None, "recommended_action": str}
    """
    db = _db()
    existing = db.execute(
        "SELECT url FROM published_urls WHERE business_id=? AND keyword=? AND status='live' AND url!=?",
        [business_id, keyword, new_url]
    ).fetchone()
    db.close()

    if existing:
        log.warning("cannibalization.pre_publish  biz=%s  kw=%s  conflict=%s", business_id, keyword, existing["url"])
        return {
            "conflict": True,
            "existing_url": existing["url"],
            "keyword": keyword,
            "recommended_action": "differentiate",  # or "consolidate" or "deprioritize"
            "message": f"Keyword '{keyword}' is already targeted by {existing['url']}. Publishing both may cause cannibalization.",
        }
    return {"conflict": False, "existing_url": None, "keyword": keyword}


def detect_serp_cannibalization(business_id: str) -> list[dict]:
    """Find keywords where multiple pages from the same site rank on the same SERP.

    Uses ranking_history to find multiple URLs at different positions for the same keyword.
    Returns list of cannibalization cases with recommended resolutions.
    """
    db = _db()
    # Find keywords where this business has multiple ranked URLs
    rows = db.execute(
        """
        SELECT keyword, url, position, recorded_at
        FROM ranking_history
        WHERE business_id = ?
        AND recorded_at >= datetime('now', '-7 days')
        ORDER BY keyword, position ASC
        """,
        [business_id]
    ).fetchall()
    db.close()

    # Group by keyword
    kw_urls: dict[str, list[dict]] = {}
    for r in rows:
        kw_urls.setdefault(r["keyword"], []).append({"url": r["url"], "position": r["position"]})

    cases = []
    for kw, urls in kw_urls.items():
        # Deduplicate by URL, keep best position
        seen: dict[str, int] = {}
        for u in urls:
            if u["url"] not in seen or u["position"] < seen[u["url"]]:
                seen[u["url"]] = u["position"]
        if len(seen) > 1:
            ranked = sorted(seen.items(), key=lambda x: x[1])
            winner_url, winner_pos = ranked[0]
            loser_url, loser_pos = ranked[1]
            cases.append({
                "keyword": kw,
                "winner": {"url": winner_url, "position": winner_pos},
                "loser": {"url": loser_url, "position": loser_pos},
                "recommended_action": _recommend_resolution(winner_pos, loser_pos),
            })
            log.info("cannibalization.detected  biz=%s  kw=%s  urls=%d", business_id, kw, len(seen))

    return cases


def _recommend_resolution(winner_pos: int, loser_pos: int) -> str:
    """Suggest consolidate, differentiate, or deprioritize based on positions."""
    if winner_pos <= 5 and loser_pos > 10:
        return "deprioritize"   # winner is strong; noindex the loser
    if winner_pos <= 10:
        return "consolidate"    # merge loser into winner, 301 redirect
    return "differentiate"      # both are weak; retarget loser to a related keyword
