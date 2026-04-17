"""Content refresh scheduling — proactive freshness-based refresh queue.

Freshness score per page considers:
  - days_since_publish / days_since_last_refresh
  - serp_volatility (how much the top 10 has changed)
  - content_type (evergreen / seasonal / news-adjacent)

Schedule:
  - High-volume evergreen: 90 days
  - Seasonal: 30 days before relevant season
  - News-adjacent: 7 days
  - Low-volume tail: 365 days
"""
from __future__ import annotations
import logging, sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

_REFRESH_INTERVALS = {
    "evergreen_high": 90,
    "evergreen_low":  365,
    "seasonal":       30,   # days before season
    "news":           7,
}


def _db():
    conn = sqlite3.connect(Path("data/storage/seo_engine.db"))
    conn.row_factory = sqlite3.Row
    return conn


def _now():
    return datetime.now(tz=timezone.utc)


def classify_content_type(keyword: str) -> str:
    """Simple heuristic content-type classifier from keyword."""
    kw = keyword.lower()
    news_signals = ["2024", "2025", "2026", "news", "update", "latest", "new"]
    seasonal_signals = ["spring", "summer", "fall", "winter", "christmas", "holiday", "season"]
    if any(s in kw for s in news_signals):
        return "news"
    if any(s in kw for s in seasonal_signals):
        return "seasonal"
    return "evergreen_high"   # default to high-value evergreen


def compute_freshness_score(published_at: str, last_refreshed_at: str | None, content_type: str) -> int:
    """Return freshness score 0-100. 100 = freshly published / just refreshed. 0 = needs refresh now."""
    try:
        pub_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    except Exception:
        return 50

    ref_dt = pub_dt
    if last_refreshed_at:
        try:
            ref_dt = datetime.fromisoformat(last_refreshed_at.replace("Z", "+00:00"))
        except Exception:
            pass

    days_stale = (_now() - ref_dt).days
    interval = _REFRESH_INTERVALS.get(content_type, 90)
    score = max(0, 100 - int(100 * days_stale / interval))
    return score


def get_refresh_queue(business_id: str, limit: int = 20) -> list[dict]:
    """Return pages sorted by freshness score ascending (most stale first)."""
    db = _db()
    rows = db.execute(
        "SELECT url, keyword, published_at FROM published_urls WHERE business_id=? AND status='live' ORDER BY published_at ASC",
        [business_id]
    ).fetchall()
    db.close()

    queue = []
    for r in rows:
        content_type = classify_content_type(r["keyword"])
        score = compute_freshness_score(r["published_at"], None, content_type)
        interval = _REFRESH_INTERVALS.get(content_type, 90)
        queue.append({
            "url": r["url"],
            "keyword": r["keyword"],
            "published_at": r["published_at"],
            "content_type": content_type,
            "freshness_score": score,
            "refresh_interval_days": interval,
            "needs_refresh": score < 30,
        })

    queue.sort(key=lambda x: x["freshness_score"])
    log.info("refresh_schedule.queue  biz=%s  stale=%d  total=%d",
             business_id, sum(1 for q in queue if q["needs_refresh"]), len(queue))
    return queue[:limit]


def mark_refreshed(business_id: str, url: str) -> None:
    """Record that a page has been refreshed (update published_at or add a refresh log entry)."""
    db = _db()
    try:
        db.execute(
            "UPDATE published_urls SET published_at=? WHERE business_id=? AND url=?",
            [_now().isoformat(), business_id, url]
        )
        db.commit()
        log.info("refresh_schedule.marked  biz=%s  url=%s", business_id, url)
    except Exception as e:
        log.warning("refresh_schedule.mark_fail  err=%s", e)
    finally:
        db.close()
