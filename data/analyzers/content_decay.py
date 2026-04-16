"""Content Decay Analyzer — detects pages losing visibility over time.

Based on searchsolved/search-solved-public-seo (MIT license).
Identifies pages where traffic has declined, enabling proactive content
updates before rankings tank completely.

Usage:
    from data.analyzers.content_decay import ContentDecayDetector, analyze_content_decay

    detector = ContentDecayDetector()
    decaying = detector.scan(domain="mysite.com", gsc_data=gsc_data)
    # Returns list of DecayAlert objects sorted by urgency

    # Legacy functional API still available:
    decaying = analyze_content_decay(gsc_data, months=12, min_peak_clicks=10)
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

GSC_CACHE_PATH = Path("data/storage/gsc_cache")

# ── DecayAlert dataclass ──────────────────────────────────────────────────────

@dataclass
class DecayAlert:
    url: str
    current_clicks: float       # avg daily clicks last 30 days
    previous_clicks: float      # avg daily clicks 31-60 days ago
    click_decline_pct: float    # percentage decline (negative = decline)
    current_rank: Optional[float]
    previous_rank: Optional[float]
    rank_drop: Optional[float]  # positions dropped (positive = worse)
    impressions_trend: str      # 'declining', 'stable', 'growing'
    urgency: str                # 'critical' >50%, 'high' >30%, 'medium' >15%, 'low' <15%
    recommended_action: str     # 'rewrite', 'update_stats', 'add_sections', 'build_links'
    last_updated: Optional[str]


# ── ContentDecayDetector class ────────────────────────────────────────────────

class ContentDecayDetector:
    """Identifies pages losing traffic/rankings over time from GSC data."""

    DECLINE_THRESHOLD = 0.15   # 15% decline triggers alert

    # urgency → sort order (lower = more urgent)
    _URGENCY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}

    def scan(self, domain: str, gsc_data: list[dict] | None = None) -> list[DecayAlert]:
        """Scan all pages for content decay.

        gsc_data: list of dicts with keys:
            page, clicks_30d, clicks_60d, impressions_30d, impressions_60d,
            position_30d, position_60d
        If gsc_data is None, loads from data/storage/gsc_cache/{domain}.json.

        Returns list of DecayAlert sorted by urgency then click_decline_pct.
        """
        if gsc_data is None:
            gsc_data = self.load_gsc_cache(domain)

        alerts: list[DecayAlert] = []
        for row in gsc_data:
            page = row.get("page", "")
            clicks_30d = float(row.get("clicks_30d", 0))
            clicks_60d = float(row.get("clicks_60d", 0))

            # Skip pages with no meaningful traffic
            if clicks_30d == 0 and clicks_60d == 0:
                continue

            pos_30d = row.get("position_30d")
            pos_60d = row.get("position_60d")
            imp_30d = row.get("impressions_30d")
            imp_60d = row.get("impressions_60d")

            alert = self.analyze_page(
                url=page,
                clicks_30d=clicks_30d,
                clicks_60d=clicks_60d,
                pos_30d=float(pos_30d) if pos_30d is not None else None,
                pos_60d=float(pos_60d) if pos_60d is not None else None,
                impressions_30d=float(imp_30d) if imp_30d is not None else None,
                impressions_60d=float(imp_60d) if imp_60d is not None else None,
            )

            # Only include pages with at least a 15% decline
            if alert.click_decline_pct <= -(self.DECLINE_THRESHOLD * 100):
                alert.recommended_action = self.recommend_action(alert)
                alerts.append(alert)

        # Sort: most urgent first, then steepest decline
        alerts.sort(key=lambda a: (
            self._URGENCY_ORDER.get(a.urgency, 9),
            a.click_decline_pct,   # most negative (worst) first
        ))

        log.info("content_decay.scan  domain=%s  pages_checked=%d  alerts=%d",
                 domain, len(gsc_data), len(alerts))
        return alerts

    def get_refresh_priority_queue(self, domain: str, limit: int = 10) -> list[dict]:
        """Return top N pages that need refreshing most urgently.

        Returns list of {url, urgency, action, estimated_traffic_recovery}.
        """
        alerts = self.scan(domain)
        queue = []
        for alert in alerts[:limit]:
            # Estimate recovery: critical → 60%, high → 45%, medium → 30%, low → 15%
            recovery_pct = {"critical": 0.60, "high": 0.45, "medium": 0.30, "low": 0.15}.get(
                alert.urgency, 0.20
            )
            lost_daily = alert.previous_clicks - alert.current_clicks
            estimated_recovery = round(lost_daily * recovery_pct * 30, 1)  # 30-day estimate

            queue.append({
                "url": alert.url,
                "urgency": alert.urgency,
                "action": alert.recommended_action,
                "click_decline_pct": round(alert.click_decline_pct, 1),
                "estimated_traffic_recovery": estimated_recovery,
            })
        return queue

    def analyze_page(
        self,
        url: str,
        clicks_30d: float,
        clicks_60d: float,
        pos_30d: float | None = None,
        pos_60d: float | None = None,
        impressions_30d: float | None = None,
        impressions_60d: float | None = None,
    ) -> DecayAlert:
        """Analyze a single page for decay signals."""
        # Click decline (negative = decline)
        if clicks_60d > 0:
            click_decline_pct = ((clicks_30d - clicks_60d) / clicks_60d) * 100
        elif clicks_30d == 0:
            click_decline_pct = 0.0
        else:
            click_decline_pct = 100.0  # Growing from zero

        # Rank drop (positive = worse)
        rank_drop: Optional[float] = None
        if pos_30d is not None and pos_60d is not None:
            rank_drop = round(pos_30d - pos_60d, 1)

        # Impressions trend
        impressions_trend = "stable"
        if impressions_30d is not None and impressions_60d is not None and impressions_60d > 0:
            imp_change = (impressions_30d - impressions_60d) / impressions_60d
            if imp_change < -0.10:
                impressions_trend = "declining"
            elif imp_change > 0.10:
                impressions_trend = "growing"

        urgency = self.calculate_urgency(click_decline_pct)

        alert = DecayAlert(
            url=url,
            current_clicks=round(clicks_30d / 30, 3),
            previous_clicks=round(clicks_60d / 30, 3),
            click_decline_pct=round(click_decline_pct, 2),
            current_rank=pos_30d,
            previous_rank=pos_60d,
            rank_drop=rank_drop,
            impressions_trend=impressions_trend,
            urgency=urgency,
            recommended_action="update_stats",  # placeholder — caller sets this
            last_updated=None,
        )
        return alert

    def calculate_urgency(self, decline_pct: float) -> str:
        """Map decline percentage to urgency level.

        decline_pct is negative for declines (e.g., -55 = 55% decline).
        """
        pct = abs(decline_pct) if decline_pct < 0 else 0
        if pct >= 50:
            return "critical"
        elif pct >= 30:
            return "high"
        elif pct >= 15:
            return "medium"
        else:
            return "low"

    def recommend_action(self, alert: DecayAlert) -> str:
        """Recommend specific action based on decay pattern.

        Logic:
        - rank_drop > 5 + click_decline > 30%: 'rewrite' (content quality issue)
        - rank stable + click_decline > 20%: 'update_title_meta' (CTR issue)
        - impressions declining + rank stable: 'add_sections' (topical coverage shrinking)
        - rank dropping + impressions stable: 'build_links' (authority issue)
        - otherwise: 'update_stats' (freshness issue)
        """
        click_decline = abs(alert.click_decline_pct) if alert.click_decline_pct < 0 else 0
        rank_drop = alert.rank_drop if alert.rank_drop is not None else 0

        # Significant rank + click drop → content is no longer competitive
        if rank_drop > 5 and click_decline > 30:
            return "rewrite"

        # Rank stable but clicks still falling → CTR / title problem
        if abs(rank_drop) <= 2 and click_decline > 20:
            return "update_title_meta"

        # Impressions contracting with stable rank → content scope shrinking
        if alert.impressions_trend == "declining" and abs(rank_drop) <= 3:
            return "add_sections"

        # Rank sliding but impressions still OK → link-building can stabilize
        if rank_drop > 3 and alert.impressions_trend in ("stable", "growing"):
            return "build_links"

        # Default: a freshness/stats update is likely enough
        return "update_stats"

    def load_gsc_cache(self, domain: str) -> list[dict]:
        """Load cached GSC data from data/storage/gsc_cache/{domain}.json."""
        cache_file = GSC_CACHE_PATH / f"{domain}.json"
        if not cache_file.exists():
            log.warning("content_decay.no_cache  domain=%s  path=%s", domain, cache_file)
            return []
        try:
            with cache_file.open() as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
            # Handle wrapped format {"rows": [...]}
            if isinstance(data, dict):
                return data.get("rows", data.get("pages", []))
        except Exception as e:
            log.error("content_decay.cache_read_fail  domain=%s  err=%s", domain, e)
        return []

    def generate_decay_report(self, domain: str) -> dict:
        """Full decay report with summary stats and action plan.

        Returns:
            {
              total_pages_analyzed, critical, high, medium, low,
              estimated_traffic_at_risk, top_actions, alerts
            }
        """
        raw_cache = self.load_gsc_cache(domain)
        alerts = self.scan(domain, gsc_data=raw_cache)

        counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        for a in alerts:
            counts[a.urgency] = counts.get(a.urgency, 0) + 1

        # Traffic at risk = sum of daily clicks being lost
        traffic_at_risk = sum(
            max(0, a.previous_clicks - a.current_clicks) * 30
            for a in alerts
        )

        # Top 5 actions by urgency
        action_counts: dict[str, int] = defaultdict(int)
        for a in alerts:
            action_counts[a.recommended_action] += 1
        top_actions = sorted(action_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        top_actions = [{"action": k, "page_count": v} for k, v in top_actions]

        # Serialize alerts to plain dicts for JSON compatibility
        alerts_dicts = []
        for a in alerts:
            alerts_dicts.append({
                "url": a.url,
                "current_clicks_daily": a.current_clicks,
                "previous_clicks_daily": a.previous_clicks,
                "click_decline_pct": a.click_decline_pct,
                "current_rank": a.current_rank,
                "previous_rank": a.previous_rank,
                "rank_drop": a.rank_drop,
                "impressions_trend": a.impressions_trend,
                "urgency": a.urgency,
                "recommended_action": a.recommended_action,
            })

        return {
            "domain": domain,
            "total_pages_analyzed": len(raw_cache),
            "total_decaying": len(alerts),
            "critical": counts["critical"],
            "high": counts["high"],
            "medium": counts["medium"],
            "low": counts["low"],
            "estimated_traffic_at_risk": round(traffic_at_risk, 1),
            "top_actions": top_actions,
            "alerts": alerts_dicts,
        }


# ── Legacy functional API ─────────────────────────────────────────────────────

def analyze_content_decay(
    gsc_data: list[dict],
    months: int = 12,
    min_peak_clicks: int = 10,
) -> list[dict]:
    """Identify pages with declining traffic (content decay).

    Args:
        gsc_data: List of dicts with keys: date (str YYYY-MM-DD), page (str URL), clicks (int)
        months: Number of recent months to analyze
        min_peak_clicks: Minimum peak monthly clicks to consider (filters noise)

    Returns:
        List of decaying pages sorted by worst decay first.
        Each dict: url, peak_clicks, peak_month, latest_clicks, latest_month, clicks_lost, decay_pct
    """
    if not gsc_data:
        return []

    # Aggregate clicks per page per month
    page_months: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for row in gsc_data:
        date_str = row.get("date", "")
        page = row.get("page", "")
        clicks = int(row.get("clicks", 0))

        if not date_str or not page:
            continue

        month_key = date_str[:7]
        page_months[page][month_key] += clicks

    all_months = sorted(set(m for pm in page_months.values() for m in pm.keys()))

    if len(all_months) < 2:
        return []

    recent_months = all_months[-months:]
    latest_month = recent_months[-1]

    results = []
    for page, monthly_clicks in page_months.items():
        relevant = {m: monthly_clicks.get(m, 0) for m in recent_months}

        if not relevant:
            continue

        peak_clicks = max(relevant.values())
        peak_month = max(relevant, key=relevant.get)
        latest_clicks = relevant.get(latest_month, 0)
        clicks_lost = latest_clicks - peak_clicks

        if clicks_lost < 0 and peak_clicks >= min_peak_clicks:
            decay_pct = round(abs(clicks_lost) / peak_clicks * 100, 1) if peak_clicks > 0 else 0
            results.append({
                "url": page,
                "peak_clicks": peak_clicks,
                "peak_month": peak_month,
                "latest_clicks": latest_clicks,
                "latest_month": latest_month,
                "clicks_lost": abs(clicks_lost),
                "decay_pct": decay_pct,
                "monthly_data": relevant,
                "severity": "critical" if decay_pct > 50 else "warning" if decay_pct > 25 else "watch",
            })

    results.sort(key=lambda x: x["clicks_lost"], reverse=True)
    log.info("content_decay.analyzed  pages=%d  decaying=%d", len(page_months), len(results))
    return results


def generate_recovery_plan(decaying_pages: list[dict]) -> list[dict]:
    """Generate recovery actions for decaying pages."""
    actions = []
    for page in decaying_pages:
        severity = page["severity"]
        url = page["url"]

        if severity == "critical":
            actions.append({
                "url": url,
                "priority": "urgent",
                "action": "Full content rewrite + freshness update",
                "details": (
                    f"Lost {page['clicks_lost']} clicks ({page['decay_pct']}% decline "
                    f"from {page['peak_month']}). Rewrite with updated statistics, new "
                    f"sections, and re-optimize for current search intent."
                ),
                "estimated_recovery": f"+{int(page['clicks_lost'] * 0.6)} clicks in 30 days",
            })
        elif severity == "warning":
            actions.append({
                "url": url,
                "priority": "high",
                "action": "Content refresh + internal link boost",
                "details": (
                    f"Lost {page['clicks_lost']} clicks ({page['decay_pct']}% decline). "
                    f"Add 200-400 words, update statistics, add FAQ section, build 2-3 "
                    f"internal links."
                ),
                "estimated_recovery": f"+{int(page['clicks_lost'] * 0.4)} clicks in 21 days",
            })
        else:
            actions.append({
                "url": url,
                "priority": "medium",
                "action": "Monitor + minor update",
                "details": (
                    f"Lost {page['clicks_lost']} clicks ({page['decay_pct']}% decline). "
                    f"Update title for CTR, add recent data points, check competitor changes."
                ),
                "estimated_recovery": f"+{int(page['clicks_lost'] * 0.3)} clicks in 14 days",
            })

    return actions
