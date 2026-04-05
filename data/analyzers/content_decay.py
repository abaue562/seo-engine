"""Content Decay Analyzer — detects pages losing visibility over time.

Based on searchsolved/search-solved-public-seo (MIT license).
Identifies pages where traffic has declined from their peak,
enabling proactive content updates before rankings tank completely.

Usage:
    from data.analyzers.content_decay import analyze_content_decay

    # From GSC data (list of dicts with: date, page, clicks)
    decaying = analyze_content_decay(gsc_data, months=12, min_peak_clicks=10)
    for page in decaying:
        print(f"{page['url']}: lost {page['clicks_lost']} clicks from peak")
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime

log = logging.getLogger(__name__)


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

        # Extract YYYY-MM
        month_key = date_str[:7]
        page_months[page][month_key] += clicks

    # Get all months, sorted
    all_months = sorted(set(m for pm in page_months.values() for m in pm.keys()))

    if len(all_months) < 2:
        return []

    # Take N most recent months
    recent_months = all_months[-months:]
    latest_month = recent_months[-1]

    # Find decaying pages
    results = []
    for page, monthly_clicks in page_months.items():
        # Only consider months in our window
        relevant = {m: monthly_clicks.get(m, 0) for m in recent_months}

        if not relevant:
            continue

        peak_clicks = max(relevant.values())
        peak_month = max(relevant, key=relevant.get)
        latest_clicks = relevant.get(latest_month, 0)

        clicks_lost = latest_clicks - peak_clicks

        # Only include if actually decaying and had meaningful traffic
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

    # Sort by worst decay first
    results.sort(key=lambda x: x["clicks_lost"], reverse=True)
    log.info("content_decay.analyzed  pages=%d  decaying=%d", len(page_months), len(results))
    return results


def generate_recovery_plan(decaying_pages: list[dict]) -> list[dict]:
    """Generate recovery actions for decaying pages.

    Returns prioritized list of actions to reverse content decay.
    """
    actions = []

    for page in decaying_pages:
        severity = page["severity"]
        url = page["url"]

        if severity == "critical":
            actions.append({
                "url": url,
                "priority": "urgent",
                "action": "Full content rewrite + freshness update",
                "details": f"Lost {page['clicks_lost']} clicks ({page['decay_pct']}% decline from {page['peak_month']}). "
                           f"Rewrite with updated statistics, new sections, and re-optimize for current search intent.",
                "estimated_recovery": f"+{int(page['clicks_lost'] * 0.6)} clicks in 30 days",
            })
        elif severity == "warning":
            actions.append({
                "url": url,
                "priority": "high",
                "action": "Content refresh + internal link boost",
                "details": f"Lost {page['clicks_lost']} clicks ({page['decay_pct']}% decline). "
                           f"Add 200-400 words, update statistics, add FAQ section, build 2-3 internal links.",
                "estimated_recovery": f"+{int(page['clicks_lost'] * 0.4)} clicks in 21 days",
            })
        else:
            actions.append({
                "url": url,
                "priority": "medium",
                "action": "Monitor + minor update",
                "details": f"Lost {page['clicks_lost']} clicks ({page['decay_pct']}% decline). "
                           f"Update title for CTR, add recent data points, check competitor changes.",
                "estimated_recovery": f"+{int(page['clicks_lost'] * 0.3)} clicks in 14 days",
            })

    return actions
