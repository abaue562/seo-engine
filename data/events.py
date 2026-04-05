"""Event Detection System — catches ranking drops, competitor spikes, review surges.

Compares current data against stored snapshots.
When a significant change is detected → triggers immediate agent run.
"""

from __future__ import annotations

import logging
from datetime import datetime
from enum import Enum
from pydantic import BaseModel

log = logging.getLogger(__name__)


class EventType(str, Enum):
    RANKING_DROP = "ranking_drop"
    RANKING_CLIMB = "ranking_climb"
    REVIEW_SURGE = "review_surge"
    REVIEW_DROP = "review_drop"
    COMPETITOR_SPIKE = "competitor_spike"
    TRAFFIC_DROP = "traffic_drop"
    TRAFFIC_SPIKE = "traffic_spike"
    NEW_COMPETITOR = "new_competitor"
    OPPORTUNITY_ALERT = "opportunity_alert"


class EventSeverity(str, Enum):
    CRITICAL = "critical"  # Needs action NOW
    WARNING = "warning"    # Should act within 48h
    INFO = "info"          # Worth knowing


class SEOEvent(BaseModel):
    type: EventType
    severity: EventSeverity
    title: str
    detail: str
    data: dict = {}
    detected_at: datetime = datetime.utcnow()
    requires_agent_run: bool = False


def detect_ranking_changes(
    current: dict[str, float],
    previous: dict[str, float],
    drop_threshold: int = 3,
    climb_threshold: int = 3,
) -> list[SEOEvent]:
    """Compare current vs previous keyword positions."""
    events: list[SEOEvent] = []

    for kw, curr_pos in current.items():
        prev_pos = previous.get(kw)
        if prev_pos is None:
            continue

        diff = curr_pos - prev_pos  # positive = dropped, negative = climbed

        if diff >= drop_threshold:
            severity = EventSeverity.CRITICAL if diff >= 5 else EventSeverity.WARNING
            events.append(SEOEvent(
                type=EventType.RANKING_DROP,
                severity=severity,
                title=f"Ranking drop: '{kw}' #{prev_pos:.0f} → #{curr_pos:.0f}",
                detail=f"Dropped {diff:.0f} positions in the last period",
                data={"keyword": kw, "from": prev_pos, "to": curr_pos, "diff": diff},
                requires_agent_run=severity == EventSeverity.CRITICAL,
            ))

        elif diff <= -climb_threshold:
            # Opportunity: keyword climbing — push harder
            if curr_pos <= 15 and prev_pos > 10:
                events.append(SEOEvent(
                    type=EventType.OPPORTUNITY_ALERT,
                    severity=EventSeverity.WARNING,
                    title=f"Opportunity: '{kw}' moved #{prev_pos:.0f} → #{curr_pos:.0f}",
                    detail=f"Keyword approaching page 1 — optimize NOW for maximum impact",
                    data={"keyword": kw, "from": prev_pos, "to": curr_pos},
                    requires_agent_run=True,
                ))
            else:
                events.append(SEOEvent(
                    type=EventType.RANKING_CLIMB,
                    severity=EventSeverity.INFO,
                    title=f"Ranking climb: '{kw}' #{prev_pos:.0f} → #{curr_pos:.0f}",
                    detail=f"Gained {abs(diff):.0f} positions",
                    data={"keyword": kw, "from": prev_pos, "to": curr_pos},
                ))

    return events


def detect_review_changes(
    current_count: int,
    previous_count: int,
    current_rating: float,
    previous_rating: float,
) -> list[SEOEvent]:
    """Detect review surges or drops."""
    events: list[SEOEvent] = []

    new_reviews = current_count - previous_count
    if new_reviews >= 5:
        events.append(SEOEvent(
            type=EventType.REVIEW_SURGE,
            severity=EventSeverity.INFO,
            title=f"Review surge: +{new_reviews} new reviews",
            detail=f"From {previous_count} to {current_count} reviews",
            data={"from": previous_count, "to": current_count, "new": new_reviews},
        ))

    rating_diff = current_rating - previous_rating
    if rating_diff <= -0.2:
        events.append(SEOEvent(
            type=EventType.REVIEW_DROP,
            severity=EventSeverity.WARNING,
            title=f"Rating drop: {previous_rating} → {current_rating}",
            detail="Rating decline may affect local rankings",
            data={"from": previous_rating, "to": current_rating},
            requires_agent_run=True,
        ))

    return events


def detect_traffic_changes(
    current_clicks: int,
    previous_clicks: int,
    threshold_pct: float = 0.20,
) -> list[SEOEvent]:
    """Detect significant traffic changes."""
    events: list[SEOEvent] = []
    if previous_clicks == 0:
        return events

    change_pct = (current_clicks - previous_clicks) / previous_clicks

    if change_pct <= -threshold_pct:
        events.append(SEOEvent(
            type=EventType.TRAFFIC_DROP,
            severity=EventSeverity.CRITICAL,
            title=f"Traffic drop: {change_pct:.0%} decline",
            detail=f"From {previous_clicks} to {current_clicks} clicks",
            data={"from": previous_clicks, "to": current_clicks, "change_pct": change_pct},
            requires_agent_run=True,
        ))
    elif change_pct >= threshold_pct:
        events.append(SEOEvent(
            type=EventType.TRAFFIC_SPIKE,
            severity=EventSeverity.INFO,
            title=f"Traffic spike: +{change_pct:.0%} growth",
            detail=f"From {previous_clicks} to {current_clicks} clicks",
            data={"from": previous_clicks, "to": current_clicks, "change_pct": change_pct},
        ))

    return events


def events_to_prompt_block(events: list[SEOEvent]) -> str:
    """Render events as agent context — agents should factor these in."""
    if not events:
        return "EVENTS: No significant changes detected."

    lines = ["RECENT EVENTS (factor these into your analysis):"]
    for e in events:
        flag = " [ACTION REQUIRED]" if e.requires_agent_run else ""
        lines.append(f"  [{e.severity.value.upper()}] {e.title}{flag}")
        lines.append(f"    {e.detail}")

    return "\n".join(lines)
