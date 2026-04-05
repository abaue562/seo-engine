"""Result Attribution Engine — connects actions to outcomes.

Every executed task gets tracked. When new data arrives (GSC, GBP, etc),
we attribute changes back to the tasks that caused them.

This is the foundation of the self-learning loop.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)


class TaskOutcome(BaseModel):
    """Measured result of a single executed task."""
    task_id: str
    action: str
    task_type: str
    target: str
    executed_at: datetime

    # Before/after metrics
    before_position: float = 0.0
    after_position: float = 0.0
    before_clicks: int = 0
    after_clicks: int = 0
    before_ctr: float = 0.0
    after_ctr: float = 0.0
    before_reviews: int = 0
    after_reviews: int = 0

    # Computed
    ranking_change: float = 0.0
    traffic_change: int = 0
    traffic_change_pct: float = 0.0
    ctr_change: float = 0.0
    time_to_effect_days: int = 0
    performance_score: float = 0.0
    success: bool = False

    measured_at: datetime = Field(default_factory=datetime.utcnow)


def attribute_result(
    task_id: str,
    action: str,
    task_type: str,
    target: str,
    executed_at: datetime,
    before: dict,
    after: dict,
) -> TaskOutcome:
    """Compare before/after data and compute attribution metrics."""
    outcome = TaskOutcome(
        task_id=task_id,
        action=action,
        task_type=task_type,
        target=target,
        executed_at=executed_at,
        before_position=before.get("position", 0),
        after_position=after.get("position", 0),
        before_clicks=before.get("clicks", 0),
        after_clicks=after.get("clicks", 0),
        before_ctr=before.get("ctr", 0),
        after_ctr=after.get("ctr", 0),
        before_reviews=before.get("reviews", 0),
        after_reviews=after.get("reviews", 0),
    )

    # Compute changes
    outcome.ranking_change = outcome.before_position - outcome.after_position  # positive = improved
    outcome.traffic_change = outcome.after_clicks - outcome.before_clicks
    if outcome.before_clicks > 0:
        outcome.traffic_change_pct = outcome.traffic_change / outcome.before_clicks
    outcome.ctr_change = outcome.after_ctr - outcome.before_ctr
    outcome.time_to_effect_days = (outcome.measured_at - executed_at).days

    # Performance score: (ranking_gain × 0.4) + (traffic_gain × 0.3) + (conversion_signal × 0.3)
    # Normalize each to 0-10 scale
    rank_score = min(10, max(0, outcome.ranking_change))  # +1 pos = +1 point, cap at 10
    traffic_score = min(10, max(0, outcome.traffic_change_pct * 20))  # +50% = 10
    conversion_score = min(10, max(0, outcome.ctr_change * 100))  # +0.1 CTR = 10

    outcome.performance_score = round(
        rank_score * 0.4 + traffic_score * 0.3 + conversion_score * 0.3,
        2,
    )
    outcome.success = outcome.performance_score >= 4.0

    log.info("attribution  task=%s  perf=%.1f  rank=%+.0f  traffic=%+d",
             task_id, outcome.performance_score, outcome.ranking_change, outcome.traffic_change)

    return outcome
