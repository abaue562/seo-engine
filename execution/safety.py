"""Safety constraints + rate limiting for execution engine.

Rules:
- Never overwrite entire pages without approval
- Never delete content without approval
- Never change core business info without approval
- Prefer additive changes over destructive ones
- Rate limit to mimic human behavior
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from collections import defaultdict

from models.task import SEOTask, TaskType, ExecutionMode

log = logging.getLogger(__name__)

# --- Rate limits (per business, per day) ---
DAILY_LIMITS = {
    TaskType.GBP: 3,         # Max 3 GBP posts/day
    TaskType.CONTENT: 2,     # Max 2 articles/day
    TaskType.WEBSITE: 5,     # Max 5 page updates/day
    TaskType.AUTHORITY: 5,   # Max 5 outreach emails/day
}

# --- Dangerous action keywords (require MANUAL or ASSISTED, never AUTO) ---
DANGEROUS_KEYWORDS = [
    "delete", "remove", "overwrite", "replace entire",
    "change business name", "change address", "change phone",
    "drop", "destroy", "reset",
]


class SafetyGate:
    """Validates tasks before execution. Blocks dangerous or rate-limited actions."""

    def __init__(self):
        # Track daily execution counts: {business_id: {TaskType: count}}
        self._daily_counts: dict[str, dict[TaskType, int]] = defaultdict(lambda: defaultdict(int))
        self._day_key: str = ""

    def _reset_if_new_day(self) -> None:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        if today != self._day_key:
            self._daily_counts.clear()
            self._day_key = today

    def check(self, task: SEOTask, business_id: str) -> tuple[bool, str]:
        """Returns (allowed, reason). If not allowed, reason explains why."""
        self._reset_if_new_day()

        # 1. Block dangerous AUTO actions
        if task.execution_mode == ExecutionMode.AUTO:
            action_lower = task.action.lower() + " " + task.execution.lower()
            for keyword in DANGEROUS_KEYWORDS:
                if keyword in action_lower:
                    return False, f"Dangerous action '{keyword}' cannot run in AUTO mode. Requires MANUAL or ASSISTED."

        # 2. Rate limit check
        limit = DAILY_LIMITS.get(task.type, 5)
        current = self._daily_counts[business_id][task.type]
        if current >= limit:
            return False, f"Rate limit reached: {task.type.value} ({current}/{limit} today). Will execute tomorrow."

        return True, "ok"

    def record_execution(self, task: SEOTask, business_id: str) -> None:
        """Record that a task was executed (for rate limiting)."""
        self._reset_if_new_day()
        self._daily_counts[business_id][task.type] += 1

    def get_remaining(self, business_id: str, task_type: TaskType) -> int:
        """How many more executions of this type are allowed today."""
        self._reset_if_new_day()
        limit = DAILY_LIMITS.get(task_type, 5)
        current = self._daily_counts[business_id][task_type]
        return max(0, limit - current)
