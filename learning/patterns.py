"""Pattern Memory — learns what SEO actions work and which don't.

Stores success rates by action type + context.
Strategy agent uses this to boost confidence for proven patterns
and kill strategies that consistently fail.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pydantic import BaseModel, Field

from data.storage.database import Database
from learning.attribution import TaskOutcome

log = logging.getLogger(__name__)

# Auto-kill threshold: if success rate drops below this, stop recommending
KILL_THRESHOLD = 0.40


class ActionPattern(BaseModel):
    """Learned pattern: how well a specific action type performs."""
    pattern: str                         # e.g., "title_optimization", "gbp_post", "service_page"
    task_type: str                       # GBP / WEBSITE / CONTENT / AUTHORITY
    times_used: int = 0
    successes: int = 0
    failures: int = 0
    avg_performance: float = 0.0
    avg_time_to_effect_days: float = 0.0
    success_rate: float = 0.0
    is_killed: bool = False              # True if success_rate < KILL_THRESHOLD after enough data
    last_updated: datetime = Field(default_factory=datetime.utcnow)

    def update(self, outcome: TaskOutcome) -> None:
        """Incorporate a new outcome into this pattern's stats."""
        self.times_used += 1
        if outcome.success:
            self.successes += 1
        else:
            self.failures += 1

        # Running average performance
        self.avg_performance = round(
            ((self.avg_performance * (self.times_used - 1)) + outcome.performance_score) / self.times_used,
            2,
        )

        # Running average time to effect
        if outcome.time_to_effect_days > 0:
            self.avg_time_to_effect_days = round(
                ((self.avg_time_to_effect_days * (self.times_used - 1)) + outcome.time_to_effect_days) / self.times_used,
                1,
            )

        self.success_rate = round(self.successes / self.times_used, 3) if self.times_used > 0 else 0

        # Auto-kill after enough data
        if self.times_used >= 5 and self.success_rate < KILL_THRESHOLD:
            self.is_killed = True
            log.warning("pattern.killed  pattern=%s  rate=%.0f%%  used=%d",
                        self.pattern, self.success_rate * 100, self.times_used)

        self.last_updated = datetime.utcnow()


class PatternMemory:
    """Stores and retrieves learned action patterns."""

    def __init__(self, db: Database | None = None):
        self.db = db or Database()
        self._cache: dict[str, ActionPattern] = {}

    def _pattern_key(self, action: str, task_type: str) -> str:
        """Derive a pattern key from action text."""
        action_lower = action.lower()

        # Map action text to canonical patterns
        if "title" in action_lower or "meta" in action_lower:
            return f"{task_type}:meta_optimization"
        elif "post" in action_lower and task_type == "GBP":
            return f"{task_type}:gbp_post"
        elif "review" in action_lower:
            return f"{task_type}:review_response"
        elif "page" in action_lower or "create" in action_lower:
            return f"{task_type}:page_creation"
        elif "article" in action_lower or "blog" in action_lower or "content" in action_lower:
            return f"{task_type}:content_creation"
        elif "link" in action_lower:
            return f"{task_type}:linking"
        elif "outreach" in action_lower or "backlink" in action_lower:
            return f"{task_type}:outreach"
        elif "directory" in action_lower or "citation" in action_lower:
            return f"{task_type}:citation"
        else:
            return f"{task_type}:other"

    async def record_outcome(self, outcome: TaskOutcome) -> ActionPattern:
        """Record a task outcome and update the corresponding pattern."""
        key = self._pattern_key(outcome.action, outcome.task_type)

        if key not in self._cache:
            # Try to load from DB
            stored = await self.db.query("pattern_memory", {"pattern": key}, limit=1)
            if stored:
                self._cache[key] = ActionPattern(**stored[0])
            else:
                self._cache[key] = ActionPattern(pattern=key, task_type=outcome.task_type)

        pattern = self._cache[key]
        pattern.update(outcome)

        # Persist
        await self.db.upsert("pattern_memory", pattern.model_dump(), key="pattern")
        return pattern

    async def get_pattern(self, action: str, task_type: str) -> ActionPattern | None:
        """Get the learned pattern for an action type."""
        key = self._pattern_key(action, task_type)
        if key in self._cache:
            return self._cache[key]

        stored = await self.db.query("pattern_memory", {"pattern": key}, limit=1)
        if stored:
            pattern = ActionPattern(**stored[0])
            self._cache[key] = pattern
            return pattern
        return None

    async def get_all_patterns(self) -> list[ActionPattern]:
        """Get all learned patterns."""
        stored = await self.db.query("pattern_memory", limit=100)
        return [ActionPattern(**p) for p in stored]

    async def get_killed_patterns(self) -> list[str]:
        """Get patterns that have been auto-killed."""
        all_patterns = await self.get_all_patterns()
        return [p.pattern for p in all_patterns if p.is_killed]

    def confidence_adjustment(self, pattern: ActionPattern | None) -> int:
        """How much to adjust confidence_score based on pattern history.
        Returns -2 to +2."""
        if pattern is None:
            return 0
        if pattern.is_killed:
            return -3
        if pattern.success_rate >= 0.8 and pattern.times_used >= 3:
            return +2
        if pattern.success_rate >= 0.6:
            return +1
        if pattern.success_rate < 0.5 and pattern.times_used >= 3:
            return -1
        return 0

    def patterns_to_prompt_block(self, patterns: list[ActionPattern]) -> str:
        """Render learned patterns as context for the Strategy Agent."""
        if not patterns:
            return "HISTORICAL PERFORMANCE: No historical data available."

        lines = ["HISTORICAL PERFORMANCE (use this to adjust confidence):"]

        winning = [p for p in patterns if p.success_rate >= 0.6 and p.times_used >= 3]
        losing = [p for p in patterns if p.success_rate < 0.5 and p.times_used >= 3]
        killed = [p for p in patterns if p.is_killed]

        if winning:
            lines.append("\n  PROVEN WINNERS (prioritize these):")
            for p in sorted(winning, key=lambda x: x.avg_performance, reverse=True):
                lines.append(f"    {p.pattern}: {p.success_rate:.0%} success, avg perf={p.avg_performance}, "
                             f"avg {p.avg_time_to_effect_days:.0f}d to effect ({p.times_used} uses)")

        if losing:
            lines.append("\n  UNDERPERFORMERS (reduce confidence):")
            for p in losing:
                lines.append(f"    {p.pattern}: {p.success_rate:.0%} success, avg perf={p.avg_performance} ({p.times_used} uses)")

        if killed:
            lines.append("\n  KILLED STRATEGIES (do NOT recommend):")
            for p in killed:
                lines.append(f"    {p.pattern}: {p.success_rate:.0%} success — STOPPED")

        return "\n".join(lines)
