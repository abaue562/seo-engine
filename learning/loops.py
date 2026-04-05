"""Learning Loops — weekly and monthly evaluation cycles.

Weekly: evaluate recent tasks, update patterns, adjust strategy weights.
Monthly: re-rank all strategies, identify new winning patterns, purge stale data.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from data.storage.database import Database
from data.connectors.gsc import GSCData, gsc_to_rankings
from learning.attribution import TaskOutcome, attribute_result
from learning.patterns import PatternMemory

log = logging.getLogger(__name__)


class LearningReport(BaseModel):
    """Output of a learning cycle."""
    cycle_type: str                     # "weekly" or "monthly"
    tasks_evaluated: int = 0
    successful: int = 0
    failed: int = 0
    patterns_updated: int = 0
    patterns_killed: int = 0
    top_performers: list[dict] = Field(default_factory=list)
    worst_performers: list[dict] = Field(default_factory=list)
    recommendations: list[str] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=datetime.utcnow)


class LearningEngine:
    """Runs weekly/monthly learning cycles."""

    def __init__(self, db: Database | None = None):
        self.db = db or Database()
        self.patterns = PatternMemory(self.db)

    async def weekly_cycle(self, business_id: str, gsc_data: GSCData | None = None) -> LearningReport:
        """Weekly evaluation: score recent tasks, update patterns."""
        report = LearningReport(cycle_type="weekly")
        log.info("learning.weekly_start  biz=%s", business_id)

        # Get tasks executed in last 7 days
        all_tasks = await self.db.query("execution_logs", {"business_id": business_id}, limit=100)

        # Filter to recent, successful executions
        cutoff = datetime.utcnow() - timedelta(days=7)
        recent_tasks = []
        for t in all_tasks:
            try:
                exec_time = datetime.fromisoformat(t.get("started_at", ""))
                if exec_time >= cutoff and t.get("status") == "success":
                    recent_tasks.append(t)
            except (ValueError, TypeError):
                pass

        report.tasks_evaluated = len(recent_tasks)

        # Get current rankings for comparison
        current_rankings = {}
        if gsc_data:
            current_rankings = gsc_to_rankings(gsc_data)

        # Attribute results
        for task_log in recent_tasks:
            before = task_log.get("before_state", {})
            after = task_log.get("after_state", {})

            # Enrich 'after' with current GSC data if available
            target = task_log.get("target", "")
            if target in current_rankings:
                after["position"] = current_rankings[target]

            outcome = attribute_result(
                task_id=task_log.get("task_id", ""),
                action=task_log.get("action", ""),
                task_type=task_log.get("task_type", ""),
                target=target,
                executed_at=datetime.fromisoformat(task_log.get("started_at", datetime.utcnow().isoformat())),
                before=before,
                after=after,
            )

            # Update pattern memory
            pattern = await self.patterns.record_outcome(outcome)
            report.patterns_updated += 1

            if pattern.is_killed:
                report.patterns_killed += 1

            if outcome.success:
                report.successful += 1
            else:
                report.failed += 1

            # Store outcome
            await self.db.upsert("task_outcomes", outcome.model_dump())

        # Generate top/worst performers
        all_patterns = await self.patterns.get_all_patterns()
        report.top_performers = [
            {"pattern": p.pattern, "success_rate": p.success_rate, "avg_perf": p.avg_performance}
            for p in sorted(all_patterns, key=lambda x: x.avg_performance, reverse=True)[:5]
            if p.times_used >= 2
        ]
        report.worst_performers = [
            {"pattern": p.pattern, "success_rate": p.success_rate, "avg_perf": p.avg_performance}
            for p in sorted(all_patterns, key=lambda x: x.avg_performance)[:3]
            if p.times_used >= 2 and p.success_rate < 0.5
        ]

        # Recommendations
        if report.failed > report.successful and report.tasks_evaluated > 3:
            report.recommendations.append("More tasks failing than succeeding — review strategy priorities.")
        killed = [p for p in all_patterns if p.is_killed]
        if killed:
            report.recommendations.append(f"{len(killed)} strategies auto-killed. Review and consider alternatives.")

        log.info("learning.weekly_done  evaluated=%d  success=%d  fail=%d  killed=%d",
                 report.tasks_evaluated, report.successful, report.failed, report.patterns_killed)

        return report

    async def monthly_cycle(self, business_id: str) -> LearningReport:
        """Monthly evolution: re-rank strategies, identify trends, purge stale patterns."""
        report = LearningReport(cycle_type="monthly")
        log.info("learning.monthly_start  biz=%s", business_id)

        all_patterns = await self.patterns.get_all_patterns()

        # Re-evaluate killed patterns — maybe they work now in new context
        for pattern in all_patterns:
            if pattern.is_killed and pattern.times_used >= 10:
                # Check if recent attempts improved
                recent_outcomes = await self.db.query("task_outcomes", {
                    "task_type": pattern.task_type,
                }, limit=5)

                recent_successes = sum(1 for o in recent_outcomes if o.get("success", False))
                if recent_successes >= 3:
                    pattern.is_killed = False
                    log.info("learning.pattern_revived  pattern=%s", pattern.pattern)
                    report.recommendations.append(f"Pattern '{pattern.pattern}' revived — recent success rate improved.")

        report.patterns_updated = len(all_patterns)
        report.top_performers = [
            {"pattern": p.pattern, "success_rate": p.success_rate, "avg_perf": p.avg_performance,
             "times_used": p.times_used}
            for p in sorted(all_patterns, key=lambda x: x.avg_performance, reverse=True)[:5]
        ]

        log.info("learning.monthly_done  patterns=%d", len(all_patterns))
        return report
