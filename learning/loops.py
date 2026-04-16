"""Learning Loops — weekly and monthly evaluation cycles.

Weekly: evaluate recent tasks, update patterns, adjust strategy weights.
Monthly: re-rank all strategies, identify new winning patterns, purge stale data.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from pydantic import BaseModel, Field

from data.storage.database import Database
from data.connectors.gsc import GSCData, gsc_to_rankings
from learning.attribution import TaskOutcome, attribute_result
from learning.patterns import PatternMemory

log = logging.getLogger(__name__)

_TASK_RESULTS_DIR = Path("data/storage/task_results")
_RANK_HISTORY_DIR = Path("data/storage/rank_history")
_STRATEGY_PARAMS_PATH = Path("data/storage/strategy_params.json")
_MONTHLY_REPORTS_PATH = Path("data/storage/monthly_reports.json")


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
    strategy_adjustments: dict = Field(default_factory=dict)
    generated_at: datetime = Field(default_factory=datetime.utcnow)


def _load_strategy_params() -> dict:
    """Load strategy params JSON, return default if missing."""
    if _STRATEGY_PARAMS_PATH.exists():
        try:
            return json.loads(_STRATEGY_PARAMS_PATH.read_text())
        except Exception:
            pass
    return {
        "impact_weight": {"content_creation": 1.0, "linking": 1.0, "meta_optimization": 1.0,
                          "gbp_post": 1.0, "citation": 1.0, "outreach": 1.0,
                          "page_creation": 1.0, "review_response": 1.0, "other": 1.0},
        "aggressiveness": 5.0,
        "content_weight": 0.5,
        "link_weight": 0.5,
        "last_updated": datetime.utcnow().isoformat(),
    }


def _save_strategy_params(params: dict) -> None:
    _STRATEGY_PARAMS_PATH.parent.mkdir(parents=True, exist_ok=True)
    params["last_updated"] = datetime.utcnow().isoformat()
    _STRATEGY_PARAMS_PATH.write_text(json.dumps(params, indent=2, default=str))


def _load_rank_history(business_id: str) -> dict:
    """Load rank history for a business. Returns {keyword: [(date, position), ...]}."""
    path = _RANK_HISTORY_DIR / f"{business_id}.json"
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}
    return {}


def _load_task_results(business_id: str, since_days: int = 7) -> list[dict]:
    """Load task results from file storage for the last N days."""
    results = []
    cutoff = datetime.utcnow() - timedelta(days=since_days)

    # Try per-business results file first
    path = _TASK_RESULTS_DIR / f"{business_id}_results.json"
    if path.exists():
        try:
            all_results = json.loads(path.read_text())
            for r in all_results:
                try:
                    dt = datetime.fromisoformat(r.get("date", ""))
                    if dt >= cutoff:
                        results.append(r)
                except (ValueError, TypeError):
                    pass
        except Exception:
            pass

    return results


class LearningEngine:
    """Runs weekly/monthly learning cycles."""

    def __init__(self, db: Database | None = None):
        self.db = db or Database()
        self.patterns = PatternMemory(self.db)

    async def weekly_cycle(
        self,
        business_id: str,
        gsc_data: GSCData | None = None,
    ) -> LearningReport:
        """Weekly evaluation: score recent tasks, update patterns, adjust strategy weights."""
        report = LearningReport(cycle_type="weekly")
        log.info("learning.weekly_start  biz=%s", business_id)

        # ------------------------------------------------------------------
        # 1. Load task results from last 7 days
        # ------------------------------------------------------------------
        # First try DB
        all_tasks = await self.db.query("execution_logs", {"business_id": business_id}, limit=200)
        cutoff = datetime.utcnow() - timedelta(days=7)
        db_recent = []
        for t in all_tasks:
            try:
                exec_time = datetime.fromisoformat(t.get("started_at", ""))
                if exec_time >= cutoff and t.get("status") == "success":
                    db_recent.append(t)
            except (ValueError, TypeError):
                pass

        # Also check file-based results (from AutonomousRunner)
        file_recent = _load_task_results(business_id, since_days=7)

        # Merge: file-based results provide task_type + action for attribution
        recent_tasks = db_recent
        if not recent_tasks:
            # Build synthetic execution log records from file results
            for fr in file_recent:
                if fr.get("status") == "success":
                    recent_tasks.append({
                        "task_id": fr.get("task_id", ""),
                        "action": fr.get("action", ""),
                        "task_type": fr.get("type", "WEBSITE"),
                        "target": fr.get("action", "")[:80],
                        "started_at": fr.get("date", datetime.utcnow().isoformat()),
                        "status": "success",
                        "before_state": {},
                        "after_state": {},
                    })

        report.tasks_evaluated = len(recent_tasks)

        # ------------------------------------------------------------------
        # 2. Load rank history for before/after comparison
        # ------------------------------------------------------------------
        rank_history = _load_rank_history(business_id)

        current_rankings: dict[str, float] = {}
        if gsc_data:
            current_rankings = {k: float(v) for k, v in gsc_to_rankings(gsc_data).items()}

        # ------------------------------------------------------------------
        # 3. Calculate rank delta per task type and record outcomes
        # ------------------------------------------------------------------
        type_rank_deltas: dict[str, list[float]] = defaultdict(list)
        type_results: dict[str, list[bool]] = defaultdict(list)

        for task_log in recent_tasks:
            task_id = task_log.get("task_id", "")
            action = task_log.get("action", "")
            task_type = task_log.get("task_type", "WEBSITE")
            target = task_log.get("target", "")
            started_at_str = task_log.get("started_at", datetime.utcnow().isoformat())

            try:
                executed_at = datetime.fromisoformat(started_at_str)
            except (ValueError, TypeError):
                executed_at = datetime.utcnow()

            before = dict(task_log.get("before_state", {}))
            after = dict(task_log.get("after_state", {}))

            # Enrich after-state with current GSC data if keyword matches target
            target_lower = target.lower()
            for kw, pos in current_rankings.items():
                if kw.lower() in target_lower or target_lower in kw.lower():
                    after["position"] = pos
                    break

            # Try to get before position from rank history
            if "position" not in before and target in rank_history:
                history_entries = rank_history[target]
                # Find entry closest to (executed_at - some window)
                before_window = executed_at - timedelta(days=3)
                for entry_date_str, pos in sorted(history_entries, key=lambda x: x[0]):
                    try:
                        entry_date = datetime.fromisoformat(entry_date_str)
                        if entry_date <= before_window:
                            before["position"] = pos
                    except (ValueError, TypeError):
                        pass

            outcome = attribute_result(
                task_id=task_id,
                action=action,
                task_type=task_type,
                target=target,
                executed_at=executed_at,
                before=before,
                after=after,
            )

            # Record in pattern memory
            pattern = await self.patterns.record_outcome(outcome)
            report.patterns_updated += 1

            if pattern.is_killed:
                report.patterns_killed += 1

            if outcome.success:
                report.successful += 1
            else:
                report.failed += 1

            # Track by type for weight adjustment
            type_rank_deltas[task_type].append(outcome.ranking_change)
            type_results[task_type].append(outcome.success)

            # Persist to DB
            try:
                await self.db.upsert("task_outcomes", {
                    k: str(v) if isinstance(v, datetime) else v
                    for k, v in outcome.model_dump().items()
                })
            except Exception as exc:
                log.debug("learning.outcome_save_fail  err=%s", exc)

        # ------------------------------------------------------------------
        # 4. Identify top 3 winning and bottom 3 failing task types
        # ------------------------------------------------------------------
        type_avg_delta: dict[str, float] = {
            t: sum(deltas) / len(deltas)
            for t, deltas in type_rank_deltas.items()
            if deltas
        }

        sorted_types = sorted(type_avg_delta.items(), key=lambda x: x[1], reverse=True)
        top_winners = sorted_types[:3]
        bottom_losers = sorted_types[-3:][::-1]

        # ------------------------------------------------------------------
        # 5. Adjust strategy_params impact_weight
        # ------------------------------------------------------------------
        params = _load_strategy_params()
        impact_weights = params.get("impact_weight", {})

        adjustments: dict[str, str] = {}
        for task_type, avg_delta in top_winners:
            key = task_type.lower()
            old = impact_weights.get(key, 1.0)
            new = min(2.0, old + 0.1)
            if abs(new - old) > 0.001:
                impact_weights[key] = round(new, 3)
                adjustments[task_type] = f"+0.1 (was {old:.2f}, now {new:.2f})"
                log.info("learning.weight_increase  type=%s  weight=%.2f", task_type, new)

        for task_type, avg_delta in bottom_losers:
            if avg_delta < 0:  # Only decrease if actually negative
                key = task_type.lower()
                old = impact_weights.get(key, 1.0)
                new = max(0.1, old - 0.1)
                if abs(new - old) > 0.001:
                    impact_weights[key] = round(new, 3)
                    adjustments[task_type] = f"-0.1 (was {old:.2f}, now {new:.2f})"
                    log.info("learning.weight_decrease  type=%s  weight=%.2f", task_type, new)

        params["impact_weight"] = impact_weights
        _save_strategy_params(params)
        report.strategy_adjustments = adjustments

        # ------------------------------------------------------------------
        # 6. Build top/worst performers from pattern memory
        # ------------------------------------------------------------------
        all_patterns = await self.patterns.get_all_patterns()
        report.top_performers = [
            {
                "pattern": p.pattern,
                "success_rate": p.success_rate,
                "avg_perf": p.avg_performance,
                "times_used": p.times_used,
            }
            for p in sorted(all_patterns, key=lambda x: x.avg_performance, reverse=True)[:3]
            if p.times_used >= 2
        ]
        report.worst_performers = [
            {
                "pattern": p.pattern,
                "success_rate": p.success_rate,
                "avg_perf": p.avg_performance,
                "times_used": p.times_used,
            }
            for p in sorted(all_patterns, key=lambda x: x.avg_performance)[:3]
            if p.times_used >= 2 and p.success_rate < 0.5
        ]

        # ------------------------------------------------------------------
        # 7. Recommendations
        # ------------------------------------------------------------------
        if report.failed > report.successful and report.tasks_evaluated > 3:
            report.recommendations.append(
                "More tasks failing than succeeding — review strategy priorities."
            )
        killed = [p for p in all_patterns if p.is_killed]
        if killed:
            report.recommendations.append(
                f"{len(killed)} strategies auto-killed. Review and consider alternatives."
            )
        if top_winners:
            top_name, top_delta = top_winners[0]
            report.recommendations.append(
                f"Best task type this week: {top_name} (avg rank delta: {top_delta:+.1f})."
            )
        if bottom_losers and bottom_losers[0][1] < 0:
            bot_name, bot_delta = bottom_losers[0]
            report.recommendations.append(
                f"Worst task type: {bot_name} (avg rank delta: {bot_delta:+.1f}). Consider pausing."
            )

        log.info(
            "learning.weekly_done  evaluated=%d  success=%d  fail=%d  killed=%d  adjustments=%d",
            report.tasks_evaluated, report.successful, report.failed,
            report.patterns_killed, len(adjustments),
        )
        return report

    async def monthly_cycle(self, business_id: str) -> LearningReport:
        """Monthly evolution: re-rank strategies, identify trends, purge stale patterns."""
        report = LearningReport(cycle_type="monthly")
        log.info("learning.monthly_start  biz=%s", business_id)

        # ------------------------------------------------------------------
        # 1. Aggregate last 4 weekly reports (re-run weekly for last 30 days)
        # ------------------------------------------------------------------
        all_outcomes = await self.db.query("task_outcomes", limit=500)
        cutoff = datetime.utcnow() - timedelta(days=30)
        monthly_outcomes = []
        for o in all_outcomes:
            try:
                measured = datetime.fromisoformat(str(o.get("measured_at", "")))
                if measured >= cutoff:
                    monthly_outcomes.append(o)
            except (ValueError, TypeError):
                pass

        report.tasks_evaluated = len(monthly_outcomes)
        log.info("learning.monthly_outcomes  count=%d", len(monthly_outcomes))

        # ------------------------------------------------------------------
        # 2. Calculate 30-day ranking trends per keyword
        # ------------------------------------------------------------------
        rank_history = _load_rank_history(business_id)
        gaining_keywords: list[str] = []
        losing_keywords: list[str] = []

        for keyword, history in rank_history.items():
            if len(history) < 2:
                continue
            try:
                sorted_history = sorted(history, key=lambda x: x[0])
                oldest_pos = float(sorted_history[0][1])
                newest_pos = float(sorted_history[-1][1])
                delta = oldest_pos - newest_pos  # positive = improved (lower rank number)
                if delta > 1:
                    gaining_keywords.append(keyword)
                elif delta < -1:
                    losing_keywords.append(keyword)
            except (ValueError, TypeError, IndexError):
                pass

        log.info("learning.monthly_trends  gaining=%d  losing=%d",
                 len(gaining_keywords), len(losing_keywords))
        if gaining_keywords:
            report.recommendations.append(
                f"Gaining clusters ({len(gaining_keywords)} keywords): {', '.join(gaining_keywords[:5])}."
            )
        if losing_keywords:
            report.recommendations.append(
                f"Losing clusters ({len(losing_keywords)} keywords): {', '.join(losing_keywords[:5])}. "
                f"Consider new content or link building."
            )

        # ------------------------------------------------------------------
        # 3. Re-evaluate killed patterns — maybe they work now
        # ------------------------------------------------------------------
        all_patterns = await self.patterns.get_all_patterns()
        revived = 0
        for pattern in all_patterns:
            if pattern.is_killed and pattern.times_used >= 10:
                recent_outcomes = await self.db.query("task_outcomes", {
                    "task_type": pattern.task_type,
                }, limit=5)
                recent_successes = sum(1 for o in recent_outcomes if o.get("success", False))
                if recent_successes >= 3:
                    pattern.is_killed = False
                    revived += 1
                    log.info("learning.pattern_revived  pattern=%s", pattern.pattern)
                    report.recommendations.append(
                        f"Pattern '{pattern.pattern}' revived — recent success rate improved."
                    )

        # ------------------------------------------------------------------
        # 4. Call StrategyEvolution.evolve() with 30-day performance data
        # ------------------------------------------------------------------
        if monthly_outcomes:
            successes = sum(1 for o in monthly_outcomes if o.get("success", False))
            success_rate = successes / len(monthly_outcomes) if monthly_outcomes else 0

            ranking_changes = [
                float(o.get("ranking_change", 0))
                for o in monthly_outcomes
                if o.get("ranking_change") is not None
            ]
            avg_rank_improvement = sum(ranking_changes) / len(ranking_changes) if ranking_changes else 0

            # Identify content vs link task performance
            content_outcomes = [o for o in monthly_outcomes
                                if "CONTENT" in str(o.get("task_type", "")).upper()
                                or "content" in str(o.get("action", "")).lower()]
            link_outcomes = [o for o in monthly_outcomes
                             if "AUTHORITY" in str(o.get("task_type", "")).upper()
                             or "link" in str(o.get("action", "")).lower()
                             or "outreach" in str(o.get("action", "")).lower()]

            content_perf = (
                sum(float(o.get("performance_score", 0)) for o in content_outcomes) / len(content_outcomes)
                if content_outcomes else 0
            )
            link_perf = (
                sum(float(o.get("performance_score", 0)) for o in link_outcomes) / len(link_outcomes)
                if link_outcomes else 0
            )

            performance_data = {
                "avg_rank_improvement": avg_rank_improvement,
                "success_rate": success_rate,
                "content_avg_performance": content_perf,
                "link_avg_performance": link_perf,
                "tasks_evaluated": len(monthly_outcomes),
                "gaining_keywords": gaining_keywords,
                "losing_keywords": losing_keywords,
            }

            try:
                from learning.evolution import StrategyEvolution
                evolution = StrategyEvolution(self.patterns)
                evo_report = await evolution.evolve(performance_data)
                report.strategy_adjustments = evo_report.get("mutations", {})
                log.info("learning.evolution_done  mutations=%d",
                         len(report.strategy_adjustments))
            except Exception as exc:
                log.warning("learning.evolution_fail  err=%s", exc)

        # ------------------------------------------------------------------
        # 5. Generate and save monthly performance summary
        # ------------------------------------------------------------------
        report.patterns_updated = len(all_patterns)
        report.top_performers = [
            {
                "pattern": p.pattern,
                "success_rate": p.success_rate,
                "avg_perf": p.avg_performance,
                "times_used": p.times_used,
            }
            for p in sorted(all_patterns, key=lambda x: x.avg_performance, reverse=True)[:5]
        ]

        # Persist monthly report
        try:
            monthly_reports = []
            if _MONTHLY_REPORTS_PATH.exists():
                monthly_reports = json.loads(_MONTHLY_REPORTS_PATH.read_text())
            monthly_reports.append({
                **report.model_dump(),
                "generated_at": report.generated_at.isoformat(),
            })
            # Keep last 12 monthly reports
            _MONTHLY_REPORTS_PATH.parent.mkdir(parents=True, exist_ok=True)
            _MONTHLY_REPORTS_PATH.write_text(
                json.dumps(monthly_reports[-12:], indent=2, default=str)
            )
        except Exception as exc:
            log.warning("learning.monthly_save_fail  err=%s", exc)

        log.info(
            "learning.monthly_done  patterns=%d  revived=%d  gaining_kw=%d  losing_kw=%d",
            len(all_patterns), revived, len(gaining_keywords), len(losing_keywords),
        )
        return report
