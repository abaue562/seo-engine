"""Full Autonomous Mode — the system runs everything without human input.

This is the final layer. When enabled, the system:
  1. Pulls fresh data on schedule (daily/weekly)
  2. Detects events and opportunities
  3. Generates and scores tasks
  4. Executes AUTO tasks immediately
  5. Queues ASSISTED tasks for approval
  6. Measures results
  7. Evolves its own strategy
  8. Repeats

The human role shifts from "operator" to "supervisor" — reviewing results
and approving high-impact actions.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pydantic import BaseModel, Field

from models.business import BusinessContext
from models.task import SEOTask

log = logging.getLogger(__name__)


class AutonomousConfig(BaseModel):
    """Configuration for autonomous operation."""
    business_id: str
    business: BusinessContext

    # Scheduling
    data_refresh_hours: int = 24         # How often to pull fresh data
    analysis_hours: int = 24             # How often to run full analysis
    learning_days: int = 7               # How often to run learning cycle
    evolution_days: int = 30             # How often to evolve strategy

    # Execution
    auto_execute: bool = True            # Execute AUTO tasks without approval
    shadow_mode: bool = False            # Log everything but don't actually execute
    max_auto_executions_per_day: int = 5 # Safety cap

    # Thresholds
    min_confidence_for_auto: float = 7.0 # Only auto-execute above this confidence
    min_score_for_auto: float = 6.0      # Only auto-execute above this total score

    # Feature flags
    enable_demand_generation: bool = False
    enable_pressure_campaigns: bool = False
    enable_competitive_suppression: bool = False
    enable_entity_building: bool = True
    enable_cross_channel: bool = False


class CycleResult(BaseModel):
    """Result of one autonomous cycle."""
    cycle_id: str = ""
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None

    # Data
    data_refreshed: bool = False
    events_detected: int = 0

    # Analysis
    tasks_generated: int = 0
    tasks_filtered: int = 0

    # Execution
    auto_executed: int = 0
    queued_for_approval: int = 0
    skipped: int = 0

    # Learning
    learning_run: bool = False
    strategy_evolved: bool = False


class AutonomousRunner:
    """Runs the full autonomous loop."""

    def __init__(self, config: AutonomousConfig):
        self.config = config

    async def run_cycle(self) -> CycleResult:
        """Execute one full autonomous cycle."""
        import uuid
        from data.storage.database import Database
        from data.pipeline import IngestionPipeline
        from core.agents.orchestrator import AgentOrchestrator
        from execution.router import ExecutionRouter
        from learning.loops import LearningEngine
        from learning.patterns import PatternMemory
        from strategy.evolution import StrategyEvolution

        result = CycleResult(cycle_id=uuid.uuid4().hex[:12])
        cfg = self.config

        log.info("autonomous.cycle_start  id=%s  biz=%s", result.cycle_id, cfg.business.business_name)

        db = Database()
        pipeline = IngestionPipeline(db)
        orchestrator = AgentOrchestrator()
        executor = ExecutionRouter(db, shadow_mode=cfg.shadow_mode)
        learner = LearningEngine(db)
        patterns = PatternMemory(db)
        strategy = StrategyEvolution(patterns)

        # 1. Ingest fresh data
        try:
            data = await pipeline.run_full(
                business=cfg.business,
                business_id=cfg.business_id,
            )
            result.data_refreshed = True
            result.events_detected = len(data.events)
            log.info("autonomous.data  events=%d  freshness=%s",
                     len(data.events), data.freshness.overall_confidence())
        except Exception as e:
            log.error("autonomous.data_fail  err=%s", e)
            result.completed_at = datetime.utcnow()
            return result

        # 2. Run multi-agent analysis
        try:
            batch, plog = await orchestrator.run(
                business=cfg.business,
                input_type="FULL",
            )
            result.tasks_generated = len(batch.tasks)
            result.tasks_filtered = batch.filtered_count
            log.info("autonomous.analysis  tasks=%d  filtered=%d",
                     len(batch.tasks), batch.filtered_count)
        except Exception as e:
            log.error("autonomous.analysis_fail  err=%s", e)
            result.completed_at = datetime.utcnow()
            return result

        # 3. Apply strategy evolution weights
        try:
            weights = await strategy.evolve()
            batch.tasks = strategy.apply_to_scores(batch.tasks)
        except Exception as e:
            log.debug("autonomous.evolution_skip  err=%s", e)

        # 4. Apply freshness penalty
        penalty = data.freshness.confidence_penalty()
        if penalty > 0:
            for task in batch.tasks:
                task.confidence_score = max(1.0, task.confidence_score - penalty)

        # 5. Execute
        auto_count = 0
        for task in batch.tasks:
            if auto_count >= cfg.max_auto_executions_per_day:
                result.skipped += 1
                continue

            # Filter by confidence and score thresholds
            if task.confidence_score < cfg.min_confidence_for_auto:
                result.skipped += 1
                continue
            if task.total_score < cfg.min_score_for_auto:
                result.skipped += 1
                continue

            if cfg.auto_execute and task.execution_mode.value == "AUTO":
                exec_result = await executor.execute_task(task, cfg.business, cfg.business_id)
                if exec_result.status.value == "success":
                    result.auto_executed += 1
                    auto_count += 1
                log.info("autonomous.exec  action=%s  status=%s", task.action[:50], exec_result.status.value)
            elif task.execution_mode.value == "ASSISTED":
                # Queue for human approval
                await executor.execute_task(task, cfg.business, cfg.business_id)
                result.queued_for_approval += 1
            else:
                result.skipped += 1

        # 6. Save tasks
        await db.save_tasks(cfg.business_id, [t.model_dump() for t in batch.tasks])

        result.completed_at = datetime.utcnow()
        log.info("autonomous.cycle_done  id=%s  auto=%d  queued=%d  skipped=%d",
                 result.cycle_id, result.auto_executed, result.queued_for_approval, result.skipped)

        return result

    def should_run_learning(self, last_learning: datetime | None) -> bool:
        """Check if it's time for a learning cycle."""
        if last_learning is None:
            return True
        days_since = (datetime.utcnow() - last_learning).days
        return days_since >= self.config.learning_days

    def should_evolve_strategy(self, last_evolution: datetime | None) -> bool:
        """Check if it's time to evolve strategy weights."""
        if last_evolution is None:
            return True
        days_since = (datetime.utcnow() - last_evolution).days
        return days_since >= self.config.evolution_days
