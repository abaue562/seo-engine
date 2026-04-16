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

Usage:
    runner = AutonomousRunner(business_context, mode="shadow")
    result = runner.run_cycle()   # single cycle
    runner.start_loop(hours=24)   # continuous loop
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from models.business import BusinessContext
from models.task import SEOTask, ImpactLevel, ExecutionMode

log = logging.getLogger(__name__)

# Directories for file-based state
_STATE_DIR = Path("data/storage/runner_state")
_APPROVAL_DIR = Path("data/storage/approval_queue")
_RESULTS_DIR = Path("data/storage/task_results")

RunMode = Literal["shadow", "assisted", "autonomous"]


# =====================================================================
# Result models
# =====================================================================

class CycleResult(BaseModel):
    """Result of one autonomous run cycle."""
    cycle_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None
    mode: str = "shadow"

    # Step results
    data_refreshed: bool = False
    tasks_generated: int = 0
    tasks_auto_executed: int = 0
    tasks_queued: int = 0
    tasks_skipped: int = 0

    # Verification
    verifications_passed: int = 0
    verifications_failed: int = 0

    # Learning
    learning_triggered: bool = False

    # Summary
    top_action: str = ""
    errors: list[str] = Field(default_factory=list)


class DailyStats(BaseModel):
    """Stats for today's execution activity."""
    date: str = Field(default_factory=lambda: datetime.utcnow().date().isoformat())
    executions: int = 0
    tasks_queued: int = 0
    roi_estimate: float = 0.0
    task_results: list[dict] = Field(default_factory=list)


# =====================================================================
# AutonomousRunner
# =====================================================================

class AutonomousRunner:
    """
    Autonomous SEO execution loop.

    Modes:
        shadow    - analyze and plan but do not execute
        assisted  - execute low-risk tasks, queue high-risk for approval
        autonomous - execute all tasks within confidence threshold

    Usage:
        runner = AutonomousRunner(business_context, mode="shadow")
        result = runner.run_cycle()   # single cycle
        runner.start_loop(hours=24)   # continuous loop
    """

    def __init__(
        self,
        business_context: BusinessContext,
        business_id: str | None = None,
        mode: RunMode = "shadow",
        confidence_threshold: float = 0.7,
        max_daily_executions: int = 5,
    ):
        self.business = business_context
        self.business_id = business_id or business_context.business_name.lower().replace(" ", "_")
        self.mode = mode
        self.confidence_threshold = confidence_threshold
        self.max_daily_executions = max_daily_executions

        # Ensure storage dirs exist
        _STATE_DIR.mkdir(parents=True, exist_ok=True)
        _APPROVAL_DIR.mkdir(parents=True, exist_ok=True)
        _RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_cycle(self) -> CycleResult:
        """Execute one full autonomous cycle (sync wrapper)."""
        return asyncio.run(self._run_cycle_async())

    def start_loop(self, hours: int = 24) -> None:
        """Run run_cycle() repeatedly every N hours (blocking)."""
        log.info("autonomous.loop_start  biz=%s  interval_h=%d  mode=%s",
                 self.business_id, hours, self.mode)
        while True:
            try:
                result = self.run_cycle()
                log.info("autonomous.loop_cycle_done  id=%s  executed=%d  queued=%d",
                         result.cycle_id, result.tasks_auto_executed, result.tasks_queued)
            except Exception as exc:
                log.error("autonomous.loop_error  err=%s", exc)
            log.info("autonomous.loop_sleep  hours=%d", hours)
            time.sleep(hours * 3600)

    def get_approval_queue(self, business_id: str | None = None) -> list[dict]:
        """Return all PENDING tasks in the approval queue for this business."""
        bid = business_id or self.business_id
        path = _APPROVAL_DIR / f"{bid}.json"
        if not path.exists():
            return []
        try:
            items = json.loads(path.read_text())
            return [t for t in items if t.get("status") == "PENDING"]
        except Exception as exc:
            log.warning("approval_queue.read_fail  err=%s", exc)
            return []

    def approve_task(self, business_id: str, task_id: str) -> dict:
        """Mark a queued task as approved and execute it."""
        path = _APPROVAL_DIR / f"{business_id}.json"
        items = self._read_json_list(path)
        for item in items:
            if item.get("task_id") == task_id and item.get("status") == "PENDING":
                item["status"] = "APPROVED"
                item["approved_at"] = datetime.utcnow().isoformat()
                self._write_json(path, items)
                log.info("approval.approved  task=%s", task_id)
                # Best-effort async execution
                try:
                    task = SEOTask(**item["task"])
                    result = asyncio.run(self._execute_single_task(task))
                    item["exec_status"] = result.status.value
                    self._write_json(path, items)
                    return {"ok": True, "task_id": task_id, "exec_status": result.status.value}
                except Exception as exc:
                    log.error("approval.exec_fail  task=%s  err=%s", task_id, exc)
                    return {"ok": False, "task_id": task_id, "error": str(exc)}
        return {"ok": False, "task_id": task_id, "error": "Task not found or not PENDING"}

    def reject_task(self, business_id: str, task_id: str) -> dict:
        """Mark a queued task as rejected."""
        path = _APPROVAL_DIR / f"{business_id}.json"
        items = self._read_json_list(path)
        for item in items:
            if item.get("task_id") == task_id and item.get("status") == "PENDING":
                item["status"] = "REJECTED"
                item["rejected_at"] = datetime.utcnow().isoformat()
                self._write_json(path, items)
                log.info("approval.rejected  task=%s", task_id)
                return {"ok": True, "task_id": task_id}
        return {"ok": False, "task_id": task_id, "error": "Task not found or not PENDING"}

    def get_daily_stats(self) -> DailyStats:
        """Return today's execution count, task results, and ROI estimate."""
        state = self._load_state()
        today = datetime.utcnow().date().isoformat()
        daily_count = state.get("daily_count", 0) if state.get("daily_date") == today else 0

        results_path = _RESULTS_DIR / f"{self.business_id}_results.json"
        task_results = self._read_json_list(results_path)
        today_results = [r for r in task_results if r.get("date", "").startswith(today)]

        # Simple ROI estimate: each executed task ~ avg_job_value * 0.01
        roi_estimate = daily_count * self.business.avg_job_value * 0.01

        return DailyStats(
            date=today,
            executions=daily_count,
            tasks_queued=len(self.get_approval_queue()),
            roi_estimate=round(roi_estimate, 2),
            task_results=today_results,
        )

    # ------------------------------------------------------------------
    # Core cycle
    # ------------------------------------------------------------------

    async def _run_cycle_async(self) -> CycleResult:
        """Full async implementation of the run cycle."""
        result = CycleResult(mode=self.mode)
        log.info("autonomous.cycle_start  id=%s  biz=%s  mode=%s",
                 result.cycle_id, self.business_id, self.mode)

        # ------------------------------------------------------------------
        # Step 1: Check data freshness
        # ------------------------------------------------------------------
        state = self._load_state()
        last_ingest_str = state.get("last_ingest")
        needs_ingest = True
        if last_ingest_str:
            try:
                last_ingest = datetime.fromisoformat(last_ingest_str)
                hours_since = (datetime.utcnow() - last_ingest).total_seconds() / 3600
                needs_ingest = hours_since > 24
            except (ValueError, TypeError):
                pass

        if needs_ingest:
            log.info("autonomous.ingest_start  biz=%s", self.business_id)
            try:
                from data.pipeline import IngestionPipeline
                from data.storage.database import Database
                db = Database()
                pipeline = IngestionPipeline(db)
                await pipeline.run_full(business=self.business, business_id=self.business_id)
                result.data_refreshed = True
                state["last_ingest"] = datetime.utcnow().isoformat()
                log.info("autonomous.ingest_done  biz=%s", self.business_id)
            except Exception as exc:
                log.warning("autonomous.ingest_fail  err=%s", exc)
                result.errors.append(f"ingest: {exc}")
        else:
            log.info("autonomous.ingest_skip  data_fresh  biz=%s", self.business_id)

        # ------------------------------------------------------------------
        # Step 2: Analyze business with SEOBrain
        # ------------------------------------------------------------------
        from core.agents.brain import SEOBrain
        brain = SEOBrain()
        try:
            batch = await brain.analyze(self.business, input_type="FULL")
            result.tasks_generated = len(batch.tasks)
            top = batch.tasks[0] if batch.tasks else None
            result.top_action = top.action[:80] if top else ""
            log.info("autonomous.analyze_done  tasks=%d  top=%s",
                     len(batch.tasks), result.top_action)
        except Exception as exc:
            log.error("autonomous.analyze_fail  err=%s", exc)
            result.errors.append(f"analyze: {exc}")
            result.completed_at = datetime.utcnow()
            self._save_state(state)
            return result

        # ------------------------------------------------------------------
        # Step 3: Score and filter tasks
        # ------------------------------------------------------------------
        # Normalize confidence_score to 0-1 scale if it's on a 1-10 scale
        for task in batch.tasks:
            if task.confidence_score > 1.0:
                task.confidence_score = task.confidence_score / 10.0

        today = datetime.utcnow().date().isoformat()
        if state.get("daily_date") != today:
            state["daily_date"] = today
            state["daily_count"] = 0
        daily_count: int = state.get("daily_count", 0)
        remaining_budget = self.max_daily_executions - daily_count

        auto_execute_tasks: list[SEOTask] = []
        queue_tasks: list[SEOTask] = []
        skip_tasks: list[SEOTask] = []

        for task in batch.tasks:
            if task.confidence_score < self.confidence_threshold:
                skip_tasks.append(task)
                continue
            # Determine bucket based on mode
            is_low_risk = (
                task.impact == ImpactLevel.LOW
                or task.execution_mode == ExecutionMode.AUTO
            )
            if self.mode == "shadow":
                # Shadow: classify but never execute
                auto_execute_tasks.append(task)
            elif self.mode == "assisted":
                if is_low_risk:
                    auto_execute_tasks.append(task)
                else:
                    queue_tasks.append(task)
            else:  # autonomous
                auto_execute_tasks.append(task)

        # Safety cap: never auto-execute more than budget
        if len(auto_execute_tasks) > remaining_budget:
            overflow = auto_execute_tasks[remaining_budget:]
            auto_execute_tasks = auto_execute_tasks[:remaining_budget]
            # Push overflow to queue (or skip) depending on mode
            if self.mode == "autonomous":
                queue_tasks.extend(overflow)
            else:
                skip_tasks.extend(overflow)

        log.info("autonomous.filter  auto=%d  queue=%d  skip=%d",
                 len(auto_execute_tasks), len(queue_tasks), len(skip_tasks))

        # ------------------------------------------------------------------
        # Step 4: Execute approved tasks
        # ------------------------------------------------------------------
        exec_results = []

        if self.mode == "shadow":
            log.info("autonomous.shadow_mode  would_execute=%d", len(auto_execute_tasks))
            for task in auto_execute_tasks:
                log.info("autonomous.shadow  action=%s  confidence=%.2f",
                         task.action[:60], task.confidence_score)
            result.tasks_skipped = len(auto_execute_tasks) + len(skip_tasks)
        else:
            from execution.router import ExecutionRouter
            from data.storage.database import Database
            db = Database()
            router = ExecutionRouter(db=db, shadow_mode=False)

            for task in auto_execute_tasks:
                try:
                    exec_result = await router.execute_task(task, self.business, self.business_id)
                    exec_results.append((task, exec_result))
                    from execution.models import ExecStatus
                    if exec_result.status == ExecStatus.SUCCESS:
                        result.tasks_auto_executed += 1
                        daily_count += 1
                        log.info("autonomous.exec_ok  action=%s", task.action[:60])
                    else:
                        log.warning("autonomous.exec_skip  action=%s  status=%s",
                                    task.action[:60], exec_result.status.value)
                except Exception as exc:
                    log.error("autonomous.exec_fail  action=%s  err=%s", task.action[:60], exc)
                    result.errors.append(f"exec: {exc}")

            result.tasks_skipped = len(skip_tasks)

        # ------------------------------------------------------------------
        # Step 5: Verify executions
        # ------------------------------------------------------------------
        if exec_results:
            from execution.verification import verify_execution
            for task, exec_result in exec_results:
                await asyncio.sleep(2)
                try:
                    verified = await verify_execution(task, exec_result)
                    if verified:
                        result.verifications_passed += 1
                    else:
                        result.verifications_failed += 1
                    log.info("autonomous.verify  action=%s  ok=%s", task.action[:40], verified)
                except Exception as exc:
                    log.warning("autonomous.verify_fail  err=%s", exc)

        # ------------------------------------------------------------------
        # Step 6: Update approval queue
        # ------------------------------------------------------------------
        if queue_tasks:
            await self._write_approval_queue(queue_tasks)
            result.tasks_queued = len(queue_tasks)
            log.info("autonomous.queue_written  count=%d", len(queue_tasks))

        # ------------------------------------------------------------------
        # Step 7: Trigger learning if 7+ days since last cycle
        # ------------------------------------------------------------------
        last_learning_str = state.get("last_learning")
        days_since_learning = 999
        if last_learning_str:
            try:
                last_learning = datetime.fromisoformat(last_learning_str)
                days_since_learning = (datetime.utcnow() - last_learning).days
            except (ValueError, TypeError):
                pass

        if days_since_learning >= 7:
            log.info("autonomous.learning_trigger  days_since=%d", days_since_learning)
            try:
                from learning.loops import LearningEngine
                from data.storage.database import Database
                db = Database()
                learner = LearningEngine(db)
                task_data = [
                    {
                        "task_id": er.task_id,
                        "action": t.action,
                        "task_type": t.type.value,
                        "status": er.status.value,
                        "started_at": datetime.utcnow().isoformat(),
                    }
                    for t, er in exec_results
                ]
                learning_report = await learner.weekly_cycle(
                    business_id=self.business_id,
                )
                result.learning_triggered = True
                state["last_learning"] = datetime.utcnow().isoformat()
                log.info("autonomous.learning_done  evaluated=%d  patterns_updated=%d",
                         learning_report.tasks_evaluated, learning_report.patterns_updated)
            except Exception as exc:
                log.warning("autonomous.learning_fail  err=%s", exc)
                result.errors.append(f"learning: {exc}")

        # ------------------------------------------------------------------
        # Step 8: Save state
        # ------------------------------------------------------------------
        state["last_run"] = datetime.utcnow().isoformat()
        state["tasks_executed"] = result.tasks_auto_executed
        state["tasks_queued"] = result.tasks_queued
        state["daily_count"] = daily_count
        state["next_run_time"] = (datetime.utcnow() + timedelta(hours=24)).isoformat()
        self._save_state(state)

        # Also save task results for stats
        self._append_task_results(exec_results, result)

        result.completed_at = datetime.utcnow()

        # ------------------------------------------------------------------
        # Step 9: Send alert if webhook configured
        # ------------------------------------------------------------------
        webhook_url = os.environ.get("ALERT_WEBHOOK_URL", "")
        if webhook_url:
            await self._send_webhook_alert(webhook_url, result)

        log.info("autonomous.cycle_done  id=%s  executed=%d  queued=%d  skipped=%d",
                 result.cycle_id, result.tasks_auto_executed,
                 result.tasks_queued, result.tasks_skipped)
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _execute_single_task(self, task: SEOTask):
        """Execute a single approved task."""
        from execution.router import ExecutionRouter
        from data.storage.database import Database
        db = Database()
        router = ExecutionRouter(db=db, shadow_mode=False)
        return await router.execute_task(task, self.business, self.business_id)

    async def _write_approval_queue(self, tasks: list[SEOTask]) -> None:
        """Write tasks to the approval queue JSON file."""
        path = _APPROVAL_DIR / f"{self.business_id}.json"
        existing = self._read_json_list(path)
        # Remove any previously pending entries for the same actions (dedup)
        existing_actions = {e.get("task_id") for e in existing}

        now = datetime.utcnow().isoformat()
        for task in tasks:
            task_id = uuid.uuid4().hex[:12]
            entry = {
                "task_id": task_id,
                "business_id": self.business_id,
                "status": "PENDING",
                "created_at": now,
                "impact": task.impact.value,
                "impact_score": task.impact_score,
                "confidence_score": task.confidence_score,
                "recommended_action": "approve" if task.impact == ImpactLevel.LOW else "review",
                "task": task.model_dump(),
            }
            existing.append(entry)

        self._write_json(path, existing)

    def _append_task_results(self, exec_results: list, cycle_result: CycleResult) -> None:
        """Persist today's execution results for stats."""
        path = _RESULTS_DIR / f"{self.business_id}_results.json"
        existing = self._read_json_list(path)
        now = datetime.utcnow().isoformat()
        for task, er in exec_results:
            existing.append({
                "date": now,
                "cycle_id": cycle_result.cycle_id,
                "task_id": er.task_id,
                "action": task.action[:100],
                "status": er.status.value,
                "type": task.type.value,
            })
        # Keep last 500 results
        self._write_json(path, existing[-500:])

    async def _send_webhook_alert(self, url: str, result: CycleResult) -> None:
        """POST a summary JSON to a Slack/Discord/Telegram-compatible webhook."""
        try:
            import urllib.request
            payload = {
                "text": (
                    f"SEO Engine Run Complete\n"
                    f"Business: {self.business.business_name}\n"
                    f"Mode: {self.mode}\n"
                    f"Tasks executed: {result.tasks_auto_executed}\n"
                    f"Tasks queued: {result.tasks_queued}\n"
                    f"Top action: {result.top_action or '(none)'}\n"
                    f"Cycle ID: {result.cycle_id}"
                )
            }
            body = json.dumps(payload).encode()
            req = urllib.request.Request(
                url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                log.info("autonomous.webhook_sent  status=%d", resp.status)
        except Exception as exc:
            log.warning("autonomous.webhook_fail  err=%s", exc)

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _state_path(self) -> Path:
        return _STATE_DIR / f"{self.business_id}.json"

    def _load_state(self) -> dict:
        path = self._state_path()
        if path.exists():
            try:
                return json.loads(path.read_text())
            except Exception:
                return {}
        return {}

    def _save_state(self, state: dict) -> None:
        path = self._state_path()
        try:
            self._write_json(path, state)
        except Exception as exc:
            log.warning("autonomous.state_save_fail  err=%s", exc)

    @staticmethod
    def _read_json_list(path: Path) -> list:
        if path.exists():
            try:
                data = json.loads(path.read_text())
                return data if isinstance(data, list) else []
            except Exception:
                return []
        return []

    @staticmethod
    def _write_json(path: Path, data) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2, default=str))


# =====================================================================
# Legacy AutonomousConfig / CycleResult shim (backward compat)
# =====================================================================

class AutonomousConfig(BaseModel):
    """Configuration for autonomous operation (legacy compat)."""
    business_id: str
    business: BusinessContext

    data_refresh_hours: int = 24
    analysis_hours: int = 24
    learning_days: int = 7
    evolution_days: int = 30

    auto_execute: bool = True
    shadow_mode: bool = False
    max_auto_executions_per_day: int = 5

    min_confidence_for_auto: float = 7.0
    min_score_for_auto: float = 6.0

    enable_demand_generation: bool = False
    enable_pressure_campaigns: bool = False
    enable_competitive_suppression: bool = False
    enable_entity_building: bool = True
    enable_cross_channel: bool = False
