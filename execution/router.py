"""Execution Router — routes tasks to handlers, enforces safety, logs everything.

Flow:
  Task → Safety Gate → Handler (GBP/Website/Content/Authority) → Verify → Log

Supports three modes:
  - LIVE: actually executes
  - SHADOW: generates content but doesn't publish (logs as if it did)
  - APPROVAL_QUEUE: generates content, holds for user approval
"""

from __future__ import annotations

import uuid
import logging
from datetime import datetime

from execution.models import ExecResult, ExecLog, ExecStatus, RollbackSnapshot
from execution.safety import SafetyGate
from execution.verification import verify_execution
from data.storage.database import Database
from models.task import SEOTask, TaskType, ExecutionMode
from models.business import BusinessContext

log = logging.getLogger(__name__)


class ExecutionRouter:
    """Routes tasks to the correct handler and manages the full execution lifecycle."""

    def __init__(self, db: Database | None = None, shadow_mode: bool = False):
        self.db = db or Database()
        self.safety = SafetyGate()
        self.shadow_mode = shadow_mode
        self._handlers = {}  # Lazy-loaded

    def _get_handler(self, task_type: TaskType):
        """Lazy-load handlers to avoid importing anthropic at module level."""
        if task_type not in self._handlers:
            if task_type == TaskType.GBP:
                from execution.handlers.gbp import GBPHandler
                self._handlers[task_type] = GBPHandler()
            elif task_type == TaskType.WEBSITE:
                from execution.handlers.website import WebsiteHandler
                self._handlers[task_type] = WebsiteHandler()
            elif task_type == TaskType.CONTENT:
                from execution.handlers.content import ContentHandler
                self._handlers[task_type] = ContentHandler()
            elif task_type == TaskType.AUTHORITY:
                from execution.handlers.authority import AuthorityHandler
                self._handlers[task_type] = AuthorityHandler()
        return self._handlers.get(task_type)

    async def execute_task(
        self,
        task: SEOTask,
        business: BusinessContext,
        business_id: str,
        force_shadow: bool = False,
    ) -> ExecResult:
        """Execute a single task through the full pipeline."""
        task_id = uuid.uuid4().hex[:12]
        is_shadow = self.shadow_mode or force_shadow

        exec_log = ExecLog(
            task_id=task_id,
            action=task.action,
            task_type=task.type.value,
            execution_mode=task.execution_mode.value,
            shadow_mode=is_shadow,
        )

        # --- Safety check ---
        allowed, reason = self.safety.check(task, business_id)
        if not allowed:
            log.warning("exec.blocked  task=%s  reason=%s", task_id, reason)
            exec_log.status = ExecStatus.SKIPPED
            exec_log.error = reason
            exec_log.completed_at = datetime.utcnow()
            await self._save_log(exec_log)
            return ExecResult(task_id=task_id, status=ExecStatus.SKIPPED, output={"reason": reason}, log=exec_log)

        # --- Mode routing ---
        # is_shadow is True when: shadow_mode=True on router, force_shadow=True on call,
        # or task.execution_mode == SHADOW
        is_shadow = is_shadow or (task.execution_mode == ExecutionMode.SHADOW)

        if task.execution_mode == ExecutionMode.MANUAL:
            # MANUAL: just return instructions, don't execute
            exec_log.status = ExecStatus.SUCCESS
            exec_log.completed_at = datetime.utcnow()
            await self._save_log(exec_log)
            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={"type": "manual_instruction", "instruction": task.execution},
                log=exec_log,
            )

        if task.execution_mode == ExecutionMode.ASSISTED and not is_shadow:
            # ASSISTED: generate content but queue for approval
            result = await self._run_handler(task, business, task_id)
            result.status = ExecStatus.QUEUED
            exec_log.status = ExecStatus.QUEUED
            exec_log.after_state = result.output
            exec_log.completed_at = datetime.utcnow()
            await self._save_log(exec_log)
            await self._save_approval_queue(task_id, task, result, business_id)
            return result

        # --- AUTO or SHADOW execution ---
        exec_log.status = ExecStatus.EXECUTING

        if is_shadow:
            # Shadow mode: generate content, log it, but do NOT publish
            result = await self._run_handler(task, business, task_id)
            result.status = ExecStatus.SKIPPED
            exec_log.status = ExecStatus.SKIPPED
            exec_log.after_state = result.output
            exec_log.completed_at = datetime.utcnow()
            await self._save_log(exec_log)
            log.info("exec.shadow  task=%s  type=%s", task_id, task.type.value)
            return result

        # Live AUTO execution
        result = await self._run_handler(task, business, task_id)
        exec_log.status = result.status
        exec_log.after_state = result.output
        exec_log.completed_at = datetime.utcnow()

        # Record for rate limiting
        if result.status == ExecStatus.SUCCESS:
            self.safety.record_execution(task, business_id)

        # Verification
        if result.status == ExecStatus.SUCCESS:
            verified = await verify_execution(task, result)
            exec_log.verified = verified
            if not verified:
                log.warning("exec.verify_fail  task=%s", task_id)

        # Save rollback snapshot if applicable
        if result.rollback_available and exec_log.before_state:
            await self._save_rollback(task_id, task.target, exec_log.before_state)

        await self._save_log(exec_log)
        log.info("exec.done  task=%s  status=%s  type=%s", task_id, result.status.value, task.type.value)
        return result

    async def execute_batch(
        self,
        tasks: list[SEOTask],
        business: BusinessContext,
        business_id: str,
        force_shadow: bool = False,
    ) -> list[ExecResult]:
        """Execute a batch of tasks in priority order."""
        results = []
        for task in sorted(tasks, key=lambda t: t.priority_rank):
            result = await self.execute_task(task, business, business_id, force_shadow)
            results.append(result)
        return results

    async def approve_task(self, task_id: str, business_id: str) -> ExecResult:
        """Approve a queued ASSISTED task for execution."""
        queued = await self.db.query("approval_queue", {"task_id": task_id}, limit=1)
        if not queued:
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": "Task not found in queue"})

        # TODO: Re-execute the approved task
        log.info("exec.approved  task=%s", task_id)
        return ExecResult(task_id=task_id, status=ExecStatus.SUCCESS, output=queued[0])

    async def rollback(self, task_id: str) -> ExecResult:
        """Roll back a previously executed task using stored snapshot."""
        snapshots = await self.db.query("rollback_snapshots", {"task_id": task_id}, limit=1)
        if not snapshots:
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": "No rollback snapshot available"})

        snapshot = snapshots[0]
        log.info("exec.rollback  task=%s  target=%s", task_id, snapshot.get("target"))

        # TODO: Apply the before_state back to the target
        return ExecResult(
            task_id=task_id,
            status=ExecStatus.ROLLED_BACK,
            output={"restored": snapshot.get("before_state", {})},
        )

    # --- Internal helpers ---

    async def _run_handler(self, task: SEOTask, business: BusinessContext, task_id: str) -> ExecResult:
        handler = self._get_handler(task.type)
        if not handler:
            return ExecResult(
                task_id=task_id,
                status=ExecStatus.FAILED,
                output={"error": f"No handler for task type: {task.type.value}"},
            )
        return await handler.execute(task_id, task.action, task.target, task.execution, business)

    async def _save_log(self, exec_log: ExecLog) -> None:
        await self.db.upsert("execution_logs", exec_log.model_dump())

    async def _save_rollback(self, task_id: str, target: str, before_state: dict) -> None:
        snapshot = RollbackSnapshot(task_id=task_id, target=target, before_state=before_state)
        await self.db.upsert("rollback_snapshots", snapshot.model_dump())

    async def _save_approval_queue(self, task_id: str, task: SEOTask, result: ExecResult, business_id: str) -> None:
        await self.db.upsert("approval_queue", {
            "task_id": task_id,
            "business_id": business_id,
            "action": task.action,
            "type": task.type.value,
            "output": result.output,
            "created_at": datetime.utcnow().isoformat(),
        })
