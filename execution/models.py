"""Execution layer data models — logs, results, rollback snapshots."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class ExecStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"        # Waiting for approval (ASSISTED mode)
    EXECUTING = "executing"
    SUCCESS = "success"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    SKIPPED = "skipped"      # Shadow mode or rate-limited


class ExecLog(BaseModel):
    """Immutable record of every execution attempt."""
    task_id: str
    action: str
    task_type: str
    execution_mode: str
    status: ExecStatus = ExecStatus.PENDING
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None
    before_state: dict = {}       # Snapshot before change
    after_state: dict = {}        # Snapshot after change
    error: str = ""
    verified: bool = False
    shadow_mode: bool = False     # True = logged but not actually executed


class ExecResult(BaseModel):
    """Result from a single task execution."""
    task_id: str
    status: ExecStatus
    output: dict = {}            # Handler-specific output (generated content, etc.)
    log: ExecLog | None = None
    rollback_available: bool = False


class RollbackSnapshot(BaseModel):
    """Stores the before-state for rollback."""
    task_id: str
    target: str
    before_state: dict
    created_at: datetime = Field(default_factory=datetime.utcnow)
