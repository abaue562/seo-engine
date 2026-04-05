from __future__ import annotations
from enum import Enum
from pydantic import BaseModel, Field


class ImpactLevel(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class TaskType(str, Enum):
    GBP = "GBP"
    WEBSITE = "WEBSITE"
    CONTENT = "CONTENT"
    AUTHORITY = "AUTHORITY"


class ExecutionMode(str, Enum):
    AUTO = "AUTO"          # System can do it (publish, update, etc.)
    MANUAL = "MANUAL"      # User must act
    ASSISTED = "ASSISTED"  # User clicks, system helps


class TaskRole(str, Enum):
    PRIMARY = "primary"            # Main ranking push
    SUPPORTING = "supporting"      # Helps primary succeed
    EXPERIMENTAL = "experimental"  # Edge play, lower confidence


class SEOTask(BaseModel):
    """Single executable SEO action output by the brain."""

    action: str
    target: str
    why: str
    impact: ImpactLevel
    estimated_result: str
    time_to_result: str
    execution: str

    # Task routing
    type: TaskType = TaskType.WEBSITE
    execution_mode: ExecutionMode = ExecutionMode.MANUAL
    role: TaskRole = TaskRole.PRIMARY

    # 1-10 scores (Claude provides these, server validates)
    impact_score: float = 0.0
    ease_score: float = 0.0
    speed_score: float = 0.0
    confidence_score: float = 0.0

    # Computed
    total_score: float = 0.0
    priority_rank: int = 0


class TaskBatch(BaseModel):
    """A scored, filtered, ranked batch of tasks from one analysis run."""

    input_type: str
    tasks: list[SEOTask] = Field(default_factory=list)
    business_name: str = ""
    run_id: str = ""
    filtered_count: int = 0  # how many got killed by hard filters

    @property
    def weekly_focus(self) -> list[SEOTask]:
        """Top 3-5 tasks — the only ones that matter this week."""
        return [t for t in self.tasks if t.priority_rank <= 5]
