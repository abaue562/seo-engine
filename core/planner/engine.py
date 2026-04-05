"""Planning Engine — generates multi-step plans with dependencies.

Instead of flat task lists, creates structured plans:
  - Goal-oriented (e.g., "rank 'plumber austin' in top 3")
  - Steps have dependencies (can't build links before page exists)
  - Parallel execution where possible
  - Adapts based on world state
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from core.claude import call_claude
from core.world_model.state import WorldState

log = logging.getLogger(__name__)


class PlanStep(BaseModel):
    id: str
    action: str
    type: str                      # GBP / WEBSITE / CONTENT / AUTHORITY / SIGNAL
    target: str
    depends_on: list[str] = []     # Step IDs that must complete first
    parallel_with: list[str] = []  # Steps that can run simultaneously
    estimated_days: int = 1
    execution: str = ""


class Plan(BaseModel):
    plan_id: str = ""
    goal: str
    keyword: str
    current_position: int = 0
    target_position: int = 3
    steps: list[PlanStep] = Field(default_factory=list)
    total_days: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)


PLANNER_PROMPT = """You are the Planning Engine. Create a multi-step plan with dependencies to achieve a specific ranking goal.

GOAL: Rank "{keyword}" in top {target_position}
Current position: #{current_position}
Business: {business_name} ({city})

WORLD STATE:
{world_state}

Create a structured plan where:
- Each step has an ID (step_1, step_2, etc.)
- Steps can depend on other steps (can't build links before page exists)
- Steps that don't depend on each other can run in parallel
- Include estimated days for each step

Return ONLY JSON:
{{
  "goal": "rank '{keyword}' in top {target_position}",
  "steps": [
    {{
      "id": "step_1",
      "action": "specific action",
      "type": "GBP | WEBSITE | CONTENT | AUTHORITY | SIGNAL",
      "target": "specific target",
      "depends_on": [],
      "parallel_with": ["step_2"],
      "estimated_days": 1,
      "execution": "specific steps"
    }}
  ],
  "total_days": 0
}}"""


class PlanningEngine:
    """Creates goal-oriented, dependency-aware plans."""

    async def create_plan(
        self,
        keyword: str,
        current_position: int,
        target_position: int,
        business_name: str,
        city: str,
        world_state_block: str = "",
    ) -> Plan:
        """Generate a multi-step plan for a ranking goal."""
        import uuid

        prompt = PLANNER_PROMPT.format(
            keyword=keyword,
            current_position=current_position,
            target_position=target_position,
            business_name=business_name,
            city=city,
            world_state=world_state_block or "No prior state available.",
        )

        try:
            raw = call_claude(
                prompt,
                system="You are a strategic planner. Return ONLY valid JSON.",
                max_tokens=2048,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            data = json.loads(raw)
            steps = [PlanStep(**s) for s in data.get("steps", [])]

            plan = Plan(
                plan_id=uuid.uuid4().hex[:12],
                goal=data.get("goal", f"Rank '{keyword}' in top {target_position}"),
                keyword=keyword,
                current_position=current_position,
                target_position=target_position,
                steps=steps,
                total_days=data.get("total_days", sum(s.estimated_days for s in steps)),
            )

            log.info("planner.created  keyword=%s  steps=%d  days=%d",
                     keyword, len(steps), plan.total_days)
            return plan

        except Exception as e:
            log.error("planner.fail  keyword=%s  err=%s", keyword, e)
            return Plan(goal=f"Rank '{keyword}' in top {target_position}", keyword=keyword)

    def get_ready_steps(self, plan: Plan, completed: set[str]) -> list[PlanStep]:
        """Get steps whose dependencies are all met — ready to execute."""
        ready = []
        for step in plan.steps:
            if step.id in completed:
                continue
            if all(dep in completed for dep in step.depends_on):
                ready.append(step)
        return ready

    def get_execution_order(self, plan: Plan) -> list[list[PlanStep]]:
        """Get steps grouped by execution wave (parallel groups)."""
        completed: set[str] = set()
        waves: list[list[PlanStep]] = []

        while len(completed) < len(plan.steps):
            wave = self.get_ready_steps(plan, completed)
            if not wave:
                break  # Prevent infinite loop on circular deps
            waves.append(wave)
            for step in wave:
                completed.add(step.id)

        return waves
