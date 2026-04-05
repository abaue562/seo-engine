"""Cognitive Autonomous System — the full perception → plan → execute → reflect loop.

This replaces the simple pipeline with:
  1. PERCEIVE — update world model from data
  2. PLAN — create goal-oriented plans with dependencies
  3. EXECUTE — run plan steps (respecting dependencies + safety)
  4. REFLECT — analyze what worked, what failed
  5. LEARN — update patterns, strategy weights, episodic memory
  6. REPEAT
"""

from __future__ import annotations

import logging
from datetime import datetime
from pydantic import BaseModel, Field

from core.world_model.state import WorldModel, WorldState
from core.planner.engine import PlanningEngine, Plan
from core.events import detect_events_from_state, emit, SystemEvent
from learning.reflection.engine import ReflectionEngine, Reflection
from learning.patterns import PatternMemory
from strategy.evolution import StrategyEvolution
from data.storage.database import Database
from models.business import BusinessContext

log = logging.getLogger(__name__)


class CognitiveResult(BaseModel):
    """Output of one cognitive cycle."""
    cycle_id: str = ""
    phase_results: dict = {}
    plan: dict = {}
    steps_executed: int = 0
    events_detected: int = 0
    reflection: dict = {}
    world_state_summary: str = ""
    duration_seconds: float = 0
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class CognitiveSystem:
    """The full cognitive autonomous loop."""

    def __init__(self, db: Database | None = None):
        self.db = db or Database()
        self.world = WorldModel(self.db)
        self.planner = PlanningEngine()
        self.reflector = ReflectionEngine(self.db)
        self.patterns = PatternMemory(self.db)

    async def run_cycle(
        self,
        business: BusinessContext,
        business_id: str,
        goal_keyword: str | None = None,
        target_position: int = 3,
        execute: bool = False,
    ) -> CognitiveResult:
        """Run one full cognitive cycle: perceive → plan → execute → reflect → learn."""
        import uuid
        start = datetime.utcnow()
        cycle_id = uuid.uuid4().hex[:12]
        result = CognitiveResult(cycle_id=cycle_id)

        log.info("cognitive.start  id=%s  biz=%s  goal=%s", cycle_id, business.business_name, goal_keyword)

        # ---- 1. PERCEIVE — update world model ----
        state = await self.world.get_state(business_id)
        old_rankings = {k: v.position for k, v in state.keywords.items()}

        # Update from business context
        if business.current_rankings:
            changes = await self.world.update_rankings(business_id, business.current_rankings)
            result.phase_results["perception"] = {"ranking_changes": changes}

        # Detect events
        new_rankings = {k: v.position for k, v in state.keywords.items()}
        events = detect_events_from_state(old_rankings, new_rankings)
        for event in events:
            emit(event)
        result.events_detected = len(events)

        # ---- 2. PLAN — create goal-oriented plan ----
        target_kw = goal_keyword
        if not target_kw and business.primary_keywords:
            # Auto-select best keyword to attack (position 5-15, or best available)
            candidates = [
                (kw, pos) for kw, pos in business.current_rankings.items()
                if 5 <= pos <= 15
            ]
            if candidates:
                candidates.sort(key=lambda x: x[1])  # Closest to page 1
                target_kw = candidates[0][0]
            elif business.primary_keywords:
                target_kw = business.primary_keywords[0]

        if target_kw:
            current_pos = business.current_rankings.get(target_kw, 0)
            world_block = self.world.to_prompt_block(state)

            # Get past episodes for context
            episodes = await self.reflector.get_past_episodes(business_id)
            episode_block = self.reflector.episodes_to_prompt_block(episodes)

            plan = await self.planner.create_plan(
                keyword=target_kw,
                current_position=current_pos,
                target_position=target_position,
                business_name=business.business_name,
                city=business.primary_city,
                world_state_block=f"{world_block}\n\n{episode_block}",
            )
            result.plan = plan.model_dump()
            result.phase_results["planning"] = {
                "goal": plan.goal,
                "steps": len(plan.steps),
                "total_days": plan.total_days,
            }

            # ---- 3. EXECUTE — run ready steps ----
            if execute and plan.steps:
                execution_waves = self.planner.get_execution_order(plan)
                actions_taken = []

                for wave_num, wave in enumerate(execution_waves[:2]):  # Max 2 waves per cycle
                    for step in wave:
                        log.info("cognitive.execute  step=%s  action=%s", step.id, step.action[:60])
                        await self.world.record_action(business_id, target_kw, step.action)
                        actions_taken.append(step.action)
                        result.steps_executed += 1

                result.phase_results["execution"] = {
                    "waves_run": min(2, len(execution_waves)),
                    "steps_executed": result.steps_executed,
                    "actions": actions_taken,
                }

                # ---- 4. REFLECT ----
                reflection = await self.reflector.reflect(
                    business_id=business_id,
                    business_name=business.business_name,
                    goal=plan.goal,
                    actions_taken=actions_taken,
                    results=result.phase_results.get("execution", {}),
                    ranking_changes={kw: f"#{pos}" for kw, pos in new_rankings.items()},
                )
                result.reflection = {
                    "what_worked": reflection.what_worked,
                    "what_failed": reflection.what_failed,
                    "adjustments": reflection.adjustments,
                }

                # ---- 5. LEARN — update strategy weights ----
                try:
                    evolution = StrategyEvolution(self.patterns)
                    await evolution.evolve()
                    result.phase_results["learning"] = {"strategy_evolved": True}
                except Exception as e:
                    log.debug("cognitive.learn_skip  err=%s", e)

        # Record cycle
        await self.world.record_cycle(business_id)
        state = await self.world.get_state(business_id)
        result.world_state_summary = self.world.to_prompt_block(state)
        result.duration_seconds = (datetime.utcnow() - start).total_seconds()

        log.info("cognitive.done  id=%s  steps=%d  events=%d  duration=%.1fs",
                 cycle_id, result.steps_executed, result.events_detected, result.duration_seconds)

        return result
