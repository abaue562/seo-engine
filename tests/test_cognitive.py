"""Verify Cognitive Autonomous System — world model, planner, events, reflection."""

import asyncio
from datetime import datetime

from core.world_model.state import WorldModel, WorldState, KeywordState
from core.planner.engine import PlanningEngine, Plan, PlanStep
from core.events import detect_events_from_state, SystemEvent
from learning.reflection.engine import ReflectionEngine, Episode


# --- World Model ---

def test_world_state_init():
    state = WorldState(business_id="test")
    assert state.cycle_count == 0
    assert state.total_actions_taken == 0


def test_world_ranking_update():
    loop = asyncio.new_event_loop()
    world = WorldModel()
    changes = loop.run_until_complete(world.update_rankings("test", {"plumber austin": 8, "drain cleaning": 5}))
    assert len(changes) == 2

    # Update again — should detect trends
    changes2 = loop.run_until_complete(world.update_rankings("test", {"plumber austin": 5, "drain cleaning": 5}))
    state = loop.run_until_complete(world.get_state("test"))
    assert state.keywords["plumber austin"].trend == "improving"
    assert state.keywords["drain cleaning"].trend == "stable"
    loop.close()


def test_world_prompt_block():
    world = WorldModel()
    state = WorldState(
        business_id="test",
        cycle_count=3,
        total_actions_taken=10,
        keywords={
            "kw1": KeywordState(keyword="kw1", position=5, previous_position=8, trend="improving"),
            "kw2": KeywordState(keyword="kw2", position=12, previous_position=10, trend="declining"),
        },
    )
    block = world.to_prompt_block(state)
    assert "IMPROVING" in block
    assert "DECLINING" in block
    assert "kw1" in block


# --- Events ---

def test_detect_ranking_drop():
    events = detect_events_from_state(
        {"kw": 5},
        {"kw": 10},
    )
    drops = [e for e in events if e.type == "ranking_drop"]
    assert len(drops) == 1
    assert drops[0].severity in ("critical", "high")


def test_detect_goal_reached():
    events = detect_events_from_state(
        {"kw": 6},
        {"kw": 2},
    )
    goals = [e for e in events if e.type == "goal_reached"]
    assert len(goals) == 1


def test_detect_stagnation():
    events = detect_events_from_state(
        {"kw": 8},
        {"kw": 8},
    )
    stag = [e for e in events if e.type == "stagnation"]
    assert len(stag) == 1


def test_no_events_outside_range():
    events = detect_events_from_state(
        {"kw": 25},
        {"kw": 25},
    )
    # Position 25 is not in stagnation range (5-15)
    assert len(events) == 0


# --- Planner ---

def test_plan_step_dependencies():
    plan = Plan(
        goal="test",
        keyword="kw",
        steps=[
            PlanStep(id="s1", action="create page", type="WEBSITE", target="/page"),
            PlanStep(id="s2", action="add links", type="WEBSITE", target="/page", depends_on=["s1"]),
            PlanStep(id="s3", action="build backlinks", type="AUTHORITY", target="/page", depends_on=["s1"]),
        ],
    )
    planner = PlanningEngine()

    # Only s1 should be ready initially
    ready = planner.get_ready_steps(plan, set())
    assert len(ready) == 1
    assert ready[0].id == "s1"

    # After s1 completes, s2 and s3 should be ready (parallel)
    ready2 = planner.get_ready_steps(plan, {"s1"})
    assert len(ready2) == 2


def test_execution_waves():
    plan = Plan(
        goal="test",
        keyword="kw",
        steps=[
            PlanStep(id="s1", action="step 1", type="WEBSITE", target="/"),
            PlanStep(id="s2", action="step 2", type="WEBSITE", target="/", depends_on=["s1"]),
            PlanStep(id="s3", action="step 3", type="AUTHORITY", target="/", depends_on=["s1"]),
            PlanStep(id="s4", action="step 4", type="SIGNAL", target="/", depends_on=["s2", "s3"]),
        ],
    )
    planner = PlanningEngine()
    waves = planner.get_execution_order(plan)
    assert len(waves) == 3  # Wave 1: [s1], Wave 2: [s2, s3], Wave 3: [s4]
    assert len(waves[0]) == 1
    assert len(waves[1]) == 2
    assert len(waves[2]) == 1


# --- Episodic Memory ---

def test_episode_creation():
    ep = Episode(
        episode_id="ep1",
        business_id="biz1",
        goal="rank 'plumber austin' in top 3",
        actions_taken=["optimized page", "built links"],
        ranking_change=4,
        success=True,
        lessons=["Title changes moved rankings fastest"],
    )
    assert ep.success is True
    assert len(ep.lessons) == 1


def test_episodes_prompt_block():
    reflector = ReflectionEngine()
    episodes = [
        Episode(episode_id="1", goal="rank kw1", success=True, actions_taken=["action1"],
                ranking_change=3, lessons=["Links worked"]),
        Episode(episode_id="2", goal="rank kw2", success=False, actions_taken=["action2"],
                lessons=["Content wasn't deep enough"]),
    ]
    block = reflector.episodes_to_prompt_block(episodes)
    assert "SUCCESS" in block
    assert "FAILED" in block
    assert "Links worked" in block


if __name__ == "__main__":
    test_world_state_init()
    test_world_ranking_update()
    test_world_prompt_block()
    test_detect_ranking_drop()
    test_detect_goal_reached()
    test_detect_stagnation()
    test_no_events_outside_range()
    test_plan_step_dependencies()
    test_execution_waves()
    test_episode_creation()
    test_episodes_prompt_block()
    print("All cognitive system tests passed.")
