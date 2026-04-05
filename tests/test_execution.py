"""Verify execution safety, routing, and learning system."""

from datetime import datetime
from models.task import SEOTask, TaskType, ExecutionMode, ImpactLevel
from execution.safety import SafetyGate
from execution.models import ExecStatus
from learning.attribution import attribute_result, TaskOutcome
from learning.patterns import ActionPattern, KILL_THRESHOLD


def _make_task(
    action="Update meta title",
    task_type=TaskType.WEBSITE,
    mode=ExecutionMode.AUTO,
):
    return SEOTask(
        action=action,
        target="/homepage",
        why="Test",
        impact=ImpactLevel.HIGH,
        estimated_result="More traffic",
        time_to_result="3 days",
        execution=action,
        type=task_type,
        execution_mode=mode,
        impact_score=8, ease_score=8, speed_score=9, confidence_score=8,
        total_score=8.2, priority_rank=1,
    )


# --- Safety Gate ---

def test_safe_task_allowed():
    gate = SafetyGate()
    task = _make_task()
    allowed, reason = gate.check(task, "biz1")
    assert allowed is True


def test_dangerous_auto_blocked():
    gate = SafetyGate()
    task = _make_task(action="Delete all old blog posts", mode=ExecutionMode.AUTO)
    allowed, reason = gate.check(task, "biz1")
    assert allowed is False
    assert "delete" in reason.lower()


def test_dangerous_manual_allowed():
    gate = SafetyGate()
    task = _make_task(action="Delete old blog posts", mode=ExecutionMode.MANUAL)
    allowed, reason = gate.check(task, "biz1")
    assert allowed is True  # MANUAL is always allowed


def test_rate_limit():
    gate = SafetyGate()
    task = _make_task(task_type=TaskType.GBP)
    # Execute up to limit
    for _ in range(3):
        gate.record_execution(task, "biz1")
    # 4th should be blocked
    allowed, reason = gate.check(task, "biz1")
    assert allowed is False
    assert "rate limit" in reason.lower()


# --- Attribution ---

def test_attribution_positive():
    outcome = attribute_result(
        task_id="t1", action="Update title", task_type="WEBSITE", target="/page",
        executed_at=datetime(2026, 3, 1),
        before={"position": 11, "clicks": 50, "ctr": 0.02},
        after={"position": 5, "clicks": 80, "ctr": 0.04},
    )
    assert outcome.ranking_change == 6  # improved by 6 positions
    assert outcome.traffic_change == 30
    assert outcome.performance_score > 0
    assert outcome.success is True


def test_attribution_negative():
    outcome = attribute_result(
        task_id="t2", action="Change meta", task_type="WEBSITE", target="/page",
        executed_at=datetime(2026, 3, 1),
        before={"position": 5, "clicks": 100, "ctr": 0.05},
        after={"position": 8, "clicks": 70, "ctr": 0.03},
    )
    assert outcome.ranking_change == -3  # dropped
    assert outcome.traffic_change == -30


# --- Pattern Memory ---

def test_pattern_update():
    pattern = ActionPattern(pattern="WEBSITE:meta_optimization", task_type="WEBSITE")

    good = TaskOutcome(
        task_id="t1", action="Update title", task_type="WEBSITE", target="/page",
        executed_at=datetime(2026, 3, 1),
        performance_score=7.5, success=True, time_to_effect_days=10,
    )
    pattern.update(good)
    assert pattern.times_used == 1
    assert pattern.successes == 1
    assert pattern.success_rate == 1.0


def test_pattern_auto_kill():
    pattern = ActionPattern(pattern="CONTENT:other", task_type="CONTENT")

    fail = TaskOutcome(
        task_id="t", action="test", task_type="CONTENT", target="/x",
        executed_at=datetime(2026, 3, 1),
        performance_score=2.0, success=False,
    )

    # 5 failures should trigger kill
    for _ in range(5):
        pattern.update(fail)

    assert pattern.is_killed is True
    assert pattern.success_rate < KILL_THRESHOLD


if __name__ == "__main__":
    test_safe_task_allowed()
    test_dangerous_auto_blocked()
    test_dangerous_manual_allowed()
    test_rate_limit()
    test_attribution_positive()
    test_attribution_negative()
    test_pattern_update()
    test_pattern_auto_kill()
    print("All execution + learning tests passed.")
