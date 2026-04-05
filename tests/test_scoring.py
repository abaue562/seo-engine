"""Verify the v2 scoring engine — scores, filters, ranks."""

from models.task import SEOTask, ImpactLevel, TaskType, ExecutionMode
from core.scoring.engine import score_task, score_and_rank, filter_garbage


def _make_task(
    impact="high", time="3 days", execution="Update meta title",
    impact_score=0, ease_score=0, speed_score=0, confidence_score=0,
):
    return SEOTask(
        action="Test action",
        target="homepage",
        why="Test reasoning",
        impact=ImpactLevel(impact),
        estimated_result="Increase traffic and conversions",
        time_to_result=time,
        execution=execution,
        type=TaskType.WEBSITE,
        execution_mode=ExecutionMode.MANUAL,
        impact_score=impact_score,
        ease_score=ease_score,
        speed_score=speed_score,
        confidence_score=confidence_score,
    )


def test_high_impact_beats_low():
    high = score_task(_make_task(impact="high"))
    low = score_task(_make_task(impact="low"))
    assert high.total_score > low.total_score


def test_fast_beats_slow():
    fast = score_task(_make_task(time="2 days"))
    slow = score_task(_make_task(time="3 months"))
    assert fast.total_score > slow.total_score


def test_simple_beats_complex():
    simple = score_task(_make_task(execution="Add a meta description"))
    hard = score_task(_make_task(execution="Full site redesign with developer migration and custom code rewrite"))
    assert simple.total_score > hard.total_score


def test_claude_scores_are_used():
    """When Claude provides scores, server should use them (clamped)."""
    t = score_task(_make_task(impact_score=9, ease_score=8, speed_score=7, confidence_score=8))
    assert t.impact_score == 9.0
    assert t.ease_score == 8.0
    assert t.speed_score == 7.0
    assert t.confidence_score == 8.0


def test_claude_scores_clamped():
    """Out-of-range Claude scores get clamped to 1-10."""
    t = score_task(_make_task(impact_score=15, ease_score=-3, speed_score=0, confidence_score=12))
    assert t.impact_score == 10.0
    assert t.ease_score == 1.0  # -3 clamped to 1
    assert t.confidence_score == 10.0


def test_total_score_formula():
    """total = impact*0.35 + ease*0.15 + speed*0.30 + confidence*0.20"""
    t = score_task(_make_task(impact_score=10, ease_score=10, speed_score=10, confidence_score=10))
    assert t.total_score == 10.0


def test_hard_filters_kill_garbage():
    """Only truly garbage tasks get killed (impact < 4 or confidence < 3)."""
    tasks = [
        score_task(_make_task(impact_score=9, confidence_score=8)),  # keep (primary)
        score_task(_make_task(impact_score=6, confidence_score=5)),  # keep (supporting)
        score_task(_make_task(impact_score=3, confidence_score=8)),  # kill (impact < 4)
        score_task(_make_task(impact_score=8, confidence_score=2)),  # kill (confidence < 3)
    ]
    kept, killed = filter_garbage(tasks)
    assert len(kept) == 2
    assert killed == 2


def test_score_and_rank_assigns_priority():
    tasks = [
        _make_task(impact_score=7, ease_score=7, speed_score=7, confidence_score=7),
        _make_task(impact_score=9, ease_score=9, speed_score=9, confidence_score=9),
        _make_task(impact_score=8, ease_score=8, speed_score=8, confidence_score=8),
    ]
    ranked, _ = score_and_rank(tasks)
    assert ranked[0].priority_rank == 1
    assert ranked[0].total_score >= ranked[1].total_score


def test_weekly_focus_max_5():
    tasks = [
        _make_task(impact_score=8, ease_score=8, speed_score=8, confidence_score=8)
        for _ in range(10)
    ]
    ranked, _ = score_and_rank(tasks)
    assert len(ranked) <= 5


def test_scores_bounded():
    t = score_task(_make_task())
    assert 1 <= t.impact_score <= 10
    assert 1 <= t.ease_score <= 10
    assert 1 <= t.speed_score <= 10
    assert 1 <= t.confidence_score <= 10
    assert 0 <= t.total_score <= 10


def test_tiered_roles():
    """Tasks get classified into primary/supporting/experimental."""
    from models.task import TaskRole
    primary = score_task(_make_task(impact_score=9, confidence_score=8))
    assert primary.role == TaskRole.PRIMARY

    supporting = score_task(_make_task(impact_score=7, confidence_score=6))
    assert supporting.role == TaskRole.SUPPORTING

    experimental = score_task(_make_task(impact_score=5, confidence_score=4))
    assert experimental.role == TaskRole.EXPERIMENTAL


def test_minimum_tasks_guaranteed():
    """Even with few inputs, we should get at least MIN_TASKS back."""
    tasks = [
        _make_task(impact_score=9, ease_score=8, speed_score=9, confidence_score=8),
        _make_task(impact_score=7, ease_score=7, speed_score=7, confidence_score=6),
        _make_task(impact_score=5, ease_score=6, speed_score=6, confidence_score=5),
        _make_task(impact_score=6, ease_score=5, speed_score=5, confidence_score=5),
    ]
    ranked, _ = score_and_rank(tasks)
    assert len(ranked) >= 3


if __name__ == "__main__":
    test_high_impact_beats_low()
    test_fast_beats_slow()
    test_simple_beats_complex()
    test_claude_scores_are_used()
    test_claude_scores_clamped()
    test_total_score_formula()
    test_hard_filters_kill_garbage()
    test_score_and_rank_assigns_priority()
    test_weekly_focus_max_5()
    test_scores_bounded()
    test_tiered_roles()
    test_minimum_tasks_guaranteed()
    print("All v3 scoring tests passed.")
