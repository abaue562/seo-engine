"""Verify prediction engine — page scoring, gap analysis, acceleration."""

from prediction.scoring import score_page, analyze_gap, build_timeline, should_accelerate
from prediction.acceleration import generate_acceleration_plan, detect_competitive_pressure


def test_high_content_predicts_better_rank():
    strong = score_page(
        url="/plumbing", keyword="plumber austin", current_rank=8,
        word_count=1500, keyword_in_title=True, keyword_in_h1=True,
        heading_count=6, backlink_count=10, domain_authority=30,
        ctr=0.05, days_since_update=10, competitor_avg_authority=25,
    )
    weak = score_page(
        url="/plumbing", keyword="plumber austin", current_rank=20,
        word_count=200, keyword_in_title=False, keyword_in_h1=False,
        heading_count=1, backlink_count=0, domain_authority=5,
        ctr=0.01, days_since_update=200, competitor_avg_authority=25,
    )
    assert strong.predicted_rank < weak.predicted_rank
    assert strong.composite > weak.composite


def test_predicted_rank_bounded():
    ps = score_page(
        url="/test", keyword="test", current_rank=1,
        word_count=2000, keyword_in_title=True, keyword_in_h1=True,
        heading_count=10, backlink_count=50, domain_authority=60,
        ctr=0.10, days_since_update=3,
    )
    assert ps.predicted_rank >= 1
    assert ps.predicted_rank <= 10


def test_gap_analysis():
    ps = score_page(
        url="/page", keyword="test", current_rank=12,
        word_count=400, keyword_in_title=True, keyword_in_h1=True,
        heading_count=3, backlink_count=1, domain_authority=15,
        ctr=0.02, days_since_update=100,
    )
    gap = analyze_gap(ps, {"avg_word_count": 1200, "avg_backlinks": 5, "page_backlinks": 1})
    assert gap.actions_needed  # should have actions
    assert gap.estimated_days_to_top3 > 0


def test_timeline():
    ps = score_page(url="/p", keyword="k", current_rank=8, word_count=1000,
                    keyword_in_title=True, keyword_in_h1=True, heading_count=5,
                    backlink_count=5, domain_authority=25, ctr=0.04, days_since_update=15)
    gap = analyze_gap(ps, {"avg_word_count": 1200, "avg_backlinks": 8})
    tl = build_timeline(ps, gap)
    assert tl.current_position == 8
    assert tl.time_to_rank_days > 0
    assert tl.acceleration_possible is True


def test_should_accelerate():
    ps5 = score_page(url="/p", keyword="k", current_rank=5, word_count=800,
                     keyword_in_title=True, keyword_in_h1=True, heading_count=4,
                     backlink_count=3, domain_authority=20, ctr=0.03, days_since_update=30)
    ps25 = score_page(url="/p", keyword="k", current_rank=25, word_count=800,
                      keyword_in_title=True, keyword_in_h1=True, heading_count=4,
                      backlink_count=3, domain_authority=20, ctr=0.03, days_since_update=30)
    assert should_accelerate(ps5) is True
    assert should_accelerate(ps25) is False


def test_acceleration_plan_generated():
    ps = score_page(url="/p", keyword="k", current_rank=9, word_count=500,
                    keyword_in_title=True, keyword_in_h1=True, heading_count=3,
                    backlink_count=1, domain_authority=15, ctr=0.02, days_since_update=60)
    gap = analyze_gap(ps, {"avg_word_count": 1200, "avg_backlinks": 5})
    plan = generate_acceleration_plan(ps, gap)
    assert plan is not None
    assert len(plan.actions) >= 2


def test_competitive_pressure_detection():
    pressures = detect_competitive_pressure(
        keyword="plumber austin",
        our_rank=5,
        competitor_changes=[
            {"name": "ABC Plumbing", "action": "gained 3 backlinks", "new_rank": 3},
        ],
    )
    assert len(pressures) == 1
    assert pressures[0].threat_level == "high"
    assert pressures[0].counter_actions


def test_no_pressure_when_we_lead():
    pressures = detect_competitive_pressure(
        keyword="plumber austin",
        our_rank=2,
        competitor_changes=[
            {"name": "ABC Plumbing", "action": "ranking jump", "new_rank": 7},
        ],
    )
    assert len(pressures) == 0


if __name__ == "__main__":
    test_high_content_predicts_better_rank()
    test_predicted_rank_bounded()
    test_gap_analysis()
    test_timeline()
    test_should_accelerate()
    test_acceleration_plan_generated()
    test_competitive_pressure_detection()
    test_no_pressure_when_we_lead()
    print("All prediction tests passed.")
