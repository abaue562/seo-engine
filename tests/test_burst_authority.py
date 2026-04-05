"""Verify Signal Burst Engine + Authority Gap Accelerator."""

from signals.burst import SignalBurstEngine, BurstIntensity, BurstPlan, BurstCampaign, BurstAction
from prediction.authority_gap import AuthorityGapAccelerator
from datetime import datetime, timedelta


# --- Signal Burst ---

def test_burst_eligibility():
    engine = SignalBurstEngine()
    assert engine.should_burst(6) is True   # In range
    assert engine.should_burst(3) is False  # Already top 3
    assert engine.should_burst(15) is False # Too far


def test_burst_intensity():
    engine = SignalBurstEngine()
    assert engine.get_intensity(5) == BurstIntensity.LOW
    assert engine.get_intensity(7) == BurstIntensity.MEDIUM
    assert engine.get_intensity(10) == BurstIntensity.HIGH


def test_create_campaign():
    engine = SignalBurstEngine()
    plan = BurstPlan(
        keyword="plumber austin",
        page_url="/plumbing",
        intensity=BurstIntensity.MEDIUM,
        duration_days=4,
        actions=[
            BurstAction(type="content_push", description="Blog post", channel="blog", day=1),
            BurstAction(type="gbp_activity", description="GBP post", channel="gbp", day=2),
        ],
    )
    campaign = engine.create_campaign("plumber austin", "/plumbing", "biz1", plan)
    assert campaign.status == "active"
    assert campaign.ends_at is not None
    assert campaign.cooldown_until is not None
    assert campaign.cooldown_until > campaign.ends_at


def test_today_actions():
    engine = SignalBurstEngine()
    plan = BurstPlan(
        keyword="kw", page_url="/p", duration_days=3,
        actions=[
            BurstAction(type="content_push", description="Day 1 blog", channel="blog", day=1),
            BurstAction(type="gbp_activity", description="Day 1 GBP", channel="gbp", day=1),
            BurstAction(type="content_push", description="Day 2 video", channel="tiktok", day=2),
        ],
    )
    campaign = engine.create_campaign("kw", "/p", "biz1", plan)
    campaign.started_at = datetime.utcnow()  # Today is day 1
    today = engine.get_today_actions(campaign)
    assert len(today) == 2  # Day 1 has 2 actions


def test_cooldown():
    engine = SignalBurstEngine()
    plan = BurstPlan(keyword="kw", page_url="/p", duration_days=3)
    campaign = engine.create_campaign("kw", "/p", "biz1", plan)
    assert engine.is_on_cooldown(campaign) is True  # Just started, cooldown active

    campaign.cooldown_until = datetime.utcnow() - timedelta(days=1)
    assert engine.is_on_cooldown(campaign) is False  # Cooldown expired


# --- Authority Gap ---

def test_gap_calculation():
    accel = AuthorityGapAccelerator()
    gap = accel.calculate_gap(
        keyword="plumber austin",
        our_da=20, our_links=15,
        competitor_name="ABC Plumbing",
        competitor_da=45, competitor_links=80,
    )
    assert gap.domain_gap == 25
    assert gap.link_gap == 65
    assert gap.severity == "critical"


def test_gap_severity_levels():
    accel = AuthorityGapAccelerator()

    critical = accel.calculate_gap("kw", 10, 5, "C", 35, 50)
    assert critical.severity == "critical"

    high = accel.calculate_gap("kw", 25, 20, "C", 38, 40)
    assert high.severity == "high"

    moderate = accel.calculate_gap("kw", 30, 25, "C", 37, 35)
    assert moderate.severity == "moderate"

    low = accel.calculate_gap("kw", 35, 30, "C", 38, 32)
    assert low.severity == "low"


def test_strategy_recommendation():
    accel = AuthorityGapAccelerator()
    gap = accel.calculate_gap("kw", 10, 5, "C", 40, 80)
    rec = accel.recommend_strategy(gap)
    assert "link building" in rec.lower() or "PR" in rec


if __name__ == "__main__":
    test_burst_eligibility()
    test_burst_intensity()
    test_create_campaign()
    test_today_actions()
    test_cooldown()
    test_gap_calculation()
    test_gap_severity_levels()
    test_strategy_recommendation()
    print("All burst + authority gap tests passed.")
