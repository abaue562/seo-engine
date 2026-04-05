"""Verify signal generation, pressure campaigns, behavioral signals, suppression, and strategy evolution."""

from signals.models import PressureCampaign, CompetitiveAction
from signals.pressure import PressureEngine
from signals.behavioral import generate_behavioral_signals
from signals.suppression import analyze_suppression_opportunities
from strategy.evolution import StrategyWeights


# --- Pressure ---

def test_pressure_campaign_standard():
    engine = PressureEngine()
    campaign = engine.plan_campaign(
        keyword="plumber austin",
        cluster_keywords=["emergency plumber", "drain cleaning"],
        business=None,  # Not needed for planning
    )
    assert campaign.total_assets > 0
    assert "service_page" in campaign.assets
    assert "tiktok_scripts" in campaign.assets


def test_pressure_campaign_blitz():
    engine = PressureEngine()
    standard = engine.plan_campaign("kw", [], None, "standard")
    blitz = engine.plan_campaign("kw", [], None, "blitz")
    assert blitz.total_assets > standard.total_assets
    assert blitz.total_assets == standard.total_assets * 3


def test_pressure_tracking():
    engine = PressureEngine()
    campaign = engine.plan_campaign("kw", [], None)
    progress = engine.track_progress(campaign, {"service_page": 1, "blog_articles": 2})
    assert progress["overall_pct"] > 0
    assert progress["channels"]["service_page"]["completed"] == 1


# --- Behavioral ---

def test_behavioral_thin_content():
    pages = [{"url": "/page", "word_count": 200, "internal_links": [], "has_schema": False}]
    signals = generate_behavioral_signals(pages, "plumber austin")
    tactics = [s.tactic for s in signals]
    assert "content_depth" in tactics
    assert "internal_link_loop" in tactics


def test_behavioral_good_page_still_gets_signals():
    pages = [{"url": "/page", "word_count": 2000, "internal_links": list(range(10)), "has_schema": True}]
    signals = generate_behavioral_signals(pages, "plumber austin")
    # Should still get CTA optimization at minimum
    assert len(signals) >= 1


# --- Suppression ---

def test_suppression_detects_close_competitors():
    actions = analyze_suppression_opportunities(
        our_keywords={"plumber austin": 6},
        competitor_keywords={"plumber austin": {"ABC Plumbing": 3}},
        competitor_links={"ABC Plumbing": 20},
        our_link_count=10,
    )
    assert len(actions) >= 2  # outpublish + outlink at minimum
    assert any(a.action == "outpublish" for a in actions)


def test_suppression_ignores_distant():
    actions = analyze_suppression_opportunities(
        our_keywords={"plumber austin": 50},  # Too far to compete
        competitor_keywords={"plumber austin": {"ABC": 3}},
    )
    assert len(actions) == 0


# --- Strategy Weights ---

def test_strategy_weights_defaults():
    w = StrategyWeights()
    assert w.content_optimization == 1.0
    assert w.link_building == 1.0


def test_strategy_weights_customizable():
    w = StrategyWeights(content_optimization=1.5, link_building=0.5)
    assert w.content_optimization == 1.5
    assert w.link_building == 0.5


if __name__ == "__main__":
    test_pressure_campaign_standard()
    test_pressure_campaign_blitz()
    test_pressure_tracking()
    test_behavioral_thin_content()
    test_behavioral_good_page_still_gets_signals()
    test_suppression_detects_close_competitors()
    test_suppression_ignores_distant()
    test_strategy_weights_defaults()
    test_strategy_weights_customizable()
    print("All signal + strategy tests passed.")
