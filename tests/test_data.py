"""Verify data freshness, event detection, and pipeline logic."""

from datetime import datetime, timedelta

from data.freshness import (
    DataSource, DataFreshnessReport, FreshnessLevel,
    evaluate_gsc, evaluate_gbp,
)
from data.events import (
    detect_ranking_changes, detect_review_changes, detect_traffic_changes,
    EventType,
)


# --- Freshness ---

def test_fresh_data():
    src = DataSource(source="GSC", fetched_at=datetime.utcnow(), record_count=100)
    src.evaluate_freshness(30)
    assert src.freshness == FreshnessLevel.FRESH
    assert src.confidence == "high"


def test_aging_data():
    src = DataSource(source="GSC", fetched_at=datetime.utcnow() - timedelta(days=45), record_count=100)
    src.evaluate_freshness(30)
    assert src.freshness == FreshnessLevel.AGING
    assert src.confidence == "medium"


def test_stale_data():
    src = DataSource(source="GSC", fetched_at=datetime.utcnow() - timedelta(days=90), record_count=100)
    src.evaluate_freshness(30)
    assert src.freshness == FreshnessLevel.STALE
    assert src.confidence == "low"


def test_freshness_report_penalty():
    report = DataFreshnessReport()
    report.add(evaluate_gsc(datetime.utcnow(), 100))  # fresh
    assert report.confidence_penalty() == 0

    report2 = DataFreshnessReport()
    report2.add(DataSource(
        source="GSC",
        fetched_at=datetime.utcnow() - timedelta(days=100),
        record_count=10,
        freshness=FreshnessLevel.STALE,
        confidence="low",
    ))
    assert report2.confidence_penalty() == 3


def test_freshness_prompt_block():
    report = DataFreshnessReport()
    report.add(evaluate_gsc(datetime.utcnow(), 50))
    block = report.to_prompt_block()
    assert "GSC" in block
    assert "fresh" in block


# --- Events ---

def test_ranking_drop_detected():
    events = detect_ranking_changes(
        current={"plumber nyc": 15},
        previous={"plumber nyc": 8},
    )
    assert len(events) == 1
    assert events[0].type == EventType.RANKING_DROP


def test_ranking_climb_ignored_below_threshold():
    events = detect_ranking_changes(
        current={"plumber nyc": 9},
        previous={"plumber nyc": 10},
    )
    assert len(events) == 0  # only 1 pos change, below threshold


def test_opportunity_alert():
    events = detect_ranking_changes(
        current={"plumber nyc": 12},
        previous={"plumber nyc": 18},
    )
    opportunities = [e for e in events if e.type == EventType.OPPORTUNITY_ALERT]
    assert len(opportunities) == 1
    assert opportunities[0].requires_agent_run


def test_review_surge():
    events = detect_review_changes(
        current_count=50, previous_count=42,
        current_rating=4.8, previous_rating=4.8,
    )
    assert any(e.type == EventType.REVIEW_SURGE for e in events)


def test_traffic_drop():
    events = detect_traffic_changes(current_clicks=80, previous_clicks=100)
    assert len(events) == 1
    assert events[0].type == EventType.TRAFFIC_DROP
    assert events[0].requires_agent_run


def test_no_events_on_stable_data():
    events = detect_ranking_changes(
        current={"plumber nyc": 5},
        previous={"plumber nyc": 5},
    )
    assert len(events) == 0


if __name__ == "__main__":
    test_fresh_data()
    test_aging_data()
    test_stale_data()
    test_freshness_report_penalty()
    test_freshness_prompt_block()
    test_ranking_drop_detected()
    test_ranking_climb_ignored_below_threshold()
    test_opportunity_alert()
    test_review_surge()
    test_traffic_drop()
    test_no_events_on_stable_data()
    print("All data layer tests passed.")
