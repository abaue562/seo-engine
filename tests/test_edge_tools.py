"""Verify Rapid Update Engine + Competitor Reaction Engine."""

from datetime import datetime, timedelta
from prediction.rapid_update import RapidUpdateEngine, UpdateCycle, MAX_UPDATES_PER_PAGE
from prediction.competitor_reaction import CompetitorReactor, CompetitorMove


# --- Rapid Update: Detection ---

def test_find_stagnant_pages():
    engine = RapidUpdateEngine()
    rankings = {"plumber austin": 8, "drain cleaning": 3, "water heater": 12}
    stagnant = engine.find_stagnant_pages(rankings)
    # Only positions 5-15 should be included
    keywords = [s["keyword"] for s in stagnant]
    assert "plumber austin" in keywords
    assert "water heater" in keywords
    assert "drain cleaning" not in keywords  # position 3 is not stagnant-eligible


def test_stagnant_respects_update_date():
    engine = RapidUpdateEngine()
    rankings = {"plumber austin": 8}
    last_updated = {"plumber austin": datetime.utcnow() - timedelta(days=3)}  # Updated recently
    stagnant = engine.find_stagnant_pages(rankings, last_updated, stagnant_days=14)
    assert len(stagnant) == 0  # Not stagnant yet


# --- Rapid Update: Cycle management ---

def test_create_cycle():
    engine = RapidUpdateEngine()
    cycle = engine.create_cycle("/page", "plumber austin", "biz1", 9)
    assert cycle.status == "active"
    assert cycle.position_at_start == 9
    assert cycle.updates_applied == 0


def test_should_update():
    engine = RapidUpdateEngine()
    cycle = engine.create_cycle("/page", "kw", "biz1", 8)
    cycle.next_update = datetime.utcnow() - timedelta(hours=1)  # Past due
    assert engine.should_update(cycle) is True

    cycle.next_update = datetime.utcnow() + timedelta(days=3)  # Not yet
    assert engine.should_update(cycle) is False


def test_record_update():
    engine = RapidUpdateEngine()
    cycle = engine.create_cycle("/page", "kw", "biz1", 10)
    engine.record_update(cycle, ["content_addition", "faq_addition"])
    assert cycle.updates_applied == 1
    assert len(cycle.update_history) == 1
    assert cycle.next_update > datetime.utcnow()


def test_cycle_completes_after_max():
    engine = RapidUpdateEngine()
    cycle = engine.create_cycle("/page", "kw", "biz1", 10)
    for _ in range(MAX_UPDATES_PER_PAGE):
        engine.record_update(cycle, ["content_addition"])
    assert cycle.status == "completed"


def test_cycle_completes_on_top_3():
    engine = RapidUpdateEngine()
    cycle = engine.create_cycle("/page", "kw", "biz1", 8)
    engine.record_ranking_change(cycle, 3)
    assert cycle.status == "completed"


# --- Competitor Reaction: Detection ---

def test_detect_ranking_jump():
    reactor = CompetitorReactor()
    moves = reactor.detect_moves(
        our_rankings={"plumber austin": 5},
        previous_rankings={"plumber austin": 5},
        competitor_rankings={"plumber austin": {"ABC": 3}},
        previous_competitor_rankings={"plumber austin": {"ABC": 8}},
    )
    assert len(moves) >= 1
    assert moves[0].move_type in ("ranking_jump", "overtake")
    assert moves[0].threat_level == "critical"


def test_detect_overtake():
    reactor = CompetitorReactor()
    moves = reactor.detect_moves(
        our_rankings={"kw": 6},
        previous_rankings={"kw": 6},
        competitor_rankings={"kw": {"Rival": 4}},
        previous_competitor_rankings={"kw": {"Rival": 8}},
    )
    overtakes = [m for m in moves if m.move_type == "overtake"]
    assert len(overtakes) >= 1


def test_no_moves_when_stable():
    reactor = CompetitorReactor()
    moves = reactor.detect_moves(
        our_rankings={"kw": 3},
        previous_rankings={"kw": 3},
        competitor_rankings={"kw": {"Rival": 5}},
        previous_competitor_rankings={"kw": {"Rival": 5}},
    )
    assert len(moves) == 0


def test_no_moves_when_we_lead():
    reactor = CompetitorReactor()
    moves = reactor.detect_moves(
        our_rankings={"kw": 2},
        previous_rankings={"kw": 2},
        competitor_rankings={"kw": {"Rival": 8}},
        previous_competitor_rankings={"kw": {"Rival": 12}},
    )
    # Competitor improved but still behind us
    assert all(m.threat_level != "critical" for m in moves) or len(moves) == 0


if __name__ == "__main__":
    test_find_stagnant_pages()
    test_stagnant_respects_update_date()
    test_create_cycle()
    test_should_update()
    test_record_update()
    test_cycle_completes_after_max()
    test_cycle_completes_on_top_3()
    test_detect_ranking_jump()
    test_detect_overtake()
    test_no_moves_when_stable()
    test_no_moves_when_we_lead()
    print("All edge tool tests passed.")
