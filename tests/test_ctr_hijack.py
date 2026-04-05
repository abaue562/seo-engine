"""Verify CTR detection, test management, and SERP hijack activation."""

from prediction.ctr import CTRDominator, CTRVariant, CTRTest
from prediction.serp_hijack import SERPHijacker


# --- CTR Detection ---

def test_detect_low_ctr():
    ctr = CTRDominator()
    data = [
        {"keyword": "plumber austin", "page": "/plumbing", "impressions": 500, "clicks": 10, "ctr": 0.02, "position": 8},
        {"keyword": "drain cleaning", "page": "/drain", "impressions": 50, "clicks": 5, "ctr": 0.10, "position": 5},  # too few impressions
        {"keyword": "water heater", "page": "/heater", "impressions": 300, "clicks": 5, "ctr": 0.017, "position": 12},
    ]
    results = ctr.detect_low_ctr_pages(data, min_impressions=100)
    assert len(results) == 2  # plumber austin + water heater
    assert results[0]["potential_clicks"] > 0


def test_detect_ignores_good_ctr():
    ctr = CTRDominator()
    data = [
        {"keyword": "test", "page": "/test", "impressions": 200, "clicks": 20, "ctr": 0.10, "position": 3},
    ]
    results = ctr.detect_low_ctr_pages(data)
    assert len(results) == 0


# --- CTR Test Management ---

def test_create_test():
    ctr = CTRDominator()
    variants = [
        CTRVariant(title="Test A", meta_description="Meta A", style="curiosity"),
        CTRVariant(title="Test B", meta_description="Meta B", style="urgency"),
    ]
    test = ctr.create_test("/page", "plumber austin", variants, baseline_ctr=0.025)
    assert test.status == "active"
    assert test.current_variant == 0
    assert len(test.variants) == 2


def test_rotation():
    ctr = CTRDominator()
    variants = [
        CTRVariant(title="A", meta_description="A", style="curiosity"),
        CTRVariant(title="B", meta_description="B", style="urgency"),
        CTRVariant(title="C", meta_description="C", style="benefit"),
    ]
    test = ctr.create_test("/page", "kw", variants, 0.03)

    # Rotate to B
    next_v = ctr.rotate(test)
    assert next_v is not None
    assert test.current_variant == 1

    # Rotate to C
    next_v = ctr.rotate(test)
    assert test.current_variant == 2

    # No more variants
    next_v = ctr.rotate(test)
    assert next_v is None
    assert test.status == "completed"


def test_pick_winner():
    ctr = CTRDominator()
    variants = [
        CTRVariant(title="A", meta_description="A", style="curiosity"),
        CTRVariant(title="B", meta_description="B", style="urgency"),
    ]
    test = ctr.create_test("/page", "kw", variants, baseline_ctr=0.03)

    ctr.record_result(test, 0, 0.032)  # barely better
    ctr.record_result(test, 1, 0.045)  # 50% better

    winner = ctr.pick_winner(test)
    assert winner is not None
    assert winner.style == "urgency"
    assert test.winner == 1


def test_no_winner_below_threshold():
    ctr = CTRDominator()
    variants = [CTRVariant(title="A", meta_description="A", style="curiosity")]
    test = ctr.create_test("/page", "kw", variants, baseline_ctr=0.05)
    ctr.record_result(test, 0, 0.051)  # only 2% better, below 10% threshold

    winner = ctr.pick_winner(test)
    assert winner is None


# --- SERP Hijack ---

def test_hijack_activation():
    h = SERPHijacker()
    assert h.should_activate(9) is True    # page 2
    assert h.should_activate(3) is False   # already top 3
    assert h.should_activate(25) is False  # too far
    assert h.should_activate(0, impressions=300) is True  # unranked but popular


if __name__ == "__main__":
    test_detect_low_ctr()
    test_detect_ignores_good_ctr()
    test_create_test()
    test_rotation()
    test_pick_winner()
    test_no_winner_below_threshold()
    test_hijack_activation()
    print("All CTR + SERP Hijack tests passed.")
