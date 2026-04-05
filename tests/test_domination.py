"""Verify Market Domination + Cross-Business Learning."""

from strategy.domination import (
    MarketDominator, MarketCluster, ClusterKeyword,
    DominationPlan, ContentAssignment, InternalLink,
)
from strategy.cross_business import CrossBusinessLearner, CrossBusinessReport


# --- Market Domination ---

def test_coverage_calculation():
    dom = MarketDominator()
    cluster = MarketCluster(
        core_keyword="plumber austin",
        supporting=[
            ClusterKeyword(keyword="emergency plumber austin"),
            ClusterKeyword(keyword="drain cleaning austin"),
            ClusterKeyword(keyword="water heater austin"),
        ],
        long_tail=[
            ClusterKeyword(keyword="24 hour plumber austin tx"),
            ClusterKeyword(keyword="best plumber near me austin"),
        ],
        total_keywords=6,  # core + 3 supporting + 2 long tail
    )

    # We have pages for 2 keywords
    coverage = dom.calculate_coverage(cluster, ["emergency-plumber-austin", "drain-cleaning-austin"])
    assert coverage > 0
    assert cluster.covered == 2


def test_prioritize_gaps():
    dom = MarketDominator()
    plan = DominationPlan(
        cluster=MarketCluster(core_keyword="test"),
        content_plan=[
            ContentAssignment(keyword="kw1", page_type="blog", priority="low"),
            ContentAssignment(keyword="kw2", page_type="service", priority="high"),
            ContentAssignment(keyword="kw3", page_type="guide", priority="medium"),
            ContentAssignment(keyword="kw4", page_type="service", priority="high"),
        ],
    )
    prioritized = dom.prioritize_gaps(plan)
    # High priority service pages should come first
    assert prioritized[0].priority == "high"
    assert prioritized[0].page_type == "service"
    # Blog low priority should be last
    assert prioritized[-1].priority == "low"


# --- Cross-Business Learning ---

def test_cross_business_report_structure():
    report = CrossBusinessReport(
        total_businesses=3,
        total_patterns=10,
        universal_winners=[
            {"pattern": "WEBSITE:meta_optimization", "avg_success_rate": 0.82, "avg_performance": 7.5,
             "businesses_tested": 3, "total_uses": 15},
        ],
        universal_losers=[
            {"pattern": "CONTENT:other", "avg_success_rate": 0.25, "businesses_tested": 2, "total_uses": 8},
        ],
    )
    assert report.total_businesses == 3
    assert len(report.universal_winners) == 1
    assert report.universal_winners[0]["avg_success_rate"] == 0.82


def test_cross_business_prompt_block():
    learner = CrossBusinessLearner()
    report = CrossBusinessReport(
        total_businesses=5,
        universal_winners=[
            {"pattern": "GBP:gbp_post", "avg_success_rate": 0.85, "avg_performance": 8.0,
             "businesses_tested": 4, "total_uses": 20},
        ],
        recommendations=["Boost 'GBP:gbp_post' across all businesses"],
    )
    block = learner.to_prompt_block(report)
    assert "5 businesses" in block
    assert "GBP:gbp_post" in block
    assert "85%" in block


if __name__ == "__main__":
    test_coverage_calculation()
    test_prioritize_gaps()
    test_cross_business_report_structure()
    test_cross_business_prompt_block()
    print("All domination + cross-business tests passed.")
