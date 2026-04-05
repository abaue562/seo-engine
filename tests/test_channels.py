"""Verify cross-channel distribution, flywheel, and content models."""

from channels.models import ContentBundle, ContentPerformance, DistributionSchedule
from channels.distribution import DistributionEngine, DistributionResult
from channels.flywheel import ContentFlywheel


def _make_bundle(keyword="plumber austin"):
    return ContentBundle(
        keyword=keyword,
        city="Austin",
        service="Plumbing",
        service_page={"title": "Test", "content": "..."},
        blog_article={"title": "Blog", "content": "..."},
        tiktok_script={"hook": "Stop!", "body": "...", "cta": "Call now"},
        gbp_post={"text": "Post text"},
        social_post={"text": "Social text", "hashtags": ["#plumber"]},
    )


def test_bundle_format_count():
    b = _make_bundle()
    assert b.format_count == 5


def test_distribution_rate_limit():
    import asyncio
    loop = asyncio.new_event_loop()
    engine = DistributionEngine(schedule=DistributionSchedule(tiktok_per_day=1))
    results1 = loop.run_until_complete(engine.distribute(_make_bundle("kw1")))
    tiktok1 = [r for r in results1 if r.channel == "tiktok"]
    assert tiktok1[0].status == "ready"

    results2 = loop.run_until_complete(engine.distribute(_make_bundle("kw2")))
    tiktok2 = [r for r in results2 if r.channel == "tiktok"]
    assert tiktok2[0].status == "rate_limited"
    loop.close()


def test_flywheel_amplifies_winners():
    fw = ContentFlywheel()
    perfs = [
        ContentPerformance(keyword="plumber austin", seo_impact=8, social_engagement=3, traffic_generated=200, conversions=8),
    ]
    actions = fw.evaluate(perfs)
    assert len(actions) >= 2
    keywords_targeted = {a.keyword for a in actions}
    assert "plumber austin" in keywords_targeted


def test_flywheel_deprioritizes_losers():
    fw = ContentFlywheel()
    perfs = [
        ContentPerformance(keyword="bad keyword", seo_impact=1, social_engagement=1, traffic_generated=5, conversions=0, composite_score=1),
    ]
    actions = fw.evaluate(perfs)
    deprioritize = [a for a in actions if "deprioritize" in a.action.lower()]
    assert len(deprioritize) >= 1


def test_flywheel_social_to_seo():
    fw = ContentFlywheel()
    perfs = [
        ContentPerformance(keyword="viral topic", seo_impact=3, social_engagement=9, traffic_generated=50),
    ]
    actions = fw.evaluate(perfs)
    seo_boost = [a for a in actions if a.channel == "seo"]
    assert len(seo_boost) >= 1


if __name__ == "__main__":
    test_bundle_format_count()
    test_distribution_rate_limit()
    test_flywheel_amplifies_winners()
    test_flywheel_deprioritizes_losers()
    test_flywheel_social_to_seo()
    print("All channel tests passed.")
