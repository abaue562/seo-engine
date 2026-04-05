"""Verify AI visibility scoring, llms.txt generation, and answer models."""

from ai_visibility.models import AnswerBlock, EntityPresence, AIVisibilityScore
from ai_visibility.scoring import score_visibility, score_to_prompt_block
from ai_visibility.llms_txt import generate_llms_txt
from ai_visibility.mentions import MentionEngine
from models.business import BusinessContext


def _make_biz():
    return BusinessContext(
        business_name="Test Plumbing",
        website="https://testplumbing.com",
        gbp_url="https://maps.google.com/test",
        primary_service="Plumbing",
        primary_city="Austin",
        service_areas=["Austin", "Round Rock"],
        secondary_services=["Drain Cleaning"],
        reviews_count=80,
        rating=4.7,
        years_active=6,
        primary_keywords=["plumber austin"],
    )


def test_visibility_score_basic():
    biz = _make_biz()
    score = score_visibility(biz, faq_count=5, schema_present=True, backlink_count=10)
    assert 0 <= score.composite <= 10
    assert score.answer_readiness > 0
    assert score.content_authority > 0


def test_visibility_score_low_for_empty_biz():
    biz = BusinessContext(
        business_name="New Co",
        website="",
        primary_service="Cleaning",
        primary_city="Nowhere",
    )
    score = score_visibility(biz)
    assert score.composite < 5


def test_visibility_score_high_for_strong_biz():
    biz = _make_biz()
    presences = [EntityPresence(platform=f"Platform {i}", status="present") for i in range(10)]
    score = score_visibility(biz, presences=presences, faq_count=15, schema_present=True,
                             mention_count=30, backlink_count=25)
    assert score.composite >= 5


def test_llms_txt_generation():
    biz = _make_biz()
    txt = generate_llms_txt(biz)
    assert "Test Plumbing" in txt
    assert "Plumbing" in txt
    assert "Austin" in txt
    assert "## Services" in txt
    assert "## Key Pages" in txt


def test_llms_txt_includes_rating():
    biz = _make_biz()
    txt = generate_llms_txt(biz)
    assert "4.7" in txt
    assert "80" in txt


def test_entity_presence_audit():
    biz = _make_biz()
    engine = MentionEngine.__new__(MentionEngine)
    presences = engine.audit_presence(biz)
    assert len(presences) > 0
    platforms = [p.platform for p in presences]
    assert "Google Business Profile" in platforms
    assert "Yelp" in platforms


def test_prompt_block():
    score = AIVisibilityScore(
        business_name="Test",
        answer_readiness=7, entity_saturation=5, mention_density=3, content_authority=6,
        composite=5.3,
    )
    block = score_to_prompt_block(score)
    assert "Test" in block
    assert "5.3" in block


if __name__ == "__main__":
    test_visibility_score_basic()
    test_visibility_score_low_for_empty_biz()
    test_visibility_score_high_for_strong_biz()
    test_llms_txt_generation()
    test_llms_txt_includes_rating()
    test_entity_presence_audit()
    test_prompt_block()
    print("All AI visibility tests passed.")
