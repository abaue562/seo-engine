"""AI Visibility Scoring — measures how likely a business is to appear in AI recommendations.

Score dimensions:
  answer_readiness  (0-10) — Is content structured for AI extraction?
  entity_saturation (0-10) — Is the brand present across knowledge sources?
  mention_density   (0-10) — How often is the brand mentioned across the web?
  content_authority (0-10) — How trusted is the content (links, reviews, age)?

Composite = answer×0.30 + entity×0.25 + mentions×0.25 + authority×0.20
"""

from __future__ import annotations

import logging

from ai_visibility.models import AIVisibilityScore, EntityPresence
from models.business import BusinessContext

log = logging.getLogger(__name__)

W_ANSWER = 0.30
W_ENTITY = 0.25
W_MENTION = 0.25
W_AUTHORITY = 0.20


def score_visibility(
    business: BusinessContext,
    presences: list[EntityPresence] | None = None,
    faq_count: int = 0,
    schema_present: bool = False,
    mention_count: int = 0,
    backlink_count: int = 0,
) -> AIVisibilityScore:
    """Calculate AI visibility score for a business."""

    # Answer readiness
    answer = 0.0
    if faq_count >= 10:
        answer += 4
    elif faq_count >= 5:
        answer += 3
    elif faq_count >= 1:
        answer += 1
    if schema_present:
        answer += 3
    # Having a website at all
    if business.website:
        answer += 2
    # Reviews signal trust to AI
    if business.reviews_count >= 50:
        answer += 1
    answer = min(10.0, answer)

    # Entity saturation
    entity = 0.0
    if presences:
        present = sum(1 for p in presences if p.status == "present")
        total = len(presences)
        entity = min(10.0, (present / max(total, 1)) * 10)
    else:
        # Estimate from business data
        if business.gbp_url:
            entity += 3
        if business.website:
            entity += 2
        if business.reviews_count > 0:
            entity += 2
        entity = min(10.0, entity)

    # Mention density
    mention = 0.0
    if mention_count >= 50:
        mention = 9
    elif mention_count >= 20:
        mention = 7
    elif mention_count >= 10:
        mention = 5
    elif mention_count >= 5:
        mention = 3
    elif mention_count >= 1:
        mention = 1

    # Content authority
    authority = 0.0
    if backlink_count >= 50:
        authority += 4
    elif backlink_count >= 20:
        authority += 3
    elif backlink_count >= 5:
        authority += 2
    elif backlink_count >= 1:
        authority += 1
    if business.rating >= 4.5 and business.reviews_count >= 50:
        authority += 3
    elif business.rating >= 4.0:
        authority += 2
    if business.years_active >= 5:
        authority += 2
    elif business.years_active >= 2:
        authority += 1
    authority = min(10.0, authority)

    composite = round(
        W_ANSWER * answer
        + W_ENTITY * entity
        + W_MENTION * mention
        + W_AUTHORITY * authority,
        2,
    )

    score = AIVisibilityScore(
        business_name=business.business_name,
        answer_readiness=round(answer, 1),
        entity_saturation=round(entity, 1),
        mention_density=round(mention, 1),
        content_authority=round(authority, 1),
        composite=composite,
    )

    log.info("ai_visibility.score  biz=%s  answer=%.1f  entity=%.1f  mention=%.1f  auth=%.1f  composite=%.2f",
             business.business_name, answer, entity, mention, authority, composite)
    return score


def score_to_prompt_block(score: AIVisibilityScore) -> str:
    """Render AI visibility score as agent context."""
    lines = [
        f"AI VISIBILITY SCORE for {score.business_name}:",
        f"  Answer Readiness:  {score.answer_readiness}/10",
        f"  Entity Saturation: {score.entity_saturation}/10",
        f"  Mention Density:   {score.mention_density}/10",
        f"  Content Authority: {score.content_authority}/10",
        f"  COMPOSITE:         {score.composite}/10",
        "",
    ]

    if score.composite < 4:
        lines.append("  STATUS: LOW — business unlikely to appear in AI recommendations")
        lines.append("  PRIORITY: Build entity presence + answer-formatted content")
    elif score.composite < 7:
        lines.append("  STATUS: MODERATE — business may appear for some queries")
        lines.append("  PRIORITY: Expand mentions + optimize content for AI extraction")
    else:
        lines.append("  STATUS: STRONG — business likely to appear in AI recommendations")
        lines.append("  PRIORITY: Maintain + expand to new service keywords")

    return "\n".join(lines)
