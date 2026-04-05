"""Page Scoring Engine — predicts ranking position from multi-factor analysis.

Formula:
  composite = content×0.25 + authority×0.25 + ctr×0.20 + freshness×0.15 - competition×0.15

predicted_rank = max(1, round(10 - composite))

A composite of 10 → rank 1. A composite of 3 → rank 7.
"""

from __future__ import annotations

import re
import logging
from datetime import datetime, timedelta

from prediction.models import PageScore, RankingGap, RankingTimeline

log = logging.getLogger(__name__)

# --- Weights ---
W_CONTENT = 0.25
W_AUTHORITY = 0.25
W_CTR = 0.20
W_FRESHNESS = 0.15
W_COMPETITION = 0.15


def score_page(
    url: str,
    keyword: str,
    current_rank: int = 0,
    word_count: int = 0,
    keyword_in_title: bool = False,
    keyword_in_h1: bool = False,
    heading_count: int = 0,
    backlink_count: int = 0,
    domain_authority: float = 0,
    ctr: float = 0,
    days_since_update: int = 0,
    competitor_avg_authority: float = 0,
    competitor_avg_words: int = 0,
) -> PageScore:
    """Score a page's ranking potential across 5 dimensions."""

    # Content score (0-10)
    content = 0.0
    if word_count >= 1500:
        content += 3
    elif word_count >= 800:
        content += 2
    elif word_count >= 300:
        content += 1
    if keyword_in_title:
        content += 2.5
    if keyword_in_h1:
        content += 2
    if heading_count >= 5:
        content += 1.5
    elif heading_count >= 3:
        content += 1
    # Compare to competitors
    if competitor_avg_words > 0 and word_count >= competitor_avg_words:
        content += 1
    content = min(10.0, content)

    # Authority score (0-10)
    authority = 0.0
    if domain_authority >= 50:
        authority += 4
    elif domain_authority >= 30:
        authority += 3
    elif domain_authority >= 15:
        authority += 2
    elif domain_authority >= 5:
        authority += 1
    if backlink_count >= 20:
        authority += 4
    elif backlink_count >= 10:
        authority += 3
    elif backlink_count >= 3:
        authority += 2
    elif backlink_count >= 1:
        authority += 1
    # Compare to competitors
    if competitor_avg_authority > 0 and domain_authority >= competitor_avg_authority:
        authority += 2
    authority = min(10.0, authority)

    # CTR score (0-10)
    ctr_score = 0.0
    if ctr >= 0.08:
        ctr_score = 9
    elif ctr >= 0.05:
        ctr_score = 7
    elif ctr >= 0.03:
        ctr_score = 5
    elif ctr >= 0.01:
        ctr_score = 3
    else:
        ctr_score = 1

    # Freshness score (0-10)
    if days_since_update <= 7:
        freshness = 10
    elif days_since_update <= 30:
        freshness = 8
    elif days_since_update <= 90:
        freshness = 6
    elif days_since_update <= 180:
        freshness = 4
    else:
        freshness = 2

    # Competition strength (0-10, higher = harder)
    competition = 0.0
    if competitor_avg_authority >= 50:
        competition = 9
    elif competitor_avg_authority >= 35:
        competition = 7
    elif competitor_avg_authority >= 20:
        competition = 5
    elif competitor_avg_authority >= 10:
        competition = 3
    else:
        competition = 1

    # Composite
    composite = (
        W_CONTENT * content
        + W_AUTHORITY * authority
        + W_CTR * ctr_score
        + W_FRESHNESS * freshness
        - W_COMPETITION * competition
    )
    composite = max(0, min(10, composite))

    predicted_rank = max(1, round(10 - composite))

    ps = PageScore(
        url=url,
        keyword=keyword,
        content_score=round(content, 1),
        authority_score=round(authority, 1),
        ctr_score=round(ctr_score, 1),
        freshness_score=round(freshness, 1),
        competition_strength=round(competition, 1),
        composite=round(composite, 2),
        predicted_rank=predicted_rank,
        current_rank=current_rank,
    )

    log.debug("predict.page  url=%s  kw=%s  composite=%.2f  predicted=#%d  current=#%d",
              url, keyword, composite, predicted_rank, current_rank)
    return ps


def analyze_gap(page_score: PageScore, serp_avg: dict) -> RankingGap:
    """Determine what's missing to reach top 3."""
    gap = RankingGap(
        url=page_score.url,
        keyword=page_score.keyword,
        current_rank=page_score.current_rank,
        predicted_rank=page_score.predicted_rank,
    )

    actions = []

    # Content gap
    avg_words = serp_avg.get("avg_word_count", 1200)
    if page_score.content_score < 7:
        words_needed = max(0, avg_words - serp_avg.get("page_word_count", 0))
        gap.content_gap = f"+{words_needed} words needed" if words_needed > 0 else "Content depth insufficient"
        actions.append(f"Expand content to {avg_words}+ words")

    # Link gap
    avg_links = serp_avg.get("avg_backlinks", 5)
    page_links = serp_avg.get("page_backlinks", 0)
    if page_score.authority_score < 7:
        links_needed = max(1, avg_links - page_links)
        gap.link_gap = f"+{links_needed} backlinks needed"
        actions.append(f"Build {links_needed} quality backlinks to this page")

    # CTR gap
    if page_score.ctr_score < 6:
        gap.ctr_gap = "Weak title/meta — rewrite for higher CTR"
        actions.append("A/B test title tag variations")

    # Freshness gap
    if page_score.freshness_score < 6:
        gap.freshness_gap = "Content stale — needs update"
        actions.append("Update page content with fresh information")

    gap.actions_needed = actions

    # Estimate time to top 3
    rank_distance = max(0, page_score.current_rank - 3)
    if rank_distance <= 3:
        gap.estimated_days_to_top3 = 14
    elif rank_distance <= 7:
        gap.estimated_days_to_top3 = 30
    elif rank_distance <= 15:
        gap.estimated_days_to_top3 = 60
    else:
        gap.estimated_days_to_top3 = 90

    # Confidence
    gap.confidence = round(min(1.0, page_score.composite / 10), 2)

    return gap


def build_timeline(page_score: PageScore, gap: RankingGap) -> RankingTimeline:
    """Predict ranking trajectory."""
    return RankingTimeline(
        keyword=page_score.keyword,
        url=page_score.url,
        current_position=page_score.current_rank,
        predicted_position=min(page_score.predicted_rank, page_score.current_rank),
        time_to_rank_days=gap.estimated_days_to_top3,
        confidence=gap.confidence,
        acceleration_possible=5 <= page_score.current_rank <= 15,
    )


def should_accelerate(page_score: PageScore) -> bool:
    """Returns True if this page is close enough to page 1 to warrant acceleration."""
    return 5 <= page_score.current_rank <= 15
