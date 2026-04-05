"""Ranking Acceleration + Competitive Pressure Detection.

When a page is close to page 1 (positions 5-15):
  → increase content updates, link building, internal linking

When a competitor gains ground:
  → trigger immediate counter-actions
"""

from __future__ import annotations

import logging
from datetime import datetime
from pydantic import BaseModel, Field

from prediction.models import PageScore, RankingGap
from models.task import SEOTask, TaskType, ExecutionMode, ImpactLevel

log = logging.getLogger(__name__)


class AccelerationPlan(BaseModel):
    """Aggressive action plan for pages close to page 1."""
    url: str
    keyword: str
    current_rank: int
    actions: list[SEOTask] = Field(default_factory=list)
    reason: str = ""


class CompetitivePressure(BaseModel):
    """Detected competitive movement requiring counter-action."""
    competitor: str
    action_detected: str         # "gained backlinks", "updated page", "ranking jump"
    our_keyword: str
    our_current_rank: int
    threat_level: str = "high"   # high / medium
    counter_actions: list[str] = Field(default_factory=list)
    detected_at: datetime = Field(default_factory=datetime.utcnow)


def generate_acceleration_plan(page_score: PageScore, gap: RankingGap) -> AccelerationPlan | None:
    """Generate acceleration actions for pages in positions 5-15."""
    if not (5 <= page_score.current_rank <= 15):
        return None

    plan = AccelerationPlan(
        url=page_score.url,
        keyword=page_score.keyword,
        current_rank=page_score.current_rank,
        reason=f"Page ranking #{page_score.current_rank} — within striking distance of top 3",
    )

    # Content acceleration
    if page_score.content_score < 8:
        plan.actions.append(SEOTask(
            action=f"Expand and update content on {page_score.url}",
            target=page_score.url,
            why=f"Content score {page_score.content_score}/10 — {gap.content_gap}",
            impact=ImpactLevel.HIGH,
            type=TaskType.WEBSITE,
            execution_mode=ExecutionMode.ASSISTED,
            estimated_result=f"Improve content score to 8+, push ranking from #{page_score.current_rank} toward top 3",
            time_to_result="7 days",
            execution="Expand page content to match or exceed top-ranking competitors. Add FAQ, detailed sections, local signals.",
            impact_score=9, ease_score=7, speed_score=8, confidence_score=8,
        ))

    # Link acceleration
    if page_score.authority_score < 7:
        plan.actions.append(SEOTask(
            action=f"Build targeted backlinks to {page_score.url}",
            target=page_score.url,
            why=f"Authority score {page_score.authority_score}/10 — {gap.link_gap}",
            impact=ImpactLevel.HIGH,
            type=TaskType.AUTHORITY,
            execution_mode=ExecutionMode.ASSISTED,
            estimated_result=f"Close authority gap, accelerate ranking from #{page_score.current_rank}",
            time_to_result="14 days",
            execution="Build 2-3 quality backlinks directly to this page. Focus on relevant local and industry sites.",
            impact_score=9, ease_score=5, speed_score=6, confidence_score=7,
        ))

    # CTR acceleration
    if page_score.ctr_score < 7:
        plan.actions.append(SEOTask(
            action=f"A/B test title tag for {page_score.keyword}",
            target=page_score.url,
            why=f"CTR score {page_score.ctr_score}/10 — optimizing title could boost clicks significantly at position #{page_score.current_rank}",
            impact=ImpactLevel.HIGH,
            type=TaskType.WEBSITE,
            execution_mode=ExecutionMode.ASSISTED,
            estimated_result="10-30% CTR improvement → more clicks → ranking signal boost",
            time_to_result="3 days",
            execution="Generate 3 title variations. Test curiosity, urgency, and benefit angles.",
            impact_score=8, ease_score=9, speed_score=9, confidence_score=7,
        ))

    # Internal linking boost
    plan.actions.append(SEOTask(
        action=f"Add internal links pointing to {page_score.url}",
        target=page_score.url,
        why=f"Internal link equity helps push page from #{page_score.current_rank} into top 3",
        impact=ImpactLevel.MEDIUM,
        type=TaskType.WEBSITE,
        execution_mode=ExecutionMode.AUTO,
        estimated_result="Improved page authority through internal link equity",
        time_to_result="3 days",
        execution="Find 3-5 existing pages that mention related topics. Add contextual links to this page.",
        impact_score=7, ease_score=9, speed_score=9, confidence_score=8,
    ))

    log.info("acceleration.plan  url=%s  keyword=%s  rank=#%d  actions=%d",
             page_score.url, page_score.keyword, page_score.current_rank, len(plan.actions))

    return plan


def detect_competitive_pressure(
    keyword: str,
    our_rank: int,
    competitor_changes: list[dict],
) -> list[CompetitivePressure]:
    """Detect competitive movements that threaten our rankings."""
    pressures = []

    for change in competitor_changes:
        competitor = change.get("name", "Unknown")
        action = change.get("action", "")
        new_rank = change.get("new_rank", 0)

        # Threat: competitor jumped ahead of us
        if new_rank > 0 and new_rank < our_rank:
            counter = []
            if "backlink" in action.lower():
                counter.append("Match or exceed competitor's link building for this keyword")
            if "update" in action.lower() or "content" in action.lower():
                counter.append("Update and expand our page content immediately")
            if "ranking" in action.lower():
                counter.append("Run full optimization cycle on our ranking page")

            pressures.append(CompetitivePressure(
                competitor=competitor,
                action_detected=action,
                our_keyword=keyword,
                our_current_rank=our_rank,
                threat_level="high" if new_rank <= 3 else "medium",
                counter_actions=counter or ["Run competitive analysis and respond"],
            ))
            log.warning("pressure.detected  competitor=%s  action=%s  their_rank=#%d  our_rank=#%d",
                        competitor, action, new_rank, our_rank)

    return pressures
