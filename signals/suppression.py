"""Competitive Suppression Engine — systematically outpace competitors.

Don't just grow. Actively suppress:
  - Outpublish: more content on their keywords
  - Outlink: more backlinks on their ranking pages
  - Out-engage: more social + GBP activity

Result: competitors drop without knowing why.
"""

from __future__ import annotations

import logging
from signals.models import CompetitiveAction

log = logging.getLogger(__name__)


def analyze_suppression_opportunities(
    our_keywords: dict[str, int],      # keyword → our rank
    competitor_keywords: dict[str, dict],  # keyword → {competitor: rank}
    competitor_links: dict[str, int] = {},  # competitor → link count
    our_link_count: int = 0,
) -> list[CompetitiveAction]:
    """Identify keywords where we can realistically suppress competitors."""
    actions: list[CompetitiveAction] = []

    for keyword, our_rank in our_keywords.items():
        comp_data = competitor_keywords.get(keyword, {})
        if not comp_data:
            continue

        for competitor, their_rank in comp_data.items():
            # Only target competitors we're close to or slightly behind
            if our_rank <= 20 and their_rank < our_rank:
                rank_gap = our_rank - their_rank

                if rank_gap <= 5:
                    # Close gap — content velocity can close it
                    actions.append(CompetitiveAction(
                        competitor=competitor,
                        keyword=keyword,
                        our_rank=our_rank,
                        their_rank=their_rank,
                        action="outpublish",
                        detail=f"Create 3 supporting content pieces around '{keyword}'. "
                              f"Build topical cluster to surpass {competitor} (gap: {rank_gap} positions).",
                        priority="high",
                    ))

                if our_link_count < competitor_links.get(competitor, 0):
                    link_gap = competitor_links.get(competitor, 0) - our_link_count
                    actions.append(CompetitiveAction(
                        competitor=competitor,
                        keyword=keyword,
                        our_rank=our_rank,
                        their_rank=their_rank,
                        action="outlink",
                        detail=f"Build {min(link_gap + 2, 10)} backlinks targeting this keyword's landing page. "
                              f"{competitor} has ~{link_gap} more links.",
                        priority="high" if rank_gap <= 3 else "medium",
                    ))

                # Always out-engage when close
                if rank_gap <= 3:
                    actions.append(CompetitiveAction(
                        competitor=competitor,
                        keyword=keyword,
                        our_rank=our_rank,
                        their_rank=their_rank,
                        action="out-engage",
                        detail=f"Increase GBP posting frequency (daily for 2 weeks), "
                              f"publish 2 TikToks about this service, respond to all reviews. "
                              f"Signal freshness + activity to outpace {competitor}.",
                        priority="high",
                    ))

    # Sort by priority and closeness
    actions.sort(key=lambda a: (0 if a.priority == "high" else 1, a.our_rank - a.their_rank))

    log.info("suppression.analyzed  keywords=%d  actions=%d",
             len(our_keywords), len(actions))
    return actions


def suppression_to_prompt_block(actions: list[CompetitiveAction]) -> str:
    """Render suppression actions as agent context."""
    if not actions:
        return "COMPETITIVE SUPPRESSION: No suppression opportunities identified."

    lines = ["COMPETITIVE SUPPRESSION OPPORTUNITIES:"]
    for a in actions[:10]:
        lines.append(f"  [{a.priority.upper()}] {a.action} vs {a.competitor}")
        lines.append(f"    Keyword: '{a.keyword}' (us #{a.our_rank} vs them #{a.their_rank})")
        lines.append(f"    Action: {a.detail[:150]}")
    return "\n".join(lines)
