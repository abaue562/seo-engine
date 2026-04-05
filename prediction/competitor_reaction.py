"""Competitor Reaction Engine — auto-detect and counter competitor moves.

Monitors competitors and triggers immediate counter-actions when:
  - Competitor gains backlinks to a competing page
  - Competitor updates their ranking page content
  - Competitor's ranking jumps for your target keyword
  - Competitor launches new service page in your area

Response time target: < 24 hours from detection to counter-action.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from core.claude import call_claude

log = logging.getLogger(__name__)


class CompetitorMove(BaseModel):
    """A detected competitor action that needs a response."""
    competitor: str
    move_type: str          # ranking_jump / content_update / new_backlinks / new_page
    keyword: str
    detail: str
    their_position: int = 0
    our_position: int = 0
    threat_level: str = "high"   # critical / high / medium
    detected_at: datetime = Field(default_factory=datetime.utcnow)


class CounterAction(BaseModel):
    """A specific counter-action to respond to a competitor move."""
    action: str
    target: str
    why: str
    urgency: str = "immediate"  # immediate / within_48h / within_week
    execution: str


class ReactionPlan(BaseModel):
    """Full reaction plan for a competitor move."""
    move: CompetitorMove
    counter_actions: list[CounterAction] = Field(default_factory=list)
    total_actions: int = 0


REACTION_PROMPT = """You are the Competitor Reaction Agent. A competitor just made a move. Generate immediate counter-actions.

OUR BUSINESS: {business_name} ({city})
OUR KEYWORD: {keyword} (our position: #{our_position})

COMPETITOR MOVE:
Competitor: {competitor}
Move type: {move_type}
Detail: {detail}
Their position: #{their_position}

Generate 2-4 IMMEDIATE counter-actions to prevent losing ground or overtake.

Rules:
- Actions must be executable within 48 hours
- Prioritize speed over perfection
- Focus on the SPECIFIC keyword being threatened
- Include exact content/changes where possible
- Each action must directly counter the competitor's advantage

Return ONLY JSON:
{{
  "counter_actions": [
    {{
      "action": "specific counter-action",
      "target": "exact page or asset",
      "why": "why this counters the competitor's move",
      "urgency": "immediate | within_48h | within_week",
      "execution": "step-by-step with specific content"
    }}
  ]
}}"""


class CompetitorReactor:
    """Detects competitor moves and generates counter-actions."""

    # ----- Detection -----

    def detect_moves(
        self,
        our_rankings: dict[str, int],
        previous_rankings: dict[str, int],
        competitor_rankings: dict[str, dict[str, int]],
        previous_competitor_rankings: dict[str, dict[str, int]] | None = None,
    ) -> list[CompetitorMove]:
        """Compare current vs previous data to detect competitor moves."""
        moves: list[CompetitorMove] = []
        prev_comp = previous_competitor_rankings or {}

        for keyword, our_pos in our_rankings.items():
            comp_data = competitor_rankings.get(keyword, {})
            prev_comp_data = prev_comp.get(keyword, {})

            for competitor, their_pos in comp_data.items():
                prev_their_pos = prev_comp_data.get(competitor, their_pos + 5)

                # Ranking jump: competitor improved significantly
                improvement = prev_their_pos - their_pos
                if improvement >= 3 and their_pos <= our_pos:
                    moves.append(CompetitorMove(
                        competitor=competitor,
                        move_type="ranking_jump",
                        keyword=keyword,
                        detail=f"Jumped {improvement} positions (#{prev_their_pos} -> #{their_pos})",
                        their_position=their_pos,
                        our_position=our_pos,
                        threat_level="critical" if their_pos < our_pos else "high",
                    ))

                # Competitor now outranks us when they didn't before
                if their_pos < our_pos and prev_their_pos >= our_pos:
                    moves.append(CompetitorMove(
                        competitor=competitor,
                        move_type="overtake",
                        keyword=keyword,
                        detail=f"Now outranking us (them #{their_pos} vs us #{our_pos})",
                        their_position=their_pos,
                        our_position=our_pos,
                        threat_level="critical",
                    ))

        moves.sort(key=lambda m: {"critical": 0, "high": 1, "medium": 2}.get(m.threat_level, 3))
        log.info("competitor_reaction.detected  moves=%d", len(moves))
        return moves

    # ----- Reaction -----

    async def generate_reaction(
        self,
        move: CompetitorMove,
        business_name: str,
        city: str,
    ) -> ReactionPlan:
        """Generate counter-actions for a detected competitor move."""
        prompt = REACTION_PROMPT.format(
            business_name=business_name,
            city=city,
            keyword=move.keyword,
            our_position=move.our_position,
            competitor=move.competitor,
            move_type=move.move_type,
            detail=move.detail,
            their_position=move.their_position,
        )

        try:
            raw = call_claude(
                prompt,
                system="You are a competitive SEO strategist. Return ONLY valid JSON.",
                max_tokens=2048,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            data, _ = json.JSONDecoder().raw_decode(raw)
            raw_actions = data.get("counter_actions", [])
            # Coerce execution to string if Claude returns a list
            for a in raw_actions:
                if isinstance(a.get("execution"), list):
                    a["execution"] = " ".join(str(x) for x in a["execution"])
            actions = [CounterAction(**a) for a in raw_actions]

            plan = ReactionPlan(
                move=move,
                counter_actions=actions,
                total_actions=len(actions),
            )

            log.info("competitor_reaction.plan  competitor=%s  keyword=%s  actions=%d",
                     move.competitor, move.keyword, len(actions))
            return plan

        except Exception as e:
            log.error("competitor_reaction.fail  err=%s", e)
            return ReactionPlan(move=move)

    async def react_to_all(
        self,
        moves: list[CompetitorMove],
        business_name: str,
        city: str,
        max_reactions: int = 3,
    ) -> list[ReactionPlan]:
        """Generate reaction plans for the most critical moves."""
        plans = []
        for move in moves[:max_reactions]:
            plan = await self.generate_reaction(move, business_name, city)
            if plan.counter_actions:
                plans.append(plan)
        return plans
