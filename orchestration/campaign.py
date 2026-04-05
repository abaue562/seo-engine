"""Campaign Orchestrator — the single brain that coordinates everything.

One campaign = one keyword goal. The orchestrator:
  1. Creates a phased plan (foundation → distribution → amplification → reinforcement)
  2. Assigns personas to channels
  3. Sequences execution over days
  4. Measures results per phase
  5. Reallocates resources based on what's working

This replaces running tools manually — the orchestrator decides what, when, who, and where.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from core.claude import call_claude
from models.business import BusinessContext

log = logging.getLogger(__name__)


class CampaignPhase(BaseModel):
    name: str                          # foundation / distribution / amplification / reinforcement
    day_start: int = 0
    day_end: int = 0
    actions: list[dict] = []           # {action, channel, persona, content_type}
    status: str = "pending"            # pending / active / completed


class Campaign(BaseModel):
    campaign_id: str = ""
    goal: str                          # "rank 'keyword' in top 3"
    keyword: str
    duration_days: int = 21
    phases: list[CampaignPhase] = Field(default_factory=list)
    metrics: dict = {}
    status: str = "planned"            # planned / active / completed / paused
    created_at: datetime = Field(default_factory=datetime.utcnow)


class CampaignResult(BaseModel):
    campaign: Campaign
    execution_summary: dict = {}
    next_actions: list[str] = []
    reallocation: dict = {}


CAMPAIGN_PROMPT = """21-day campaign to rank "{keyword}" top 3 for {business_name} in {city} (currently #{position}).

4 phases, max 3 actions each. Assign persona + channel per action.

Personas: Practical Homeowner, Design Enthusiast, Local Expert, Smart Home Tech
Channels: blog, tiktok, reddit, quora, medium, gbp, social

JSON only:
{{"phases":[{{"name":"foundation","day_start":1,"day_end":3,"actions":[{{"action":"...","channel":"...","persona":"...","day":1}}]}},{{"name":"distribution","day_start":3,"day_end":7,"actions":[...]}},{{"name":"amplification","day_start":7,"day_end":14,"actions":[...]}},{{"name":"reinforcement","day_start":14,"day_end":21,"actions":[...]}}]}}"""


class CampaignOrchestrator:
    """Plans, assigns, executes, and adapts keyword campaigns."""

    async def create_campaign(
        self,
        keyword: str,
        business: BusinessContext,
        duration_days: int = 21,
    ) -> Campaign:
        """Generate a full phased campaign."""
        import uuid

        position = business.current_rankings.get(keyword, 0)
        prompt = CAMPAIGN_PROMPT.format(
            keyword=keyword,
            business_name=business.business_name,
            city=business.primary_city,
            service=business.primary_service,
            position=position or "unranked",
        )

        # Retry up to 2 times with shorter prompt on failure
        for attempt in range(2):
            try:
                raw = call_claude(
                    prompt,
                    system="Campaign strategist. Return ONLY valid JSON. Keep it concise.",
                    max_tokens=2048,
                )

                # Handle CLI errors
                if "Error:" in raw and len(raw) < 100:
                    log.warning("campaign.cli_error  attempt=%d  raw=%s", attempt + 1, raw)
                    continue

                if raw.startswith("```"):
                    raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
                start = raw.find("{")
                if start > 0:
                    raw = raw[start:]

                data = json.loads(raw)

                phases = []
                for p in data.get("phases", []):
                    phases.append(CampaignPhase(
                        name=p.get("name", ""),
                        day_start=p.get("day_start", 0),
                        day_end=p.get("day_end", 0),
                        actions=p.get("actions", []),
                    ))

                if not phases:
                    log.warning("campaign.empty_phases  attempt=%d", attempt + 1)
                    continue

                campaign = Campaign(
                    campaign_id=uuid.uuid4().hex[:12],
                    goal=f"Rank '{keyword}' in top 3",
                    keyword=keyword,
                    duration_days=duration_days,
                    phases=phases,
                    status="planned",
                )

                total_actions = sum(len(p.actions) for p in phases)
                log.info("campaign.created  keyword=%s  phases=%d  actions=%d",
                         keyword, len(phases), total_actions)
                return campaign

            except Exception as e:
                log.error("campaign.attempt_fail  attempt=%d  keyword=%s  err=%s", attempt + 1, keyword, e)

        # All attempts failed — return a default campaign
        log.warning("campaign.fallback  keyword=%s", keyword)
        return Campaign(
            campaign_id=uuid.uuid4().hex[:8],
            goal=f"Rank '{keyword}' in top 3",
            keyword=keyword,
            duration_days=duration_days,
            phases=[
                CampaignPhase(name="foundation", day_start=1, day_end=3, actions=[
                    {"action": "Optimize/create main service page", "channel": "blog", "persona": "Local Expert", "day": 1},
                    {"action": "Optimize GBP listing", "channel": "gbp", "persona": "Local Expert", "day": 2},
                ]),
                CampaignPhase(name="distribution", day_start=3, day_end=7, actions=[
                    {"action": "Publish supporting blog post", "channel": "blog", "persona": "Design Enthusiast", "day": 4},
                    {"action": "Social media push", "channel": "social", "persona": "Practical Homeowner", "day": 5},
                ]),
                CampaignPhase(name="amplification", day_start=7, day_end=14, actions=[
                    {"action": "TikTok content", "channel": "tiktok", "persona": "Design Enthusiast", "day": 8},
                    {"action": "Build 2-3 backlinks", "channel": "directories", "persona": "Local Expert", "day": 10},
                ]),
                CampaignPhase(name="reinforcement", day_start=14, day_end=21, actions=[
                    {"action": "Update page content + FAQ", "channel": "blog", "persona": "Local Expert", "day": 15},
                    {"action": "CTR title test", "channel": "blog", "persona": "Practical Homeowner", "day": 18},
                ]),
            ],
            status="planned",
        )

    def get_today_actions(self, campaign: Campaign, day: int) -> list[dict]:
        """Get all actions scheduled for a specific day."""
        actions = []
        for phase in campaign.phases:
            if phase.day_start <= day <= phase.day_end:
                for action in phase.actions:
                    if action.get("day", phase.day_start) == day:
                        actions.append(action)
        return actions

    def get_current_phase(self, campaign: Campaign, day: int) -> CampaignPhase | None:
        """Get the active phase for today."""
        for phase in campaign.phases:
            if phase.day_start <= day <= phase.day_end:
                return phase
        return None

    def reallocate(self, campaign: Campaign, metrics: dict) -> dict:
        """Adjust campaign based on performance."""
        realloc = {}

        ranking_change = metrics.get("ranking_change", 0)
        traffic_change = metrics.get("traffic_change", 0)

        if ranking_change < 0:
            realloc["action"] = "increase_authority"
            realloc["reason"] = "Ranking dropped — add more backlinks + content"
        elif ranking_change == 0 and campaign.status == "active":
            realloc["action"] = "increase_distribution"
            realloc["reason"] = "No movement — push more content across channels"
        elif ranking_change >= 3:
            realloc["action"] = "maintain"
            realloc["reason"] = "Good progress — maintain current pace"

        if traffic_change < -10:
            realloc["ctr_action"] = "run_ctr_test"
            realloc["ctr_reason"] = "Traffic declining — test new titles"

        return realloc

    def campaign_summary(self, campaign: Campaign) -> dict:
        """Get a summary of the campaign state."""
        total_actions = sum(len(p.actions) for p in campaign.phases)
        channels = set()
        personas = set()
        for phase in campaign.phases:
            for action in phase.actions:
                channels.add(action.get("channel", ""))
                personas.add(action.get("persona", ""))

        return {
            "campaign_id": campaign.campaign_id,
            "goal": campaign.goal,
            "keyword": campaign.keyword,
            "status": campaign.status,
            "duration_days": campaign.duration_days,
            "total_phases": len(campaign.phases),
            "total_actions": total_actions,
            "channels": list(channels),
            "personas": list(personas),
            "phases": [
                {
                    "name": p.name,
                    "days": f"{p.day_start}-{p.day_end}",
                    "actions": len(p.actions),
                    "action_list": p.actions,
                }
                for p in campaign.phases
            ],
        }
