"""Signal Burst Engine — controlled spikes of activity to force ranking jumps.

Instead of steady growth, creates 3-5 day bursts of:
  - Content publishing (blog + video + GBP posts)
  - Traffic injection (social → page)
  - Engagement boosting (CTR + dwell time)

Trigger: keyword stuck at positions 4-10 with optimized page ready for final push.
Result: +2-5 positions in 3-10 days.

Safety: max 1 burst per keyword per 14 days.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from enum import Enum
from pydantic import BaseModel, Field

from core.claude import call_claude

log = logging.getLogger(__name__)

BURST_COOLDOWN_DAYS = 14


class BurstIntensity(str, Enum):
    LOW = "low"          # 2-3 content pieces, light traffic push
    MEDIUM = "medium"    # 4-5 content pieces, moderate traffic
    HIGH = "high"        # 6-8 content pieces, aggressive traffic + engagement


class BurstAction(BaseModel):
    type: str              # content_push / traffic_generation / engagement_boost / gbp_activity
    description: str
    content: str = ""      # Ready-to-deploy content if applicable
    channel: str = ""      # blog / tiktok / gbp / social / email
    day: int = 1           # Which day of the burst


class BurstPlan(BaseModel):
    keyword: str
    page_url: str
    intensity: BurstIntensity = BurstIntensity.MEDIUM
    duration_days: int = 3
    actions: list[BurstAction] = Field(default_factory=list)
    expected_effect: str = ""
    total_content_pieces: int = 0


class BurstCampaign(BaseModel):
    campaign_id: str = ""
    keyword: str
    page_url: str
    business_id: str = ""
    plan: BurstPlan | None = None
    status: str = "planned"     # planned / active / completed / cooldown
    started_at: datetime | None = None
    ends_at: datetime | None = None
    cooldown_until: datetime | None = None
    actions_completed: int = 0
    results: dict = {}


BURST_PROMPT = """You are the Signal Burst Agent. Create a {duration}-day burst campaign to push a keyword from #{position} into top 3.

Keyword: {keyword}
Page: {page_url}
Business: {business_name}
City: {city}
Current position: #{position}
Intensity: {intensity}

Generate a day-by-day burst plan. For {intensity} intensity, create {content_count} content pieces spread across {duration} days.

Content types to use:
- Blog post targeting keyword variation (500-800 words)
- TikTok/Reels script with CTA driving to page
- GBP post mentioning service + city
- Social media post with link to page
- Email snippet for existing customers

For each action, include ACTUAL ready-to-deploy content (write the blog intro, the TikTok script, the GBP post text).

Return ONLY JSON:
{{
  "duration_days": {duration},
  "actions": [
    {{
      "type": "content_push | traffic_generation | engagement_boost | gbp_activity",
      "description": "what to do",
      "content": "actual ready-to-use content",
      "channel": "blog | tiktok | gbp | social | email",
      "day": 1
    }}
  ],
  "expected_effect": "specific expected ranking movement"
}}"""


class SignalBurstEngine:
    """Creates controlled spikes of activity to break through ranking plateaus."""

    def should_burst(self, position: int, page_optimized: bool = True) -> bool:
        """Determine if a keyword qualifies for a signal burst."""
        return 4 <= position <= 10 and page_optimized

    def get_intensity(self, position: int, gap_to_top3: int = 0) -> BurstIntensity:
        """Determine burst intensity based on position."""
        if position <= 5:
            return BurstIntensity.LOW    # Close — light push
        elif position <= 8:
            return BurstIntensity.MEDIUM  # Moderate push
        else:
            return BurstIntensity.HIGH    # Aggressive push needed

    async def plan_burst(
        self,
        keyword: str,
        page_url: str,
        position: int,
        business_name: str,
        city: str,
        intensity: BurstIntensity | None = None,
    ) -> BurstPlan:
        """Generate a full burst plan via Claude."""
        if intensity is None:
            intensity = self.get_intensity(position)

        duration = {"low": 3, "medium": 4, "high": 5}[intensity.value]
        content_count = {"low": 3, "medium": 5, "high": 8}[intensity.value]

        prompt = BURST_PROMPT.format(
            keyword=keyword,
            page_url=page_url,
            business_name=business_name,
            city=city,
            position=position,
            intensity=intensity.value,
            duration=duration,
            content_count=content_count,
        )

        try:
            raw = call_claude(
                prompt,
                system="You are a content marketing strategist. Return ONLY valid JSON.",
                max_tokens=4096,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            start = raw.find('{')
            if start > 0:
                raw = raw[start:]
            data, _ = json.JSONDecoder().raw_decode(raw)
            actions = [BurstAction(**a) for a in data.get("actions", [])]

            plan = BurstPlan(
                keyword=keyword,
                page_url=page_url,
                intensity=intensity,
                duration_days=data.get("duration_days", duration),
                actions=actions,
                expected_effect=data.get("expected_effect", ""),
                total_content_pieces=len(actions),
            )

            log.info("burst.planned  keyword=%s  intensity=%s  actions=%d  days=%d",
                     keyword, intensity.value, len(actions), plan.duration_days)
            return plan

        except Exception as e:
            log.error("burst.plan_fail  keyword=%s  err=%s", keyword, e)
            return BurstPlan(keyword=keyword, page_url=page_url, intensity=intensity)

    def create_campaign(
        self,
        keyword: str,
        page_url: str,
        business_id: str,
        plan: BurstPlan,
    ) -> BurstCampaign:
        """Create a burst campaign from a plan."""
        import uuid
        now = datetime.utcnow()
        campaign = BurstCampaign(
            campaign_id=uuid.uuid4().hex[:12],
            keyword=keyword,
            page_url=page_url,
            business_id=business_id,
            plan=plan,
            status="active",
            started_at=now,
            ends_at=now + timedelta(days=plan.duration_days),
            cooldown_until=now + timedelta(days=plan.duration_days + BURST_COOLDOWN_DAYS),
        )
        log.info("burst.campaign_created  id=%s  keyword=%s  ends=%s",
                 campaign.campaign_id, keyword, campaign.ends_at)
        return campaign

    def get_today_actions(self, campaign: BurstCampaign) -> list[BurstAction]:
        """Get actions scheduled for today."""
        if not campaign.plan or not campaign.started_at:
            return []
        day = (datetime.utcnow() - campaign.started_at).days + 1
        return [a for a in campaign.plan.actions if a.day == day]

    def is_on_cooldown(self, campaign: BurstCampaign) -> bool:
        """Check if this keyword is in cooldown period."""
        if campaign.cooldown_until and datetime.utcnow() < campaign.cooldown_until:
            return True
        return False
