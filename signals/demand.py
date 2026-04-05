"""Demand Generation Engine — creates the searches that lead to rankings.

Instead of waiting for people to search, this engine:
  1. Creates curiosity/problem content on social (TikTok, Reels, posts)
  2. Content drives people to search "brand + service"
  3. Branded searches → Google trust signal → higher rankings

This is the loop:
  Social hook → "Google this" → Branded search → Click → Ranking signal
"""

from __future__ import annotations

import json
import logging
from core.claude import call_claude, call_claude_json, call_claude_raw


from signals.models import DemandCampaign
from models.business import BusinessContext

log = logging.getLogger(__name__)


DEMAND_PROMPT = """You are the Demand Generation Agent.

Your job is NOT to optimize for Google. Your job is to CREATE the searches that lead to rankings.

Business: {business_name}
Service: {service}
City: {city}
Target keyword: {keyword}

Create a demand generation campaign that makes people search for this business.

Strategy: Create short-form content (TikTok, Reels) with hooks that make people Google the business.

Generate:
1. 5 curiosity/problem hooks for short-form video
   - Must create urgency or curiosity
   - Must naturally lead to searching "{business_name} {service} {city}"
   - Examples: "This one mistake costs homeowners $5,000..." → viewer Googles service

2. The exact branded search you want to trigger
   - e.g., "{business_name} {city}" or "{business_name} {service}"

3. Content chain strategy
   - How each piece leads to the next search/click

Return ONLY JSON:
{{
  "target_search": "",
  "hooks": [
    {{
      "hook": "",
      "platform": "tiktok | reels | youtube_shorts",
      "cta_type": "search | visit | call",
      "script_outline": ""
    }}
  ],
  "content_chain": {{
    "step_1": "",
    "step_2": "",
    "step_3": ""
  }},
  "expected_branded_searches_per_month": 0
}}

Rules:
- Every hook must create genuine curiosity (not clickbait)
- Every hook must naturally lead to a search action
- Content must provide real value while driving brand discovery
- Think like a content creator, not an SEO"""


class DemandEngine:
    """Creates demand generation campaigns that drive branded searches."""

    def __init__(self):
        pass


    async def create_campaign(self, keyword: str, business: BusinessContext) -> DemandCampaign:
        """Generate a full demand generation campaign for a keyword."""
        prompt = DEMAND_PROMPT.format(
            business_name=business.business_name,
            service=business.primary_service,
            city=business.primary_city,
            keyword=keyword,
        )

        try:
            response = call_claude_raw(
                model=None,
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            data = json.loads(raw)

            campaign = DemandCampaign(
                keyword=keyword,
                brand=business.business_name,
                city=business.primary_city,
                target_search=data.get("target_search", ""),
                channels=[h.get("platform", "tiktok") for h in data.get("hooks", [])],
                content_hooks=[h.get("hook", "") for h in data.get("hooks", [])],
                expected_branded_searches=data.get("expected_branded_searches_per_month", 0),
                status="planned",
            )

            log.info("demand.campaign  keyword=%s  hooks=%d  target=%s",
                     keyword, len(campaign.content_hooks), campaign.target_search)
            return campaign

        except Exception as e:
            log.error("demand.fail  keyword=%s  err=%s", keyword, e)
            return DemandCampaign(keyword=keyword, brand=business.business_name, city=business.primary_city)
