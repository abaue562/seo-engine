"""AIC Engine — Attention → Intent → Conversion unified pipeline.

One call produces the COMPLETE funnel for a keyword:
  1. ATTENTION: hooks, TikTok script, social post, blog intro (top-of-funnel)
  2. INTENT: search bridges, branded phrases, follow-up triggers (mid-funnel)
  3. CONVERSION: full landing page structure with CTAs (bottom-funnel)
  4. DISTRIBUTION: publishing plan across channels with cadence
  5. MEASUREMENT: what to track and when to re-optimize

This is the single endpoint that replaces running 15 tools separately.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from core.claude import call_claude
from models.business import BusinessContext

log = logging.getLogger(__name__)


class AttentionAssets(BaseModel):
    """Top-of-funnel: capture attention before search."""
    hooks: list[dict] = []             # {type, text, platform}
    tiktok_script: dict = {}           # {hook, body, cta, caption}
    social_posts: list[dict] = []      # {platform, text, hashtags}
    blog_intro: str = ""
    search_bridges: list[str] = []     # Phrases that nudge "search this"


class IntentShaping(BaseModel):
    """Mid-funnel: shape raw attention into search + qualified visits."""
    target_search_phrase: str = ""     # The exact search we want triggered
    branded_phrases: list[str] = []    # Consistent phrasing across all assets
    follow_up_triggers: list[str] = [] # Content that makes them come back
    internal_links: list[dict] = []    # {from_page, to_page, anchor}


class ConversionPage(BaseModel):
    """Bottom-funnel: page optimized for revenue."""
    title: str = ""
    meta_description: str = ""
    hero: dict = {}
    sections: list[dict] = []
    cta_placements: list[str] = []     # Where CTAs go
    trust_elements: list[str] = []     # Reviews, years, certifications


class DistributionPlan(BaseModel):
    """Multi-channel publishing schedule."""
    day_1: list[dict] = []    # {channel, asset, action}
    day_2: list[dict] = []
    day_3_5: list[dict] = []
    day_7: list[dict] = []
    ongoing: list[dict] = []


class MeasurementPlan(BaseModel):
    """What to track and when to re-optimize."""
    kpis: list[dict] = []              # {metric, target, check_after_days}
    triggers: list[dict] = []          # {condition, action}


class AICResult(BaseModel):
    """Complete AIC pipeline output."""
    keyword: str
    business_name: str = ""
    attention: AttentionAssets = Field(default_factory=AttentionAssets)
    intent: IntentShaping = Field(default_factory=IntentShaping)
    conversion: ConversionPage = Field(default_factory=ConversionPage)
    distribution: DistributionPlan = Field(default_factory=DistributionPlan)
    measurement: MeasurementPlan = Field(default_factory=MeasurementPlan)
    generated_at: datetime = Field(default_factory=datetime.utcnow)


AIC_PROMPT = """You are the AIC Engine — Attention, Intent, Conversion. Generate a COMPLETE marketing funnel for ONE keyword.

Keyword: {keyword}
Business: {business_name}
Service: {service}
City: {city}
Service Areas: {service_areas}
Reviews: {reviews} at {rating} stars
Competitors: {competitors}

Generate ALL five layers:

1. ATTENTION (top-of-funnel — capture attention before search):
   - 3 hooks: emotional, practical, curiosity — each under 15 words
   - 1 TikTok/Reels script (hook + body + CTA, 30 seconds)
   - 2 social media posts (with hashtags)
   - 1 blog intro paragraph (100 words)
   - 3 "search bridges" — phrases in content that nudge viewers to Google your brand

2. INTENT (mid-funnel — shape attention into qualified search):
   - The exact search phrase you want people to type
   - 3 branded phrases to use consistently across ALL assets
   - 3 follow-up triggers (reasons to come back / search again)
   - 3 internal link suggestions (from_page, to_page, anchor_text)

3. CONVERSION (bottom-funnel — page that converts):
   - Title tag (under 60 chars)
   - Meta description (under 160 chars)
   - Hero: headline + subheadline + CTA text
   - Page sections: problem, solution, benefits (6 items), services, proof, FAQ (5 items), final CTA
   - Where to place CTAs (above fold, after benefits, after FAQ, sticky mobile)
   - Trust elements to include

4. DISTRIBUTION (publishing schedule):
   - Day 1: what to publish where
   - Day 2: what next
   - Day 3-5: supporting content
   - Day 7: reinforcement
   - Ongoing: maintenance cadence

5. MEASUREMENT (tracking plan):
   - 3 KPIs with targets and check dates
   - 3 auto-triggers (if X happens, do Y)

Return ONLY JSON:
{{
  "attention": {{
    "hooks": [{{"type": "emotional|practical|curiosity", "text": "", "platform": "tiktok|social|blog"}}],
    "tiktok_script": {{"hook": "", "body": "", "cta": "", "caption": ""}},
    "social_posts": [{{"platform": "instagram|facebook", "text": "", "hashtags": []}}],
    "blog_intro": "",
    "search_bridges": ["phrase that makes people search your brand"]
  }},
  "intent": {{
    "target_search_phrase": "{business_name} {service} {city}",
    "branded_phrases": [],
    "follow_up_triggers": [],
    "internal_links": [{{"from_page": "", "to_page": "", "anchor": ""}}]
  }},
  "conversion": {{
    "title": "",
    "meta_description": "",
    "hero": {{"headline": "", "subheadline": "", "cta": ""}},
    "sections": [
      {{"type": "problem", "title": "", "content": ""}},
      {{"type": "solution", "title": "", "content": ""}},
      {{"type": "benefits", "title": "", "items": [{{"name": "", "description": ""}}]}},
      {{"type": "services", "title": "", "items": [{{"name": "", "description": ""}}]}},
      {{"type": "proof", "content": ""}},
      {{"type": "faq", "items": [{{"question": "", "answer": ""}}]}},
      {{"type": "cta", "title": "", "cta": ""}}
    ],
    "cta_placements": ["above_fold", "after_benefits", "after_faq", "sticky_mobile"],
    "trust_elements": []
  }},
  "distribution": {{
    "day_1": [{{"channel": "", "asset": "", "action": ""}}],
    "day_2": [{{"channel": "", "asset": "", "action": ""}}],
    "day_3_5": [{{"channel": "", "asset": "", "action": ""}}],
    "day_7": [{{"channel": "", "asset": "", "action": ""}}],
    "ongoing": [{{"channel": "", "asset": "", "action": ""}}]
  }},
  "measurement": {{
    "kpis": [{{"metric": "", "target": "", "check_after_days": 7}}],
    "triggers": [{{"condition": "", "action": ""}}]
  }}
}}"""


class AICEngine:
    """Generates a complete Attention → Intent → Conversion funnel in one call."""

    async def generate(self, keyword: str, business: BusinessContext) -> AICResult:
        """Generate full AIC funnel for a keyword."""
        prompt = AIC_PROMPT.format(
            keyword=keyword,
            business_name=business.business_name,
            service=business.primary_service,
            city=business.primary_city,
            service_areas=", ".join(business.service_areas) if business.service_areas else business.primary_city,
            reviews=business.reviews_count,
            rating=business.rating,
            competitors=", ".join(business.competitors[:3]) if business.competitors else "unknown",
        )

        try:
            raw = call_claude(
                prompt,
                system="You are a full-funnel marketing engine. Return ONLY valid JSON. No other text.",
                max_tokens=8000,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            # Find JSON in response
            start = raw.find("{")
            if start > 0:
                raw = raw[start:]

            data = json.loads(raw)

            result = AICResult(
                keyword=keyword,
                business_name=business.business_name,
            )

            # Parse attention
            att = data.get("attention", {})
            result.attention = AttentionAssets(
                hooks=att.get("hooks", []),
                tiktok_script=att.get("tiktok_script", {}),
                social_posts=att.get("social_posts", []),
                blog_intro=att.get("blog_intro", ""),
                search_bridges=att.get("search_bridges", []),
            )

            # Parse intent
            intent = data.get("intent", {})
            result.intent = IntentShaping(
                target_search_phrase=intent.get("target_search_phrase", ""),
                branded_phrases=intent.get("branded_phrases", []),
                follow_up_triggers=intent.get("follow_up_triggers", []),
                internal_links=intent.get("internal_links", []),
            )

            # Parse conversion
            conv = data.get("conversion", {})
            result.conversion = ConversionPage(
                title=conv.get("title", ""),
                meta_description=conv.get("meta_description", ""),
                hero=conv.get("hero", {}),
                sections=conv.get("sections", []),
                cta_placements=conv.get("cta_placements", []),
                trust_elements=conv.get("trust_elements", []),
            )

            # Parse distribution
            dist = data.get("distribution", {})
            result.distribution = DistributionPlan(
                day_1=dist.get("day_1", []),
                day_2=dist.get("day_2", []),
                day_3_5=dist.get("day_3_5", dist.get("day_3-5", [])),
                day_7=dist.get("day_7", []),
                ongoing=dist.get("ongoing", []),
            )

            # Parse measurement
            meas = data.get("measurement", {})
            result.measurement = MeasurementPlan(
                kpis=meas.get("kpis", []),
                triggers=meas.get("triggers", []),
            )

            log.info("aic.done  keyword=%s  hooks=%d  sections=%d  kpis=%d",
                     keyword, len(result.attention.hooks), len(result.conversion.sections),
                     len(result.measurement.kpis))

            return result

        except Exception as e:
            log.error("aic.fail  keyword=%s  err=%s", keyword, e)
            return AICResult(keyword=keyword, business_name=business.business_name)
