"""Influence Operating System — perception engineering + narrative control + consensus building.

Layers:
  1. PERCEPTION: What does the internet currently believe about this topic?
  2. NARRATIVE: Which framing do we push? Generate angles that reshape perception.
  3. CONSENSUS: Same message across all channels → AI treats it as truth.
  4. IDENTITY: Build the brand as an entity, not just a website.
  5. DEMAND CREATION: Introduce new concepts, own the phrasing.

This is not SEO. This is influence architecture.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from core.claude import call_claude
from models.business import BusinessContext

log = logging.getLogger(__name__)


class PerceptionMap(BaseModel):
    """What the internet currently believes about this topic."""
    topic: str
    current_narratives: list[dict] = []      # {narrative, strength, sentiment}
    gaps: list[str] = []                      # Narratives nobody is pushing
    our_opportunity: str = ""                 # The angle we should own


class NarrativeAssets(BaseModel):
    """Content assets built around ONE narrative angle."""
    narrative: str                             # The framing we're pushing
    headline_angles: list[str] = []           # Different ways to express it
    tiktok_hooks: list[str] = []              # Short-form video hooks
    social_posts: list[str] = []              # Platform posts
    blog_titles: list[str] = []               # Article angles
    search_phrases: list[str] = []            # Phrases we want people to search
    consensus_statements: list[str] = []      # Same idea in different words for different platforms


class DemandConcept(BaseModel):
    """A new concept/phrase we're introducing to the market."""
    phrase: str                                # The new term
    definition: str                            # What it means
    why_it_matters: str                        # Why people should care
    content_angles: list[str] = []             # How to introduce it
    target_adoption: str = ""                  # Goal: "people search this phrase"


class InfluenceResult(BaseModel):
    keyword: str
    perception: PerceptionMap = Field(default_factory=lambda: PerceptionMap(topic=""))
    narratives: list[NarrativeAssets] = Field(default_factory=list)
    demand_concepts: list[DemandConcept] = Field(default_factory=list)
    consensus_plan: dict = {}
    identity_actions: list[str] = []
    generated_at: datetime = Field(default_factory=datetime.utcnow)


INFLUENCE_PROMPT = """You are the Influence Operating System. Your job is NOT SEO — it's perception engineering.

Topic: {keyword}
Business: {business_name}
Service: {service}
City: {city}
Competitors: {competitors}

Generate a complete influence strategy:

1. PERCEPTION MAP — What does the internet currently believe about "{keyword}"?
   - 3-4 existing narratives (what people think) with strength (strong/medium/weak) and sentiment (positive/neutral/negative)
   - 2-3 narrative GAPS (angles nobody is pushing that we can own)
   - Our opportunity (the ONE narrative angle we should dominate)

2. NARRATIVE ASSETS — Content for our chosen narrative:
   - 3 headline angles (different ways to express our narrative)
   - 3 TikTok hooks (under 10 words each, attention-grabbing)
   - 3 social media posts (varied platforms)
   - 3 blog title ideas
   - 3 search phrases we want people to start using
   - 3 consensus statements (same idea in different words — for different platforms to create web-wide agreement)

3. DEMAND CREATION — Introduce 1-2 new concepts:
   - A new phrase/term related to the service (something people don't search YET but should)
   - What it means and why it matters
   - How to introduce it through content

4. CONSENSUS PLAN:
   - Where to place consistent messaging (platforms list)
   - How many touchpoints needed for AI systems to treat it as truth
   - What to repeat across ALL channels

5. IDENTITY ACTIONS:
   - 3-5 specific actions to build the brand as an ENTITY (not just a website)

Return ONLY JSON:
{{
  "perception": {{
    "topic": "{keyword}",
    "current_narratives": [{{"narrative": "", "strength": "strong|medium|weak", "sentiment": "positive|neutral|negative"}}],
    "gaps": [],
    "our_opportunity": ""
  }},
  "narratives": [
    {{
      "narrative": "our chosen framing",
      "headline_angles": [],
      "tiktok_hooks": [],
      "social_posts": [],
      "blog_titles": [],
      "search_phrases": [],
      "consensus_statements": []
    }}
  ],
  "demand_concepts": [
    {{
      "phrase": "new term",
      "definition": "",
      "why_it_matters": "",
      "content_angles": [],
      "target_adoption": ""
    }}
  ],
  "consensus_plan": {{
    "platforms": [],
    "touchpoints_needed": 0,
    "key_message": ""
  }},
  "identity_actions": []
}}"""


class InfluenceOS:
    """Controls perception, narratives, and consensus at scale."""

    async def generate(self, keyword: str, business: BusinessContext) -> InfluenceResult:
        """Generate complete influence strategy for a keyword."""
        prompt = INFLUENCE_PROMPT.format(
            keyword=keyword,
            business_name=business.business_name,
            service=business.primary_service,
            city=business.primary_city,
            competitors=", ".join(business.competitors[:3]) if business.competitors else "unknown",
        )

        try:
            raw = call_claude(
                prompt,
                system="You are a perception engineer and influence strategist. Return ONLY valid JSON.",
                max_tokens=4096,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            start = raw.find("{")
            if start > 0:
                raw = raw[start:]

            data = json.loads(raw)

            result = InfluenceResult(keyword=keyword)

            # Perception
            perc = data.get("perception", {})
            result.perception = PerceptionMap(
                topic=perc.get("topic", keyword),
                current_narratives=perc.get("current_narratives", []),
                gaps=perc.get("gaps", []),
                our_opportunity=perc.get("our_opportunity", ""),
            )

            # Narratives
            for n in data.get("narratives", []):
                result.narratives.append(NarrativeAssets(**n))

            # Demand concepts
            for d in data.get("demand_concepts", []):
                result.demand_concepts.append(DemandConcept(**d))

            # Consensus + Identity
            result.consensus_plan = data.get("consensus_plan", {})
            result.identity_actions = data.get("identity_actions", [])

            log.info("influence.done  keyword=%s  narratives=%d  concepts=%d",
                     keyword, len(result.narratives), len(result.demand_concepts))
            return result

        except Exception as e:
            log.error("influence.fail  keyword=%s  err=%s", keyword, e)
            return InfluenceResult(keyword=keyword)
