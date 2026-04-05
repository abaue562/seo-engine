"""Mention Engine — saturates the web with brand references.

AI systems favor brands that are:
  - Mentioned across multiple authoritative sources
  - Referenced in context of their service + location
  - Present on platforms AI trusts (directories, review sites, Q&A)

This engine identifies where mentions are missing and generates
content/strategies to fill those gaps.
"""

from __future__ import annotations

import json
import logging
from core.claude import call_claude, call_claude_json, call_claude_raw


from ai_visibility.models import MentionOpportunity, EntityPresence, AISource
from models.business import BusinessContext

log = logging.getLogger(__name__)


# Platforms AI systems pull from — ordered by importance
AI_KNOWLEDGE_SOURCES = [
    {"platform": "Google Business Profile", "type": "directory", "importance": 10},
    {"platform": "Yelp", "type": "review", "importance": 9},
    {"platform": "BBB", "type": "directory", "importance": 8},
    {"platform": "Angi (Angie's List)", "type": "review", "importance": 7},
    {"platform": "HomeAdvisor", "type": "directory", "importance": 7},
    {"platform": "Thumbtack", "type": "directory", "importance": 6},
    {"platform": "Facebook Business", "type": "social", "importance": 8},
    {"platform": "LinkedIn Company", "type": "social", "importance": 6},
    {"platform": "Apple Maps", "type": "directory", "importance": 7},
    {"platform": "Bing Places", "type": "directory", "importance": 6},
    {"platform": "Yellow Pages", "type": "directory", "importance": 5},
    {"platform": "Nextdoor", "type": "social", "importance": 6},
    {"platform": "Local Chamber of Commerce", "type": "directory", "importance": 5},
    {"platform": "Industry-specific directories", "type": "directory", "importance": 7},
]


MENTION_STRATEGY_PROMPT = """You are the AI Mention Agent.

Your goal: identify where this business needs brand mentions so AI systems (ChatGPT, Perplexity, Google AI) recommend it.

Business: {business_name}
Service: {service}
City: {city}
Website: {website}

Current known presence:
{current_presence}

Generate a mention acquisition strategy:
1. Where the brand should be mentioned but isn't
2. How to get mentioned there
3. Priority order

Return ONLY JSON array:
[
  {{
    "platform": "",
    "type": "guest_post | directory | forum | qa_answer | news | blog",
    "url": "",
    "difficulty": "easy | medium | hard",
    "impact": "high | medium",
    "strategy": "specific action to get mentioned here"
  }}
]

Focus on:
- Platforms that AI systems actually scrape
- Local/niche authority sources
- Q&A sites where the service is discussed
- "Best of {city}" lists and guides"""


class MentionEngine:
    """Identifies and fills brand mention gaps across AI knowledge sources."""

    def __init__(self):
        pass


    def audit_presence(self, business: BusinessContext) -> list[EntityPresence]:
        """Audit which AI knowledge sources the business is present on.
        Returns a list of platforms with status.

        Note: In production, this would check each platform via API/scraping.
        For now, it generates the checklist based on known sources.
        """
        presences = []
        for source in AI_KNOWLEDGE_SOURCES:
            presences.append(EntityPresence(
                platform=source["platform"],
                status="unknown",  # Would be checked via API in production
            ))
        return presences

    async def find_opportunities(self, business: BusinessContext) -> list[MentionOpportunity]:
        """Find where the brand should be mentioned but isn't."""
        presences = self.audit_presence(business)
        presence_block = "\n".join(
            f"  - {p.platform}: {p.status}" for p in presences
        )

        prompt = MENTION_STRATEGY_PROMPT.format(
            business_name=business.business_name,
            service=business.primary_service,
            city=business.primary_city,
            website=business.website,
            current_presence=presence_block,
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
            opportunities = [MentionOpportunity(**item) for item in data]
            log.info("mentions.opportunities  count=%d", len(opportunities))
            return opportunities

        except Exception as e:
            log.error("mentions.fail  err=%s", e)
            return []

    @staticmethod
    def presence_to_prompt_block(presences: list[EntityPresence]) -> str:
        """Render entity presence as agent context."""
        lines = ["ENTITY PRESENCE ACROSS AI KNOWLEDGE SOURCES:"]
        for p in presences:
            status_icon = {"present": "+", "missing": "-", "incomplete": "~", "unknown": "?"}.get(p.status, "?")
            nap = " [NAP OK]" if p.nap_consistent else " [NAP MISMATCH]"
            lines.append(f"  [{status_icon}] {p.platform}: {p.status}{nap if p.status == 'present' else ''}")
        return "\n".join(lines)
