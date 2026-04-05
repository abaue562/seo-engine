"""External Publishing Network — strategic content placement across the web.

NOT spam. NOT blast-posting. This is:
  Selective, tailored, platform-adapted content placed for maximum signal value.

Platform types:
  1. Publishing: Medium, Substack, Blogger (long-form authority)
  2. Community: Reddit, Quora (trust + discussion)
  3. Entity: Directories, profiles (entity signals)
  4. Partner: Guest posts, local sites (backlinks)

Each platform gets DIFFERENT content adapted to its norms.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from core.claude import call_claude
from execution.connectors.base import PublishResult

log = logging.getLogger(__name__)

# Velocity limits per platform per day
VELOCITY = {
    "medium": 1,
    "reddit": 1,
    "quora": 2,
    "directory": 3,
    "guest_post": 1,
}


class ExternalAsset(BaseModel):
    """One piece of content adapted for an external platform."""
    platform: str
    content_type: str        # article / discussion / answer / listing / guest_post
    title: str = ""
    content: str = ""
    link_strategy: str = ""  # direct / soft / none
    link_url: str = ""
    status: str = "ready"    # ready / published / queued


class ExternalPlan(BaseModel):
    """Distribution plan across external platforms."""
    keyword: str
    assets: list[ExternalAsset] = Field(default_factory=list)
    total_platforms: int = 0
    link_distribution: dict = {}  # main_page: 40%, supporting: 30%, none: 30%


ADAPT_PROMPT = """You are the External Content Adapter. Take ONE topic and create platform-specific content for EACH platform.

Topic: {keyword}
Business: {business_name}
Service: {service}
City: {city}
Target page: {target_page}

Create content for these 4 platforms. Each must be DIFFERENT in tone, format, and approach:

1. MEDIUM ARTICLE — 600-800 word educational article
   - Professional, informative tone
   - Include soft mention of the business
   - Link naturally at the end

2. REDDIT DISCUSSION — Community-style post
   - Value-first, no hard sell
   - Frame as sharing experience/knowledge
   - No link in main post (add in comments if needed)
   - Title should invite discussion

3. QUORA ANSWER — Answer a real question about this topic
   - The question people actually ask
   - Detailed, helpful answer
   - Mention business as example, not advertisement

4. DIRECTORY LISTING — Business profile copy
   - 200 words, professional
   - Keywords naturally included
   - Direct link to website

Return ONLY JSON:
{{
  "medium": {{
    "title": "",
    "content": "",
    "link_strategy": "soft",
    "tags": []
  }},
  "reddit": {{
    "subreddit_suggestion": "",
    "title": "",
    "content": "",
    "link_strategy": "none"
  }},
  "quora": {{
    "question": "",
    "answer": "",
    "link_strategy": "soft"
  }},
  "directory": {{
    "business_description": "",
    "categories": [],
    "link_strategy": "direct"
  }}
}}"""


class ExternalNetwork:
    """Plans and generates content for external web distribution."""

    async def plan_distribution(
        self,
        keyword: str,
        business_name: str,
        service: str,
        city: str,
        target_page: str,
    ) -> ExternalPlan:
        """Generate platform-adapted content for external distribution."""
        prompt = ADAPT_PROMPT.format(
            keyword=keyword,
            business_name=business_name,
            service=service,
            city=city,
            target_page=target_page,
        )

        try:
            raw = call_claude(
                prompt,
                system="You are a content distribution strategist. Return ONLY valid JSON.",
                max_tokens=4096,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            start = raw.find("{")
            if start > 0:
                raw = raw[start:]

            data = json.loads(raw)

            assets = []

            # Medium
            m = data.get("medium", {})
            if m.get("title"):
                assets.append(ExternalAsset(
                    platform="medium",
                    content_type="article",
                    title=m.get("title", ""),
                    content=m.get("content", ""),
                    link_strategy=m.get("link_strategy", "soft"),
                    link_url=target_page,
                ))

            # Reddit
            r = data.get("reddit", {})
            if r.get("title"):
                assets.append(ExternalAsset(
                    platform="reddit",
                    content_type="discussion",
                    title=r.get("title", ""),
                    content=r.get("content", ""),
                    link_strategy="none",
                ))

            # Quora
            q = data.get("quora", {})
            if q.get("question"):
                assets.append(ExternalAsset(
                    platform="quora",
                    content_type="answer",
                    title=q.get("question", ""),
                    content=q.get("answer", ""),
                    link_strategy="soft",
                    link_url=target_page,
                ))

            # Directory
            d = data.get("directory", {})
            if d.get("business_description"):
                assets.append(ExternalAsset(
                    platform="directory",
                    content_type="listing",
                    content=d.get("business_description", ""),
                    link_strategy="direct",
                    link_url=target_page,
                ))

            plan = ExternalPlan(
                keyword=keyword,
                assets=assets,
                total_platforms=len(assets),
                link_distribution={
                    "direct": sum(1 for a in assets if a.link_strategy == "direct"),
                    "soft": sum(1 for a in assets if a.link_strategy == "soft"),
                    "none": sum(1 for a in assets if a.link_strategy == "none"),
                },
            )

            log.info("external.planned  keyword=%s  platforms=%d", keyword, len(assets))
            return plan

        except Exception as e:
            log.error("external.plan_fail  keyword=%s  err=%s", keyword, e)
            return ExternalPlan(keyword=keyword)
