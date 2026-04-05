"""Content Multiplication Engine — one keyword becomes 5+ content formats.

Takes a keyword + business context and generates:
  - Service page
  - Blog article
  - TikTok script
  - GBP post
  - Social post

All from a single Claude call for efficiency.
"""

from __future__ import annotations

import json
import logging
from core.claude import call_claude, call_claude_json, call_claude_raw


from channels.models import ContentBundle
from models.business import BusinessContext

log = logging.getLogger(__name__)


MULTIPLY_PROMPT = """You are the Content Multiplication Agent.

Create content for ALL channels from this single keyword.

Business: {business_name}
Service: {service}
City: {city}
Target keyword: {keyword}

Generate ALL of the following in one response:

1. SERVICE PAGE (for website)
   - title (under 60 chars)
   - meta_description (under 160 chars)
   - h1
   - intro (100 words)
   - main_content (300 words)

2. BLOG ARTICLE
   - title
   - content (500 words, practical tips format)

3. TIKTOK SCRIPT
   - hook (first 2 seconds, attention-grabbing)
   - body (problem + insight, 15-20 seconds)
   - cta (clear action)
   - caption (with hashtags)

4. GBP POST
   - text (120-150 words, includes service + city + CTA)
   - cta_text (button text)

5. SOCIAL POST
   - text (under 280 chars, engaging)
   - hashtags (5-8 relevant)

Return ONLY JSON:
{{
  "service_page": {{
    "title": "",
    "meta_description": "",
    "h1": "",
    "intro": "",
    "main_content": ""
  }},
  "blog_article": {{
    "title": "",
    "content": ""
  }},
  "tiktok_script": {{
    "hook": "",
    "body": "",
    "cta": "",
    "caption": ""
  }},
  "gbp_post": {{
    "text": "",
    "cta_text": ""
  }},
  "social_post": {{
    "text": "",
    "hashtags": []
  }}
}}

Rules:
- Adapt tone per platform (professional for website, casual for TikTok, concise for social)
- Keep core message consistent across all formats
- Include keyword naturally in every format
- Include city in website/GBP content
- TikTok must hook in first 2 seconds
- Social must be scroll-stopping"""


class ContentMultiplier:
    """Turns one keyword into a full content bundle across all channels."""

    def __init__(self):
        pass


    async def multiply(self, keyword: str, business: BusinessContext) -> ContentBundle:
        """Generate a full content bundle from a single keyword."""
        prompt = MULTIPLY_PROMPT.format(
            business_name=business.business_name,
            service=business.primary_service,
            city=business.primary_city,
            keyword=keyword,
        )

        try:
            response = call_claude_raw(
                model=None,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            data = json.loads(raw)

            bundle = ContentBundle(
                keyword=keyword,
                city=business.primary_city,
                service=business.primary_service,
                service_page=data.get("service_page", {}),
                blog_article=data.get("blog_article", {}),
                tiktok_script=data.get("tiktok_script", {}),
                gbp_post=data.get("gbp_post", {}),
                social_post=data.get("social_post", {}),
            )

            log.info("multiply.done  keyword=%s  formats=%d", keyword, bundle.format_count)
            return bundle

        except Exception as e:
            log.error("multiply.fail  keyword=%s  err=%s", keyword, e)
            return ContentBundle(keyword=keyword, city=business.primary_city, service=business.primary_service)

    async def multiply_batch(self, keywords: list[str], business: BusinessContext) -> list[ContentBundle]:
        """Generate bundles for multiple keywords."""
        bundles = []
        for kw in keywords:
            bundle = await self.multiply(kw, business)
            bundles.append(bundle)
        return bundles
