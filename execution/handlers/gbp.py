"""GBP Execution Handler — generates real GBP content via Claude CLI."""

from __future__ import annotations

import json
import logging
from core.claude import call_claude

from execution.models import ExecResult, ExecStatus
from models.business import BusinessContext

log = logging.getLogger(__name__)


GBP_OPTIMIZE_PROMPT = """Generate a complete Google Business Profile optimization package.

Business: {business_name}
Service: {service}
City: {city}
Service Areas: {service_areas}

Create ALL of the following:

1. PRIMARY CATEGORY: Best GBP category for this business
2. SECONDARY CATEGORIES: 2-3 additional relevant categories
3. BUSINESS DESCRIPTION: 750 characters max, include keywords naturally, mention city + services
4. SERVICES LIST: 5-8 services with short descriptions (50 words each)
5. FIRST GBP POST: 150 words, announce the business, include CTA

Return ONLY JSON:
{{
  "primary_category": "",
  "secondary_categories": [],
  "description": "",
  "services": [{{"name": "", "description": ""}}],
  "first_post": {{
    "text": "",
    "cta": ""
  }}
}}"""


GBP_POST_PROMPT = """Generate a Google Business Profile post.

Business: {business_name}
Service: {service}
City: {city}

Requirements:
- 120-150 words
- Include service keyword + city naturally
- Include a clear CTA (call, book, visit)
- Sound human, not robotic
- Engaging opening line

Return ONLY JSON:
{{
  "text": "",
  "cta": "",
  "image_prompt": ""
}}"""


REVIEW_RESPONSE_PROMPT = """Write a review response.

Business: {business_name}
Service: {service}
City: {city}
Review text: {review_text}

Include: service mention, city mention, gratitude + personalization.
Keep under 80 words. Professional but warm.

Return ONLY the response text."""


class GBPHandler:

    async def execute(self, task_id: str, action: str, target: str, execution: str, business: BusinessContext) -> ExecResult:
        action_lower = action.lower()

        # Route by action keywords — broader matching
        if any(kw in action_lower for kw in ["optimize", "claim", "create", "setup", "set up", "categories", "profile"]):
            return await self.optimize_profile(task_id, business)
        elif any(kw in action_lower for kw in ["post", "publish", "update", "announce"]):
            return await self.create_post(task_id, business)
        elif any(kw in action_lower for kw in ["review", "respond", "reply"]):
            return await self.respond_to_review(task_id, target, business)
        else:
            # Fallback: generate optimization package (most useful default)
            return await self.optimize_profile(task_id, business)

    async def optimize_profile(self, task_id: str, business: BusinessContext) -> ExecResult:
        """Generate full GBP optimization package — categories, description, services, first post."""
        prompt = GBP_OPTIMIZE_PROMPT.format(
            business_name=business.business_name,
            service=business.primary_service,
            city=business.primary_city,
            service_areas=", ".join(business.service_areas) if business.service_areas else business.primary_city,
        )

        try:
            raw = call_claude(prompt)
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            data = json.loads(raw)
            log.info("gbp.optimize_generated  task=%s", task_id)

            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={
                    "type": "gbp_optimization",
                    "ready_to_apply": True,
                    **data,
                },
            )
        except Exception as e:
            log.error("gbp.optimize_fail  task=%s  err=%s", task_id, e)
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": str(e)})

    async def create_post(self, task_id: str, business: BusinessContext) -> ExecResult:
        """Generate a GBP post."""
        prompt = GBP_POST_PROMPT.format(
            business_name=business.business_name,
            service=business.primary_service,
            city=business.primary_city,
        )

        try:
            raw = call_claude(prompt)
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            data = json.loads(raw)
            log.info("gbp.post_generated  task=%s", task_id)

            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={"type": "gbp_post", "ready_to_apply": True, **data},
            )
        except Exception as e:
            log.error("gbp.post_fail  task=%s  err=%s", task_id, e)
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": str(e)})

    async def respond_to_review(self, task_id: str, review_text: str, business: BusinessContext) -> ExecResult:
        """Generate a review response."""
        prompt = REVIEW_RESPONSE_PROMPT.format(
            business_name=business.business_name,
            service=business.primary_service,
            city=business.primary_city,
            review_text=review_text or "Great service, very professional!",
        )

        try:
            reply = call_claude(prompt, max_tokens=512)
            log.info("gbp.review_generated  task=%s", task_id)

            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={"type": "review_response", "response": reply, "ready_to_apply": True},
            )
        except Exception as e:
            log.error("gbp.review_fail  task=%s  err=%s", task_id, e)
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": str(e)})
