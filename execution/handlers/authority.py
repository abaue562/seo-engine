"""Authority Execution Handler v2 — full backlink engine.

Capabilities:
- Outreach email generation + follow-ups
- Competitor link replication
- Citation/directory submissions
- PR/HARO response generation
- Link asset creation (guides, stats pages)
- Link scoring + velocity control
"""

from __future__ import annotations

import json
import logging
from core.claude import call_claude, call_claude_json, call_claude_raw
from datetime import datetime


from execution.models import ExecResult, ExecStatus
from execution.templates.prompts import OUTREACH_EMAIL_PROMPT
from models.business import BusinessContext

log = logging.getLogger(__name__)

# --- Link velocity limits (per month) ---
VELOCITY_LIMITS = {
    "citation": 10,
    "outreach": 5,
    "pr": 3,
    "link_asset": 2,
}

# --- Link scoring weights ---
LINK_SCORE_WEIGHTS = {
    "authority": 0.3,
    "relevance": 0.3,
    "traffic": 0.2,
    "ease": 0.2,
}


REPLICATION_PROMPT = """You are the Link Replication Agent.

Competitor backlinks:
{competitor_links}

Our business: {business_name} ({website})
Our city: {city}

Analyze these competitor backlinks and identify which ones we can replicate.

OUTPUT: Return ONLY a JSON array:
[
  {{
    "domain": "",
    "type": "citation | guest_post | pr | resource | other",
    "how_competitor_got_link": "",
    "replication_strategy": "",
    "difficulty": "easy | medium | hard",
    "priority": "high | medium",
    "link_score": 0
  }}
]

Rules:
- Prioritize links multiple competitors share
- Focus on replicable links only
- Skip spam domains or irrelevant niches
- Score each link 1-10 based on: authority(30%) + relevance(30%) + traffic(20%) + ease(20%)"""


PR_RESPONSE_PROMPT = """Respond to a journalist request for expert commentary.

Business: {business_name}
Service: {service}
City: {city}
Journalist topic: {topic}

Requirements:
- Provide expert insight from a {service} professional
- Include a quotable line (1-2 sentences)
- Mention credentials/experience
- Under 120 words
- Confident, authoritative tone

Return ONLY JSON:
{{
  "response": "",
  "quote": "",
  "bio": ""
}}"""


LINK_ASSET_PROMPT = """Create a high-linkability resource page.

Business: {business_name}
Service: {service}
City: {city}
Asset type: {asset_type}

Requirements:
- Unique data or insight that other sites would reference
- Highly useful to homeowners/businesses in {city}
- Easy for journalists and bloggers to cite
- Structured with clear headings and data points
- 800-1200 words

Return ONLY JSON:
{{
  "title": "",
  "meta_description": "",
  "content_html": "",
  "target_keywords": [],
  "linkability_hooks": []
}}"""


FOLLOW_UP_PROMPT = """Write a follow-up outreach email.

Original email subject: {original_subject}
Days since sent: {days_since}
Target site: {target_site}
Our business: {business_name}

Requirements:
- Reference the original email
- Add new value or angle
- Shorter than original (under 80 words)
- Not pushy, but direct

Return ONLY JSON:
{{
  "subject": "",
  "body": ""
}}"""


class AuthorityHandler:
    """Full authority/backlink execution engine."""

    def __init__(self):
        pass


    async def execute(self, task_id: str, action: str, target: str, execution: str, business: BusinessContext) -> ExecResult:
        action_lower = action.lower()

        if "replicate" in action_lower or "competitor link" in action_lower:
            return await self.replicate_links(task_id, target, business)
        elif "outreach" in action_lower or "email" in action_lower or "backlink" in action_lower:
            return await self.create_outreach(task_id, target, business)
        elif "follow" in action_lower:
            return await self.create_followup(task_id, target, business)
        elif "pr" in action_lower or "haro" in action_lower or "journalist" in action_lower:
            return await self.pr_response(task_id, target, business)
        elif "asset" in action_lower or "guide" in action_lower or "resource" in action_lower:
            return await self.create_link_asset(task_id, target, business)
        elif "directory" in action_lower or "citation" in action_lower:
            return await self.directory_submission(task_id, target, business)
        else:
            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={"type": "authority_instruction", "instruction": execution},
            )

    def _call_claude(self, prompt: str, max_tokens: int = 2048) -> str:
        response = call_claude_raw(
            model=None,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        return raw

    async def replicate_links(self, task_id: str, competitor_data: str, business: BusinessContext) -> ExecResult:
        """Analyze competitor backlinks and generate replication plan."""
        prompt = REPLICATION_PROMPT.format(
            competitor_links=competitor_data or "No specific links provided — analyze based on business context",
            business_name=business.business_name,
            website=business.website,
            city=business.primary_city,
        )

        try:
            raw = call_claude(prompt)
            opportunities = json.loads(raw)
            log.info("authority.replication  task=%s  opportunities=%d", task_id, len(opportunities))

            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={"type": "link_replication", "opportunities": opportunities},
            )
        except Exception as e:
            log.error("authority.replication_fail  task=%s  err=%s", task_id, e)
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": str(e)})

    async def create_outreach(self, task_id: str, target_site: str, business: BusinessContext) -> ExecResult:
        """Generate backlink outreach email."""
        prompt = OUTREACH_EMAIL_PROMPT.format(
            business_name=business.business_name,
            website=business.website,
            target_site=target_site,
            contact_name="Site Owner",
            resource=f"{business.primary_service} expertise and local content",
        )

        try:
            raw = call_claude(prompt, max_tokens=512)
            email = json.loads(raw)
            log.info("authority.outreach  task=%s  target=%s", task_id, target_site)

            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={"type": "outreach_email", "target": target_site, **email, "sent": False},
            )
        except Exception as e:
            log.error("authority.outreach_fail  task=%s  err=%s", task_id, e)
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": str(e)})

    async def create_followup(self, task_id: str, target_site: str, business: BusinessContext) -> ExecResult:
        """Generate follow-up outreach email."""
        prompt = FOLLOW_UP_PROMPT.format(
            original_subject="Partnership opportunity",
            days_since=3,
            target_site=target_site,
            business_name=business.business_name,
        )

        try:
            raw = call_claude(prompt, max_tokens=512)
            email = json.loads(raw)
            log.info("authority.followup  task=%s  target=%s", task_id, target_site)

            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={"type": "followup_email", "target": target_site, **email},
            )
        except Exception as e:
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": str(e)})

    async def pr_response(self, task_id: str, topic: str, business: BusinessContext) -> ExecResult:
        """Generate PR/HARO response."""
        prompt = PR_RESPONSE_PROMPT.format(
            business_name=business.business_name,
            service=business.primary_service,
            city=business.primary_city,
            topic=topic or business.primary_service,
        )

        try:
            raw = call_claude(prompt, max_tokens=512)
            response_data = json.loads(raw)
            log.info("authority.pr_response  task=%s  topic=%s", task_id, topic)

            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={"type": "pr_response", "topic": topic, **response_data},
            )
        except Exception as e:
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": str(e)})

    async def create_link_asset(self, task_id: str, asset_type: str, business: BusinessContext) -> ExecResult:
        """Generate a link-worthy content asset (cost guide, stats page, etc)."""
        prompt = LINK_ASSET_PROMPT.format(
            business_name=business.business_name,
            service=business.primary_service,
            city=business.primary_city,
            asset_type=asset_type or f"{business.primary_service} cost guide for {business.primary_city}",
        )

        try:
            raw = call_claude(prompt, max_tokens=4096)
            asset = json.loads(raw)
            log.info("authority.link_asset  task=%s  type=%s", task_id, asset_type)

            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={"type": "link_asset", "asset_type": asset_type, **asset},
            )
        except Exception as e:
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": str(e)})

    async def directory_submission(self, task_id: str, directory: str, business: BusinessContext) -> ExecResult:
        """Generate directory/citation submission data."""
        return ExecResult(
            task_id=task_id,
            status=ExecStatus.SUCCESS,
            output={
                "type": "citation_submission",
                "directory": directory,
                "business_name": business.business_name,
                "website": business.website,
                "city": business.primary_city,
                "service": business.primary_service,
                "instruction": f"Submit to {directory} with consistent NAP details.",
            },
        )
