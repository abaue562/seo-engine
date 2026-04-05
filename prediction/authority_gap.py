"""Authority Gap Accelerator — closes the domain authority gap with targeted link building.

Calculates the exact authority gap between you and competitors,
then generates a prioritized link building plan to close it.

Works at both domain level and page level.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from core.claude import call_claude

log = logging.getLogger(__name__)


class AuthorityProfile(BaseModel):
    name: str
    domain_authority: float = 0
    page_authority: float = 0
    backlink_count: int = 0
    referring_domains: int = 0


class AuthorityGap(BaseModel):
    keyword: str
    our_profile: AuthorityProfile
    top_competitor: AuthorityProfile
    domain_gap: float = 0          # Their DA - our DA
    page_gap: float = 0
    link_gap: int = 0              # Links they have that we don't
    severity: str = "moderate"     # critical / high / moderate / low
    close_strategy: str = ""


class LinkTarget(BaseModel):
    type: str              # guest_post / directory / resource / outreach / pr
    target: str            # Domain or specific URL
    difficulty: str = ""   # easy / medium / hard
    authority_value: str = ""
    strategy: str = ""


class GapClosePlan(BaseModel):
    keyword: str
    gap: AuthorityGap
    targets: list[LinkTarget] = Field(default_factory=list)
    links_needed: int = 0
    estimated_timeline: str = ""
    total_effort: str = ""


GAP_PROMPT = """You are the Authority Gap Accelerator. Analyze the authority gap and create a link building plan.

Our business: {business_name} ({city})
Our domain authority: {our_da}
Our backlinks: {our_links}

Top competitor: {competitor}
Their domain authority: {comp_da}
Their backlinks: {comp_links}

Target keyword: {keyword}
Authority gap: {gap} points

Generate a targeted link building plan to close this gap. Prioritize:
1. Easy wins first (directories, citations, existing relationships)
2. Medium effort (guest posts, resource pages, local partnerships)
3. High impact (PR, industry publications, data-driven content)

Return ONLY JSON:
{{
  "links_needed": 0,
  "estimated_timeline": "X weeks/months",
  "targets": [
    {{
      "type": "directory | guest_post | resource | outreach | pr | citation",
      "target": "specific domain or type of site",
      "difficulty": "easy | medium | hard",
      "authority_value": "estimated DA/DR gain",
      "strategy": "exact approach to acquire this link"
    }}
  ]
}}"""


class AuthorityGapAccelerator:
    """Calculates authority gaps and generates plans to close them."""

    def calculate_gap(
        self,
        keyword: str,
        our_da: float,
        our_links: int,
        competitor_name: str,
        competitor_da: float,
        competitor_links: int,
    ) -> AuthorityGap:
        """Calculate the authority gap for a keyword."""
        domain_gap = competitor_da - our_da
        link_gap = max(0, competitor_links - our_links)

        if domain_gap >= 20:
            severity = "critical"
        elif domain_gap >= 10:
            severity = "high"
        elif domain_gap >= 5:
            severity = "moderate"
        else:
            severity = "low"

        gap = AuthorityGap(
            keyword=keyword,
            our_profile=AuthorityProfile(
                name="Our Business",
                domain_authority=our_da,
                backlink_count=our_links,
            ),
            top_competitor=AuthorityProfile(
                name=competitor_name,
                domain_authority=competitor_da,
                backlink_count=competitor_links,
            ),
            domain_gap=domain_gap,
            link_gap=link_gap,
            severity=severity,
        )

        log.info("authority_gap.calculated  keyword=%s  gap=%.0f  severity=%s",
                 keyword, domain_gap, severity)
        return gap

    async def generate_plan(
        self,
        gap: AuthorityGap,
        business_name: str,
        city: str,
        keyword: str,
    ) -> GapClosePlan:
        """Generate a link building plan to close the authority gap."""
        prompt = GAP_PROMPT.format(
            business_name=business_name,
            city=city,
            our_da=gap.our_profile.domain_authority,
            our_links=gap.our_profile.backlink_count,
            competitor=gap.top_competitor.name,
            comp_da=gap.top_competitor.domain_authority,
            comp_links=gap.top_competitor.backlink_count,
            keyword=keyword,
            gap=gap.domain_gap,
        )

        try:
            raw = call_claude(
                prompt,
                system="You are a link building strategist. Return ONLY valid JSON.",
                max_tokens=2048,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            start = raw.find('{')
            if start > 0:
                raw = raw[start:]
            data, _ = json.JSONDecoder().raw_decode(raw)
            targets = [LinkTarget(**t) for t in data.get("targets", [])]

            plan = GapClosePlan(
                keyword=keyword,
                gap=gap,
                targets=targets,
                links_needed=data.get("links_needed", len(targets)),
                estimated_timeline=data.get("estimated_timeline", ""),
            )

            log.info("authority_gap.plan  keyword=%s  targets=%d  timeline=%s",
                     keyword, len(targets), plan.estimated_timeline)
            return plan

        except Exception as e:
            log.error("authority_gap.plan_fail  keyword=%s  err=%s", keyword, e)
            return GapClosePlan(keyword=keyword, gap=gap)

    def recommend_strategy(self, gap: AuthorityGap) -> str:
        """Quick strategy recommendation based on gap severity."""
        if gap.severity == "critical":
            return "Heavy link building required. Focus on PR + guest posts + authority content. 3-6 month timeline."
        elif gap.severity == "high":
            return "Significant gap. Prioritize 10-15 quality links over 2-3 months. Mix directories + outreach."
        elif gap.severity == "moderate":
            return "Achievable gap. 5-10 targeted links over 1-2 months can close this."
        else:
            return "Small gap. 3-5 strategic links may be enough. Focus on page-level authority."
