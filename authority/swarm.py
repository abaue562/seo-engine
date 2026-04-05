"""Distributed Authority Swarm — coordinated content+link ecosystem that simulates organic growth.

NOT a link blaster. This generates:
  - Contextual content pieces on varied platforms
  - Embedded links that appear as natural references
  - Cross-linked content nodes that build a graph
  - Tiered authority flow (Tier 1 → target, Tier 2 → supporting, Tier 3 → each other)
  - Velocity control to prevent footprint detection
  - Varied anchor text distribution

Safety: anti-pattern detection, velocity limits, anchor diversity enforcement.
"""

from __future__ import annotations

import json
import logging
import random
from datetime import datetime
from pydantic import BaseModel, Field

from core.claude import call_claude

log = logging.getLogger(__name__)

# Anchor distribution targets (avoid patterns)
ANCHOR_DISTRIBUTION = {
    "exact": 0.20,     # "plumber austin"
    "partial": 0.30,   # "austin plumbing services"
    "brand": 0.30,     # "Demo Plumbing"
    "generic": 0.20,   # "click here", "learn more", "this company"
}

VELOCITY_LIMITS = {"low": 1, "medium": 3, "high": 5}  # Per day


class ContentNode(BaseModel):
    """A content piece with embedded authority signal."""
    type: str              # article / blog / directory / mention / guest_post
    platform: str          # target platform/site type
    content_angle: str     # what the content is about
    link_type: str = ""    # direct / contextual / branded
    anchor_text: str = ""
    anchor_category: str = ""  # exact / partial / brand / generic
    target_url: str = ""
    content: str = ""      # Generated content
    tier: int = 1          # 1=links to target, 2=links to supporting, 3=links to other nodes


class SwarmPlan(BaseModel):
    keyword: str
    target_page: str
    nodes: list[ContentNode] = Field(default_factory=list)
    link_distribution: dict[str, float] = {}  # target_page: 60%, supporting: 30%, homepage: 10%
    velocity: str = "medium"
    total_nodes: int = 0
    estimated_days: int = 0
    anchor_mix: dict[str, int] = {}  # exact: 2, partial: 3, etc.


class SwarmCampaign(BaseModel):
    campaign_id: str = ""
    keyword: str
    target_page: str
    business_id: str = ""
    plan: SwarmPlan | None = None
    nodes_deployed: int = 0
    status: str = "planned"
    started_at: datetime | None = None
    daily_count: int = 0
    last_deploy_date: str = ""


SWARM_PROMPT = """You are the Authority Swarm Planner. Create a distributed authority plan that looks like ORGANIC web growth.

Keyword: {keyword}
Target page: {target_page}
Business: {business_name} ({city})
Service: {service}
Velocity: {velocity}
Nodes to create: {node_count}

Create {node_count} content nodes. Each is a content piece on a DIFFERENT platform type with an embedded link.

Requirements:
- Vary platforms (local blogs, directories, resource pages, industry sites, news sites, Q&A forums)
- Vary content angles (each node covers a DIFFERENT aspect of the topic)
- Vary anchor text: ~20% exact keyword, ~30% partial variation, ~30% brand name, ~20% generic
- Mix link types: contextual (in-content), directory listing, resource mention
- Some nodes should link to supporting pages (not just the target)
- Include actual content snippet for each node (100-200 words)

Return ONLY JSON:
{{
  "nodes": [
    {{
      "type": "article | blog | directory | mention | guest_post | resource",
      "platform": "specific platform type",
      "content_angle": "what this piece covers",
      "link_type": "contextual | directory | resource | branded",
      "anchor_text": "the actual anchor text",
      "anchor_category": "exact | partial | brand | generic",
      "content": "100-200 word content snippet with link naturally embedded",
      "tier": 1
    }}
  ],
  "link_distribution": {{"target_page": 0.6, "supporting_pages": 0.3, "homepage": 0.1}},
  "estimated_days": 0
}}"""


class AuthoritySwarm:
    """Coordinates distributed authority building."""

    async def plan_swarm(
        self,
        keyword: str,
        target_page: str,
        business_name: str,
        city: str,
        service: str,
        velocity: str = "medium",
    ) -> SwarmPlan:
        """Generate a swarm plan via Claude."""
        node_count = {"low": 5, "medium": 8, "high": 12}.get(velocity, 8)

        prompt = SWARM_PROMPT.format(
            keyword=keyword,
            target_page=target_page,
            business_name=business_name,
            city=city,
            service=service,
            velocity=velocity,
            node_count=node_count,
        )

        try:
            raw = call_claude(
                prompt,
                system="You are an authority building strategist. Return ONLY valid JSON.",
                max_tokens=4096,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            start = raw.find('{')
            if start > 0:
                raw = raw[start:]
            data, _ = json.JSONDecoder().raw_decode(raw)
            nodes = [ContentNode(**n) for n in data.get("nodes", [])]

            # Calculate anchor mix
            anchor_mix: dict[str, int] = {}
            for n in nodes:
                cat = n.anchor_category or "generic"
                anchor_mix[cat] = anchor_mix.get(cat, 0) + 1

            plan = SwarmPlan(
                keyword=keyword,
                target_page=target_page,
                nodes=nodes,
                link_distribution=data.get("link_distribution", {}),
                velocity=velocity,
                total_nodes=len(nodes),
                estimated_days=data.get("estimated_days", len(nodes) // VELOCITY_LIMITS.get(velocity, 3) + 1),
                anchor_mix=anchor_mix,
            )

            log.info("swarm.planned  keyword=%s  nodes=%d  velocity=%s  days=%d",
                     keyword, len(nodes), velocity, plan.estimated_days)
            return plan

        except Exception as e:
            log.error("swarm.plan_fail  keyword=%s  err=%s", keyword, e)
            return SwarmPlan(keyword=keyword, target_page=target_page, velocity=velocity)

    def check_anchor_diversity(self, plan: SwarmPlan) -> dict:
        """Verify anchor distribution is safe (not over-optimized)."""
        total = plan.total_nodes or 1
        actual = {}
        for cat, count in plan.anchor_mix.items():
            actual[cat] = round(count / total, 2)

        warnings = []
        if actual.get("exact", 0) > 0.35:
            warnings.append("Too many exact-match anchors (>35%) — reduce to avoid over-optimization penalty")
        if actual.get("brand", 0) < 0.15:
            warnings.append("Too few branded anchors (<15%) — add more to look natural")

        return {"distribution": actual, "target": ANCHOR_DISTRIBUTION, "warnings": warnings, "safe": len(warnings) == 0}

    def get_daily_batch(self, campaign: SwarmCampaign) -> list[ContentNode]:
        """Get nodes to deploy today (respecting velocity limits)."""
        if not campaign.plan:
            return []

        limit = VELOCITY_LIMITS.get(campaign.plan.velocity, 3)
        today = datetime.utcnow().strftime("%Y-%m-%d")

        if campaign.last_deploy_date == today:
            remaining = max(0, limit - campaign.daily_count)
        else:
            remaining = limit
            campaign.daily_count = 0
            campaign.last_deploy_date = today

        undeployed = campaign.plan.nodes[campaign.nodes_deployed:]
        batch = undeployed[:remaining]

        return batch

    def record_deploy(self, campaign: SwarmCampaign, count: int) -> None:
        """Record that nodes were deployed."""
        campaign.nodes_deployed += count
        campaign.daily_count += count

        if campaign.nodes_deployed >= campaign.plan.total_nodes:
            campaign.status = "completed"
            log.info("swarm.complete  keyword=%s  total=%d", campaign.keyword, campaign.nodes_deployed)
