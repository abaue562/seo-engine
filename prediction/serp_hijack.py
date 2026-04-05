"""SERP Hijack Engine — dominate multiple positions for a single keyword.

Instead of ranking one page, this builds a cluster:
  - Main service page (primary ranking target)
  - 2-3 supporting blog posts (different angles)
  - 1 authority/best-of page
  - Internal linking plan flowing authority to main page

Result: 3-5 positions in SERP instead of 1.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from core.claude import call_claude

log = logging.getLogger(__name__)

MAX_CLUSTERS_PER_14_DAYS = 1  # Safety limit


class ClusterPage(BaseModel):
    type: str              # service / blog / authority / faq
    title: str
    url_slug: str
    target_keyword: str = ""
    angle: str = ""
    content_outline: list[str] = []
    word_count_target: int = 0


class LinkPlan(BaseModel):
    from_slug: str
    to_slug: str
    anchor_text: str


class SERPCluster(BaseModel):
    keyword: str
    main_page: ClusterPage | None = None
    supporting_pages: list[ClusterPage] = Field(default_factory=list)
    authority_page: ClusterPage | None = None
    link_plan: list[LinkPlan] = Field(default_factory=list)
    total_pages: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)


HIJACK_PROMPT = """You are the SERP Hijack Agent. Your goal: dominate multiple positions in search results for ONE keyword.

Keyword: {keyword}
Business: {business_name}
Service: {service}
City: {city}
Current position: {position}

Build a page cluster that gives this business 3-5 ranking opportunities for "{keyword}".

Create:
1. MAIN PAGE — the primary ranking target (service page)
2. SUPPORTING PAGES — 2-3 blog posts with different angles (how-to, cost guide, comparison)
3. AUTHORITY PAGE — "best of" or comprehensive guide that other sites would link to
4. LINK PLAN — every supporting page links to the main page with keyword-rich anchors

Return ONLY JSON:
{{
  "main_page": {{
    "type": "service",
    "title": "",
    "url_slug": "",
    "target_keyword": "{keyword}",
    "angle": "primary service page",
    "content_outline": ["H2 section 1", "H2 section 2"],
    "word_count_target": 1500
  }},
  "supporting_pages": [
    {{
      "type": "blog",
      "title": "",
      "url_slug": "",
      "target_keyword": "",
      "angle": "",
      "content_outline": [],
      "word_count_target": 1000
    }}
  ],
  "authority_page": {{
    "type": "authority",
    "title": "",
    "url_slug": "",
    "target_keyword": "",
    "angle": "",
    "content_outline": [],
    "word_count_target": 2000
  }},
  "link_plan": [
    {{"from_slug": "", "to_slug": "", "anchor_text": ""}}
  ]
}}

Rules:
- Every page targets a DIFFERENT angle of the same keyword
- All supporting + authority pages link to main page
- Main page gets most internal link equity
- Titles must be unique and keyword-optimized
- URL slugs must be clean and descriptive"""


class SERPHijacker:
    """Builds page clusters for multi-position SERP domination."""

    async def plan_cluster(
        self,
        keyword: str,
        business_name: str,
        service: str,
        city: str,
        current_position: int = 0,
    ) -> SERPCluster:
        """Generate a full SERP hijack cluster plan."""
        prompt = HIJACK_PROMPT.format(
            keyword=keyword,
            business_name=business_name,
            service=service,
            city=city,
            position=current_position or "unranked",
        )

        try:
            raw = call_claude(
                prompt,
                system="You are an SEO cluster strategist. Return ONLY valid JSON. No other text.",
                max_tokens=2048,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            data = json.loads(raw)

            cluster = SERPCluster(
                keyword=keyword,
                main_page=ClusterPage(**data["main_page"]) if data.get("main_page") else None,
                supporting_pages=[ClusterPage(**p) for p in data.get("supporting_pages", [])],
                authority_page=ClusterPage(**data["authority_page"]) if data.get("authority_page") else None,
                link_plan=[LinkPlan(**l) for l in data.get("link_plan", [])],
            )
            cluster.total_pages = (
                (1 if cluster.main_page else 0)
                + len(cluster.supporting_pages)
                + (1 if cluster.authority_page else 0)
            )

            log.info("serp_hijack.planned  keyword=%s  pages=%d  links=%d",
                     keyword, cluster.total_pages, len(cluster.link_plan))
            return cluster

        except Exception as e:
            log.error("serp_hijack.fail  keyword=%s  err=%s", keyword, e)
            return SERPCluster(keyword=keyword)

    def should_activate(self, position: int, impressions: int = 0) -> bool:
        """Determine if SERP hijack should be activated for this keyword."""
        # Activate for page 2 keywords or high-impression unranked keywords
        return (5 <= position <= 15) or (position == 0 and impressions >= 200)

    @staticmethod
    def cluster_to_prompt_block(cluster: SERPCluster) -> str:
        lines = [
            f"SERP CLUSTER for '{cluster.keyword}':",
            f"  Total pages: {cluster.total_pages}",
        ]
        if cluster.main_page:
            lines.append(f"  Main: {cluster.main_page.title} ({cluster.main_page.url_slug})")
        for p in cluster.supporting_pages:
            lines.append(f"  Support: {p.title} ({p.url_slug}) — {p.angle}")
        if cluster.authority_page:
            lines.append(f"  Authority: {cluster.authority_page.title} ({cluster.authority_page.url_slug})")
        lines.append(f"  Internal links: {len(cluster.link_plan)}")
        return "\n".join(lines)
