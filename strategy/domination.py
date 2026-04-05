"""Market Domination Layer — control entire keyword markets, not just individual rankings.

Flow:
  1. Discover full keyword cluster from a primary keyword
  2. Map coverage gaps (which keywords have no page)
  3. Generate content deployment plan (service pages + blogs + guides)
  4. Build internal linking network (all pages cross-linked)
  5. Distribute authority (supporting pages → main pages)
  6. Monitor coverage % and reinforce weak spots

Trigger: primary keyword reaches top 5, or high-value niche identified.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from core.claude import call_claude

log = logging.getLogger(__name__)


class ClusterKeyword(BaseModel):
    keyword: str
    intent: str = ""           # informational / commercial / transactional / navigational
    search_volume: str = ""    # estimated if available
    difficulty: str = ""       # easy / medium / hard
    has_page: bool = False     # Do we already have a page for this?
    current_position: int = 0


class ContentAssignment(BaseModel):
    keyword: str
    page_type: str             # service / blog / guide / faq / comparison
    title: str = ""
    url_slug: str = ""
    priority: str = "medium"   # high / medium / low
    word_count_target: int = 0


class InternalLink(BaseModel):
    from_slug: str
    to_slug: str
    anchor_text: str
    weight: str = "normal"     # high / normal — high for main page links


class MarketCluster(BaseModel):
    core_keyword: str
    supporting: list[ClusterKeyword] = Field(default_factory=list)
    long_tail: list[ClusterKeyword] = Field(default_factory=list)
    total_keywords: int = 0
    covered: int = 0
    coverage_pct: float = 0.0


class DominationPlan(BaseModel):
    cluster: MarketCluster
    content_plan: list[ContentAssignment] = Field(default_factory=list)
    link_network: list[InternalLink] = Field(default_factory=list)
    authority_flow: list[InternalLink] = Field(default_factory=list)  # Supporting → main
    pages_to_create: int = 0
    estimated_timeline: str = ""


DOMINATION_PROMPT = """You are the Market Domination Agent. Take one keyword and map the ENTIRE search space around it.

Primary keyword: {keyword}
Business: {business_name}
Service: {service}
City: {city}
Current coverage: {existing_pages}

Generate a complete market domination plan:

1. KEYWORD CLUSTER — all related keywords people search for
   - Supporting keywords (2-3 words, commercial intent)
   - Long-tail keywords (4+ words, specific intent)
   - Include intent type for each (informational/commercial/transactional)

2. CONTENT PLAN — one page per keyword gap
   - Service pages for commercial keywords
   - Blog posts for informational keywords
   - Comparison/guide pages for research keywords
   - Include title + URL slug for each

3. INTERNAL LINKING — connect everything
   - Every page links to the main service page
   - Related pages link to each other
   - Use keyword-rich anchor text

Return ONLY JSON:
{{
  "cluster": {{
    "core_keyword": "{keyword}",
    "supporting": [
      {{"keyword": "", "intent": "commercial|informational|transactional", "difficulty": "easy|medium|hard"}}
    ],
    "long_tail": [
      {{"keyword": "", "intent": "", "difficulty": ""}}
    ]
  }},
  "content_plan": [
    {{
      "keyword": "",
      "page_type": "service|blog|guide|faq|comparison",
      "title": "",
      "url_slug": "",
      "priority": "high|medium|low",
      "word_count_target": 0
    }}
  ],
  "link_network": [
    {{"from_slug": "", "to_slug": "", "anchor_text": ""}}
  ],
  "estimated_timeline": ""
}}"""


class MarketDominator:
    """Plans and executes full market domination for a keyword cluster."""

    async def analyze_market(
        self,
        keyword: str,
        business_name: str,
        service: str,
        city: str,
        existing_pages: list[str] | None = None,
    ) -> DominationPlan:
        """Generate a full market domination plan."""
        pages_str = ", ".join(existing_pages[:10]) if existing_pages else "none yet"

        prompt = DOMINATION_PROMPT.format(
            keyword=keyword,
            business_name=business_name,
            service=service,
            city=city,
            existing_pages=pages_str,
        )

        try:
            raw = call_claude(
                prompt,
                system="You are a market strategy specialist. Return ONLY valid JSON.",
                max_tokens=4096,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            data = json.loads(raw)

            # Build cluster
            cluster_data = data.get("cluster", {})
            supporting = [ClusterKeyword(**k) for k in cluster_data.get("supporting", [])]
            long_tail = [ClusterKeyword(**k) for k in cluster_data.get("long_tail", [])]
            total = len(supporting) + len(long_tail) + 1  # +1 for core

            cluster = MarketCluster(
                core_keyword=keyword,
                supporting=supporting,
                long_tail=long_tail,
                total_keywords=total,
                covered=len(existing_pages) if existing_pages else 0,
                coverage_pct=round((len(existing_pages) / total * 100) if existing_pages and total > 0 else 0, 1),
            )

            # Build content plan
            content_plan = [ContentAssignment(**c) for c in data.get("content_plan", [])]

            # Build link network
            link_network = [InternalLink(**l) for l in data.get("link_network", [])]

            # Authority flow: all supporting pages → main page
            main_slug = keyword.lower().replace(" ", "-")
            authority_flow = [
                InternalLink(from_slug=c.url_slug, to_slug=main_slug, anchor_text=keyword, weight="high")
                for c in content_plan if c.url_slug != main_slug
            ]

            plan = DominationPlan(
                cluster=cluster,
                content_plan=content_plan,
                link_network=link_network,
                authority_flow=authority_flow,
                pages_to_create=len(content_plan),
                estimated_timeline=data.get("estimated_timeline", ""),
            )

            log.info("domination.planned  keyword=%s  total_kw=%d  pages=%d  links=%d",
                     keyword, total, len(content_plan), len(link_network))
            return plan

        except Exception as e:
            log.error("domination.fail  keyword=%s  err=%s", keyword, e)
            return DominationPlan(cluster=MarketCluster(core_keyword=keyword))

    def calculate_coverage(self, cluster: MarketCluster, existing_slugs: list[str]) -> float:
        """Calculate what % of the keyword cluster is covered by existing pages."""
        if cluster.total_keywords == 0:
            return 0

        covered = 0
        for kw in cluster.supporting + cluster.long_tail:
            slug = kw.keyword.lower().replace(" ", "-")
            if any(slug in s for s in existing_slugs):
                covered += 1
                kw.has_page = True

        cluster.covered = covered
        cluster.coverage_pct = round(covered / cluster.total_keywords * 100, 1)
        return cluster.coverage_pct

    def prioritize_gaps(self, plan: DominationPlan) -> list[ContentAssignment]:
        """Return content assignments sorted by priority (high first, then by type)."""
        type_order = {"service": 0, "guide": 1, "comparison": 2, "blog": 3, "faq": 4}
        priority_order = {"high": 0, "medium": 1, "low": 2}

        return sorted(
            plan.content_plan,
            key=lambda c: (priority_order.get(c.priority, 2), type_order.get(c.page_type, 5)),
        )
