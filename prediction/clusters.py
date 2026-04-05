"""Keyword Cluster Engine — builds topic clusters for SERP domination.

Instead of targeting 1 keyword with 1 page, builds:
  main page + supporting pages + internal links

This creates topical authority that Google rewards.
"""

from __future__ import annotations

import json
import logging
from core.claude import call_claude, call_claude_json, call_claude_raw


from prediction.models import KeywordCluster

log = logging.getLogger(__name__)


CLUSTER_PROMPT = """Generate a keyword cluster for SERP domination.

Primary keyword: {keyword}
City: {city}
Service: {service}

Create a complete keyword cluster including:
1. 5-8 long-tail keyword variations
2. 3-5 local modifier variations (neighborhood, "near me", zip codes)
3. 3-5 high-intent variations (with "cost", "best", "emergency", "near me")
4. Suggested cluster pages (main page + 3-5 supporting pages)

Return ONLY JSON:
{{
  "primary_keyword": "",
  "variants": [],
  "long_tail": [],
  "local_modifiers": [],
  "high_intent": [],
  "cluster_pages": [
    {{
      "slug": "",
      "title": "",
      "target_keyword": "",
      "role": "main | support"
    }}
  ]
}}"""


class ClusterEngine:
    """Builds keyword clusters for topical domination."""

    def __init__(self):
        pass


    async def build_cluster(self, keyword: str, city: str, service: str) -> KeywordCluster:
        """Generate a full keyword cluster via Claude."""
        prompt = CLUSTER_PROMPT.format(keyword=keyword, city=city, service=service)

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
            cluster = KeywordCluster(
                primary_keyword=data.get("primary_keyword", keyword),
                variants=data.get("variants", []),
                long_tail=data.get("long_tail", []),
                local_modifiers=data.get("local_modifiers", []),
                high_intent=data.get("high_intent", []),
                cluster_pages=[p.get("slug", "") for p in data.get("cluster_pages", [])],
            )
            log.info("cluster.built  keyword=%s  variants=%d  pages=%d",
                     keyword, len(cluster.variants), len(cluster.cluster_pages))
            return cluster

        except Exception as e:
            log.error("cluster.fail  keyword=%s  err=%s", keyword, e)
            return KeywordCluster(primary_keyword=keyword)

    @staticmethod
    def cluster_to_prompt_block(cluster: KeywordCluster) -> str:
        """Render cluster as agent context."""
        lines = [
            f"KEYWORD CLUSTER for '{cluster.primary_keyword}':",
            f"  Variants: {', '.join(cluster.variants[:5])}",
            f"  Long-tail: {', '.join(cluster.long_tail[:5])}",
            f"  Local: {', '.join(cluster.local_modifiers[:5])}",
            f"  High-intent: {', '.join(cluster.high_intent[:5])}",
            f"  Cluster pages: {len(cluster.cluster_pages)}",
        ]
        return "\n".join(lines)
