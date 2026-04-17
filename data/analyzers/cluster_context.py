"""Cluster context utility for content generation.

Usage:
    from data.analyzers.cluster_context import get_cluster_context
    ctx = get_cluster_context("gutter guard installation Kelowna", "75354f9d")
    # Returns: {cluster_slug, pillar_slug, existing_members, link_to, link_from}
"""

from __future__ import annotations
import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

_CLUSTERS_PATH = Path("data/storage/clusters.json")
_CLUSTERS_CACHE: dict | None = None


def _load_clusters() -> dict:
    global _CLUSTERS_CACHE
    if _CLUSTERS_CACHE is None and _CLUSTERS_PATH.exists():
        _CLUSTERS_CACHE = json.loads(_CLUSTERS_PATH.read_text())
    return _CLUSTERS_CACHE or {"clusters": []}


def _keyword_to_cluster(keyword: str) -> dict | None:
    """Find the best matching cluster for a keyword."""
    kw_lower = keyword.lower()
    data = _load_clusters()
    best = None
    best_score = 0
    for cluster in data.get("clusters", []):
        # Check pillar keyword
        if cluster.get("pillar_keyword", "").lower() in kw_lower:
            return cluster
        # Check supporting keywords
        for skw in cluster.get("supporting_keywords", []):
            if skw.lower() in kw_lower or kw_lower in skw.lower():
                # Longer overlap = better match
                score = len(set(kw_lower.split()) & set(skw.lower().split()))
                if score > best_score:
                    best_score = score
                    best = cluster
        # Slug-based match
        if cluster.get("slug", "") in kw_lower:
            best_score = max(best_score, 2)
            best = cluster
    return best


def get_cluster_context(keyword: str, business_id: str) -> dict:
    """Return cluster context for injecting into the generate_content prompt.

    Args:
        keyword:     Target keyword for the new article.
        business_id: Business UUID.

    Returns:
        dict with keys: found, cluster_slug, pillar_slug, supporting_count,
        link_targets (list of {slug, anchor}), prompt_block (str ready to inject).
    """
    cluster = _keyword_to_cluster(keyword)
    if not cluster:
        return {"found": False, "prompt_block": ""}

    pillar_slug = cluster.get("pillar_slug", "")
    supporting_slugs = cluster.get("supporting_slugs", [])
    cluster_slug = cluster.get("slug", "")

    # Build 2-3 internal link suggestions
    link_targets = []
    if pillar_slug and pillar_slug not in keyword.lower().replace(" ", "-"):
        link_targets.append({
            "slug": f"/{pillar_slug}/",
            "anchor": cluster.get("display_name", cluster_slug),
            "rel": "pillar",
        })
    for slug in supporting_slugs[:3]:
        link_targets.append({
            "slug": f"/{slug}/",
            "anchor": slug.replace("-", " ").title(),
            "rel": "sibling",
        })

    link_lines = "\n".join(
        f"- Link TO: /{t['slug'].strip('/')}/ (anchor: \"{t['anchor']}\")" 
        for t in link_targets[:4]
    )

    prompt_block = (
        f"\nCLUSTER CONTEXT — this article belongs to the \"{cluster.get('display_name')}\" cluster.\n"
        f"Pillar page: /{pillar_slug}/\n"
        f"Required internal links (include AT LEAST 2 of these as contextual inline links):\n"
        f"{link_lines}\n"
        f"Use {{LINK:anchor text:relative/path}} placeholders for internal links."
    )

    return {
        "found": True,
        "cluster_slug": cluster_slug,
        "pillar_slug": pillar_slug,
        "display_name": cluster.get("display_name", ""),
        "supporting_count": len(supporting_slugs),
        "link_targets": link_targets,
        "prompt_block": prompt_block,
    }
