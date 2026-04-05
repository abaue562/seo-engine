"""Script Authority Swarm — deterministic node plan, zero LLM calls.

Node types and platforms are always the same for local service businesses.
Only content_angle needs customization — we template that from keyword + service.

Anchor distribution enforces the 20/30/30/20 rule automatically.
"""

from __future__ import annotations

import logging
import random
from typing import NamedTuple

from authority.swarm import (
    ContentNode, SwarmPlan,
    ANCHOR_DISTRIBUTION, VELOCITY_LIMITS,
)

log = logging.getLogger(__name__)


class _NodeTemplate(NamedTuple):
    type: str
    platform_pattern: str   # use {city}/{service}/{biz} placeholders
    angle_pattern: str
    link_type: str
    anchor_category: str
    tier: int


# Pre-defined node library for local service businesses
# Covers every platform category that matters for local SEO authority
_NODE_LIBRARY = [
    _NodeTemplate(
        type="directory",
        platform_pattern="HomeStars / Houzz Business Profile (contractor directory listing)",
        angle_pattern="Business credibility listing — {svc} services, service area, and portfolio photos",
        link_type="directory",
        anchor_category="brand",
        tier=1,
    ),
    _NodeTemplate(
        type="directory",
        platform_pattern="Yelp business listing — {city} home services",
        angle_pattern="Review platform listing with service description, photos, and NAP for {svc} in {city}",
        link_type="directory",
        anchor_category="brand",
        tier=1,
    ),
    _NodeTemplate(
        type="article",
        platform_pattern="Local lifestyle blog (e.g., {city} Life Magazine / Okanagan Living)",
        angle_pattern="Year-round exterior value: why {city} homeowners are choosing {svc} over seasonal alternatives",
        link_type="contextual",
        anchor_category="exact",
        tier=1,
    ),
    _NodeTemplate(
        type="mention",
        platform_pattern="Q&A Forum (Quora topic: Home Improvement / Outdoor Lighting)",
        angle_pattern="Answering 'Are {svc} worth the investment?' with a local {city} perspective",
        link_type="contextual",
        anchor_category="partial",
        tier=1,
    ),
    _NodeTemplate(
        type="article",
        platform_pattern="Local news / community publication (Castanet.net or {city}Now.com contributor post)",
        angle_pattern="{city} home exterior trends — {svc} leads renovation wish lists this year",
        link_type="contextual",
        anchor_category="partial",
        tier=1,
    ),
    _NodeTemplate(
        type="guest_post",
        platform_pattern="Home renovation / DIY blog (Canadian Home Workshop, Better Homes Canada contributor)",
        angle_pattern="How {svc} cut long-term costs while improving curb appeal year-round",
        link_type="contextual",
        anchor_category="brand",
        tier=1,
    ),
    _NodeTemplate(
        type="blog",
        platform_pattern="Real estate / home staging blog (local {city} REALTOR blog or Okanagan property site)",
        angle_pattern="Curb appeal investments that increase perceived home value in the {city} market",
        link_type="contextual",
        anchor_category="partial",
        tier=1,
    ),
    _NodeTemplate(
        type="resource",
        platform_pattern="Industry resource page (Electrical Contractor or lighting trade directory)",
        angle_pattern="Safety and installation standards for permanent LED lighting in Canadian climates",
        link_type="resource",
        anchor_category="generic",
        tier=1,
    ),
    _NodeTemplate(
        type="resource",
        platform_pattern="Community resource page ({city} neighbourhood association or home improvement hub)",
        angle_pattern="Local vendor resource guide — where {city} residents find {svc} services",
        link_type="resource",
        anchor_category="generic",
        tier=1,
    ),
    _NodeTemplate(
        type="directory",
        platform_pattern="BBB (Better Business Bureau Canada) accreditation listing",
        angle_pattern="Trust signal listing with accreditation badge, business description, and service area",
        link_type="directory",
        anchor_category="brand",
        tier=1,
    ),
    _NodeTemplate(
        type="article",
        platform_pattern="Medium publication — Home Design & Renovation",
        angle_pattern="The case for {svc}: why smart homeowners in {city} are making the switch",
        link_type="contextual",
        anchor_category="partial",
        tier=2,
    ),
    _NodeTemplate(
        type="mention",
        platform_pattern="Reddit r/{city_lower} community post",
        angle_pattern="Sharing personal experience with {svc} install — cost breakdown and honest review",
        link_type="contextual",
        anchor_category="generic",
        tier=2,
    ),
]

# Anchor text templates by category
_ANCHOR_TEMPLATES = {
    "exact": [
        "{keyword}",
        "{keyword} services",
        "{keyword} {city}",
    ],
    "partial": [
        "{svc} in {city}",
        "professional {svc}",
        "{city} {svc} installation",
        "{svc} company",
    ],
    "brand": [
        "{biz}",
        "{biz} {city}",
        "{biz} — {svc}",
    ],
    "generic": [
        "learn more about their installation process",
        "visit their {svc} services page",
        "this company",
        "click here for pricing",
        "see their portfolio",
    ],
}


def _make_anchor(category: str, keyword: str, svc: str, city: str, biz: str) -> str:
    templates = _ANCHOR_TEMPLATES.get(category, _ANCHOR_TEMPLATES["generic"])
    tmpl = random.choice(templates)
    return tmpl.format(
        keyword=keyword, svc=svc.lower(), city=city,
        biz=biz, city_lower=city.lower()
    )


def _fill(pattern: str, keyword: str, svc: str, city: str, biz: str) -> str:
    return pattern.format(
        keyword=keyword, svc=svc.lower(), city=city,
        biz=biz, city_lower=city.lower()
    )


def plan_swarm_script(
    keyword: str,
    target_page: str,
    business_name: str,
    city: str,
    service: str,
    velocity: str = "medium",
) -> SwarmPlan:
    """Build authority swarm plan deterministically — no LLM.

    Selects nodes from the pre-defined library, assigns anchor text
    following the 20/30/30/20 distribution rule, and calculates timeline.
    """
    node_count = {"low": 5, "medium": 8, "high": 12}.get(velocity, 8)

    # Shuffle for variety but keep distribution intact
    shuffled = list(_NODE_LIBRARY)
    random.shuffle(shuffled)
    selected_templates = shuffled[:node_count]

    # Sort by anchor_category to enforce distribution
    # Target: ~20% exact, ~30% partial, ~30% brand, ~20% generic
    distribution_plan = []
    n = node_count
    counts = {
        "exact":   max(1, round(n * 0.20)),
        "partial": max(1, round(n * 0.30)),
        "brand":   max(1, round(n * 0.30)),
        "generic": max(1, round(n * 0.20)),
    }
    # Adjust to hit total exactly
    while sum(counts.values()) < n:
        counts["partial"] += 1
    while sum(counts.values()) > n:
        counts["generic"] -= 1

    # Assign categories in order
    category_queue = []
    for cat, cnt in counts.items():
        category_queue.extend([cat] * cnt)

    nodes: list[ContentNode] = []
    anchor_mix: dict[str, int] = {}

    for i, tmpl in enumerate(selected_templates):
        cat = category_queue[i] if i < len(category_queue) else tmpl.anchor_category
        anchor = _make_anchor(cat, keyword, service, city, business_name)

        node = ContentNode(
            type=tmpl.type,
            platform=_fill(tmpl.platform_pattern, keyword, service, city, business_name),
            content_angle=_fill(tmpl.angle_pattern, keyword, service, city, business_name),
            link_type=tmpl.link_type,
            anchor_text=anchor,
            anchor_category=cat,
            target_url=target_page,
            tier=tmpl.tier,
        )
        nodes.append(node)
        anchor_mix[cat] = anchor_mix.get(cat, 0) + 1

    # Link distribution: 60% target page, 30% supporting, 10% homepage
    link_distribution = {
        "target_page": 0.60,
        "supporting_pages": 0.30,
        "homepage": 0.10,
    }

    days_per_node = max(1, 1 / VELOCITY_LIMITS.get(velocity, 3))
    estimated_days = max(7, int(node_count * days_per_node * 1.5))

    plan = SwarmPlan(
        keyword=keyword,
        target_page=target_page,
        nodes=nodes,
        link_distribution=link_distribution,
        velocity=velocity,
        total_nodes=len(nodes),
        estimated_days=estimated_days,
        anchor_mix=anchor_mix,
    )

    log.info("swarm_script.planned  keyword=%s  nodes=%d  velocity=%s  days=%d",
             keyword, len(nodes), velocity, estimated_days)
    return plan
