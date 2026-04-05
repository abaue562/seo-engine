"""Entity Dominance System — make Google understand the business as THE authority.

Google is moving from pages to entities. This engine builds entity signals:
  - Consistent brand mentions across platforms
  - Structured data (schema markup) everywhere
  - Knowledge panel optimization
  - Brand + service association

Result: Google treats the business as the dominant entity for its services in its area.
"""

from __future__ import annotations

import json
import logging
from core.claude import call_claude, call_claude_json, call_claude_raw


from models.business import BusinessContext
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)


class EntityProfile(BaseModel):
    """The entity profile Google should build for this business."""
    name: str
    type: str = "LocalBusiness"
    services: list[str] = []
    service_area: list[str] = []
    schema_types: list[str] = []      # LocalBusiness, Service, FAQPage, etc.
    platform_presence: dict[str, str] = {}  # platform → URL/status
    consistency_score: float = 0.0     # NAP consistency 0-100
    mentions_needed: list[str] = []    # Where we need more mentions
    schema_gaps: list[str] = []        # Missing schema types


class EntityAction(BaseModel):
    """Specific action to build entity dominance."""
    action: str
    target: str
    why: str
    impact: str = "high"
    implementation: str


ENTITY_AUDIT_PROMPT = """You are the Entity Dominance Agent.

Google is moving from ranking pages to ranking ENTITIES.
Your job is to make this business the dominant entity for its services in its area.

Business: {business_name}
Website: {website}
Services: {services}
City: {city}
Service Areas: {service_areas}

Analyze and output:

1. Entity Profile Assessment
   - What entity does Google currently associate with this business?
   - What's missing?

2. Schema Markup Gaps
   - What structured data types should be on the website?
   - LocalBusiness, Service, FAQPage, Review, etc.

3. Platform Presence Gaps
   - Where does the business need presence for entity confirmation?
   - Google, Yelp, BBB, industry directories, social profiles

4. NAP Consistency Issues
   - Common problems with Name/Address/Phone across platforms

5. Entity Actions (max 5)
   - Specific actions to strengthen entity signals

Return ONLY JSON:
{{
  "entity_assessment": "",
  "schema_gaps": [],
  "platform_gaps": [],
  "nap_issues": [],
  "actions": [
    {{
      "action": "",
      "target": "",
      "why": "",
      "impact": "high | medium",
      "implementation": ""
    }}
  ]
}}"""


class EntityEngine:
    """Builds and maintains entity dominance for a business."""

    def __init__(self):
        pass


    async def audit(self, business: BusinessContext) -> tuple[EntityProfile, list[EntityAction]]:
        """Run entity dominance audit and generate actions."""
        prompt = ENTITY_AUDIT_PROMPT.format(
            business_name=business.business_name,
            website=business.website,
            services=", ".join([business.primary_service] + business.secondary_services),
            city=business.primary_city,
            service_areas=", ".join(business.service_areas),
        )

        data = {}
        try:
            from core.claude import call_claude as _call_claude
            raw = _call_claude(
                prompt,
                system="You are an entity SEO specialist. Return ONLY valid JSON, no other text.",
                max_tokens=2048,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            start = raw.find("{")
            if start >= 0:
                raw = raw[start:]
            if raw:
                data, _ = json.JSONDecoder().raw_decode(raw)
        except Exception as e:
            log.warning("entity.audit_claude_fail  err=%s  using_fallback=True", e)

        # Rule-based fallback when Claude returns nothing or fails
        if not data:
            data = self._rule_based_audit(business)

        def _to_str_list(items: list) -> list[str]:
            """Coerce a list of str or dict to list[str]."""
            result = []
            for item in items:
                if isinstance(item, str):
                    result.append(item)
                elif isinstance(item, dict):
                    # Extract most informative field
                    label = (
                        item.get("type") or item.get("platform") or
                        item.get("name") or item.get("action") or str(item)
                    )
                    detail = item.get("priority") or item.get("description") or ""
                    result.append(f"{label}: {detail}".strip(": ") if detail else str(label))
            return result

        profile = EntityProfile(
            name=business.business_name,
            services=[business.primary_service] + business.secondary_services,
            service_area=business.service_areas,
            schema_gaps=_to_str_list(data.get("schema_gaps", [])),
            mentions_needed=_to_str_list(data.get("platform_gaps", [])),
        )

        actions = []
        for a in data.get("actions", []):
            try:
                # Coerce any list fields to strings
                if isinstance(a.get("execution"), list):
                    a["execution"] = " ".join(str(x) for x in a["execution"])
                if isinstance(a.get("why"), list):
                    a["why"] = " ".join(str(x) for x in a["why"])
                actions.append(EntityAction(**a))
            except Exception:
                pass

        log.info("entity.audit  biz=%s  gaps=%d  actions=%d  source=%s",
                 business.business_name, len(profile.schema_gaps), len(actions),
                 "claude" if data.get("_source") != "fallback" else "fallback")
        return profile, actions

    @staticmethod
    def _rule_based_audit(business: BusinessContext) -> dict:
        """Generate entity recommendations without Claude — fast, deterministic."""
        name = business.business_name
        city = business.primary_city
        svc  = business.primary_service
        services = [svc] + business.secondary_services

        schema_gaps = [
            "LocalBusiness (add @type, name, url, telephone, address, openingHours)",
            f"Service (separate schema block for each service: {', '.join(services[:3])})",
            "FAQPage (add FAQ schema to any page with Q&A content)",
            "AggregateRating (wire in your review data — boosts CTR from SERP)",
            "BreadcrumbList (navigation breadcrumbs — helps Google parse site structure)",
        ]

        platform_gaps = [
            f"Google Business Profile — fully optimized for {city}",
            "Yelp — claimed and verified with service categories",
            "Better Business Bureau (BBB) — accreditation + listing",
            "HomeStars / Houzz — service provider profile",
            "Local Chamber of Commerce directory",
            "Yellow Pages Canada — NAP citation",
            f"{city} business directory listing",
            "LinkedIn company page",
        ]

        actions = [
            {
                "action": "Add comprehensive LocalBusiness + Service schema to every page",
                "target": business.website,
                "why": "Google uses structured data to confirm entity identity — missing schema = weaker entity signals",
                "impact": "high",
                "implementation": (
                    f"Add JSON-LD block: @type LocalBusiness, name '{name}', "
                    f"url '{business.website}', areaServed {business.service_areas}. "
                    "Add Service schema blocks for each service. Validate at schema.org/validator."
                ),
            },
            {
                "action": f"Claim and fully optimize all directory listings for NAP consistency",
                "target": f"{name} — Google, Yelp, BBB, HomeStars",
                "why": "Inconsistent NAP across directories confuses Google's entity graph and dilutes local authority",
                "impact": "high",
                "implementation": (
                    f"Name MUST be exactly '{name}' everywhere. "
                    f"Address must match Google Business Profile exactly. "
                    "Use a tool like BrightLocal to audit consistency."
                ),
            },
            {
                "action": "Add AggregateRating schema using your review data",
                "target": business.website,
                "why": f"You have {business.reviews_count} reviews at {business.rating}★ — schema markup makes this visible in SERP, boosting CTR",
                "impact": "high",
                "implementation": (
                    f"Add @type AggregateRating with ratingValue={business.rating}, "
                    f"reviewCount={business.reviews_count}. "
                    "Place in LocalBusiness schema block."
                ),
            },
            {
                "action": f"Build Wikipedia-style 'about' entity content for {name}",
                "target": f"{business.website}/about",
                "why": "Google's Knowledge Panel is seeded from about pages and third-party mentions — clear entity definition = higher chance of Knowledge Panel",
                "impact": "medium",
                "implementation": (
                    f"Write a 300-word factual About page: when founded, service area ({city}), "
                    f"services ({svc}), mission, certifications. "
                    "Avoid marketing language — write like Wikipedia."
                ),
            },
            {
                "action": "Create a Wikidata entity entry for the business",
                "target": "https://www.wikidata.org/wiki/Special:NewItem",
                "why": "Wikidata is a primary source for Google's Knowledge Graph — having an entry dramatically increases Knowledge Panel eligibility",
                "impact": "medium",
                "implementation": (
                    f"Create entry: label='{name}', instance of=business, "
                    f"country=Canada, location={city}, "
                    f"official website={business.website}. "
                    "Link to your Google Business Profile and Wikipedia if mentioned."
                ),
            },
        ]

        return {
            "_source": "fallback",
            "entity_assessment": (
                f"{name} has basic entity presence in {city} but lacks structured data, "
                "Knowledge Panel, and consistent cross-platform NAP. "
                "Completing schema + platform presence will strengthen Google's entity understanding."
            ),
            "schema_gaps": schema_gaps,
            "platform_gaps": platform_gaps,
            "nap_issues": [
                "Verify business name spelling is identical across all platforms",
                "Ensure phone number format is consistent (use local format: 250-XXX-XXXX)",
                "Address must be identical: use Google Maps format as source of truth",
            ],
            "actions": actions,
        }

    @staticmethod
    def generate_schema_markup(business: BusinessContext) -> dict:
        """Generate comprehensive schema.org JSON-LD for the business."""
        return {
            "@context": "https://schema.org",
            "@type": "LocalBusiness",
            "name": business.business_name,
            "url": business.website,
            "telephone": "",
            "address": {
                "@type": "PostalAddress",
                "addressLocality": business.primary_city,
            },
            "areaServed": [
                {"@type": "City", "name": area} for area in business.service_areas
            ],
            "aggregateRating": {
                "@type": "AggregateRating",
                "ratingValue": str(business.rating),
                "reviewCount": str(business.reviews_count),
            },
            "makesOffer": [
                {
                    "@type": "Offer",
                    "itemOffered": {
                        "@type": "Service",
                        "name": svc,
                    }
                }
                for svc in [business.primary_service] + business.secondary_services
            ],
        }
