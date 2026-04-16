"""Template engine for programmatic content generation.

Produces sufficiently differentiated content for each location×service
combination to avoid thin content penalties. Uses structural variation
and local data injection.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.programmatic.generator import ProgrammaticPage

log = logging.getLogger(__name__)

# Real local data for major cities — used to add specificity
_CITY_DATA: dict[str, dict] = {
    "New York": {"climate": "humid continental", "avg_income": "$67,000", "note": "one of the world's busiest metros"},
    "Los Angeles": {"climate": "Mediterranean", "avg_income": "$62,000", "note": "sprawling metro with intense heat seasons"},
    "Chicago": {"climate": "continental with harsh winters", "avg_income": "$58,000", "note": "known for extreme temperature swings"},
    "Houston": {"climate": "humid subtropical", "avg_income": "$52,000", "note": "subject to flooding and hurricane impacts"},
    "Phoenix": {"climate": "desert (110°F+ summers)", "avg_income": "$56,000", "note": "extreme heat stresses home systems"},
    "Philadelphia": {"climate": "humid subtropical", "avg_income": "$45,000", "note": "older housing stock with frequent plumbing issues"},
    "San Antonio": {"climate": "subtropical", "avg_income": "$50,000", "note": "rapid growth creating strong service demand"},
    "San Diego": {"climate": "Mediterranean", "avg_income": "$71,000", "note": "coastal city with strong year-round demand"},
    "Dallas": {"climate": "humid subtropical", "avg_income": "$54,000", "note": "fast-growing metro with high service demand"},
    "San Jose": {"climate": "Mediterranean", "avg_income": "$110,000", "note": "high-income tech hub"},
    "Austin": {"climate": "subtropical", "avg_income": "$72,000", "note": "fastest-growing large city in the US"},
    "Seattle": {"climate": "oceanic with heavy rain", "avg_income": "$92,000", "note": "wet climate drives high maintenance needs"},
    "Denver": {"climate": "semi-arid", "avg_income": "$68,000", "note": "extreme freeze-thaw cycles stress pipes"},
    "Nashville": {"climate": "humid subtropical", "avg_income": "$55,000", "note": "booming growth creating strong demand"},
    "Las Vegas": {"climate": "desert", "avg_income": "$54,000", "note": "extreme heat and dry conditions"},
    "Atlanta": {"climate": "humid subtropical", "avg_income": "$59,000", "note": "rapidly growing metro with aging infrastructure"},
    "Miami": {"climate": "tropical", "avg_income": "$43,000", "note": "high humidity drives frequent HVAC needs"},
    "Minneapolis": {"climate": "continental (severe winters)", "avg_income": "$59,000", "note": "frozen pipe risk is extremely high"},
    "Portland": {"climate": "oceanic", "avg_income": "$62,000", "note": "wet winters create moisture-related issues"},
    "Tampa": {"climate": "tropical", "avg_income": "$52,000", "note": "hurricane season creates high emergency demand"},
    "St. Louis": {"climate": "continental", "avg_income": "$46,000", "note": "significant temperature swings year-round"},
    "Baltimore": {"climate": "humid subtropical", "avg_income": "$51,000", "note": "older rowhouse stock with frequent service needs"},
    "Raleigh": {"climate": "humid subtropical", "avg_income": "$66,000", "note": "tech-corridor growth driving residential demand"},
    "Salt Lake City": {"climate": "semi-arid", "avg_income": "$57,000", "note": "rapid population growth strains services"},
}


@dataclass
class LocalData:
    city: str
    state: str
    population: int
    county: str
    timezone: str
    avg_income_estimate: str = "$55,000"
    climate_note: str = "temperate"
    local_competitor_note: str = ""

    @classmethod
    def for_city(cls, city: str, state: str, population: int, county: str, timezone: str) -> "LocalData":
        extra = _CITY_DATA.get(city, {})
        return cls(
            city=city,
            state=state,
            population=population,
            county=county,
            timezone=timezone,
            avg_income_estimate=extra.get("avg_income", "$55,000"),
            climate_note=extra.get("climate", "temperate"),
            local_competitor_note=extra.get("note", ""),
        )


@dataclass
class ContentTemplate:
    name: str
    structure: list[str]
    intro_variants: list[str]
    cta_position: str  # top | mid | bottom | all


class TemplateEngine:
    """Selects and applies content templates for programmatic pages."""

    TEMPLATES: list[ContentTemplate] = [
        ContentTemplate(
            name="local_service",
            structure=["intro", "why_choose", "services_offered", "service_area", "faq", "cta"],
            intro_variants=[
                "When you need {service} in {city}, you deserve fast, reliable service from licensed professionals who know {county} County.",
                "Finding a trusted {service} provider in {city}, {state} shouldn't be stressful. Here's what to look for and who to call.",
                "Residents of {city} trust local {service} experts for everything from routine maintenance to emergency repairs.",
            ],
            cta_position="all",
        ),
        ContentTemplate(
            name="emergency",
            structure=["urgency_intro", "response_time", "services", "pricing_guide", "faq", "cta"],
            intro_variants=[
                "{city} homeowners facing an emergency need {service} help now — not tomorrow.",
                "A {service} emergency in {city} can't wait. Our licensed technicians serve {county} County 24 hours a day.",
                "When {service} problems strike in {city}, every minute matters. Same-day response guaranteed.",
            ],
            cta_position="top",
        ),
        ContentTemplate(
            name="comparison",
            structure=["intro", "vs_competitors", "pricing_table", "reviews", "faq", "cta"],
            intro_variants=[
                "Choosing the right {service} company in {city} means comparing prices, licensing, and reviews.",
                "With dozens of {service} providers in {city}, {state}, here's how to find the best value.",
                "Before you hire a {service} company in {city}, compare these key factors that affect quality and cost.",
            ],
            cta_position="bottom",
        ),
        ContentTemplate(
            name="how_to_guide",
            structure=["intro", "step_by_step", "when_to_call", "cost_breakdown", "faq", "cta"],
            intro_variants=[
                "Understanding {service} in {city} starts with knowing what's involved, what it costs, and when to DIY vs. call a pro.",
                "This guide covers everything {city} homeowners need to know about {service} — from warning signs to fair pricing.",
                "Whether you're a new homeowner in {city} or dealing with your first {service} issue, this guide covers every step.",
            ],
            cta_position="mid",
        ),
        ContentTemplate(
            name="location_authority",
            structure=["city_intro", "local_stats", "services", "service_radius", "faq", "cta"],
            intro_variants=[
                "{city} is home to approximately {population:,} residents in {county} County — and all of them need reliable {service} services.",
                "As {city}'s trusted {service} experts, we understand the unique challenges of {climate} climate on local homes.",
                "Serving {city} and surrounding {county} County communities, our {service} team knows local code requirements and building styles.",
            ],
            cta_position="bottom",
        ),
    ]

    def select_template(self, intent: str, modifier: str = "") -> ContentTemplate:
        """Pick the best template for the intent and modifier."""
        if modifier in ("emergency", "24/7", "urgent", "same day"):
            return next(t for t in self.TEMPLATES if t.name == "emergency")
        if modifier in ("cost", "price", "how much", "compare", "best"):
            return next(t for t in self.TEMPLATES if t.name == "comparison")
        if intent == "informational":
            return next(t for t in self.TEMPLATES if t.name == "how_to_guide")
        if intent == "transactional" and not modifier:
            return next(t for t in self.TEMPLATES if t.name == "location_authority")
        return next(t for t in self.TEMPLATES if t.name == "local_service")

    def build_prompt(
        self,
        page: "ProgrammaticPage",
        local_data: LocalData,
        template: ContentTemplate,
    ) -> str:
        """Build a Claude prompt for generating this programmatic page."""
        # Pick an intro variant based on page index (hash of slug for consistency)
        intro_idx = hash(page.slug) % len(template.intro_variants)
        intro_hint = template.intro_variants[intro_idx].format(
            service=page.service,
            city=local_data.city,
            state=local_data.state,
            county=local_data.county,
            climate=local_data.climate_note,
            population=local_data.population,
        )
        structure_str = " → ".join(template.structure)

        return f"""Generate a {page.target_words}-word SEO page for:

KEYWORD: "{page.keyword}"
PAGE TYPE: {page.page_type}
INTENT: {page.intent}
CITY: {page.city}, {page.state}
COUNTY: {local_data.county}
POPULATION: {local_data.population:,}
CLIMATE: {local_data.climate_note}
AVG HOUSEHOLD INCOME: {local_data.avg_income_estimate}

CONTENT STRUCTURE: {structure_str}

START WITH THIS INTRO (rewrite naturally, don't copy verbatim):
"{intro_hint}"

MANDATORY REQUIREMENTS:
1. Direct answer in first 100 words: factual statement about {page.service} in {page.city}
2. Mention {local_data.county} County naturally at least twice
3. Reference the local climate ({local_data.climate_note}) and how it affects {page.service} needs
4. Include a pricing section with realistic cost ranges for {page.city} area
5. FAQ section: 5 Q&As using questions residents of {page.city} actually ask
6. Include {{{{LINK:anchor:related-slug}}}} placeholder for 2 internal links
7. End with clear CTA referencing {page.city} and {page.state}
8. Minimum {page.target_words} words
9. Schema: LocalBusiness + FAQPage
10. One original statistic about {page.service} or the local market

IMPORTANT: Do NOT make this generic. Mention {page.city}-specific details.
Mention that service area includes surrounding {local_data.county} County cities.

Return valid JSON:
{{
  "title": "60 char max",
  "meta_description": "155 char max",
  "slug": "{page.slug}",
  "h1": "exact H1",
  "direct_answer": "100-word direct answer",
  "content_html": "full HTML with all sections",
  "faq": [{{"question": "", "answer": ""}}],
  "word_count": 0,
  "schema_json": {{}},
  "original_data_point": ""
}}"""

    def inject_local_data(self, content_html: str, local_data: LocalData) -> str:
        """Replace template placeholders with real local data."""
        replacements = {
            "{{CITY}}": local_data.city,
            "{{STATE}}": local_data.state,
            "{{POPULATION}}": f"{local_data.population:,}",
            "{{COUNTY}}": local_data.county,
            "{{CLIMATE}}": local_data.climate_note,
            "{{AVG_INCOME}}": local_data.avg_income_estimate,
        }
        for placeholder, value in replacements.items():
            content_html = content_html.replace(placeholder, value)

        # Add service area note if not present
        service_area_note = f"Serving {local_data.city} and surrounding {local_data.county} County"
        if service_area_note.lower().replace(" ", "") not in content_html.lower().replace(" ", ""):
            content_html += f'\n<p class="service-area"><strong>Service Area:</strong> {service_area_note}.</p>'

        return content_html

    def get_local_data(self, city: str, state: str, population: int = 100_000, county: str = "", timezone: str = "America/New_York") -> LocalData:
        """Return LocalData for a city."""
        return LocalData.for_city(city, state, population, county, timezone)
