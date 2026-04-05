"""Page Generator v2 — Claude generates structured page data WITH image queries, renderer builds HTML."""

from __future__ import annotations

import json
import logging

from core.claude import call_claude
from execution.renderers.page_renderer import render_page

log = logging.getLogger(__name__)


PAGE_PROMPT = """You are a conversion-focused web page architect for a LOCAL SERVICE BUSINESS.

Keyword: {keyword}
Business: {business_name}
Service: {service}
City: {city}
Service Areas: {service_areas}
Reviews: {reviews} at {rating} stars
Years Active: {years}

Generate a COMPLETE page structure. For EVERY section that should have an image, include an "image_query" field with a SPECIFIC search term that would find a photo matching that exact section content.

Image query rules:
- Be VERY specific to the actual service (e.g. "permanent LED roof lights house night" not "house")
- Include the actual product/service being shown (e.g. "landscape pathway lighting garden" not "garden")
- For before/after: "dark house no outdoor lights" vs "house with permanent roof LED lights night"
- For hero: show the FINISHED result of the service looking stunning
- NO generic queries like "home" or "building" — always include the service type

Return ONLY JSON:
{{
  "meta_title": "under 60 chars, keyword + city + brand",
  "meta_description": "under 160 chars, compelling + keyword",
  "hero": {{
    "headline": "powerful H1 with keyword + city",
    "subheadline": "value proposition in 1-2 sentences",
    "cta": "action button text",
    "image_query": "specific query showing the service result looking amazing at night"
  }},
  "sections": [
    {{
      "type": "problem",
      "title": "section heading",
      "content": "2-3 paragraphs about the problem",
      "image_query": "specific photo query showing the problem (e.g. dark unlit house)"
    }},
    {{
      "type": "solution",
      "title": "section heading",
      "content": "2-3 paragraphs about how you solve it",
      "image_query": "specific photo showing the solution (e.g. beautiful outdoor lighting installation)"
    }},
    {{
      "type": "before_after",
      "before_query": "specific query for the BEFORE state",
      "after_query": "specific query for the AFTER state with this service installed"
    }},
    {{
      "type": "benefits",
      "title": "Why Choose {business_name}",
      "items": [
        {{"name": "benefit", "description": "1-2 sentences"}},
        {{"name": "benefit", "description": "1-2 sentences"}},
        {{"name": "benefit", "description": "1-2 sentences"}},
        {{"name": "benefit", "description": "1-2 sentences"}},
        {{"name": "benefit", "description": "1-2 sentences"}},
        {{"name": "benefit", "description": "1-2 sentences"}}
      ]
    }},
    {{
      "type": "services",
      "title": "Our {service} Services in {city}",
      "items": [
        {{"name": "service name", "description": "what it includes"}}
      ]
    }},
    {{
      "type": "proof",
      "content": "paragraph about experience, reviews, trust"
    }},
    {{
      "type": "process",
      "title": "How It Works",
      "items": [
        {{"name": "step name", "description": "what happens"}},
        {{"name": "step name", "description": "what happens"}},
        {{"name": "step name", "description": "what happens"}},
        {{"name": "step name", "description": "what happens"}}
      ]
    }},
    {{
      "type": "content",
      "title": "{service} in {city} - What You Need to Know",
      "content": "300+ words of local content with neighborhoods and details",
      "image_query": "specific photo of this service in a residential setting"
    }},
    {{
      "type": "faq",
      "items": [
        {{"question": "keyword-rich question", "answer": "detailed answer"}},
        {{"question": "", "answer": ""}},
        {{"question": "", "answer": ""}},
        {{"question": "", "answer": ""}},
        {{"question": "", "answer": ""}}
      ]
    }},
    {{
      "type": "cta",
      "title": "Ready for {service} in {city}?",
      "cta": "Get Your Free Quote Today"
    }}
  ],
  "schema": {{
    "@context": "https://schema.org",
    "@type": "LocalBusiness",
    "name": "{business_name}",
    "address": {{"@type": "PostalAddress", "addressLocality": "{city}"}},
    "aggregateRating": {{"@type": "AggregateRating", "ratingValue": "{rating}", "reviewCount": "{reviews}"}}
  }}
}}

CRITICAL:
- Hero headline MUST include keyword + city
- Every image_query must be SPECIFIC to the actual service (permanent lights, landscape lighting, roof line lights, etc.)
- FAQ questions must be real searches people make
- Content section 300+ words with local neighborhoods
- All text sounds like a real local business"""


class PageGenerator:

    async def generate_page(self, keyword: str, business: dict) -> str:
        prompt = PAGE_PROMPT.format(
            keyword=keyword,
            business_name=business.get("business_name", ""),
            service=business.get("primary_service", keyword),
            city=business.get("primary_city", ""),
            service_areas=", ".join(business.get("service_areas", [])),
            reviews=business.get("reviews_count", 0),
            rating=business.get("rating", 0),
            years=business.get("years_active", 0),
        )

        try:
            raw = call_claude(
                prompt,
                system="You are a web page architect. Return ONLY valid JSON. No markdown, no explanation.",
                max_tokens=4096,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            page_data = json.loads(raw)
            html = render_page(page_data, business)

            log.info("page_gen.done  keyword=%s  sections=%d", keyword, len(page_data.get("sections", [])))
            return html

        except Exception as e:
            log.error("page_gen.fail  keyword=%s  err=%s", keyword, e)
            return f"<html><body><h1>Error: {e}</h1></body></html>"

    async def generate_and_save(self, keyword: str, business: dict, output_dir: str = "generated_pages") -> str:
        import os
        os.makedirs(output_dir, exist_ok=True)

        html = await self.generate_page(keyword, business)

        slug = keyword.lower().replace(" ", "-").replace("'", "")
        filepath = os.path.join(output_dir, f"{slug}.html")

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html)

        log.info("page_gen.saved  file=%s", filepath)
        return filepath
