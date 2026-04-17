import logging
from typing import Dict

log = logging.getLogger(__name__)

GEO_BRIEF_TEMPLATE = """You are building a content brief optimized for AI search engines (ChatGPT, Perplexity, Google SGE) AND traditional Google SEO.

KEYWORD: {keyword}
INTENT: {intent}
BUSINESS: {business_name}
LOCATION: {location}
VERTICAL: {vertical}

GEO REQUIREMENTS (non-negotiable):
1. The page MUST open with a direct, factual answer to the implied question in the first 40 words
2. Every factual claim must be specific (numbers, percentages, timeframes) — AI engines cite specificity
3. Include a "Key Takeaways" section with 4-5 bullets after the intro
4. For informational intent: define the primary term in the first paragraph
5. For commercial intent: include specific pricing ranges or cost factors
6. For local intent: include city-specific data points (climate, local regulations, typical conditions)
7. Structure must use numbered lists for any process/how-to content
8. Include at least 3 entity-attribute-value facts (e.g., "Gutter cleaning costs $150-$300 for a standard home")

CONTENT BRIEF:
- Target word count: {word_count}
- Primary keyword: {keyword}
- Secondary keywords: {secondary_keywords}
- H1: [write a question-form H1 that matches search intent]
- Opening answer block: [1-2 sentences that directly answer the H1 question]
- H2 structure: [list 4-6 H2s]
- Schema types: {schema_types}
- FAQ questions: [5 specific questions with answer sketches]
- Local data to include: [specific facts about {location}]
- Competitor differentiation: [what makes this page better than generic answers]

Output the brief in structured format."""

GEO_PASS1_TEMPLATE = """You are writing SEO content optimized for citation by AI search engines (ChatGPT, Perplexity, Gemini) and ranking in Google.

KEYWORD: {keyword}
INTENT: {intent}
BRIEF: {brief}

CRITICAL STRUCTURE RULES:
1. FIRST PARAGRAPH: Answer the question directly. Start with the answer, not context. Example: "Gutter cleaning in Kelowna costs between $150-$300 for a standard home, depending on linear footage and debris level." NOT "If you're wondering about gutter cleaning costs..."
2. Use numbered lists (not bullet points) for any process with more than 2 steps
3. Every section must have at least one specific fact with a number
4. Include a definition of the primary topic in the first 100 words
5. H2s should be phrased as questions when possible (AI engines extract Q&A pairs)
6. End each major section with a concrete takeaway sentence

Write the complete HTML content following these rules exactly. Include proper H1, H2s, paragraphs, and lists."""

GEO_PASS4_TEMPLATE = """You are humanizing SEO content while preserving its AI-citation-optimized structure.

RULES (do not violate):
1. Keep the direct answer block FIRST — do not move it or bury it in new paragraphs
2. Keep all numbered lists as numbered lists — do not convert to prose
3. Keep all specific numbers and statistics exactly as written
4. Keep the Key Takeaways section intact
5. Keep all definition sentences

WHAT TO IMPROVE:
- Make the prose sound natural and helpful, not robotic
- Add personality appropriate to the business voice
- Vary sentence length and structure
- Remove any AI-tell phrases ("It's worth noting", "It's important to", "In conclusion")
- Ensure local relevance feels genuine, not inserted

CONTENT TO HUMANIZE:
{content}

Return the complete humanized HTML, preserving all GEO structural elements."""

def register_geo_prompts():
    try:
        from core.prompt_library import register_prompt, list_prompts
        existing = {p["name"] for p in list_prompts()}
        registered = []

        prompts_to_register = [
            ("geo_brief", GEO_BRIEF_TEMPLATE, ["keyword","intent","business_name","location","vertical","word_count","secondary_keywords","schema_types"], "fast"),
            ("geo_pass1", GEO_PASS1_TEMPLATE, ["keyword","intent","brief"], "smart"),
            ("geo_pass4", GEO_PASS4_TEMPLATE, ["content"], "smart"),
        ]
        for name, template, variables, complexity in prompts_to_register:
            if name not in existing:
                register_prompt(name, template, variables, complexity)
                registered.append(name)
                log.info("geo_prompts.registered  name=%s", name)
        return registered
    except Exception as exc:
        log.exception("geo_prompts.register_error")
        return []

def get_geo_brief_prompt(keyword: str, intent: str, business_context: Dict) -> str:
    return GEO_BRIEF_TEMPLATE.format(
        keyword=keyword,
        intent=intent,
        business_name=business_context.get("name", "this business"),
        location=business_context.get("city", ""),
        vertical=business_context.get("vertical", "local services"),
        word_count=business_context.get("target_word_count", 1000),
        secondary_keywords=", ".join(business_context.get("secondary_keywords", [])),
        schema_types=", ".join(business_context.get("schema_types", ["LocalBusiness", "FAQPage"])),
    )

def get_geo_pass1_prompt() -> str:
    return GEO_PASS1_TEMPLATE

def get_geo_pass4_prompt() -> str:
    return GEO_PASS4_TEMPLATE
