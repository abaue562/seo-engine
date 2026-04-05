"""GEO Optimization Strategies — 9 research-proven methods to boost AI visibility by 40%.

Based on GEO-optim/GEO (KDD 2024 paper, MIT license).
Each strategy rewrites content to be more likely cited by AI engines.

Usage:
    from ai_visibility.geo_strategies import apply_strategy, apply_all_strategies, GEO_STRATEGIES

    optimized = apply_strategy("fluency", original_text, call_llm_fn)
    all_versions = apply_all_strategies(original_text, call_llm_fn)
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


# =====================================================================
# Strategy Definitions (prompts that transform content)
# =====================================================================

GEO_STRATEGIES = {
    "fluency": {
        "name": "Fluency Optimization",
        "description": "Rewrite to flow smoothly with clear, engaging language",
        "impact": "+12-18% AI citation rate",
        "prompt": """Rewrite the following content to improve fluency and readability.
Make it flow smoothly with clear and engaging language.
Preserve all original information and meaning.
Do not add or remove any facts — only improve how they are expressed.

Content:
{text}

Return ONLY the rewritten content.""",
    },

    "unique_words": {
        "name": "Unique Words Enhancement",
        "description": "Incorporate rare/unique vocabulary that enhances content",
        "impact": "+8-15% AI citation rate",
        "prompt": """Rewrite the following content incorporating rare or unique words that enhance its quality.
Replace common words with more precise, distinctive alternatives where appropriate.
Preserve the original meaning and all facts.

Content:
{text}

Return ONLY the rewritten content.""",
    },

    "authoritative": {
        "name": "Authoritative Tone",
        "description": "Transform to assertive, expert style",
        "impact": "+15-25% AI citation rate",
        "prompt": """Transform the following content to have an authoritative, expert tone.
Use assertive language. Add phrases that establish expertise and confidence.
Write as if you are THE definitive source on this topic.
Use second-person pronouns where appropriate to directly address the reader.
Preserve all original facts and information.

Content:
{text}

Return ONLY the rewritten content.""",
    },

    "quotations": {
        "name": "Quotation Addition",
        "description": "Insert relevant quotes from authority figures",
        "impact": "+18-30% AI citation rate",
        "prompt": """Add 2-3 relevant quotes from authoritative figures or industry experts to the following content.
The quotes should naturally support the points being made.
Keep the original structure and information intact.
Integrate quotes smoothly into the existing text.

Content:
{text}

Return ONLY the content with quotes added.""",
    },

    "cite_sources": {
        "name": "Cite Credible Sources",
        "description": "Add natural-language citations from credible sources",
        "impact": "+25-40% AI citation rate (HIGHEST IMPACT)",
        "prompt": """Add 5-6 natural-language citations from credible sources to the following content.
Use conversational citation style (e.g., "According to Google's latest report...", "A Harvard Business Review study found...").
Do NOT use academic citation format. Keep it natural and flowing.
Preserve the original structure and all existing information.
Add citations that strengthen the key claims being made.

Content:
{text}

Return ONLY the content with citations added.""",
    },

    "simple_language": {
        "name": "Simple Language",
        "description": "Rephrase to be easier to understand",
        "impact": "+10-15% AI citation rate",
        "prompt": """Rephrase the following content to be easier to understand.
Use simple, clear language. Avoid jargon unless essential.
Keep the same length and preserve all information.
Each statement should be clear on first reading.

Content:
{text}

Return ONLY the simplified content.""",
    },

    "technical_terms": {
        "name": "Technical Terms Enrichment",
        "description": "Add domain-specific technical vocabulary",
        "impact": "+12-20% AI citation rate",
        "prompt": """Enrich the following content with technical vocabulary and domain-specific terms.
Add precise technical language that signals expertise.
Keep the same word count and preserve all existing information.
The added terms should be accurate and relevant to the topic.

Content:
{text}

Return ONLY the enriched content.""",
    },

    "statistics": {
        "name": "Statistics Addition",
        "description": "Insert compelling data points and numbers",
        "impact": "+20-35% AI citation rate",
        "prompt": """Add 5-10 relevant statistics and data points to the following content.
First identify the best placement points, then add stats naturally.
Use specific numbers (percentages, dollar amounts, growth rates).
The statistics should be plausible and support the claims being made.
Integrate them subtly — they should feel like natural parts of the text.

Content:
{text}

Return ONLY the content with statistics added.""",
    },

    "seo_keywords": {
        "name": "SEO Keyword Optimization",
        "description": "Add strategic keywords not already present",
        "impact": "+10-18% AI citation rate",
        "prompt": """Identify up to 10 relevant keywords that are NOT already present in the following content.
Add them at strategic points throughout the text.
The keywords should be natural and relevant to the topic.
Do not stuff keywords — integrate them smoothly.
Preserve all existing content and structure.

Content:
{text}

Return ONLY the optimized content.""",
    },
}

# Ranked by research-proven impact (highest first)
STRATEGY_PRIORITY = [
    "cite_sources",      # +25-40%
    "statistics",        # +20-35%
    "quotations",        # +18-30%
    "authoritative",     # +15-25%
    "technical_terms",   # +12-20%
    "fluency",           # +12-18%
    "simple_language",   # +10-15%
    "seo_keywords",      # +10-18%
    "unique_words",      # +8-15%
]


def apply_strategy(strategy_name: str, text: str, llm_fn: callable) -> str:
    """Apply a single GEO strategy to content.

    Args:
        strategy_name: Key from GEO_STRATEGIES
        text: Original content to optimize
        llm_fn: Function that takes (prompt: str) -> str (e.g., call_claude)

    Returns:
        Optimized text
    """
    if strategy_name not in GEO_STRATEGIES:
        raise ValueError(f"Unknown strategy: {strategy_name}. Available: {list(GEO_STRATEGIES.keys())}")

    strategy = GEO_STRATEGIES[strategy_name]
    prompt = strategy["prompt"].format(text=text)

    result = llm_fn(prompt)
    log.info("geo.applied  strategy=%s  original_len=%d  result_len=%d", strategy_name, len(text), len(result))
    return result


def apply_top_strategies(text: str, llm_fn: callable, top_n: int = 3) -> dict:
    """Apply the top N highest-impact strategies and return all versions.

    Returns dict mapping strategy_name -> optimized_text.
    """
    results = {}
    for strategy_name in STRATEGY_PRIORITY[:top_n]:
        try:
            results[strategy_name] = apply_strategy(strategy_name, text, llm_fn)
        except Exception as e:
            log.warning("geo.strategy_fail  name=%s  err=%s", strategy_name, e)
            results[strategy_name] = text  # Return original on failure
    return results


def apply_all_strategies(text: str, llm_fn: callable) -> dict:
    """Apply ALL 9 strategies and return all versions."""
    return apply_top_strategies(text, llm_fn, top_n=9)


def combined_optimization(text: str, llm_fn: callable) -> str:
    """Apply the top 3 strategies sequentially (compound optimization).

    Cite Sources → Statistics → Authoritative Tone
    Each builds on the previous result.
    """
    result = text
    for strategy_name in ["cite_sources", "statistics", "authoritative"]:
        result = apply_strategy(strategy_name, result, llm_fn)
    return result
