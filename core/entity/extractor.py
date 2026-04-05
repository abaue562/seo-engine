"""Entity SEO Extractor — extract entities and find gaps vs competitors.

Extracts named entities from content, maps them to Google's entity graph,
identifies entity gaps between your content and top-ranking content.

Usage:
    from core.entity.extractor import extract_entities, find_entity_gaps

    entities = extract_entities("Blend Bright Lights provides permanent lighting in Kelowna...")
    gaps = find_entity_gaps(your_entities, competitor_entities)
"""

from __future__ import annotations

import re
import logging
from collections import Counter

log = logging.getLogger(__name__)


# Entity types relevant to SEO
ENTITY_TYPES = {
    "ORGANIZATION": 1.0,
    "PERSON": 0.8,
    "LOCATION": 0.9,
    "PRODUCT": 0.9,
    "SERVICE": 0.9,
    "EVENT": 0.5,
    "DATE": 0.3,
    "NUMBER": 0.2,
}


def extract_entities(text: str, method: str = "regex") -> list[dict]:
    """Extract named entities from text.

    Methods:
        "regex" — fast pattern matching (no ML)
        "spacy" — spaCy NER (if installed)
        "ollama" — local LLM extraction

    Returns list of {text, type, salience, count}
    """
    if method == "spacy":
        return _extract_spacy(text)
    elif method == "ollama":
        return _extract_ollama(text)
    return _extract_regex(text)


def _extract_regex(text: str) -> list[dict]:
    """Fast regex-based entity extraction. No ML needed."""
    entities = Counter()

    # Proper nouns (capitalized multi-word phrases)
    for match in re.finditer(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b', text):
        entities[("ORGANIZATION", match.group())] += 1

    # Locations (city + state/province patterns)
    for match in re.finditer(r'\b([A-Z][a-z]+(?:,\s*[A-Z]{2}))\b', text):
        entities[("LOCATION", match.group())] += 1

    # Single proper nouns
    for match in re.finditer(r'\b([A-Z][a-z]{2,})\b', text):
        word = match.group()
        # Skip common sentence starters
        if word.lower() not in {"the", "this", "that", "these", "those", "when", "where", "what", "which", "there"}:
            entities[("ENTITY", word)] += 1

    # Dollar amounts
    for match in re.finditer(r'\$[\d,]+(?:\.\d+)?(?:\s*(?:million|billion|M|B|K))?', text):
        entities[("NUMBER", match.group())] += 1

    # Percentages
    for match in re.finditer(r'\d+(?:\.\d+)?%', text):
        entities[("NUMBER", match.group())] += 1

    # URLs/domains
    for match in re.finditer(r'(?:https?://)?(?:www\.)?[\w.-]+\.\w{2,}', text):
        entities[("URL", match.group())] += 1

    # Convert to list
    total_mentions = sum(entities.values()) or 1
    results = []
    for (etype, etext), count in entities.most_common(50):
        salience = count / total_mentions * ENTITY_TYPES.get(etype, 0.5)
        results.append({
            "text": etext,
            "type": etype,
            "count": count,
            "salience": round(salience, 3),
        })

    results.sort(key=lambda x: x["salience"], reverse=True)
    return results


def _extract_spacy(text: str) -> list[dict]:
    """Extract entities using spaCy NER."""
    try:
        import spacy
        nlp = spacy.load("en_core_web_sm")
    except (ImportError, OSError):
        log.warning("entity.spacy_unavailable  falling back to regex")
        return _extract_regex(text)

    doc = nlp(text[:10000])  # Limit to 10K chars
    entities = Counter()
    for ent in doc.ents:
        entities[(ent.label_, ent.text)] += 1

    total = sum(entities.values()) or 1
    results = []
    for (etype, etext), count in entities.most_common(50):
        salience = count / total * ENTITY_TYPES.get(etype, 0.5)
        results.append({"text": etext, "type": etype, "count": count, "salience": round(salience, 3)})
    results.sort(key=lambda x: x["salience"], reverse=True)
    return results


def _extract_ollama(text: str) -> list[dict]:
    """Extract entities using local Ollama LLM."""
    try:
        from core.llm_pool import call_fast
        prompt = f"""Extract all named entities from this text. Return JSON array:
[{{"text": "entity name", "type": "ORGANIZATION|PERSON|LOCATION|PRODUCT|SERVICE"}}]

Text: {text[:3000]}

Return ONLY the JSON array."""

        result = call_fast(prompt)
        import json
        # Find JSON array in response
        start = result.find("[")
        end = result.rfind("]") + 1
        if start >= 0 and end > start:
            entities = json.loads(result[start:end])
            return [{"text": e.get("text", ""), "type": e.get("type", "ENTITY"),
                      "count": 1, "salience": 0.5} for e in entities if e.get("text")]
    except Exception as e:
        log.warning("entity.ollama_fail  err=%s", e)

    return _extract_regex(text)


def find_entity_gaps(our_entities: list[dict], competitor_entities: list[dict]) -> list[dict]:
    """Find entities competitors mention that we don't.

    Args:
        our_entities: Output from extract_entities(our_content)
        competitor_entities: Output from extract_entities(competitor_content)

    Returns:
        List of missing entities sorted by competitor salience
    """
    our_set = {e["text"].lower() for e in our_entities}

    gaps = []
    for ent in competitor_entities:
        if ent["text"].lower() not in our_set:
            gaps.append({
                "text": ent["text"],
                "type": ent["type"],
                "competitor_salience": ent["salience"],
                "recommendation": f"Add mention of '{ent['text']}' to improve entity coverage",
            })

    gaps.sort(key=lambda x: x["competitor_salience"], reverse=True)
    return gaps


def generate_entity_schema(entities: list[dict], page_url: str = "") -> str:
    """Generate JSON-LD schema with entity mentions for Google's entity graph."""
    import json

    mentions = []
    for ent in entities[:10]:
        if ent["type"] in ("ORGANIZATION", "PERSON", "LOCATION", "PRODUCT"):
            schema_type = {
                "ORGANIZATION": "Organization",
                "PERSON": "Person",
                "LOCATION": "Place",
                "PRODUCT": "Product",
            }.get(ent["type"], "Thing")

            mentions.append({
                "@type": schema_type,
                "name": ent["text"],
            })

    if not mentions:
        return ""

    schema = {
        "@context": "https://schema.org",
        "@type": "WebPage",
        "url": page_url,
        "mentions": mentions,
    }

    return f'<script type="application/ld+json">\n{json.dumps(schema, indent=2)}\n</script>'
