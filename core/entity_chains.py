import json
import logging
import re
from typing import Dict, List, Optional

log = logging.getLogger(__name__)

def extract_entities(text: str) -> List[Dict]:
    """Extract EAV triples via broad regex patterns + LLM fallback."""
    STOP = {"the", "a", "an", "this", "that", "it", "he", "she", "they", "we", "you", "i", "its"}
    patterns = [
        (re.compile(r"\b([A-Za-z][\w\s]{2,30}?)\s+(?:is|are)\s+([^.!?\n]{10,120})", re.I), "description"),
        (re.compile(r"\b([A-Za-z][\w\s]{2,30}?)\s+use[sd]?\s+([^.!?\n]{10,100})", re.I), "uses"),
        (re.compile(r"\b([\w\s]{3,30}?)\s+(?:costs?|prices?|runs?)\s+([\$\d][\d,\.%\s\-]+(?:per|each|for)?[\w\s]{0,20})", re.I), "cost"),
        (re.compile(r"\b([\w\s]{3,30}?)\s+takes?\s+(\d[\d\s\-]*(?:hours?|days?|weeks?|minutes?))", re.I), "duration"),
        (re.compile(r"\b([\w\s]{3,30}?)\s+lasts?\s+(\d[\d\s\-]*(?:years?|months?|hours?))", re.I), "lifespan"),
        (re.compile(r"\b([A-Za-z][\w\s]{1,25}?):\s*([\d][^.\n]{3,60})", re.I), "specification"),
        (re.compile(r"\b([\w\s]{3,30}?)\s+(?:saves?|reduces?|cuts?)\s+([\d]+[\d\s\-\.%]+(?:more|less|energy|cost)?)", re.I), "saving"),
    ]
    triples = []
    seen = set()
    for pattern, attr in patterns:
        for match in pattern.finditer(text):
            entity = match.group(1).strip().rstrip(".,;:")
            value = match.group(2).strip()
            if len(entity) < 3 or entity.lower() in STOP:
                continue
            if len(value) < 3:
                continue
            key = (entity.lower()[:40], attr)
            if key not in seen:
                seen.add(key)
                triples.append({"entity": entity, "attribute": attr, "value": value})
    # LLM fallback when regex finds nothing
    if not triples:
        try:
            from core.claude import call_claude
            llm_prompt = (
                "Extract entity-attribute-value facts from this text as a JSON array. "
                "Each item: {entity, attribute, value}. Max 8 items. "
                "Only include specific factual claims with numbers or clear definitions.\n\n"
                f"TEXT:\n{text[:800]}\n\nJSON array only, no explanation:"
            )
            raw = call_claude(llm_prompt, max_tokens=400)
            m = re.search(r"\[[\s\S]*\]", raw)
            if m:
                import json as _j
                items = _j.loads(m.group())
                triples = [i for i in items if isinstance(i, dict) and i.get("entity") and i.get("value")][:10]
        except Exception:
            pass
    return triples[:20]


def inject_entity_section(html: str, entity_chains: List[Dict], entity_name: str) -> str:
    if not entity_chains or 'class="entity-facts"' in html:
        return html

    dl_items = []
    for chain in entity_chains[:10]:
        attr = chain.get('attribute', 'fact').replace('_', ' ').title()
        val = chain.get('value', '')
        if val:
            dl_items.append(f'  <dt>{attr}</dt><dd>{val}</dd>')

    if not dl_items:
        return html

    section = f"""<div class="entity-facts" aria-label="Key facts about {entity_name}" itemscope>
<h3>About {entity_name}</h3>
<dl>
{chr(10).join(dl_items)}
</dl>
</div>"""

    # Insert before the last </article> or </main> or at end of body
    for closing in ['</article>', '</main>', '</div>', '</body>']:
        if closing in html:
            idx = html.rindex(closing)
            return html[:idx] + '\n' + section + '\n' + html[idx:]
    return html + '\n' + section

def build_entity_schema(entity_name: str, entity_type: str, chains: List[Dict], additional_props: Dict = None) -> Dict:
    schema = {
        "@context": "https://schema.org",
        "@type": entity_type,
        "name": entity_name,
    }
    prop_map = {
        "price": "offers",
        "cost": "offers",
        "duration": "duration",
        "description": "description",
        "location": "areaServed",
        "phone": "telephone",
        "address": "address",
    }
    for chain in chains:
        attr = chain.get("attribute", "").lower()
        val = chain.get("value", "")
        schema_prop = prop_map.get(attr)
        if schema_prop and val and schema_prop not in schema:
            schema[schema_prop] = val
    if additional_props:
        schema.update(additional_props)
    return schema

def auto_enrich_content(html: str, keyword: str, business_context: dict = None, business_id: str = "") -> Dict:
    ctx = business_context or {}
    entity_name = keyword.split(" in ")[0].strip() if " in " in keyword else keyword
    entity_type = ctx.get("schema_type", "Service")

    text = re.sub(r'<[^>]+>', ' ', html)
    extracted = extract_entities(text)

    if len(extracted) < 3:
        try:
            prompt = f"""Extract 5 entity-attribute-value facts from this content about "{keyword}".
Return JSON array only:
[{{"entity": "...", "attribute": "cost|duration|description|benefit|process", "value": "specific value"}}]
Content: {text[:1500]}"""
            from core.llm_gateway import LLMGateway
            gw = LLMGateway(business_id=business_id)
            raw = gw.generate(prompt, complexity="fast")
            clean = raw.strip().lstrip('```json').lstrip('```').rstrip('```').strip()
            llm_chains = json.loads(clean)
            extracted = (extracted + llm_chains)[:15]
        except Exception:
            pass

    enriched_html = inject_entity_section(html, extracted, entity_name)
    entity_schema = build_entity_schema(entity_name, entity_type, extracted,
                                         additional_props={"areaServed": ctx.get("city", ""), "provider": {"@type": "LocalBusiness", "name": ctx.get("name", "")}})

    # Inject entity schema as JSON-LD
    script_tag = f'\n<script type="application/ld+json">\n{json.dumps(entity_schema, indent=2)}\n</script>\n'
    if '</head>' in enriched_html:
        enriched_html = enriched_html.replace('</head>', script_tag + '</head>', 1)
    else:
        enriched_html = script_tag + enriched_html

    log.info("entity_chains.enriched  keyword=%s  chains=%d", keyword, len(extracted))
    return {"html": enriched_html, "entity_chains": extracted, "entity_schema": entity_schema, "entity_name": entity_name}
