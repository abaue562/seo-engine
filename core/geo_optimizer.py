import logging
import re
from datetime import date
from typing import Dict, List, Optional

log = logging.getLogger(__name__)

DIRECT_ANSWER_TEMPLATE = """<div class="seo-direct-answer" itemscope itemtype="https://schema.org/Answer">
<p><strong>{question}</strong></p>
<p itemprop="text">{answer}</p>
</div>"""

KEY_TAKEAWAYS_TEMPLATE = """<div class="key-takeaways">
<h3>Key Takeaways</h3>
<ul>
{items}
</ul>
</div>"""

def inject_direct_answer(html: str, question: str, answer: str) -> str:
    block = DIRECT_ANSWER_TEMPLATE.format(question=question, answer=answer)
    # Insert after opening <article> or <main> or before first <p>
    for tag in ['<article', '<main', '<div class="entry-content"', '<div class="post-content"']:
        if tag in html:
            idx = html.index(tag)
            end = html.index('>', idx) + 1
            return html[:end] + '\n' + block + '\n' + html[end:]
    # Fallback: before first <p>
    if '<p' in html:
        idx = html.index('<p')
        return html[:idx] + block + '\n' + html[idx:]
    return block + '\n' + html

def inject_key_takeaways(html: str, takeaways: List[str]) -> str:
    if not takeaways:
        return html
    items = '\n'.join(f'<li>{t}</li>' for t in takeaways[:5])
    block = KEY_TAKEAWAYS_TEMPLATE.format(items=items)
    # Insert after first H2
    match = re.search(r'</h2>', html, re.I)
    if match:
        pos = match.end()
        return html[:pos] + '\n' + block + '\n' + html[pos:]
    # Fallback: after first paragraph
    match = re.search(r'</p>', html, re.I)
    if match:
        pos = match.end()
        return html[:pos] + '\n' + block + '\n' + html[pos:]
    return html

def inject_last_updated(html: str) -> str:
    today = date.today().strftime('%B %d, %Y')
    tag = f'<p class="last-updated"><em>Last updated: {today}</em></p>'
    if 'last-updated' in html:
        return html
    match = re.search(r'</h1>', html, re.I)
    if match:
        return html[:match.end()] + '\n' + tag + '\n' + html[match.end():]
    return tag + '\n' + html

def inject_definition(html: str, term: str, definition: str) -> str:
    if not term or not definition:
        return html
    if 'class="definition"' in html:
        return html
    escaped = re.escape(term)
    pattern = re.compile(rf'\b({escaped})\b', re.I)
    replacement = f'<span class="definition" title="{definition}"><strong>\\1</strong></span>'
    # Only replace first occurrence
    return pattern.sub(replacement, html, count=1)

def score_geo_readiness(html: str) -> Dict:
    text = re.sub(r'<[^>]+>', ' ', html)
    first_150 = ' '.join(text.split()[:150])
    sentences = [s.strip() for s in re.split(r'[.!?]', text) if len(s.strip()) > 20]
    factual = sum(1 for s in sentences if re.search(r'\d+[%$]?|\bpercent\b|\bstatistic\b|\bstudy\b|\bdata\b|\bsurvey\b', s, re.I))
    factual_density = factual / max(len(sentences), 1)

    breakdown = {
        'has_direct_answer_block': 20 if 'seo-direct-answer' in html else 0,
        'has_key_takeaways': 15 if 'key-takeaways' in html else 0,
        'has_definition': 10 if 'class="definition"' in html else 0,
        'answer_in_first_150_words': 20 if len(first_150.split()) > 80 and re.search(r'\b(is|are|means|refers to|defined as)\b', first_150, re.I) else 0,
        'has_numbered_lists': 10 if '<ol' in html else 0,
        'factual_density': round(factual_density * 15),
        'has_last_updated': 10 if 'last-updated' in html else 0,
    }
    score = sum(breakdown.values())
    return {'score': min(score, 100), 'breakdown': breakdown, 'passing': score >= 60}

def optimize_for_geo(html: str, keyword: str, intent: str, business_context: dict = None, business_id: str = "") -> Dict:
    ctx = business_context or {}
    business_name = ctx.get('name', 'this business')
    location = ctx.get('city', '')

    prompt = f"""You are an AI search optimization expert. Generate GEO elements for this content.

KEYWORD: {keyword}
INTENT: {intent}
BUSINESS: {business_name}{f' in {location}' if location else ''}

Generate JSON only:
{{
  "direct_question": "The exact question form of the keyword (e.g. 'What is...' or 'How much does...')",
  "direct_answer": "1-2 sentences. Start with the answer immediately. Include specific facts/numbers if possible. This will appear as the first thing on the page.",
  "key_takeaways": ["takeaway 1", "takeaway 2", "takeaway 3", "takeaway 4"],
  "primary_term": "The main term to define (only for informational intent, else null)",
  "primary_definition": "One sentence definition of the primary term (else null)"
}}"""

    geo_elements = {}
    try:
        from core.llm_gateway import LLMGateway
        gw = LLMGateway(business_id=business_id)
        raw = gw.generate(prompt, complexity="fast")
        import json
        clean = raw.strip().lstrip('```json').lstrip('```').rstrip('```').strip()
        geo_elements = json.loads(clean)
    except Exception as exc:
        log.exception("geo_optimizer.llm_error  keyword=%s", keyword)
        geo_elements = {
            'direct_question': f"What is {keyword}?",
            'direct_answer': f"{keyword.capitalize()} is a service offered by {business_name}.",
            'key_takeaways': [f"Learn about {keyword}", "Get a free quote today"],
            'primary_term': None,
            'primary_definition': None,
        }

    original_len = len(html.split())
    result_html = inject_last_updated(html)

    q = geo_elements.get('direct_question', f"What is {keyword}?")
    a = geo_elements.get('direct_answer', '')
    if a:
        result_html = inject_direct_answer(result_html, q, a)

    takeaways = geo_elements.get('key_takeaways', [])
    if takeaways:
        result_html = inject_key_takeaways(result_html, takeaways)

    term = geo_elements.get('primary_term')
    defn = geo_elements.get('primary_definition')
    if term and defn and intent == 'informational':
        result_html = inject_definition(result_html, term, defn)

    new_len = len(result_html.split())
    geo_score = score_geo_readiness(result_html)

    log.info("geo_optimizer.done  keyword=%s  geo_score=%d  words_added=%d", keyword, geo_score['score'], new_len - original_len)
    return {'html': result_html, 'geo_elements': geo_elements, 'geo_score': geo_score, 'words_added': new_len - original_len}
