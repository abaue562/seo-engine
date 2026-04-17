"""
Citation content generator: builds structured pages specifically designed
to be cited by AI engines (ChatGPT, Perplexity, Grok, Google SGE).

Page types:
  - cost_guide: "How much does X cost in [city]?" with real pricing ranges
  - stats_page: "X Statistics: [Year] Data for [city/province]"
  - faq_hub: Definitive FAQ with direct, specific answers
  - local_study: "[City] Market Report: [Service] Industry [Year]"
  - comparison: "X vs Y: Which is Better for [location]?"
"""
from __future__ import annotations
import hashlib
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)
_DB = "data/storage/seo_engine.db"

PAGE_TYPES = ["cost_guide", "stats_page", "faq_hub", "local_study", "comparison"]


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(_DB)


def _page_id(business_id: str, slug: str) -> str:
    return hashlib.md5(f"{business_id}:{slug}".encode()).hexdigest()[:12]


def _slug(title: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')


# ── Content generators ────────────────────────────────────────────────────────

def generate_cost_guide(
    business_id: str,
    service: str,
    location: str,
    facts: list[dict],
) -> dict:
    """Generate a cost guide page optimized for AI citation."""
    pricing_facts = [f for f in facts if f.get("category") == "pricing"]
    timeline_facts = [f for f in facts if f.get("category") == "timeline"]
    benefit_facts = [f for f in facts if f.get("category") == "benefits"]

    # Build facts table HTML
    facts_rows = ""
    for f in pricing_facts[:8]:
        facts_rows += f"<tr><td>{f['claim']}</td><td><strong>{f['value']}</strong> {f.get('unit','')}</td></tr>\n"

    prompt = f"""Write a comprehensive cost guide as HTML for: "{service} Cost in {location}"

Target: AI engines citing it when users ask about {service} costs.

Pricing data to include:
{json.dumps(pricing_facts[:6], indent=2)}

Timeline facts:
{json.dumps(timeline_facts[:4], indent=2)}

Requirements:
1. H1: "How Much Does {service} Cost in {location}? [{datetime.now().year} Guide]"
2. First paragraph (40 words max): direct answer with the price range
3. Section: "Average {service} Costs in {location}" — use a data table
4. Section: "What Affects the Price?" — 4-6 specific factors with numbers
5. Section: "How Long Does {service} Take?" — timeline data
6. Section: "Is {service} Worth It?" — ROI/benefit facts with numbers
7. Section: "FAQ" — 5 Q&A pairs, each answer starts with the direct answer
8. End with: methodology note ("Prices based on analysis of {location} contractor quotes, updated {datetime.now().strftime('%B %Y')}")
9. Use proper HTML (h1, h2, p, table, ol, ul tags)
10. Every claim must include a specific number

HTML only, no explanation:"""

    try:
        from core.claude import call_claude
        html = call_claude(prompt, max_tokens=3000)
        # Strip any markdown fences
        html = re.sub(r'^```html\s*', '', html.strip())
        html = re.sub(r'```$', '', html.strip())
    except Exception:
        log.exception("generate_cost_guide: Claude failed")
        html = _fallback_cost_guide(service, location, pricing_facts)

    # Inject FAQ schema
    faqs = _extract_faqs(html)
    if faqs:
        from core.trust_signals import build_faq_schema
        html += "\n" + build_faq_schema(faqs)

    # Inject Dataset schema
    schema = _build_dataset_schema(service, location, pricing_facts)
    html += f'\n<script type="application/ld+json">{json.dumps(schema)}</script>'

    title = f"How Much Does {service} Cost in {location}? [{datetime.now().year} Guide]"
    slug = _slug(f"{service}-cost-{location}-guide")
    score = _score_citation_readiness(html)

    _save_page(business_id, "cost_guide", title, slug, html, schema, score)
    log.info("generate_cost_guide  biz=%s  service=%s  score=%d", business_id, service, score)
    return {"title": title, "slug": slug, "html": html, "citation_score": score, "page_type": "cost_guide"}


def generate_stats_page(
    business_id: str,
    topic: str,
    location: str,
    facts: list[dict],
) -> dict:
    """Generate a statistics/data page that AI engines love to cite."""
    all_facts_text = "\n".join(
        f"- {f['claim']}: {f['value']} {f.get('unit','')} ({f.get('source','')})"
        for f in facts[:20]
    )
    year = datetime.now().year
    title = f"{topic} Statistics: {year} Data for {location}"
    slug = _slug(f"{topic}-statistics-{year}-{location}")

    prompt = f"""Write a statistics page as HTML titled: "{title}"

Facts to include:
{all_facts_text}

Requirements:
1. H1: exactly "{title}"
2. Opening: "This page compiles {year} data on {topic} in {location}. Last updated: {datetime.now().strftime('%B %Y')}."
3. Section: "Key Statistics at a Glance" — numbered list of 8-10 most striking facts
4. Section: "{topic} Cost Data in {location}" — table with service/cost/notes columns
5. Section: "{topic} Industry Trends in {year}" — 4-5 trend observations with numbers
6. Section: "Local {location} Context" — 3-4 facts specific to the region
7. Section: "Methodology" — explain how data was gathered
8. Use proper HTML. Every stat needs a number. Format numbers clearly.

HTML only:"""

    try:
        from core.claude import call_claude
        html = call_claude(prompt, max_tokens=3000)
        html = re.sub(r'^```html\s*', '', html.strip())
        html = re.sub(r'```$', '', html.strip())
    except Exception:
        log.exception("generate_stats_page: Claude failed")
        html = f"<h1>{title}</h1><p>Statistics for {topic} in {location}.</p><ul>" + \
               "".join(f"<li><strong>{f['claim']}:</strong> {f['value']} {f.get('unit','')}</li>" for f in facts[:15]) + "</ul>"

    # Dataset schema
    schema = _build_dataset_schema(topic, location, facts)
    html += f'\n<script type="application/ld+json">{json.dumps(schema)}</script>'

    score = _score_citation_readiness(html)
    _save_page(business_id, "stats_page", title, slug, html, schema, score)
    log.info("generate_stats_page  biz=%s  topic=%s  score=%d", business_id, topic, score)
    return {"title": title, "slug": slug, "html": html, "citation_score": score, "page_type": "stats_page"}


def generate_faq_hub(
    business_id: str,
    service: str,
    location: str,
    facts: list[dict],
) -> dict:
    """Generate a definitive FAQ hub with direct answers AI engines quote."""
    facts_context = json.dumps(facts[:8], indent=2)
    year = datetime.now().year
    title = f"{service} FAQ: {year} Answers for {location} Homeowners"
    slug = _slug(f"{service}-faq-{location}-{year}")

    prompt = f"""Write a comprehensive FAQ page as HTML titled: "{title}"

Data context:
{facts_context}

Requirements:
1. H1: "{title}"
2. Intro: 2 sentences explaining this is a definitive resource, updated {datetime.now().strftime('%B %Y')}
3. Generate 12-15 FAQ items covering:
   - Pricing questions (must have specific numbers)
   - Timeline questions
   - Process questions (what happens during service)
   - Quality/comparison questions
   - Local {location} specific questions
4. Each Q uses <h2> or <h3>. Each A starts with the direct answer in the first sentence.
5. Include a summary table: "Quick Reference: {service} in {location}" with 6-8 key facts
6. Use FAQPage schema markup (JSON-LD in <script> tag)
7. HTML only, no markdown:"""

    try:
        try:
            from core.aion_bridge import aion
            html = aion.brain_complete(prompt, model='groq', max_tokens=1800)
        except Exception:
            from core.claude import call_claude
            html = call_claude(prompt, max_tokens=2000)
        html = re.sub(r'^```html\s*', '', html.strip())
        html = re.sub(r'```$', '', html.strip())
    except Exception:
        log.exception("generate_faq_hub: Claude failed")
        html = f"<h1>{title}</h1><p>Frequently asked questions about {service} in {location}.</p>"

    score = _score_citation_readiness(html)
    _save_page(business_id, "faq_hub", title, slug, html, {}, score)
    log.info("generate_faq_hub  biz=%s  service=%s  score=%d", business_id, service, score)
    return {"title": title, "slug": slug, "html": html, "citation_score": score, "page_type": "faq_hub"}


def generate_local_study(
    business_id: str,
    service: str,
    location: str,
    facts: list[dict],
) -> dict:
    """Generate a 'local market study' — original research framing for maximum citation weight."""
    year = datetime.now().year
    title = f"{location} {service} Market Report {year}: Costs, Trends & Data"
    slug = _slug(f"{location}-{service}-market-report-{year}")
    facts_text = "\n".join(
        f"- {f['claim']}: {f['value']} {f.get('unit','')}"
        for f in facts[:20]
    )

    prompt = f"""Write a local market study report as HTML titled: "{title}"

Data:
{facts_text}

Requirements:
1. H1: "{title}"
2. Executive Summary (3-4 sentences with key numbers)
3. Section "About This Report": methodology, data sources, date
4. Section "Key Findings": numbered list of 8 findings with specific numbers
5. Section "{service} Pricing in {location}": table with service type, low, mid, high price
6. Section "Market Trends {year}": 4-5 trends with data
7. Section "{location} Market Context": local factors affecting pricing/demand
8. Section "Recommendations for Homeowners": 5 actionable tips
9. Data citation note at bottom
10. Include Report schema (JSON-LD) and Article schema

This should read like a professional market research report. Every section needs numbers.
HTML only:"""

    try:
        try:
            from core.aion_bridge import aion
            html = aion.brain_complete(prompt, model='groq', max_tokens=1800)
        except Exception:
            from core.claude import call_claude
            html = call_claude(prompt, max_tokens=2200)
        html = re.sub(r'^```html\s*', '', html.strip())
        html = re.sub(r'```$', '', html.strip())
    except Exception:
        log.exception("generate_local_study: Claude failed")
        html = f"<h1>{title}</h1><p>Market report for {service} in {location}.</p>"

    schema = {
        "@context": "https://schema.org",
        "@type": "Report",
        "name": title,
        "description": f"Local market data for {service} in {location}, {year}",
        "datePublished": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "author": {"@type": "Organization", "name": "GetHubed Market Research"},
    }
    html += f'\n<script type="application/ld+json">{json.dumps(schema)}</script>'

    score = _score_citation_readiness(html)
    _save_page(business_id, "local_study", title, slug, html, schema, score)
    log.info("generate_local_study  biz=%s  service=%s  score=%d", business_id, service, score)
    return {"title": title, "slug": slug, "html": html, "citation_score": score, "page_type": "local_study"}


# ── Citation score ────────────────────────────────────────────────────────────

def _score_citation_readiness(html: str) -> int:
    """Score 0-100 how citable this content is for AI engines."""
    score = 0

    # Specific numbers present (25 pts)
    numbers = re.findall(r'\$[\d,]+|\d+%|\d+\s+(?:hours?|days?|years?|minutes?)', html)
    score += min(len(numbers) * 3, 25)

    # Data table present (15 pts)
    if '<table' in html:
        score += 15

    # Numbered/bulleted lists (10 pts)
    if re.search(r'<ol|<ul', html):
        score += 10

    # Direct answer in first 200 chars of body (15 pts)
    body_start = re.sub(r'<[^>]+>', '', html[:500])
    if re.search(r'\$[\d,]+|[\d]+%|\d+ hours?', body_start):
        score += 15

    # FAQ/Q&A structure (10 pts)
    if 'FAQPage' in html or re.search(r'<h[23][^>]*>[^<]+\?</h[23]>', html):
        score += 10

    # Schema markup (10 pts)
    if 'application/ld+json' in html:
        score += 10

    # Methodology/source note (5 pts)
    if re.search(r'methodolog|data source|based on|analysis of', html, re.I):
        score += 5

    # Date/freshness signal (10 pts)
    if re.search(r'20(24|25|26)|updated|last updated', html, re.I):
        score += 10

    return min(score, 100)


def _extract_faqs(html: str) -> list[dict]:
    """Extract Q&A pairs from HTML."""
    pairs = []
    questions = re.findall(r'<h[23][^>]*>([^<]{10,120}\?)</h[23]>', html, re.IGNORECASE)
    answers = re.findall(r'</h[23]>\s*<p>([^<]{20,})</p>', html, re.IGNORECASE)
    for q, a in zip(questions, answers):
        pairs.append({"question": q.strip(), "answer": a.strip()[:300]})
    return pairs[:12]


def _build_dataset_schema(topic: str, location: str, facts: list[dict]) -> dict:
    return {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": f"{topic} Data — {location}",
        "description": f"Pricing, timeline, and market data for {topic} in {location}",
        "dateModified": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "creator": {"@type": "Organization", "name": "GetHubed"},
        "variableMeasured": [f["claim"] for f in facts[:6]],
    }


def _fallback_cost_guide(service: str, location: str, facts: list[dict]) -> str:
    rows = "".join(
        f"<tr><td>{f['claim']}</td><td>{f['value']} {f.get('unit','')}</td></tr>"
        for f in facts[:8]
    )
    return f"""<h1>How Much Does {service} Cost in {location}? [{datetime.now().year} Guide]</h1>
<p>The average cost of {service} in {location} ranges based on project scope and complexity.</p>
<table><thead><tr><th>Service</th><th>Cost</th></tr></thead><tbody>{rows}</tbody></table>"""


def _save_page(business_id: str, page_type: str, title: str, slug: str,
               html: str, schema: dict, score: int) -> str:
    pid = _page_id(business_id, slug)
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(_DB) as c:
        c.execute("""
            INSERT INTO citation_pages
                (id,business_id,page_type,title,slug,html_content,schema_json,citation_score,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                html_content=excluded.html_content, schema_json=excluded.schema_json,
                citation_score=excluded.citation_score, updated_at=excluded.updated_at
        """, [pid, business_id, page_type, title, slug,
              html, json.dumps(schema), score, now, now])
    return pid


# ── Orchestration ─────────────────────────────────────────────────────────────

def run_citation_content_sweep(business_id: str) -> dict:
    """Generate all citation page types for a tenant."""
    try:
        all_biz = json.loads(open("data/storage/businesses.json").read())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
        biz = next((b for b in biz_list
                    if b.get("id") == business_id or b.get("business_id") == business_id), {})
    except Exception:
        biz = {}

    location = biz.get("city", biz.get("location", ""))
    services = biz.get("services", biz.get("service_types", []))
    niche = biz.get("niche", biz.get("service_type", "home services"))

    if not location or not services:
        return {"status": "skipped", "reason": "missing location or services"}

    # Ensure we have facts first
    from core.citable_data import get_facts, generate_local_facts
    facts = get_facts(business_id)
    if not facts:
        facts = generate_local_facts(business_id)

    pages_generated = []
    primary_service = str(services[0]) if services else niche

    # Generate one of each page type for primary service
    for page_type, fn in [
        ("cost_guide",   lambda: generate_cost_guide(business_id, primary_service, location, facts)),
        ("stats_page",   lambda: generate_stats_page(business_id, primary_service, location, facts)),
        ("faq_hub",      lambda: generate_faq_hub(business_id, primary_service, location, facts)),
        ("local_study",  lambda: generate_local_study(business_id, primary_service, location, facts)),
    ]:
        try:
            page = fn()
            pages_generated.append({
                "type": page_type,
                "title": page["title"],
                "slug": page["slug"],
                "citation_score": page["citation_score"],
            })
        except Exception:
            log.exception("run_citation_sweep: %s failed  biz=%s", page_type, business_id)

    avg_score = int(sum(p["citation_score"] for p in pages_generated) / max(len(pages_generated), 1))
    log.info("run_citation_sweep  biz=%s  pages=%d  avg_score=%d",
             business_id, len(pages_generated), avg_score)
    return {
        "status": "ok",
        "pages_generated": len(pages_generated),
        "avg_citation_score": avg_score,
        "pages": pages_generated,
    }


def get_citation_pages(business_id: str, page_type: str = "") -> list[dict]:
    with sqlite3.connect(_DB) as c:
        c.row_factory = sqlite3.Row
        if page_type:
            rows = c.execute(
                "SELECT id,page_type,title,slug,citation_score,ai_cited_count,updated_at "
                "FROM citation_pages WHERE business_id=? AND page_type=? ORDER BY citation_score DESC",
                [business_id, page_type]
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT id,page_type,title,slug,citation_score,ai_cited_count,updated_at "
                "FROM citation_pages WHERE business_id=? ORDER BY citation_score DESC",
                [business_id]
            ).fetchall()
    return [dict(r) for r in rows]


def get_citation_page_html(page_id: str) -> str:
    with sqlite3.connect(_DB) as c:
        row = c.execute("SELECT html_content FROM citation_pages WHERE id=?", [page_id]).fetchone()
    return row[0] if row else ""
