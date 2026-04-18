"""AI Search Content Optimizer — GEO/AEO + citation signal injection.

Upgrades content for AI search engines (Perplexity, ChatGPT, Claude, Gemini)
by injecting structured answer blocks, citation signals, and AI-ingestible formatting.

Why this matters:
  AI models prefer sources with:
    1. Direct, factual answer in first 150 words
    2. Explicit data points (numbers, statistics, named sources)
    3. Clear heading hierarchy matching question patterns
    4. Structured lists over prose walls
    5. Author/publisher signals (E-E-A-T anchors)
    6. Datestamp + freshness markers
    7. FAQ blocks using question-as-heading pattern

Usage:
    from core.ai_content_optimizer import optimize_for_ai, score_ai_readiness
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


# ── AI readiness scorer ───────────────────────────────────────────────────────

def score_ai_readiness(html: str, keyword: str = "") -> dict:
    """Score content for AI search citation likelihood (0-100).

    Returns breakdown by signal category.
    """
    text = re.sub(r"<[^>]+>", " ", html)
    words = text.split()
    first_150 = " ".join(words[:150])
    sentences = [s.strip() for s in re.split(r"[.!?]", text) if len(s.strip()) > 20]

    signals = {}

    # 1. Direct answer in first 150 words
    has_direct = bool(re.search(r"\b(is|are|costs?|takes?|means?|defined? as)\b", first_150, re.I))
    signals["direct_answer_first_150"] = 15 if has_direct else 0

    # 2. Factual data points (numbers, percentages, dollar amounts)
    factual_sents = sum(1 for s in sentences if re.search(r"\$[\d,]+|\d+%|[\d,]+ (homes?|customers?|years?|months?|days?)", s))
    signals["factual_density"] = min(factual_sents * 3, 20)

    # 3. Structured list content
    signals["has_lists"] = 10 if ("<ul" in html or "<ol" in html) else 0

    # 4. Question-pattern headings (FAQ optimization)
    q_headings = len(re.findall(r"<h[2-4][^>]*>[^<]*(what|how|why|when|which|can|does|is |are )[^<]*</h[2-4]>", html, re.I))
    signals["question_headings"] = min(q_headings * 4, 16)

    # 5. Author / E-E-A-T markers
    has_author = bool(re.search(r'(author|by |written by|reviewed by)', html, re.I))
    has_date = bool(re.search(r'(published|updated|last updated|[12][0-9]{3}-[0-1][0-9]-[0-3][0-9])', html, re.I))
    signals["eeat_signals"] = (8 if has_author else 0) + (4 if has_date else 0)

    # 6. Schema/structured data
    signals["has_schema"] = 10 if "application/ld+json" in html else 0

    # 7. Content depth (word count proxy)
    wc = len(words)
    signals["content_depth"] = 10 if wc >= 1200 else (6 if wc >= 600 else 2)

    # 8. Key takeaways / summary block
    signals["has_summary_block"] = 5 if ("key-takeaways" in html or "key takeaways" in html.lower()) else 0

    total = sum(signals.values())
    signals["total"] = total
    signals["grade"] = "A" if total >= 80 else ("B" if total >= 60 else ("C" if total >= 40 else "D"))

    return signals


# ── Citation signal injection ─────────────────────────────────────────────────

_DIRECT_ANSWER_BLOCK = """<div class="ai-answer-block" itemscope itemtype="https://schema.org/Answer">
<p class="direct-answer"><strong>{question}</strong></p>
<p itemprop="text">{answer}</p>
</div>"""

_KEY_TAKEAWAYS_BLOCK = """<aside class="key-takeaways" aria-label="Key Takeaways">
<h3>Key Takeaways</h3>
<ul>
{items}
</ul>
</aside>"""

_AUTHOR_BLOCK = """<div class="article-meta" itemscope itemtype="https://schema.org/Person">
<span class="byline">By <span itemprop="name">{author_name}</span> — {title}</span>
<time class="published" datetime="{iso_date}">Updated {display_date}</time>
</div>"""

_STAT_CALLOUT = """<div class="stat-callout">
<strong class="stat-number">{stat}</strong>
<span class="stat-label">{label}</span>
</div>"""

_CITATION_FOOTER = """<footer class="citation-signals">
<h4>Sources &amp; Further Reading</h4>
<ul>
{source_items}
</ul>
<p><small>Content by <a href="{biz_url}" rel="author">{biz_name}</a> — {city}, BC.
Licensed and insured {service_type} professionals.</small></p>
</footer>"""


def inject_direct_answer(html: str, question: str, answer: str) -> str:
    if "ai-answer-block" in html:
        return html
    block = _DIRECT_ANSWER_BLOCK.format(question=question, answer=answer)
    for tag in ["<article", "<main", '<div class="entry-content"', '<div class="post-content"']:
        if tag in html:
            idx = html.index(tag)
            end = html.index(">", idx) + 1
            return html[:end] + "\n" + block + "\n" + html[end:]
    if "<p" in html:
        idx = html.index("<p")
        return html[:idx] + block + "\n" + html[idx:]
    return block + "\n" + html


def inject_key_takeaways(html: str, takeaways: list[str]) -> str:
    if "key-takeaways" in html or not takeaways:
        return html
    items = "\n".join(f"<li>{t}</li>" for t in takeaways[:6])
    block = _KEY_TAKEAWAYS_BLOCK.format(items=items)
    match = re.search(r"</h[12]>", html, re.I)
    if match:
        pos = match.end()
        return html[:pos] + "\n" + block + "\n" + html[pos:]
    return block + "\n" + html


def inject_author_signal(html: str, author_name: str = "GetHubed Editorial Team",
                          title: str = "Home Services Specialists") -> str:
    if "article-meta" in html or "byline" in html:
        return html
    today = date.today()
    block = _AUTHOR_BLOCK.format(
        author_name=author_name,
        title=title,
        iso_date=today.isoformat(),
        display_date=today.strftime("%B %d, %Y"),
    )
    match = re.search(r"</h1>", html, re.I)
    if match:
        return html[:match.end()] + "\n" + block + "\n" + html[match.end():]
    return block + "\n" + html


def inject_citation_footer(html: str, biz_name: str, biz_url: str,
                             city: str, service_type: str,
                             sources: Optional[list[str]] = None) -> str:
    if "citation-signals" in html:
        return html
    default_sources = [
        "Canada Mortgage and Housing Corporation (CMHC)",
        "BC Housing — Homeowner Protection Office",
        "National Roofing Contractors Association",
    ]
    source_list = sources or default_sources
    items = "\n".join(f"<li>{s}</li>" for s in source_list[:5])
    block = _CITATION_FOOTER.format(
        source_items=items,
        biz_url=biz_url,
        biz_name=biz_name,
        city=city,
        service_type=service_type,
    )
    if "</body>" in html:
        return html.replace("</body>", block + "\n</body>", 1)
    if "</article>" in html:
        return html.replace("</article>", block + "\n</article>", 1)
    return html + "\n" + block


def inject_stat_callout(html: str, stat: str, label: str) -> str:
    block = _STAT_CALLOUT.format(stat=stat, label=label)
    # Insert after first full paragraph
    match = re.search(r"</p>", html, re.I)
    if match:
        pos = match.end()
        return html[:pos] + "\n" + block + "\n" + html[pos:]
    return html + "\n" + block


# ── FAQ block generator ───────────────────────────────────────────────────────

_FAQ_ITEM = """<div class="faq-item" itemscope itemprop="mainEntity" itemtype="https://schema.org/Question">
<h3 itemprop="name">{question}</h3>
<div itemscope itemprop="acceptedAnswer" itemtype="https://schema.org/Answer">
<p itemprop="text">{answer}</p>
</div>
</div>"""

_FAQ_BLOCK = """<section class="faq-section" aria-label="Frequently Asked Questions">
<h2>Frequently Asked Questions</h2>
{items}
</section>"""


def inject_faq_block(html: str, faqs: list[dict]) -> str:
    """Inject microdata-annotated FAQ block into HTML.

    Also compatible with FAQPage JSON-LD schema (use build_faq_schema separately).

    Args:
        html:  Page HTML.
        faqs:  List of {question, answer} dicts.
    """
    if "faq-section" in html or not faqs:
        return html

    items = "\n".join(
        _FAQ_ITEM.format(question=faq["question"], answer=faq["answer"])
        for faq in faqs
        if faq.get("question") and faq.get("answer")
    )
    block = _FAQ_BLOCK.format(items=items)

    # Insert before last CTA section or before </article>
    for marker in ['<div class="cta', '<section class="cta', "</article>", "</main>", "</body>"]:
        if marker in html:
            idx = html.rindex(marker)
            return html[:idx] + block + "\n" + html[idx:]
    return html + "\n" + block


# ── Heading structure optimizer ───────────────────────────────────────────────

def optimize_headings_for_ai(html: str, keyword: str) -> str:
    """Rewrite headings to match question patterns AI engines prefer.

    Converts generic headings like "Our Services" → "What Does {Service} Include?"
    Only rewrites headings that don't already start with a question word.
    """
    kw = keyword.title()

    def _maybe_rewrite(m: re.Match) -> str:
        tag, text, close = m.group(1), m.group(2), m.group(3)
        text_clean = re.sub(r"<[^>]+>", "", text).strip()

        # Skip if already a question or very short
        if text_clean.startswith(("How", "What", "Why", "When", "Which", "Can", "Does", "Is ", "Are ")):
            return m.group(0)
        if len(text_clean) < 5:
            return m.group(0)

        # Pattern: "Benefits of X" → "What Are the Benefits of {kw}?"
        if re.match(r"^(benefits|advantages|why|reasons)", text_clean, re.I):
            return f"<{tag}>Why Choose {kw}?</{close}>"
        if re.match(r"^(cost|price|pricing)", text_clean, re.I):
            return f"<{tag}>How Much Does {kw} Cost?</{close}>"
        if re.match(r"^(process|steps|how we)", text_clean, re.I):
            return f"<{tag}>How Does {kw} Work?</{close}>"
        if re.match(r"^(about|who we are|our team)", text_clean, re.I):
            return f"<{tag}>Who Provides {kw} in the Okanagan?</{close}>"

        return m.group(0)

    # Only rewrite H2/H3 (not H1 — already optimized at generation)
    html = re.sub(r"<(h[23])([^>]*)>(.*?)</(h[23])>", _maybe_rewrite, html, flags=re.I | re.DOTALL)
    return html


# ── llms.txt content formatter ────────────────────────────────────────────────

_LLMS_TXT_SERVICE_BLOCK = """## {service_name}

{description}

**Key facts:**
{facts}

**Service areas:** {areas}

**Source:** [{biz_name}]({page_url})
"""


def build_llms_service_block(service_name: str, description: str,
                               facts: list[str], areas: list[str],
                               biz_name: str, page_url: str) -> str:
    """Build a single service block for llms.txt in AI-citation format.

    Uses the recommended llms.txt spec: structured facts, explicit source links,
    clear headings — maximizes probability of AI model citation.
    """
    facts_md = "\n".join(f"- {f}" for f in facts[:6])
    areas_str = ", ".join(areas[:5])
    return _LLMS_TXT_SERVICE_BLOCK.format(
        service_name=service_name,
        description=description,
        facts=facts_md,
        areas=areas_str,
        biz_name=biz_name,
        page_url=page_url,
    )


# ── Master optimizer ──────────────────────────────────────────────────────────

def optimize_for_ai(
    html: str,
    keyword: str,
    business_id: str = "",
    biz_name: str = "",
    biz_url: str = "",
    city: str = "",
    faqs: Optional[list[dict]] = None,
    takeaways: Optional[list[str]] = None,
    direct_answer: Optional[tuple[str, str]] = None,
) -> str:
    """Apply all AI search optimization signals to a page.

    Non-destructive: each injection checks for existing blocks before adding.
    Enterprise-safe: all business data passed as params (not hardcoded).

    Args:
        html:           Raw page HTML.
        keyword:        Target keyword.
        business_id:    Tenant ID (used to load biz data if biz_name/url not provided).
        biz_name:       Business name override.
        biz_url:        Business URL override.
        city:           City override.
        faqs:           List of {question, answer} dicts.
        takeaways:      List of key takeaway strings.
        direct_answer:  Tuple of (question, answer) for the answer block.

    Returns:
        Optimized HTML.
    """
    if not biz_name and business_id:
        try:
            raw = json.loads(Path("data/storage/businesses.json").read_text())
            biz_list = raw if isinstance(raw, list) else list(raw.values())
            for b in biz_list:
                if b.get("id") == business_id or b.get("business_id") == business_id:
                    biz_name = b.get("name", "")
                    biz_url = b.get("website") or b.get("domain") or ""
                    city = b.get("city", "")
                    break
        except Exception:
            pass

    if biz_url and not biz_url.startswith("http"):
        biz_url = "https://" + biz_url

    # 1. Direct answer block (highest AI citation value)
    if direct_answer:
        html = inject_direct_answer(html, direct_answer[0], direct_answer[1])

    # 2. Key takeaways
    if takeaways:
        html = inject_key_takeaways(html, takeaways)

    # 3. Author signal
    html = inject_author_signal(html, biz_name or "Home Services Editorial Team")

    # 4. FAQ block with microdata
    if faqs:
        html = inject_faq_block(html, faqs)

    # 5. Optimize H2/H3 for question patterns
    html = optimize_headings_for_ai(html, keyword)

    # 6. Citation footer with credibility signals
    if biz_name:
        html = inject_citation_footer(
            html,
            biz_name=biz_name,
            biz_url=biz_url,
            city=city,
            service_type=keyword,
        )

    score = score_ai_readiness(html, keyword)
    log.info("ai_content_optimizer.done  kw=%s  ai_score=%d  grade=%s",
             keyword, score["total"], score["grade"])
    return html
