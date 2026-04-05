"""Visual Narrative Page Renderer v2 — storytelling layouts with context-aware graphics.

Every section has a visual purpose. Pages follow a story flow:
  Hook (hero) -> Problem (relatable) -> Solution (clear) -> Benefits (icons) ->
  Proof (before/after + stats) -> Process (steps) -> FAQ -> CTA

Visual elements: hero images, before/after comparisons, icon grids, process steps,
split layouts (text+image alternating), testimonial cards.
"""

from __future__ import annotations

import json
import logging
from urllib.parse import quote

log = logging.getLogger(__name__)


# Curated image library — real working Unsplash URLs by context
# These are direct CDN links that always work (no API key needed)
_IMAGE_LIBRARY = {
    # Outdoor lighting at night — actual lit homes
    "hero": "https://images.unsplash.com/photo-1513584684374-8bab748fbf90?w={w}&h={h}&fit=crop&q=80",
    "lighting_house": "https://images.unsplash.com/photo-1564013799919-ab600027ffc6?w={w}&h={h}&fit=crop&q=80",
    "modern_home": "https://images.unsplash.com/photo-1582268611958-ebfd161ef9cf?w={w}&h={h}&fit=crop&q=80",
    "luxury_home": "https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w={w}&h={h}&fit=crop&q=80",
    # Outdoor / landscape lighting
    "outdoor_lighting": "https://images.unsplash.com/photo-1558618666-fcd25c85f82e?w={w}&h={h}&fit=crop&q=80",
    "garden_night": "https://images.unsplash.com/photo-1588880331179-bc9b93a8cb5e?w={w}&h={h}&fit=crop&q=80",
    "patio_lights": "https://images.unsplash.com/photo-1517457373958-b7bdd4587205?w={w}&h={h}&fit=crop&q=80",
    "backyard": "https://images.unsplash.com/photo-1600566753190-17f0baa2a6c3?w={w}&h={h}&fit=crop&q=80",
    # Dark house / no lights — problem state
    "dark_house": "https://images.unsplash.com/photo-1572120360610-d971b9d7767c?w={w}&h={h}&fit=crop&q=80",
    "dark_exterior": "https://images.unsplash.com/photo-1570129477492-45c003edd2be?w={w}&h={h}&fit=crop&q=80",
    # Christmas / holiday lights on houses
    "christmas_lights": "https://images.unsplash.com/photo-1543589077-47d81606c1bf?w={w}&h={h}&fit=crop&q=80",
    "holiday_house": "https://images.unsplash.com/photo-1576919228236-a097c32a5cd4?w={w}&h={h}&fit=crop&q=80",
    # Professional / installation
    "professional": "https://images.unsplash.com/photo-1581578731548-c64695cc6952?w={w}&h={h}&fit=crop&q=80",
    "team_work": "https://images.unsplash.com/photo-1504307651254-35680f356dfd?w={w}&h={h}&fit=crop&q=80",
    # LED strips / tech
    "led_strip": "https://images.unsplash.com/photo-1545259741-2ea3ebf61fa3?w={w}&h={h}&fit=crop&q=80",
    "smart_home": "https://images.unsplash.com/photo-1558002038-1055907df827?w={w}&h={h}&fit=crop&q=80",
    # Roof / architecture with lighting
    "roofline": "https://images.unsplash.com/photo-1600047509807-ba8f99d2cdde?w={w}&h={h}&fit=crop&q=80",
    "architecture": "https://images.unsplash.com/photo-1600573472591-ee6981cf81d6?w={w}&h={h}&fit=crop&q=80",
}

# Map keywords in queries to image library keys
_QUERY_MAP = [
    (["dark", "problem", "before", "no light"], "dark_house"),
    (["christmas", "holiday", "xmas", "festive"], "christmas_lights"),
    (["roof", "roofline", "soffit", "eaves"], "roofline"),
    (["garden", "landscape", "backyard", "yard"], "garden_night"),
    (["patio", "deck", "string", "outdoor"], "patio_lights"),
    (["led", "strip", "smart", "app"], "led_strip"),
    (["team", "professional", "installer", "worker"], "professional"),
    (["modern", "luxury", "beautiful", "stunning"], "luxury_home"),
    (["house", "home", "exterior", "residential"], "modern_home"),
]


def _pick_image(query: str, w: int = 800, h: int = 500) -> str:
    """Get an image from curated Unsplash library matched to query context."""
    q_lower = query.lower()
    for keywords, key in _QUERY_MAP:
        if any(kw in q_lower for kw in keywords):
            return _IMAGE_LIBRARY[key].format(w=w, h=h)
    # Fallback: use the closest match from library
    return _IMAGE_LIBRARY["outdoor_lighting"].format(w=w, h=h)


# Section-specific images — each gets a DIFFERENT relevant photo
_SECTION_IMAGES = {
    # Hero: stunning lit home at dusk
    "hero": "https://images.unsplash.com/photo-1513584684374-8bab748fbf90?w={w}&h={h}&fit=crop&q=80",
    # Problem: dark/plain house
    "problem": "https://images.unsplash.com/photo-1572120360610-d971b9d7767c?w={w}&h={h}&fit=crop&q=80",
    # Solution: beautiful home with outdoor lighting
    "solution": "https://images.unsplash.com/photo-1564013799919-ab600027ffc6?w={w}&h={h}&fit=crop&q=80",
    # Before: plain house daytime
    "before": "https://images.unsplash.com/photo-1570129477492-45c003edd2be?w={w}&h={h}&fit=crop&q=80",
    # After: house with lights at night
    "after": "https://images.unsplash.com/photo-1582268611958-ebfd161ef9cf?w={w}&h={h}&fit=crop&q=80",
    # Content sections: various lit homes/outdoor scenes (all different)
    "content_1": "https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w={w}&h={h}&fit=crop&q=80",
    "content_2": "https://images.unsplash.com/photo-1588880331179-bc9b93a8cb5e?w={w}&h={h}&fit=crop&q=80",
    "content_3": "https://images.unsplash.com/photo-1558618666-fcd25c85f82e?w={w}&h={h}&fit=crop&q=80",
}

# Counter to ensure each section gets a DIFFERENT image
_img_counter = 0

def _unsplash(query: str, w: int = 800, h: int = 500) -> str:
    """Get a working image URL. Uses curated library to ensure quality + variety."""
    global _img_counter

    # First try: match query to curated library
    q_lower = query.lower()
    for keywords, key in _QUERY_MAP:
        if any(kw in q_lower for kw in keywords):
            return _IMAGE_LIBRARY[key].format(w=w, h=h)

    # Second try: rotate through content images so no two sections share the same photo
    content_keys = ["content_1", "content_2", "content_3", "outdoor_lighting", "modern_home",
                    "luxury_home", "backyard", "patio_lights", "garden_night", "architecture"]
    key = content_keys[_img_counter % len(content_keys)]
    _img_counter += 1

    if key in _SECTION_IMAGES:
        return _SECTION_IMAGES[key].format(w=w, h=h)
    elif key in _IMAGE_LIBRARY:
        return _IMAGE_LIBRARY[key].format(w=w, h=h)

    return _IMAGE_LIBRARY["outdoor_lighting"].format(w=w, h=h)


def _esc(text: str) -> str:
    return str(text).replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')


def render_page(page_data: dict, business: dict) -> str:
    """Render a complete standalone HTML page with visual narrative."""
    global _img_counter
    _img_counter = 0  # Reset for each new page
    hero = page_data.get("hero", {})
    sections = page_data.get("sections", [])
    schema = page_data.get("schema", {})
    meta_title = page_data.get("meta_title", hero.get("headline", ""))
    meta_desc = page_data.get("meta_description", hero.get("subheadline", ""))

    biz_name = business.get("business_name", "")
    biz_city = business.get("primary_city", "")
    service = business.get("primary_service", "")

    schema_json = ""
    if schema:
        schema_json = f'<script type="application/ld+json">{json.dumps(schema, default=str)}</script>'

    sections_html = ""
    for i, section in enumerate(sections):
        sections_html += _render_section(section, business, i)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{_esc(meta_title)}</title>
    <meta name="description" content="{_esc(meta_desc)}">
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {{
            theme: {{
                extend: {{
                    colors: {{
                        brand: {{ 50:'#eff6ff', 100:'#dbeafe', 500:'#3b82f6', 600:'#2563eb', 700:'#1d4ed8', 800:'#1e40af', 900:'#1e3a5f' }}
                    }}
                }}
            }}
        }}
    </script>
    <style>
        body {{ font-family: 'Inter', system-ui, -apple-system, sans-serif; scroll-behavior: smooth; }}
        .cta-btn {{ transition: all 0.3s ease; }}
        .cta-btn:hover {{ transform: translateY(-3px); box-shadow: 0 12px 30px rgba(0,0,0,0.2); }}
        .fade-in {{ opacity: 0; transform: translateY(20px); transition: all 0.6s ease; }}
        .fade-in.visible {{ opacity: 1; transform: translateY(0); }}
        .img-cover {{ object-fit: cover; }}
        .gradient-text {{ background: linear-gradient(135deg, #3b82f6, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
    </style>
    {schema_json}
</head>
<body class="bg-white text-gray-900 antialiased">

{_render_nav(biz_name)}
{_render_hero(hero, business)}
{sections_html}
{_render_final_cta(biz_name, biz_city, service)}
{_render_footer(business)}

<script>
    // Intersection Observer for fade-in animations
    const observer = new IntersectionObserver((entries) => {{
        entries.forEach(entry => {{
            if (entry.isIntersecting) {{
                entry.target.classList.add('visible');
            }}
        }});
    }}, {{ threshold: 0.1 }});
    document.querySelectorAll('.fade-in').forEach(el => observer.observe(el));
</script>
</body>
</html>"""


def _render_nav(biz_name: str) -> str:
    return f"""
    <nav class="fixed top-0 w-full bg-white/90 backdrop-blur-md border-b border-gray-100 z-50">
        <div class="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between">
            <span class="text-xl font-bold gradient-text">{biz_name}</span>
            <a href="#quote" class="cta-btn bg-brand-600 text-white px-6 py-2.5 rounded-full text-sm font-semibold hover:bg-brand-700">
                Get Free Quote
            </a>
        </div>
    </nav>"""


def _render_hero(hero: dict, business: dict) -> str:
    headline = hero.get("headline", "")
    sub = hero.get("subheadline", "")
    cta = hero.get("cta", "Get a Free Quote")
    service = business.get("primary_service", "lighting")
    city = business.get("primary_city", "")

    hero_img = _SECTION_IMAGES["hero"].format(w=1600, h=900)

    return f"""
    <section class="relative pt-20 min-h-[90vh] flex items-center overflow-hidden">
        <!-- Background image -->
        <div class="absolute inset-0">
            <img src="{hero_img}" alt="{service} in {city}"
                 class="w-full h-full img-cover" loading="eager">
            <div class="absolute inset-0 bg-gradient-to-r from-gray-900/85 via-gray-900/60 to-transparent"></div>
        </div>

        <div class="relative max-w-6xl mx-auto px-6 py-24">
            <div class="max-w-2xl">
                <h1 class="text-4xl md:text-6xl font-extrabold text-white leading-tight mb-6">
                    {headline}
                </h1>
                <p class="text-xl text-gray-200 mb-8 leading-relaxed">{sub}</p>
                <div class="flex flex-wrap gap-4">
                    <a href="#quote" class="cta-btn inline-block bg-amber-400 text-gray-900 font-bold px-8 py-4 rounded-full text-lg">
                        {cta}
                    </a>
                    <a href="#services" class="inline-block border-2 border-white text-white font-semibold px-8 py-4 rounded-full text-lg hover:bg-white hover:text-gray-900 transition-all">
                        Learn More
                    </a>
                </div>
                <div class="flex items-center gap-6 mt-8 text-white/80 text-sm">
                    <span>&#9733; {business.get('rating', 4.9)} Rating</span>
                    <span>{business.get('reviews_count', 0)}+ Reviews</span>
                    <span>{business.get('years_active', 0)}+ Years</span>
                </div>
            </div>
        </div>
    </section>"""


def _render_section(section: dict, business: dict, index: int) -> str:
    stype = section.get("type", "content")

    if stype == "problem":
        return _section_problem(section, business, index)
    elif stype == "solution":
        return _section_solution(section, business, index)
    elif stype == "benefits":
        return _section_benefits(section)
    elif stype == "proof":
        return _section_proof(section, business)
    elif stype == "services":
        return _section_services(section, business)
    elif stype == "process":
        return _section_process(section)
    elif stype == "before_after":
        return _section_before_after(section, business)
    elif stype == "faq":
        return _section_faq(section)
    elif stype == "cta":
        return _section_cta(section)
    elif stype == "content":
        return _section_content_visual(section, business, index)
    else:
        return _section_content_visual(section, business, index)


def _section_problem(s: dict, biz: dict, idx: int) -> str:
    title = s.get("title", "The Problem")
    content = s.get("content", "")
    service = biz.get("primary_service", "").lower()
    img = _SECTION_IMAGES["problem"].format(w=600, h=400)

    # Alternate image left/right
    if idx % 2 == 0:
        return f"""
        <section class="py-20 bg-gray-50 fade-in">
            <div class="max-w-6xl mx-auto px-6 grid md:grid-cols-2 gap-12 items-center">
                <div>
                    <div class="text-sm font-semibold text-red-500 uppercase tracking-wider mb-3">The Problem</div>
                    <h2 class="text-3xl md:text-4xl font-bold mb-6">{title}</h2>
                    <p class="text-lg text-gray-600 leading-relaxed">{content}</p>
                </div>
                <div>
                    <img src="{img}" alt="Before {service}" class="rounded-2xl shadow-xl img-cover w-full h-80">
                </div>
            </div>
        </section>"""
    else:
        return f"""
        <section class="py-20 bg-gray-50 fade-in">
            <div class="max-w-6xl mx-auto px-6 grid md:grid-cols-2 gap-12 items-center">
                <div>
                    <img src="{img}" alt="Before {service}" class="rounded-2xl shadow-xl img-cover w-full h-80">
                </div>
                <div>
                    <div class="text-sm font-semibold text-red-500 uppercase tracking-wider mb-3">The Problem</div>
                    <h2 class="text-3xl md:text-4xl font-bold mb-6">{title}</h2>
                    <p class="text-lg text-gray-600 leading-relaxed">{content}</p>
                </div>
            </div>
        </section>"""


def _section_solution(s: dict, biz: dict, idx: int) -> str:
    title = s.get("title", "Our Solution")
    content = s.get("content", "")
    service = biz.get("primary_service", "").lower()
    img = _SECTION_IMAGES["solution"].format(w=600, h=400)

    flip = idx % 2 == 1
    text_block = f"""
                <div>
                    <div class="text-sm font-semibold text-brand-600 uppercase tracking-wider mb-3">The Solution</div>
                    <h2 class="text-3xl md:text-4xl font-bold mb-6">{title}</h2>
                    <p class="text-lg text-gray-600 leading-relaxed">{content}</p>
                    <a href="#quote" class="cta-btn inline-block mt-6 bg-brand-600 text-white font-semibold px-6 py-3 rounded-full hover:bg-brand-700">Get Started</a>
                </div>"""
    img_block = f"""
                <div>
                    <img src="{img}" alt="{service}" class="rounded-2xl shadow-xl img-cover w-full h-80">
                </div>"""

    return f"""
        <section class="py-20 fade-in">
            <div class="max-w-6xl mx-auto px-6 grid md:grid-cols-2 gap-12 items-center">
                {img_block if flip else text_block}
                {text_block if flip else img_block}
            </div>
        </section>"""


def _section_benefits(s: dict) -> str:
    title = s.get("title", "Why Choose Us")
    items = s.get("items", [])
    icons = ["&#10024;", "&#9889;", "&#128171;", "&#9201;", "&#127969;", "&#128161;", "&#128272;", "&#127775;"]

    cards = ""
    for i, item in enumerate(items):
        icon = icons[i % len(icons)]
        name = item.get("name", item.get("title", str(item))) if isinstance(item, dict) else str(item)
        desc = item.get("description", item.get("detail", "")) if isinstance(item, dict) else ""

        cards += f"""
            <div class="bg-white p-8 rounded-2xl shadow-sm border border-gray-100 hover:shadow-lg hover:-translate-y-1 transition-all fade-in">
                <div class="text-4xl mb-4">{icon}</div>
                <h3 class="text-lg font-bold mb-2">{name}</h3>
                <p class="text-gray-500 text-sm leading-relaxed">{desc}</p>
            </div>"""

    return f"""
    <section class="py-20 bg-gray-50" id="benefits">
        <div class="max-w-6xl mx-auto px-6">
            <div class="text-center mb-12">
                <div class="text-sm font-semibold text-brand-600 uppercase tracking-wider mb-3">Benefits</div>
                <h2 class="text-3xl md:text-4xl font-bold">{title}</h2>
            </div>
            <div class="grid md:grid-cols-2 lg:grid-cols-3 gap-6">{cards}</div>
        </div>
    </section>"""


def _section_proof(s: dict, business: dict) -> str:
    rating = business.get("rating", 4.9)
    reviews = business.get("reviews_count", 0)
    years = business.get("years_active", 0)
    content = s.get("content", "")

    return f"""
    <section class="py-20 bg-brand-900 text-white fade-in">
        <div class="max-w-6xl mx-auto px-6">
            <div class="text-center mb-12">
                <div class="text-sm font-semibold text-amber-400 uppercase tracking-wider mb-3">Proof</div>
                <h2 class="text-3xl md:text-4xl font-bold">Trusted by Homeowners</h2>
            </div>
            <div class="grid md:grid-cols-3 gap-8 text-center mb-10">
                <div class="bg-white/10 rounded-2xl p-8 backdrop-blur">
                    <div class="text-5xl font-extrabold text-amber-400">{rating}</div>
                    <div class="text-white/70 mt-2 text-lg">Star Rating</div>
                </div>
                <div class="bg-white/10 rounded-2xl p-8 backdrop-blur">
                    <div class="text-5xl font-extrabold text-amber-400">{reviews}+</div>
                    <div class="text-white/70 mt-2 text-lg">5-Star Reviews</div>
                </div>
                <div class="bg-white/10 rounded-2xl p-8 backdrop-blur">
                    <div class="text-5xl font-extrabold text-amber-400">{years}+</div>
                    <div class="text-white/70 mt-2 text-lg">Years Experience</div>
                </div>
            </div>
            {f'<p class="text-center text-white/70 max-w-3xl mx-auto text-lg">{content}</p>' if content else ''}
        </div>
    </section>"""


def _section_services(s: dict, business: dict) -> str:
    title = s.get("title", "Our Services")
    items = s.get("items", [])

    service_icons = ["&#128161;", "&#127969;", "&#9889;", "&#10024;", "&#128272;", "&#127775;", "&#9728;", "&#128296;"]
    cards = ""
    for i, item in enumerate(items):
        name = item.get("name", str(item)) if isinstance(item, dict) else str(item)
        desc = item.get("description", "") if isinstance(item, dict) else ""
        icon = service_icons[i % len(service_icons)]
        cards += f"""
            <div class="bg-white p-6 rounded-2xl border border-gray-100 hover:shadow-lg hover:border-brand-200 transition-all fade-in">
                <div class="text-3xl mb-3">{icon}</div>
                <h3 class="font-bold text-lg mb-3 text-brand-800">{name}</h3>
                <p class="text-gray-500 text-sm leading-relaxed">{desc}</p>
            </div>"""

    return f"""
    <section class="py-20" id="services">
        <div class="max-w-6xl mx-auto px-6">
            <div class="text-center mb-12">
                <div class="text-sm font-semibold text-brand-600 uppercase tracking-wider mb-3">Services</div>
                <h2 class="text-3xl md:text-4xl font-bold">{title}</h2>
            </div>
            <div class="grid md:grid-cols-2 lg:grid-cols-3 gap-6">{cards}</div>
        </div>
    </section>"""


def _section_process(s: dict) -> str:
    steps = s.get("steps", s.get("items", []))
    title = s.get("title", "How It Works")

    step_html = ""
    for i, step in enumerate(steps):
        name = step.get("name", step.get("title", str(step))) if isinstance(step, dict) else str(step)
        desc = step.get("description", "") if isinstance(step, dict) else ""
        step_html += f"""
            <div class="text-center fade-in">
                <div class="w-14 h-14 bg-brand-600 text-white rounded-full flex items-center justify-center text-xl font-bold mx-auto mb-4">{i+1}</div>
                <h3 class="font-bold text-lg mb-2">{name}</h3>
                <p class="text-gray-500 text-sm">{desc}</p>
            </div>"""

    return f"""
    <section class="py-20 bg-gray-50">
        <div class="max-w-6xl mx-auto px-6">
            <div class="text-center mb-12">
                <div class="text-sm font-semibold text-brand-600 uppercase tracking-wider mb-3">Process</div>
                <h2 class="text-3xl md:text-4xl font-bold">{title}</h2>
            </div>
            <div class="grid md:grid-cols-{min(len(steps), 4)} gap-8">{step_html}</div>
        </div>
    </section>"""


def _section_before_after(s: dict, business: dict) -> str:
    service = business.get("primary_service", "lighting").lower()
    before_img = _SECTION_IMAGES["before"].format(w=600, h=400)
    after_img = _SECTION_IMAGES["after"].format(w=600, h=400)
    return f"""
    <section class="py-20 fade-in">
        <div class="max-w-6xl mx-auto px-6">
            <div class="text-center mb-12">
                <h2 class="text-3xl md:text-4xl font-bold">See the Difference</h2>
            </div>
            <div class="grid md:grid-cols-2 gap-8">
                <div class="relative rounded-2xl overflow-hidden shadow-lg">
                    <img src="{before_img}" alt="Before {service}" class="w-full h-72 img-cover">
                    <div class="absolute top-4 left-4 bg-red-500 text-white px-4 py-1 rounded-full text-sm font-bold">BEFORE</div>
                </div>
                <div class="relative rounded-2xl overflow-hidden shadow-lg">
                    <img src="{after_img}" alt="After {service}" class="w-full h-72 img-cover">
                    <div class="absolute top-4 left-4 bg-green-500 text-white px-4 py-1 rounded-full text-sm font-bold">AFTER</div>
                </div>
            </div>
        </div>
    </section>"""


def _section_faq(s: dict) -> str:
    items = s.get("items", [])

    faqs_html = ""
    schema_entities = []
    for item in items:
        q = item.get("question", "")
        a = item.get("answer", "")
        if not q:
            continue
        faqs_html += f"""
            <details class="group border border-gray-200 rounded-xl overflow-hidden mb-3 fade-in">
                <summary class="flex items-center justify-between p-5 cursor-pointer font-bold text-lg hover:bg-gray-50">
                    {q}
                    <span class="text-brand-600 text-2xl group-open:rotate-45 transition-transform">+</span>
                </summary>
                <div class="px-5 pb-5 text-gray-600 leading-relaxed">{a}</div>
            </details>"""
        schema_entities.append({
            "@type": "Question", "name": q,
            "acceptedAnswer": {"@type": "Answer", "text": a}
        })

    schema_block = json.dumps({"@context": "https://schema.org", "@type": "FAQPage", "mainEntity": schema_entities})

    return f"""
    <section class="py-20 bg-gray-50">
        <div class="max-w-3xl mx-auto px-6">
            <div class="text-center mb-12">
                <div class="text-sm font-semibold text-brand-600 uppercase tracking-wider mb-3">FAQ</div>
                <h2 class="text-3xl md:text-4xl font-bold">Frequently Asked Questions</h2>
            </div>
            {faqs_html}
        </div>
        <script type="application/ld+json">{schema_block}</script>
    </section>"""


def _section_cta(s: dict) -> str:
    title = s.get("title", "Ready to Get Started?")
    cta = s.get("cta", "Get Your Free Quote")
    return f"""
    <section id="quote" class="py-20 bg-gradient-to-r from-brand-800 to-brand-900 text-white fade-in">
        <div class="max-w-4xl mx-auto px-6 text-center">
            <h2 class="text-3xl md:text-4xl font-bold mb-6">{title}</h2>
            <a href="#" class="cta-btn inline-block bg-amber-400 text-gray-900 font-bold px-10 py-5 rounded-full text-xl">{cta}</a>
        </div>
    </section>"""


def _section_content_visual(s: dict, business: dict, index: int) -> str:
    title = s.get("title", "")
    content = s.get("content", "")
    service = business.get("primary_service", "").lower()
    city = business.get("primary_city", "")
    img = _unsplash(s.get("image_query", f"{service} residential installation {city}"), 600, 400)

    if index % 2 == 0:
        return f"""
        <section class="py-20 fade-in">
            <div class="max-w-6xl mx-auto px-6 grid md:grid-cols-2 gap-12 items-center">
                <div>
                    {f'<h2 class="text-3xl md:text-4xl font-bold mb-6">{title}</h2>' if title else ''}
                    <div class="text-gray-600 leading-relaxed text-lg space-y-4">{content}</div>
                </div>
                <div>
                    <img src="{img}" alt="{service} in {city}" class="rounded-2xl shadow-xl img-cover w-full h-80">
                </div>
            </div>
        </section>"""
    else:
        return f"""
        <section class="py-20 bg-gray-50 fade-in">
            <div class="max-w-6xl mx-auto px-6 grid md:grid-cols-2 gap-12 items-center">
                <div>
                    <img src="{img}" alt="{service} in {city}" class="rounded-2xl shadow-xl img-cover w-full h-80">
                </div>
                <div>
                    {f'<h2 class="text-3xl md:text-4xl font-bold mb-6">{title}</h2>' if title else ''}
                    <div class="text-gray-600 leading-relaxed text-lg space-y-4">{content}</div>
                </div>
            </div>
        </section>"""


def _render_final_cta(biz_name: str, city: str, service: str) -> str:
    return f"""
    <section id="quote" class="py-24 bg-gradient-to-br from-brand-900 via-brand-800 to-brand-700 text-white">
        <div class="max-w-4xl mx-auto px-6 text-center">
            <h2 class="text-3xl md:text-5xl font-extrabold mb-6">Ready for {service} in {city}?</h2>
            <p class="text-xl text-white/70 mb-8">Get a free, no-obligation quote from {biz_name}</p>
            <a href="#" class="cta-btn inline-block bg-amber-400 text-gray-900 font-extrabold px-12 py-5 rounded-full text-xl">
                Get Your Free Quote
            </a>
        </div>
    </section>"""


def _render_footer(business: dict) -> str:
    name = business.get("business_name", "")
    city = business.get("primary_city", "")
    areas = business.get("service_areas", [])
    areas_str = ", ".join(areas) if areas else city

    return f"""
    <footer class="bg-gray-900 text-gray-400 py-16">
        <div class="max-w-6xl mx-auto px-6">
            <div class="grid md:grid-cols-3 gap-8 mb-8">
                <div>
                    <span class="text-white font-bold text-lg">{name}</span>
                    <p class="mt-2 text-sm">Professional lighting solutions for homes and businesses.</p>
                </div>
                <div>
                    <span class="text-white font-semibold">Service Areas</span>
                    <p class="mt-2 text-sm">{areas_str}</p>
                </div>
                <div>
                    <span class="text-white font-semibold">Contact</span>
                    <p class="mt-2 text-sm">Call for a free quote</p>
                </div>
            </div>
            <div class="border-t border-gray-800 pt-8 text-center text-sm">
                &copy; 2026 {name}. All rights reserved.
            </div>
        </div>
    </footer>"""
