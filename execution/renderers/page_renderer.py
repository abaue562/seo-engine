"""Visual Narrative Page Renderer v3 — storytelling layouts with context-aware graphics.

Every section has a visual purpose. Pages follow a story flow:
  Hook (hero) -> Problem (relatable) -> Solution (clear) -> Benefits (icons) ->
  Proof (before/after + stats) -> Process (steps) -> FAQ -> CTA

Visual elements: hero images, before/after comparisons, icon grids, process steps,
split layouts (text+image alternating), testimonial cards.

CWV fixes (v3):
  - Removed Tailwind CDN (saves 400-800 ms LCP); replaced with ~250-line inline CSS.
  - Viewport meta always present.
  - All <img> tags: loading=lazy, width/height, decoding=async.
  - Hero image: loading=eager, fetchpriority=high.
  - Unsplash preconnect hint added.
  - robots + last-modified meta added.
  - Optional canonical <link> injected when page_url is provided.
  - Footer shows "Last updated: {date}".
"""

from __future__ import annotations

import json
import logging
from datetime import date
from urllib.parse import quote

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Inline critical CSS — replaces Tailwind CDN to eliminate render-blocking
# resource and save 400-800 ms LCP on first contentful paint.
# ---------------------------------------------------------------------------
_INLINE_CSS = """
/* === Reset & base === */
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html{line-height:1.5;-webkit-text-size-adjust:100%;tab-size:4}
body{font-family:'Inter',system-ui,-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;color:#111827;background:#fff}
img,video{max-width:100%;display:block}
a{color:inherit;text-decoration:none}

/* === Layout — display === */
.block{display:block}.inline-block{display:inline-block}.inline{display:inline}
.flex{display:flex}.inline-flex{display:inline-flex}
.grid{display:grid}.hidden{display:none}
.relative{position:relative}.absolute{position:absolute}.fixed{position:fixed}.sticky{position:sticky}
.inset-0{top:0;right:0;bottom:0;left:0}
.top-0{top:0}.right-0{right:0}.bottom-0{bottom:0}.left-0{left:0}
.top-4{top:1rem}.left-4{left:1rem}
.z-50{z-index:50}
.overflow-hidden{overflow:hidden}
.w-full{width:100%}.h-full{height:100%}
.w-14{width:3.5rem}.h-14{height:3.5rem}
.h-72{height:18rem}.h-80{height:20rem}
.min-h-screen{min-height:100vh}
.min-h-\[90vh\]{min-height:90vh}

/* === Flexbox === */
.flex-wrap{flex-wrap:wrap}
.flex-col{flex-direction:column}
.items-center{align-items:center}
.items-start{align-items:flex-start}
.items-end{align-items:flex-end}
.justify-center{justify-content:center}
.justify-between{justify-content:space-between}
.justify-start{justify-content:flex-start}
.justify-end{justify-content:flex-end}
.flex-1{flex:1 1 0%}
.flex-none{flex:none}
.self-center{align-self:center}

/* === Grid === */
.grid-cols-1{grid-template-columns:repeat(1,minmax(0,1fr))}
.grid-cols-2{grid-template-columns:repeat(2,minmax(0,1fr))}
.grid-cols-3{grid-template-columns:repeat(3,minmax(0,1fr))}
.grid-cols-4{grid-template-columns:repeat(4,minmax(0,1fr))}
.col-span-2{grid-column:span 2/span 2}
.col-span-3{grid-column:span 3/span 3}

/* === Spacing — margin === */
.m-0{margin:0}.mx-auto{margin-left:auto;margin-right:auto}
.mt-2{margin-top:.5rem}.mt-3{margin-top:.75rem}.mt-4{margin-top:1rem}
.mt-6{margin-top:1.5rem}.mt-8{margin-top:2rem}.mt-10{margin-top:2.5rem}
.mt-12{margin-top:3rem}.mt-16{margin-top:4rem}
.mb-2{margin-bottom:.5rem}.mb-3{margin-bottom:.75rem}.mb-4{margin-bottom:1rem}
.mb-6{margin-bottom:1.5rem}.mb-8{margin-bottom:2rem}.mb-10{margin-bottom:2.5rem}
.mb-12{margin-bottom:3rem}.mb-16{margin-bottom:4rem}
.ml-2{margin-left:.5rem}.mr-2{margin-right:.5rem}
.gap-2{gap:.5rem}.gap-3{gap:.75rem}.gap-4{gap:1rem}
.gap-6{gap:1.5rem}.gap-8{gap:2rem}.gap-10{gap:2.5rem}.gap-12{gap:3rem}

/* === Spacing — padding === */
.p-4{padding:1rem}.p-5{padding:1.25rem}.p-6{padding:1.5rem}
.p-8{padding:2rem}.p-10{padding:2.5rem}.p-12{padding:3rem}
.px-4{padding-left:1rem;padding-right:1rem}
.px-6{padding-left:1.5rem;padding-right:1.5rem}
.px-8{padding-left:2rem;padding-right:2rem}
.px-10{padding-left:2.5rem;padding-right:2.5rem}
.px-12{padding-left:3rem;padding-right:3rem}
.py-2{padding-top:.5rem;padding-bottom:.5rem}
.py-2\.5{padding-top:.625rem;padding-bottom:.625rem}
.py-3{padding-top:.75rem;padding-bottom:.75rem}
.py-4{padding-top:1rem;padding-bottom:1rem}
.py-5{padding-top:1.25rem;padding-bottom:1.25rem}
.py-8{padding-top:2rem;padding-bottom:2rem}
.py-16{padding-top:4rem;padding-bottom:4rem}
.py-20{padding-top:5rem;padding-bottom:5rem}
.py-24{padding-top:6rem;padding-bottom:6rem}
.pt-20{padding-top:5rem}.pb-5{padding-bottom:1.25rem}
.pb-8{padding-bottom:2rem}

/* === Typography === */
.text-xs{font-size:.75rem;line-height:1rem}
.text-sm{font-size:.875rem;line-height:1.25rem}
.text-base{font-size:1rem;line-height:1.5rem}
.text-lg{font-size:1.125rem;line-height:1.75rem}
.text-xl{font-size:1.25rem;line-height:1.75rem}
.text-2xl{font-size:1.5rem;line-height:2rem}
.text-3xl{font-size:1.875rem;line-height:2.25rem}
.text-4xl{font-size:2.25rem;line-height:2.5rem}
.text-5xl{font-size:3rem;line-height:1}
.text-6xl{font-size:3.75rem;line-height:1}
.font-normal{font-weight:400}.font-medium{font-weight:500}
.font-semibold{font-weight:600}.font-bold{font-weight:700}
.font-extrabold{font-weight:800}.font-black{font-weight:900}
.leading-none{line-height:1}.leading-tight{line-height:1.25}
.leading-snug{line-height:1.375}.leading-normal{line-height:1.5}
.leading-relaxed{line-height:1.625}.leading-loose{line-height:2}
.tracking-tight{letter-spacing:-.025em}
.tracking-wide{letter-spacing:.025em}
.tracking-wider{letter-spacing:.05em}
.tracking-widest{letter-spacing:.1em}
.uppercase{text-transform:uppercase}.capitalize{text-transform:capitalize}
.text-center{text-align:center}.text-left{text-align:left}.text-right{text-align:right}
.antialiased{-webkit-font-smoothing:antialiased;-moz-osx-font-smoothing:grayscale}
.truncate{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.space-y-4>*+*{margin-top:1rem}
.space-y-6>*+*{margin-top:1.5rem}
.space-x-4>*+*{margin-left:1rem}
.whitespace-nowrap{white-space:nowrap}

/* === Colors — text === */
.text-white{color:#fff}.text-black{color:#000}
.text-gray-400{color:#9ca3af}.text-gray-500{color:#6b7280}
.text-gray-600{color:#4b5563}.text-gray-700{color:#374151}
.text-gray-800{color:#1f2937}.text-gray-900{color:#111827}
.text-red-500{color:#ef4444}.text-red-600{color:#dc2626}
.text-green-500{color:#22c55e}.text-green-600{color:#16a34a}
.text-blue-500{color:#3b82f6}.text-blue-600{color:#2563eb}
.text-amber-400{color:#fbbf24}.text-amber-500{color:#f59e0b}
.text-brand-600{color:#2563eb}.text-brand-800{color:#1e40af}

/* === Colors — backgrounds === */
.bg-white{background-color:#fff}.bg-black{background-color:#000}
.bg-gray-50{background-color:#f9fafb}.bg-gray-100{background-color:#f3f4f6}
.bg-gray-200{background-color:#e5e7eb}.bg-gray-800{background-color:#1f2937}
.bg-gray-900{background-color:#111827}
.bg-red-500{background-color:#ef4444}
.bg-green-500{background-color:#22c55e}
.bg-amber-400{background-color:#fbbf24}
.bg-brand-600{background-color:#2563eb}
.bg-brand-700{background-color:#1d4ed8}
.bg-brand-800{background-color:#1e40af}
.bg-brand-900{background-color:#1e3a5f}

/* === Colors — border === */
.border{border-width:1px;border-style:solid}
.border-2{border-width:2px;border-style:solid}
.border-t{border-top-width:1px;border-top-style:solid}
.border-b{border-bottom-width:1px;border-bottom-style:solid}
.border-gray-100{border-color:#f3f4f6}
.border-gray-200{border-color:#e5e7eb}
.border-gray-800{border-color:#1f2937}
.border-brand-200{border-color:#bfdbfe}
.border-white{border-color:#fff}

/* === Gradients === */
.bg-gradient-to-r{background-image:linear-gradient(to right,var(--tw-gradient-stops))}
.bg-gradient-to-br{background-image:linear-gradient(to bottom right,var(--tw-gradient-stops))}
.from-gray-900\/85{--tw-gradient-from:rgba(17,24,39,.85);--tw-gradient-stops:var(--tw-gradient-from),var(--tw-gradient-to,rgba(17,24,39,0))}
.via-gray-900\/60{--tw-gradient-stops:var(--tw-gradient-from),rgba(17,24,39,.6),var(--tw-gradient-to,rgba(17,24,39,0))}
.to-transparent{--tw-gradient-to:transparent}
.from-brand-800{--tw-gradient-from:#1e40af;--tw-gradient-stops:var(--tw-gradient-from),var(--tw-gradient-to,rgba(30,64,175,0))}
.from-brand-900{--tw-gradient-from:#1e3a5f;--tw-gradient-stops:var(--tw-gradient-from),var(--tw-gradient-to,rgba(30,58,95,0))}
.via-brand-800{--tw-gradient-stops:var(--tw-gradient-from),#1e40af,var(--tw-gradient-to,rgba(30,64,175,0))}
.to-brand-700{--tw-gradient-to:#1d4ed8}
.to-brand-900{--tw-gradient-to:#1e3a5f}

/* === Opacity === */
.opacity-0{opacity:0}.opacity-100{opacity:1}
.bg-white\/10{background-color:rgba(255,255,255,.1)}
.bg-white\/90{background-color:rgba(255,255,255,.9)}
.text-white\/70{color:rgba(255,255,255,.7)}
.text-white\/80{color:rgba(255,255,255,.8)}

/* === Border radius === */
.rounded{border-radius:.25rem}.rounded-md{border-radius:.375rem}
.rounded-lg{border-radius:.5rem}.rounded-xl{border-radius:.75rem}
.rounded-2xl{border-radius:1rem}.rounded-3xl{border-radius:1.5rem}
.rounded-full{border-radius:9999px}

/* === Shadows === */
.shadow{box-shadow:0 1px 3px 0 rgba(0,0,0,.1),0 1px 2px -1px rgba(0,0,0,.1)}
.shadow-sm{box-shadow:0 1px 2px 0 rgba(0,0,0,.05)}
.shadow-md{box-shadow:0 4px 6px -1px rgba(0,0,0,.1),0 2px 4px -2px rgba(0,0,0,.1)}
.shadow-lg{box-shadow:0 10px 15px -3px rgba(0,0,0,.1),0 4px 6px -4px rgba(0,0,0,.1)}
.shadow-xl{box-shadow:0 20px 25px -5px rgba(0,0,0,.1),0 8px 10px -6px rgba(0,0,0,.1)}

/* === Max-width / container === */
.max-w-xs{max-width:20rem}.max-w-sm{max-width:24rem}
.max-w-md{max-width:28rem}.max-w-lg{max-width:32rem}
.max-w-xl{max-width:36rem}.max-w-2xl{max-width:42rem}
.max-w-3xl{max-width:48rem}.max-w-4xl{max-width:56rem}
.max-w-5xl{max-width:64rem}.max-w-6xl{max-width:72rem}
.max-w-7xl{max-width:80rem}.max-w-full{max-width:100%}

/* === Object-fit === */
.object-cover{object-fit:cover}
.object-contain{object-fit:contain}
.object-center{object-position:center}

/* === Backdrop === */
.backdrop-blur-md{backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px)}
.backdrop-blur{backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px)}

/* === Transitions === */
.transition-all{transition:all .15s cubic-bezier(.4,0,.2,1)}
.transition{transition:color .15s cubic-bezier(.4,0,.2,1),background-color .15s cubic-bezier(.4,0,.2,1),border-color .15s cubic-bezier(.4,0,.2,1),opacity .15s cubic-bezier(.4,0,.2,1),box-shadow .15s cubic-bezier(.4,0,.2,1),transform .15s cubic-bezier(.4,0,.2,1)}
.duration-300{transition-duration:.3s}
.ease-in-out{transition-timing-function:cubic-bezier(.4,0,.2,1)}

/* === Transforms === */
.rotate-45{transform:rotate(45deg)}
.group-open\:rotate-45{} /* applied via JS / details[open] */
details[open] .group-open\:rotate-45{transform:rotate(45deg)}
.-translate-y-1{transform:translateY(-.25rem)}

/* === Scroll === */
html{scroll-behavior:smooth}

/* === Responsive — mobile first, md=768px, lg=1024px === */
@media(min-width:768px){
  .md\:grid-cols-2{grid-template-columns:repeat(2,minmax(0,1fr))}
  .md\:grid-cols-3{grid-template-columns:repeat(3,minmax(0,1fr))}
  .md\:grid-cols-4{grid-template-columns:repeat(4,minmax(0,1fr))}
  .md\:text-4xl{font-size:2.25rem;line-height:2.5rem}
  .md\:text-5xl{font-size:3rem;line-height:1}
  .md\:text-6xl{font-size:3.75rem;line-height:1}
  .md\:flex{display:flex}
  .md\:hidden{display:none}
  .md\:px-10{padding-left:2.5rem;padding-right:2.5rem}
}
@media(min-width:1024px){
  .lg\:grid-cols-2{grid-template-columns:repeat(2,minmax(0,1fr))}
  .lg\:grid-cols-3{grid-template-columns:repeat(3,minmax(0,1fr))}
  .lg\:grid-cols-4{grid-template-columns:repeat(4,minmax(0,1fr))}
  .lg\:text-5xl{font-size:3rem;line-height:1}
  .lg\:px-12{padding-left:3rem;padding-right:3rem}
}

/* === Brand palette (custom — mirrors old Tailwind config) === */
:root{
  --brand-50:#eff6ff;--brand-100:#dbeafe;
  --brand-500:#3b82f6;--brand-600:#2563eb;
  --brand-700:#1d4ed8;--brand-800:#1e40af;--brand-900:#1e3a5f;
}

/* === Component styles === */
body{scroll-behavior:smooth}
.cta-btn{transition:all .3s ease;display:inline-block}
.cta-btn:hover{transform:translateY(-3px);box-shadow:0 12px 30px rgba(0,0,0,.2)}
.fade-in{opacity:0;transform:translateY(20px);transition:all .6s ease}
.fade-in.visible{opacity:1;transform:translateY(0)}
.img-cover{object-fit:cover;width:100%}
.gradient-text{background:linear-gradient(135deg,#3b82f6,#8b5cf6);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.hover\:shadow-lg:hover{box-shadow:0 10px 15px -3px rgba(0,0,0,.1),0 4px 6px -4px rgba(0,0,0,.1)}
.hover\:-translate-y-1:hover{transform:translateY(-.25rem)}
.hover\:bg-brand-700:hover{background-color:#1d4ed8}
.hover\:bg-white:hover{background-color:#fff}
.hover\:text-gray-900:hover{color:#111827}
.hover\:bg-gray-50:hover{background-color:#f9fafb}
.hover\:border-brand-200:hover{border-color:#bfdbfe}
"""


# ---------------------------------------------------------------------------
# Curated image library — real working Unsplash URLs by context
# ---------------------------------------------------------------------------
_IMAGE_LIBRARY = {
    "hero":            "https://images.unsplash.com/photo-1513584684374-8bab748fbf90?w={w}&h={h}&fit=crop&q=80",
    "lighting_house":  "https://images.unsplash.com/photo-1564013799919-ab600027ffc6?w={w}&h={h}&fit=crop&q=80",
    "modern_home":     "https://images.unsplash.com/photo-1582268611958-ebfd161ef9cf?w={w}&h={h}&fit=crop&q=80",
    "luxury_home":     "https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w={w}&h={h}&fit=crop&q=80",
    "outdoor_lighting":"https://images.unsplash.com/photo-1558618666-fcd25c85f82e?w={w}&h={h}&fit=crop&q=80",
    "garden_night":    "https://images.unsplash.com/photo-1588880331179-bc9b93a8cb5e?w={w}&h={h}&fit=crop&q=80",
    "patio_lights":    "https://images.unsplash.com/photo-1517457373958-b7bdd4587205?w={w}&h={h}&fit=crop&q=80",
    "backyard":        "https://images.unsplash.com/photo-1600566753190-17f0baa2a6c3?w={w}&h={h}&fit=crop&q=80",
    "dark_house":      "https://images.unsplash.com/photo-1572120360610-d971b9d7767c?w={w}&h={h}&fit=crop&q=80",
    "dark_exterior":   "https://images.unsplash.com/photo-1570129477492-45c003edd2be?w={w}&h={h}&fit=crop&q=80",
    "christmas_lights":"https://images.unsplash.com/photo-1543589077-47d81606c1bf?w={w}&h={h}&fit=crop&q=80",
    "holiday_house":   "https://images.unsplash.com/photo-1576919228236-a097c32a5cd4?w={w}&h={h}&fit=crop&q=80",
    "professional":    "https://images.unsplash.com/photo-1581578731548-c64695cc6952?w={w}&h={h}&fit=crop&q=80",
    "team_work":       "https://images.unsplash.com/photo-1504307651254-35680f356dfd?w={w}&h={h}&fit=crop&q=80",
    "led_strip":       "https://images.unsplash.com/photo-1545259741-2ea3ebf61fa3?w={w}&h={h}&fit=crop&q=80",
    "smart_home":      "https://images.unsplash.com/photo-1558002038-1055907df827?w={w}&h={h}&fit=crop&q=80",
    "roofline":        "https://images.unsplash.com/photo-1600047509807-ba8f99d2cdde?w={w}&h={h}&fit=crop&q=80",
    "architecture":    "https://images.unsplash.com/photo-1600573472591-ee6981cf81d6?w={w}&h={h}&fit=crop&q=80",
}

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

_SECTION_IMAGES = {
    "hero":      "https://images.unsplash.com/photo-1513584684374-8bab748fbf90?w={w}&h={h}&fit=crop&q=80",
    "problem":   "https://images.unsplash.com/photo-1572120360610-d971b9d7767c?w={w}&h={h}&fit=crop&q=80",
    "solution":  "https://images.unsplash.com/photo-1564013799919-ab600027ffc6?w={w}&h={h}&fit=crop&q=80",
    "before":    "https://images.unsplash.com/photo-1570129477492-45c003edd2be?w={w}&h={h}&fit=crop&q=80",
    "after":     "https://images.unsplash.com/photo-1582268611958-ebfd161ef9cf?w={w}&h={h}&fit=crop&q=80",
    "content_1": "https://images.unsplash.com/photo-1600596542815-ffad4c1539a9?w={w}&h={h}&fit=crop&q=80",
    "content_2": "https://images.unsplash.com/photo-1588880331179-bc9b93a8cb5e?w={w}&h={h}&fit=crop&q=80",
    "content_3": "https://images.unsplash.com/photo-1558618666-fcd25c85f82e?w={w}&h={h}&fit=crop&q=80",
}

_img_counter = 0


def _pick_image(query: str, w: int = 800, h: int = 500) -> str:
    q_lower = query.lower()
    for keywords, key in _QUERY_MAP:
        if any(kw in q_lower for kw in keywords):
            return _IMAGE_LIBRARY[key].format(w=w, h=h)
    return _IMAGE_LIBRARY["outdoor_lighting"].format(w=w, h=h)


def _unsplash(query: str, w: int = 800, h: int = 500) -> str:
    global _img_counter
    q_lower = query.lower()
    for keywords, key in _QUERY_MAP:
        if any(kw in q_lower for kw in keywords):
            return _IMAGE_LIBRARY[key].format(w=w, h=h)

    content_keys = [
        "content_1", "content_2", "content_3", "outdoor_lighting", "modern_home",
        "luxury_home", "backyard", "patio_lights", "garden_night", "architecture",
    ]
    key = content_keys[_img_counter % len(content_keys)]
    _img_counter += 1

    if key in _SECTION_IMAGES:
        return _SECTION_IMAGES[key].format(w=w, h=h)
    return _IMAGE_LIBRARY.get(key, _IMAGE_LIBRARY["outdoor_lighting"]).format(w=w, h=h)


def _esc(text: str) -> str:
    return str(text).replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')


# ---------------------------------------------------------------------------
# Public render entry point
# ---------------------------------------------------------------------------

def render_page(page_data: dict, business: dict, page_url: str = None) -> str:
    """Render a complete standalone HTML page with visual narrative.

    Args:
        page_data:  Dict with hero, sections, schema, meta_title, meta_description.
        business:   Dict with business_name, primary_city, primary_service, etc.
        page_url:   Optional canonical URL. When provided, a <link rel="canonical">
                    tag is injected into <head>.

    Returns:
        Complete HTML string.
    """
    global _img_counter
    _img_counter = 0

    hero       = page_data.get("hero", {})
    sections   = page_data.get("sections", [])
    schema     = page_data.get("schema", {})
    meta_title = page_data.get("meta_title", hero.get("headline", ""))
    meta_desc  = page_data.get("meta_description", hero.get("subheadline", ""))

    biz_name = business.get("business_name", "")
    biz_city = business.get("primary_city", "")
    service  = business.get("primary_service", "")

    today_iso  = date.today().isoformat()          # e.g. "2026-04-15"
    today_disp = date.today().strftime("%B %-d, %Y") if hasattr(date, "strftime") else today_iso

    schema_json = ""
    if schema:
        schema_json = (
            f'<script type="application/ld+json">'
            f'{json.dumps(schema, default=str)}'
            f'</script>'
        )

    canonical_tag = ""
    if page_url:
        canonical_tag = f'    <link rel="canonical" href="{_esc(page_url)}">'

    sections_html = ""
    for i, section in enumerate(sections):
        sections_html += _render_section(section, business, i)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta name="robots" content="index, follow">
    <meta name="last-modified" content="{today_iso}">
    <title>{_esc(meta_title)}</title>
    <meta name="description" content="{_esc(meta_desc)}">
    <!-- Preconnect to image CDN before any Unsplash <img> tags -->
    <link rel="preconnect" href="https://images.unsplash.com">
{canonical_tag}
    <!-- Inline critical CSS — replaces Tailwind CDN to eliminate render-blocking resource -->
    <style>{_INLINE_CSS}</style>
    {schema_json}
</head>
<body class="bg-white text-gray-900 antialiased">

{_render_nav(biz_name)}
{_render_hero(hero, business)}
{sections_html}
{_render_final_cta(biz_name, biz_city, service)}
{_render_footer(business, today_disp)}

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


# ---------------------------------------------------------------------------
# Section renderers
# ---------------------------------------------------------------------------

def _render_nav(biz_name: str) -> str:
    return f"""
    <nav class="fixed top-0 w-full bg-white\/90 backdrop-blur-md border-b border-gray-100 z-50">
        <div class="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between">
            <span class="text-xl font-bold gradient-text">{biz_name}</span>
            <a href="#quote" class="cta-btn bg-brand-600 text-white px-6 py-2.5 rounded-full text-sm font-semibold hover:bg-brand-700">
                Get Free Quote
            </a>
        </div>
    </nav>"""


def _render_hero(hero: dict, business: dict) -> str:
    headline = hero.get("headline", "")
    sub      = hero.get("subheadline", "")
    cta      = hero.get("cta", "Get a Free Quote")
    service  = business.get("primary_service", "lighting")
    city     = business.get("primary_city", "")

    hero_img = _SECTION_IMAGES["hero"].format(w=1600, h=900)

    return f"""
    <section class="relative pt-20 min-h-[90vh] flex items-center overflow-hidden">
        <!-- Background image — above-fold hero: eager load + high fetch priority -->
        <div class="absolute inset-0">
            <img src="{hero_img}"
                 alt="{_esc(service)} in {_esc(city)}"
                 class="w-full h-full img-cover"
                 width="1600" height="900"
                 loading="eager"
                 fetchpriority="high"
                 decoding="async">
            <div class="absolute inset-0 bg-gradient-to-r from-gray-900\/85 via-gray-900\/60 to-transparent"></div>
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
                <div class="flex items-center gap-6 mt-8 text-white\/80 text-sm">
                    <span>&#9733; {business.get('rating', 4.9)} Rating</span>
                    <span>{business.get('reviews_count', 0)}+ Reviews</span>
                    <span>{business.get('years_active', 0)}+ Years</span>
                </div>
            </div>
        </div>
    </section>"""


def _render_section(section: dict, business: dict, index: int) -> str:
    stype = section.get("type", "content")
    dispatch = {
        "problem":     _section_problem,
        "solution":    _section_solution,
        "benefits":    lambda s, b, i: _section_benefits(s),
        "proof":       lambda s, b, i: _section_proof(s, b),
        "services":    lambda s, b, i: _section_services(s, b),
        "process":     lambda s, b, i: _section_process(s),
        "before_after":lambda s, b, i: _section_before_after(s, b),
        "faq":         lambda s, b, i: _section_faq(s),
        "cta":         lambda s, b, i: _section_cta(s),
    }
    fn = dispatch.get(stype, _section_content_visual)
    return fn(section, business, index)


def _img_tag(
    src: str,
    alt: str,
    css_class: str = "",
    w: int = 800,
    h: int = 600,
    loading: str = "lazy",
    fetchpriority: str = "",
) -> str:
    """Return a standards-compliant <img> tag with all CWV-required attributes."""
    fp_attr = f' fetchpriority="{fetchpriority}"' if fetchpriority else ""
    cls_attr = f' class="{css_class}"' if css_class else ""
    return (
        f'<img src="{src}" alt="{_esc(alt)}"{cls_attr}'
        f' width="{w}" height="{h}"'
        f' loading="{loading}"'
        f' decoding="async"'
        f'{fp_attr}>'
    )


def _section_problem(s: dict, biz: dict, idx: int) -> str:
    title   = s.get("title", "The Problem")
    content = s.get("content", "")
    service = biz.get("primary_service", "").lower()
    img_src = _SECTION_IMAGES["problem"].format(w=600, h=400)
    img     = _img_tag(img_src, f"Before {service}", "rounded-2xl shadow-xl img-cover w-full h-80", 600, 400)

    if idx % 2 == 0:
        return f"""
        <section class="py-20 bg-gray-50 fade-in">
            <div class="max-w-6xl mx-auto px-6 grid md:grid-cols-2 gap-12 items-center">
                <div>
                    <div class="text-sm font-semibold text-red-500 uppercase tracking-wider mb-3">The Problem</div>
                    <h2 class="text-3xl md:text-4xl font-bold mb-6">{title}</h2>
                    <p class="text-lg text-gray-600 leading-relaxed">{content}</p>
                </div>
                <div>{img}</div>
            </div>
        </section>"""
    else:
        return f"""
        <section class="py-20 bg-gray-50 fade-in">
            <div class="max-w-6xl mx-auto px-6 grid md:grid-cols-2 gap-12 items-center">
                <div>{img}</div>
                <div>
                    <div class="text-sm font-semibold text-red-500 uppercase tracking-wider mb-3">The Problem</div>
                    <h2 class="text-3xl md:text-4xl font-bold mb-6">{title}</h2>
                    <p class="text-lg text-gray-600 leading-relaxed">{content}</p>
                </div>
            </div>
        </section>"""


def _section_solution(s: dict, biz: dict, idx: int) -> str:
    title   = s.get("title", "Our Solution")
    content = s.get("content", "")
    service = biz.get("primary_service", "").lower()
    img_src = _SECTION_IMAGES["solution"].format(w=600, h=400)
    img     = _img_tag(img_src, service, "rounded-2xl shadow-xl img-cover w-full h-80", 600, 400)

    flip = idx % 2 == 1
    text_block = f"""
                <div>
                    <div class="text-sm font-semibold text-brand-600 uppercase tracking-wider mb-3">The Solution</div>
                    <h2 class="text-3xl md:text-4xl font-bold mb-6">{title}</h2>
                    <p class="text-lg text-gray-600 leading-relaxed">{content}</p>
                    <a href="#quote" class="cta-btn inline-block mt-6 bg-brand-600 text-white font-semibold px-6 py-3 rounded-full hover:bg-brand-700">Get Started</a>
                </div>"""
    img_block = f"<div>{img}</div>"

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
    rating  = business.get("rating", 4.9)
    reviews = business.get("reviews_count", 0)
    years   = business.get("years_active", 0)
    content = s.get("content", "")

    return f"""
    <section class="py-20 bg-brand-900 text-white fade-in">
        <div class="max-w-6xl mx-auto px-6">
            <div class="text-center mb-12">
                <div class="text-sm font-semibold text-amber-400 uppercase tracking-wider mb-3">Proof</div>
                <h2 class="text-3xl md:text-4xl font-bold">Trusted by Homeowners</h2>
            </div>
            <div class="grid md:grid-cols-3 gap-8 text-center mb-10">
                <div class="bg-white\/10 rounded-2xl p-8 backdrop-blur">
                    <div class="text-5xl font-extrabold text-amber-400">{rating}</div>
                    <div class="text-white\/70 mt-2 text-lg">Star Rating</div>
                </div>
                <div class="bg-white\/10 rounded-2xl p-8 backdrop-blur">
                    <div class="text-5xl font-extrabold text-amber-400">{reviews}+</div>
                    <div class="text-white\/70 mt-2 text-lg">5-Star Reviews</div>
                </div>
                <div class="bg-white\/10 rounded-2xl p-8 backdrop-blur">
                    <div class="text-5xl font-extrabold text-amber-400">{years}+</div>
                    <div class="text-white\/70 mt-2 text-lg">Years Experience</div>
                </div>
            </div>
            {f'<p class="text-center text-white\/70 max-w-3xl mx-auto text-lg">{content}</p>' if content else ''}
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

    cols = min(len(steps), 4) if steps else 1
    return f"""
    <section class="py-20 bg-gray-50">
        <div class="max-w-6xl mx-auto px-6">
            <div class="text-center mb-12">
                <div class="text-sm font-semibold text-brand-600 uppercase tracking-wider mb-3">Process</div>
                <h2 class="text-3xl md:text-4xl font-bold">{title}</h2>
            </div>
            <div class="grid md:grid-cols-{cols} gap-8">{step_html}</div>
        </div>
    </section>"""


def _section_before_after(s: dict, business: dict) -> str:
    service  = business.get("primary_service", "lighting").lower()
    before_src = _SECTION_IMAGES["before"].format(w=600, h=400)
    after_src  = _SECTION_IMAGES["after"].format(w=600, h=400)
    before_img = _img_tag(before_src, f"Before {service}", "w-full h-72 img-cover", 600, 400)
    after_img  = _img_tag(after_src,  f"After {service}",  "w-full h-72 img-cover", 600, 400)

    return f"""
    <section class="py-20 fade-in">
        <div class="max-w-6xl mx-auto px-6">
            <div class="text-center mb-12">
                <h2 class="text-3xl md:text-4xl font-bold">See the Difference</h2>
            </div>
            <div class="grid md:grid-cols-2 gap-8">
                <div class="relative rounded-2xl overflow-hidden shadow-lg">
                    {before_img}
                    <div class="absolute top-4 left-4 bg-red-500 text-white px-4 py-1 rounded-full text-sm font-bold">BEFORE</div>
                </div>
                <div class="relative rounded-2xl overflow-hidden shadow-lg">
                    {after_img}
                    <div class="absolute top-4 left-4 bg-green-500 text-white px-4 py-1 rounded-full text-sm font-bold">AFTER</div>
                </div>
            </div>
        </div>
    </section>"""


def _section_faq(s: dict) -> str:
    items = s.get("items", [])

    faqs_html      = ""
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
                    <span class="text-brand-600 text-2xl group-open:rotate-45 transition-all">+</span>
                </summary>
                <div class="px-5 pb-5 text-gray-600 leading-relaxed">{a}</div>
            </details>"""
        schema_entities.append({
            "@type": "Question", "name": q,
            "acceptedAnswer": {"@type": "Answer", "text": a},
        })

    schema_block = json.dumps({
        "@context": "https://schema.org",
        "@type":    "FAQPage",
        "mainEntity": schema_entities,
    })

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
    cta   = s.get("cta", "Get Your Free Quote")
    return f"""
    <section id="quote" class="py-20 bg-gradient-to-r from-brand-800 to-brand-900 text-white fade-in">
        <div class="max-w-4xl mx-auto px-6 text-center">
            <h2 class="text-3xl md:text-4xl font-bold mb-6">{title}</h2>
            <a href="#" class="cta-btn inline-block bg-amber-400 text-gray-900 font-bold px-10 py-5 rounded-full text-xl">{cta}</a>
        </div>
    </section>"""


def _section_content_visual(s: dict, business: dict, index: int) -> str:
    title   = s.get("title", "")
    content = s.get("content", "")
    service = business.get("primary_service", "").lower()
    city    = business.get("primary_city", "")
    img_src = _unsplash(s.get("image_query", f"{service} residential installation {city}"), 600, 400)
    img     = _img_tag(img_src, f"{service} in {city}", "rounded-2xl shadow-xl img-cover w-full h-80", 600, 400)

    if index % 2 == 0:
        return f"""
        <section class="py-20 fade-in">
            <div class="max-w-6xl mx-auto px-6 grid md:grid-cols-2 gap-12 items-center">
                <div>
                    {f'<h2 class="text-3xl md:text-4xl font-bold mb-6">{title}</h2>' if title else ''}
                    <div class="text-gray-600 leading-relaxed text-lg space-y-4">{content}</div>
                </div>
                <div>{img}</div>
            </div>
        </section>"""
    else:
        return f"""
        <section class="py-20 bg-gray-50 fade-in">
            <div class="max-w-6xl mx-auto px-6 grid md:grid-cols-2 gap-12 items-center">
                <div>{img}</div>
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
            <p class="text-xl text-white\/70 mb-8">Get a free, no-obligation quote from {biz_name}</p>
            <a href="#" class="cta-btn inline-block bg-amber-400 text-gray-900 font-extrabold px-12 py-5 rounded-full text-xl">
                Get Your Free Quote
            </a>
        </div>
    </section>"""


def _render_footer(business: dict, today_disp: str = "") -> str:
    name      = business.get("business_name", "")
    city      = business.get("primary_city", "")
    areas     = business.get("service_areas", [])
    areas_str = ", ".join(areas) if areas else city

    last_updated_line = ""
    if today_disp:
        last_updated_line = f'<p class="text-gray-500 text-sm mt-2">Last updated: {today_disp}</p>'

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
                {last_updated_line}
            </div>
        </div>
    </footer>"""
