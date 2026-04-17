"""
CTA optimizer: AI-powered call-to-action injection and A/B variant generation.
Analyzes page content and inserts high-converting CTAs tuned to service + intent.
"""
from __future__ import annotations
import hashlib
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone

log = logging.getLogger(__name__)
_DB = "data/storage/seo_engine.db"

# Intent signals mapped to CTA urgency level
_URGENCY_SIGNALS = [
    "emergency", "urgent", "asap", "today", "tonight", "broken",
    "leak", "flood", "no heat", "no power", "dangerous",
]

_CTA_TEMPLATES = {
    "high_urgency": {
        "headline": "Need Help Right Now?",
        "body": "Our team is available 24/7 for emergency calls. Get a certified technician on-site fast.",
        "button": "Call Now — Available 24/7",
        "button_type": "tel",
        "badge": "Emergency Service Available",
    },
    "quote_intent": {
        "headline": "Get Your Free Estimate Today",
        "body": "No obligation quote from a licensed local professional. Most estimates in under 24 hours.",
        "button": "Request Free Quote",
        "button_type": "form",
        "badge": "Free, No-Obligation Estimate",
    },
    "info_intent": {
        "headline": "Talk to a Local Expert",
        "body": "Have questions? Our certified team is happy to help — no sales pressure.",
        "button": "Get Expert Advice",
        "button_type": "form",
        "badge": "Licensed & Insured",
    },
    "comparison_intent": {
        "headline": "See Why {location} Homeowners Choose Us",
        "body": "Compare our pricing, reviews, and certifications. Satisfaction guaranteed.",
        "button": "Get a Competing Quote",
        "button_type": "form",
        "badge": "Beat Any Written Quote",
    },
}


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS cta_variants (
            id          TEXT PRIMARY KEY,
            business_id TEXT NOT NULL,
            page_id     TEXT DEFAULT '',
            intent      TEXT DEFAULT 'info_intent',
            variant     TEXT NOT NULL,
            headline    TEXT NOT NULL,
            body_text   TEXT NOT NULL,
            button_text TEXT NOT NULL,
            button_type TEXT DEFAULT 'form',
            badge       TEXT DEFAULT '',
            impressions INTEGER DEFAULT 0,
            clicks      INTEGER DEFAULT 0,
            conversions INTEGER DEFAULT 0,
            active      INTEGER DEFAULT 1,
            created_at  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_cta_biz ON cta_variants(business_id, page_id);
    """)
    c.commit()
    return c


def detect_intent(html: str, title: str = "") -> str:
    text = (html + " " + title).lower()
    if any(s in text for s in _URGENCY_SIGNALS):
        return "high_urgency"
    if any(s in text for s in ["cost", "price", "how much", "quote", "estimate", "fee"]):
        return "quote_intent"
    if any(s in text for s in ["vs", "versus", "compare", "difference", "best", "alternative"]):
        return "comparison_intent"
    return "info_intent"


def generate_cta_variants(
    business_id: str,
    page_id: str,
    service: str,
    location: str,
    intent: str = "",
    business_name: str = "",
    phone: str = "",
) -> list[dict]:
    """Generate 2-3 CTA variants using Claude, fall back to templates."""
    if not intent:
        intent = "info_intent"

    prompt = f"""Generate 3 high-converting call-to-action variants for a {service} business in {location}.
Business: {business_name or service + ' company'}
Intent: {intent.replace('_', ' ')}
Phone: {phone or '(250) 555-0100'}

Return JSON array, each object:
{{
  "variant": "A" | "B" | "C",
  "headline": "...",
  "body_text": "...(1-2 sentences, specific, no fluff)",
  "button_text": "...(action verb, max 6 words)",
  "button_type": "tel" | "form",
  "badge": "...(trust signal, max 5 words)"
}}

Rules:
- Use specific local references ({location})
- Include at least one number or stat if possible
- button_type "tel" for urgency/emergency, "form" for others
- No generic phrases like "contact us today"
- JSON array only"""

    variants = []
    try:
        from core.claude import call_claude
        raw = call_claude(prompt, max_tokens=800)
        m = re.search(r'\[[\s\S]*\]', raw)
        if m:
            items = json.loads(m.group())
            for item in items:
                if not isinstance(item, dict) or not item.get("headline"):
                    continue
                variants.append(item)
    except Exception:
        log.warning("generate_cta_variants: Claude failed, using templates  biz=%s", business_id)

    # Fall back to template if Claude failed
    if not variants:
        tmpl = _CTA_TEMPLATES.get(intent, _CTA_TEMPLATES["info_intent"])
        variants = [{
            "variant": "A",
            "headline": tmpl["headline"].replace("{location}", location),
            "body_text": tmpl["body"],
            "button_text": tmpl["button"],
            "button_type": tmpl["button_type"],
            "badge": tmpl["badge"],
        }]

    now = datetime.now(timezone.utc).isoformat()
    saved = []
    with _conn() as c:
        for v in variants:
            vid = hashlib.md5(
                f"{business_id}:{page_id}:{v.get('variant','A')}".encode()
            ).hexdigest()[:12]
            c.execute("""
                INSERT INTO cta_variants
                    (id, business_id, page_id, intent, variant, headline, body_text,
                     button_text, button_type, badge, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    headline=excluded.headline, body_text=excluded.body_text,
                    button_text=excluded.button_text, button_type=excluded.button_type,
                    badge=excluded.badge
            """, [vid, business_id, page_id, intent,
                  v.get("variant", "A"), v.get("headline", ""),
                  v.get("body_text", ""), v.get("button_text", "Get in Touch"),
                  v.get("button_type", "form"), v.get("badge", ""), now])
            saved.append({"id": vid, **v})

    return saved


def inject_cta(
    html: str,
    business_id: str,
    service: str,
    location: str,
    phone: str = "",
    variant: dict | None = None,
) -> str:
    """Inject a CTA block into HTML. Picks best variant or uses provided one."""
    if not variant:
        # Pick highest-converting active variant
        with _conn() as c:
            row = c.execute("""
                SELECT * FROM cta_variants WHERE business_id=? AND active=1
                ORDER BY (CAST(conversions AS REAL) / MAX(impressions, 1)) DESC
                LIMIT 1
            """, [business_id]).fetchone()
            variant = dict(row) if row else None

    if not variant:
        # Ultimate fallback
        intent = detect_intent(html)
        variants = generate_cta_variants(business_id, "", service, location, intent, phone=phone)
        variant = variants[0] if variants else {}

    if not variant:
        return html

    phone_clean = re.sub(r'[^0-9+]', '', phone) if phone else ""
    btn_type = variant.get("button_type", "form")
    btn_text = variant.get("button_text", "Get in Touch")
    badge = variant.get("badge", "")
    vid = variant.get("id", "")

    if btn_type == "tel" and phone_clean:
        btn_html = f'<a href="tel:{phone_clean}" class="cta-btn cta-btn--tel" data-cta-id="{vid}">{btn_text}</a>'
    else:
        btn_html = f'<a href="#contact" class="cta-btn cta-btn--form" data-cta-id="{vid}">{btn_text}</a>'

    badge_html = f'<span class="cta-badge">{badge}</span>' if badge else ""

    cta_block = f"""
<section class="cta-block" data-cta-id="{vid}" data-business="{business_id}">
  <div class="cta-inner">
    {badge_html}
    <h2 class="cta-headline">{variant.get("headline", "Get in Touch")}</h2>
    <p class="cta-body">{variant.get("body_text", "")}</p>
    {btn_html}
  </div>
</section>
<style>
.cta-block{{background:linear-gradient(135deg,#1a3c5e,#2563eb);color:#fff;padding:2.5rem 1.5rem;border-radius:12px;margin:2rem 0;text-align:center}}
.cta-inner{{max-width:600px;margin:auto}}
.cta-badge{{display:inline-block;background:rgba(255,255,255,.15);border:1px solid rgba(255,255,255,.3);color:#fff;font-size:.8rem;font-weight:600;padding:.25rem .75rem;border-radius:50px;margin-bottom:1rem;letter-spacing:.05em;text-transform:uppercase}}
.cta-headline{{font-size:1.75rem;font-weight:700;margin:.5rem 0 1rem;line-height:1.25}}
.cta-body{{opacity:.9;margin-bottom:1.5rem;font-size:1.05rem}}
.cta-btn{{display:inline-block;background:#f97316;color:#fff;font-weight:700;font-size:1.1rem;padding:.85rem 2rem;border-radius:8px;text-decoration:none;transition:background .2s}}
.cta-btn:hover{{background:#ea6c0a}}
</style>"""

    # Insert after first H2 or H1, or before </main>, or before </body>
    for pattern, replacement in [
        (r'(</h[12][^>]*>)', r'\1' + cta_block),
        ("</main>", cta_block + "\n</main>"),
        ("</article>", cta_block + "\n</article>"),
        ("</body>", cta_block + "\n</body>"),
    ]:
        new_html = re.sub(pattern, replacement, html, count=1) if r'\1' in str(replacement) else html.replace(pattern, replacement, 1)
        if new_html != html:
            # Track impression
            if vid:
                _track_impression(vid)
            return new_html

    return html + cta_block


def _track_impression(variant_id: str) -> None:
    try:
        with _conn() as c:
            c.execute("UPDATE cta_variants SET impressions=impressions+1 WHERE id=?", [variant_id])
    except Exception:
        pass


def track_cta_click(variant_id: str) -> None:
    with _conn() as c:
        c.execute("UPDATE cta_variants SET clicks=clicks+1 WHERE id=?", [variant_id])


def track_cta_conversion(variant_id: str) -> None:
    with _conn() as c:
        c.execute("UPDATE cta_variants SET conversions=conversions+1 WHERE id=?", [variant_id])


def get_cta_performance(business_id: str) -> list[dict]:
    with _conn() as c:
        rows = c.execute("""
            SELECT *, ROUND(CAST(clicks AS REAL) / MAX(impressions,1) * 100, 2) as ctr,
                   ROUND(CAST(conversions AS REAL) / MAX(clicks,1) * 100, 2) as cvr
            FROM cta_variants WHERE business_id=? AND active=1
            ORDER BY cvr DESC, ctr DESC
        """, [business_id]).fetchall()
    return [dict(r) for r in rows]


def auto_optimize_cta(business_id: str) -> dict:
    """Pause low-performing variants, keep the winner."""
    perf = get_cta_performance(business_id)
    if len(perf) < 2:
        return {"action": "insufficient_data", "variants": len(perf)}

    # Need at least 50 impressions to make a call
    eligible = [v for v in perf if v["impressions"] >= 50]
    if not eligible:
        return {"action": "insufficient_impressions"}

    winner = eligible[0]
    losers = [v for v in eligible[1:] if v["ctr"] < winner["ctr"] * 0.7]

    paused = []
    with _conn() as c:
        for v in losers:
            c.execute("UPDATE cta_variants SET active=0 WHERE id=?", [v["id"]])
            paused.append(v["id"])

    log.info("auto_optimize_cta  biz=%s  winner=%s  paused=%d", business_id, winner["id"], len(paused))
    return {"action": "optimized", "winner": winner["id"], "paused": paused}
