"""Conversion Intelligence Layer — ConversionInjector.

Post-processes generated content HTML to inject conversion elements:

  - CTA blocks (call-to-action)
  - Click-to-call buttons with phone number
  - Trust signals (reviews, guarantees, certifications)
  - Lead capture form embeds
  - Google Analytics 4 event tracking snippets
  - Intent-appropriate urgency signals

Usage
-----
    from aic.conversion_injector import ConversionInjector

    injector = ConversionInjector()
    enriched_html = injector.inject(
        content_html=raw_html,
        keyword="emergency plumber NYC",
        intent="transactional",
        business={
            "name": "Example Plumbing NYC",
            "phone": "+1-778-363-6289",
            "email": "info@example.com",
            "reviews_avg": 4.9,
            "reviews_count": 312,
            "guarantee": "100% Satisfaction Guarantee",
        },
    )
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Intent → CTA strategy mapping
# ---------------------------------------------------------------------------

_CTA_STRATEGY = {
    "transactional": {
        "primary_cta":    "Call Now — Available 24/7",
        "secondary_cta":  "Get a Free Quote",
        "urgency":        True,
        "trust_signals":  True,
        "form":           True,
        "cta_frequency":  "high",   # inject at top + mid + bottom
    },
    "commercial": {
        "primary_cta":    "Get a Free Estimate Today",
        "secondary_cta":  "Compare Our Prices",
        "urgency":        False,
        "trust_signals":  True,
        "form":           True,
        "cta_frequency":  "medium", # inject at mid + bottom
    },
    "informational": {
        "primary_cta":    "Talk to an Expert — Free Consultation",
        "secondary_cta":  "See Our Services",
        "urgency":        False,
        "trust_signals":  False,
        "form":           False,
        "cta_frequency":  "low",    # inject at bottom only
    },
    "navigational": {
        "primary_cta":    "Contact Us Today",
        "secondary_cta":  "View All Services",
        "urgency":        False,
        "trust_signals":  True,
        "form":           False,
        "cta_frequency":  "low",
    },
}


class ConversionInjector:
    """Injects conversion-optimised elements into content HTML."""

    def inject(
        self,
        content_html: str,
        keyword: str,
        intent: str,
        business: dict,
        *,
        ga4_measurement_id: str = "",
    ) -> str:
        """Inject conversion elements into content HTML.

        Args:
            content_html:       Raw HTML body from ContentHandler.
            keyword:            Target keyword (used for CTA personalisation).
            intent:             "transactional" | "commercial" | "informational" | "navigational"
            business:           Dict with name, phone, email, reviews_avg, reviews_count,
                                guarantee, city, service.
            ga4_measurement_id: GA4 Measurement ID for event tracking.

        Returns:
            Enriched HTML string.
        """
        strategy = _CTA_STRATEGY.get(intent, _CTA_STRATEGY["informational"])
        name     = business.get("name", "")
        # Load phone from nap sub-object if not set directly
        _nap     = business.get("nap", {})
        phone    = business.get("phone", "") or _nap.get("phone_display", "") or _nap.get("phone", "")
        email    = business.get("email", "")
        city     = business.get("city", "")
        service  = business.get("service", keyword)
        reviews_avg   = business.get("reviews_avg", 0)
        reviews_count = business.get("reviews_count", 0)
        guarantee     = business.get("guarantee", "")

        # Build reusable HTML blocks
        phone_btn  = _phone_button(phone, strategy["primary_cta"]) if phone else ""
        trust_box  = _trust_box(name, reviews_avg, reviews_count, guarantee) if strategy["trust_signals"] else ""
        form_block = _lead_form(name, email, keyword, city) if strategy["form"] and email else ""
        hero_cta   = _hero_cta_block(
            primary_cta=strategy["primary_cta"],
            secondary_cta=strategy["secondary_cta"],
            phone=phone,
            urgency=strategy["urgency"],
            service=service,
            city=city,
        )
        footer_cta = _footer_cta_block(
            primary_cta=strategy["primary_cta"],
            phone=phone,
            email=email,
            form_block=form_block,
        )
        ga4_snippet = _ga4_tracking(ga4_measurement_id or os.getenv("GA4_MEASUREMENT_ID", "")) if (ga4_measurement_id or os.getenv("GA4_MEASUREMENT_ID", "")) else ""

        # Injection logic based on frequency
        freq = strategy["cta_frequency"]

        if freq == "high":
            # Inject: before first <h2>, after middle <p>, at end
            html = _inject_after_intro(content_html, hero_cta)
            html = _inject_at_midpoint(html, trust_box)
            html = html + footer_cta
        elif freq == "medium":
            # Inject: after middle section, at end
            html = _inject_at_midpoint(content_html, trust_box + phone_btn)
            html = html + footer_cta
        else:
            # Low: only at end
            html = content_html + footer_cta

        if ga4_snippet:
            html = ga4_snippet + html

        log.info(
            "conversion_injector.done  keyword=%s  intent=%s  freq=%s",
            keyword, intent, freq,
        )
        return html

    def classify_intent(self, keyword: str, city: str = "") -> str:
        """Heuristic intent classification for a keyword.

        Returns: "transactional" | "commercial" | "informational" | "navigational"

        Priority: informational signals (how to/what is) override action words
        because "how to fix X" is informational, not transactional.
        """
        kw = keyword.lower()
        city_lower = city.lower()

        # Informational first — these override action words like "fix"
        if any(w in kw for w in ["what is", "how to", "why does", "why is", "guide", "tips",
                                  "learn", "explained", "tutorial", "definition", "vs", "difference"]):
            return "informational"
        # Commercial (research/comparison)
        if any(w in kw for w in ["cost", "price", "how much", "fee", "quote", "estimate",
                                  "best", "top", "review", "compare"]):
            return "commercial"
        # Transactional (ready to act)
        if any(w in kw for w in ["emergency", "near me", "call", "book", "hire", "fix now",
                                  "same day", "24/7", "urgent"]):
            return "transactional"
        if city_lower and city_lower in kw:
            return "transactional"
        return "informational"


# ---------------------------------------------------------------------------
# HTML building blocks
# ---------------------------------------------------------------------------

def _phone_button(phone: str, label: str) -> str:
    clean = re.sub(r'[^\d+]', '', phone)
    return f"""
<div class="seo-cta-phone" style="text-align:center;margin:24px 0;">
  <a href="tel:{clean}" onclick="gtag && gtag('event','phone_click',{{event_category:'CTA',event_label:'{label}'}});"
     style="display:inline-block;background:#e53e3e;color:#fff;padding:16px 32px;
            border-radius:8px;font-size:1.2rem;font-weight:700;text-decoration:none;">
    📞 {label} — {phone}
  </a>
</div>"""


def _trust_box(name: str, avg: float, count: int, guarantee: str) -> str:
    stars = "⭐" * min(5, round(avg)) if avg else ""
    review_line = f'<p>{stars} <strong>{avg}/5</strong> from {count:,} verified reviews</p>' if count else ""
    guarantee_line = f'<p>✓ {guarantee}</p>' if guarantee else ""
    if not review_line and not guarantee_line:
        return ""
    return f"""
<div class="seo-trust-box" style="background:#f7fafc;border:1px solid #e2e8f0;
     border-radius:8px;padding:20px;margin:24px 0;text-align:center;">
  <h3 style="margin:0 0 8px;font-size:1.1rem;">Why Choose {name}?</h3>
  {review_line}
  {guarantee_line}
  <p>✓ Licensed &amp; Insured &nbsp;|&nbsp; ✓ Same-Day Service &nbsp;|&nbsp; ✓ Upfront Pricing</p>
</div>"""


def _lead_form(name: str, email: str, keyword: str, city: str) -> str:
    return f"""
<div class="seo-lead-form" style="background:#ebf8ff;border:1px solid #bee3f8;
     border-radius:8px;padding:24px;margin:24px 0;">
  <h3 style="margin:0 0 12px;">Get a Free Quote for {keyword.title()} in {city}</h3>
  <form method="POST" action="/contact"
        onsubmit="gtag && gtag('event','form_submit',{{event_category:'Lead',event_label:'{keyword}'}});">
    <input type="hidden" name="keyword" value="{keyword}">
    <div style="margin-bottom:12px;">
      <input type="text" name="name" placeholder="Your Name" required
             style="width:100%;padding:10px;border:1px solid #ccc;border-radius:4px;">
    </div>
    <div style="margin-bottom:12px;">
      <input type="tel" name="phone" placeholder="Phone Number" required
             style="width:100%;padding:10px;border:1px solid #ccc;border-radius:4px;">
    </div>
    <div style="margin-bottom:12px;">
      <textarea name="message" placeholder="Describe your issue..." rows="3"
                style="width:100%;padding:10px;border:1px solid #ccc;border-radius:4px;"></textarea>
    </div>
    <button type="submit"
            style="background:#3182ce;color:#fff;padding:12px 24px;
                   border:none;border-radius:6px;font-size:1rem;font-weight:600;cursor:pointer;width:100%;">
      Send My Free Quote Request
    </button>
  </form>
</div>"""


def _hero_cta_block(
    primary_cta: str,
    secondary_cta: str,
    phone: str,
    urgency: bool,
    service: str,
    city: str,
) -> str:
    urgency_banner = (
        '<div style="background:#fed7d7;color:#c53030;padding:10px;text-align:center;'
        'font-weight:700;border-radius:4px;margin-bottom:12px;">'
        '⚡ EMERGENCY RESPONSE — Technicians Available Now</div>'
    ) if urgency else ""

    clean_phone = re.sub(r'[^\d+]', '', phone) if phone else ""
    phone_btn = (
        f'<a href="tel:{clean_phone}" style="display:inline-block;background:#e53e3e;'
        f'color:#fff;padding:14px 28px;border-radius:6px;font-weight:700;'
        f'text-decoration:none;margin-right:12px;">📞 {primary_cta}</a>'
    ) if phone else ""

    return f"""
<div class="seo-hero-cta" style="background:#f0fff4;border:2px solid #68d391;
     border-radius:10px;padding:24px;margin:0 0 28px;">
  {urgency_banner}
  <h2 style="margin:0 0 8px;font-size:1.3rem;">{service.title()} in {city} — Get Help Today</h2>
  <div style="margin-top:16px;">
    {phone_btn}
  </div>
</div>"""


def _footer_cta_block(
    primary_cta: str,
    phone: str,
    email: str,
    form_block: str,
) -> str:
    clean_phone = re.sub(r'[^\d+]', '', phone) if phone else ""
    contact_line = ""
    if phone:
        contact_line += f'<a href="tel:{clean_phone}" style="color:#3182ce;font-weight:700;">📞 {phone}</a>'
    if email:
        sep = " &nbsp;|&nbsp; " if phone else ""
        contact_line += f'{sep}<a href="mailto:{email}" style="color:#3182ce;">✉ {email}</a>'

    return f"""
<div class="seo-footer-cta" style="background:#2d3748;color:#fff;
     border-radius:10px;padding:32px;margin:32px 0;text-align:center;">
  <h2 style="margin:0 0 8px;font-size:1.4rem;">Ready to Get Started?</h2>
  <p style="margin:0 0 16px;opacity:0.85;">{primary_cta}</p>
  <p style="margin:0 0 20px;">{contact_line}</p>
  {form_block}
</div>"""


def _ga4_tracking(measurement_id: str) -> str:
    if not measurement_id or not measurement_id.startswith("G-"):
        return ""
    return f"""<!-- GA4 + Conversion Tracking -->
<script async src="https://www.googletagmanager.com/gtag/js?id={measurement_id}"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){{dataLayer.push(arguments);}}
  gtag('js', new Date());
  gtag('config', '{measurement_id}', {{
    send_page_view: true,
    allow_enhanced_conversions: true
  }});
  // Track all phone clicks
  document.addEventListener('click', function(e) {{
    var el = e.target.closest('a[href^="tel:"]');
    if (el) {{
      gtag('event', 'phone_call', {{event_category: 'Conversion', event_label: el.href}});
    }}
  }});
</script>"""


# ---------------------------------------------------------------------------
# Content insertion helpers
# ---------------------------------------------------------------------------

def _inject_after_intro(html: str, block: str) -> str:
    """Insert block after the first <h2> tag (= end of intro section)."""
    m = re.search(r'<h2[\s>]', html, re.IGNORECASE)
    if m:
        return html[:m.start()] + block + html[m.start():]
    # Fallback: after first 3 paragraphs
    paras = [m.start() for m in re.finditer(r'</p>', html, re.IGNORECASE)]
    if len(paras) >= 3:
        idx = paras[2] + 4
        return html[:idx] + block + html[idx:]
    return block + html


def _inject_at_midpoint(html: str, block: str) -> str:
    """Insert block at the midpoint of the HTML."""
    mid = len(html) // 2
    # Find the nearest </p> to the midpoint to avoid breaking tags
    search_zone = html[max(0, mid - 200): mid + 200]
    m = re.search(r'</p>', search_zone)
    if m:
        insert_at = max(0, mid - 200) + m.end()
        return html[:insert_at] + block + html[insert_at:]
    return html[:mid] + block + html[mid:]
