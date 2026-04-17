"""Content output validators for the SEO engine.

validate_content_output() is the primary E-E-A-T gate called by generate_content.
"""

from __future__ import annotations
import re
import logging

log = logging.getLogger(__name__)


def validate_content_output(html: str) -> tuple[bool, list[str]]:
    """Validate generated content contains all required E-E-A-T elements.

    Args:
        html: The content_html string from generate_content output.

    Returns:
        (passed: bool, missing: list[str]) — empty list means all checks passed.
    """
    checks: dict[str, bool] = {
        # Quick Answer / featured snippet block
        "quick_answer": (
            "Quick Answer" in html
            or "background:#f0fdf4" in html
            or 'class="quick-answer"' in html
        ),
        # CTA with real phone number (not placeholder)
        "cta_with_phone": (
            "linear-gradient" in html and "tel:+" in html
        ) or "tel:+1778" in html or "tel:+17783636289" in html,
        # FAQ section
        "faq_section": (
            ("FAQ" in html or "Frequently Asked" in html)
            and html.lower().count("<h2") >= 1
        ),
        # Minimum word count (rough estimate via split)
        "word_count_900": len(html.split()) >= 900,
        # Author / company credibility signal
        "author_or_company": (
            "author-bio" in html
            or "company-footer" in html
            or "Blend Bright Lights" in html
            or "Licensed" in html
        ),
        # At least 2 H2 headings
        "h2_minimum": html.lower().count("<h2") >= 2,
    }

    missing = [k for k, v in checks.items() if not v]
    passed = len(missing) == 0

    if missing:
        log.debug("validate_content_output.fail  missing=%s", missing)

    return passed, missing


def validate_schema_present(html: str) -> tuple[bool, list[str]]:
    """Check that required JSON-LD schema types are present."""
    checks: dict[str, bool] = {
        "Article_or_LocalBusiness": '"@type"' in html and (
            '"Article"' in html or '"LocalBusiness"' in html
        ),
        "FAQPage_schema": '"FAQPage"' in html,
    }
    missing = [k for k, v in checks.items() if not v]
    return len(missing) == 0, missing


def validate_no_placeholder_data(html: str) -> tuple[bool, list[str]]:
    """Detect placeholder data that should never ship."""
    issues = []
    if "555-0100" in html or "555-0" in html:
        issues.append("placeholder_phone_555")
    if "Lorem ipsum" in html:
        issues.append("lorem_ipsum")
    if "[PLACEHOLDER]" in html or "[YOUR " in html.upper():
        issues.append("template_placeholders")
    return len(issues) == 0, issues
