"""
E-E-A-T pipeline: orchestrates author injection, trust signals,
FAQ schema, and breadcrumbs in the content post-processing step.
Scores content for E-E-A-T compliance.
"""
from __future__ import annotations
import re
import logging
from typing import Optional

log = logging.getLogger(__name__)


def score_eeat(html: str) -> dict:
    """
    Score HTML content for E-E-A-T signals (0–100).
    Returns dict with component scores and list of missing signals.
    """
    scores: dict[str, int] = {}
    missing: list[str] = []

    # Author bio block (25 pts)
    if 'class="author-bio-block"' in html or 'schema.org/Person' in html:
        scores["author_bio"] = 25
    else:
        scores["author_bio"] = 0
        missing.append("author bio block with schema.org/Person markup")

    # Trust badge / aggregate rating (15 pts)
    if 'AggregateRating' in html or 'class="trust-badge"' in html:
        scores["trust_badge"] = 15
    else:
        scores["trust_badge"] = 0
        missing.append("aggregate rating trust badge")

    # FAQ schema (15 pts)
    if 'FAQPage' in html:
        scores["faq_schema"] = 15
    else:
        scores["faq_schema"] = 0
        missing.append("FAQPage schema markup")

    # Breadcrumb schema (10 pts)
    if 'BreadcrumbList' in html:
        scores["breadcrumb"] = 10
    else:
        scores["breadcrumb"] = 0
        missing.append("BreadcrumbList breadcrumb schema")

    # Review schema (15 pts)
    if '"Review"' in html or 'reviewBody' in html:
        scores["review_schema"] = 15
    else:
        scores["review_schema"] = 0
        missing.append("individual Review schema markup")

    # Last-updated signal (10 pts) — shared with GEO
    if re.search(r'last.updated|dateModified|updated.{0,10}\d{4}', html, re.IGNORECASE):
        scores["last_updated"] = 10
    else:
        scores["last_updated"] = 0
        missing.append("last-updated date signal")

    # Credential / expertise mentions (10 pts)
    if 'author-credentials' in html or re.search(r'\d+\s+years?.{0,20}(experience|serving)', html, re.IGNORECASE):
        scores["credentials"] = 10
    else:
        scores["credentials"] = 0
        missing.append("author credentials or experience statements")

    total = sum(scores.values())
    return {
        "total": total,
        "passing": total >= 55,
        "scores": scores,
        "missing": missing,
    }


def run_eeat_pipeline(
    html: str,
    business_id: str,
    content_url: str = "",
    breadcrumbs: list[dict] | None = None,
    faqs: list[dict] | None = None,
    reviews: list[dict] | None = None,
    review_count: int = 0,
    avg_rating: float = 0.0,
    business_name: str = "",
) -> dict:
    """
    Full E-E-A-T post-processing pipeline.
    Returns enriched HTML + score.
    """
    from core.author_profiles import auto_inject_author
    from core.trust_signals import (
        inject_trust_badge,
        inject_review_schema,
        inject_breadcrumb_schema,
        inject_faq_schema,
    )

    original_score = score_eeat(html)

    # 1. Author bio
    try:
        html = auto_inject_author(html, business_id, content_url)
    except Exception:
        log.exception("eeat_pipeline: author injection failed")

    # 2. Trust badge (aggregate rating)
    if review_count > 0 and avg_rating > 0 and business_name:
        try:
            html = inject_trust_badge(html, review_count, avg_rating, business_name)
        except Exception:
            log.exception("eeat_pipeline: trust badge failed")

    # 3. Review schema (individual reviews)
    if reviews:
        try:
            html = inject_review_schema(html, reviews, business_name or "Business")
        except Exception:
            log.exception("eeat_pipeline: review schema failed")

    # 4. Breadcrumb schema
    if breadcrumbs:
        try:
            html = inject_breadcrumb_schema(html, breadcrumbs)
        except Exception:
            log.exception("eeat_pipeline: breadcrumb injection failed")

    # 5. FAQ schema (auto-extract from H2/H3 questions if not supplied)
    try:
        html = inject_faq_schema(html, faqs or [])
    except Exception:
        log.exception("eeat_pipeline: faq schema failed")

    final_score = score_eeat(html)
    log.info(
        "eeat_pipeline.done  biz=%s  url=%s  before=%d  after=%d",
        business_id, content_url, original_score["total"], final_score["total"]
    )

    return {
        "html": html,
        "score_before": original_score,
        "score_after": final_score,
        "improved": final_score["total"] > original_score["total"],
    }
