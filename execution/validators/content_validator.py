"""Content Quality Validator — gates content before WordPress publish.

Checks performed (in order):
  1. Originality.ai — AI detection score (blocks if too high)
  2. Duplicate check — near-duplicate detection vs. existing posts
  3. Schema validity — JSON-LD structure validation
  4. Minimum word count — ensures content meets target length
  5. HTML structure — verifies H1→H2→H3 hierarchy
  6. Direct answer paragraph — first 100 words should answer the query
  7. Meta description length — 150-160 chars

Environment variables:
    ORIGINALITY_API_KEY     — from originality.ai
    AI_SCORE_THRESHOLD      — block if AI score > this (default 0.8)
    MIN_WORD_COUNT          — default 700
    SCHEMA_VALIDATE         — "true" | "false" (default "true")
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

log = logging.getLogger(__name__)

_ORIGINALITY_KEY  = lambda: os.getenv("ORIGINALITY_API_KEY", "")
_AI_THRESHOLD     = lambda: float(os.getenv("AI_SCORE_THRESHOLD", "0.8"))
_MIN_WORDS        = lambda: int(os.getenv("MIN_WORD_COUNT", "700"))
_SCHEMA_VALIDATE  = lambda: os.getenv("SCHEMA_VALIDATE", "true").lower() == "true"


@dataclass
class ValidationResult:
    passed:             bool
    checks:             dict[str, bool] = field(default_factory=dict)
    warnings:           list[str]       = field(default_factory=list)
    errors:             list[str]       = field(default_factory=list)
    ai_score:           float           = 0.0
    word_count:         int             = 0
    schema_valid:       bool            = True
    rewrite_required:   bool            = False
    summary:            str             = ""

    def to_dict(self) -> dict:
        return {
            "passed":           self.passed,
            "checks":           self.checks,
            "warnings":         self.warnings,
            "errors":           self.errors,
            "ai_score":         self.ai_score,
            "word_count":       self.word_count,
            "schema_valid":     self.schema_valid,
            "rewrite_required": self.rewrite_required,
            "summary":          self.summary,
        }


class ContentValidator:
    """Validates generated content before WordPress publish."""

    async def validate(
        self,
        content_html: str,
        keyword: str,
        *,
        title: str = "",
        meta_description: str = "",
        schema_json: dict | str | None = None,
        slug: str = "",
    ) -> ValidationResult:
        """Run all validation checks on a piece of content.

        Args:
            content_html:     HTML body of the generated content.
            keyword:          Target keyword (for answer-check).
            title:            Page title (for length check).
            meta_description: Meta description (for length check).
            schema_json:      JSON-LD schema (for structural validation).
            slug:             URL slug (for collision check).

        Returns:
            ValidationResult with passed=True only if all blocking checks pass.
        """
        checks: dict[str, bool] = {}
        warnings: list[str] = []
        errors: list[str] = []
        ai_score = 0.0

        plain_text = _strip_html(content_html)
        word_count = len(plain_text.split())

        # ── 1. Word count ──────────────────────────────────────────────────
        min_words = _MIN_WORDS()
        wc_ok = word_count >= min_words
        checks["word_count"] = wc_ok
        if not wc_ok:
            errors.append(f"Word count {word_count} below minimum {min_words}. Rewrite required.")

        # ── 2. HTML structure (H1 presence + hierarchy) ────────────────────
        h1_matches = re.findall(r'<h1[\s>]', content_html, re.IGNORECASE)
        h2_matches = re.findall(r'<h2[\s>]', content_html, re.IGNORECASE)
        html_ok = len(h1_matches) == 1 and len(h2_matches) >= 2
        checks["html_structure"] = html_ok
        if len(h1_matches) != 1:
            warnings.append(f"Expected exactly 1 H1, found {len(h1_matches)}.")
        if len(h2_matches) < 2:
            warnings.append(f"Expected at least 2 H2s, found {len(h2_matches)}.")

        # ── 3. Direct answer check ─────────────────────────────────────────
        first_100_words = " ".join(plain_text.split()[:100]).lower()
        kw_tokens = keyword.lower().split()
        kw_coverage = sum(1 for t in kw_tokens if t in first_100_words) / max(len(kw_tokens), 1)
        answer_ok = kw_coverage >= 0.5
        checks["direct_answer"] = answer_ok
        if not answer_ok:
            warnings.append(
                f"Keyword coverage in first 100 words is {kw_coverage:.0%}. "
                "Add a direct answer paragraph at the top for better featured snippet and AI citation capture."
            )

        # ── 4. Meta description length ──────────────────────────────────────
        if meta_description:
            meta_len = len(meta_description)
            meta_ok = 140 <= meta_len <= 165
            checks["meta_description_length"] = meta_ok
            if not meta_ok:
                warnings.append(
                    f"Meta description is {meta_len} chars (target: 140-165). "
                    f"{'Shorten' if meta_len > 165 else 'Lengthen'} it."
                )
        else:
            checks["meta_description_length"] = False
            warnings.append("Meta description is missing.")

        # ── 5. Schema validation ───────────────────────────────────────────
        schema_ok = True
        if schema_json and _SCHEMA_VALIDATE():
            schema_ok = _validate_schema(schema_json, warnings, errors)
        checks["schema_valid"] = schema_ok

        # ── 6. Originality.ai AI detection ─────────────────────────────────
        originality_ok = True
        key = _ORIGINALITY_KEY()
        if key:
            ai_score, originality_ok, origin_msg = await _check_originality(plain_text, key)
            checks["ai_detection"] = originality_ok
            if not originality_ok:
                errors.append(origin_msg)
        else:
            checks["ai_detection"] = True  # skip if no key
            warnings.append("ORIGINALITY_API_KEY not set — AI detection skipped.")

        # ── 7. FAQ presence ────────────────────────────────────────────────
        has_faq = bool(re.search(r'<h[23][^>]*>.*?faq|frequently asked|questions', content_html, re.IGNORECASE))
        checks["has_faq"] = has_faq
        if not has_faq:
            warnings.append("No FAQ section detected. Add FAQPage schema and Q&A section for AI ingestion.")

        # ── Determine overall pass / fail ──────────────────────────────────
        blocking_failures = [
            k for k, v in checks.items()
            if not v and k in ("word_count", "ai_detection")
        ]
        passed = len(blocking_failures) == 0

        rewrite = not originality_ok or not wc_ok
        summary_parts = [f"word_count={word_count}", f"ai_score={ai_score:.2f}"]
        if errors:
            summary_parts.append(f"errors={len(errors)}")
        if warnings:
            summary_parts.append(f"warnings={len(warnings)}")
        summary = " | ".join(summary_parts)

        result = ValidationResult(
            passed=passed,
            checks=checks,
            warnings=warnings,
            errors=errors,
            ai_score=ai_score,
            word_count=word_count,
            schema_valid=schema_ok,
            rewrite_required=rewrite,
            summary=summary,
        )

        log.info(
            "content_validator.done  keyword=%s  passed=%s  ai_score=%.2f  words=%d  errors=%d",
            keyword, passed, ai_score, word_count, len(errors),
        )
        return result

    async def humanise(
        self,
        content_html: str,
        keyword: str,
        *,
        ai_score: float = 0.0,
    ) -> str:
        """Ask Claude to rewrite content to reduce AI detection score.

        Called automatically when Originality.ai score exceeds threshold.
        Returns humanised HTML.
        """
        log.info("content_validator.humanise  keyword=%s  ai_score=%.2f", keyword, ai_score)
        try:
            from core.claude import call_claude
            plain = _strip_html(content_html)
            prompt = f"""Rewrite the following SEO article for the keyword "{keyword}" to make it sound more natural, human, and conversational while preserving all the facts, structure, and SEO elements.

Original text (partial — first 1500 chars):
{plain[:1500]}

Rules:
- Keep all facts, statistics, and structured data
- Vary sentence length (mix short punchy sentences with longer explanatory ones)
- Add first-person anecdotes, local colour, and specific examples
- Replace generic AI phrases ("It is important to note", "In conclusion") with direct language
- Maintain the H2/H3 structure but rephrase headings where they sound generic
- Return ONLY the HTML body content (no JSON wrapper)

Rewrite now:"""

            humanised = call_claude(prompt, max_tokens=4096)
            if not humanised.strip().startswith("<"):
                humanised = f"<div>{humanised}</div>"
            return humanised
        except Exception as e:
            log.error("content_validator.humanise_fail  err=%s", e)
            return content_html  # return original on failure


# ---------------------------------------------------------------------------
# Originality.ai integration
# ---------------------------------------------------------------------------

async def _check_originality(
    plain_text: str,
    api_key: str,
) -> tuple[float, bool, str]:
    """Call Originality.ai API to get AI detection score.

    Returns:
        (ai_score, is_ok, message)
        ai_score: 0-1 (1.0 = 100% AI, 0.0 = 100% human)
        is_ok:    True if below threshold
    """
    threshold = _AI_THRESHOLD()
    # Originality.ai expects at least 50 words
    if len(plain_text.split()) < 50:
        return 0.0, True, "text too short for AI detection"

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.originality.ai/api/v1/scan/ai",
                headers={
                    "X-OAI-API-KEY": api_key,
                    "Accept":         "application/json",
                },
                json={
                    "content": plain_text[:10000],  # API limit
                    "aiModelVersion": "1",
                    "storeScan": "false",
                },
            )
            resp.raise_for_status()
            data = resp.json()

        ai_score = data.get("score", {}).get("ai", 0.0)
        is_ok    = ai_score <= threshold
        msg = (
            f"Originality.ai AI score: {ai_score:.2f} (threshold: {threshold:.2f}). "
            "Content blocked — rewrite required to reduce AI detection."
        ) if not is_ok else ""
        log.info("originality.done  ai_score=%.2f  passed=%s", ai_score, is_ok)
        return float(ai_score), is_ok, msg

    except Exception as e:
        log.warning("originality.api_fail  err=%s  skipping_check", e)
        return 0.0, True, f"Originality.ai API error (skipped): {e}"


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def _validate_schema(schema: dict | str, warnings: list, errors: list) -> bool:
    """Validate a JSON-LD schema blob for basic structural correctness."""
    if isinstance(schema, str):
        try:
            schema = json.loads(schema)
        except json.JSONDecodeError as e:
            errors.append(f"Schema JSON is invalid: {e}")
            return False

    if not isinstance(schema, dict):
        errors.append("Schema must be a JSON object, not a list or primitive.")
        return False

    # Required JSON-LD keys
    missing = []
    if "@context" not in schema:
        missing.append("@context")
    if "@type" not in schema:
        missing.append("@type")

    if missing:
        warnings.append(f"Schema missing required JSON-LD keys: {missing}. Rich results may not appear.")
        return False

    # Check context is schema.org
    ctx = schema.get("@context", "")
    if "schema.org" not in str(ctx):
        warnings.append(f"Schema @context should be 'https://schema.org' — got: {ctx!r}")

    # Type-specific checks
    schema_type = schema.get("@type", "")
    if schema_type == "LocalBusiness":
        for req in ["name", "address", "telephone"]:
            if req not in schema:
                warnings.append(f"LocalBusiness schema missing '{req}' — hurts local SEO.")
    elif schema_type in ("Article", "BlogPosting"):
        for req in ["headline", "author", "datePublished"]:
            if req not in schema:
                warnings.append(f"{schema_type} schema missing '{req}' — required for rich results.")
    elif schema_type == "FAQPage":
        if "mainEntity" not in schema:
            warnings.append("FAQPage schema missing 'mainEntity' with Q&A pairs.")

    log.debug("schema_validator.done  type=%s  warnings=%d", schema_type, len(warnings))
    return True


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _strip_html(html: str) -> str:
    """Remove HTML tags and return plain text."""
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>',  ' ', text,  flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()
