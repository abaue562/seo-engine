"""Google PageSpeed Insights connector — fetches Core Web Vitals data per URL.

Provides:
- LCP, CLS, INP, FCP, TTFB metrics (field data from CrUX)
- Lab-based Lighthouse scores (Performance, Accessibility, Best Practices, SEO)
- Specific opportunities and diagnostics from Lighthouse audit
- CWV pass/fail verdict per URL

Environment variables:
    GOOGLE_PAGESPEED_API_KEY  — from Google Cloud Console (optional but recommended)
                                 Without a key: 25K requests/day free
                                 With a key:    quota depends on billing tier

Usage:
    connector = PageSpeedConnector()
    result = await connector.analyze("https://example.com/page")
    tasks = connector.to_remediation_tasks(result)
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

log = logging.getLogger(__name__)

_API_BASE  = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
_CWV_DIR   = Path("data/storage/cwv_history")


@dataclass
class CWVMetric:
    name:         str
    value:        float       # actual measured value (ms or score 0-1)
    unit:         str         # "ms" | "cls" | "score"
    rating:       str         # "FAST" | "AVERAGE" | "SLOW" | "PASS" | "FAIL"
    percentile:   int = 0     # p75 from CrUX field data


@dataclass
class LighthouseAudit:
    id:           str
    title:        str
    score:        float       # 0-1
    display_value: str = ""
    description:  str = ""
    details_type: str = ""    # "table" | "list" | "opportunity" etc.


@dataclass
class PageSpeedResult:
    url:                str
    strategy:           str           # "mobile" | "desktop"
    performance_score:  float = 0.0   # 0-100
    seo_score:          float = 0.0
    accessibility_score: float = 0.0
    best_practices_score: float = 0.0

    # Core Web Vitals (field data / CrUX p75)
    lcp:    CWVMetric | None = None   # Largest Contentful Paint
    cls:    CWVMetric | None = None   # Cumulative Layout Shift
    inp:    CWVMetric | None = None   # Interaction to Next Paint
    fcp:    CWVMetric | None = None   # First Contentful Paint
    ttfb:   CWVMetric | None = None   # Time to First Byte

    cwv_pass: bool = False            # True if LCP + CLS + INP all FAST/PASS

    # Lighthouse opportunities and failures
    opportunities: list[LighthouseAudit] = field(default_factory=list)
    failed_audits: list[LighthouseAudit] = field(default_factory=list)

    timestamp: str = ""
    error:     str = ""

    def severity(self) -> str:
        """Overall CWV health: GOOD | NEEDS_IMPROVEMENT | POOR"""
        if self.performance_score >= 90 and self.cwv_pass:
            return "GOOD"
        if self.performance_score >= 50:
            return "NEEDS_IMPROVEMENT"
        return "POOR"

    def to_dict(self) -> dict:
        return {
            "url":                  self.url,
            "strategy":             self.strategy,
            "performance_score":    self.performance_score,
            "seo_score":            self.seo_score,
            "accessibility_score":  self.accessibility_score,
            "best_practices_score": self.best_practices_score,
            "cwv_pass":             self.cwv_pass,
            "severity":             self.severity(),
            "lcp":  _metric_dict(self.lcp),
            "cls":  _metric_dict(self.cls),
            "inp":  _metric_dict(self.inp),
            "fcp":  _metric_dict(self.fcp),
            "ttfb": _metric_dict(self.ttfb),
            "opportunities":  [{"id": a.id, "title": a.title, "score": a.score, "details": a.display_value} for a in self.opportunities],
            "failed_audits":  [{"id": a.id, "title": a.title, "score": a.score} for a in self.failed_audits],
            "timestamp":      self.timestamp,
        }


class PageSpeedConnector:
    """Fetches PageSpeed / Core Web Vitals data via Google PageSpeed Insights API."""

    def __init__(self):
        self.api_key = os.getenv("GOOGLE_PAGESPEED_API_KEY", "")

    async def analyze(
        self,
        url: str,
        strategy: str = "mobile",
    ) -> PageSpeedResult:
        """Fetch PageSpeed Insights for a URL.

        Args:
            url:      Full URL to analyze (must be publicly accessible).
            strategy: "mobile" | "desktop"

        Returns:
            PageSpeedResult with CWV metrics + Lighthouse scores + opportunities.
        """
        params: dict[str, str] = {
            "url":      url,
            "strategy": strategy,
            "category": "performance,seo,accessibility,best-practices",
            "locale":   "en",
        }
        if self.api_key:
            params["key"] = self.api_key

        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.get(_API_BASE, params=params)
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            log.error("pagespeed.api_fail  url=%s  err=%s", url, e)
            return PageSpeedResult(url=url, strategy=strategy, error=str(e),
                                   timestamp=_utc_now())

        result = _parse_response(data, url, strategy)
        _save_cwv_history(result)

        log.info(
            "pagespeed.done  url=%s  perf=%s  cwv=%s  severity=%s",
            url, result.performance_score, result.cwv_pass, result.severity(),
        )
        return result

    async def analyze_batch(
        self,
        urls: list[str],
        strategy: str = "mobile",
    ) -> list[PageSpeedResult]:
        """Analyze multiple URLs sequentially (API rate limit: 1 req/s)."""
        import asyncio
        results = []
        for url in urls:
            result = await self.analyze(url, strategy)
            results.append(result)
            await asyncio.sleep(1.1)  # stay under rate limit
        return results

    def to_remediation_tasks(
        self,
        result: PageSpeedResult,
        *,
        business_id: str = "",
    ) -> list[dict]:
        """Convert a PageSpeedResult into SEO task dicts ready for the execution router.

        Returns:
            List of task dicts with action, type, target, why, impact, execution fields.
        """
        tasks = []
        sev = result.severity()

        if sev == "GOOD":
            return tasks  # nothing to fix

        # LCP task
        if result.lcp and result.lcp.rating in ("SLOW", "AVERAGE"):
            tasks.append({
                "action":           "Fix Largest Contentful Paint (LCP)",
                "type":             "WEBSITE",
                "target":           result.url,
                "why":              f"LCP is {result.lcp.value:.0f}ms (rating: {result.lcp.rating}). Google threshold for GOOD is <2500ms. Slow LCP directly reduces rankings.",
                "impact":           "high" if result.lcp.rating == "SLOW" else "medium",
                "impact_score":     9 if result.lcp.rating == "SLOW" else 7,
                "ease_score":       6,
                "speed_score":      7,
                "confidence_score": 9,
                "estimated_result": "LCP improvement to <2.5s within 2 weeks after optimisation",
                "time_to_result":   "14 days",
                "execution":        _lcp_fix_steps(result),
                "business_id":      business_id,
            })

        # CLS task
        if result.cls and result.cls.rating in ("SLOW", "AVERAGE"):
            tasks.append({
                "action":           "Fix Cumulative Layout Shift (CLS)",
                "type":             "WEBSITE",
                "target":           result.url,
                "why":              f"CLS is {result.cls.value:.3f} (rating: {result.cls.rating}). Google threshold for GOOD is <0.1. High CLS hurts UX and rankings.",
                "impact":           "high" if result.cls.rating == "SLOW" else "medium",
                "impact_score":     8,
                "ease_score":       7,
                "speed_score":      8,
                "confidence_score": 9,
                "estimated_result": "CLS below 0.1 within 1 week of layout fixes",
                "time_to_result":   "7 days",
                "execution":        "1. Add explicit width/height to all images. 2. Reserve space for ads/embeds. 3. Avoid inserting content above existing content. 4. Use CSS aspect-ratio for media.",
                "business_id":      business_id,
            })

        # INP task
        if result.inp and result.inp.rating in ("SLOW", "AVERAGE"):
            tasks.append({
                "action":           "Fix Interaction to Next Paint (INP)",
                "type":             "WEBSITE",
                "target":           result.url,
                "why":              f"INP is {result.inp.value:.0f}ms (rating: {result.inp.rating}). Google threshold for GOOD is <200ms. Poor INP indicates heavy JS blocking the main thread.",
                "impact":           "medium",
                "impact_score":     7,
                "ease_score":       5,
                "speed_score":      6,
                "confidence_score": 8,
                "estimated_result": "INP below 200ms after JS optimisation",
                "time_to_result":   "21 days",
                "execution":        "1. Audit JS bundle — identify heavy scripts. 2. Defer non-critical scripts. 3. Remove unused plugins. 4. Use code splitting. 5. Minify and compress JS.",
                "business_id":      business_id,
            })

        # Performance score task
        if result.performance_score < 50:
            opportunity_list = "; ".join(
                a.title for a in result.opportunities[:5]
            ) if result.opportunities else "general performance issues"
            tasks.append({
                "action":           f"Fix Critical Performance Issues (score: {result.performance_score:.0f}/100)",
                "type":             "WEBSITE",
                "target":           result.url,
                "why":              f"PageSpeed score is {result.performance_score:.0f}/100. Top issues: {opportunity_list}",
                "impact":           "high",
                "impact_score":     9,
                "ease_score":       5,
                "speed_score":      7,
                "confidence_score": 9,
                "estimated_result": "Performance score above 70 within 30 days",
                "time_to_result":   "30 days",
                "execution":        _opportunity_steps(result.opportunities),
                "business_id":      business_id,
            })

        return tasks

    def load_history(self, url: str, strategy: str = "mobile") -> list[dict]:
        """Load historical CWV data for a URL."""
        slug = _url_to_slug(url)
        history_file = _CWV_DIR / strategy / f"{slug}.json"
        if not history_file.exists():
            return []
        try:
            import json
            return json.loads(history_file.read_text(encoding="utf-8"))
        except Exception:
            return []


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_response(data: dict, url: str, strategy: str) -> PageSpeedResult:
    cats = data.get("lighthouseResult", {}).get("categories", {})
    perf_score  = cats.get("performance",      {}).get("score", 0) * 100
    seo_score   = cats.get("seo",              {}).get("score", 0) * 100
    a11y_score  = cats.get("accessibility",    {}).get("score", 0) * 100
    bp_score    = cats.get("best-practices",   {}).get("score", 0) * 100

    # CrUX / field data
    crux = data.get("loadingExperience", {}).get("metrics", {})
    lcp  = _parse_crux_metric("LARGEST_CONTENTFUL_PAINT_MS",        crux, "ms",  2500, 4000)
    cls  = _parse_crux_metric("CUMULATIVE_LAYOUT_SHIFT_SCORE",       crux, "cls", 0.1,  0.25)
    inp  = _parse_crux_metric("INTERACTION_TO_NEXT_PAINT",           crux, "ms",  200,  500)
    fcp  = _parse_crux_metric("FIRST_CONTENTFUL_PAINT_MS",           crux, "ms",  1800, 3000)
    ttfb = _parse_crux_metric("EXPERIMENTAL_TIME_TO_FIRST_BYTE",     crux, "ms",  800,  1800)

    cwv_pass = all(
        m is None or m.rating in ("FAST", "PASS")
        for m in [lcp, cls, inp]
    )

    # Lighthouse audits
    audits = data.get("lighthouseResult", {}).get("audits", {})
    opportunities = []
    failed_audits = []
    for audit_id, audit in audits.items():
        score = audit.get("score")
        if score is None:
            continue
        a = LighthouseAudit(
            id=audit_id,
            title=audit.get("title", ""),
            score=float(score),
            display_value=audit.get("displayValue", ""),
            description=audit.get("description", ""),
            details_type=audit.get("details", {}).get("type", ""),
        )
        if score < 0.5:
            failed_audits.append(a)
        if audit.get("details", {}).get("type") == "opportunity" and score < 0.9:
            opportunities.append(a)

    # Sort by impact
    opportunities.sort(key=lambda a: a.score)
    failed_audits.sort(key=lambda a: a.score)

    return PageSpeedResult(
        url=url,
        strategy=strategy,
        performance_score=round(perf_score, 1),
        seo_score=round(seo_score, 1),
        accessibility_score=round(a11y_score, 1),
        best_practices_score=round(bp_score, 1),
        lcp=lcp, cls=cls, inp=inp, fcp=fcp, ttfb=ttfb,
        cwv_pass=cwv_pass,
        opportunities=opportunities[:10],
        failed_audits=failed_audits[:10],
        timestamp=_utc_now(),
    )


def _parse_crux_metric(
    key: str,
    crux: dict,
    unit: str,
    good_threshold: float,
    poor_threshold: float,
) -> CWVMetric | None:
    if key not in crux:
        return None
    m = crux[key]
    percentile = m.get("percentile", 0)
    category   = m.get("category", "")

    if category == "FAST":
        rating = "FAST"
    elif category == "AVERAGE":
        rating = "AVERAGE"
    elif category == "SLOW":
        rating = "SLOW"
    else:
        # Derive from value
        val = percentile
        if val <= good_threshold:
            rating = "FAST"
        elif val <= poor_threshold:
            rating = "AVERAGE"
        else:
            rating = "SLOW"

    return CWVMetric(
        name=key,
        value=float(percentile) / (1000 if unit == "ms" else 1),
        unit=unit,
        rating=rating,
        percentile=percentile,
    )


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def _save_cwv_history(result: PageSpeedResult) -> None:
    try:
        import json
        slug = _url_to_slug(result.url)
        history_dir = _CWV_DIR / result.strategy
        history_dir.mkdir(parents=True, exist_ok=True)
        history_file = history_dir / f"{slug}.json"

        history: list[dict] = []
        if history_file.exists():
            history = json.loads(history_file.read_text(encoding="utf-8"))

        history.append(result.to_dict())
        history = history[-90:]  # keep 90 snapshots max
        history_file.write_text(json.dumps(history, indent=2), encoding="utf-8")
    except Exception as e:
        log.warning("pagespeed.save_history_fail  err=%s", e)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _url_to_slug(url: str) -> str:
    import re
    slug = re.sub(r'https?://', '', url)
    slug = re.sub(r'[^a-z0-9]', '_', slug.lower())
    return slug[:120]


def _metric_dict(m: CWVMetric | None) -> dict | None:
    if m is None:
        return None
    return {"name": m.name, "value": m.value, "unit": m.unit, "rating": m.rating, "percentile": m.percentile}


def _lcp_fix_steps(result: PageSpeedResult) -> str:
    steps = [
        "1. Identify the LCP element (run PageSpeed Insights and check 'LCP element' in the report).",
        "2. If LCP is an image: add loading='eager' and fetchpriority='high' attributes.",
        "3. Convert hero images to WebP format (30-50% smaller).",
        "4. Add explicit width and height to LCP image to prevent layout shift.",
        "5. Ensure the LCP image is not lazy-loaded.",
        "6. Preload the LCP image with <link rel='preload' as='image'>.",
        "7. Reduce server response time (TTFB) — upgrade hosting or add server-side caching.",
        "8. Minify render-blocking CSS above the fold.",
    ]
    if result.opportunities:
        for opp in result.opportunities[:3]:
            if opp.display_value:
                steps.append(f"   Lighthouse: {opp.title} — {opp.display_value}")
    return "\n".join(steps)


def _opportunity_steps(opportunities: list[LighthouseAudit]) -> str:
    if not opportunities:
        return "Run Lighthouse audit and address top 5 opportunities."
    steps = []
    for i, opp in enumerate(opportunities[:8], 1):
        detail = f" ({opp.display_value})" if opp.display_value else ""
        steps.append(f"{i}. {opp.title}{detail}")
    return "\n".join(steps)
