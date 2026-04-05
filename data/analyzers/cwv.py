"""Core Web Vitals Analyzer — real LCP, CLS, INP via PageSpeed Insights API.

Uses Google's PageSpeed Insights API (free tier: 25,000 requests/day, no key needed for basic).
Set PSI_API_KEY in config/.env for higher quota.

Metrics tracked:
  - LCP  (Largest Contentful Paint) — target < 2.5s   [ranking signal]
  - CLS  (Cumulative Layout Shift)  — target < 0.1     [ranking signal]
  - INP  (Interaction to Next Paint)— target < 200ms   [ranking signal, replaced FID 2024]
  - TTFB (Time to First Byte)       — target < 800ms
  - FCP  (First Contentful Paint)   — target < 1.8s
  - Speed Index                     — target < 3.4s
  - TBT  (Total Blocking Time)      — target < 200ms

Usage:
    from data.analyzers.cwv import measure_cwv, score_cwv, CWVResult

    result = measure_cwv("https://blendbrightlights.com")
    print(f"LCP: {result.lcp_ms}ms  CLS: {result.cls}  INP: {result.inp_ms}ms")
    print(f"CWV grade: {result.grade}  Passed: {result.passed}")
"""

from __future__ import annotations

import logging
import os
import time

import requests
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

PSI_URL = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"

# CWV pass/fail thresholds (Google's official values)
THRESHOLDS = {
    "lcp":  {"good": 2500,  "poor": 4000},   # ms
    "cls":  {"good": 0.1,   "poor": 0.25},   # unitless
    "inp":  {"good": 200,   "poor": 500},     # ms
    "ttfb": {"good": 800,   "poor": 1800},    # ms
    "fcp":  {"good": 1800,  "poor": 3000},    # ms
    "tbt":  {"good": 200,   "poor": 600},     # ms
}


class CWVMetric(BaseModel):
    value: float = 0.0
    display: str = ""
    rating: str = "unknown"   # good / needs-improvement / poor


class CWVResult(BaseModel):
    url: str
    strategy: str = "mobile"  # mobile / desktop

    # Core Web Vitals (the 3 ranking signals)
    lcp: CWVMetric = Field(default_factory=CWVMetric)   # ms
    cls: CWVMetric = Field(default_factory=CWVMetric)   # unitless
    inp: CWVMetric = Field(default_factory=CWVMetric)   # ms

    # Supporting metrics
    ttfb: CWVMetric = Field(default_factory=CWVMetric)  # ms
    fcp: CWVMetric = Field(default_factory=CWVMetric)   # ms
    tbt: CWVMetric = Field(default_factory=CWVMetric)   # ms
    speed_index: CWVMetric = Field(default_factory=CWVMetric)

    # Scores
    performance_score: int = 0     # 0-100
    passed: bool = False           # True if all 3 CWVs pass

    # Derived
    grade: str = "F"               # A / B / C / D / F
    bottleneck: str = ""           # Which metric is worst
    action: str = ""               # Top recommended fix
    opportunities: list[str] = Field(default_factory=list)  # PSI audit items

    # Meta
    fetched_at: float = Field(default_factory=time.time)
    error: str = ""


def _rating(metric: str, value: float) -> str:
    t = THRESHOLDS.get(metric, {})
    if not t:
        return "unknown"
    if value <= t["good"]:
        return "good"
    elif value <= t["poor"]:
        return "needs-improvement"
    return "poor"


def _grade(score: int) -> str:
    if score >= 90: return "A"
    if score >= 70: return "B"
    if score >= 50: return "C"
    if score >= 30: return "D"
    return "F"


def measure_cwv(
    url: str,
    strategy: str = "mobile",
    api_key: str = "",
) -> CWVResult:
    """Measure Core Web Vitals for a URL via PageSpeed Insights API.

    Args:
        url: Full URL to measure (e.g. "https://blendbrightlights.com")
        strategy: "mobile" or "desktop" — Google uses mobile-first indexing
        api_key: PSI API key (optional — increases quota from 25K/day to higher)

    Returns:
        CWVResult with all metrics and actionable recommendations
    """
    result = CWVResult(url=url, strategy=strategy)

    params: dict = {"url": url, "strategy": strategy}
    key = api_key or os.getenv("PSI_API_KEY", "")
    if key:
        params["key"] = key

    for attempt in range(3):
        try:
            resp = requests.get(PSI_URL, params=params, timeout=60)
            if resp.status_code == 429:
                wait = (attempt + 1) * 10
                log.warning("cwv.rate_limited  attempt=%d  waiting=%ds", attempt + 1, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            if attempt == 2:
                log.error("cwv.fetch_fail  url=%s  err=%s", url, e)
                result.error = str(e)
                return result
            time.sleep(5)
    else:
        result.error = "Rate limited after 3 attempts"
        return result

    # --- Performance score ---
    categories = data.get("lighthouseResult", {}).get("categories", {})
    perf = categories.get("performance", {})
    result.performance_score = int(round(perf.get("score", 0) * 100))
    result.grade = _grade(result.performance_score)

    # --- Lab metrics (Lighthouse) ---
    audits = data.get("lighthouseResult", {}).get("audits", {})

    def _extract(audit_id: str) -> tuple[float, str]:
        a = audits.get(audit_id, {})
        num = a.get("numericValue", 0.0)
        display = a.get("displayValue", "")
        return round(num, 1), display

    lcp_val, lcp_disp   = _extract("largest-contentful-paint")
    cls_val, cls_disp   = _extract("cumulative-layout-shift")
    inp_val, inp_disp   = _extract("interaction-to-next-paint")
    ttfb_val, ttfb_disp = _extract("server-response-time")
    fcp_val, fcp_disp   = _extract("first-contentful-paint")
    tbt_val, tbt_disp   = _extract("total-blocking-time")
    si_val, si_disp     = _extract("speed-index")

    result.lcp  = CWVMetric(value=lcp_val,  display=lcp_disp,  rating=_rating("lcp",  lcp_val))
    result.cls  = CWVMetric(value=cls_val,  display=cls_disp,  rating=_rating("cls",  cls_val))
    result.inp  = CWVMetric(value=inp_val,  display=inp_disp,  rating=_rating("inp",  inp_val))
    result.ttfb = CWVMetric(value=ttfb_val, display=ttfb_disp, rating=_rating("ttfb", ttfb_val))
    result.fcp  = CWVMetric(value=fcp_val,  display=fcp_disp,  rating=_rating("fcp",  fcp_val))
    result.tbt  = CWVMetric(value=tbt_val,  display=tbt_disp,  rating=_rating("tbt",  tbt_val))
    result.speed_index = CWVMetric(value=si_val, display=si_disp, rating="good" if si_val < 3400 else "needs-improvement")

    # --- CWV pass/fail (all 3 must be "good") ---
    result.passed = all(
        m.rating == "good"
        for m in [result.lcp, result.cls, result.inp]
    )

    # --- Find worst bottleneck ---
    ranked = sorted([
        ("LCP",  lcp_val  / THRESHOLDS["lcp"]["good"],  "Improve LCP: compress images, use CDN, reduce TTFB"),
        ("CLS",  cls_val  / THRESHOLDS["cls"]["good"],  "Fix CLS: set explicit width/height on images, avoid late-loading content"),
        ("INP",  inp_val  / THRESHOLDS["inp"]["good"],  "Fix INP: reduce JS execution time, break up long tasks"),
        ("TTFB", ttfb_val / THRESHOLDS["ttfb"]["good"], "Reduce TTFB: upgrade hosting, add server-side caching, use CDN"),
        ("TBT",  tbt_val  / THRESHOLDS["tbt"]["good"],  "Reduce TBT: defer unused JS, split code bundles"),
    ], key=lambda x: x[1], reverse=True)

    result.bottleneck = ranked[0][0]
    result.action = ranked[0][2]

    # --- Extract opportunity items from PSI ---
    opportunities = []
    for audit_id, audit in audits.items():
        if audit.get("score") is not None and audit.get("score") < 0.9:
            title = audit.get("title", "")
            savings = audit.get("details", {}).get("overallSavingsMs", 0)
            if title and (savings > 100 or audit.get("score", 1) < 0.5):
                opportunities.append(f"{title} (~{savings:.0f}ms savings)" if savings else title)

    result.opportunities = sorted(opportunities)[:8]

    log.info("cwv.measured  url=%s  strategy=%s  score=%d  lcp=%sms  cls=%s  inp=%sms  passed=%s",
             url, strategy, result.performance_score,
             f"{lcp_val:.0f}", f"{cls_val:.3f}", f"{inp_val:.0f}", result.passed)

    return result


def measure_cwv_both(url: str, api_key: str = "") -> dict[str, CWVResult]:
    """Measure CWV for both mobile and desktop."""
    return {
        "mobile":  measure_cwv(url, strategy="mobile",  api_key=api_key),
        "desktop": measure_cwv(url, strategy="desktop", api_key=api_key),
    }


def score_cwv(result: CWVResult) -> dict:
    """Convert CWV result to a scoring block for the task engine."""
    metric_scores = {
        "lcp":  {"good": 10, "needs-improvement": 5, "poor": 0, "unknown": 5}[result.lcp.rating],
        "cls":  {"good": 10, "needs-improvement": 5, "poor": 0, "unknown": 5}[result.cls.rating],
        "inp":  {"good": 10, "needs-improvement": 5, "poor": 0, "unknown": 5}[result.inp.rating],
    }
    total = sum(metric_scores.values()) / 3

    return {
        "url": result.url,
        "performance_score": result.performance_score,
        "grade": result.grade,
        "cwv_score": round(total, 1),
        "passed": result.passed,
        "bottleneck": result.bottleneck,
        "top_action": result.action,
        "lcp": f"{result.lcp.value:.0f}ms ({result.lcp.rating})",
        "cls": f"{result.cls.value:.3f} ({result.cls.rating})",
        "inp": f"{result.inp.value:.0f}ms ({result.inp.rating})",
        "ttfb": f"{result.ttfb.value:.0f}ms ({result.ttfb.rating})",
        "opportunities": result.opportunities[:3],
    }


def cwv_to_task(result: CWVResult, business_name: str) -> dict | None:
    """Convert CWV failures into an actionable SEO task."""
    if result.passed:
        return None

    failing = []
    if result.lcp.rating != "good":
        failing.append(f"LCP {result.lcp.display} (target <2.5s)")
    if result.cls.rating != "good":
        failing.append(f"CLS {result.cls.display} (target <0.1)")
    if result.inp.rating != "good":
        failing.append(f"INP {result.inp.display} (target <200ms)")

    return {
        "type": "TECHNICAL",
        "priority": "HIGH" if result.performance_score < 50 else "MEDIUM",
        "action": f"Fix Core Web Vitals: {', '.join(failing)}",
        "target": result.url,
        "why": f"CWV are confirmed ranking signals. {business_name} scores {result.performance_score}/100 on mobile. Fixing {result.bottleneck} alone can recover 2-5 positions.",
        "execution": result.action,
        "total_score": max(0, 10 - result.performance_score // 10),
    }
