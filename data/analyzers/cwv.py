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
    from data.analyzers.cwv import CWVAnalyzer

    # Functional API:
    result = measure_cwv("https://blendbrightlights.com")
    print(f"LCP: {result.lcp.value}ms  CLS: {result.cls.value}  INP: {result.inp.value}ms")
    print(f"CWV grade: {result.grade}  Passed: {result.passed}")

    # Class API:
    cwv = CWVAnalyzer()
    result = cwv.analyze("https://mysite.com")
    batch = cwv.analyze_batch(["url1", "url2"])
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

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

# PSI audit → recommended quick-win mapping
_AUDIT_ACTIONS = {
    "render-blocking-resources":   ("Remove render-blocking CSS/JS",          "medium"),
    "uses-optimized-images":       ("Compress and next-gen-format images",    "easy"),
    "unused-javascript":           ("Remove or defer unused JavaScript",      "medium"),
    "uses-text-compression":       ("Enable gzip/brotli compression",         "easy"),
    "efficient-animated-content":  ("Replace GIFs with WebM/MP4",            "easy"),
    "unused-css-rules":            ("Remove unused CSS",                      "medium"),
    "uses-long-cache-ttl":         ("Set long cache TTL on static assets",   "easy"),
    "total-byte-weight":           ("Reduce total page weight",               "hard"),
    "dom-size":                    ("Reduce DOM size (target <1500 nodes)",   "hard"),
    "server-response-time":        ("Improve server response time / TTFB",   "hard"),
    "largest-contentful-paint-element": ("Optimize the LCP element",         "hard"),
    "layout-shift-elements":       ("Fix CLS: set explicit dimensions",       "medium"),
    "long-tasks":                  ("Break up long JavaScript tasks",         "hard"),
    "uses-responsive-images":      ("Serve correctly-sized images",           "easy"),
    "uses-rel-preconnect":         ("Add <link rel=preconnect> for origins",  "easy"),
    "uses-rel-preload":            ("Preload critical resources",             "medium"),
    "critical-request-chains":     ("Reduce critical request chain depth",    "hard"),
    "offscreen-images":            ("Lazy-load offscreen images",             "easy"),
    "third-party-summary":         ("Audit and reduce third-party scripts",   "hard"),
}


# ── Pydantic models (unchanged public API) ────────────────────────────────────

class CWVMetric(BaseModel):
    value: float = 0.0
    display: str = ""
    rating: str = "unknown"   # good / needs-improvement / poor


class CWVResult(BaseModel):
    url: str
    strategy: str = "mobile"

    lcp: CWVMetric = Field(default_factory=CWVMetric)
    cls: CWVMetric = Field(default_factory=CWVMetric)
    inp: CWVMetric = Field(default_factory=CWVMetric)
    ttfb: CWVMetric = Field(default_factory=CWVMetric)
    fcp: CWVMetric = Field(default_factory=CWVMetric)
    tbt: CWVMetric = Field(default_factory=CWVMetric)
    speed_index: CWVMetric = Field(default_factory=CWVMetric)

    performance_score: int = 0
    passed: bool = False

    grade: str = "F"
    bottleneck: str = ""
    action: str = ""
    opportunities: list[str] = Field(default_factory=list)

    fetched_at: float = Field(default_factory=time.time)
    error: str = ""


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def _overall_grade(result: CWVResult) -> str:
    """Assign overall CWV grade: 'good', 'needs_improvement', or 'poor'."""
    ratings = [result.lcp.rating, result.cls.rating, result.inp.rating]
    if "poor" in ratings:
        return "poor"
    if "needs-improvement" in ratings:
        return "needs_improvement"
    return "good"


# ── Core measurement function ─────────────────────────────────────────────────

def measure_cwv(
    url: str,
    strategy: str = "mobile",
    api_key: str = "",
) -> CWVResult:
    """Measure Core Web Vitals for a URL via PageSpeed Insights API."""
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

    categories = data.get("lighthouseResult", {}).get("categories", {})
    perf = categories.get("performance", {})
    result.performance_score = int(round(perf.get("score", 0) * 100))
    result.grade = _grade(result.performance_score)

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
    result.speed_index = CWVMetric(
        value=si_val, display=si_disp,
        rating="good" if si_val < 3400 else "needs-improvement",
    )

    result.passed = all(m.rating == "good" for m in [result.lcp, result.cls, result.inp])

    ranked = sorted([
        ("LCP",  lcp_val  / THRESHOLDS["lcp"]["good"],  "Improve LCP: compress images, use CDN, reduce TTFB"),
        ("CLS",  cls_val  / THRESHOLDS["cls"]["good"],  "Fix CLS: set explicit width/height on images, avoid late-loading content"),
        ("INP",  inp_val  / THRESHOLDS["inp"]["good"],  "Fix INP: reduce JS execution time, break up long tasks"),
        ("TTFB", ttfb_val / THRESHOLDS["ttfb"]["good"], "Reduce TTFB: upgrade hosting, add server-side caching, use CDN"),
        ("TBT",  tbt_val  / THRESHOLDS["tbt"]["good"],  "Reduce TBT: defer unused JS, split code bundles"),
    ], key=lambda x: x[1], reverse=True)

    result.bottleneck = ranked[0][0]
    result.action = ranked[0][2]

    opportunities = []
    for audit_id, audit in audits.items():
        if audit.get("score") is not None and audit.get("score") < 0.9:
            title = audit.get("title", "")
            savings = audit.get("details", {}).get("overallSavingsMs", 0)
            if title and (savings > 100 or audit.get("score", 1) < 0.5):
                opportunities.append(
                    f"{title} (~{savings:.0f}ms savings)" if savings else title
                )

    result.opportunities = sorted(opportunities)[:8]

    log.info(
        "cwv.measured  url=%s  strategy=%s  score=%d  lcp=%sms  cls=%s  inp=%sms  passed=%s",
        url, strategy, result.performance_score,
        f"{lcp_val:.0f}", f"{cls_val:.3f}", f"{inp_val:.0f}", result.passed,
    )
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
        "why": (
            f"CWV are confirmed ranking signals. {business_name} scores "
            f"{result.performance_score}/100 on mobile. Fixing {result.bottleneck} "
            f"alone can recover 2-5 positions."
        ),
        "execution": result.action,
        "total_score": max(0, 10 - result.performance_score // 10),
    }


# ── CWVAnalyzer class API ─────────────────────────────────────────────────────

class CWVAnalyzer:
    """Class-based CWV analyzer with caching and batch support."""

    PSI_URL = PSI_URL
    CACHE_PATH = Path("data/storage/cwv_cache")
    CACHE_TTL_HOURS = 24

    def __init__(self) -> None:
        self.api_key = os.getenv("PSI_API_KEY", "")
        self.cache_path = self.CACHE_PATH
        self.cache_path.mkdir(parents=True, exist_ok=True)

    def analyze(self, url: str, strategy: str = "mobile", use_cache: bool = True) -> CWVResult:
        """Analyze Core Web Vitals for a URL.

        strategy: 'mobile' or 'desktop'
        Caches results for CACHE_TTL_HOURS hours.
        """
        if use_cache:
            cached = self._load_cache(url, strategy)
            if cached is not None:
                return cached

        result = measure_cwv(url, strategy=strategy, api_key=self.api_key)

        if use_cache and not result.error:
            self._save_cache(url, strategy, result)

        return result

    def analyze_batch(self, urls: list[str], strategy: str = "mobile") -> list[CWVResult]:
        """Analyze multiple URLs. Adds 1s delay between PSI calls."""
        results = []
        for i, url in enumerate(urls):
            if i > 0:
                time.sleep(1)
            results.append(self.analyze(url, strategy=strategy))
        return results

    def get_quick_wins(self, result: CWVResult) -> list[dict]:
        """Return quick-win opportunities sorted by estimated savings.

        Returns list of {action, description, estimated_savings_ms, difficulty}.
        """
        # Re-fetch raw audits for savings data — we only stored summaries in CWVResult
        # If we can't re-fetch, derive quick wins from the stored opportunities list
        quick_wins = []

        for opp_text in result.opportunities:
            # Parse opportunity text: "Title (~1200ms savings)" or just "Title"
            savings_ms = 0.0
            import re
            m = re.search(r"~(\d+)ms savings", opp_text)
            if m:
                savings_ms = float(m.group(1))
            title = re.sub(r"\s*\(~\d+ms savings\)", "", opp_text).strip()

            # Look up difficulty from known audit actions
            difficulty = "medium"
            description = title
            for audit_id, (action_desc, diff) in _AUDIT_ACTIONS.items():
                if any(word in title.lower() for word in audit_id.replace("-", " ").split()):
                    difficulty = diff
                    description = action_desc
                    break

            quick_wins.append({
                "action": title,
                "description": description,
                "estimated_savings_ms": savings_ms,
                "difficulty": difficulty,
            })

        # Sort by savings descending, then easy first
        diff_order = {"easy": 0, "medium": 1, "hard": 2}
        quick_wins.sort(key=lambda x: (-x["estimated_savings_ms"], diff_order.get(x["difficulty"], 1)))
        return quick_wins

    def grade(self, result: CWVResult) -> str:
        """Assign overall CWV grade: 'good', 'needs_improvement', or 'poor'.

        Google's thresholds:
          LCP: good <2500ms, needs_improvement <4000ms, poor >=4000ms
          CLS: good <0.1, needs_improvement <0.25, poor >=0.25
          INP: good <200ms, needs_improvement <500ms, poor >=500ms
        Overall: 'poor' if any poor, 'needs_improvement' if any needs_improvement, else 'good'.
        """
        return _overall_grade(result)

    def _parse_psi_response(self, data: dict, url: str) -> CWVResult:
        """Parse PageSpeed Insights API response into CWVResult."""
        # This mirrors measure_cwv but accepts a pre-fetched dict
        result = CWVResult(url=url, strategy="mobile")

        categories = data.get("lighthouseResult", {}).get("categories", {})
        perf = categories.get("performance", {})
        result.performance_score = int(round(perf.get("score", 0) * 100))
        result.grade = _grade(result.performance_score)

        audits = data.get("lighthouseResult", {}).get("audits", {})

        def _extract(audit_id: str) -> tuple[float, str]:
            a = audits.get(audit_id, {})
            return round(a.get("numericValue", 0.0), 1), a.get("displayValue", "")

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
        result.speed_index = CWVMetric(
            value=si_val, display=si_disp,
            rating="good" if si_val < 3400 else "needs-improvement",
        )
        result.passed = all(m.rating == "good" for m in [result.lcp, result.cls, result.inp])

        # Opportunities
        opportunities = []
        for audit_id, audit in audits.items():
            if audit.get("score") is not None and audit.get("score") < 0.9:
                title = audit.get("title", "")
                savings = audit.get("details", {}).get("overallSavingsMs", 0)
                if title and (savings > 100 or audit.get("score", 1) < 0.5):
                    opportunities.append(
                        f"{title} (~{savings:.0f}ms savings)" if savings else title
                    )
        result.opportunities = sorted(opportunities)[:8]

        # Bottleneck
        ranked = sorted([
            ("LCP",  lcp_val  / THRESHOLDS["lcp"]["good"]),
            ("CLS",  cls_val  / THRESHOLDS["cls"]["good"]),
            ("INP",  inp_val  / THRESHOLDS["inp"]["good"]),
            ("TTFB", ttfb_val / THRESHOLDS["ttfb"]["good"]),
            ("TBT",  tbt_val  / THRESHOLDS["tbt"]["good"]),
        ], key=lambda x: x[1], reverse=True)
        result.bottleneck = ranked[0][0]

        return result

    def _load_cache(self, url: str, strategy: str) -> Optional[CWVResult]:
        """Load cached result if still fresh (within CACHE_TTL_HOURS)."""
        cache_file = self._cache_file(url, strategy)
        if not cache_file.exists():
            return None
        try:
            with cache_file.open() as f:
                data = json.load(f)
            cached_at = data.get("_cached_at", 0)
            age_hours = (time.time() - cached_at) / 3600
            if age_hours > self.CACHE_TTL_HOURS:
                return None
            # Remove internal key before parsing
            data.pop("_cached_at", None)
            return CWVResult(**data)
        except Exception as e:
            log.debug("cwv.cache_miss  url=%s  err=%s", url, e)
            return None

    def _save_cache(self, url: str, strategy: str, result: CWVResult) -> None:
        """Save result to cache as JSON."""
        cache_file = self._cache_file(url, strategy)
        try:
            data = result.model_dump()
            data["_cached_at"] = time.time()
            with cache_file.open("w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            log.warning("cwv.cache_save_fail  url=%s  err=%s", url, e)

    def _cache_file(self, url: str, strategy: str) -> Path:
        """Return deterministic cache file path for a URL + strategy."""
        import hashlib
        key = hashlib.md5(f"{url}:{strategy}".encode()).hexdigest()
        return self.cache_path / f"{key}.json"
