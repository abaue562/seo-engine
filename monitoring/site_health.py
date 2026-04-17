"""Site health monitoring — daily uptime checks, weekly PageSpeed sampling, GSC coverage.

Feeds the Site Health dashboard in doc 05/06.
Alert thresholds:
  - > 1% non-2xx pages: ops alert + customer banner
  - CWV drops > 15% WoW: customer alert
  - New GSC coverage errors: customer alert
"""
from __future__ import annotations
import logging, os, time, json
from datetime import datetime, timezone, timedelta
from pathlib import Path
import requests

log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "SEOEngineHealthBot/1.0 (+https://gethubed.com/bot)"}
_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


def _redis():
    try:
        import redis
        r = redis.from_url(_REDIS_URL, decode_responses=True, socket_timeout=2)
        r.ping()
        return r
    except Exception:
        return None


def check_url_uptime(url: str, timeout: int = 10) -> dict:
    """HEAD request to check uptime and response time."""
    start = time.time()
    try:
        r = requests.head(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        ms = int((time.time() - start) * 1000)
        return {"url": url, "status": r.status_code, "ok": 200 <= r.status_code < 300, "ms": ms}
    except Exception as e:
        ms = int((time.time() - start) * 1000)
        return {"url": url, "status": 0, "ok": False, "ms": ms, "error": str(e)}


def run_uptime_check(business_id: str, urls: list[str]) -> dict:
    """Check uptime for all published URLs for a business."""
    results = [check_url_uptime(u) for u in urls]
    total = len(results)
    failed = [r for r in results if not r["ok"]]
    pct_failed = round(100 * len(failed) / total, 2) if total else 0

    summary = {
        "business_id": business_id,
        "checked_at": datetime.now(tz=timezone.utc).isoformat(),
        "total": total,
        "ok": total - len(failed),
        "failed": len(failed),
        "pct_failed": pct_failed,
        "failures": failed[:20],
        "alert": pct_failed > 1.0,
    }

    # Cache in Redis for dashboard
    r = _redis()
    if r:
        try:
            r.setex(f"health:uptime:{business_id}", 86400, json.dumps(summary))
        except Exception:
            pass

    if summary["alert"]:
        log.warning("site_health.uptime_alert  biz=%s  pct_failed=%.1f%%  n=%d",
                    business_id, pct_failed, len(failed))

    return summary


def run_pagespeed_sample(business_id: str, urls: list[str], sample_size: int = 3) -> dict:
    """Run PageSpeed on a random sample of URLs. Respects API rate limits."""
    api_key = os.getenv("GOOGLE_PAGESPEED_API_KEY", "")
    if not api_key:
        return {"business_id": business_id, "skipped": True, "reason": "no_api_key"}

    import random
    sample = random.sample(urls, min(sample_size, len(urls)))
    scores = []
    for url in sample:
        api = f"https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url={url}&key={api_key}&strategy=mobile"
        try:
            resp = requests.get(api, timeout=30)
            if resp.ok:
                data = resp.json()
                score = data.get("lighthouseResult", {}).get("categories", {}).get("performance", {}).get("score", None)
                if score is not None:
                    scores.append({"url": url, "score": round(score * 100)})
                time.sleep(1)  # rate limit
        except Exception as e:
            log.debug("site_health.pagespeed_fail  url=%s  err=%s", url, e)

    avg = round(sum(s["score"] for s in scores) / len(scores)) if scores else None

    # Compare to last week's average from Redis
    r = _redis()
    prev_avg = None
    alert = False
    if r:
        try:
            prev = r.get(f"health:pagespeed:{business_id}:avg")
            if prev:
                prev_avg = int(prev)
                if avg and prev_avg and avg < prev_avg * 0.85:
                    alert = True
                    log.warning("site_health.cwv_drop  biz=%s  prev=%d  now=%d", business_id, prev_avg, avg)
            if avg:
                r.setex(f"health:pagespeed:{business_id}:avg", 86400 * 7, str(avg))
        except Exception:
            pass

    return {
        "business_id": business_id,
        "sampled": len(scores), "avg_score": avg, "prev_avg": prev_avg,
        "scores": scores, "alert": alert,
    }


def get_health_summary(business_id: str) -> dict:
    """Return cached health summary for dashboard."""
    r = _redis()
    if not r:
        return {"business_id": business_id, "error": "redis_unavailable"}
    uptime = r.get(f"health:uptime:{business_id}")
    pagespeed = r.get(f"health:pagespeed:{business_id}:avg")
    return {
        "business_id": business_id,
        "uptime": json.loads(uptime) if uptime else None,
        "pagespeed_avg": int(pagespeed) if pagespeed else None,
    }
