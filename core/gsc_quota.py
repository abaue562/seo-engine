"""GSC API quota tracker — per-tenant daily quota with priority queuing.

Google limits:
  - Indexing API:      200 req/day per service account
  - URL Inspection:   2000 req/day per property

Quota is tracked in Redis per (tenant_id, api, date).
When quota is exhausted, submissions are deferred to the next day.

Priority order within a tenant's daily quota:
  Indexing API:   new_publish > optimization_update > retry
  URL Inspection: first_check > 6h_followup > subsequent
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Literal

log = logging.getLogger(__name__)

_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

_LIMITS: dict[str, int] = {
    "indexing_api":    200,
    "url_inspection": 2000,
}

_WARN_THRESHOLD = 0.80   # warn at 80% consumed
_DEFER_THRESHOLD = 1.00  # defer at 100%

Priority = Literal["new_publish", "optimization_update", "retry",
                   "first_check", "6h_followup", "subsequent"]

_PRIORITY_WEIGHTS: dict[str, int] = {
    "new_publish": 3, "optimization_update": 2, "retry": 1,
    "first_check": 3, "6h_followup": 2, "subsequent": 1,
}


def _get_redis():
    try:
        import redis
        r = redis.from_url(_REDIS_URL, decode_responses=True, socket_timeout=2)
        r.ping()
        return r
    except Exception as e:
        log.debug("gsc_quota.redis_unavailable  err=%s", e)
        return None


def _quota_key(tenant_id: str, api: str, date: str) -> str:
    return f"gsc:quota:{tenant_id}:{api}:{date}"


def _today() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")


def check_quota(tenant_id: str, api: str, priority: Priority = "new_publish") -> dict:
    """Check whether a GSC API call is allowed for this tenant today.

    Returns:
        {"allowed": bool, "remaining": int, "reason": str, "defer_to": str|None}
    """
    limit = _LIMITS.get(api)
    if limit is None:
        return {"allowed": True, "remaining": -1, "reason": "unknown_api"}

    r = _get_redis()
    if r is None:
        return {"allowed": True, "remaining": -1, "reason": "redis_unavailable"}

    today = _today()
    key = _quota_key(tenant_id, api, today)
    try:
        used = int(r.get(key) or 0)
        remaining = max(0, limit - used)
        utilization = used / limit

        if utilization >= _DEFER_THRESHOLD:
            log.warning(
                "gsc_quota.exhausted  tenant=%s  api=%s  used=%d  limit=%d",
                tenant_id, api, used, limit,
            )
            return {
                "allowed": False, "remaining": 0,
                "reason": "quota_exhausted", "defer_to": "tomorrow",
            }

        if utilization >= _WARN_THRESHOLD:
            log.warning(
                "gsc_quota.high_usage  tenant=%s  api=%s  used=%d  limit=%d  pct=%.0f%%",
                tenant_id, api, used, limit, 100 * utilization,
            )

        return {"allowed": True, "remaining": remaining, "reason": "ok", "defer_to": None}
    except Exception as e:
        log.debug("gsc_quota.check_fail  err=%s", e)
        return {"allowed": True, "remaining": -1, "reason": "error_allow"}


def consume_quota(tenant_id: str, api: str, count: int = 1) -> bool:
    """Increment quota counter. Call after a successful GSC API call."""
    r = _get_redis()
    if r is None:
        return False
    today = _today()
    key = _quota_key(tenant_id, api, today)
    try:
        p = r.pipeline()
        p.incrby(key, count)
        p.expire(key, 90000)   # 25h — survives midnight UTC
        p.execute()
        return True
    except Exception as e:
        log.debug("gsc_quota.consume_fail  err=%s", e)
        return False


def get_quota_summary(tenant_id: str) -> dict:
    """Return today's quota usage across all GSC APIs for a tenant."""
    r = _get_redis()
    if r is None:
        return {"error": "redis_unavailable"}
    today = _today()
    result = {}
    for api, limit in _LIMITS.items():
        key = _quota_key(tenant_id, api, today)
        try:
            used = int(r.get(key) or 0)
            result[api] = {
                "used": used, "limit": limit,
                "remaining": max(0, limit - used),
                "utilization_pct": round(100 * used / limit, 1),
            }
        except Exception:
            result[api] = {"error": "unavailable"}
    return {"tenant_id": tenant_id, "date": today, "apis": result}


def enqueue_deferred(tenant_id: str, api: str, url: str, priority: Priority = "retry") -> bool:
    """Push a deferred GSC submission to tomorrow's priority queue."""
    r = _get_redis()
    if r is None:
        return False
    score = _PRIORITY_WEIGHTS.get(priority, 1)
    queue_key = f"gsc:deferred:{tenant_id}:{api}"
    try:
        r.zadd(queue_key, {url: score})
        r.expire(queue_key, 86400 * 2)
        log.info("gsc_quota.deferred  tenant=%s  api=%s  url=%s  priority=%s", tenant_id, api, url, priority)
        return True
    except Exception as e:
        log.debug("gsc_quota.enqueue_fail  err=%s", e)
        return False


def pop_deferred_batch(tenant_id: str, api: str, n: int = 10) -> list[str]:
    """Pop highest-priority deferred URLs for processing (highest score first)."""
    r = _get_redis()
    if r is None:
        return []
    queue_key = f"gsc:deferred:{tenant_id}:{api}"
    try:
        items = r.zrevrange(queue_key, 0, n - 1)
        if items:
            r.zrem(queue_key, *items)
        return list(items)
    except Exception as e:
        log.debug("gsc_quota.pop_fail  err=%s", e)
        return []
