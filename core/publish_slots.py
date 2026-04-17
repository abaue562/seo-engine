"""Atomic publish slot reservation to prevent velocity race conditions.

Fixes: two workers processing the same tenant simultaneously both see
count=9 (limit=10), both publish, tenant exceeds their plan limit.

Uses Redis INCR for atomic slot reservation. On publish failure, the slot
is released with DECR so it can be used again.

Usage:
    from core.publish_slots import reserve_publish_slot, release_publish_slot

    slot = reserve_publish_slot(tenant_id, plan_limit=10)
    if not slot:
        return {"status": "quota_exceeded", ...}
    try:
        result = do_publish()
    except Exception:
        release_publish_slot(tenant_id)   # give back the slot
        raise
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


def _get_redis():
    import redis
    return redis.from_url(_REDIS_URL, decode_responses=True, socket_timeout=3)


def _slot_key(tenant_id: str, date_str: str) -> str:
    return f"tenant:{tenant_id}:publish:{date_str}"


def _end_of_day_utc() -> int:
    """Unix timestamp of the end of today UTC (midnight tonight)."""
    now = datetime.now(timezone.utc)
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int(midnight.timestamp())


def reserve_publish_slot(
    tenant_id: str,
    plan_limit: int,
    date_str: str | None = None,
) -> bool:
    """Atomically reserve one publish slot for today. Thread-safe via Redis INCR.

    Args:
        tenant_id:  Tenant UUID string.
        plan_limit: Maximum publishes allowed today for this tenant's plan.
        date_str:   Override date (YYYY-MM-DD). Defaults to today UTC.

    Returns:
        True if slot was reserved (publish may proceed).
        False if quota is already at or over the limit.
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = _slot_key(tenant_id, date_str)
    try:
        r = _get_redis()
        # Atomically increment and check
        count = r.incr(key)
        # Set TTL to end of day so the key auto-expires
        r.expireat(key, _end_of_day_utc())
        if count > plan_limit:
            # Over limit: undo the increment and refuse
            r.decr(key)
            log.warning(
                "publish_slot.quota_exceeded  tenant=%s  count=%d  limit=%d",
                tenant_id[:8], count - 1, plan_limit,
            )
            return False
        log.debug(
            "publish_slot.reserved  tenant=%s  slot=%d/%d  date=%s",
            tenant_id[:8], count, plan_limit, date_str,
        )
        return True
    except Exception as e:
        # Fail open if Redis is unreachable: let the publish proceed
        # rather than silently blocking all publishes
        log.warning(
            "publish_slot.reserve_fail  tenant=%s  err=%s  (failing_open)",
            tenant_id[:8], e,
        )
        return True


def release_publish_slot(tenant_id: str, date_str: str | None = None) -> None:
    """Release a previously reserved publish slot (call on publish failure).

    Args:
        tenant_id: Tenant UUID string.
        date_str:  Override date (YYYY-MM-DD). Defaults to today UTC.
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = _slot_key(tenant_id, date_str)
    try:
        r = _get_redis()
        new_val = r.decr(key)
        # Don't let it go negative
        if new_val < 0:
            r.set(key, 0)
        log.debug("publish_slot.released  tenant=%s  date=%s", tenant_id[:8], date_str)
    except Exception as e:
        log.warning("publish_slot.release_fail  tenant=%s  err=%s", tenant_id[:8], e)


def get_publish_count(tenant_id: str, date_str: str | None = None) -> int:
    """Return today's publish count for a tenant."""
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = _slot_key(tenant_id, date_str)
    try:
        r = _get_redis()
        val = r.get(key)
        return int(val) if val else 0
    except Exception as e:
        log.warning("publish_slot.count_fail  tenant=%s  err=%s", tenant_id[:8], e)
        return 0
