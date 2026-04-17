"""Idempotency helpers for external writes.

Prevents duplicate WordPress posts, Google Indexing API submissions,
outreach emails, and CRM pushes when Celery retries a task after a
network failure that actually succeeded.

Usage:
    from core.idempotency import safe_external_write, mark_seen, is_seen

    # Wrap any external write with a deterministic key
    result = safe_external_write(
        key=f"wp_publish:{tenant_id}:{task_id}",
        fn=lambda: wp_client.create_post(title, content),
        ttl=86400,  # 24h
    )

    # Manual check/mark (for more complex flows)
    if is_seen("indexnow:{sha}"):
        return cached_result
    result = submit_to_indexnow(url)
    mark_seen("indexnow:{sha}", result, ttl=86400)
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Any, Callable

log = logging.getLogger(__name__)

_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_DEFAULT_TTL = 86400  # 24 hours
_KEY_PREFIX = "idem:"


def _get_redis():
    import redis
    return redis.from_url(_REDIS_URL, decode_responses=True, socket_timeout=3)


def _full_key(key: str) -> str:
    return _KEY_PREFIX + key


def is_seen(key: str) -> bool:
    """Return True if this key has been marked as seen within its TTL."""
    try:
        r = _get_redis()
        return bool(r.exists(_full_key(key)))
    except Exception as e:
        log.warning("idempotency.is_seen_fail  key=%s  err=%s  (allowing through)", key[:40], e)
        return False  # Fail open: allow the write if Redis is down


def get_result(key: str) -> dict | None:
    """Return the cached result for a key, or None if not seen."""
    try:
        r = _get_redis()
        raw = r.get(_full_key(key))
        if raw:
            return json.loads(raw)
        return None
    except Exception as e:
        log.warning("idempotency.get_result_fail  key=%s  err=%s", key[:40], e)
        return None


def mark_seen(key: str, result: Any = None, ttl: int = _DEFAULT_TTL) -> None:
    """Mark a key as seen with an optional result payload."""
    try:
        r = _get_redis()
        payload = json.dumps(result, default=str) if result is not None else "1"
        r.setex(_full_key(key), ttl, payload)
        log.debug("idempotency.marked  key=%s  ttl=%d", key[:40], ttl)
    except Exception as e:
        log.warning("idempotency.mark_fail  key=%s  err=%s", key[:40], e)


def safe_external_write(
    key: str,
    fn: Callable[[], Any],
    ttl: int = _DEFAULT_TTL,
) -> dict:
    """Execute fn() exactly once per key within the TTL window.

    If the key has been seen (prior successful execution), returns the cached
    result without calling fn(). Protects against duplicate writes on retry.

    Args:
        key: Deterministic idempotency key (e.g. "wp_publish:{tenant}:{task_id}").
        fn:  Callable to execute. Should return a JSON-serialisable dict.
        ttl: Seconds to remember the key (default 24h).

    Returns:
        Result dict from fn(), or {"status": "duplicate", "cached": True, ...}
        if the key was already seen.
    """
    cached = get_result(key)
    if cached is not None:
        log.info("idempotency.duplicate_skipped  key=%s", key[:40])
        return {**cached, "idempotent_replay": True}

    result = fn()
    mark_seen(key, result, ttl=ttl)
    return result


# ---------------------------------------------------------------------------
# Domain-specific helpers
# ---------------------------------------------------------------------------

def wp_idempotency_key(tenant_id: str, task_id: str) -> str:
    """Key for a WordPress post publish operation."""
    return f"wp_publish:{tenant_id}:{task_id}"


def indexnow_idempotency_key(url: str) -> str:
    """Key for an IndexNow submission -- deduplicates within 24h."""
    url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
    from datetime import datetime, timezone
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"indexnow:{url_hash}:{date_str}"


def google_index_key(url: str) -> str:
    """Key for a Google Indexing API submission -- deduplicates within 24h."""
    url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
    from datetime import datetime, timezone
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"gindex:{url_hash}:{date_str}"


def outreach_key(campaign_id: str, step_num: int, recipient: str) -> str:
    """Key for an outreach email send -- deduplicates permanently (7d TTL)."""
    recipient_hash = hashlib.sha256(recipient.encode()).hexdigest()[:12]
    return f"outreach:{campaign_id}:{step_num}:{recipient_hash}"


def crm_push_key(tenant_id: str, lead_event_id: str, platform: str) -> str:
    """Key for a CRM push -- deduplicates within 24h."""
    return f"crm:{tenant_id[:8]}:{lead_event_id}:{platform}"
