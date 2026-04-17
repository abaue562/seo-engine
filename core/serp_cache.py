"""SERP result cache — platform-wide Redis cache to prevent duplicate scrapes.

Cache key: serp_cache:{sha256(keyword + "|" + location_signal)}
TTL strategy:
  - high-volume keywords (volume > 1000): 24h
  - low-volume keywords: 72h
  - branded/volatile (contains brand name): 6h

Cost impact: target > 40% cache hit rate at scale.
No PII: SERP data is public; safe to share across tenants.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any

log = logging.getLogger(__name__)

_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_TTL_HIGH_VOLUME = 24 * 3600     # 24h for volume > 1000
_TTL_LOW_VOLUME  = 72 * 3600     # 72h default
_TTL_VOLATILE    = 6  * 3600     # 6h for branded / volatile
_HIGH_VOLUME_THRESHOLD = 1000
_METRICS_KEY = "serp_cache:metrics"


def _get_redis():
    try:
        import redis
        r = redis.from_url(_REDIS_URL, decode_responses=True, socket_timeout=2)
        r.ping()
        return r
    except Exception as e:
        log.debug("serp_cache.redis_unavailable  err=%s", e)
        return None


def _cache_key(keyword: str, location: str = "") -> str:
    raw = f"{keyword.lower().strip()}|{location.lower().strip()}"
    digest = hashlib.sha256(raw.encode()).hexdigest()
    return f"serp_cache:{digest}"


def _pick_ttl(keyword: str, volume: int = 0, volatile: bool = False) -> int:
    if volatile:
        return _TTL_VOLATILE
    if volume and volume > _HIGH_VOLUME_THRESHOLD:
        return _TTL_HIGH_VOLUME
    return _TTL_LOW_VOLUME


def get_cached_serp(keyword: str, location: str = "") -> dict | None:
    """Return cached SERP data or None if miss/expired."""
    r = _get_redis()
    if r is None:
        return None
    key = _cache_key(keyword, location)
    try:
        raw = r.get(key)
        if raw:
            r.hincrby(_METRICS_KEY, "hits", 1)
            data = json.loads(raw)
            log.debug("serp_cache.hit  kw=%s  loc=%s", keyword, location)
            return data
        r.hincrby(_METRICS_KEY, "misses", 1)
        return None
    except Exception as e:
        log.debug("serp_cache.get_fail  err=%s", e)
        return None


def set_cached_serp(
    keyword: str,
    data: dict,
    location: str = "",
    volume: int = 0,
    volatile: bool = False,
    bypass: bool = False,
) -> bool:
    """Store SERP result in cache. bypass=True forces a fresh scrape next time."""
    if bypass:
        return invalidate_serp(keyword, location)
    r = _get_redis()
    if r is None:
        return False
    key = _cache_key(keyword, location)
    ttl = _pick_ttl(keyword, volume, volatile)
    try:
        payload = json.dumps({**data, "_cached_at": time.time(), "_ttl": ttl})
        r.setex(key, ttl, payload)
        r.hincrby(_METRICS_KEY, "writes", 1)
        log.debug("serp_cache.set  kw=%s  ttl=%ds", keyword, ttl)
        return True
    except Exception as e:
        log.debug("serp_cache.set_fail  err=%s", e)
        return False


def invalidate_serp(keyword: str, location: str = "") -> bool:
    """Force next request to bypass cache for this keyword."""
    r = _get_redis()
    if r is None:
        return False
    key = _cache_key(keyword, location)
    try:
        r.delete(key)
        log.info("serp_cache.invalidated  kw=%s", keyword)
        return True
    except Exception as e:
        log.debug("serp_cache.invalidate_fail  err=%s", e)
        return False


def get_cache_stats() -> dict:
    """Return hit/miss/write counts. Used by health endpoint and dashboard."""
    r = _get_redis()
    if r is None:
        return {"error": "redis_unavailable"}
    try:
        raw = r.hgetall(_METRICS_KEY)
        hits   = int(raw.get("hits",   0))
        misses = int(raw.get("misses", 0))
        writes = int(raw.get("writes", 0))
        total  = hits + misses
        hit_rate = round(100 * hits / total, 1) if total else 0
        return {
            "hits": hits, "misses": misses, "writes": writes,
            "total_requests": total, "hit_rate_pct": hit_rate,
            "target_pct": 40,
        }
    except Exception as e:
        return {"error": str(e)}
