"""API rate limiter -- token bucket per API key, plan-based limits.

Enforces per-plan RPS limits at the API gateway layer.
Returns 429 with Retry-After when bucket is empty.

Usage (FastAPI middleware / dependency):
    from core.rate_limiter import check_rate_limit, RateLimitExceeded

    try:
        check_rate_limit(api_key="key_abc", plan="growth")
    except RateLimitExceeded as e:
        return JSONResponse(
            {"error": "rate_limit_exceeded", "retry_after": e.retry_after},
            status_code=429,
            headers={"Retry-After": str(e.retry_after)},
        )
"""
from __future__ import annotations

import logging
import math
import os
import time

log = logging.getLogger(__name__)

# RPS limits per plan (sustained); burst = 2x
_PLAN_RPS: dict[str, float] = {
    "starter":    10.0,
    "growth":     30.0,
    "enterprise": 100.0,
    "agency":     100.0,
    "trial":       5.0,
    "default":    10.0,
}
_BURST_MULTIPLIER = 2.0
_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


class RateLimitExceeded(Exception):
    """Raised when an API key's rate limit is exceeded."""
    def __init__(self, message: str, retry_after: int = 1, limit: float = 10.0, remaining: int = 0):
        super().__init__(message)
        self.retry_after = retry_after
        self.limit = limit
        self.remaining = remaining


def _get_redis():
    import redis
    return redis.from_url(_REDIS_URL, decode_responses=True, socket_timeout=2)


def check_rate_limit(
    api_key: str,
    plan: str = "starter",
    weight: float = 1.0,
) -> dict:
    """Check and consume tokens from the token bucket for api_key.

    Uses Redis sliding window with token bucket algorithm.
    Thread-safe via atomic Lua script.

    Args:
        api_key: The API key making the request.
        plan:    Plan name (starter/growth/enterprise/agency/trial).
        weight:  Token cost of this request (default 1.0).

    Returns:
        dict with keys: allowed, remaining, reset_in, limit.

    Raises:
        RateLimitExceeded: If the bucket is empty.
    """
    rps = _PLAN_RPS.get(plan.lower(), _PLAN_RPS["default"])
    burst = rps * _BURST_MULTIPLIER
    window = 1.0  # 1-second sliding window

    key = f"rl:bucket:{api_key}"
    now = time.time()

    # Lua script for atomic token bucket
    # Tokens refill at rps/sec up to burst capacity
    LUA_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local rps = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local weight = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1]) or burst
local last_refill = tonumber(bucket[2]) or now

-- Refill tokens based on time elapsed
local elapsed = now - last_refill
local refilled = math.min(burst, tokens + elapsed * rps)

-- Check if request can proceed
local allowed = 0
local remaining = math.floor(refilled)
if refilled >= weight then
    allowed = 1
    refilled = refilled - weight
    remaining = math.floor(refilled)
end

-- Update bucket
redis.call('HMSET', key, 'tokens', tostring(refilled), 'last_refill', tostring(now))
redis.call('EXPIRE', key, 60)

return {allowed, remaining, math.ceil(weight / rps)}
"""

    try:
        r = _get_redis()
        result = r.eval(LUA_SCRIPT, 1, key, now, rps, burst, weight)
        allowed, remaining, retry_after = int(result[0]), int(result[1]), int(result[2])

        if not allowed:
            log.warning(
                "rate_limit.exceeded  key=%s  plan=%s  rps=%.0f",
                api_key[:12], plan, rps,
            )
            raise RateLimitExceeded(
                f"Rate limit exceeded for plan {plan!r}: {rps:.0f} RPS",
                retry_after=max(1, retry_after),
                limit=rps,
                remaining=0,
            )

        return {"allowed": True, "remaining": remaining, "limit": rps, "reset_in": retry_after}

    except RateLimitExceeded:
        raise
    except Exception as e:
        log.warning("rate_limiter.redis_fail  err=%s  (allowing)", e)
        # Fail open: if Redis is down, allow the request
        return {"allowed": True, "remaining": -1, "limit": rps, "reset_in": 1}


def get_plan_limits(plan: str) -> dict:
    """Return the rate limit configuration for a plan."""
    rps = _PLAN_RPS.get(plan.lower(), _PLAN_RPS["default"])
    return {
        "plan": plan,
        "requests_per_second": rps,
        "burst": int(rps * _BURST_MULTIPLIER),
        "requests_per_minute": int(rps * 60),
        "requests_per_hour": int(rps * 3600),
    }
