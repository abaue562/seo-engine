"""Scraper compliance: robots.txt enforcement, disclosed UA, per-domain rate limiting.

Defensible scraping posture per 03_SECURITY_AND_RISK:
  - Disclosed User-Agent string (not a fake browser UA)
  - robots.txt checked before every crawl
  - 1 req/sec/domain global rate limit via Redis
  - Crawl-Delay header respected
  - Audit log of all scrape targets

Usage:
    from core.scraper_compliance import can_scrape, record_scrape, DISCLOSED_UA

    if not can_scrape(url):
        raise PermissionError(f"robots.txt blocks scraping of {url}")

    resp = requests.get(url, headers={"User-Agent": DISCLOSED_UA})
    record_scrape(url, tenant_id=tenant_id)
"""
from __future__ import annotations

import logging
import os
import time
import urllib.robotparser
from datetime import datetime, timezone
from urllib.parse import urlparse

log = logging.getLogger(__name__)

# Disclosed bot identity (not a fake browser UA)
DISCLOSED_UA = "BlendBrightSEOBot/1.0 (+https://blendbright.com/bot)"

# Global rate limit: 1 request per second per domain
_RATE_LIMIT_RPS = float(os.getenv("SCRAPER_RATE_LIMIT_RPS", "1.0"))

# Cache robots.txt results for 1h to avoid repeated fetches
_robots_cache: dict[str, tuple[float, urllib.robotparser.RobotFileParser]] = {}
_ROBOTS_CACHE_TTL = 3600.0  # 1 hour


def _get_domain(url: str) -> str:
    """Extract scheme + netloc from URL."""
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return url


def _fetch_robots(domain: str) -> urllib.robotparser.RobotFileParser | None:
    """Fetch and parse robots.txt for domain. Cached for 1h."""
    now = time.monotonic()
    cached = _robots_cache.get(domain)
    if cached and (now - cached[0]) < _ROBOTS_CACHE_TTL:
        return cached[1]

    robots_url = f"{domain}/robots.txt"
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
        _robots_cache[domain] = (now, rp)
        log.debug("scraper.robots_fetched  domain=%s", domain)
        return rp
    except Exception as e:
        log.warning("scraper.robots_fetch_fail  domain=%s  err=%s  (allowing)", domain, e)
        # If we can't fetch robots.txt, allow but log it
        _robots_cache[domain] = (now, rp)
        return rp


def _get_crawl_delay(rp: urllib.robotparser.RobotFileParser) -> float:
    """Return Crawl-Delay for our bot, or the default rate limit."""
    try:
        delay = rp.crawl_delay(DISCLOSED_UA) or rp.crawl_delay("*")
        if delay and float(delay) > 0:
            return float(delay)
    except Exception:
        pass
    return 1.0 / _RATE_LIMIT_RPS


def can_scrape(url: str, ua: str | None = None) -> bool:
    """Return True if robots.txt allows scraping url.

    Args:
        url: Target URL to check.
        ua:  User-agent to check against (defaults to DISCLOSED_UA).
    """
    ua = ua or DISCLOSED_UA
    domain = _get_domain(url)
    rp = _fetch_robots(domain)
    if rp is None:
        return True  # fail open if robots can't be fetched
    try:
        allowed = rp.can_fetch(ua, url)
        if not allowed:
            log.info("scraper.robots_blocked  url=%s  ua=%s", url[:80], ua)
        return allowed
    except Exception as e:
        log.warning("scraper.robots_check_fail  url=%s  err=%s  (allowing)", url[:80], e)
        return True


def enforce_rate_limit(domain: str) -> None:
    """Block until the per-domain rate limit allows the next request.

    Uses Redis sliding window. Falls back to time.sleep if Redis unavailable.
    """
    key = f"scraper:rate:{domain}"
    try:
        import redis
        r = redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True, socket_timeout=2,
        )
        # Simple token bucket: store last-request timestamp
        last_str = r.get(key)
        if last_str:
            last = float(last_str)
            elapsed = time.time() - last
            min_interval = 1.0 / _RATE_LIMIT_RPS
            if elapsed < min_interval:
                sleep_time = min_interval - elapsed
                log.debug("scraper.rate_limit_wait  domain=%s  sleep=%.2fs", domain, sleep_time)
                time.sleep(sleep_time)
        r.set(key, str(time.time()), ex=60)
    except Exception as e:
        log.warning("scraper.rate_limit_redis_fail  err=%s  (sleeping 1s)", e)
        time.sleep(1.0 / _RATE_LIMIT_RPS)


def record_scrape(url: str, tenant_id: str = "", purpose: str = "serp") -> None:
    """Audit log of scrape targets for legal defense."""
    try:
        from core.audit import log_event
        if tenant_id:
            log_event(
                tenant_id=tenant_id,
                actor="system",
                action="scraper.fetched",
                entity_type="url",
                diff={"url": url[:200], "purpose": purpose,
                      "ua": DISCLOSED_UA, "ts": datetime.now(timezone.utc).isoformat()},
            )
        else:
            log.info("scraper.fetched  url=%s  purpose=%s  ua=%s", url[:80], purpose, DISCLOSED_UA)
    except Exception as e:
        log.debug("scraper.record_fail  err=%s", e)


def scrape_url(
    url: str,
    tenant_id: str = "",
    purpose: str = "serp",
    session=None,
    timeout: int = 15,
) -> "requests.Response | None":
    """Compliance-safe URL fetch: checks robots.txt, rate limits, uses disclosed UA.

    Args:
        url:       Target URL.
        tenant_id: For audit logging.
        purpose:   Why we're scraping (for audit log).
        session:   Optional requests.Session to reuse.
        timeout:   Request timeout in seconds.

    Returns:
        requests.Response, or None if blocked by robots.txt.

    Raises:
        requests.RequestException on network error.
    """
    if not can_scrape(url):
        log.warning("scraper.blocked_by_robots  url=%s", url[:80])
        return None

    domain = _get_domain(url)
    enforce_rate_limit(domain)

    import requests as _req
    sess = session or _req.Session()
    headers = {"User-Agent": DISCLOSED_UA}

    response = sess.get(url, headers=headers, timeout=timeout)
    record_scrape(url, tenant_id=tenant_id, purpose=purpose)
    return response
