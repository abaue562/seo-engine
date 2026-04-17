"""SERP data quality gate for content brief generation.

Prevents generic content from being generated when SERP data is stale or
failed. Implements the freshness check and quality classification from P1-D.

Usage:
    from core.serp_gate import check_serp_quality, SerpQuality, SerpGateError

    try:
        quality = check_serp_quality(tenant_id, keyword_id, serp_data)
    except SerpGateError as e:
        # Mark keyword as serp_blocked, alert ops
        raise
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any

log = logging.getLogger(__name__)

_FRESHNESS_DAYS = 14    # SERP data older than this triggers re-scrape
_BLOCKED_THRESHOLD = 5  # Alert if > 5% of tenant keywords are serp_blocked


class SerpQuality(str, Enum):
    FULL = "full"
    PARTIAL = "partial"
    FAILED = "failed"
    STALE = "stale"


class SerpGateError(Exception):
    """Raised when SERP data is too poor to generate a useful brief."""
    def __init__(self, message: str, keyword: str = "", reason: str = "failed"):
        super().__init__(message)
        self.keyword = keyword
        self.reason = reason


def assess_serp_quality(serp_data: dict | None) -> SerpQuality:
    """Classify SERP data quality based on completeness.

    Args:
        serp_data: Dict from scrape_serp task or None.

    Returns:
        SerpQuality enum value.
    """
    if not serp_data:
        return SerpQuality.FAILED

    # Check for explicit quality field from scraper
    if "quality" in serp_data:
        q = serp_data["quality"]
        if q == "full":
            return SerpQuality.FULL
        if q == "partial":
            return SerpQuality.PARTIAL
        if q in ("failed", "error"):
            return SerpQuality.FAILED

    # Infer quality from data completeness
    results = serp_data.get("results", serp_data.get("organic_results", []))
    if not results:
        return SerpQuality.FAILED

    has_competitors = len(results) >= 5
    has_paa = bool(serp_data.get("people_also_ask") or serp_data.get("paa"))
    has_featured = "featured_snippet" in serp_data

    if has_competitors and (has_paa or has_featured):
        return SerpQuality.FULL
    if has_competitors:
        return SerpQuality.PARTIAL
    return SerpQuality.FAILED


def is_serp_stale(scraped_at: datetime | str | None) -> bool:
    """Return True if SERP data is older than _FRESHNESS_DAYS."""
    if scraped_at is None:
        return True
    if isinstance(scraped_at, str):
        try:
            scraped_at = datetime.fromisoformat(scraped_at.replace("Z", "+00:00"))
        except ValueError:
            return True
    if scraped_at.tzinfo is None:
        scraped_at = scraped_at.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - scraped_at
    return age > timedelta(days=_FRESHNESS_DAYS)


def check_serp_quality(
    tenant_id: str,
    keyword: str,
    keyword_id: str | None,
    serp_data: dict | None,
    scraped_at: datetime | str | None = None,
    strict: bool = False,
) -> SerpQuality:
    """Gate SERP data quality before brief generation.

    On FAILED: marks keyword as serp_blocked and raises SerpGateError.
    On STALE: raises SerpGateError requesting re-scrape.
    On PARTIAL: returns SerpQuality.PARTIAL (caller applies stricter validation).
    On FULL: returns SerpQuality.FULL.

    Args:
        tenant_id:  Tenant UUID.
        keyword:    Keyword string for logging.
        keyword_id: Keyword UUID (used to mark serp_blocked).
        serp_data:  Raw SERP dict from scraper, or None.
        scraped_at: When the SERP was scraped (for freshness check).
        strict:     If True, treat PARTIAL as FAILED.

    Raises:
        SerpGateError: If data is FAILED or STALE.
    """
    scraped_at = scraped_at or (serp_data.get("scraped_at") if serp_data else None)

    # Freshness check first (stale data can look full)
    if is_serp_stale(scraped_at):
        log.warning(
            "serp_gate.stale  tenant=%s  keyword=%s  age=>%dd",
            tenant_id[:8], keyword[:40], _FRESHNESS_DAYS,
        )
        raise SerpGateError(
            f"SERP data for '{keyword}' is older than {_FRESHNESS_DAYS} days",
            keyword=keyword,
            reason="stale",
        )

    quality = assess_serp_quality(serp_data)
    log.debug("serp_gate.quality  tenant=%s  keyword=%s  quality=%s", tenant_id[:8], keyword[:40], quality)

    if quality == SerpQuality.FAILED or (strict and quality == SerpQuality.PARTIAL):
        # Mark keyword as serp_blocked in PG
        if keyword_id and tenant_id:
            try:
                from core.pg import execute_write
                execute_write(
                    "UPDATE keywords SET status = %s, updated_at = NOW() WHERE id = %s AND tenant_id = %s",
                    ["serp_blocked", keyword_id, tenant_id],
                    tenant_id=tenant_id,
                )
                log.warning("serp_gate.blocked  tenant=%s  keyword=%s  kw_id=%s",
                            tenant_id[:8], keyword[:40], str(keyword_id)[:8])
            except Exception as e:
                log.warning("serp_gate.block_fail  err=%s", e)

        # Alert if blocking rate is high
        try:
            _check_block_rate(tenant_id)
        except Exception:
            pass

        raise SerpGateError(
            f"SERP data quality too low to generate useful brief for '{keyword}'",
            keyword=keyword,
            reason=quality.value,
        )

    return quality


def _check_block_rate(tenant_id: str) -> None:
    """Alert if more than _BLOCKED_THRESHOLD% of tenant keywords are serp_blocked."""
    try:
        from core.pg import execute_one
        row = execute_one(
            "SELECT "
            "  COUNT(*) FILTER (WHERE status = 'serp_blocked') AS blocked, "
            "  COUNT(*) AS total "
            "FROM keywords WHERE tenant_id = %s",
            [tenant_id],
            tenant_id=tenant_id,
        )
        if row and row[1] and row[1] > 0:
            rate = (row[0] / row[1]) * 100
            if rate > _BLOCKED_THRESHOLD:
                log.error(
                    "serp_gate.high_block_rate  tenant=%s  blocked=%d  total=%d  rate=%.1f%%",
                    tenant_id[:8], row[0], row[1], rate,
                )
    except Exception as e:
        log.debug("serp_gate.rate_check_fail  err=%s", e)
