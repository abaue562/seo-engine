"""Ahrefs API v3 connector.

Docs: https://developers.ahrefs.com/api/v3

Env var: AHREFS_API_TOKEN

Supported on Lite plan and above:
  - domain_rating
  - backlinks / referring_domains
  - organic_keywords
  - top_pages

Note on API shape: Ahrefs v3 uses GET requests with query params.
All endpoints return JSON with a top-level key matching the resource name.
Example: GET /site-explorer/domain-rating → {"domain_rating": {"domain": ..., "ahrefs_rank": ..., "domain_rating": ...}}
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import httpx

log = logging.getLogger(__name__)


class AhrefsClient:
    """Ahrefs API v3 client.

    Authentication: Bearer token in Authorization header.
    All methods make synchronous GET requests and return plain dicts/lists.
    """

    BASE_URL = "https://api.ahrefs.com/v3"

    def __init__(self, api_token: str = ""):
        self.token = api_token or os.getenv("AHREFS_API_TOKEN", "")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        self.client = httpx.Client(timeout=30)

    # ── Low-level helper ───────────────────────────────────────────────────────

    def _get(self, endpoint: str, params: dict) -> dict:
        """Authenticated GET request. Returns parsed JSON or raises on error.

        Ahrefs v3 returns HTTP 200 for success, 4xx/5xx for errors.
        Error bodies: {"error": {"code": "...", "message": "..."}}
        """
        if not self.token:
            log.warning("ahrefs.not_configured  endpoint=%s", endpoint)
            raise ValueError("AHREFS_API_TOKEN is not set")

        url = f"{self.BASE_URL}/{endpoint}"
        try:
            resp = self.client.get(url, headers=self.headers, params=params)
        except httpx.RequestError as exc:
            log.error("ahrefs.request_error  endpoint=%s  err=%s", endpoint, exc)
            raise ValueError(f"Ahrefs request failed: {exc}") from exc

        if resp.status_code != 200:
            try:
                err_body = resp.json()
                err_msg  = err_body.get("error", {}).get("message", resp.text[:200])
            except Exception:
                err_msg = resp.text[:200]
            log.error("ahrefs.http_error  endpoint=%s  status=%s  msg=%s",
                      endpoint, resp.status_code, err_msg)
            raise ValueError(f"Ahrefs HTTP {resp.status_code}: {err_msg}")

        return resp.json()

    # ── Domain Rating ──────────────────────────────────────────────────────────

    def domain_rating(self, domain: str) -> dict:
        """Get Domain Rating for a domain.

        GET /site-explorer/domain-rating?target={domain}&mode=domain

        Returns:
            {domain, domain_rating, ahrefs_rank}
        """
        try:
            body = self._get(
                "site-explorer/domain-rating",
                params={"target": domain, "mode": "domain"},
            )
        except ValueError as exc:
            log.error("ahrefs.domain_rating  domain=%s  err=%s", domain, exc)
            return {"domain": domain, "domain_rating": 0, "ahrefs_rank": 0}

        # Response: {"domain_rating": {"domain": "...", "ahrefs_rank": N, "domain_rating": N}}
        dr_data = body.get("domain_rating", {}) or {}
        result = {
            "domain":        dr_data.get("domain", domain),
            "domain_rating": dr_data.get("domain_rating", 0),
            "ahrefs_rank":   dr_data.get("ahrefs_rank", 0),
        }
        log.info("ahrefs.domain_rating  domain=%s  dr=%s", domain, result["domain_rating"])
        return result

    # ── Backlinks Stats ────────────────────────────────────────────────────────

    def backlinks_stats(self, domain: str) -> dict:
        """Backlink overview stats.

        GET /site-explorer/backlinks-stats?target={domain}&mode=domain

        Returns:
            {live_refdomains, live_backlinks, domain_rating}
        """
        try:
            body = self._get(
                "site-explorer/backlinks-stats",
                params={"target": domain, "mode": "domain"},
            )
        except ValueError as exc:
            log.error("ahrefs.backlinks_stats  domain=%s  err=%s", domain, exc)
            return {"live_refdomains": 0, "live_backlinks": 0, "domain_rating": 0}

        # Response: {"stats": {"live_refdomains": N, "live_backlinks": N, ...}}
        stats = body.get("stats", {}) or {}
        result = {
            "live_refdomains": stats.get("live_refdomains", 0),
            "live_backlinks":  stats.get("live_backlinks", 0),
            "domain_rating":   stats.get("domain_rating", 0),
        }
        log.info("ahrefs.backlinks_stats  domain=%s  backlinks=%s  refdomains=%s",
                 domain, result["live_backlinks"], result["live_refdomains"])
        return result

    # ── Top Pages ─────────────────────────────────────────────────────────────

    def top_pages(self, domain: str, limit: int = 50) -> list[dict]:
        """Top organic pages by estimated traffic.

        GET /site-explorer/top-pages?target={domain}&mode=domain&limit={limit}

        Returns list of:
            {url, traffic, top_keyword, position, traffic_value}
        """
        try:
            body = self._get(
                "site-explorer/top-pages",
                params={
                    "target": domain,
                    "mode":   "domain",
                    "limit":  limit,
                    "select": "url,traffic,top_keyword,pos,traffic_value",
                },
            )
        except ValueError as exc:
            log.error("ahrefs.top_pages  domain=%s  err=%s", domain, exc)
            return []

        # Response: {"pages": [{"url": ..., "traffic": ..., ...}]}
        pages = body.get("pages", []) or []
        results = [
            {
                "url":           p.get("url", ""),
                "traffic":       p.get("traffic", 0),
                "top_keyword":   p.get("top_keyword", ""),
                "position":      p.get("pos", 0),
                "traffic_value": p.get("traffic_value", 0.0),
            }
            for p in pages
        ]
        log.info("ahrefs.top_pages  domain=%s  count=%d", domain, len(results))
        return results

    # ── Organic Keywords ──────────────────────────────────────────────────────

    def organic_keywords(self, domain: str, limit: int = 100) -> list[dict]:
        """Top organic keywords for a domain.

        GET /site-explorer/organic-keywords?target={domain}&mode=domain&limit={limit}

        Returns list of:
            {keyword, position, traffic, volume, difficulty}
        """
        try:
            body = self._get(
                "site-explorer/organic-keywords",
                params={
                    "target":   domain,
                    "mode":     "domain",
                    "limit":    limit,
                    "country":  "us",
                    "select":   "keyword,pos,traffic,volume,keyword_difficulty",
                },
            )
        except ValueError as exc:
            log.error("ahrefs.organic_keywords  domain=%s  err=%s", domain, exc)
            return []

        # Response: {"keywords": [{...}]}
        keywords = body.get("keywords", []) or []
        results = [
            {
                "keyword":    kw.get("keyword", ""),
                "position":   kw.get("pos", 0),
                "traffic":    kw.get("traffic", 0),
                "volume":     kw.get("volume", 0),
                "difficulty": kw.get("keyword_difficulty", 0),
            }
            for kw in keywords
        ]
        log.info("ahrefs.organic_keywords  domain=%s  count=%d", domain, len(results))
        return results

    # ── Referring Domains ─────────────────────────────────────────────────────

    def referring_domains(self, domain: str, limit: int = 100) -> list[dict]:
        """Referring domains with Domain Rating.

        GET /site-explorer/refdomains?target={domain}&mode=domain&limit={limit}

        Returns list of:
            {domain, domain_rating, backlinks_count, first_seen}
        """
        try:
            body = self._get(
                "site-explorer/refdomains",
                params={
                    "target": domain,
                    "mode":   "domain",
                    "limit":  limit,
                    "select": "domain,domain_rating,backlinks,first_seen",
                },
            )
        except ValueError as exc:
            log.error("ahrefs.referring_domains  domain=%s  err=%s", domain, exc)
            return []

        # Response: {"refdomains": [{...}]}
        refdomains = body.get("refdomains", []) or []
        results = [
            {
                "domain":          rd.get("domain", ""),
                "domain_rating":   rd.get("domain_rating", 0),
                "backlinks_count": rd.get("backlinks", 0),
                "first_seen":      rd.get("first_seen", ""),
            }
            for rd in refdomains
        ]
        log.info("ahrefs.referring_domains  domain=%s  count=%d", domain, len(results))
        return results

    # ── Competitor Domains ────────────────────────────────────────────────────

    def competitor_domains(self, domain: str, limit: int = 10) -> list[dict]:
        """Organic search competitors sorted by common keyword overlap.

        GET /site-explorer/competing-domains?target={domain}&mode=domain

        Returns list of:
            {domain, common_keywords, organic_keywords, domain_rating}
        """
        try:
            body = self._get(
                "site-explorer/competing-domains",
                params={
                    "target": domain,
                    "mode":   "domain",
                    "limit":  limit,
                    "select": "domain,common_keywords,organic_keywords,domain_rating",
                },
            )
        except ValueError as exc:
            log.error("ahrefs.competitor_domains  domain=%s  err=%s", domain, exc)
            return []

        # Response: {"domains": [{...}]}
        domains = body.get("domains", []) or []
        results = [
            {
                "domain":           d.get("domain", ""),
                "common_keywords":  d.get("common_keywords", 0),
                "organic_keywords": d.get("organic_keywords", 0),
                "domain_rating":    d.get("domain_rating", 0),
            }
            for d in domains
        ]
        log.info("ahrefs.competitor_domains  domain=%s  count=%d", domain, len(results))
        return results

    # ── Availability check ────────────────────────────────────────────────────

    def is_available(self) -> bool:
        """Return True if an API token is configured.

        Does not make a network call — just checks for token presence.
        For a live connectivity check use domain_rating() and catch ValueError.
        """
        return bool(self.token)
