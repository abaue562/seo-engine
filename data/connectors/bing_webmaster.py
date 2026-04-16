"""Bing Webmaster Tools API connector.

Docs: https://learn.microsoft.com/en-us/bingwebmaster/getting-access

Env var: BING_WEBMASTER_API_KEY

Used for:
  - Submitting URLs for Bing indexing (single or batch)
  - IndexNow protocol submissions (instant indexing signal to Bing/Yandex)
  - Crawl statistics
  - Bing keyword research data
  - Site verification status

API base: https://ssl.bing.com/webmaster/api.svc/json
API key is sent as a query param: ?apikey={key}

IndexNow base: https://www.bing.com/indexnow
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import httpx

log = logging.getLogger(__name__)


class BingWebmasterClient:
    """Bing Webmaster Tools API client.

    All Bing Webmaster API endpoints accept the API key as a query param.
    Responses are JSON with a ``d`` key wrapping the actual payload for
    most endpoints (WCF-style JSON), e.g.:
        {"d": {"CrawlInfoSummary": {...}, ...}}
    Some endpoints (like SubmitUrl) return HTTP 200 with no body on success.
    """

    BASE_URL     = "https://ssl.bing.com/webmaster/api.svc/json"
    INDEXNOW_URL = "https://www.bing.com/indexnow"

    def __init__(self, api_key: str = ""):
        self.api_key = api_key or os.getenv("BING_WEBMASTER_API_KEY", "")
        self.client  = httpx.Client(timeout=30)

    # ── Low-level helper ───────────────────────────────────────────────────────

    def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Authenticated request with api_key injected as query param.

        On success returns parsed JSON (or empty dict for 200-no-body).
        Raises ValueError on HTTP or API-level errors.
        """
        if not self.api_key:
            log.warning("bing_webmaster.not_configured  endpoint=%s", endpoint)
            raise ValueError("BING_WEBMASTER_API_KEY is not set")

        # Inject api key into query params
        params = kwargs.pop("params", {}) or {}
        params["apikey"] = self.api_key

        url = f"{self.BASE_URL}/{endpoint}"
        try:
            resp = self.client.request(method, url, params=params, **kwargs)
        except httpx.RequestError as exc:
            log.error("bing_webmaster.request_error  endpoint=%s  err=%s", endpoint, exc)
            raise ValueError(f"Bing Webmaster request failed: {exc}") from exc

        if resp.status_code not in (200, 204):
            log.error("bing_webmaster.http_error  endpoint=%s  status=%s  body=%s",
                      endpoint, resp.status_code, resp.text[:200])
            raise ValueError(f"Bing Webmaster HTTP {resp.status_code}: {resp.text[:200]}")

        if not resp.content:
            return {}

        try:
            return resp.json()
        except Exception:
            # Some endpoints return plain text on success
            return {"raw": resp.text}

    # ── URL submission ─────────────────────────────────────────────────────────

    def submit_url(self, site_url: str, url: str) -> bool:
        """Submit a single URL for Bing indexing.

        POST /SubmitUrl?apikey={key}
        Body: {"siteUrl": site_url, "url": url}

        Returns True on success, False on failure.
        """
        try:
            self._request(
                "POST",
                "SubmitUrl",
                json={"siteUrl": site_url, "url": url},
            )
            log.info("bing_webmaster.submit_url  url=%s  ok=True", url)
            return True
        except ValueError as exc:
            log.error("bing_webmaster.submit_url  url=%s  err=%s", url, exc)
            return False

    def submit_url_batch(self, site_url: str, urls: list[str]) -> dict:
        """Submit a batch of URLs for Bing indexing (max 500 per call).

        POST /SubmitUrlBatch?apikey={key}
        Body: {"siteUrl": site_url, "urlList": [...]}

        Returns {submitted: int, failed: int}.
        """
        # Bing's batch endpoint accepts at most 500 URLs at a time
        MAX_BATCH = 500
        submitted = 0
        failed    = 0

        for i in range(0, len(urls), MAX_BATCH):
            batch = urls[i : i + MAX_BATCH]
            try:
                self._request(
                    "POST",
                    "SubmitUrlBatch",
                    json={"siteUrl": site_url, "urlList": batch},
                )
                submitted += len(batch)
                log.info("bing_webmaster.submit_url_batch  site=%s  batch_size=%d  ok=True",
                         site_url, len(batch))
            except ValueError as exc:
                failed += len(batch)
                log.error("bing_webmaster.submit_url_batch  site=%s  batch_size=%d  err=%s",
                          site_url, len(batch), exc)

        return {"submitted": submitted, "failed": failed}

    # ── IndexNow ───────────────────────────────────────────────────────────────

    def indexnow_submit(self, host: str, urls: list[str], key: str) -> bool:
        """Submit URLs via the IndexNow protocol to Bing.

        POST https://www.bing.com/indexnow
        Body: {host, key, keyLocation, urlList}

        The IndexNow key file must be hosted at https://{host}/{key}.txt
        (keyLocation defaults to that path).

        Returns True on success (HTTP 200/202), False otherwise.
        """
        key_location = f"https://{host}/{key}.txt"
        payload = {
            "host":        host,
            "key":         key,
            "keyLocation": key_location,
            "urlList":     urls,
        }

        try:
            resp = self.client.post(
                self.INDEXNOW_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
        except httpx.RequestError as exc:
            log.error("bing_webmaster.indexnow  host=%s  err=%s", host, exc)
            return False

        # IndexNow returns 200 or 202 on success, 400/422 on bad input,
        # 429 on rate limit, 403 if key validation fails
        if resp.status_code in (200, 202):
            log.info("bing_webmaster.indexnow  host=%s  urls=%d  status=%s",
                     host, len(urls), resp.status_code)
            return True

        log.error("bing_webmaster.indexnow  host=%s  status=%s  body=%s",
                  host, resp.status_code, resp.text[:200])
        return False

    # ── Crawl stats ────────────────────────────────────────────────────────────

    def get_crawl_stats(self, site_url: str) -> dict:
        """Get crawl statistics for a site.

        GET /GetCrawlStats?apikey={key}&siteUrl={site_url}

        Response shape (WCF JSON):
            {"d": {"CrawlInfoSummary": {"AllCrawled": N, "InIndex": N, "Blocked": N, "CrawlErrors": N}}}

        Returns:
            {all_crawled, in_index, blocked, errors}
        """
        try:
            body = self._request("GET", "GetCrawlStats", params={"siteUrl": site_url})
        except ValueError as exc:
            log.error("bing_webmaster.crawl_stats  site=%s  err=%s", site_url, exc)
            return {"all_crawled": 0, "in_index": 0, "blocked": 0, "errors": 0}

        # Unwrap WCF envelope
        data = body.get("d", body)
        summary = data.get("CrawlInfoSummary", data) if isinstance(data, dict) else {}

        result = {
            "all_crawled": summary.get("AllCrawled", 0),
            "in_index":    summary.get("InIndex", 0),
            "blocked":     summary.get("Blocked", 0),
            "errors":      summary.get("CrawlErrors", 0),
        }
        log.info("bing_webmaster.crawl_stats  site=%s  crawled=%d  in_index=%d",
                 site_url, result["all_crawled"], result["in_index"])
        return result

    # ── Keyword data ───────────────────────────────────────────────────────────

    def get_keyword_data(self, query: str, country: str = "us") -> dict:
        """Get Bing keyword search statistics.

        GET /GetKeywordStats?apikey={key}&query={query}&country={country}

        Response shape (WCF JSON):
            {"d": {"Impressions": N, "Clicks": N, "AvgPosition": N}}

        Returns:
            {query, impressions, clicks, avg_position}
        """
        try:
            body = self._request(
                "GET",
                "GetKeywordStats",
                params={"query": query, "country": country},
            )
        except ValueError as exc:
            log.error("bing_webmaster.keyword_data  query=%s  err=%s", query, exc)
            return {"query": query, "impressions": 0, "clicks": 0, "avg_position": 0}

        data = body.get("d", body)
        if isinstance(data, dict):
            result = {
                "query":        query,
                "impressions":  data.get("Impressions", 0),
                "clicks":       data.get("Clicks", 0),
                "avg_position": data.get("AvgPosition", 0),
            }
        else:
            result = {"query": query, "impressions": 0, "clicks": 0, "avg_position": 0}

        log.info("bing_webmaster.keyword_data  query=%s  impressions=%d",
                 query, result["impressions"])
        return result

    # ── Site verification ──────────────────────────────────────────────────────

    def verify_site(self, site_url: str) -> dict:
        """Check site verification status.

        GET /VerifySite?apikey={key}&siteUrl={site_url}

        Response shape (WCF JSON):
            {"d": {"Verified": true, "VerificationMethod": "..."}}

        Returns:
            {verified: bool, verification_method: str}
        """
        try:
            body = self._request("GET", "VerifySite", params={"siteUrl": site_url})
        except ValueError as exc:
            log.error("bing_webmaster.verify_site  site=%s  err=%s", site_url, exc)
            return {"verified": False, "verification_method": ""}

        data = body.get("d", body)
        result = {
            "verified":             bool(data.get("Verified", False)) if isinstance(data, dict) else False,
            "verification_method":  data.get("VerificationMethod", "") if isinstance(data, dict) else "",
        }
        log.info("bing_webmaster.verify_site  site=%s  verified=%s", site_url, result["verified"])
        return result

    # ── Availability check ────────────────────────────────────────────────────

    def is_available(self) -> bool:
        """Return True if an API key is configured."""
        return bool(self.api_key)
