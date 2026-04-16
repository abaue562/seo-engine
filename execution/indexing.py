"""Indexing submission system — Google Indexing API + GSC + Bing IndexNow.

Google does NOT use IndexNow for general web pages.
This module calls the correct APIs for each engine.

Submission sequence per URL:
  1. Google Indexing API (fastest for new URLs)
  2. Google sitemap ping
  3. GSC URL Inspection API (request indexing)
  4. Bing IndexNow
  5. Schedule 48h verification
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlparse

log = logging.getLogger(__name__)


@dataclass
class IndexingResult:
    url: str
    google_api: bool = False
    google_sitemap_ping: bool = False
    gsc_request: bool = False
    bing_indexnow: bool = False
    submitted_at: str = ""
    errors: list[str] = field(default_factory=list)

    @property
    def any_success(self) -> bool:
        return any([self.google_api, self.google_sitemap_ping, self.bing_indexnow])


class IndexingSystem:
    """Submits URLs to Google + Bing indexing endpoints."""

    def __init__(
        self,
        gsc_credentials_path: str = "",
        indexnow_key: str = "",
        sitemap_url: str = "",
    ):
        self.gsc_credentials_path = gsc_credentials_path or os.getenv("GSC_CREDENTIALS_PATH", "")
        self.indexnow_key = indexnow_key or os.getenv("INDEXNOW_API_KEY", "")
        self.sitemap_url = sitemap_url or os.getenv("SITEMAP_URL", "")

    async def submit(self, url: str, *, ping_sitemap: bool = True) -> IndexingResult:
        """Submit URL to all indexing endpoints concurrently."""
        result = IndexingResult(url=url, submitted_at=datetime.now(tz=timezone.utc).isoformat())
        parsed = urlparse(url)
        sitemap = self.sitemap_url or f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"

        tasks = [
            self._submit_google_indexing_api(url),
            self._submit_bing_indexnow(url, parsed.netloc),
            self._request_gsc_indexing(url),
        ]
        if ping_sitemap:
            tasks.append(self._ping_google_sitemap(sitemap))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        result.google_api = bool(results[0]) if not isinstance(results[0], Exception) else False
        result.bing_indexnow = bool(results[1]) if not isinstance(results[1], Exception) else False
        result.gsc_request = bool(results[2]) if not isinstance(results[2], Exception) else False

        if ping_sitemap and len(results) > 3:
            result.google_sitemap_ping = bool(results[3]) if not isinstance(results[3], Exception) else False

        for r in results:
            if isinstance(r, Exception):
                result.errors.append(str(r))

        log.info(
            "indexing.submitted  url=%s  google_api=%s  bing=%s  gsc=%s",
            url, result.google_api, result.bing_indexnow, result.gsc_request,
        )
        return result

    async def _submit_google_indexing_api(self, url: str) -> bool:
        """Submit to Google Indexing API via service account."""
        if not self.gsc_credentials_path or not os.path.exists(self.gsc_credentials_path):
            log.debug("indexing.google_api  no credentials configured")
            return False
        try:
            import httpx
            # Load service account and get OAuth token
            creds = json.loads(open(self.gsc_credentials_path).read())
            token = await self._get_service_account_token(
                creds,
                scope="https://www.googleapis.com/auth/indexing",
            )
            if not token:
                return False

            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    "https://indexing.googleapis.com/v3/urlNotifications:publish",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={"url": url, "type": "URL_UPDATED"},
                )
                success = resp.status_code in (200, 202)
                if not success:
                    log.warning("indexing.google_api_fail  status=%d  body=%s", resp.status_code, resp.text[:200])
                return success
        except Exception as e:
            log.warning("indexing.google_api_error  err=%s", e)
            return False

    async def _ping_google_sitemap(self, sitemap_url: str) -> bool:
        """Ping Google with the sitemap URL."""
        if not sitemap_url:
            return False
        try:
            import httpx
            ping_url = f"https://www.google.com/ping?sitemap={sitemap_url}"
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(ping_url)
                return resp.status_code in (200, 201, 202)
        except Exception as e:
            log.warning("indexing.sitemap_ping_fail  err=%s", e)
            return False

    async def _submit_bing_indexnow(self, url: str, host: str) -> bool:
        """Submit to Bing via IndexNow protocol."""
        if not self.indexnow_key:
            log.debug("indexing.bing  no IndexNow key configured")
            return False
        try:
            import httpx
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    "https://api.indexnow.org/indexnow",
                    headers={"Content-Type": "application/json; charset=utf-8"},
                    json={
                        "host": host,
                        "key": self.indexnow_key,
                        "keyLocation": f"https://{host}/{self.indexnow_key}.txt",
                        "urlList": [url],
                    },
                )
                return resp.status_code in (200, 202)
        except Exception as e:
            log.warning("indexing.bing_fail  err=%s", e)
            return False

    async def _request_gsc_indexing(self, url: str) -> bool:
        """Request indexing via GSC URL Inspection API."""
        if not self.gsc_credentials_path or not os.path.exists(self.gsc_credentials_path):
            return False
        try:
            import httpx
            creds = json.loads(open(self.gsc_credentials_path).read())
            token = await self._get_service_account_token(
                creds,
                scope="https://www.googleapis.com/auth/webmasters",
            )
            if not token:
                return False

            parsed = urlparse(url)
            site_url = f"sc-domain:{parsed.netloc}"

            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    "https://searchconsole.googleapis.com/v1/urlInspection/index:inspect",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={"inspectionUrl": url, "siteUrl": site_url, "languageCode": "en"},
                )
                return resp.status_code == 200
        except Exception as e:
            log.debug("indexing.gsc_fail  err=%s", e)
            return False

    async def _get_service_account_token(self, creds: dict, scope: str) -> str | None:
        """Get OAuth2 access token from service account credentials."""
        try:
            import time
            import base64
            import json as _json
            import hashlib
            import hmac

            # Build JWT for Google OAuth
            now = int(time.time())
            header = base64.urlsafe_b64encode(
                _json.dumps({"alg": "RS256", "typ": "JWT"}).encode()
            ).rstrip(b"=")
            payload = base64.urlsafe_b64encode(
                _json.dumps({
                    "iss": creds.get("client_email", ""),
                    "scope": scope,
                    "aud": "https://oauth2.googleapis.com/token",
                    "exp": now + 3600,
                    "iat": now,
                }).encode()
            ).rstrip(b"=")

            signing_input = header + b"." + payload

            # Sign with RSA private key using cryptography library
            try:
                from cryptography.hazmat.primitives import hashes, serialization
                from cryptography.hazmat.primitives.asymmetric import padding
                from cryptography.hazmat.backends import default_backend

                private_key = serialization.load_pem_private_key(
                    creds.get("private_key", "").encode(),
                    password=None,
                    backend=default_backend(),
                )
                signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
                sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=")
                jwt_token = (signing_input + b"." + sig_b64).decode()
            except ImportError:
                log.warning("indexing.jwt  cryptography not installed")
                return None

            import httpx
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    "https://oauth2.googleapis.com/token",
                    data={
                        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                        "assertion": jwt_token,
                    },
                )
                if resp.status_code == 200:
                    return resp.json().get("access_token")
        except Exception as e:
            log.warning("indexing.jwt_fail  err=%s", e)
        return None

    async def verify_indexed(self, url: str) -> bool:
        """Check if URL is indexed via GSC URL Inspection."""
        if not self.gsc_credentials_path or not os.path.exists(self.gsc_credentials_path):
            return False
        try:
            import httpx
            creds = json.loads(open(self.gsc_credentials_path).read())
            token = await self._get_service_account_token(creds, "https://www.googleapis.com/auth/webmasters.readonly")
            if not token:
                return False
            parsed = urlparse(url)
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.post(
                    "https://searchconsole.googleapis.com/v1/urlInspection/index:inspect",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"inspectionUrl": url, "siteUrl": f"sc-domain:{parsed.netloc}", "languageCode": "en"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    verdict = data.get("inspectionResult", {}).get("indexStatusResult", {}).get("verdict", "")
                    return verdict in ("PASS", "PARTIAL")
        except Exception as e:
            log.warning("indexing.verify_fail  err=%s", e)
        return False


async def submit_url(url: str) -> IndexingResult:
    """Convenience function — submits a single URL using settings config."""
    try:
        from config.settings import GSC_CREDENTIALS_PATH, INDEXNOW_API_KEY, SITE_BASE_URL
    except ImportError:
        GSC_CREDENTIALS_PATH = ""
        INDEXNOW_API_KEY = ""
        SITE_BASE_URL = ""

    parsed = urlparse(url)
    sitemap = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"

    system = IndexingSystem(
        gsc_credentials_path=GSC_CREDENTIALS_PATH,
        indexnow_key=INDEXNOW_API_KEY,
        sitemap_url=sitemap,
    )
    return await system.submit(url)
