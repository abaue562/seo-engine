"""WordPress connector — publishes blog posts and pages via REST API.

Capabilities
------------
- Publish posts / pages with title, content, slug, excerpt, status
- Inject JSON-LD schema into post meta (Yoast SEO / RankMath / custom field)
- Set canonical URL via Yoast SEO REST API field
- Set meta title + meta description via Yoast SEO / RankMath meta fields
- Request Google indexing via Search Console URL Inspection API after publish
- Verify post is live after publish
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import httpx

from execution.connectors.base import Connector, PublishResult

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Yoast SEO field names (Yoast injects these via WP REST API)
# ---------------------------------------------------------------------------
_YOAST_FIELDS = {
    "meta_title":       "yoast_wpseo_title",
    "meta_description": "yoast_wpseo_metadesc",
    "canonical":        "yoast_wpseo_canonical",
    "schema":           "yoast_wpseo_schema_page_type",  # less useful but available
}

# RankMath field names (alternative plugin)
_RANKMATH_FIELDS = {
    "meta_title":       "rank_math_title",
    "meta_description": "rank_math_description",
    "canonical":        "rank_math_canonical_url",
}


class WordPressConnector(Connector):
    platform = "wordpress"

    def __init__(self, base_url: str, username: str, app_password: str):
        self.base_url = base_url.rstrip("/")
        self.auth = (username, app_password)
        self.api = f"{self.base_url}/wp-json/wp/v2"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def publish(self, payload: dict) -> PublishResult:
        """Publish a post or page to WordPress.

        Payload keys
        ------------
        type            : "posts" | "pages"  (default: "posts")
        status          : "draft" | "publish"  (default: "draft")
        title           : str
        content         : str  (HTML body — will have schema appended)
        slug            : str
        excerpt         : str
        meta_title      : str  (50-60 chars, for Yoast/RankMath title tag)
        meta_description: str  (150-160 chars)
        canonical_url   : str  (full canonical URL)
        schema_json     : dict | str  (JSON-LD object — injected as post meta + <script> block)
        categories      : list[int]  (WP category IDs)
        tags            : list[int]  (WP tag IDs)
        featured_media  : int  (attachment ID)
        author_id       : int  (WP user ID for author attribution)
        """
        post_type = payload.get("type", "posts")
        status = payload.get("status", os.getenv("WP_PUBLISH_STATUS", "draft"))

        # ── Build content HTML (append JSON-LD schema as inline <script>) ──
        content_html = payload.get("content", "")
        schema_json = payload.get("schema_json")
        if schema_json:
            content_html = _append_schema_script(content_html, schema_json)

        # ── Build request body ──────────────────────────────────────────────
        body: dict[str, Any] = {
            "title":   payload.get("title", ""),
            "content": content_html,
            "status":  status,
        }
        if payload.get("slug"):
            body["slug"] = payload["slug"]
        if payload.get("excerpt"):
            body["excerpt"] = payload["excerpt"]
        if payload.get("categories"):
            body["categories"] = payload["categories"]
        if payload.get("tags"):
            body["tags"] = payload["tags"]
        if payload.get("featured_media"):
            body["featured_media"] = payload["featured_media"]
        if payload.get("author_id"):
            body["author"] = payload["author_id"]

        # ── Yoast SEO meta fields (sent as top-level WP REST keys) ─────────
        meta: dict[str, str] = {}
        if payload.get("meta_title"):
            meta[_YOAST_FIELDS["meta_title"]]       = payload["meta_title"]
            meta[_RANKMATH_FIELDS["meta_title"]]    = payload["meta_title"]
        if payload.get("meta_description"):
            meta[_YOAST_FIELDS["meta_description"]] = payload["meta_description"]
            meta[_RANKMATH_FIELDS["meta_description"]] = payload["meta_description"]
        if payload.get("canonical_url"):
            meta[_YOAST_FIELDS["canonical"]]        = payload["canonical_url"]
            meta[_RANKMATH_FIELDS["canonical"]]     = payload["canonical_url"]
        if schema_json:
            # Store raw schema in a custom meta field for retrieval/validation
            schema_str = schema_json if isinstance(schema_json, str) else json.dumps(schema_json)
            meta["_seo_schema_json"] = schema_str

        if meta:
            body["meta"] = meta

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{self.api}/{post_type}",
                    auth=self.auth,
                    json=body,
                )
                resp.raise_for_status()
                data = resp.json()

            post_id = str(data.get("id", ""))
            live_url = data.get("link", "")

            log.info(
                "wp.published  id=%s  type=%s  status=%s  url=%s",
                post_id, post_type, status, live_url,
            )

            return PublishResult(
                platform="wordpress",
                status="success",
                url=live_url,
                post_id=post_id,
            )

        except Exception as e:
            log.error("wp.publish_fail  err=%s", e)
            return PublishResult(platform="wordpress", status="failed", error=str(e))

    async def verify(self, result: PublishResult) -> bool:
        """Confirm the published post is accessible via HTTP."""
        if not result.url:
            return False
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                resp = await client.get(result.url)
            ok = resp.status_code == 200
            if not ok:
                log.warning("wp.verify_fail  url=%s  status=%d", result.url, resp.status_code)
            return ok
        except Exception as e:
            log.warning("wp.verify_error  url=%s  err=%s", result.url, e)
            return False

    async def update_post(self, post_id: str, payload: dict) -> bool:
        """PATCH an existing post — used by link injector and content refresh."""
        body: dict[str, Any] = {}
        if "content" in payload:
            body["content"] = payload["content"]
        if "meta_title" in payload:
            body["meta"] = body.get("meta", {})
            body["meta"][_YOAST_FIELDS["meta_title"]]    = payload["meta_title"]
            body["meta"][_RANKMATH_FIELDS["meta_title"]] = payload["meta_title"]
        if "meta_description" in payload:
            body.setdefault("meta", {})[_YOAST_FIELDS["meta_description"]] = payload["meta_description"]
        if "schema_json" in payload:
            schema_str = (
                payload["schema_json"]
                if isinstance(payload["schema_json"], str)
                else json.dumps(payload["schema_json"])
            )
            body.setdefault("meta", {})["_seo_schema_json"] = schema_str

        if not body:
            return True  # nothing to update

        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.patch(
                    f"{self.api}/posts/{post_id}",
                    auth=self.auth,
                    json=body,
                )
                resp.raise_for_status()
            log.info("wp.update_ok  post_id=%s", post_id)
            return True
        except Exception as e:
            log.warning("wp.update_fail  post_id=%s  err=%s", post_id, e)
            return False

    async def fetch_post(self, post_id: str) -> dict:
        """Retrieve a post by ID."""
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    f"{self.api}/posts/{post_id}",
                    auth=self.auth,
                )
                resp.raise_for_status()
                return resp.json()
        except Exception as e:
            log.warning("wp.fetch_fail  post_id=%s  err=%s", post_id, e)
            return {}

    async def request_google_indexing(self, url: str) -> bool:
        """Submit a URL for immediate Google indexing via Search Console URL Inspection API.

        Requires:
            GSC_CREDENTIALS_PATH env var pointing to a service-account JSON key with
            Search Console API access + "https://www.googleapis.com/auth/webmasters" scope.

        Returns True on success, False if not configured or on error.
        """
        credentials_path = os.getenv("GSC_CREDENTIALS_PATH", "")
        if not credentials_path or not os.path.isfile(credentials_path):
            log.debug("wp.gsc_index_skip  reason=no_credentials")
            return False

        try:
            from google.oauth2 import service_account
            from google.auth.transport.requests import Request

            creds = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/webmasters"],
            )
            creds.refresh(Request())
            token = creds.token

            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    "https://searchconsoleapi.googleapis.com/v1/urlInspection/index:inspect",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "inspectionUrl": url,
                        "siteUrl": self.base_url,
                        "languageCode": "en",
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                verdict = data.get("urlInspectionResult", {}).get("indexStatusResult", {}).get("coverageState", "")
                log.info("wp.gsc_index_requested  url=%s  verdict=%s", url, verdict)
                return True

        except ImportError:
            log.debug("wp.gsc_index_skip  reason=google-auth-not-installed")
            return False
        except Exception as e:
            log.warning("wp.gsc_index_fail  url=%s  err=%s", url, e)
            return False

    async def submit_sitemap(self, sitemap_url: str) -> bool:
        """Submit sitemap to Bing Webmaster (and log for manual GSC submission)."""
        bing_key = os.getenv("BING_WEBMASTER_API_KEY", "")
        if not bing_key:
            log.debug("wp.sitemap_bing_skip  reason=no_key")
            return False

        site_url = self.base_url
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.get(
                    "https://ssl.bing.com/webmaster/api.svc/json/SubmitSitemap",
                    params={
                        "apikey": bing_key,
                        "siteUrl": site_url,
                        "sitemapUrl": sitemap_url,
                    },
                )
                resp.raise_for_status()
            log.info("wp.sitemap_submitted  sitemap=%s", sitemap_url)
            return True
        except Exception as e:
            log.warning("wp.sitemap_fail  sitemap=%s  err=%s", sitemap_url, e)
            return False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _append_schema_script(content_html: str, schema_json: dict | str) -> str:
    """Append a JSON-LD <script> block to the end of the HTML content."""
    if isinstance(schema_json, dict):
        schema_str = json.dumps(schema_json, indent=2)
    elif isinstance(schema_json, str):
        # Validate it's real JSON
        try:
            json.loads(schema_json)
            schema_str = schema_json
        except json.JSONDecodeError:
            log.warning("wp.schema_invalid_json  skipping_injection")
            return content_html
    else:
        return content_html

    script_block = f'\n<script type="application/ld+json">\n{schema_str}\n</script>'
    return content_html + script_block
