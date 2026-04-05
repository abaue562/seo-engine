"""WordPress connector — publishes blog posts and pages via REST API."""

from __future__ import annotations
import logging
import httpx

from execution.connectors.base import Connector, PublishResult

log = logging.getLogger(__name__)


class WordPressConnector(Connector):
    platform = "wordpress"

    def __init__(self, base_url: str, username: str, app_password: str):
        self.base_url = base_url.rstrip("/")
        self.auth = (username, app_password)
        self.api = f"{self.base_url}/wp-json/wp/v2"

    async def publish(self, payload: dict) -> PublishResult:
        """Publish a post or page to WordPress."""
        post_type = payload.get("type", "posts")  # "posts" or "pages"
        status = payload.get("status", "draft")    # "draft" or "publish"

        body = {
            "title": payload.get("title", ""),
            "content": payload.get("content", ""),
            "status": status,
        }
        if payload.get("slug"):
            body["slug"] = payload["slug"]
        if payload.get("excerpt"):
            body["excerpt"] = payload["excerpt"]

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{self.api}/{post_type}",
                    auth=self.auth,
                    json=body,
                )
                resp.raise_for_status()
                data = resp.json()

            log.info("wp.published  id=%s  type=%s  status=%s", data.get("id"), post_type, status)
            return PublishResult(
                platform="wordpress",
                status="success",
                url=data.get("link", ""),
                post_id=str(data.get("id", "")),
            )
        except Exception as e:
            log.error("wp.publish_fail  err=%s", e)
            return PublishResult(platform="wordpress", status="failed", error=str(e))
