"""Medium connector — auto-publishes articles via Medium API.

Setup:
  1. Go to medium.com/me/settings/security
  2. Generate an Integration Token
  3. Add MEDIUM_TOKEN to config/.env

Articles are published as "draft" by default (you review + publish).
Set status="public" for auto-publish.
"""

from __future__ import annotations

import logging
import httpx

from execution.connectors.base import Connector, PublishResult

log = logging.getLogger(__name__)

MEDIUM_API = "https://api.medium.com/v1"


class MediumConnector(Connector):
    platform = "medium"

    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self._user_id: str | None = None

    async def _get_user_id(self) -> str:
        if self._user_id:
            return self._user_id
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{MEDIUM_API}/me", headers=self.headers)
            resp.raise_for_status()
            self._user_id = resp.json()["data"]["id"]
        return self._user_id

    async def publish(self, payload: dict) -> PublishResult:
        """Publish an article to Medium."""
        title = payload.get("title", "Untitled")
        content = payload.get("content", "")
        tags = payload.get("tags", [])[:5]
        status = payload.get("status", "draft")  # draft or public

        # Medium accepts HTML or markdown
        content_format = "html" if "<" in content else "markdown"

        try:
            user_id = await self._get_user_id()

            body = {
                "title": title,
                "contentFormat": content_format,
                "content": f"<h1>{title}</h1>\n{content}",
                "tags": tags,
                "publishStatus": status,
            }

            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{MEDIUM_API}/users/{user_id}/posts",
                    headers=self.headers,
                    json=body,
                )
                resp.raise_for_status()
                data = resp.json()["data"]

            log.info("medium.published  id=%s  title=%s  status=%s", data.get("id"), title[:40], status)
            return PublishResult(
                platform="medium",
                status="success",
                url=data.get("url", ""),
                post_id=data.get("id", ""),
            )

        except Exception as e:
            log.error("medium.publish_fail  title=%s  err=%s", title[:40], e)
            return PublishResult(platform="medium", status="failed", error=str(e))

    async def create_post(self, title: str, content: str, status: str = "draft") -> PublishResult:
        return await self.publish({"title": title, "content": content, "status": status})
