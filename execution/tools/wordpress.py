"""WordPress REST API connector — real page creation and updates.

Requires:
  - WordPress site with REST API enabled
  - Application Password (Users → Your Profile → Application Passwords)
  - Site URL in config

Usage:
  wp = WordPressTool("https://your-site.com", "admin", "xxxx xxxx xxxx xxxx")
  result = await wp.create_page("My Title", "<p>Content</p>", slug="my-page")
"""

from __future__ import annotations

import logging

import httpx

from execution.tools.base import Tool

log = logging.getLogger(__name__)


class WordPressTool(Tool):
    """WordPress REST API v2 connector."""

    def __init__(self, base_url: str, username: str, app_password: str):
        self.base_url = base_url.rstrip("/")
        self.auth = (username, app_password)
        self.api = f"{self.base_url}/wp-json/wp/v2"

    async def create_page(self, title: str, content: str, slug: str = "", status: str = "draft") -> dict:
        """Create a new WordPress page."""
        payload = {"title": title, "content": content, "status": status}
        if slug:
            payload["slug"] = slug

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.api}/pages",
                auth=self.auth,
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()

        log.info("wp.page_created  id=%s  slug=%s  status=%s", data.get("id"), data.get("slug"), status)
        return {"id": data.get("id"), "url": data.get("link"), "slug": data.get("slug"), "status": status}

    async def update_page(self, page_id: str, data: dict) -> dict:
        """Update an existing WordPress page."""
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.api}/pages/{page_id}",
                auth=self.auth,
                json=data,
            )
            resp.raise_for_status()
            result = resp.json()

        log.info("wp.page_updated  id=%s", page_id)
        return {"id": result.get("id"), "url": result.get("link"), "modified": result.get("modified")}

    async def create_post(self, title: str, content: str, status: str = "draft") -> dict:
        """Create a new WordPress blog post."""
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.api}/posts",
                auth=self.auth,
                json={"title": title, "content": content, "status": status},
            )
            resp.raise_for_status()
            data = resp.json()

        log.info("wp.post_created  id=%s  status=%s", data.get("id"), status)
        return {"id": data.get("id"), "url": data.get("link"), "status": status}

    async def update_post_meta(self, post_id: str, title: str | None = None, excerpt: str | None = None) -> dict:
        """Update title/meta for a post or page."""
        payload: dict = {}
        if title:
            payload["title"] = title
        if excerpt:
            payload["excerpt"] = excerpt

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.api}/posts/{post_id}",
                auth=self.auth,
                json=payload,
            )
            resp.raise_for_status()
            return resp.json()

    async def get_pages(self, per_page: int = 20) -> list[dict]:
        """List existing pages."""
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(f"{self.api}/pages", params={"per_page": per_page})
            resp.raise_for_status()
            return resp.json()
