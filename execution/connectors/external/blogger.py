"""Blogger/Blogspot connector — auto-publishes via Google Blogger API.

Setup:
  1. Create a Blogger blog (free)
  2. Enable Blogger API in Google Cloud Console
  3. Use same OAuth credentials as GSC
  4. Add BLOGGER_BLOG_ID to config/.env

Good for: creating satellite content sites that link back to main site.
"""

from __future__ import annotations

import logging

from execution.connectors.base import Connector, PublishResult

log = logging.getLogger(__name__)


class BloggerConnector(Connector):
    platform = "blogger"

    def __init__(self, blog_id: str, credentials_path: str = ""):
        self.blog_id = blog_id
        self.credentials_path = credentials_path

    async def publish(self, payload: dict) -> PublishResult:
        """Publish a post to Blogger."""
        title = payload.get("title", "")
        content = payload.get("content", "")
        labels = payload.get("labels", payload.get("tags", []))
        status = payload.get("status", "draft")  # draft or live

        try:
            from googleapiclient.discovery import build
            from google.oauth2.credentials import Credentials

            creds = Credentials.from_authorized_user_file(self.credentials_path)
            service = build("blogger", "v3", credentials=creds)

            body = {
                "kind": "blogger#post",
                "title": title,
                "content": content,
                "labels": labels,
            }

            if status == "draft":
                result = service.posts().insert(blogId=self.blog_id, body=body, isDraft=True).execute()
            else:
                result = service.posts().insert(blogId=self.blog_id, body=body).execute()

            log.info("blogger.published  id=%s  title=%s", result.get("id"), title[:40])
            return PublishResult(
                platform="blogger",
                status="success",
                url=result.get("url", ""),
                post_id=result.get("id", ""),
            )

        except Exception as e:
            log.error("blogger.publish_fail  title=%s  err=%s", title[:40], e)
            return PublishResult(platform="blogger", status="failed", error=str(e))
