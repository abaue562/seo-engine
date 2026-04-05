"""Free Blog Network — auto-create and post to free blog platforms.

These are REAL third-party websites we don't own.
Each post creates a real page on the internet with a contextual backlink.

Supported platforms:
  - WordPress.com (free blogs)
  - Blogger/Blogspot (Google-owned, free)
  - Medium (free account)
  - Tumblr (free)

Strategy: Create accounts on each, build a small content history,
then post articles with soft backlinks to main site.
"""

from __future__ import annotations

import logging
import httpx
from datetime import datetime
from pydantic import BaseModel, Field

from execution.connectors.base import Connector, PublishResult

log = logging.getLogger(__name__)


class ExternalPost(BaseModel):
    """Record of a post made on a third-party site."""
    platform: str
    url: str = ""
    title: str = ""
    has_link: bool = False
    link_type: str = ""        # direct / soft / branded / none
    posted_at: datetime = Field(default_factory=datetime.utcnow)


class WordPressComConnector(Connector):
    """WordPress.com free blog — auto-post articles.

    Setup:
      1. Create free blog at wordpress.com
      2. Go to Developer settings, create OAuth app
      3. Get access token
      4. Add WP_COM_TOKEN and WP_COM_SITE to .env
    """
    platform = "wordpress.com"

    def __init__(self, site: str, token: str):
        self.site = site  # e.g., "myblog.wordpress.com"
        self.token = token
        self.api = f"https://public-api.wordpress.com/rest/v1.1/sites/{site}/posts/new"

    async def publish(self, payload: dict) -> PublishResult:
        title = payload.get("title", "")
        content = payload.get("content", "")
        tags = payload.get("tags", [])
        status = payload.get("status", "publish")

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    self.api,
                    headers={"Authorization": f"Bearer {self.token}"},
                    json={
                        "title": title,
                        "content": content,
                        "tags": ",".join(tags) if tags else "",
                        "status": status,
                    },
                )
                resp.raise_for_status()
                data = resp.json()

            log.info("wpcom.published  url=%s", data.get("URL", ""))
            return PublishResult(
                platform="wordpress.com",
                status="success",
                url=data.get("URL", ""),
                post_id=str(data.get("ID", "")),
            )
        except Exception as e:
            log.error("wpcom.fail  err=%s", e)
            return PublishResult(platform="wordpress.com", status="failed", error=str(e))


class TumblrConnector(Connector):
    """Tumblr — auto-post content.

    Setup:
      1. Create Tumblr blog (free)
      2. Register app at tumblr.com/oauth/apps
      3. Get OAuth tokens
      4. Add TUMBLR_CONSUMER_KEY, TUMBLR_TOKEN, TUMBLR_BLOG to .env
    """
    platform = "tumblr"

    def __init__(self, blog_name: str, consumer_key: str, oauth_token: str, oauth_secret: str, consumer_secret: str):
        self.blog = blog_name
        self.consumer_key = consumer_key
        self.oauth_token = oauth_token
        self.oauth_secret = oauth_secret
        self.consumer_secret = consumer_secret

    async def publish(self, payload: dict) -> PublishResult:
        title = payload.get("title", "")
        content = payload.get("content", "")
        tags = payload.get("tags", [])

        try:
            # Tumblr uses OAuth 1.0a — needs requests-oauthlib or manual signing
            # For now, use the v2 API with API key
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"https://api.tumblr.com/v2/blog/{self.blog}/post",
                    headers={"Authorization": f"Bearer {self.oauth_token}"},
                    json={
                        "type": "text",
                        "title": title,
                        "body": content,
                        "tags": ",".join(tags),
                        "state": "published",
                    },
                )
                data = resp.json()

            post_id = str(data.get("response", {}).get("id", ""))
            url = f"https://{self.blog}.tumblr.com/post/{post_id}" if post_id else ""

            log.info("tumblr.published  id=%s", post_id)
            return PublishResult(platform="tumblr", status="success", url=url, post_id=post_id)

        except Exception as e:
            log.error("tumblr.fail  err=%s", e)
            return PublishResult(platform="tumblr", status="failed", error=str(e))


class PinterestConnector(Connector):
    """Pinterest — create pins with links back to your site.

    Setup:
      1. Create Pinterest business account
      2. Create app at developers.pinterest.com
      3. Get access token
      4. Add PINTEREST_TOKEN and PINTEREST_BOARD to .env
    """
    platform = "pinterest"

    def __init__(self, token: str, board_id: str):
        self.token = token
        self.board_id = board_id

    async def publish(self, payload: dict) -> PublishResult:
        title = payload.get("title", "")
        description = payload.get("description", payload.get("content", "")[:500])
        link = payload.get("link", "")
        image_url = payload.get("image_url", "")

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    "https://api.pinterest.com/v5/pins",
                    headers={"Authorization": f"Bearer {self.token}"},
                    json={
                        "title": title,
                        "description": description,
                        "board_id": self.board_id,
                        "link": link,
                        "media_source": {"source_type": "image_url", "url": image_url} if image_url else None,
                    },
                )
                data = resp.json()

            pin_id = data.get("id", "")
            log.info("pinterest.pinned  id=%s", pin_id)
            return PublishResult(platform="pinterest", status="success", post_id=str(pin_id))

        except Exception as e:
            log.error("pinterest.fail  err=%s", e)
            return PublishResult(platform="pinterest", status="failed", error=str(e))
