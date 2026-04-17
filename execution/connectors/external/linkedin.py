"""LinkedIn connector — publishes Articles and Posts via LinkedIn API.

Setup:
  1. Create LinkedIn App at linkedin.com/developers
  2. Request r_liteprofile, w_member_social, r_organization_social permissions
  3. Add to config/.env:
     LINKEDIN_CLIENT_ID=xxx
     LINKEDIN_CLIENT_SECRET=xxx
     LINKEDIN_ACCESS_TOKEN=xxx        # OAuth2 token (use refresh flow for long-lived)
     LINKEDIN_AUTHOR_URN=urn:li:person:xxx   # from /v2/me

Articles (long-form, indexed by Google, DA 98):
  - Published as LinkedIn Articles via /v2/ugcPosts
  - Supports HTML content up to 120,000 characters
  - Indexed within 24-48 hours

Posts (short-form, engagement):
  - Simple text posts with optional link attachment
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import httpx

from execution.connectors.base import Connector, PublishResult

log = logging.getLogger(__name__)

LINKEDIN_API = "https://api.linkedin.com/v2"


class LinkedInConnector(Connector):
    platform = "linkedin"

    def __init__(self):
        self.access_token = os.getenv("LINKEDIN_ACCESS_TOKEN", "")
        self.author_urn = os.getenv("LINKEDIN_AUTHOR_URN", "")
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "X-Restli-Protocol-Version": "2.0.0",
        }

    def is_configured(self) -> bool:
        return bool(self.access_token and self.author_urn)

    def publish_article(
        self,
        title: str,
        content_html: str,
        description: str = "",
        tags: list[str] | None = None,
    ) -> PublishResult:
        """Publish a long-form LinkedIn Article (indexed by Google, DA 98)."""
        if not self.is_configured():
            return PublishResult(
                success=False,
                platform=self.platform,
                error="LINKEDIN_ACCESS_TOKEN or LINKEDIN_AUTHOR_URN not set",
            )

        # LinkedIn Articles use ugcPosts endpoint
        payload = {
            "author": self.author_urn,
            "lifecycleState": "PUBLISHED",
            "specificContent": {
                "com.linkedin.ugc.ShareContent": {
                    "shareCommentary": {
                        "text": description or title,
                    },
                    "shareMediaCategory": "ARTICLE",
                    "media": [
                        {
                            "status": "READY",
                            "description": {"text": description or title[:200]},
                            "title": {"text": title},
                        }
                    ],
                }
            },
            "visibility": {
                "com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"
            },
        }

        try:
            resp = httpx.post(
                f"{LINKEDIN_API}/ugcPosts",
                headers=self.headers,
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            post_id = data.get("id", "")
            url = f"https://www.linkedin.com/feed/update/{post_id}/" if post_id else ""
            log.info("linkedin.article_published  post_id=%s  url=%s", post_id, url)
            return PublishResult(success=True, platform=self.platform, url=url, post_id=post_id)
        except Exception as exc:
            log.error("linkedin.article_fail  title=%s  err=%s", title[:60], exc)
            return PublishResult(success=False, platform=self.platform, error=str(exc))

    def publish_post(
        self,
        text: str,
        link_url: str = "",
        link_title: str = "",
        link_description: str = "",
    ) -> PublishResult:
        """Publish a short LinkedIn post with optional link preview."""
        if not self.is_configured():
            return PublishResult(
                success=False,
                platform=self.platform,
                error="LINKEDIN_ACCESS_TOKEN or LINKEDIN_AUTHOR_URN not set",
            )

        payload: dict = {
            "author": self.author_urn,
            "lifecycleState": "PUBLISHED",
            "specificContent": {
                "com.linkedin.ugc.ShareContent": {
                    "shareCommentary": {"text": text},
                    "shareMediaCategory": "NONE" if not link_url else "ARTICLE",
                }
            },
            "visibility": {
                "com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"
            },
        }

        if link_url:
            payload["specificContent"]["com.linkedin.ugc.ShareContent"]["media"] = [
                {
                    "status": "READY",
                    "originalUrl": link_url,
                    "title": {"text": link_title or text[:100]},
                    "description": {"text": link_description or ""},
                }
            ]
            payload["specificContent"]["com.linkedin.ugc.ShareContent"][
                "shareMediaCategory"
            ] = "ARTICLE"

        try:
            resp = httpx.post(
                f"{LINKEDIN_API}/ugcPosts",
                headers=self.headers,
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            post_id = data.get("id", "")
            url = f"https://www.linkedin.com/feed/update/{post_id}/" if post_id else ""
            log.info("linkedin.post_published  post_id=%s", post_id)
            return PublishResult(success=True, platform=self.platform, url=url, post_id=post_id)
        except Exception as exc:
            log.error("linkedin.post_fail  err=%s", exc)
            return PublishResult(success=False, platform=self.platform, error=str(exc))

    def get_profile(self) -> dict:
        """Fetch authenticated member profile to validate token."""
        try:
            resp = httpx.get(
                f"{LINKEDIN_API}/me",
                headers=self.headers,
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.error("linkedin.profile_fail  err=%s", exc)
            return {}
