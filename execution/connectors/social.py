"""Social media connector — generates ready-to-post content packages.

Currently outputs structured payloads for manual or n8n-based posting.
Can be extended with platform APIs (Twitter/X, Facebook, Instagram, LinkedIn).
"""

from __future__ import annotations
import logging
from execution.connectors.base import Connector, PublishResult

log = logging.getLogger(__name__)


class SocialConnector(Connector):
    """Generates social post packages. Queues for posting via external tool."""
    platform = "social"

    async def publish(self, payload: dict) -> PublishResult:
        """Generate a ready-to-post social media package."""
        text = payload.get("text", "")
        hashtags = payload.get("hashtags", [])
        platform = payload.get("platform", "general")
        link = payload.get("link", "")

        formatted = text
        if link and link not in text:
            formatted += f"\n\n{link}"
        if hashtags:
            formatted += "\n\n" + " ".join(f"#{h}" if not h.startswith("#") else h for h in hashtags)

        log.info("social.queued  platform=%s  len=%d", platform, len(formatted))
        return PublishResult(
            platform=f"social:{platform}",
            status="queued",
            url="",
            post_id="",
        )


class TikTokConnector(Connector):
    """Generates TikTok-ready script packages. Queued for production."""
    platform = "tiktok"

    async def publish(self, payload: dict) -> PublishResult:
        """Queue a TikTok script for production."""
        hook = payload.get("hook", "")
        body = payload.get("body", "")
        cta = payload.get("cta", "")
        caption = payload.get("caption", "")

        log.info("tiktok.queued  hook=%s", hook[:50])
        return PublishResult(
            platform="tiktok",
            status="queued",
        )


class GBPConnector(Connector):
    """Google Business Profile — generates post content. Queued for manual posting or Playwright automation."""
    platform = "gbp"

    async def publish(self, payload: dict) -> PublishResult:
        """Queue a GBP post."""
        text = payload.get("text", "")
        cta = payload.get("cta", "")

        log.info("gbp.queued  len=%d", len(text))
        return PublishResult(
            platform="gbp",
            status="queued",
        )
