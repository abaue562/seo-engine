"""Multi-Channel Publisher — routes content packages to all connected platforms.

Flow:
  Content Package → Channel Router → Platform Connectors → Live Publishing → Verification

One topic generates 5+ assets across 5+ platforms = 25+ signal points.

Channel roles:
  TikTok → attention capture
  Social → reinforcement + reach
  Blog/Website → ranking + authority
  GBP → local trust + map pack
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from execution.connectors.base import Connector, PublishResult

log = logging.getLogger(__name__)

# Default spacing between publishes (seconds)
PUBLISH_DELAY = 5


class ContentPackage(BaseModel):
    """Standardized multi-format content from any engine."""
    topic: str
    keyword: str = ""
    assets: dict = {}     # {channel_name: payload_dict}
    source: str = ""      # Which engine created this (aic, multiplier, etc.)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class PublishReport(BaseModel):
    """Results from publishing a content package."""
    topic: str
    total_attempted: int = 0
    total_success: int = 0
    total_queued: int = 0
    total_failed: int = 0
    results: list[dict] = Field(default_factory=list)
    published_at: datetime = Field(default_factory=datetime.utcnow)


class ChannelRouter:
    """Routes content assets to the correct platform connectors."""

    # Map asset type → platform connector name
    ROUTE_MAP = {
        "blog": "wordpress",
        "blog_article": "wordpress",
        "service_page": "wordpress",
        "page": "wordpress",
        "social_post": "social",
        "social": "social",
        "facebook": "social",
        "instagram": "social",
        "twitter": "social",
        "tiktok_script": "tiktok",
        "tiktok": "tiktok",
        "short_video": "tiktok",
        "gbp_post": "gbp",
        "gbp": "gbp",
    }

    def route(self, package: ContentPackage) -> list[tuple[str, dict]]:
        """Route a content package to platform connectors.
        Returns list of (connector_name, payload) tuples."""
        routes = []
        for asset_type, payload in package.assets.items():
            connector_name = self.ROUTE_MAP.get(asset_type.lower())
            if connector_name and payload:
                routes.append((connector_name, payload))
            else:
                log.debug("router.no_route  asset=%s", asset_type)
        return routes


class MultiChannelPublisher:
    """Publishes content across all connected platforms."""

    def __init__(self):
        self.connectors: dict[str, Connector] = {}
        self.router = ChannelRouter()

    def register(self, name: str, connector: Connector):
        """Register a platform connector."""
        self.connectors[name] = connector
        log.info("publisher.registered  connector=%s", name)

    async def publish_package(
        self,
        package: ContentPackage,
        delay_seconds: int = PUBLISH_DELAY,
        dry_run: bool = False,
    ) -> PublishReport:
        """Publish a content package across all channels."""
        routes = self.router.route(package)
        report = PublishReport(topic=package.topic, total_attempted=len(routes))

        log.info("publisher.start  topic=%s  routes=%d  dry_run=%s",
                 package.topic, len(routes), dry_run)

        for i, (connector_name, payload) in enumerate(routes):
            connector = self.connectors.get(connector_name)

            if not connector:
                log.warning("publisher.no_connector  name=%s", connector_name)
                report.results.append({
                    "platform": connector_name,
                    "status": "skipped",
                    "reason": "no connector registered",
                })
                continue

            if dry_run:
                report.results.append({
                    "platform": connector_name,
                    "status": "dry_run",
                    "payload_preview": str(payload)[:200],
                })
                report.total_queued += 1
                continue

            try:
                # Delay between publishes to avoid rate limits
                if i > 0 and delay_seconds > 0:
                    await asyncio.sleep(delay_seconds)

                result = await connector.publish(payload)
                report.results.append(result.model_dump())

                if result.status == "success":
                    report.total_success += 1

                    # Verify
                    verified = await connector.verify(result)
                    if not verified:
                        log.warning("publisher.verify_fail  platform=%s  url=%s",
                                    connector_name, result.url)

                elif result.status == "queued":
                    report.total_queued += 1
                else:
                    report.total_failed += 1

            except Exception as e:
                log.error("publisher.fail  platform=%s  err=%s", connector_name, e)
                report.results.append({
                    "platform": connector_name,
                    "status": "failed",
                    "error": str(e),
                })
                report.total_failed += 1

        log.info("publisher.done  topic=%s  success=%d  queued=%d  failed=%d",
                 package.topic, report.total_success, report.total_queued, report.total_failed)

        return report

    async def publish_aic_result(self, aic_result: dict, dry_run: bool = False) -> PublishReport:
        """Convert an AIC engine result into a content package and publish."""
        assets = {}

        # Blog from conversion page
        conv = aic_result.get("conversion", {})
        if conv.get("title"):
            assets["blog"] = {
                "title": conv["title"],
                "content": self._sections_to_html(conv.get("sections", [])),
                "slug": aic_result.get("keyword", "").replace(" ", "-"),
                "status": "draft",
                "type": "pages",
            }

        # Social posts from attention layer
        att = aic_result.get("attention", {})
        for sp in att.get("social_posts", [])[:1]:
            assets["social_post"] = sp

        # TikTok from attention
        if att.get("tiktok_script", {}).get("hook"):
            assets["tiktok_script"] = att["tiktok_script"]

        # GBP post
        if att.get("social_posts"):
            # Reuse a social post as GBP
            assets["gbp_post"] = {
                "text": att["social_posts"][0].get("text", ""),
                "cta": "Learn more",
            }

        package = ContentPackage(
            topic=aic_result.get("keyword", "unknown"),
            keyword=aic_result.get("keyword", ""),
            assets=assets,
            source="aic",
        )

        return await self.publish_package(package, dry_run=dry_run)

    @staticmethod
    def _sections_to_html(sections: list[dict]) -> str:
        """Convert page sections to basic HTML."""
        html_parts = []
        for s in sections:
            title = s.get("title", "")
            content = s.get("content", "")
            items = s.get("items", [])

            if title:
                html_parts.append(f"<h2>{title}</h2>")
            if content:
                html_parts.append(f"<p>{content}</p>")
            if items:
                for item in items:
                    if isinstance(item, dict):
                        q = item.get("question", item.get("name", ""))
                        a = item.get("answer", item.get("description", ""))
                        if q:
                            html_parts.append(f"<h3>{q}</h3><p>{a}</p>")
                    else:
                        html_parts.append(f"<p>{item}</p>")

        return "\n".join(html_parts)
