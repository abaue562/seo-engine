"""Data Ingestion Pipeline — orchestrates all connectors into a unified data package.

Runs connectors → normalizes → stores → evaluates freshness → detects events.
Produces an enriched context block for agents.
"""

from __future__ import annotations

import logging
from datetime import datetime
from dataclasses import dataclass, field

from data.freshness import (
    DataFreshnessReport,
    evaluate_gsc, evaluate_gbp, evaluate_competitors, evaluate_traffic,
)
from data.events import (
    SEOEvent,
    detect_ranking_changes, detect_review_changes, detect_traffic_changes,
    events_to_prompt_block,
)
from data.connectors.gsc import GSCData, gsc_to_prompt_block, gsc_to_rankings
from data.connectors.keywords import KeywordData, keywords_to_prompt_block
from data.crawlers.website import CrawlResult, crawl_to_prompt_block
from data.crawlers.gbp import GBPData, gbp_to_prompt_block
from data.crawlers.competitors import CompetitorData, competitors_to_prompt_block
from data.storage.database import Database
from models.business import BusinessContext

log = logging.getLogger(__name__)


@dataclass
class IngestedData:
    """Full data package ready for agents."""
    business: BusinessContext
    gsc: GSCData | None = None
    gbp: GBPData | None = None
    crawl: CrawlResult | None = None
    competitors: CompetitorData | None = None
    keywords: KeywordData | None = None
    freshness: DataFreshnessReport = field(default_factory=DataFreshnessReport)
    events: list[SEOEvent] = field(default_factory=list)

    def to_agent_context(self) -> str:
        """Build the full context block agents receive — business + live data."""
        blocks = [self.business.to_prompt_block()]

        if self.gsc:
            blocks.append(gsc_to_prompt_block(self.gsc))
        if self.gbp:
            blocks.append(gbp_to_prompt_block(self.gbp))
        if self.crawl:
            blocks.append(crawl_to_prompt_block(self.crawl))
        if self.competitors:
            blocks.append(competitors_to_prompt_block(self.competitors))
        if self.keywords:
            blocks.append(keywords_to_prompt_block(self.keywords))

        blocks.append(self.freshness.to_prompt_block())
        blocks.append(events_to_prompt_block(self.events))

        return "\n\n---\n\n".join(blocks)


class IngestionPipeline:
    """Runs all data connectors and produces agent-ready context."""

    def __init__(self, db: Database | None = None):
        self.db = db or Database()

    async def run_full(
        self,
        business: BusinessContext,
        business_id: str,
        skip_gsc: bool = False,
        skip_gbp: bool = False,
        skip_crawl: bool = False,
        skip_competitors: bool = False,
        skip_keywords: bool = False,
    ) -> IngestedData:
        """Run all enabled connectors and build enriched data package."""
        result = IngestedData(business=business)
        log.info("pipeline.start  biz=%s", business.business_name)

        # --- GSC ---
        if not skip_gsc and business.website:
            try:
                from data.connectors.gsc import fetch_gsc
                result.gsc = await fetch_gsc(business.website)
                result.freshness.add(evaluate_gsc(result.gsc.fetched_at, len(result.gsc.rows)))

                # Update business rankings from GSC
                live_rankings = gsc_to_rankings(result.gsc)
                if live_rankings:
                    business.current_rankings = {k: int(v) for k, v in live_rankings.items()}

                # Store snapshot
                await self.db.save_snapshot(business_id, "gsc", {"rows": len(result.gsc.rows)})
                log.info("pipeline.gsc_done  rows=%d", len(result.gsc.rows))
            except Exception as e:
                log.warning("pipeline.gsc_fail  err=%s", e)

        # --- GBP ---
        if not skip_gbp and business.gbp_url:
            try:
                from data.crawlers.gbp import scrape_gbp
                result.gbp = await scrape_gbp(business.gbp_url)
                result.freshness.add(evaluate_gbp(result.gbp.fetched_at, result.gbp.review_count))

                # Update business from live GBP data
                if result.gbp.review_count:
                    business.reviews_count = result.gbp.review_count
                if result.gbp.rating:
                    business.rating = result.gbp.rating

                await self.db.save_snapshot(business_id, "gbp", result.gbp.model_dump())
                log.info("pipeline.gbp_done  reviews=%d", result.gbp.review_count)
            except Exception as e:
                log.warning("pipeline.gbp_fail  err=%s", e)

        # --- Website crawl ---
        if not skip_crawl and business.website:
            try:
                from data.crawlers.website import crawl_website
                result.crawl = await crawl_website(business.website, max_pages=20)
                pages_data = [p.model_dump() for p in result.crawl.pages]
                await self.db.save_pages(business_id, pages_data)
                log.info("pipeline.crawl_done  pages=%d", len(result.crawl.pages))
            except Exception as e:
                log.warning("pipeline.crawl_fail  err=%s", e)

        # --- Competitors ---
        if not skip_competitors and business.primary_keywords:
            try:
                from data.crawlers.competitors import discover_serp_competitors
                result.competitors = await discover_serp_competitors(
                    keywords=business.primary_keywords[:3],
                    city=business.primary_city,
                )
                result.freshness.add(evaluate_competitors(
                    result.competitors.fetched_at,
                    len(result.competitors.competitors),
                ))
                comp_data = [c.model_dump() for c in result.competitors.competitors]
                await self.db.save_competitors(business_id, comp_data)
                log.info("pipeline.competitors_done  count=%d", len(result.competitors.competitors))
            except Exception as e:
                log.warning("pipeline.competitors_fail  err=%s", e)

        # --- Keywords ---
        if not skip_keywords and business.primary_keywords:
            try:
                from data.connectors.keywords import discover_keywords
                result.keywords = await discover_keywords(
                    seed_queries=[f"{kw} {business.primary_city}" for kw in business.primary_keywords[:3]],
                )
                kw_data = [s.model_dump() for s in result.keywords.suggestions]
                await self.db.save_keywords(business_id, kw_data)
                log.info("pipeline.keywords_done  count=%d", len(result.keywords.suggestions))
            except Exception as e:
                log.warning("pipeline.keywords_fail  err=%s", e)

        # --- Event detection ---
        result.events = await self._detect_events(business_id, result)

        log.info("pipeline.done  freshness=%s  events=%d",
                 result.freshness.overall_confidence(), len(result.events))
        return result

    async def _detect_events(self, business_id: str, data: IngestedData) -> list[SEOEvent]:
        """Compare current data against stored snapshots to detect changes."""
        events: list[SEOEvent] = []

        # Ranking changes
        prev_snapshot = await self.db.get_latest_snapshot(business_id, "gsc")
        if prev_snapshot and data.gsc:
            import json
            try:
                prev_data = json.loads(prev_snapshot.get("data_json", "{}"))
                prev_rankings = prev_data.get("rankings", {})
                curr_rankings = gsc_to_rankings(data.gsc)
                events.extend(detect_ranking_changes(
                    {k: float(v) for k, v in curr_rankings.items()},
                    {k: float(v) for k, v in prev_rankings.items()},
                ))
            except Exception as e:
                log.debug("events.ranking_skip  err=%s", e)

        # Review changes
        prev_gbp = await self.db.get_latest_snapshot(business_id, "gbp")
        if prev_gbp and data.gbp:
            import json
            try:
                prev = json.loads(prev_gbp.get("data_json", "{}"))
                events.extend(detect_review_changes(
                    data.gbp.review_count, prev.get("review_count", 0),
                    data.gbp.rating, prev.get("rating", 0.0),
                ))
            except Exception as e:
                log.debug("events.review_skip  err=%s", e)

        return events
