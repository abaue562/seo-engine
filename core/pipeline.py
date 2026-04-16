"""Unified SEO execution pipeline.

Canonical flow:
  keyword → brief → content → validate → publish → canonicalize
         → link_inject → index → verify → track → convert

Each stage is independently retryable. Failures are logged and the pipeline
continues with partial results where possible.

Usage:
    from core.pipeline import SEOPipeline
    from data.db import get_db

    db = get_db()
    pipeline = SEOPipeline(db=db, business=business_config)
    result = await pipeline.run("emergency plumber NYC", intent="transactional")
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)


class ValidationFailed(Exception):
    """Raised when content fails a blocking quality gate."""
    def __init__(self, message: str, failures: list[str]):
        super().__init__(message)
        self.failures = failures


class PipelineError(Exception):
    """Non-recoverable pipeline error."""


@dataclass
class StageResult:
    name: str
    success: bool
    output: Any = None
    error: str = ""
    duration_ms: int = 0


@dataclass
class PipelineResult:
    keyword: str
    url: str = ""
    post_id: int = 0
    status: str = "pending"   # pending | success | partial | failed
    stages_completed: list[str] = field(default_factory=list)
    stages_failed: list[str] = field(default_factory=list)
    published_at: str = ""
    errors: list[str] = field(default_factory=list)
    article: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "keyword": self.keyword,
            "url": self.url,
            "post_id": self.post_id,
            "status": self.status,
            "stages_completed": self.stages_completed,
            "stages_failed": self.stages_failed,
            "published_at": self.published_at,
            "errors": self.errors,
        }


class SEOPipeline:
    """Runs the full keyword-to-conversion pipeline for a single piece of content."""

    def __init__(self, db=None, business: dict = None):
        self.db = db
        self.business = business or {}

    async def run(
        self,
        keyword: str,
        intent: str = "informational",
        location: str = "",
        page_type: str = "blog_post",
        word_count: int = None,
        skip_stages: list[str] = None,
    ) -> PipelineResult:
        skip = set(skip_stages or [])
        result = PipelineResult(keyword=keyword)
        log.info("pipeline.start  keyword=%s  intent=%s", keyword, intent)

        stages = [
            ("brief",       self._stage_brief),
            ("content",     self._stage_content),
            ("validate",    self._stage_validate),
            ("publish",     self._stage_publish),
            ("canonicalize",self._stage_canonicalize),
            ("link_inject", self._stage_link_inject),
            ("index",       self._stage_index),
            ("verify",      self._stage_verify),
            ("track",       self._stage_track),
            ("convert",     self._stage_convert),
        ]

        ctx: dict[str, Any] = {
            "keyword": keyword,
            "intent": intent,
            "location": location,
            "page_type": page_type,
            "word_count": word_count,
            "business": self.business,
        }

        for stage_name, stage_fn in stages:
            if stage_name in skip:
                log.debug("pipeline.skip_stage  stage=%s", stage_name)
                continue

            t0 = datetime.now(tz=timezone.utc)
            try:
                output = await stage_fn(ctx)
                ctx[stage_name] = output
                result.stages_completed.append(stage_name)
                ms = int((datetime.now(tz=timezone.utc) - t0).total_seconds() * 1000)
                log.info("pipeline.stage_ok  stage=%s  ms=%d", stage_name, ms)
            except ValidationFailed as exc:
                result.stages_failed.append(stage_name)
                result.errors.append(f"{stage_name}: {exc}")
                result.status = "failed"
                log.error("pipeline.validation_failed  stage=%s  failures=%s", stage_name, exc.failures)
                break
            except Exception as exc:
                result.stages_failed.append(stage_name)
                result.errors.append(f"{stage_name}: {exc}")
                log.warning("pipeline.stage_error  stage=%s  error=%s", stage_name, exc)
                # Non-blocking stages: continue even on failure
                if stage_name in ("link_inject", "verify", "track", "convert"):
                    continue
                # Blocking stages: stop pipeline
                result.status = "partial"
                break

        # Set final status
        if not result.stages_failed or all(
            s in ("link_inject", "verify", "track", "convert") for s in result.stages_failed
        ):
            if "publish" in result.stages_completed:
                result.status = "success"

        # Extract key outputs
        if "publish" in ctx:
            pub = ctx["publish"] or {}
            result.url = pub.get("url", "")
            result.post_id = pub.get("post_id", 0)
            result.published_at = datetime.now(tz=timezone.utc).isoformat()
        if "content" in ctx:
            result.article = ctx["content"] or {}

        # Save to db
        if self.db:
            try:
                business_id = self.business.get("id", "unknown")
                self.db.save_task_result(
                    business_id, "pipeline", keyword, result.to_dict(),
                    status=result.status
                )
            except Exception as e:
                log.warning("pipeline.db_save_fail  err=%s", e)

        log.info(
            "pipeline.done  keyword=%s  status=%s  url=%s",
            keyword, result.status, result.url
        )
        return result

    # ----------------------------------------------------------------
    # Stage implementations
    # ----------------------------------------------------------------

    async def _stage_brief(self, ctx: dict) -> dict:
        """Build a content brief from SERP data."""
        keyword = ctx["keyword"]
        intent = ctx["intent"]
        word_targets = {"transactional": 900, "commercial": 1200, "informational": 1500, "pillar": 2500}
        target_words = ctx.get("word_count") or word_targets.get(intent, 900)
        return {
            "keyword": keyword,
            "intent": intent,
            "target_words": target_words,
            "location": ctx.get("location", ""),
            "page_type": ctx.get("page_type", "blog_post"),
            "competitor_h2s": [],
        }

    async def _stage_content(self, ctx: dict) -> dict:
        """Generate content via Claude."""
        try:
            from core.claude import call_claude_json
        except ImportError:
            raise PipelineError("core.claude not available")

        brief = ctx["brief"]
        business = ctx["business"]
        keyword = brief["keyword"]
        intent = brief["intent"]
        location = brief.get("location", "")
        target_words = brief["target_words"]

        loc_suffix = f" in {location}" if location else ""
        prompt = f"""Generate a complete SEO article for the keyword: "{keyword}{loc_suffix}"

Business: {business.get('name', 'the business')}, {business.get('service_type', '')} in {business.get('city', location or 'the area')}.
Intent: {intent}
Target word count: {target_words}
Page type: {brief.get('page_type', 'blog_post')}

MANDATORY STRUCTURE:
1. Direct answer paragraph in first 100 words (factual, declarative)
2. H1 matching the keyword intent
3. Minimum 3 H2 sections
4. FAQ section with 5 Q&A pairs using real local questions
5. Author byline placeholder: <!-- AUTHOR_BYLINE -->
6. Internal link placeholders: {{{{LINK:anchor text:target-slug}}}}

CONTENT REQUIREMENTS:
- Include at least one specific statistic or data point
- Mention the city/location naturally throughout
- Use conversational language with contractions
- Vary sentence length (mix short and long)
- Include a comparison or "how to choose" section
- End with a clear call-to-action

Return ONLY valid JSON:
{{
  "title": "60 char max title starting with keyword",
  "meta_description": "155 char meta with keyword + location + CTA",
  "slug": "url-safe-slug",
  "h1": "exact H1",
  "direct_answer": "first 100-word direct answer paragraph",
  "content_html": "full HTML body with all sections",
  "faq": [{{"question": "", "answer": ""}}],
  "word_count": 0,
  "schema_json": {{}},
  "original_data_point": "the statistic used"
}}"""

        article = await asyncio.to_thread(
            call_claude_json, prompt,
            system="You are an expert SEO content writer. Return only valid JSON.",
            max_tokens=8192
        )
        if not article or not article.get("content_html"):
            raise PipelineError("Content generation returned empty article")
        return article

    async def _stage_validate(self, ctx: dict) -> dict:
        """Run content through quality gate."""
        try:
            from execution.validators.content_gate import ContentGate
        except ImportError:
            log.warning("content_gate not available — skipping validation")
            return {"passed": True, "skipped": True}

        try:
            from config.settings import ORIGINALITY_API_KEY, AI_SCORE_THRESHOLD
        except ImportError:
            ORIGINALITY_API_KEY = ""
            AI_SCORE_THRESHOLD = 0.45

        article = ctx["content"]
        gate = ContentGate(
            originality_api_key=ORIGINALITY_API_KEY,
            ai_threshold=float(AI_SCORE_THRESHOLD),
        )
        gate_result = await gate.check_and_humanise(
            content_html=article.get("content_html", ""),
            keyword=ctx["keyword"],
            intent=ctx["intent"],
            title=article.get("title", ""),
            meta_description=article.get("meta_description", ""),
            schema_json=article.get("schema_json"),
        )

        if not gate_result.passed:
            raise ValidationFailed(
                f"Content failed quality gate for '{ctx['keyword']}'",
                gate_result.blocking_failures,
            )

        # Update article with humanised version if rewritten
        if gate_result.humanised_html:
            ctx["content"]["content_html"] = gate_result.humanised_html

        return {"passed": True, "scores": gate_result.scores, "warnings": gate_result.warnings}

    async def _stage_publish(self, ctx: dict) -> dict:
        """Publish to WordPress."""
        try:
            from execution.connectors.wordpress import WordPressConnector
            from config.settings import WP_URL, WP_USER, WP_APP_PASSWORD, WP_PUBLISH_STATUS
        except ImportError:
            log.warning("WordPress connector not available")
            return {"url": "", "post_id": 0, "platform": "wordpress", "skipped": True}

        if not WP_URL:
            log.warning("WP_URL not configured — skipping publish")
            return {"url": "", "post_id": 0, "platform": "wordpress", "skipped": True}

        article = ctx["content"]
        business = ctx["business"]

        wp = WordPressConnector(
            site_url=WP_URL,
            username=WP_USER,
            app_password=WP_APP_PASSWORD,
        )
        result = await wp.publish(
            title=article.get("title", ctx["keyword"]),
            content_html=article.get("content_html", ""),
            slug=article.get("slug", ""),
            meta_description=article.get("meta_description", ""),
            schema_json=article.get("schema_json", {}),
            status=WP_PUBLISH_STATUS,
            keyword=ctx["keyword"],
        )
        return {
            "url": result.get("url", ""),
            "post_id": result.get("post_id", 0),
            "platform": "wordpress",
        }

    async def _stage_canonicalize(self, ctx: dict) -> dict:
        """Register URL and assign canonical."""
        try:
            from execution.canonical import CanonicalRegistry
        except ImportError:
            return {"registered": False}

        pub = ctx.get("publish", {})
        url = pub.get("url", "")
        if not url:
            return {"registered": False}

        article = ctx["content"]
        registry = CanonicalRegistry(db=self.db)
        registry.register(
            business_id=self.business.get("id", "unknown"),
            primary_url=url,
            slug=article.get("slug", ""),
            keyword=ctx["keyword"],
            platform="wordpress",
        )
        return {"registered": True, "canonical_url": url}

    async def _stage_link_inject(self, ctx: dict) -> dict:
        """Inject internal links into the published post."""
        try:
            from execution.link_injector import LinkInjector
            from config.settings import WP_URL, WP_USER, WP_APP_PASSWORD
        except ImportError:
            return {"injected": False}

        pub = ctx.get("publish", {})
        post_id = pub.get("post_id", 0)
        if not post_id:
            return {"injected": False}

        injector = LinkInjector()
        article = ctx["content"]
        result = await asyncio.to_thread(
            injector.inject,
            content_html=article.get("content_html", ""),
            keyword=ctx["keyword"],
            business_name=self.business.get("name", ""),
        )
        return {"injected": True, "links_added": result.get("links_added", 0) if isinstance(result, dict) else 0}

    async def _stage_index(self, ctx: dict) -> dict:
        """Submit URL to Google + Bing indexing APIs."""
        try:
            from execution.indexing import submit_url
        except ImportError:
            return {"submitted": False}

        pub = ctx.get("publish", {})
        url = pub.get("url", "")
        if not url:
            return {"submitted": False}

        index_result = await submit_url(url)
        return {
            "submitted": True,
            "google_api": index_result.google_api,
            "bing": index_result.bing_indexnow,
        }

    async def _stage_verify(self, ctx: dict) -> dict:
        """Schedule 48h indexing verification."""
        pub = ctx.get("publish", {})
        url = pub.get("url", "")
        if not url or not self.db:
            return {"scheduled": False}
        self.db.queue_url_for_verification(url, check_after_hours=48)
        return {"scheduled": True, "url": url}

    async def _stage_track(self, ctx: dict) -> dict:
        """Register keyword for rank tracking."""
        if not self.db:
            return {"tracked": False}
        pub = ctx.get("publish", {})
        url = pub.get("url", "")
        business_id = self.business.get("id", "unknown")
        if url:
            self.db.save_ranking(business_id, ctx["keyword"], position=0, url=url, volume=0)
        return {"tracked": True}

    async def _stage_convert(self, ctx: dict) -> dict:
        """Inject conversion elements and update WP post."""
        try:
            from aic.conversion_injector import ConversionInjector
            from execution.connectors.wordpress import WordPressConnector
            from config.settings import WP_URL, WP_USER, WP_APP_PASSWORD, GA4_MEASUREMENT_ID
        except ImportError:
            return {"converted": False}

        pub = ctx.get("publish", {})
        post_id = pub.get("post_id", 0)
        if not post_id or not WP_URL:
            return {"converted": False}

        article = ctx["content"]
        injector = ConversionInjector()
        enriched_html = injector.inject(
            content_html=article.get("content_html", ""),
            keyword=ctx["keyword"],
            intent=ctx["intent"],
            business=self.business,
            ga4_measurement_id=GA4_MEASUREMENT_ID,
        )

        try:
            wp = WordPressConnector(WP_URL, WP_USER, WP_APP_PASSWORD)
            await wp.update_post(post_id, content_html=enriched_html)
        except Exception as e:
            log.warning("convert.update_fail  post_id=%s  err=%s", post_id, e)
            return {"converted": False, "error": str(e)}

        return {"converted": True}
