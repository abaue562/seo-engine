"""Website Execution Handler — generates real page content, meta tags, internal links."""

from __future__ import annotations

import json
import logging
from core.claude import call_claude

from execution.models import ExecResult, ExecStatus
from execution.templates.prompts import META_UPDATE_PROMPT, SERVICE_PAGE_PROMPT, INTERNAL_LINK_PROMPT
from models.business import BusinessContext

log = logging.getLogger(__name__)


class WebsiteHandler:

    async def execute(self, task_id: str, action: str, target: str, execution: str, business: BusinessContext) -> ExecResult:
        action_lower = action.lower()

        # Broad keyword matching for routing
        if any(kw in action_lower for kw in ["title", "meta", "description tag"]):
            return await self.update_meta(task_id, target, business)
        elif any(kw in action_lower for kw in ["page", "create", "build", "publish", "homepage", "landing"]):
            return await self.create_page(task_id, target, action, business)
        elif any(kw in action_lower for kw in ["link", "internal", "interlinking"]):
            return await self.suggest_links(task_id, target, business)
        elif any(kw in action_lower for kw in ["schema", "structured data", "json-ld"]):
            return await self.generate_schema(task_id, target, business)
        else:
            # Default: create page content (most useful fallback)
            return await self.create_page(task_id, target, action, business)

    async def update_meta(self, task_id: str, page_url: str, business: BusinessContext) -> ExecResult:
        keyword = business.primary_keywords[0] if business.primary_keywords else business.primary_service
        prompt = META_UPDATE_PROMPT.format(
            current_title="(current title unknown)",
            current_meta="(current meta unknown)",
            page_url=page_url,
            keyword=keyword,
            business_name=business.business_name,
            city=business.primary_city,
        )

        try:
            raw = call_claude(prompt, max_tokens=512)
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            data = json.loads(raw)
            log.info("website.meta_generated  task=%s", task_id)

            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={"type": "meta_update", "page": page_url, "ready_to_apply": True, **data},
                rollback_available=True,
            )
        except Exception as e:
            log.error("website.meta_fail  task=%s  err=%s", task_id, e)
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": str(e)})

    async def create_page(self, task_id: str, target: str, action: str, business: BusinessContext) -> ExecResult:
        keyword = business.primary_keywords[0] if business.primary_keywords else business.primary_service

        prompt = SERVICE_PAGE_PROMPT.format(
            business_name=business.business_name,
            service=target or business.primary_service,
            city=business.primary_city,
            keyword=keyword,
        )

        try:
            raw = call_claude(prompt, max_tokens=4096)
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            data = json.loads(raw)
            log.info("website.page_generated  task=%s", task_id)

            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={"type": "page_create", "target": target, "ready_to_apply": True, **data},
            )
        except Exception as e:
            log.error("website.page_fail  task=%s  err=%s", task_id, e)
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": str(e)})

    async def suggest_links(self, task_id: str, page_url: str, business: BusinessContext) -> ExecResult:
        prompt = INTERNAL_LINK_PROMPT.format(
            page_url=page_url,
            content_summary=f"{business.primary_service} in {business.primary_city}",
            available_pages="\n".join(f"- {business.website}/{kw.lower().replace(' ', '-')}" for kw in business.primary_keywords[:5]),
        )

        try:
            raw = call_claude(prompt, max_tokens=1024)
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            links = json.loads(raw)
            log.info("website.links_generated  task=%s  count=%d", task_id, len(links) if isinstance(links, list) else 0)

            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={"type": "internal_links", "page": page_url, "ready_to_apply": True, "suggestions": links},
            )
        except Exception as e:
            log.error("website.links_fail  task=%s  err=%s", task_id, e)
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": str(e)})

    async def generate_schema(self, task_id: str, target: str, business: BusinessContext) -> ExecResult:
        """Generate JSON-LD schema markup."""
        schema = {
            "@context": "https://schema.org",
            "@type": "LocalBusiness",
            "name": business.business_name,
            "url": business.website,
            "address": {"@type": "PostalAddress", "addressLocality": business.primary_city},
            "areaServed": [{"@type": "City", "name": a} for a in business.service_areas],
        }
        if business.rating:
            schema["aggregateRating"] = {
                "@type": "AggregateRating",
                "ratingValue": str(business.rating),
                "reviewCount": str(business.reviews_count),
            }

        return ExecResult(
            task_id=task_id,
            status=ExecStatus.SUCCESS,
            output={"type": "schema_markup", "ready_to_apply": True, "json_ld": schema},
        )
