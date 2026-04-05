"""Content Execution Handler — generates real articles via Claude CLI."""

from __future__ import annotations

import json
import logging
from core.claude import call_claude

from execution.models import ExecResult, ExecStatus
from execution.templates.prompts import ARTICLE_PROMPT
from models.business import BusinessContext

log = logging.getLogger(__name__)


class ContentHandler:

    async def execute(self, task_id: str, action: str, target: str, execution: str, business: BusinessContext) -> ExecResult:
        return await self.create_article(task_id, target, action, business)

    async def create_article(self, task_id: str, target: str, action: str, business: BusinessContext) -> ExecResult:
        keyword = target or (business.primary_keywords[0] if business.primary_keywords else business.primary_service)

        prompt = ARTICLE_PROMPT.format(
            business_name=business.business_name,
            keyword=keyword,
            city=business.primary_city,
            topic=f"{keyword} services in {business.primary_city}",
        )

        try:
            raw = call_claude(prompt, max_tokens=4096)
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            article = json.loads(raw)
            log.info("content.article_generated  task=%s  keyword=%s", task_id, keyword)

            return ExecResult(
                task_id=task_id,
                status=ExecStatus.SUCCESS,
                output={"type": "article", "keyword": keyword, "ready_to_apply": True, **article},
            )
        except Exception as e:
            log.error("content.article_fail  task=%s  err=%s", task_id, e)
            return ExecResult(task_id=task_id, status=ExecStatus.FAILED, output={"error": str(e)})
