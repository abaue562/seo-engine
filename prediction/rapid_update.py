"""Rapid Update Engine — continuous page improvement to trigger re-ranking.

Google rewards freshness. This engine:
  1. Identifies stagnant or near-ranking pages
  2. Generates small, meaningful updates every 5-7 days
  3. Applies updates incrementally
  4. Triggers re-indexing
  5. Tracks which update types cause ranking movement

Typical result: +2-5 positions within 2-3 weeks WITHOUT new backlinks.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from core.claude import call_claude
from data.storage.database import Database

log = logging.getLogger(__name__)

UPDATE_INTERVAL_DAYS = 5
MAX_UPDATES_PER_PAGE = 8  # Stop after 8 cycles to avoid over-updating


class PageUpdate(BaseModel):
    """A single incremental update to a page."""
    type: str               # content_addition / faq_addition / heading_improvement / stat_update / cta_improvement
    instruction: str        # What to do
    content: str = ""       # Ready-to-deploy content
    word_count: int = 0


class UpdatePlan(BaseModel):
    """Plan for updating a page."""
    page_url: str
    keyword: str
    updates: list[PageUpdate] = Field(default_factory=list)
    total_word_additions: int = 0
    reindex_recommended: bool = True


class UpdateCycle(BaseModel):
    """Tracks the update lifecycle for a page."""
    cycle_id: str = ""
    page_url: str
    keyword: str
    business_id: str = ""
    updates_applied: int = 0
    last_update: datetime | None = None
    next_update: datetime | None = None
    position_at_start: int = 0
    current_position: int = 0
    update_history: list[dict] = Field(default_factory=list)
    status: str = "active"  # active / completed / paused


RAPID_UPDATE_PROMPT = """You are the Rapid Update Agent. Generate small but meaningful improvements for this page.

Page: {page_url}
Keyword: {keyword}
Business: {business_name}
City: {city}
Current position: {position}
Update number: {update_number} of {max_updates}
Previous updates: {previous_updates}

Generate 2-3 SPECIFIC updates to improve this page. Each update should:
- Add real value (not filler)
- Be small enough to apply in 10 minutes
- Help with ranking for "{keyword}"
- Be DIFFERENT from previous updates

Update types to choose from:
- content_addition: Add 100-200 words of new relevant content
- faq_addition: Add 1-2 new FAQ items with schema-ready Q&A
- heading_improvement: Rewrite a subheading for better keyword targeting
- stat_update: Add current statistics, pricing, or data points
- cta_improvement: Improve call-to-action for better conversion

Return ONLY JSON:
{{
  "updates": [
    {{
      "type": "content_addition | faq_addition | heading_improvement | stat_update | cta_improvement",
      "instruction": "what to do and where",
      "content": "actual ready-to-deploy content",
      "word_count": 0
    }}
  ]
}}"""


class RapidUpdateEngine:
    """Generates and manages continuous page updates for freshness-driven ranking."""

    def __init__(self, db: Database | None = None):
        self.db = db or Database()

    # ----- Detection -----

    def find_stagnant_pages(
        self,
        rankings: dict[str, int],
        last_updated: dict[str, datetime] | None = None,
        stagnant_days: int = 14,
    ) -> list[dict]:
        """Find pages ranking 5-15 that haven't been updated recently."""
        candidates = []
        now = datetime.utcnow()

        for keyword, position in rankings.items():
            if not (5 <= position <= 15):
                continue

            days_since_update = stagnant_days + 1  # Default: assume stagnant
            if last_updated and keyword in last_updated:
                days_since_update = (now - last_updated[keyword]).days

            if days_since_update >= stagnant_days:
                candidates.append({
                    "keyword": keyword,
                    "position": position,
                    "days_since_update": days_since_update,
                    "priority": "high" if position <= 10 else "medium",
                })

        candidates.sort(key=lambda x: x["position"])
        log.info("rapid_update.stagnant  found=%d", len(candidates))
        return candidates

    # ----- Generation -----

    async def generate_updates(
        self,
        page_url: str,
        keyword: str,
        business_name: str,
        city: str,
        position: int,
        update_number: int = 1,
        previous_updates: list[str] | None = None,
    ) -> UpdatePlan:
        """Generate specific incremental updates for a page."""
        prev_str = ", ".join(previous_updates[:5]) if previous_updates else "none yet"

        prompt = RAPID_UPDATE_PROMPT.format(
            page_url=page_url,
            keyword=keyword,
            business_name=business_name,
            city=city,
            position=position,
            update_number=update_number,
            max_updates=MAX_UPDATES_PER_PAGE,
            previous_updates=prev_str,
        )

        try:
            raw = call_claude(
                prompt,
                system="You are a content improvement specialist. Return ONLY valid JSON.",
                max_tokens=2048,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            start = raw.find('{')
            if start > 0:
                raw = raw[start:]
            data, _ = json.JSONDecoder().raw_decode(raw)
            updates = [PageUpdate(**u) for u in data.get("updates", [])]
            total_words = sum(u.word_count for u in updates)

            plan = UpdatePlan(
                page_url=page_url,
                keyword=keyword,
                updates=updates,
                total_word_additions=total_words,
            )

            log.info("rapid_update.generated  url=%s  updates=%d  words=+%d",
                     page_url, len(updates), total_words)
            return plan

        except Exception as e:
            log.error("rapid_update.generate_fail  url=%s  err=%s", page_url, e)
            return UpdatePlan(page_url=page_url, keyword=keyword)

    # ----- Cycle management -----

    def create_cycle(
        self,
        page_url: str,
        keyword: str,
        business_id: str,
        current_position: int,
    ) -> UpdateCycle:
        """Start a new update cycle for a page."""
        import uuid
        cycle = UpdateCycle(
            cycle_id=uuid.uuid4().hex[:12],
            page_url=page_url,
            keyword=keyword,
            business_id=business_id,
            position_at_start=current_position,
            current_position=current_position,
            next_update=datetime.utcnow(),
            status="active",
        )
        log.info("rapid_update.cycle_created  id=%s  url=%s  pos=#%d", cycle.cycle_id, page_url, current_position)
        return cycle

    def should_update(self, cycle: UpdateCycle) -> bool:
        """Check if it's time for the next update."""
        if cycle.status != "active":
            return False
        if cycle.updates_applied >= MAX_UPDATES_PER_PAGE:
            return False
        if cycle.next_update and datetime.utcnow() >= cycle.next_update:
            return True
        return False

    def record_update(self, cycle: UpdateCycle, update_types: list[str]) -> None:
        """Record that an update was applied."""
        cycle.updates_applied += 1
        cycle.last_update = datetime.utcnow()
        cycle.next_update = datetime.utcnow() + timedelta(days=UPDATE_INTERVAL_DAYS)
        cycle.update_history.append({
            "update_number": cycle.updates_applied,
            "types": update_types,
            "applied_at": datetime.utcnow().isoformat(),
        })

        if cycle.updates_applied >= MAX_UPDATES_PER_PAGE:
            cycle.status = "completed"
            log.info("rapid_update.cycle_complete  id=%s  total=%d", cycle.cycle_id, cycle.updates_applied)

    def record_ranking_change(self, cycle: UpdateCycle, new_position: int) -> None:
        """Track ranking movement during the cycle."""
        old = cycle.current_position
        cycle.current_position = new_position
        movement = old - new_position  # positive = improved

        if movement > 0:
            log.info("rapid_update.ranking_improved  id=%s  %d -> %d (+%d)",
                     cycle.cycle_id, old, new_position, movement)
        elif movement < 0:
            log.warning("rapid_update.ranking_dropped  id=%s  %d -> %d (%d)",
                        cycle.cycle_id, old, new_position, movement)

        # Auto-complete if reached top 3
        if new_position <= 3:
            cycle.status = "completed"
            log.info("rapid_update.goal_reached  id=%s  position=#%d", cycle.cycle_id, new_position)

    # ----- Persistence -----

    async def save_cycle(self, cycle: UpdateCycle) -> None:
        await self.db.upsert("update_cycles", cycle.model_dump(), key="cycle_id")

    async def get_active_cycles(self, business_id: str) -> list[UpdateCycle]:
        rows = await self.db.query("update_cycles", {"business_id": business_id, "status": "active"})
        return [UpdateCycle(**r) for r in rows]
