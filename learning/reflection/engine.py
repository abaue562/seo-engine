"""Reflection Engine — post-cycle analysis that feeds back into strategy.

After every run:
  1. What worked? (actions that caused ranking movement)
  2. What failed? (actions with no effect)
  3. What to adjust? (strategy changes for next cycle)
  4. Store as episodic memory

This is what makes the system genuinely self-improving.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from core.claude import call_claude
from data.storage.database import Database

log = logging.getLogger(__name__)


class Episode(BaseModel):
    """One complete goal attempt — stored as episodic memory."""
    episode_id: str = ""
    business_id: str = ""
    goal: str
    keyword: str = ""
    actions_taken: list[str] = []
    starting_position: int = 0
    ending_position: int = 0
    ranking_change: int = 0
    duration_days: int = 0
    success: bool = False
    lessons: list[str] = []
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class Reflection(BaseModel):
    """Output of one reflection cycle."""
    what_worked: list[str] = []
    what_failed: list[str] = []
    adjustments: list[str] = []
    confidence_updates: dict[str, float] = {}  # pattern → new confidence
    episode: Episode | None = None


REFLECTION_PROMPT = """You are the Reflection Agent. Analyze what happened in the last cycle and extract lessons.

Business: {business_name}
Goal: {goal}
Actions taken: {actions}
Results: {results}
Ranking changes: {ranking_changes}

Analyze honestly:
1. What WORKED? (actions that caused positive movement — be specific)
2. What FAILED? (actions with no measurable effect)
3. What to ADJUST? (specific changes for next cycle)

Return ONLY JSON:
{{
  "what_worked": ["specific finding"],
  "what_failed": ["specific finding"],
  "adjustments": ["specific change to make"],
  "key_lesson": ""
}}"""


class ReflectionEngine:
    """Runs post-cycle reflection and builds episodic memory."""

    def __init__(self, db: Database | None = None):
        self.db = db or Database()

    async def reflect(
        self,
        business_id: str,
        business_name: str,
        goal: str,
        actions_taken: list[str],
        results: dict,
        ranking_changes: dict[str, str],
    ) -> Reflection:
        """Run reflection on a completed cycle."""
        prompt = REFLECTION_PROMPT.format(
            business_name=business_name,
            goal=goal,
            actions=", ".join(actions_taken[:10]),
            results=json.dumps(results, default=str)[:500],
            ranking_changes=json.dumps(ranking_changes)[:300],
        )

        try:
            raw = call_claude(
                prompt,
                system="You are an honest analyst. Return ONLY valid JSON.",
                max_tokens=1024,
            )
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

            data = json.loads(raw)

            reflection = Reflection(
                what_worked=data.get("what_worked", []),
                what_failed=data.get("what_failed", []),
                adjustments=data.get("adjustments", []),
            )

            log.info("reflection.done  worked=%d  failed=%d  adjustments=%d",
                     len(reflection.what_worked), len(reflection.what_failed), len(reflection.adjustments))

            # Store as episode
            episode = Episode(
                episode_id=f"{business_id}-{datetime.utcnow().strftime('%Y%m%d')}",
                business_id=business_id,
                goal=goal,
                actions_taken=actions_taken,
                lessons=reflection.adjustments,
                success=len(reflection.what_worked) > len(reflection.what_failed),
            )
            reflection.episode = episode
            await self.db.upsert("episodes", episode.model_dump(), key="episode_id")

            return reflection

        except Exception as e:
            log.error("reflection.fail  err=%s", e)
            return Reflection()

    async def get_past_episodes(self, business_id: str, limit: int = 5) -> list[Episode]:
        """Retrieve past episodes for context."""
        rows = await self.db.query("episodes", {"business_id": business_id}, limit=limit)
        return [Episode(**r) for r in rows]

    def episodes_to_prompt_block(self, episodes: list[Episode]) -> str:
        """Render episodic memory as agent context."""
        if not episodes:
            return "EPISODIC MEMORY: No past episodes recorded."

        lines = [f"EPISODIC MEMORY ({len(episodes)} past attempts):"]
        for ep in episodes:
            status = "SUCCESS" if ep.success else "FAILED"
            lines.append(f"\n  [{status}] {ep.goal}")
            lines.append(f"    Actions: {', '.join(ep.actions_taken[:3])}")
            if ep.ranking_change:
                lines.append(f"    Ranking change: {ep.ranking_change:+d} positions")
            for lesson in ep.lessons[:2]:
                lines.append(f"    Lesson: {lesson}")

        return "\n".join(lines)
