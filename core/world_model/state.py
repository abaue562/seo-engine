"""World Model — persistent understanding of reality across runs.

Instead of reacting per run, the system maintains a living model of:
  - Current rankings (keyword → position + trend)
  - Competitor state (who's doing what)
  - Page inventory (what we have, what's missing)
  - Signal history (what actions were taken, what moved)
  - Active campaigns (bursts, CTR tests, update cycles)

This state persists between cycles and drives planning decisions.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from data.storage.database import Database

log = logging.getLogger(__name__)


class KeywordState(BaseModel):
    keyword: str
    position: int = 0
    previous_position: int = 0
    trend: str = "stable"          # improving / stable / declining
    impressions: int = 0
    ctr: float = 0.0
    last_action: str = ""
    last_action_date: datetime | None = None


class CompetitorState(BaseModel):
    name: str
    rankings: dict[str, int] = {}   # keyword → position
    recent_moves: list[str] = []
    threat_level: str = "medium"    # critical / high / medium / low


class PageState(BaseModel):
    url: str
    keyword: str = ""
    word_count: int = 0
    last_updated: datetime | None = None
    update_count: int = 0
    has_schema: bool = False
    internal_links_in: int = 0
    internal_links_out: int = 0


class WorldState(BaseModel):
    """The system's understanding of reality."""
    business_id: str = ""
    keywords: dict[str, KeywordState] = {}
    competitors: dict[str, CompetitorState] = {}
    pages: dict[str, PageState] = {}
    active_campaigns: list[str] = []          # IDs of running campaigns
    total_actions_taken: int = 0
    total_ranking_gains: int = 0
    cycle_count: int = 0
    last_cycle: datetime | None = None
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class WorldModel:
    """Manages the persistent world state."""

    def __init__(self, db: Database | None = None):
        self.db = db or Database()
        self._state: dict[str, WorldState] = {}

    async def get_state(self, business_id: str) -> WorldState:
        """Load or create world state for a business."""
        if business_id in self._state:
            return self._state[business_id]

        stored = await self.db.query("world_state", {"business_id": business_id}, limit=1)
        if stored:
            state = WorldState(**stored[0])
        else:
            state = WorldState(business_id=business_id)

        self._state[business_id] = state
        return state

    async def update_rankings(self, business_id: str, rankings: dict[str, int]) -> list[str]:
        """Update keyword rankings and detect trends. Returns list of changes."""
        state = await self.get_state(business_id)
        changes = []

        for kw, new_pos in rankings.items():
            if kw in state.keywords:
                old = state.keywords[kw]
                old.previous_position = old.position
                old.position = new_pos

                diff = old.previous_position - new_pos
                if diff > 0:
                    old.trend = "improving"
                    changes.append(f"{kw}: #{old.previous_position} → #{new_pos} (+{diff})")
                elif diff < 0:
                    old.trend = "declining"
                    changes.append(f"{kw}: #{old.previous_position} → #{new_pos} ({diff})")
                else:
                    old.trend = "stable"
            else:
                state.keywords[kw] = KeywordState(keyword=kw, position=new_pos)
                changes.append(f"{kw}: NEW at #{new_pos}")

        state.updated_at = datetime.utcnow()
        await self._save(state)
        log.info("world.rankings_updated  biz=%s  changes=%d", business_id, len(changes))
        return changes

    async def update_competitors(self, business_id: str, competitors: dict[str, dict]) -> None:
        """Update competitor state."""
        state = await self.get_state(business_id)
        for name, data in competitors.items():
            if name in state.competitors:
                state.competitors[name].rankings.update(data.get("rankings", {}))
            else:
                state.competitors[name] = CompetitorState(name=name, **data)
        await self._save(state)

    async def record_action(self, business_id: str, keyword: str, action: str) -> None:
        """Record that an action was taken for a keyword."""
        state = await self.get_state(business_id)
        if keyword in state.keywords:
            state.keywords[keyword].last_action = action
            state.keywords[keyword].last_action_date = datetime.utcnow()
        state.total_actions_taken += 1
        await self._save(state)

    async def record_cycle(self, business_id: str) -> None:
        """Record that a full cycle completed."""
        state = await self.get_state(business_id)
        state.cycle_count += 1
        state.last_cycle = datetime.utcnow()
        await self._save(state)

    def to_prompt_block(self, state: WorldState) -> str:
        """Render world state as context for agents."""
        lines = [f"WORLD STATE (cycle #{state.cycle_count}, {state.total_actions_taken} actions taken):"]

        # Keywords
        improving = [k for k in state.keywords.values() if k.trend == "improving"]
        declining = [k for k in state.keywords.values() if k.trend == "declining"]
        stable = [k for k in state.keywords.values() if k.trend == "stable"]

        if improving:
            lines.append("\n  IMPROVING:")
            for k in improving:
                lines.append(f"    {k.keyword}: #{k.position} (was #{k.previous_position})")
        if declining:
            lines.append("\n  DECLINING (prioritize these):")
            for k in declining:
                lines.append(f"    {k.keyword}: #{k.position} (was #{k.previous_position})")
        if stable:
            lines.append(f"\n  STABLE: {len(stable)} keywords unchanged")

        # Competitors
        threats = [c for c in state.competitors.values() if c.threat_level in ("critical", "high")]
        if threats:
            lines.append("\n  COMPETITOR THREATS:")
            for c in threats:
                lines.append(f"    {c.name}: {c.threat_level} — {', '.join(c.recent_moves[:2])}")

        return "\n".join(lines)

    async def _save(self, state: WorldState) -> None:
        await self.db.upsert("world_state", state.model_dump(), key="business_id")
