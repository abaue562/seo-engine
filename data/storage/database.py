"""Database layer — Supabase storage for all SEO data.

Tables:
  businesses  — id, name, website, city, created_at
  keywords    — id, business_id, keyword, position, volume, difficulty, updated_at
  pages       — id, business_id, url, title, h1, word_count, updated_at
  competitors — id, business_id, name, rating, review_count, website, updated_at
  tasks       — id, business_id, action, type, priority_rank, status, total_score, created_at
  snapshots   — id, business_id, source, data_json, fetched_at

Also works with a local JSON fallback if Supabase is not configured.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from config.settings import SUPABASE_URL, SUPABASE_KEY

log = logging.getLogger(__name__)

_LOCAL_STORE = Path("data/storage/local_db")


def _get_client():
    """Get Supabase client, or None if not configured."""
    if not SUPABASE_URL or not SUPABASE_KEY or "xxx" in SUPABASE_URL or "xxx" in SUPABASE_KEY:
        return None
    try:
        from supabase import create_client
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except ImportError:
        return None


class Database:
    """Unified storage — Supabase if available, local JSON fallback."""

    def __init__(self):
        self.client = _get_client()
        self.is_remote = self.client is not None
        if not self.is_remote:
            _LOCAL_STORE.mkdir(parents=True, exist_ok=True)
            log.info("db.local_mode  path=%s", _LOCAL_STORE)
        else:
            log.info("db.supabase_connected")

    # ---- Generic operations ----

    async def upsert(self, table: str, data: dict, key: str = "id") -> dict:
        if self.is_remote:
            result = self.client.table(table).upsert(data).execute()
            return result.data[0] if result.data else data
        else:
            return self._local_upsert(table, data, key)

    async def query(self, table: str, filters: dict | None = None, limit: int = 100) -> list[dict]:
        if self.is_remote:
            q = self.client.table(table).select("*").limit(limit)
            if filters:
                for k, v in filters.items():
                    q = q.eq(k, v)
            result = q.execute()
            return result.data or []
        else:
            return self._local_query(table, filters, limit)

    # ---- Business-specific helpers ----

    async def save_business(self, business_id: str, data: dict) -> dict:
        data["id"] = business_id
        data["updated_at"] = datetime.utcnow().isoformat()
        return await self.upsert("businesses", data)

    async def save_keywords(self, business_id: str, keywords: list[dict]) -> int:
        count = 0
        for kw in keywords:
            kw["business_id"] = business_id
            kw["updated_at"] = datetime.utcnow().isoformat()
            await self.upsert("keywords", kw, key="keyword")
            count += 1
        return count

    async def save_pages(self, business_id: str, pages: list[dict]) -> int:
        count = 0
        for page in pages:
            page["business_id"] = business_id
            page["updated_at"] = datetime.utcnow().isoformat()
            await self.upsert("pages", page, key="url")
            count += 1
        return count

    async def save_competitors(self, business_id: str, competitors: list[dict]) -> int:
        count = 0
        for comp in competitors:
            comp["business_id"] = business_id
            comp["updated_at"] = datetime.utcnow().isoformat()
            await self.upsert("competitors", comp, key="name")
            count += 1
        return count

    async def save_tasks(self, business_id: str, tasks: list[dict]) -> int:
        count = 0
        for task in tasks:
            task["business_id"] = business_id
            task["status"] = task.get("status", "pending")
            task["created_at"] = datetime.utcnow().isoformat()
            await self.upsert("tasks", task)
            count += 1
        return count

    async def save_snapshot(self, business_id: str, source: str, data: dict) -> dict:
        """Store a raw data snapshot for history/debugging."""
        record = {
            "business_id": business_id,
            "source": source,
            "data_json": json.dumps(data),
            "fetched_at": datetime.utcnow().isoformat(),
        }
        return await self.upsert("snapshots", record)

    async def get_latest_snapshot(self, business_id: str, source: str) -> dict | None:
        results = await self.query("snapshots", {"business_id": business_id, "source": source}, limit=1)
        return results[0] if results else None

    # ---- Local JSON fallback ----

    def _local_path(self, table: str) -> Path:
        return _LOCAL_STORE / f"{table}.json"

    def _local_read(self, table: str) -> list[dict]:
        path = self._local_path(table)
        if not path.exists():
            return []
        return json.loads(path.read_text())

    def _local_write(self, table: str, data: list[dict]) -> None:
        path = self._local_path(table)
        path.write_text(json.dumps(data, indent=2, default=str))

    def _local_upsert(self, table: str, record: dict, key: str = "id") -> dict:
        records = self._local_read(table)
        key_val = record.get(key)

        if key_val:
            for i, r in enumerate(records):
                if r.get(key) == key_val:
                    records[i] = {**r, **record}
                    self._local_write(table, records)
                    return records[i]

        records.append(record)
        self._local_write(table, records)
        return record

    def _local_query(self, table: str, filters: dict | None, limit: int) -> list[dict]:
        records = self._local_read(table)
        if filters:
            for k, v in filters.items():
                records = [r for r in records if r.get(k) == v]
        return records[:limit]
