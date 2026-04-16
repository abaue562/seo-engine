"""Centralised SQLite database — replaces scattered JSON file storage.

Tables:
  published_urls    — every URL the engine has published
  task_results      — all task outcomes
  ranking_history   — keyword rank snapshots
  anchor_dist       — per-domain anchor text distribution
  citation_reports  — AI citation monitoring results
  businesses        — business profiles
  backlink_prospects — outreach targets
  outreach_log      — sent emails + responses
  leads             — captured leads from forms
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


class SEODatabase:
    """Thread-safe SQLite database for the SEO engine."""

    _SCHEMA = """
    CREATE TABLE IF NOT EXISTS published_urls (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        business_id   TEXT NOT NULL,
        url           TEXT NOT NULL UNIQUE,
        platform      TEXT NOT NULL DEFAULT 'wordpress',
        canonical_url TEXT,
        slug          TEXT,
        keyword       TEXT,
        simhash       INTEGER,
        published_at  TEXT NOT NULL DEFAULT (datetime('now')),
        status        TEXT NOT NULL DEFAULT 'live'
    );
    CREATE INDEX IF NOT EXISTS idx_pu_business ON published_urls(business_id);
    CREATE INDEX IF NOT EXISTS idx_pu_slug     ON published_urls(slug);

    CREATE TABLE IF NOT EXISTS syndications (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        primary_url     TEXT NOT NULL,
        syndicated_url  TEXT NOT NULL UNIQUE,
        platform        TEXT NOT NULL,
        canonical_url   TEXT NOT NULL,
        created_at      TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_syn_primary ON syndications(primary_url);

    CREATE TABLE IF NOT EXISTS task_results (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        business_id TEXT NOT NULL,
        task_type   TEXT NOT NULL,
        keyword     TEXT,
        result_json TEXT,
        status      TEXT NOT NULL DEFAULT 'success',
        created_at  TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_tr_business ON task_results(business_id);
    CREATE INDEX IF NOT EXISTS idx_tr_created  ON task_results(created_at);

    CREATE TABLE IF NOT EXISTS ranking_history (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        business_id TEXT NOT NULL,
        keyword     TEXT NOT NULL,
        position    INTEGER,
        url         TEXT,
        volume      INTEGER DEFAULT 0,
        recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_rh_business_kw ON ranking_history(business_id, keyword);

    CREATE TABLE IF NOT EXISTS anchor_dist (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        domain      TEXT NOT NULL,
        anchor_type TEXT NOT NULL,
        anchor_text TEXT NOT NULL,
        target_url  TEXT,
        created_at  TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_ad_domain ON anchor_dist(domain);

    CREATE TABLE IF NOT EXISTS citation_reports (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        business_id TEXT NOT NULL,
        query       TEXT NOT NULL,
        engine      TEXT NOT NULL,
        cited       INTEGER NOT NULL DEFAULT 0,
        source_url  TEXT,
        report_json TEXT,
        checked_at  TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_cr_business ON citation_reports(business_id);

    CREATE TABLE IF NOT EXISTS businesses (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL,
        domain      TEXT,
        config_json TEXT,
        created_at  TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS backlink_prospects (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        domain        TEXT NOT NULL UNIQUE,
        contact_email TEXT,
        da_score      INTEGER DEFAULT 0,
        niche         TEXT,
        status        TEXT NOT NULL DEFAULT 'discovered',
        notes         TEXT,
        created_at    TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_bp_status ON backlink_prospects(status);

    CREATE TABLE IF NOT EXISTS outreach_log (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        prospect_domain  TEXT NOT NULL,
        email_type       TEXT NOT NULL,
        subject          TEXT,
        body_preview     TEXT,
        status           TEXT NOT NULL DEFAULT 'sent',
        response_preview TEXT,
        sent_at          TEXT NOT NULL DEFAULT (datetime('now')),
        responded_at     TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_ol_domain ON outreach_log(prospect_domain);

    CREATE TABLE IF NOT EXISTS leads (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        business_id TEXT NOT NULL,
        name        TEXT,
        phone       TEXT,
        email       TEXT,
        message     TEXT,
        source_url  TEXT,
        keyword     TEXT,
        crm_id      TEXT,
        created_at  TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_leads_business ON leads(business_id);

    CREATE TABLE IF NOT EXISTS indexing_queue (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        url          TEXT NOT NULL UNIQUE,
        submitted_at TEXT NOT NULL DEFAULT (datetime('now')),
        check_after  TEXT NOT NULL,
        verified     INTEGER NOT NULL DEFAULT 0,
        retry_count  INTEGER NOT NULL DEFAULT 0
    );
    """

    def __init__(self, db_path: str = "data/seo_engine.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()
        log.info("db.init  path=%s", db_path)

    def _init_schema(self):
        with self._lock:
            self._conn.executescript(self._SCHEMA)
            self._conn.commit()

    def _row_to_dict(self, row) -> dict | None:
        if row is None:
            return None
        return dict(row)

    def _rows_to_list(self, rows) -> list[dict]:
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Published URLs
    # ------------------------------------------------------------------

    def register_url(
        self,
        business_id: str,
        url: str,
        platform: str,
        canonical_url: str,
        slug: str,
        keyword: str,
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                """INSERT OR IGNORE INTO published_urls
                   (business_id, url, platform, canonical_url, slug, keyword)
                   VALUES (?,?,?,?,?,?)""",
                (business_id, url, platform, canonical_url, slug, keyword),
            )
            self._conn.commit()
            if cur.lastrowid:
                return cur.lastrowid
            row = self._conn.execute(
                "SELECT id FROM published_urls WHERE url=?", (url,)
            ).fetchone()
            return row["id"] if row else -1

    def get_url_by_slug(self, slug: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM published_urls WHERE slug=?", (slug,)
            ).fetchone()
            return self._row_to_dict(row)

    def get_urls_by_business(self, business_id: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM published_urls WHERE business_id=? ORDER BY published_at DESC",
                (business_id,),
            ).fetchall()
            return self._rows_to_list(rows)

    def save_content_hash(self, url_id: int, simhash_value: int):
        with self._lock:
            self._conn.execute(
                "UPDATE published_urls SET simhash=? WHERE id=?",
                (simhash_value, url_id),
            )
            self._conn.commit()

    def simhash_exists(self, content_hash: int, threshold: float = 0.85) -> bool:
        """Check if a similar content hash already exists (Hamming distance)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT simhash FROM published_urls WHERE simhash IS NOT NULL"
            ).fetchall()
        max_diff = int((1 - threshold) * 64)
        for row in rows:
            stored = row["simhash"]
            diff = bin(stored ^ content_hash).count("1")
            if diff <= max_diff:
                return True
        return False

    def get_orphan_urls(self, business_id: str) -> list[dict]:
        """URLs with no syndications recorded (proxy for no inbound links)."""
        with self._lock:
            rows = self._conn.execute(
                """SELECT pu.* FROM published_urls pu
                   WHERE pu.business_id=?
                   AND pu.url NOT IN (SELECT canonical_url FROM syndications)""",
                (business_id,),
            ).fetchall()
            return self._rows_to_list(rows)

    # ------------------------------------------------------------------
    # Syndications
    # ------------------------------------------------------------------

    def register_syndication(
        self, primary_url: str, syndicated_url: str, platform: str, canonical_url: str
    ):
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO syndications
                   (primary_url, syndicated_url, platform, canonical_url)
                   VALUES (?,?,?,?)""",
                (primary_url, syndicated_url, platform, canonical_url),
            )
            self._conn.commit()

    def get_syndications(self, primary_url: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM syndications WHERE primary_url=?", (primary_url,)
            ).fetchall()
            return self._rows_to_list(rows)

    # ------------------------------------------------------------------
    # Task Results
    # ------------------------------------------------------------------

    def save_task_result(
        self,
        business_id: str,
        task_type: str,
        keyword: str,
        result_json: Any,
        status: str = "success",
    ):
        if not isinstance(result_json, str):
            result_json = json.dumps(result_json, default=str)
        with self._lock:
            self._conn.execute(
                """INSERT INTO task_results (business_id, task_type, keyword, result_json, status)
                   VALUES (?,?,?,?,?)""",
                (business_id, task_type, keyword, result_json, status),
            )
            self._conn.commit()

    def get_task_results(self, business_id: str, limit: int = 50) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM task_results WHERE business_id=?
                   ORDER BY created_at DESC LIMIT ?""",
                (business_id, limit),
            ).fetchall()
            return self._rows_to_list(rows)

    # ------------------------------------------------------------------
    # Rankings
    # ------------------------------------------------------------------

    def save_ranking(
        self,
        business_id: str,
        keyword: str,
        position: int,
        url: str,
        volume: int = 0,
        date: str = None,
    ):
        recorded_at = date or datetime.now(tz=timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                """INSERT INTO ranking_history
                   (business_id, keyword, position, url, volume, recorded_at)
                   VALUES (?,?,?,?,?,?)""",
                (business_id, keyword, position, url, volume, recorded_at),
            )
            self._conn.commit()

    def get_ranking_history(self, business_id: str, keyword: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT recorded_at, position FROM ranking_history
                   WHERE business_id=? AND keyword=?
                   ORDER BY recorded_at ASC""",
                (business_id, keyword),
            ).fetchall()
            return self._rows_to_list(rows)

    # ------------------------------------------------------------------
    # Anchor Distribution
    # ------------------------------------------------------------------

    def save_anchor_dist(
        self, domain: str, anchor_type: str, anchor_text: str, target_url: str
    ):
        with self._lock:
            self._conn.execute(
                """INSERT INTO anchor_dist (domain, anchor_type, anchor_text, target_url)
                   VALUES (?,?,?,?)""",
                (domain, anchor_type, anchor_text, target_url),
            )
            self._conn.commit()

    def get_anchor_dist(self, domain: str) -> dict:
        with self._lock:
            rows = self._conn.execute(
                """SELECT anchor_type, COUNT(*) as cnt FROM anchor_dist
                   WHERE domain=? GROUP BY anchor_type""",
                (domain,),
            ).fetchall()
        total = sum(r["cnt"] for r in rows) or 1
        return {r["anchor_type"]: {"count": r["cnt"], "pct": r["cnt"] / total} for r in rows}

    # ------------------------------------------------------------------
    # Citation Reports
    # ------------------------------------------------------------------

    def save_citation_report(
        self,
        business_id: str,
        query: str,
        engine: str,
        cited: bool,
        source_url: str,
        report_json: Any,
    ):
        if not isinstance(report_json, str):
            report_json = json.dumps(report_json, default=str)
        with self._lock:
            self._conn.execute(
                """INSERT INTO citation_reports
                   (business_id, query, engine, cited, source_url, report_json)
                   VALUES (?,?,?,?,?,?)""",
                (business_id, query, engine, int(cited), source_url, report_json),
            )
            self._conn.commit()

    def get_citation_summary(self, business_id: str) -> dict:
        with self._lock:
            rows = self._conn.execute(
                """SELECT engine, SUM(cited) as hits, COUNT(*) as total
                   FROM citation_reports WHERE business_id=?
                   GROUP BY engine""",
                (business_id,),
            ).fetchall()
        return {
            r["engine"]: {
                "hits": r["hits"],
                "total": r["total"],
                "rate": round(r["hits"] / max(r["total"], 1), 3),
            }
            for r in rows
        }

    # ------------------------------------------------------------------
    # Businesses
    # ------------------------------------------------------------------

    def save_business(self, business_id: str, name: str, domain: str, config_json: Any):
        if not isinstance(config_json, str):
            config_json = json.dumps(config_json, default=str)
        with self._lock:
            self._conn.execute(
                """INSERT INTO businesses (id, name, domain, config_json)
                   VALUES (?,?,?,?)
                   ON CONFLICT(id) DO UPDATE SET
                     name=excluded.name,
                     domain=excluded.domain,
                     config_json=excluded.config_json,
                     updated_at=datetime('now')""",
                (business_id, name, domain, config_json),
            )
            self._conn.commit()

    def get_business(self, business_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM businesses WHERE id=?", (business_id,)
            ).fetchone()
            return self._row_to_dict(row)

    def get_all_businesses(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM businesses ORDER BY name"
            ).fetchall()
            return self._rows_to_list(rows)

    # ------------------------------------------------------------------
    # Backlink Prospects
    # ------------------------------------------------------------------

    def save_prospect(
        self,
        domain: str,
        contact_email: str,
        da_score: int,
        niche: str,
        status: str = "discovered",
    ):
        with self._lock:
            self._conn.execute(
                """INSERT INTO backlink_prospects (domain, contact_email, da_score, niche, status)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(domain) DO UPDATE SET
                     contact_email=excluded.contact_email,
                     da_score=excluded.da_score,
                     status=excluded.status""",
                (domain, contact_email, da_score, niche, status),
            )
            self._conn.commit()

    def log_outreach(
        self,
        prospect_domain: str,
        email_type: str,
        subject: str,
        body_preview: str,
        status: str = "sent",
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                """INSERT INTO outreach_log
                   (prospect_domain, email_type, subject, body_preview, status)
                   VALUES (?,?,?,?,?)""",
                (prospect_domain, email_type, subject, body_preview[:500], status),
            )
            self._conn.commit()
            return cur.lastrowid

    def update_outreach(
        self, outreach_id: int, status: str, response_preview: str = None
    ):
        with self._lock:
            self._conn.execute(
                """UPDATE outreach_log SET status=?, response_preview=?, responded_at=datetime('now')
                   WHERE id=?""",
                (status, response_preview, outreach_id),
            )
            self._conn.commit()

    def get_outreach_sequence(self, domain: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM outreach_log WHERE prospect_domain=? ORDER BY sent_at",
                (domain,),
            ).fetchall()
            return self._rows_to_list(rows)

    # ------------------------------------------------------------------
    # Leads
    # ------------------------------------------------------------------

    def save_lead(
        self,
        business_id: str,
        name: str,
        phone: str,
        email: str,
        message: str,
        source_url: str,
        keyword: str,
        crm_id: str = None,
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                """INSERT INTO leads
                   (business_id, name, phone, email, message, source_url, keyword, crm_id)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (business_id, name, phone, email, message, source_url, keyword, crm_id),
            )
            self._conn.commit()
            return cur.lastrowid

    def get_leads(self, business_id: str, limit: int = 100) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM leads WHERE business_id=? ORDER BY created_at DESC LIMIT ?",
                (business_id, limit),
            ).fetchall()
            return self._rows_to_list(rows)

    # ------------------------------------------------------------------
    # Indexing Queue
    # ------------------------------------------------------------------

    def queue_url_for_verification(self, url: str, check_after_hours: int = 48):
        from datetime import timedelta
        check_after = (
            datetime.now(tz=timezone.utc) + timedelta(hours=check_after_hours)
        ).isoformat()
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO indexing_queue (url, check_after)
                   VALUES (?,?)""",
                (url, check_after),
            )
            self._conn.commit()

    def get_pending_verifications(self) -> list[dict]:
        now = datetime.now(tz=timezone.utc).isoformat()
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM indexing_queue
                   WHERE verified=0 AND check_after <= ? AND retry_count < 3
                   ORDER BY check_after""",
                (now,),
            ).fetchall()
            return self._rows_to_list(rows)

    def mark_verified(self, url: str):
        with self._lock:
            self._conn.execute(
                "UPDATE indexing_queue SET verified=1 WHERE url=?", (url,)
            )
            self._conn.commit()

    def increment_retry(self, url: str):
        from datetime import timedelta
        new_check = (
            datetime.now(tz=timezone.utc) + timedelta(hours=24)
        ).isoformat()
        with self._lock:
            self._conn.execute(
                "UPDATE indexing_queue SET retry_count=retry_count+1, check_after=? WHERE url=?",
                (new_check, url),
            )
            self._conn.commit()

    # ------------------------------------------------------------------
    # Aggregation / Analytics
    # ------------------------------------------------------------------

    def aggregate_task_stats(self, days: int = 30) -> dict:
        cutoff = datetime.now(tz=timezone.utc)
        from datetime import timedelta
        cutoff_str = (cutoff - timedelta(days=days)).isoformat()
        with self._lock:
            rows = self._conn.execute(
                """SELECT task_type, status, COUNT(*) as cnt
                   FROM task_results
                   WHERE created_at >= ?
                   GROUP BY task_type, status""",
                (cutoff_str,),
            ).fetchall()
        stats: dict = {}
        for row in rows:
            tt = row["task_type"]
            if tt not in stats:
                stats[tt] = {"success": 0, "failure": 0, "total": 0}
            stats[tt][row["status"]] = stats[tt].get(row["status"], 0) + row["cnt"]
            stats[tt]["total"] += row["cnt"]
        for tt in stats:
            total = stats[tt]["total"] or 1
            stats[tt]["success_rate"] = round(stats[tt].get("success", 0) / total, 3)
        return stats

    def get_pending_indexing(self, limit: int = 20) -> list[dict]:
        """Return URLs in the indexing queue that are due for submission."""
        now = datetime.now(tz=timezone.utc).isoformat()
        with self._lock:
            rows = self._conn.execute(
                """SELECT url, business_id, submitted_at, retry_count
                   FROM indexing_queue
                   WHERE indexed=0 AND (check_after IS NULL OR check_after <= ?)
                   ORDER BY submitted_at ASC LIMIT ?""",
                (now, limit),
            ).fetchall()
        return [dict(r) for r in rows]

    def mark_indexed(self, url: str) -> None:
        """Mark a URL as successfully indexed."""
        with self._lock:
            self._conn.execute(
                "UPDATE indexing_queue SET indexed=1 WHERE url=?",
                (url,),
            )
            self._conn.commit()

    def get_businesses(self) -> list[dict]:
        """Return all registered businesses."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT id as business_id, name, config_json FROM businesses ORDER BY created_at DESC"
            ).fetchall()
        import json
        result = []
        for r in rows:
            try:
                config = json.loads(r["config_json"] or "{}")
            except Exception:
                config = {}
            result.append({"business_id": r["business_id"], "name": r["name"], **config})
        return result


    def add_business(self, business_id, name, domain, config):
        """Register a new business in SQLite and sync to businesses.json."""
        import json as _json
        import uuid as _uuid
        from datetime import datetime, timezone
        from pathlib import Path

        bid = business_id or str(_uuid.uuid4())
        config_str = _json.dumps(config)

        with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO businesses (id, name, domain, config_json, updated_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (bid, name, domain, config_str, datetime.now(tz=timezone.utc).isoformat())
            )
            self._conn.commit()

        # Sync to businesses.json (what tasks actually read)
        biz_file = Path("data/storage/businesses.json")
        biz_file.parent.mkdir(parents=True, exist_ok=True)

        businesses = []
        if biz_file.exists():
            try:
                businesses = _json.loads(biz_file.read_text())
            except Exception:
                businesses = []

        # Remove existing entry for this domain/id
        businesses = [b for b in businesses if b.get("domain") != domain and b.get("business_id") != bid]

        # Add new entry
        entry = {"business_id": bid, "name": name, "domain": domain}
        entry.update(config)
        businesses.append(entry)
        biz_file.write_text(_json.dumps(businesses, indent=2))

    def remove_business(self, business_id):
        """Remove a business from SQLite and businesses.json."""
        import json as _json
        from pathlib import Path

        with self._lock:
            self._conn.execute("DELETE FROM businesses WHERE id = ?", (business_id,))
            self._conn.commit()

        biz_file = Path("data/storage/businesses.json")
        if biz_file.exists():
            businesses = _json.loads(biz_file.read_text())
            orig_len = len(businesses)
            businesses = [b for b in businesses if b.get("business_id") != business_id]
            if len(businesses) < orig_len:
                biz_file.write_text(_json.dumps(businesses, indent=2))
                return True
        return False

    def close(self):
        self._conn.close()


# Module-level singleton
_db_instance: SEODatabase | None = None
_db_lock = threading.Lock()


def get_db(db_path: str = "data/seo_engine.db") -> SEODatabase:
    """Get or create the shared SEODatabase instance."""
    global _db_instance
    with _db_lock:
        if _db_instance is None:
            _db_instance = SEODatabase(db_path)
    return _db_instance
