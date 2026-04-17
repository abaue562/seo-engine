import hashlib, json, logging, sqlite3
from datetime import datetime
from typing import Dict, List, Optional

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

COMPONENTS = ["brief_prompt", "content_prompt", "llm_judge_rubric", "intent_classifier", "pattern_retrieval", "generation_pipeline"]

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS ai_version_registry (
        id TEXT PRIMARY KEY, component TEXT NOT NULL, version TEXT NOT NULL,
        config_hash TEXT, deployed_at TEXT DEFAULT (datetime('now')),
        deprecated_at TEXT, active INTEGER DEFAULT 1, notes TEXT
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_reg_component ON ai_version_registry(component, active)")
    c.execute("""CREATE TABLE IF NOT EXISTS ai_version_outcomes (
        id TEXT PRIMARY KEY, registry_id TEXT NOT NULL,
        evaluated_at TEXT DEFAULT (datetime('now')), sample_size INTEGER DEFAULT 0,
        rank_at_90d_avg REAL, time_to_first_rank_avg REAL,
        indexing_success_rate REAL, needs_review_rate REAL
    )""")
    c.commit()
    return c

def register_version(component: str, version: str, config: dict = None, notes: str = "") -> str:
    config_hash = hashlib.sha256(json.dumps(config or {}, sort_keys=True).encode()).hexdigest()[:16]
    uid = hashlib.sha256(f"{component}:{version}:{config_hash}".encode()).hexdigest()[:16]
    conn = _conn()
    conn.execute("INSERT OR IGNORE INTO ai_version_registry (id, component, version, config_hash, notes) VALUES (?,?,?,?,?)",
                 [uid, component, version, config_hash, notes])
    conn.commit()
    conn.close()
    log.info("ai_version_registry.registered  component=%s  version=%s", component, version)
    return uid

def get_active_version(component: str) -> Dict:
    conn = _conn()
    row = conn.execute("SELECT id, component, version, config_hash, deployed_at, notes FROM ai_version_registry WHERE component=? AND active=1 ORDER BY deployed_at DESC LIMIT 1", [component]).fetchone()
    conn.close()
    if not row:
        return {}
    return {"id": row[0], "component": row[1], "version": row[2], "config_hash": row[3], "deployed_at": row[4], "notes": row[5]}

def deprecate_version(registry_id: str):
    conn = _conn()
    conn.execute("UPDATE ai_version_registry SET active=0, deprecated_at=datetime('now') WHERE id=?", [registry_id])
    conn.commit()
    conn.close()

def promote_version(registry_id: str, component: str):
    conn = _conn()
    conn.execute("UPDATE ai_version_registry SET active=0, deprecated_at=datetime('now') WHERE component=? AND id!=?", [component, registry_id])
    conn.execute("UPDATE ai_version_registry SET active=1, deprecated_at=NULL WHERE id=?", [registry_id])
    conn.commit()
    conn.close()
    log.info("ai_version_registry.promoted  id=%s  component=%s", registry_id, component)

def evaluate_version(registry_id: str) -> Dict:
    conn = _conn()
    reg = conn.execute("SELECT component, version, deployed_at FROM ai_version_registry WHERE id=?", [registry_id]).fetchone()
    if not reg:
        conn.close()
        return {"error": "not found"}
    component, version, deployed_at = reg
    rows = conn.execute("""
        SELECT COUNT(*), AVG(o.rank_median), AVG(o.indexed)
        FROM content_provenance p
        JOIN content_outcomes o ON p.content_id = o.content_id AND o.snapshot_days = 90
        WHERE p.prompt_version = ? AND o.rollback_flag = 0
    """, [version]).fetchone()
    conn.close()
    sample_size = rows[0] or 0
    if sample_size < 5:
        return {"registry_id": registry_id, "status": "insufficient_data", "sample_size": sample_size}
    uid = hashlib.sha256(f"{registry_id}:{datetime.utcnow().date().isoformat()}".encode()).hexdigest()[:16]
    db = _conn()
    db.execute("INSERT OR REPLACE INTO ai_version_outcomes (id, registry_id, sample_size, rank_at_90d_avg, indexing_success_rate) VALUES (?,?,?,?,?)",
               [uid, registry_id, sample_size, rows[1], rows[2]])
    db.commit()
    db.close()
    result = {"registry_id": registry_id, "component": component, "version": version, "sample_size": sample_size, "rank_at_90d_avg": round(rows[1] or 0, 1), "indexing_success_rate": round(rows[2] or 0, 3)}
    log.info("ai_version_registry.evaluated  component=%s  version=%s  samples=%d", component, version, sample_size)
    return result

def get_version_history(component: str) -> List[Dict]:
    conn = _conn()
    rows = conn.execute("""
        SELECT r.id, r.version, r.active, r.deployed_at, r.deprecated_at, r.notes,
               o.rank_at_90d_avg, o.indexing_success_rate, o.sample_size
        FROM ai_version_registry r
        LEFT JOIN ai_version_outcomes o ON r.id = o.registry_id
        WHERE r.component=? ORDER BY r.deployed_at DESC
    """, [component]).fetchall()
    conn.close()
    return [{"id": r[0], "version": r[1], "active": bool(r[2]), "deployed_at": r[3], "deprecated_at": r[4], "notes": r[5], "rank_at_90d_avg": r[6], "indexing_success_rate": r[7], "sample_size": r[8]} for r in rows]
