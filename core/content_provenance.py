import hashlib, json, logging, sqlite3
from datetime import datetime
from typing import Dict, List, Optional

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS content_provenance (
        content_id TEXT PRIMARY KEY, business_id TEXT NOT NULL,
        prompt_version TEXT, brief_template_version TEXT,
        model_provider TEXT, model_name TEXT, complexity_tier TEXT,
        passes_run TEXT DEFAULT '[]', generation_cost_cents REAL DEFAULT 0,
        generated_at TEXT DEFAULT (datetime('now')),
        keyword TEXT, intent TEXT, cohort_fingerprint TEXT
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_prov_biz ON content_provenance(business_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_prov_cohort ON content_provenance(cohort_fingerprint)")
    c.execute("""CREATE TABLE IF NOT EXISTS content_outcomes (
        id TEXT PRIMARY KEY, content_id TEXT NOT NULL,
        snapshot_days INTEGER NOT NULL, rank_median REAL,
        impressions INTEGER DEFAULT 0, clicks INTEGER DEFAULT 0,
        ctr REAL DEFAULT 0, leads INTEGER DEFAULT 0,
        indexed INTEGER DEFAULT 0, rollback_flag INTEGER DEFAULT 0,
        captured_at TEXT DEFAULT (datetime('now'))
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_out_content ON content_outcomes(content_id)")
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_out_unique ON content_outcomes(content_id, snapshot_days)")
    c.commit()
    return c

def record_provenance(content_id: str, business_id: str, *, prompt_version: str = "",
                      model_name: str = "", model_provider: str = "anthropic",
                      complexity_tier: str = "smart", passes_run: list = None,
                      cost_cents: float = 0, keyword: str = "", intent: str = "",
                      cohort_fingerprint: str = "", brief_template_version: str = "1"):
    conn = _conn()
    conn.execute("""INSERT OR IGNORE INTO content_provenance
        (content_id, business_id, prompt_version, brief_template_version, model_provider,
         model_name, complexity_tier, passes_run, generation_cost_cents, keyword, intent, cohort_fingerprint)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        [content_id, business_id, prompt_version, brief_template_version, model_provider,
         model_name, complexity_tier, json.dumps(passes_run or []),
         cost_cents, keyword, intent, cohort_fingerprint])
    conn.commit()
    conn.close()
    log.debug("content_provenance.recorded  content_id=%s  keyword=%s", content_id, keyword)

def record_outcome_snapshot(content_id: str, snapshot_days: int, *, rank_median: Optional[float] = None,
                             impressions: int = 0, clicks: int = 0, ctr: float = 0,
                             leads: int = 0, indexed: int = 0, rollback_flag: int = 0):
    uid = hashlib.sha256(f"{content_id}:{snapshot_days}".encode()).hexdigest()[:16]
    conn = _conn()
    conn.execute("""INSERT OR REPLACE INTO content_outcomes
        (id, content_id, snapshot_days, rank_median, impressions, clicks, ctr, leads, indexed, rollback_flag)
        VALUES (?,?,?,?,?,?,?,?,?,?)""",
        [uid, content_id, snapshot_days, rank_median, impressions, clicks, ctr, leads, indexed, rollback_flag])
    conn.commit()
    conn.close()
    log.debug("content_provenance.outcome_snapshot  content_id=%s  days=%d  rank=%s", content_id, snapshot_days, rank_median)

def get_provenance(content_id: str) -> Dict:
    conn = _conn()
    row = conn.execute("SELECT * FROM content_provenance WHERE content_id=?", [content_id]).fetchone()
    conn.close()
    if not row:
        return {}
    cols = ["content_id","business_id","prompt_version","brief_template_version","model_provider","model_name","complexity_tier","passes_run","generation_cost_cents","generated_at","keyword","intent","cohort_fingerprint"]
    return dict(zip(cols, row))

def get_outcomes(content_id: str) -> List[Dict]:
    conn = _conn()
    rows = conn.execute("SELECT snapshot_days, rank_median, impressions, clicks, ctr, leads, indexed, rollback_flag, captured_at FROM content_outcomes WHERE content_id=? ORDER BY snapshot_days", [content_id]).fetchall()
    conn.close()
    return [{"snapshot_days": r[0], "rank_median": r[1], "impressions": r[2], "clicks": r[3], "ctr": r[4], "leads": r[5], "indexed": bool(r[6]), "rollback": bool(r[7]), "captured_at": r[8]} for r in rows]

def get_corpus_stats() -> Dict:
    conn = _conn()
    total = conn.execute("SELECT COUNT(*) FROM content_provenance").fetchone()[0]
    with_90d = conn.execute("SELECT COUNT(DISTINCT content_id) FROM content_outcomes WHERE snapshot_days=90").fetchone()[0]
    avg_rank = conn.execute("SELECT AVG(rank_median) FROM content_outcomes WHERE snapshot_days=90 AND rank_median IS NOT NULL AND rank_median <= 25").fetchone()[0]
    conn.close()
    return {"total_pages": total, "pages_with_90d_outcome": with_90d, "pct_with_outcomes": round(with_90d / max(total, 1) * 100, 1), "avg_rank_at_90d_top_quartile": round(avg_rank or 0, 1)}

def export_corpus_sample(limit: int = 1000) -> List[Dict]:
    conn = _conn()
    rows = conn.execute("""
        SELECT p.content_id, p.business_id, p.model_name, p.complexity_tier, p.keyword, p.intent,
               p.cohort_fingerprint, p.generation_cost_cents, p.generated_at,
               o.rank_median, o.impressions, o.clicks, o.indexed
        FROM content_provenance p
        LEFT JOIN content_outcomes o ON p.content_id = o.content_id AND o.snapshot_days = 90
        WHERE o.rollback_flag = 0 OR o.rollback_flag IS NULL
        ORDER BY RANDOM() LIMIT ?
    """, [limit]).fetchall()
    conn.close()
    return [{"content_id": r[0], "business_id": r[1], "model": r[2], "tier": r[3], "keyword": r[4], "intent": r[5], "cohort": r[6], "cost_cents": r[7], "generated_at": r[8], "rank_at_90d": r[9], "impressions": r[10], "clicks": r[11], "indexed": bool(r[12])} for r in rows]
