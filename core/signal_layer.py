import hashlib, json, logging, math, random, sqlite3
from datetime import datetime
from typing import Dict, List, Optional

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"
K_ANONYMITY_MIN = 20
NOISE_THRESHOLD = 50  # add noise for cohorts < 50 tenants

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS platform_performance_signals (
        id TEXT PRIMARY KEY, cohort_fingerprint TEXT NOT NULL,
        industry_code TEXT, location_tier TEXT, intent TEXT, volume_bucket TEXT,
        pattern_key TEXT NOT NULL, success_count INTEGER DEFAULT 0,
        total_count INTEGER DEFAULT 0, avg_rank_at_90d REAL DEFAULT 0,
        confidence REAL DEFAULT 0, tenant_count INTEGER DEFAULT 0,
        last_updated TEXT DEFAULT (datetime('now'))
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_sig_cohort ON platform_performance_signals(cohort_fingerprint)")
    c.execute("""CREATE TABLE IF NOT EXISTS signal_contributions (
        id TEXT PRIMARY KEY, business_id TEXT NOT NULL,
        signal_id TEXT NOT NULL, contributed_at TEXT DEFAULT (datetime('now'))
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS signal_opt_outs (
        business_id TEXT PRIMARY KEY, opted_out_at TEXT DEFAULT (datetime('now'))
    )""")
    c.commit()
    return c

def get_cohort_fingerprint(industry_code: str, location_tier: str, intent: str, volume_bucket: str) -> str:
    key = f"{industry_code}|{location_tier}|{intent}|{volume_bucket}"
    return hashlib.sha256(key.encode()).hexdigest()[:24]

def _is_opted_out(business_id: str) -> bool:
    conn = _conn()
    row = conn.execute("SELECT 1 FROM signal_opt_outs WHERE business_id=?", [business_id]).fetchone()
    conn.close()
    return bool(row)

def contribute_signal(business_id: str, cohort_fingerprint: str, pattern_key: str,
                      success: bool, rank_at_90d: Optional[float] = None,
                      industry_code: str = "", location_tier: str = "",
                      intent: str = "", volume_bucket: str = ""):
    if _is_opted_out(business_id):
        return
    signal_id = hashlib.sha256(f"{cohort_fingerprint}:{pattern_key}".encode()).hexdigest()[:16]
    conn = _conn()
    conn.execute("""INSERT OR IGNORE INTO platform_performance_signals
        (id, cohort_fingerprint, industry_code, location_tier, intent, volume_bucket, pattern_key)
        VALUES (?,?,?,?,?,?,?)""",
        [signal_id, cohort_fingerprint, industry_code, location_tier, intent, volume_bucket, pattern_key])
    if success:
        conn.execute("UPDATE platform_performance_signals SET success_count=success_count+1, total_count=total_count+1, last_updated=datetime('now') WHERE id=?", [signal_id])
    else:
        conn.execute("UPDATE platform_performance_signals SET total_count=total_count+1, last_updated=datetime('now') WHERE id=?", [signal_id])
    if rank_at_90d is not None:
        conn.execute("""UPDATE platform_performance_signals SET
            avg_rank_at_90d = (avg_rank_at_90d * total_count + ?) / (total_count + 1),
            last_updated=datetime('now') WHERE id=?""", [rank_at_90d, signal_id])
    conn.execute("""UPDATE platform_performance_signals SET
        confidence = CAST(success_count AS REAL) / MAX(total_count, 1),
        tenant_count = (SELECT COUNT(DISTINCT business_id) FROM signal_contributions WHERE signal_id=?)
        WHERE id=?""", [signal_id, signal_id])
    audit_id = hashlib.sha256(f"{business_id}:{signal_id}:{datetime.utcnow().isoformat()}".encode()).hexdigest()[:16]
    conn.execute("INSERT OR IGNORE INTO signal_contributions (id, business_id, signal_id) VALUES (?,?,?)",
                 [audit_id, business_id, signal_id])
    conn.commit()
    conn.close()
    log.debug("signal_layer.contributed  biz=%s  cohort=%s  pattern=%s", business_id, cohort_fingerprint[:8], pattern_key)

def _add_noise(value: float, sensitivity: float = 0.5) -> float:
    # Laplace mechanism: noise scale = sensitivity / epsilon; epsilon=1.0
    noise = random.expovariate(1.0 / sensitivity)
    if random.random() < 0.5:
        noise = -noise
    return round(value + noise, 3)

def get_cohort_patterns(cohort_fingerprint: str, min_confidence: float = 0.6) -> List[Dict]:
    conn = _conn()
    rows = conn.execute("""
        SELECT pattern_key, confidence, success_count, total_count, avg_rank_at_90d, tenant_count
        FROM platform_performance_signals
        WHERE cohort_fingerprint=? AND tenant_count >= ? AND confidence >= ?
        ORDER BY confidence DESC LIMIT 10
    """, [cohort_fingerprint, K_ANONYMITY_MIN, min_confidence]).fetchall()
    conn.close()
    results = []
    for pattern_key, confidence, successes, total, avg_rank, tenant_count in rows:
        if tenant_count < NOISE_THRESHOLD:
            confidence = _add_noise(confidence, sensitivity=0.05)
            avg_rank = _add_noise(avg_rank, sensitivity=1.0) if avg_rank else None
        results.append({
            "pattern_key": pattern_key, "confidence": round(max(0, min(1, confidence)), 3),
            "success_rate": round(successes / max(total, 1), 3),
            "avg_rank_at_90d": round(avg_rank, 1) if avg_rank else None,
            "tenant_count": tenant_count,
            "k_anonymous": tenant_count >= K_ANONYMITY_MIN,
        })
    return results

def opt_out(business_id: str):
    conn = _conn()
    conn.execute("INSERT OR IGNORE INTO signal_opt_outs (business_id) VALUES (?)", [business_id])
    conn.commit()
    conn.close()
    log.info("signal_layer.opt_out  biz=%s", business_id)

def opt_in(business_id: str):
    conn = _conn()
    conn.execute("DELETE FROM signal_opt_outs WHERE business_id=?", [business_id])
    conn.commit()
    conn.close()
    log.info("signal_layer.opt_in  biz=%s", business_id)

def get_signal_stats() -> Dict:
    conn = _conn()
    total = conn.execute("SELECT COUNT(*) FROM platform_performance_signals").fetchone()[0]
    cohorts = conn.execute("SELECT COUNT(DISTINCT cohort_fingerprint) FROM platform_performance_signals").fetchone()[0]
    k_anon = conn.execute("SELECT COUNT(*) FROM platform_performance_signals WHERE tenant_count >= ?", [K_ANONYMITY_MIN]).fetchone()[0]
    avg_tc = conn.execute("SELECT AVG(tenant_count) FROM platform_performance_signals").fetchone()[0]
    conn.close()
    return {"total_signals": total, "unique_cohorts": cohorts, "k_anonymous_signals": k_anon, "avg_tenant_count": round(avg_tc or 0, 1)}
