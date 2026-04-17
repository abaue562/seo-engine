import json, logging, sqlite3, hashlib
from typing import Dict

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

DEFAULTS = {
    "min_entropy": 3.8, "min_word_count_ratio": 0.9, "min_h2_count": 2,
    "max_ai_tell_ratio": 0.15, "min_readability": 0.6,
}

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS validator_thresholds (
        id TEXT PRIMARY KEY, name TEXT UNIQUE NOT NULL, value REAL NOT NULL,
        min_val REAL, max_val REAL, rejection_rate REAL DEFAULT 0,
        rank_correlation REAL DEFAULT 0, last_tuned TEXT DEFAULT (datetime('now'))
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS threshold_events (
        id TEXT PRIMARY KEY, threshold_name TEXT, value_checked REAL,
        passed INTEGER, content_id TEXT, created_at TEXT DEFAULT (datetime('now'))
    )""")
    c.commit()
    return c

def _ensure_defaults():
    conn = _conn()
    ranges = {"min_entropy": (2.5, 5.0), "min_word_count_ratio": (0.5, 1.0), "min_h2_count": (1, 5), "max_ai_tell_ratio": (0.05, 0.4), "min_readability": (0.3, 0.9)}
    for name, default in DEFAULTS.items():
        uid = hashlib.sha256(name.encode()).hexdigest()[:16]
        lo, hi = ranges.get(name, (default * 0.5, default * 2))
        conn.execute("INSERT OR IGNORE INTO validator_thresholds (id, name, value, min_val, max_val) VALUES (?,?,?,?,?)", [uid, name, default, lo, hi])
    conn.commit()
    conn.close()

def get_threshold(name: str) -> float:
    _ensure_defaults()
    conn = _conn()
    row = conn.execute("SELECT value FROM validator_thresholds WHERE name=?", [name]).fetchone()
    conn.close()
    return row[0] if row else DEFAULTS.get(name, 0.5)

def get_all_thresholds() -> Dict[str, float]:
    _ensure_defaults()
    conn = _conn()
    rows = conn.execute("SELECT name, value FROM validator_thresholds").fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}

def record_validation_result(threshold_name: str, value_checked: float, passed: bool, content_id: str = ""):
    conn = _conn()
    uid = hashlib.sha256(f"{threshold_name}:{content_id}:{value_checked}".encode()).hexdigest()[:16]
    conn.execute("INSERT OR IGNORE INTO threshold_events (id, threshold_name, value_checked, passed, content_id) VALUES (?,?,?,?,?)",
                 [uid, threshold_name, value_checked, 1 if passed else 0, content_id])
    conn.commit()
    conn.close()

def tune_thresholds():
    _ensure_defaults()
    conn = _conn()
    thresholds = conn.execute("SELECT name, value, min_val, max_val FROM validator_thresholds").fetchall()
    tuned = 0
    for name, current_val, min_val, max_val in thresholds:
        events = conn.execute("SELECT passed, content_id FROM threshold_events WHERE threshold_name=? ORDER BY created_at DESC LIMIT 200", [name]).fetchall()
        if len(events) < 20:
            continue
        rejection_rate = sum(1 for e in events if not e[0]) / len(events)
        if rejection_rate > 0.5:
            new_val = max(min_val, current_val * 0.95)
        elif rejection_rate < 0.05:
            new_val = min(max_val, current_val * 1.05)
        else:
            continue
        conn.execute("UPDATE validator_thresholds SET value=?, rejection_rate=?, last_tuned=datetime('now') WHERE name=?", [new_val, rejection_rate, name])
        log.info("threshold_tuner.adjusted  name=%s  old=%.3f  new=%.3f  rejection_rate=%.2f", name, current_val, new_val, rejection_rate)
        tuned += 1
    conn.commit()
    conn.close()
    return tuned
