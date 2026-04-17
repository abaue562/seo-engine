import json, logging, sqlite3, hashlib
from typing import Dict, List, Optional, Tuple
import numpy as np

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS content_patterns (
        id TEXT PRIMARY KEY,
        pattern_key TEXT UNIQUE NOT NULL,
        alpha REAL DEFAULT 1.0,
        beta REAL DEFAULT 1.0,
        sample_count INTEGER DEFAULT 0,
        last_updated TEXT DEFAULT (datetime('now')),
        meta TEXT DEFAULT '{}'
    )""")
    c.commit()
    return c

def record_outcome(pattern_key: str, success: bool, meta: dict = None):
    conn = _conn()
    uid = hashlib.sha256(pattern_key.encode()).hexdigest()
    conn.execute("""INSERT INTO content_patterns (id, pattern_key, alpha, beta, sample_count, meta)
        VALUES (?,?,1,1,0,?) ON CONFLICT(pattern_key) DO NOTHING""",
        [uid, pattern_key, json.dumps(meta or {})])
    if success:
        conn.execute("UPDATE content_patterns SET alpha=alpha+1, sample_count=sample_count+1, last_updated=datetime('now') WHERE pattern_key=?", [pattern_key])
    else:
        conn.execute("UPDATE content_patterns SET beta=beta+1, sample_count=sample_count+1, last_updated=datetime('now') WHERE pattern_key=?", [pattern_key])
    conn.commit()
    conn.close()
    log.debug("learning_loop.record  key=%s  success=%s", pattern_key, success)

def thompson_sample(pattern_keys: List[str]) -> Optional[str]:
    if not pattern_keys:
        return None
    conn = _conn()
    rows = conn.execute(f"SELECT pattern_key, alpha, beta FROM content_patterns WHERE pattern_key IN ({','.join('?'*len(pattern_keys))})", pattern_keys).fetchall()
    conn.close()
    known = {r[0]: (r[1], r[2]) for r in rows}
    samples = {}
    for k in pattern_keys:
        a, b = known.get(k, (1.0, 1.0))
        samples[k] = float(np.random.beta(a, b))
    return max(samples, key=lambda k: samples[k])

def get_pattern_confidence(pattern_key: str) -> Dict:
    conn = _conn()
    row = conn.execute("SELECT alpha, beta, sample_count FROM content_patterns WHERE pattern_key=?", [pattern_key]).fetchone()
    conn.close()
    if not row:
        return {"mean": 0.5, "lower": 0.0, "upper": 1.0, "sample_count": 0}
    a, b, n = row
    mean = a / (a + b)
    from scipy import stats
    lower, upper = stats.beta.interval(0.95, a, b)
    return {"mean": round(mean, 3), "lower": round(lower, 3), "upper": round(upper, 3), "sample_count": n}

def get_top_patterns(limit: int = 10) -> List[Dict]:
    conn = _conn()
    rows = conn.execute("SELECT pattern_key, alpha, beta, sample_count FROM content_patterns WHERE sample_count >= 3 ORDER BY alpha*1.0/(alpha+beta) DESC LIMIT ?", [limit]).fetchall()
    conn.close()
    return [{"pattern_key": r[0], "mean": round(r[1]/(r[1]+r[2]), 3), "sample_count": r[3]} for r in rows]

def attribute_content(content_id: str, provenance: dict):
    import redis
    r = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
    r.setex(f"provenance:{content_id}", 86400 * 180, json.dumps(provenance))
