import json, logging, sqlite3, hashlib, random
from typing import Optional, Dict, List

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS prompts (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        version INTEGER DEFAULT 1,
        template TEXT NOT NULL,
        variables TEXT DEFAULT '[]',
        complexity_tier TEXT DEFAULT 'fast',
        active INTEGER DEFAULT 1,
        win_count INTEGER DEFAULT 0,
        use_count INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_prompts_name ON prompts(name)")
    c.commit()
    return c

def register_prompt(name: str, template: str, variables: list = None, complexity_tier: str = "fast") -> str:
    conn = _conn()
    max_ver = conn.execute("SELECT MAX(version) FROM prompts WHERE name=?", [name]).fetchone()[0] or 0
    new_ver = max_ver + 1
    uid = hashlib.sha256(f"{name}:{new_ver}".encode()).hexdigest()[:16]
    conn.execute("INSERT INTO prompts (id, name, version, template, variables, complexity_tier) VALUES (?,?,?,?,?,?)",
                 [uid, name, new_ver, template, json.dumps(variables or []), complexity_tier])
    conn.commit()
    conn.close()
    log.info("prompt_library.registered  name=%s  version=%d", name, new_ver)
    return uid

def get_prompt(name: str, context: dict = None) -> Optional[Dict]:
    conn = _conn()
    rows = conn.execute("SELECT id, template, version, win_count, use_count FROM prompts WHERE name=? AND active=1 ORDER BY version DESC", [name]).fetchall()
    conn.close()
    if not rows:
        return None
    weights = []
    for _, _, _, wins, uses in rows:
        win_rate = (wins + 1) / (uses + 2)  # Laplace smoothed
        weights.append(win_rate)
    total = sum(weights)
    probs = [w / total for w in weights]
    chosen_idx = random.choices(range(len(rows)), weights=probs, k=1)[0]
    pid, template, ver, _, _ = rows[chosen_idx]
    if context:
        try:
            template = template.format(**context)
        except KeyError:
            pass
    return {"id": pid, "name": name, "version": ver, "template": template}

def record_prompt_outcome(prompt_id: str, success: bool):
    conn = _conn()
    if success:
        conn.execute("UPDATE prompts SET win_count=win_count+1, use_count=use_count+1 WHERE id=?", [prompt_id])
    else:
        conn.execute("UPDATE prompts SET use_count=use_count+1 WHERE id=?", [prompt_id])
    conn.commit()
    conn.close()

def rollback_prompt(name: str, version: int):
    conn = _conn()
    conn.execute("UPDATE prompts SET active=0 WHERE name=? AND version > ?", [name, version])
    conn.execute("UPDATE prompts SET active=1 WHERE name=? AND version=?", [name, version])
    conn.commit()
    conn.close()
    log.info("prompt_library.rollback  name=%s  to_version=%d", name, version)

def list_prompts() -> List[Dict]:
    conn = _conn()
    rows = conn.execute("SELECT name, version, active, win_count, use_count, created_at FROM prompts ORDER BY name, version DESC").fetchall()
    conn.close()
    return [{"name": r[0], "version": r[1], "active": bool(r[2]), "win_rate": round((r[3]+1)/(r[4]+2), 2), "uses": r[4]} for r in rows]
