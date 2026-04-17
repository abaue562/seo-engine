import hashlib, json, logging, os, secrets, sqlite3
from typing import Dict, List, Optional

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"
VALID_SCOPES = ["read:content", "write:content", "read:analytics", "admin:tenant"]

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS api_keys (
        id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, name TEXT NOT NULL,
        key_hash TEXT UNIQUE NOT NULL, scopes TEXT DEFAULT '["read:content"]',
        last_used_at TEXT, active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now'))
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_apikeys_tenant ON api_keys(tenant_id)")
    c.commit()
    return c

def _hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()

def create_api_key(tenant_id: str, name: str, scopes: List[str]) -> str:
    invalid = [s for s in scopes if s not in VALID_SCOPES]
    if invalid:
        raise ValueError(f"Invalid scopes: {invalid}")
    raw = "seo_live_" + secrets.token_hex(16)
    uid = hashlib.sha256(f"{tenant_id}:{name}".encode()).hexdigest()[:16]
    conn = _conn()
    conn.execute("INSERT OR REPLACE INTO api_keys (id, tenant_id, name, key_hash, scopes) VALUES (?,?,?,?,?)",
                 [uid, tenant_id, name, _hash_key(raw), json.dumps(scopes)])
    conn.commit()
    conn.close()
    log.info("api_keys.created  tenant=%s  name=%s", tenant_id, name)
    return raw

def validate_api_key(raw_key: str) -> Optional[Dict]:
    if not raw_key or not raw_key.startswith("seo_live_"):
        return None
    key_hash = _hash_key(raw_key)
    conn = _conn()
    row = conn.execute("SELECT id, tenant_id, scopes, active FROM api_keys WHERE key_hash=?", [key_hash]).fetchone()
    if not row or not row[3]:
        conn.close()
        return None
    conn.execute("UPDATE api_keys SET last_used_at=datetime('now') WHERE id=?", [row[0]])
    conn.commit()
    conn.close()
    return {"key_id": row[0], "tenant_id": row[1], "scopes": json.loads(row[2])}

def check_scope(raw_key: str, required_scope: str) -> bool:
    result = validate_api_key(raw_key)
    if not result:
        return False
    return required_scope in result["scopes"] or "admin:tenant" in result["scopes"]

def revoke_api_key(key_id: str, tenant_id: str) -> bool:
    conn = _conn()
    cur = conn.execute("UPDATE api_keys SET active=0 WHERE id=? AND tenant_id=?", [key_id, tenant_id])
    conn.commit()
    conn.close()
    return cur.rowcount > 0

def list_api_keys(tenant_id: str) -> List[Dict]:
    conn = _conn()
    rows = conn.execute("SELECT id, name, scopes, last_used_at, created_at, active FROM api_keys WHERE tenant_id=? ORDER BY created_at DESC", [tenant_id]).fetchall()
    conn.close()
    return [{"id": r[0], "name": r[1], "scopes": json.loads(r[2]), "last_used_at": r[3], "created_at": r[4], "active": bool(r[5])} for r in rows]
