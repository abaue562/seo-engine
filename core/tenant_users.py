import hashlib, json, logging, secrets, sqlite3
from typing import Dict, List, Optional
import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
DB_PATH = "data/storage/seo_engine.db"
VALID_ROLES = ["admin", "editor", "viewer"]

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS tenant_users (
        id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, user_id TEXT,
        email TEXT, role TEXT DEFAULT 'viewer',
        invited_by TEXT, invite_token TEXT UNIQUE,
        invited_at TEXT DEFAULT (datetime('now')), joined_at TEXT,
        active INTEGER DEFAULT 1
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tu_tenant ON tenant_users(tenant_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_tu_token ON tenant_users(invite_token)")
    c.commit()
    return c

def invite_user(tenant_id: str, email: str, role: str, invited_by: str) -> Optional[str]:
    if role not in VALID_ROLES:
        raise ValueError(f"Invalid role: {role}")
    limit_check = check_seat_limit(tenant_id)
    if not limit_check["has_capacity"]:
        log.warning("tenant_users.invite_limit  tenant=%s", tenant_id)
        return None
    token = secrets.token_urlsafe(24)
    uid = hashlib.sha256(f"{tenant_id}:{email}".encode()).hexdigest()[:16]
    conn = _conn()
    conn.execute("INSERT OR REPLACE INTO tenant_users (id, tenant_id, email, role, invited_by, invite_token) VALUES (?,?,?,?,?,?)",
                 [uid, tenant_id, email, role, invited_by, token])
    conn.commit()
    conn.close()
    log.info("tenant_users.invited  tenant=%s  email=%s  role=%s", tenant_id, email, role)
    return token

def accept_invite(token: str, user_id: str) -> bool:
    conn = _conn()
    row = conn.execute("SELECT id FROM tenant_users WHERE invite_token=? AND joined_at IS NULL", [token]).fetchone()
    if not row:
        conn.close()
        return False
    conn.execute("UPDATE tenant_users SET user_id=?, joined_at=datetime('now'), invite_token=NULL WHERE id=?", [user_id, row[0]])
    conn.commit()
    conn.close()
    log.info("tenant_users.accepted  user=%s", user_id)
    return True

def get_tenant_users(tenant_id: str) -> List[Dict]:
    conn = _conn()
    rows = conn.execute("SELECT id, user_id, email, role, joined_at, active FROM tenant_users WHERE tenant_id=? ORDER BY joined_at", [tenant_id]).fetchall()
    conn.close()
    return [{"id": r[0], "user_id": r[1], "email": r[2], "role": r[3], "joined_at": r[4], "active": bool(r[5])} for r in rows]

def check_seat_limit(tenant_id: str) -> Dict:
    try:
        from core.pricing import get_tenant_plan, get_plan
        plan = get_plan(get_tenant_plan(tenant_id))
        seat_limit = plan.get("seats", 1)
    except Exception:
        seat_limit = 2
    conn = _conn()
    used = conn.execute("SELECT COUNT(*) FROM tenant_users WHERE tenant_id=? AND active=1", [tenant_id]).fetchone()[0]
    conn.close()
    return {"seat_limit": seat_limit, "seats_used": used, "has_capacity": used < seat_limit}

def get_user_role(tenant_id: str, user_id: str) -> str:
    conn = _conn()
    row = conn.execute("SELECT role FROM tenant_users WHERE tenant_id=? AND user_id=? AND active=1", [tenant_id, user_id]).fetchone()
    conn.close()
    return row[0] if row else ""

def remove_user(tenant_id: str, user_id: str) -> bool:
    conn = _conn()
    cur = conn.execute("UPDATE tenant_users SET active=0 WHERE tenant_id=? AND user_id=?", [tenant_id, user_id])
    conn.commit()
    conn.close()
    return cur.rowcount > 0
