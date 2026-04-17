import hashlib, hmac, json, logging, sqlite3, time
from typing import Dict, List
import requests

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS webhook_subscriptions (
        id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, event_type TEXT NOT NULL,
        url TEXT NOT NULL, secret TEXT NOT NULL, active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now'))
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS webhook_delivery_log (
        id TEXT PRIMARY KEY, subscription_id TEXT, event_type TEXT,
        payload_hash TEXT, status TEXT DEFAULT 'pending', attempts INTEGER DEFAULT 0,
        last_attempt_at TEXT, response_code INTEGER
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_wh_tenant ON webhook_subscriptions(tenant_id)")
    c.commit()
    return c

def register_webhook(tenant_id: str, event_type: str, url: str, secret: str) -> str:
    uid = hashlib.sha256(f"{tenant_id}:{event_type}:{url}".encode()).hexdigest()[:16]
    conn = _conn()
    conn.execute("INSERT OR REPLACE INTO webhook_subscriptions (id, tenant_id, event_type, url, secret) VALUES (?,?,?,?,?)",
                 [uid, tenant_id, event_type, url, secret])
    conn.commit()
    conn.close()
    log.info("webhooks.registered  tenant=%s  event=%s", tenant_id, event_type)
    return uid

def _sign(secret: str, payload_bytes: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), payload_bytes, hashlib.sha256).hexdigest()

def deliver(subscription_id: str, url: str, secret: str, event_type: str, payload: dict, delivery_id: str) -> bool:
    payload_bytes = json.dumps(payload).encode()
    sig = _sign(secret, payload_bytes)
    conn = _conn()
    for attempt in range(3):
        try:
            resp = requests.post(url, data=payload_bytes, headers={
                "Content-Type": "application/json",
                "X-Platform-Signature": sig,
                "X-Platform-Event": event_type,
            }, timeout=10)
            ok = 200 <= resp.status_code < 300
            conn.execute("UPDATE webhook_delivery_log SET status=?, attempts=attempts+1, last_attempt_at=datetime('now'), response_code=? WHERE id=?",
                         ["delivered" if ok else "failed", resp.status_code, delivery_id])
            conn.commit()
            if ok:
                conn.close()
                log.info("webhooks.delivered  sub=%s  event=%s", subscription_id, event_type)
                return True
        except Exception as exc:
            log.warning("webhooks.deliver_attempt_error  attempt=%d  url=%s  exc=%s", attempt, url, exc)
        if attempt < 2:
            time.sleep(2 ** attempt)
    conn.execute("UPDATE webhook_delivery_log SET status='dead_letter' WHERE id=?", [delivery_id])
    conn.commit()
    conn.close()
    log.error("webhooks.dead_letter  sub=%s  event=%s", subscription_id, event_type)
    return False

def fire_event(tenant_id: str, event_type: str, payload: dict):
    conn = _conn()
    subs = conn.execute("SELECT id, url, secret FROM webhook_subscriptions WHERE tenant_id=? AND event_type IN (?,?) AND active=1",
                        [tenant_id, event_type, "*"]).fetchall()
    payload_hash = hashlib.sha256(json.dumps(payload).encode()).hexdigest()[:16]
    for sub_id, url, secret in subs:
        delivery_id = hashlib.sha256(f"{sub_id}:{payload_hash}".encode()).hexdigest()[:16]
        conn.execute("INSERT OR IGNORE INTO webhook_delivery_log (id, subscription_id, event_type, payload_hash) VALUES (?,?,?,?)",
                     [delivery_id, sub_id, event_type, payload_hash])
        conn.commit()
        deliver(sub_id, url, secret, event_type, payload, delivery_id)
    conn.close()
    log.info("webhooks.fired  tenant=%s  event=%s  subs=%d", tenant_id, event_type, len(subs))

def list_webhooks(tenant_id: str) -> List[Dict]:
    conn = _conn()
    rows = conn.execute("SELECT id, event_type, url, active, created_at FROM webhook_subscriptions WHERE tenant_id=? ORDER BY created_at DESC", [tenant_id]).fetchall()
    conn.close()
    return [{"id": r[0], "event_type": r[1], "url": r[2], "active": bool(r[3]), "created_at": r[4]} for r in rows]

def replay(delivery_id: str) -> bool:
    conn = _conn()
    row = conn.execute("SELECT subscription_id, event_type FROM webhook_delivery_log WHERE id=?", [delivery_id]).fetchone()
    if not row:
        conn.close()
        return False
    sub = conn.execute("SELECT url, secret FROM webhook_subscriptions WHERE id=?", [row[0]]).fetchone()
    conn.close()
    if not sub:
        return False
    return deliver(row[0], sub[0], sub[1], row[1], {}, delivery_id)
