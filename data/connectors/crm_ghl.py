import json, logging, os, hashlib, sqlite3
from typing import Dict, Optional
from datetime import datetime
import redis, requests

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
GHL_BASE = "https://rest.gohighlevel.com/v1"
DB_PATH = "data/storage/seo_engine.db"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS crm_push_log (
        id TEXT PRIMARY KEY, business_id TEXT, crm_type TEXT DEFAULT 'ghl',
        contact_email TEXT, status TEXT, response TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")
    c.commit()
    return c

class GHLConnector:
    def __init__(self, api_key: str = "", location_id: str = ""):
        self.api_key = api_key or os.getenv("GHL_API_KEY", "")
        self.location_id = location_id or os.getenv("GHL_LOCATION_ID", "")
        self._headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json", "Version": "2021-07-28"}

    def _post(self, endpoint: str, body: dict) -> dict:
        if not self.api_key:
            log.warning("ghl: GHL_API_KEY not set")
            return {}
        try:
            resp = requests.post(f"{GHL_BASE}{endpoint}", headers=self._headers, json=body, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.exception("ghl.post_error  endpoint=%s", endpoint)
            return {}

    def push_lead(self, lead: dict, business_id: str = "") -> bool:
        body = {
            "locationId": self.location_id,
            "firstName": lead.get("first_name", ""),
            "lastName": lead.get("last_name", ""),
            "email": lead.get("email", ""),
            "phone": lead.get("phone", ""),
            "source": lead.get("source", "SEO Engine"),
            "tags": lead.get("tags", ["seo-engine-lead"]),
        }
        result = self._post("/contacts/", body)
        ok = bool(result.get("contact") or result.get("id"))
        uid = hashlib.sha256(f"{business_id}:{lead.get('email')}:{datetime.utcnow().isoformat()}".encode()).hexdigest()[:16]
        conn = _conn()
        conn.execute("INSERT OR IGNORE INTO crm_push_log (id, business_id, contact_email, status, response) VALUES (?,?,?,?,?)",
                     [uid, business_id, lead.get("email", ""), "success" if ok else "failed", json.dumps(result)[:500]])
        conn.commit()
        conn.close()
        if not ok:
            _redis.lpush(f"ghl:dead_letter:{business_id}", json.dumps(lead))
            _redis.ltrim(f"ghl:dead_letter:{business_id}", 0, 99)
            log.warning("ghl.push_lead_failed  biz=%s  email=%s", business_id, lead.get("email"))
        else:
            log.info("ghl.push_lead_ok  biz=%s  email=%s", business_id, lead.get("email"))
        return ok

    def get_contact(self, email: str) -> Dict:
        if not self.api_key:
            return {}
        try:
            resp = requests.get(f"{GHL_BASE}/contacts/search?locationId={self.location_id}&email={email}",
                                headers=self._headers, timeout=15)
            data = resp.json()
            contacts = data.get("contacts", [])
            return contacts[0] if contacts else {}
        except Exception as exc:
            log.exception("ghl.get_contact_error  email=%s", email)
            return {}

    def update_lead_status(self, contact_id: str, status: str) -> bool:
        result = self._post(f"/contacts/{contact_id}/tags/", {"tags": [status]})
        return bool(result)

    def retry_dead_letter(self, business_id: str, max_retries: int = 5) -> int:
        retried = 0
        for _ in range(max_retries):
            item = _redis.rpop(f"ghl:dead_letter:{business_id}")
            if not item:
                break
            lead = json.loads(item)
            if self.push_lead(lead, business_id):
                retried += 1
        return retried
