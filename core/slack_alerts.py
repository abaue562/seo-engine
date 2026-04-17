import json, logging, os, sqlite3
from typing import Dict, List, Optional
import requests

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS tenant_slack (
        business_id TEXT PRIMARY KEY, webhook_url TEXT NOT NULL,
        events TEXT DEFAULT '["content.published","rank.changed","indexing.failed"]',
        active INTEGER DEFAULT 1
    )""")
    c.commit()
    return c

class SlackAlerter:
    def __init__(self):
        self.ops_webhook = os.getenv("SLACK_OPS_WEBHOOK", "")

    def send(self, webhook_url: str, message: str, severity: str = "info", fields: Optional[List[Dict]] = None) -> bool:
        if not webhook_url:
            return False
        color = {"info": "#36a64f", "warning": "#ffa500", "error": "#ff0000", "critical": "#8b0000"}.get(severity, "#36a64f")
        attachment = {"color": color, "text": message, "fields": fields or [], "footer": "SEO Engine"}
        try:
            resp = requests.post(webhook_url, json={"attachments": [attachment]}, timeout=10)
            return resp.status_code == 200
        except Exception as exc:
            log.exception("slack_alerts.send_error")
            return False

    def alert_tenant(self, business_id: str, event_type: str, message: str, fields: Optional[List[Dict]] = None) -> bool:
        conn = _conn()
        row = conn.execute("SELECT webhook_url, events, active FROM tenant_slack WHERE business_id=?", [business_id]).fetchone()
        conn.close()
        if not row or not row[2]:
            return False
        webhook, events_json, _ = row
        events = json.loads(events_json)
        if event_type not in events and "*" not in events:
            return False
        return self.send(webhook, message, fields=fields)

    def alert_ops(self, message: str, severity: str = "warning", fields: Optional[List[Dict]] = None) -> bool:
        return self.send(self.ops_webhook, message, severity=severity, fields=fields)

    def notify_content_published(self, business_id: str, url: str, keyword: str):
        self.alert_tenant(business_id, "content.published", f"New content published: *{keyword}*",
                          fields=[{"title": "URL", "value": url, "short": False}])

    def notify_rank_change(self, business_id: str, keyword: str, old_pos: int, new_pos: int):
        direction = "improved" if new_pos < old_pos else "dropped"
        icon = "🟢" if direction == "improved" else "🔴"
        self.alert_tenant(business_id, "rank.changed", f"{icon} Rank {direction}: *{keyword}*",
                          fields=[{"title": "Position", "value": f"{old_pos} → {new_pos}", "short": True}])

    def notify_billing_failed(self, business_id: str, reason: str):
        self.alert_ops(f"Billing failed for tenant `{business_id}`: {reason}", severity="error",
                       fields=[{"title": "Tenant", "value": business_id, "short": True}])

    def configure_tenant(self, business_id: str, webhook_url: str, events: List[str]):
        conn = _conn()
        conn.execute("INSERT OR REPLACE INTO tenant_slack (business_id, webhook_url, events) VALUES (?,?,?)",
                     [business_id, webhook_url, json.dumps(events)])
        conn.commit()
        conn.close()
