import json, logging, os, sqlite3
from typing import Dict
import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
DB_PATH = "data/storage/seo_engine.db"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS tenant_billing (
        tenant_id TEXT PRIMARY KEY, stripe_customer_id TEXT,
        stripe_subscription_id TEXT, plan TEXT DEFAULT 'free',
        billing_status TEXT DEFAULT 'active', current_period_end TEXT,
        updated_at TEXT DEFAULT (datetime('now'))
    )""")
    c.commit()
    return c

class StripeWebhookHandler:
    def __init__(self):
        self.secret_key = os.getenv("STRIPE_SECRET_KEY", "")
        self.webhook_secret = os.getenv("STRIPE_WEBHOOK_SECRET", "")

    def handle_webhook(self, payload_bytes: bytes, sig_header: str) -> Dict:
        if not self.webhook_secret:
            log.warning("billing: STRIPE_WEBHOOK_SECRET not set")
            return {"status": "skipped"}
        try:
            import stripe
            stripe.api_key = self.secret_key
            event = stripe.Webhook.construct_event(payload_bytes, sig_header, self.webhook_secret)
        except Exception as exc:
            log.exception("billing.webhook_verify_error")
            return {"status": "error", "error": str(exc)}

        etype = event["type"]
        handlers = {
            "customer.subscription.created": self.handle_subscription_created,
            "customer.subscription.updated": self.handle_subscription_created,
            "customer.subscription.deleted": self.handle_subscription_deleted,
            "invoice.payment_failed": self.handle_payment_failed,
            "invoice.paid": self.handle_invoice_paid,
        }
        handler = handlers.get(etype)
        if handler:
            handler(event)
            log.info("billing.webhook  type=%s", etype)
        return {"status": "ok", "type": etype}

    def handle_subscription_created(self, event: dict):
        sub = event["data"]["object"]
        tenant_id = sub.get("metadata", {}).get("tenant_id", "")
        if not tenant_id:
            return
        plan = sub.get("items", {}).get("data", [{}])[0].get("price", {}).get("metadata", {}).get("plan", "starter")
        period_end = sub.get("current_period_end", "")
        conn = _conn()
        conn.execute("""INSERT OR REPLACE INTO tenant_billing (tenant_id, stripe_customer_id, stripe_subscription_id, plan, billing_status, current_period_end)
            VALUES (?,?,?,?,?,datetime(?, 'unixepoch'))""",
            [tenant_id, sub.get("customer"), sub.get("id"), plan, "active", str(period_end)])
        conn.commit()
        conn.close()
        _redis.set(f"billing:plan:{tenant_id}", plan)

    def handle_subscription_deleted(self, event: dict):
        sub = event["data"]["object"]
        tenant_id = sub.get("metadata", {}).get("tenant_id", "")
        if not tenant_id:
            return
        conn = _conn()
        conn.execute("UPDATE tenant_billing SET plan='free', billing_status='cancelled' WHERE tenant_id=?", [tenant_id])
        conn.commit()
        conn.close()
        _redis.set(f"billing:plan:{tenant_id}", "free")
        log.info("billing.cancelled  tenant=%s", tenant_id)

    def handle_payment_failed(self, event: dict):
        invoice = event["data"]["object"]
        customer_id = invoice.get("customer")
        conn = _conn()
        conn.execute("UPDATE tenant_billing SET billing_status='past_due' WHERE stripe_customer_id=?", [customer_id])
        conn.commit()
        conn.close()
        log.warning("billing.payment_failed  customer=%s", customer_id)

    def handle_invoice_paid(self, event: dict):
        invoice = event["data"]["object"]
        customer_id = invoice.get("customer")
        conn = _conn()
        conn.execute("UPDATE tenant_billing SET billing_status='active' WHERE stripe_customer_id=?", [customer_id])
        conn.commit()
        conn.close()

    def record_usage(self, tenant_id: str, metric: str, quantity: int):
        if not self.secret_key:
            return
        try:
            import stripe
            stripe.api_key = self.secret_key
            conn = _conn()
            row = conn.execute("SELECT stripe_subscription_id FROM tenant_billing WHERE tenant_id=?", [tenant_id]).fetchone()
            conn.close()
            if not row:
                return
            items = stripe.SubscriptionItem.list(subscription=row[0])
            for item in items.data:
                if item.price.metadata.get("metric") == metric:
                    stripe.SubscriptionItem.create_usage_record(item.id, quantity=quantity, action="increment")
                    break
        except Exception as exc:
            log.exception("billing.record_usage_error  tenant=%s  metric=%s", tenant_id, metric)

def get_billing_status(tenant_id: str) -> Dict:
    conn = _conn()
    row = conn.execute("SELECT plan, billing_status, current_period_end FROM tenant_billing WHERE tenant_id=?", [tenant_id]).fetchone()
    conn.close()
    if not row:
        return {"plan": "free", "billing_status": "active", "current_period_end": None}
    return {"plan": row[0], "billing_status": row[1], "current_period_end": row[2]}
