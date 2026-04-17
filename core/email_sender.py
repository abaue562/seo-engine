import json, logging, os
from datetime import date
from typing import Optional
import redis, requests

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
RESEND_API = "https://api.resend.com/emails"

class EmailSender:
    def __init__(self):
        self.api_key = os.getenv("RESEND_API_KEY", "")
        self.default_from = os.getenv("EMAIL_FROM", "noreply@gethubed.com")

    def send_transactional(self, to: str, subject: str, html: str, from_addr: Optional[str] = None) -> bool:
        if not self.api_key:
            log.warning("email_sender: RESEND_API_KEY not set")
            return False
        sender = from_addr or self.default_from
        domain = sender.split("@")[-1]
        if not self._check_allowed(domain):
            log.warning("email_sender: domain %s paused or over limit", domain)
            return False
        try:
            resp = requests.post(RESEND_API, headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json={"from": sender, "to": [to], "subject": subject, "html": html}, timeout=15)
            resp.raise_for_status()
            self._track_send(domain)
            log.info("email_sender.sent  to=%s  subject=%s", to, subject[:40])
            return True
        except Exception as exc:
            log.exception("email_sender.error  to=%s", to)
            return False

    def send_report(self, business_id: str, to: str, subject: str, html: str) -> bool:
        ok = self.send_transactional(to, subject, html)
        try:
            from core.audit import log_event
            log_event(business_id, "email_report_sent", {"to": to, "subject": subject, "ok": ok})
        except Exception:
            pass
        return ok

    def _check_allowed(self, domain: str) -> bool:
        paused = _redis.get(f"email:paused:{domain}")
        if paused:
            return False
        status = self.check_domain_warmup(domain)
        return status["emails_sent_today"] < status["daily_limit"]

    def _track_send(self, domain: str):
        today = date.today().isoformat()
        _redis.incr(f"email:sent:{domain}:{today}")
        _redis.expire(f"email:sent:{domain}:{today}", 86400 * 2)

    def check_domain_warmup(self, domain: str) -> dict:
        today = date.today().isoformat()
        start_key = f"email:warmup_start:{domain}"
        start = _redis.get(start_key)
        if not start:
            _redis.set(start_key, today)
            start = today
        from datetime import datetime
        warmup_day = max(0, (datetime.fromisoformat(today) - datetime.fromisoformat(start)).days)
        daily_limit = min(int(10 * (1.2 ** warmup_day)), 500)
        sent = int(_redis.get(f"email:sent:{domain}:{today}") or 0)
        return {"domain": domain, "daily_limit": daily_limit, "emails_sent_today": sent, "warmup_day": warmup_day, "status": "active"}

    def record_bounce(self, domain: str, email: str):
        _redis.incr(f"email:bounces:{domain}")
        total = int(_redis.get(f"email:bounces:{domain}") or 0)
        sent = int(_redis.get(f"email:sent:{domain}:{date.today().isoformat()}") or 1)
        if total / max(sent, 1) > 0.05:
            _redis.set(f"email:paused:{domain}", "bounce_rate")
            log.warning("email_sender: paused domain %s — bounce rate exceeded", domain)

    def record_complaint(self, domain: str, email: str):
        _redis.incr(f"email:complaints:{domain}")
        total = int(_redis.get(f"email:complaints:{domain}") or 0)
        sent = int(_redis.get(f"email:sent:{domain}:{date.today().isoformat()}") or 1)
        if total / max(sent, 1) > 0.001:
            _redis.set(f"email:paused:{domain}", "complaint_rate")
            log.warning("email_sender: paused domain %s — complaint rate exceeded", domain)
