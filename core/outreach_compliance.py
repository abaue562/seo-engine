"""Outreach email compliance layer (CAN-SPAM / GDPR).

Every outbound email MUST pass through this layer before dispatch.
Default posture: if compliance check fails or service is unavailable,
BLOCK the send (fail-closed).

Usage:
    from core.outreach_compliance import compliance_check, record_unsubscribe

    try:
        approved = compliance_check(
            tenant_id="abc-123",
            recipient_email="prospect@example.com",
            campaign_id="camp-1",
            step_num=1,
            sender_domain="mybusiness.com",
            postal_address="123 Main St, Vancouver BC V1A 1A1, Canada",
        )
    except OutreachBlockedError as e:
        log.warning("outreach blocked: %s", e)
        return  # do not send

    # ... send email with approved.required_headers + approved.footer_html
"""
from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

# TLDs where GDPR applies (EU + UK + EEA)
_GDPR_TLDS = frozenset({
    ".at", ".be", ".bg", ".hr", ".cy", ".cz", ".dk", ".ee", ".fi", ".fr",
    ".de", ".gr", ".hu", ".ie", ".it", ".lv", ".lt", ".lu", ".mt", ".nl",
    ".pl", ".pt", ".ro", ".sk", ".si", ".es", ".se", ".uk", ".gb",
    # EEA
    ".is", ".li", ".no",
})

# Daily cold email cap per domain per tenant (CAN-SPAM safe harbor)
_DAILY_DOMAIN_CAP = 30


class OutreachBlockedError(Exception):
    """Raised when an email is blocked by compliance checks."""
    def __init__(self, message: str, reason: str = "blocked"):
        super().__init__(message)
        self.reason = reason


@dataclass
class ComplianceApproval:
    """Result of a successful compliance check."""
    recipient_email: str
    required_headers: dict = field(default_factory=dict)  # RFC 8058 List-Unsubscribe
    footer_html: str = ""                                  # CAN-SPAM footer
    is_gdpr_subject: bool = False
    jurisdiction: str = "unknown"
    warnings: list[str] = field(default_factory=list)


def _recipient_tld(email: str) -> str:
    """Return the TLD of the email domain."""
    try:
        domain = email.split("@", 1)[1].lower()
        parts = domain.rsplit(".", 1)
        return "." + parts[-1] if len(parts) > 1 else ""
    except Exception:
        return ""


def _is_gdpr_subject(email: str) -> bool:
    """Best-effort GDPR jurisdiction check based on email TLD."""
    return _recipient_tld(email) in _GDPR_TLDS


def _recipient_hash(email: str) -> str:
    return hashlib.sha256(email.lower().strip().encode()).hexdigest()


def _recipient_domain(email: str) -> str:
    try:
        return email.split("@", 1)[1].lower().strip()
    except Exception:
        return ""


def is_suppressed(recipient_email: str, tenant_id: str) -> tuple[bool, str]:
    """Check global + tenant suppression lists.

    Returns (is_suppressed, reason).
    """
    email_hash = _recipient_hash(recipient_email)
    domain = _recipient_domain(recipient_email)
    try:
        from core.pg import admin_one
        # Global suppression (applies to all tenants)
        row = admin_one(
            "SELECT reason FROM outreach_suppression "
            "WHERE (email_hash = %s OR domain = %s) AND (tenant_id IS NULL OR tenant_id = %s) "
            "LIMIT 1",
            [email_hash, domain, tenant_id],
        )
        if row:
            return True, row[0] or "suppressed"
        return False, ""
    except Exception as e:
        log.warning("outreach.suppression_check_fail  err=%s  (blocking_for_safety)", e)
        # Fail closed: if we cannot check the suppression list, block the send
        return True, "suppression_check_unavailable"


def record_unsubscribe(
    recipient_email: str,
    tenant_id: Optional[str] = None,
    reason: str = "unsubscribe",
) -> None:
    """Record an unsubscribe. Writes within 60s of webhook receipt."""
    email_hash = _recipient_hash(recipient_email)
    domain = _recipient_domain(recipient_email)
    try:
        from core.pg import admin_write
        admin_write(
            "INSERT INTO outreach_suppression (email_hash, domain, tenant_id, reason) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (email_hash, tenant_id) DO NOTHING",
            [email_hash, domain, tenant_id, reason],
        )
        log.info("outreach.unsubscribed  hash=%s  tenant=%s  reason=%s",
                 email_hash[:8], (tenant_id or "global")[:8], reason)
    except Exception as e:
        log.error("outreach.unsubscribe_record_fail  err=%s", e)


def record_bounce(recipient_email: str, tenant_id: Optional[str], hard: bool = True) -> None:
    """Record a bounce. 2+ bounces = permanent suppression."""
    record_unsubscribe(
        recipient_email,
        tenant_id=tenant_id,
        reason="hard_bounce" if hard else "soft_bounce",
    )


def _check_daily_domain_cap(tenant_id: str, domain: str) -> bool:
    """Return True if under the daily domain cap, False if capped."""
    try:
        import redis, os
        from datetime import datetime, timezone
        r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"),
                           decode_responses=True, socket_timeout=2)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"outreach:daily:{tenant_id[:8]}:{domain}:{date_str}"
        count = r.incr(key)
        if count == 1:
            r.expire(key, 86400)
        return count <= _DAILY_DOMAIN_CAP
    except Exception as e:
        log.warning("outreach.domain_cap_check_fail  err=%s  (allowing)", e)
        return True  # Fail open on counter only (not suppression)


def _build_can_spam_footer(
    sender_name: str,
    postal_address: str,
    unsubscribe_url: str,
) -> str:
    return (
        f'''<div style="font-size:11px;color:#666;margin-top:32px;border-top:1px solid #eee;padding-top:12px">'''
        f'''<p>{sender_name} &bull; {postal_address}</p>'''
        f'''<p>You received this because you are a potential business contact. '''
        f'''<a href="{unsubscribe_url}">Unsubscribe</a></p>'''
        f'''</div>'''
    )


def compliance_check(
    tenant_id: str,
    recipient_email: str,
    campaign_id: str,
    step_num: int,
    sender_name: str = "",
    postal_address: str = "",
    unsubscribe_base_url: str = "",
) -> ComplianceApproval:
    """Run all compliance checks before sending an outreach email.

    Raises OutreachBlockedError if the send should not proceed.
    Returns ComplianceApproval with required headers and footer to inject.

    Args:
        tenant_id:            Tenant UUID.
        recipient_email:      Target email address.
        campaign_id:          Campaign identifier (for idempotency).
        step_num:             Sequence step number (1-indexed).
        sender_name:          Business name for CAN-SPAM footer.
        postal_address:       Physical postal address (CAN-SPAM required).
        unsubscribe_base_url: Base URL for unsubscribe link generation.
    """
    if not recipient_email or "@" not in recipient_email:
        raise OutreachBlockedError("Invalid recipient email", reason="invalid_email")

    # 1. Suppression list (fail-closed if unreachable)
    suppressed, sup_reason = is_suppressed(recipient_email, tenant_id)
    if suppressed:
        raise OutreachBlockedError(
            f"Recipient {recipient_email[:30]}... is suppressed: {sup_reason}",
            reason=sup_reason,
        )

    # 2. Daily domain cap
    domain = _recipient_domain(recipient_email)
    if not _check_daily_domain_cap(tenant_id, domain):
        raise OutreachBlockedError(
            f"Daily domain cap ({_DAILY_DOMAIN_CAP}/day) reached for {domain}",
            reason="daily_cap_exceeded",
        )

    # 3. GDPR check: only allow B2B role-based emails
    gdpr = _is_gdpr_subject(recipient_email)
    warnings = []
    if gdpr:
        # For EU/UK: must be a B2B role email (info@, contact@, hello@, support@, etc.)
        local_part = recipient_email.split("@")[0].lower()
        b2b_prefixes = {"info", "contact", "hello", "support", "sales", "marketing",
                        "admin", "office", "enquiries", "enquiry", "business", "partner"}
        is_role_email = any(local_part.startswith(p) for p in b2b_prefixes) or                         re.match(r"^[a-z]{2,4}@", local_part)  # Very short = likely role
        if not is_role_email:
            # Personal-looking email + GDPR jurisdiction = block
            raise OutreachBlockedError(
                f"GDPR: personal email addresses in EU/UK/EEA require consent basis; "
                f"only B2B role emails permitted",
                reason="gdpr_personal_email",
            )
        warnings.append("gdpr_jurisdiction_eu")

    # 4. Build CAN-SPAM headers and footer
    from core.idempotency import outreach_key
    email_hash = _recipient_hash(recipient_email)
    unsub_url = (
        f"{unsubscribe_base_url.rstrip('/')}/unsubscribe?eh={email_hash[:16]}&tid={tenant_id[:8]}&cid={campaign_id}"
        if unsubscribe_base_url
        else "#unsubscribe"
    )

    required_headers = {
        "List-Unsubscribe": f"<{unsub_url}>",
        "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",  # RFC 8058
    }

    footer_html = ""
    if postal_address:
        footer_html = _build_can_spam_footer(
            sender_name=sender_name or "BlendBright SEO",
            postal_address=postal_address,
            unsubscribe_url=unsub_url,
        )

    # 5. Audit log
    try:
        from core.audit import log_event
        log_event(
            tenant_id=tenant_id,
            actor="system",
            action="outreach.send_approved",
            entity_type="outreach_campaign",
            diff={
                "campaign_id": campaign_id,
                "step": step_num,
                "domain": domain,
                "gdpr": gdpr,
                "warnings": warnings,
            },
        )
    except Exception:
        pass

    log.info(
        "outreach.compliance_ok  tenant=%s  domain=%s  gdpr=%s  campaign=%s  step=%d",
        tenant_id[:8], domain, gdpr, campaign_id, step_num,
    )

    return ComplianceApproval(
        recipient_email=recipient_email,
        required_headers=required_headers,
        footer_html=footer_html,
        is_gdpr_subject=gdpr,
        jurisdiction="eu" if gdpr else "ca_us",
        warnings=warnings,
    )
