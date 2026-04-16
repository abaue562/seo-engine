"""CRM integration — routes captured leads to HubSpot or Pipedrive.

Also provides a FastAPI router with a /contact POST endpoint that:
1. Validates the form submission
2. Stores the lead in SQLite
3. Pushes to CRM
4. Sends notification email to business owner
"""

from __future__ import annotations

import logging
import re
import smtplib
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class LeadData:
    name: str
    phone: str
    email: str
    message: str = ""
    keyword: str = ""
    source_url: str = ""
    business_id: str = ""


class CRMRouter:
    """Routes leads to configured CRM and sends owner notifications."""

    def __init__(
        self,
        hubspot_key: str = "",
        pipedrive_token: str = "",
        smtp_config: dict = None,
    ):
        self.hubspot_key = hubspot_key
        self.pipedrive_token = pipedrive_token
        self.smtp = smtp_config or {}

    async def route_lead(self, lead: LeadData, business: dict) -> dict:
        """Push lead to configured CRM. Returns result dict."""
        try:
            import httpx
            if self.hubspot_key:
                contact_id = await self._push_hubspot(lead, business)
                crm = "hubspot"
            elif self.pipedrive_token:
                contact_id = await self._push_pipedrive(lead, business)
                crm = "pipedrive"
            else:
                log.info("crm.route  no CRM configured — lead stored in DB only")
                return {"crm": "none", "contact_id": None, "success": True}

            # Send owner notification
            try:
                await self._send_owner_notification(lead, business)
            except Exception as e:
                log.warning("crm.notification_fail  err=%s", e)

            return {"crm": crm, "contact_id": contact_id, "success": True}
        except Exception as e:
            log.error("crm.route_fail  err=%s", e)
            return {"crm": "error", "contact_id": None, "success": False, "error": str(e)}

    async def _push_hubspot(self, lead: LeadData, business: dict) -> str:
        """Create/update HubSpot contact."""
        import httpx
        name_parts = lead.name.strip().split(" ", 1)
        firstname = name_parts[0]
        lastname = name_parts[1] if len(name_parts) > 1 else ""

        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://api.hubapi.com/crm/v3/objects/contacts",
                headers={
                    "Authorization": f"Bearer {self.hubspot_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "properties": {
                        "firstname": firstname,
                        "lastname": lastname,
                        "email": lead.email,
                        "phone": lead.phone,
                        "message": lead.message,
                        "hs_lead_source": f"SEO - {lead.keyword}",
                        "website": lead.source_url,
                        "company": business.get("name", ""),
                    }
                },
            )
            if resp.status_code in (200, 201):
                return resp.json().get("id", "")
            # Handle duplicate (409)
            if resp.status_code == 409:
                log.info("crm.hubspot  contact exists")
                return "existing"
            log.warning("crm.hubspot_fail  status=%d", resp.status_code)
            raise Exception(f"HubSpot API error {resp.status_code}: {resp.text[:200]}")

    async def _push_pipedrive(self, lead: LeadData, business: dict) -> str:
        """Create Pipedrive person + deal."""
        import httpx
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://api.pipedrive.com/v1/persons",
                params={"api_token": self.pipedrive_token},
                json={
                    "name": lead.name,
                    "email": [{"value": lead.email, "primary": True}],
                    "phone": [{"value": lead.phone, "primary": True}],
                },
            )
            if resp.status_code in (200, 201):
                person_id = str(resp.json().get("data", {}).get("id", ""))
                # Create deal
                await client.post(
                    "https://api.pipedrive.com/v1/deals",
                    params={"api_token": self.pipedrive_token},
                    json={
                        "title": f"{lead.name} — {lead.keyword or 'SEO Lead'}",
                        "person_id": person_id,
                        "note": f"Source: {lead.source_url}\nMessage: {lead.message}",
                    },
                )
                return person_id
            raise Exception(f"Pipedrive error {resp.status_code}")

    async def _send_owner_notification(self, lead: LeadData, business: dict):
        """Send email notification to business owner."""
        owner_email = business.get("owner_email") or business.get("email", "")
        if not owner_email or not self.smtp.get("host"):
            return

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"New Lead: {lead.name} — {lead.keyword or 'Website'}"
        msg["From"] = self.smtp.get("user", "leads@seoengine.local")
        msg["To"] = owner_email

        body = f"""New lead from your website:

Name: {lead.name}
Phone: {lead.phone}
Email: {lead.email}
Source keyword: {lead.keyword}
Source URL: {lead.source_url}

Message:
{lead.message}

---
SEO Engine Lead Capture
"""
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(self.smtp["host"], self.smtp.get("port", 587)) as server:
            server.starttls()
            if self.smtp.get("user") and self.smtp.get("pass"):
                server.login(self.smtp["user"], self.smtp["pass"])
            server.sendmail(msg["From"], [owner_email], msg.as_string())


# ---------------------------------------------------------------------------
# FastAPI router
# ---------------------------------------------------------------------------

try:
    from fastapi import APIRouter, HTTPException
    from pydantic import BaseModel

    lead_router = APIRouter(tags=["conversion"])

    class LeadForm(BaseModel):
        name: str
        phone: str
        email: str
        message: str = ""
        keyword: str = ""
        source_url: str = ""
        business_id: str = "default"

    @lead_router.post("/contact")
    async def submit_lead(form: LeadForm):
        """Handle lead form submissions."""
        # Validation
        if not form.name.strip():
            raise HTTPException(status_code=422, detail="Name is required")
        if not re.search(r"\d{10,}", re.sub(r"[^\d]", "", form.phone)):
            raise HTTPException(status_code=422, detail="Valid phone number required (10+ digits)")
        if "@" not in form.email or "." not in form.email.split("@")[-1]:
            raise HTTPException(status_code=422, detail="Valid email required")

        lead = LeadData(
            name=form.name.strip(),
            phone=form.phone.strip(),
            email=form.email.strip().lower(),
            message=form.message.strip(),
            keyword=form.keyword,
            source_url=form.source_url,
            business_id=form.business_id,
        )

        # Save to DB
        try:
            from data.db import get_db
            db = get_db()
            lead_id = db.save_lead(
                business_id=form.business_id,
                name=lead.name,
                phone=lead.phone,
                email=lead.email,
                message=lead.message,
                source_url=lead.source_url,
                keyword=lead.keyword,
            )
        except Exception as e:
            log.error("lead.db_save_fail  err=%s", e)
            lead_id = None

        # Route to CRM
        try:
            from config.settings import HUBSPOT_API_KEY, PIPEDRIVE_API_TOKEN, SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS
        except ImportError:
            HUBSPOT_API_KEY = ""
            PIPEDRIVE_API_TOKEN = ""
            SMTP_HOST = ""
            SMTP_PORT = 587
            SMTP_USER = ""
            SMTP_PASS = ""

        business = {}
        try:
            from data.db import get_db as _get_db
            b = _get_db().get_business(form.business_id)
            if b:
                import json
                business = json.loads(b.get("config_json", "{}"))
        except Exception:
            pass

        router = CRMRouter(
            hubspot_key=HUBSPOT_API_KEY,
            pipedrive_token=PIPEDRIVE_API_TOKEN,
            smtp_config={"host": SMTP_HOST, "port": SMTP_PORT, "user": SMTP_USER, "pass": SMTP_PASS},
        )
        crm_result = await router.route_lead(lead, business)

        # Update DB with CRM ID
        if lead_id and crm_result.get("contact_id"):
            try:
                from data.db import get_db as _get_db2
                _get_db2()._conn.execute(
                    "UPDATE leads SET crm_id=? WHERE id=?",
                    (crm_result["contact_id"], lead_id)
                )
                _get_db2()._conn.commit()
            except Exception:
                pass

        log.info("lead.captured  name=%s  keyword=%s  crm=%s", lead.name, lead.keyword, crm_result.get("crm"))
        return {
            "success": True,
            "message": "Thank you! We'll be in touch within 24 hours.",
            "lead_id": lead_id,
        }

    @lead_router.get("/leads/{business_id}")
    async def get_leads(business_id: str, limit: int = 50):
        """Internal endpoint — returns recent leads for a business."""
        from data.db import get_db
        leads = get_db().get_leads(business_id, limit=limit)
        return {"business_id": business_id, "leads": leads, "total": len(leads)}

except ImportError:
    lead_router = None
    log.warning("crm.fastapi_not_available  FastAPI not installed")


# ---------------------------------------------------------------------------
# GA4 helpers
# ---------------------------------------------------------------------------

def validate_ga4_event_code(html: str) -> bool:
    """Check if GA4 gtag script is properly loaded in HTML."""
    return bool(re.search(r"gtag\.js\?id=G-", html) or re.search(r"G-[A-Z0-9]{6,}", html))


def inject_ga4_script(html: str, measurement_id: str) -> str:
    """Inject full GA4 script tag into <head>."""
    if not measurement_id or not measurement_id.startswith("G-"):
        return html
    if "gtag.js" in html:
        return html  # Already present

    script = f"""<!-- Google Analytics 4 -->
<script async src="https://www.googletagmanager.com/gtag/js?id={measurement_id}"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){{dataLayer.push(arguments);}}
  gtag('js', new Date());
  gtag('config', '{measurement_id}', {{send_page_view: true}});
</script>"""

    if re.search(r"<head[^>]*>", html, re.IGNORECASE):
        return re.sub(
            r"(<head[^>]*>)",
            r"\1\n" + script,
            html,
            count=1,
            flags=re.IGNORECASE,
        )
    return script + "\n" + html
