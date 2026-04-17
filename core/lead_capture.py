"""
Lead capture layer: form injection, lead storage, CRM push, email notification.
Injects smart lead forms into content pages with service-specific qualification questions.
"""
from __future__ import annotations
import hashlib
import json
import logging
import re
import sqlite3
import smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional

log = logging.getLogger(__name__)
_DB = "data/storage/seo_engine.db"


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS leads (
            id              TEXT PRIMARY KEY,
            business_id     TEXT NOT NULL,
            name            TEXT DEFAULT '',
            email           TEXT DEFAULT '',
            phone           TEXT DEFAULT '',
            service         TEXT DEFAULT '',
            message         TEXT DEFAULT '',
            source          TEXT DEFAULT 'organic',
            medium          TEXT DEFAULT '',
            campaign        TEXT DEFAULT '',
            page_url        TEXT DEFAULT '',
            qualified_score INTEGER DEFAULT 0,
            status          TEXT DEFAULT 'new',
            crm_pushed      INTEGER DEFAULT 0,
            metadata        TEXT DEFAULT '{}',
            created_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_leads_biz ON leads(business_id, status, created_at);

        CREATE TABLE IF NOT EXISTS lead_forms (
            id          TEXT PRIMARY KEY,
            business_id TEXT NOT NULL,
            form_type   TEXT DEFAULT 'contact',
            title       TEXT NOT NULL,
            fields_json TEXT NOT NULL,
            service     TEXT DEFAULT '',
            active      INTEGER DEFAULT 1,
            created_at  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_lf_biz ON lead_forms(business_id);
    """)
    c.commit()
    return c


def _qualify_lead(data: dict) -> int:
    """Score lead quality 0-100 based on completeness and signals."""
    score = 0
    if data.get("name"): score += 15
    if data.get("email"): score += 20
    if data.get("phone"): score += 25
    if data.get("service"): score += 15
    msg = (data.get("message") or "").lower()
    if len(msg) > 30: score += 10
    if any(w in msg for w in ["asap", "urgent", "today", "this week", "emergency"]): score += 10
    if any(w in msg for w in ["budget", "quote", "price", "cost", "estimate"]): score += 5
    return min(score, 100)


def save_lead(
    business_id: str,
    name: str = "",
    email: str = "",
    phone: str = "",
    service: str = "",
    message: str = "",
    source: str = "organic",
    medium: str = "",
    campaign: str = "",
    page_url: str = "",
    metadata: dict | None = None,
) -> dict:
    lid = hashlib.md5(
        f"{business_id}:{email or phone}:{datetime.now().isoformat()}".encode()
    ).hexdigest()[:16]
    now = datetime.now(timezone.utc).isoformat()
    data = {"name": name, "email": email, "phone": phone,
            "service": service, "message": message}
    score = _qualify_lead(data)

    with _conn() as c:
        c.execute("""
            INSERT INTO leads
                (id, business_id, name, email, phone, service, message,
                 source, medium, campaign, page_url, qualified_score, metadata, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, [lid, business_id, name, email, phone, service, message,
              source, medium, campaign, page_url, score,
              json.dumps(metadata or {}), now])

    log.info("lead.saved  biz=%s  score=%d  src=%s", business_id, score, source)
    return {"id": lid, "qualified_score": score, "status": "new"}


def get_leads(business_id: str, status: str = "", days: int = 30, limit: int = 50) -> list[dict]:
    with _conn() as c:
        if status:
            rows = c.execute("""
                SELECT * FROM leads WHERE business_id=? AND status=?
                  AND created_at >= datetime('now', ? || ' days')
                ORDER BY created_at DESC LIMIT ?
            """, [business_id, status, f"-{days}", limit]).fetchall()
        else:
            rows = c.execute("""
                SELECT * FROM leads WHERE business_id=?
                  AND created_at >= datetime('now', ? || ' days')
                ORDER BY created_at DESC LIMIT ?
            """, [business_id, f"-{days}", limit]).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["metadata"] = json.loads(d.get("metadata") or "{}")
        result.append(d)
    return result


def update_lead_status(lead_id: str, status: str) -> None:
    with _conn() as c:
        c.execute("UPDATE leads SET status=? WHERE id=?", [status, lead_id])


def get_lead_stats(business_id: str, days: int = 30) -> dict:
    with _conn() as c:
        rows = c.execute("""
            SELECT source, status, COUNT(*) as cnt,
                   AVG(qualified_score) as avg_score
            FROM leads WHERE business_id=?
              AND created_at >= datetime('now', ? || ' days')
            GROUP BY source, status
        """, [business_id, f"-{days}"]).fetchall()

        total = c.execute(
            "SELECT COUNT(*) as n FROM leads WHERE business_id=? AND created_at >= datetime('now', ? || ' days')",
            [business_id, f"-{days}"]
        ).fetchone()["n"]

        hot_leads = c.execute(
            "SELECT COUNT(*) as n FROM leads WHERE business_id=? AND qualified_score>=70 AND created_at >= datetime('now', ? || ' days')",
            [business_id, f"-{days}"]
        ).fetchone()["n"]

    by_source: dict[str, dict] = {}
    for r in rows:
        src = r["source"]
        if src not in by_source:
            by_source[src] = {"total": 0, "avg_score": 0}
        by_source[src]["total"] += r["cnt"]
        by_source[src]["avg_score"] = round(r["avg_score"] or 0, 1)

    return {
        "business_id": business_id,
        "days": days,
        "total_leads": total,
        "hot_leads": hot_leads,
        "hot_rate": round(hot_leads / max(total, 1) * 100, 1),
        "by_source": by_source,
    }


def notify_lead(business_id: str, lead: dict, notify_email: str = "") -> bool:
    """Send email notification for a new lead. Uses SMTP from env."""
    import os
    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    from_email = os.environ.get("SMTP_FROM", smtp_user)

    if not (smtp_host and smtp_user and smtp_pass and notify_email):
        log.warning("notify_lead: SMTP not configured  biz=%s", business_id)
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"New Lead: {lead.get('name', 'Unknown')} — {lead.get('service', 'General Inquiry')}"
        msg["From"] = from_email
        msg["To"] = notify_email

        body = f"""
<h2>New Lead Received</h2>
<table>
<tr><td><b>Name:</b></td><td>{lead.get('name','')}</td></tr>
<tr><td><b>Email:</b></td><td>{lead.get('email','')}</td></tr>
<tr><td><b>Phone:</b></td><td>{lead.get('phone','')}</td></tr>
<tr><td><b>Service:</b></td><td>{lead.get('service','')}</td></tr>
<tr><td><b>Message:</b></td><td>{lead.get('message','')}</td></tr>
<tr><td><b>Source:</b></td><td>{lead.get('source','')}</td></tr>
<tr><td><b>Quality Score:</b></td><td>{lead.get('qualified_score',0)}/100</td></tr>
<tr><td><b>Page:</b></td><td>{lead.get('page_url','')}</td></tr>
</table>
"""
        msg.attach(MIMEText(body, "html"))

        port = int(os.environ.get("SMTP_PORT", "587"))
        with smtplib.SMTP(smtp_host, port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_email, notify_email, msg.as_string())

        log.info("notify_lead.sent  biz=%s  to=%s", business_id, notify_email)
        return True
    except Exception:
        log.exception("notify_lead.error  biz=%s", business_id)
        return False


def push_to_crm(business_id: str, lead_id: str, lead: dict) -> bool:
    """Push lead to AION CRM bridge if configured."""
    try:
        from core import aion_bridge as aion
        payload = {
            "business_id": business_id,
            "lead_id": lead_id,
            "name": lead.get("name", ""),
            "email": lead.get("email", ""),
            "phone": lead.get("phone", ""),
            "service": lead.get("service", ""),
            "source": lead.get("source", ""),
            "score": lead.get("qualified_score", 0),
            "message": lead.get("message", ""),
        }
        import requests
        r = requests.post("http://localhost:9082/crm/lead", json=payload, timeout=5)
        if r.status_code == 200:
            with _conn() as c:
                c.execute("UPDATE leads SET crm_pushed=1 WHERE id=?", [lead_id])
            log.info("push_to_crm.ok  biz=%s  lead=%s", business_id, lead_id)
            return True
    except Exception:
        log.warning("push_to_crm.failed  biz=%s  lead=%s", business_id, lead_id)
    return False


def build_lead_form(
    business_id: str,
    service: str,
    location: str,
    form_type: str = "quote",
    phone: str = "",
) -> str:
    """Generate service-specific HTML lead form with schema markup."""
    fid = hashlib.md5(f"{business_id}:{service}:{form_type}".encode()).hexdigest()[:10]

    # Service-specific qualifier questions
    qualifiers: dict[str, list[dict]] = {
        "electrical": [
            {"name": "project_type", "label": "Project Type", "type": "select",
             "options": ["Panel Upgrade", "New Wiring", "EV Charger Install", "LED Lighting", "Other"]},
            {"name": "home_age", "label": "Home Age (approx.)", "type": "select",
             "options": ["<10 years", "10-25 years", "25-50 years", "50+ years"]},
        ],
        "plumbing": [
            {"name": "project_type", "label": "Issue Type", "type": "select",
             "options": ["Emergency Leak", "Drain Clog", "Hot Water Tank", "New Fixture", "Other"]},
            {"name": "urgency", "label": "How Soon?", "type": "select",
             "options": ["Emergency (today)", "This week", "Within a month", "Planning ahead"]},
        ],
        "hvac": [
            {"name": "project_type", "label": "Service Needed", "type": "select",
             "options": ["AC Repair", "Furnace Repair", "New Installation", "Annual Maintenance", "Other"]},
            {"name": "urgency", "label": "How Soon?", "type": "select",
             "options": ["Emergency (today)", "This week", "Within a month", "Planning ahead"]},
        ],
    }

    # Match service to qualifier group
    qual_fields = []
    for key, fields in qualifiers.items():
        if key in service.lower():
            qual_fields = fields
            break

    # Build qualifier HTML
    qual_html = ""
    for field in qual_fields:
        options_html = "".join(f'<option value="{o}">{o}</option>' for o in field["options"])
        qual_html += f"""
    <div class="lf-field">
      <label for="{field['name']}">{field['label']}</label>
      <select id="{field['name']}" name="{field['name']}">
        <option value="">Select...</option>
        {options_html}
      </select>
    </div>"""

    titles = {
        "quote": f"Get Your Free {service.title()} Quote",
        "contact": "Request a Callback",
        "emergency": "Emergency Service Request",
    }
    title = titles.get(form_type, f"Contact Us About {service.title()}")

    form_html = f"""
<section class="lead-form-wrap" id="contact">
  <div class="lead-form-inner">
    <h2 class="lf-title">{title}</h2>
    <p class="lf-sub">Serving {location} — Licensed &amp; Insured</p>
    <form class="lead-form" id="lf-{fid}" data-business="{business_id}" data-service="{service}">
      <div class="lf-row">
        <div class="lf-field">
          <label for="lf-name">Full Name *</label>
          <input type="text" id="lf-name" name="name" required placeholder="Jane Smith">
        </div>
        <div class="lf-field">
          <label for="lf-phone">Phone *</label>
          <input type="tel" id="lf-phone" name="phone" required placeholder="(250) 555-0100">
        </div>
      </div>
      <div class="lf-field">
        <label for="lf-email">Email</label>
        <input type="email" id="lf-email" name="email" placeholder="jane@example.com">
      </div>
      {qual_html}
      <div class="lf-field">
        <label for="lf-message">Tell us about your project</label>
        <textarea id="lf-message" name="message" rows="4" placeholder="Describe your project or question..."></textarea>
      </div>
      <button type="submit" class="lf-submit">Get My Free Quote</button>
      <p class="lf-disclaimer">No obligation. Typically respond within 2 hours during business hours.</p>
    </form>
    <div class="lf-success" style="display:none">
      <h3>Thanks! We'll be in touch shortly.</h3>
      <p>Most inquiries receive a response within 2 hours.</p>
    </div>
  </div>
</section>

<style>
.lead-form-wrap{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:2.5rem 2rem;margin:2rem 0}}
.lead-form-inner{{max-width:580px;margin:auto}}
.lf-title{{font-size:1.6rem;font-weight:700;margin-bottom:.25rem;color:#1e293b}}
.lf-sub{{color:#64748b;margin-bottom:1.5rem}}
.lf-row{{display:grid;grid-template-columns:1fr 1fr;gap:1rem}}
.lf-field{{margin-bottom:1rem}}
.lf-field label{{display:block;font-weight:600;font-size:.9rem;color:#374151;margin-bottom:.35rem}}
.lf-field input,.lf-field select,.lf-field textarea{{width:100%;padding:.65rem .9rem;border:1px solid #d1d5db;border-radius:6px;font-size:1rem;transition:border .2s;box-sizing:border-box}}
.lf-field input:focus,.lf-field select:focus,.lf-field textarea:focus{{border-color:#2563eb;outline:none}}
.lf-submit{{width:100%;background:#2563eb;color:#fff;font-weight:700;font-size:1.1rem;padding:.9rem;border:none;border-radius:8px;cursor:pointer;margin-top:.5rem;transition:background .2s}}
.lf-submit:hover{{background:#1d4ed8}}
.lf-disclaimer{{font-size:.8rem;color:#9ca3af;text-align:center;margin-top:.75rem}}
@media(max-width:540px){{.lf-row{{grid-template-columns:1fr}}}}
</style>

<script>
document.getElementById('lf-{fid}').addEventListener('submit', function(e){{
  e.preventDefault();
  var form = e.target;
  var data = Object.fromEntries(new FormData(form).entries());
  data.business_id = '{business_id}';
  data.service = '{service}';
  data.source = new URLSearchParams(window.location.search).get('utm_source') || 'organic';
  data.medium = new URLSearchParams(window.location.search).get('utm_medium') || '';
  data.campaign = new URLSearchParams(window.location.search).get('utm_campaign') || '';
  data.page_url = window.location.href;
  fetch('/conversion/lead', {{
    method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify(data)
  }}).then(function(r){{ return r.json(); }}).then(function(){{
    form.style.display='none';
    document.querySelector('.lf-success').style.display='block';
    // Fire conversion event
    fetch('/conversion/event', {{
      method:'POST',
      headers:{{'Content-Type':'application/json'}},
      body: JSON.stringify({{business_id:'{business_id}', event_type:'form_submit', source:data.source, page_url:data.page_url}})
    }}).catch(function(){{}});
  }}).catch(function(){{
    alert('Something went wrong. Please call us directly.');
  }});
}});
</script>"""

    # Save form definition
    now = datetime.now(timezone.utc).isoformat()
    form_def_id = hashlib.md5(f"{business_id}:{service}:{form_type}".encode()).hexdigest()[:12]
    with _conn() as c:
        c.execute("""
            INSERT INTO lead_forms (id, business_id, form_type, title, fields_json, service, created_at)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET title=excluded.title, fields_json=excluded.fields_json
        """, [form_def_id, business_id, form_type, title,
              json.dumps(qual_fields), service, now])

    return form_html


def inject_lead_form(html: str, business_id: str, service: str, location: str,
                     phone: str = "", form_type: str = "quote") -> str:
    """Inject lead form into page HTML before </body> or after CTA block."""
    form_html = build_lead_form(business_id, service, location, form_type, phone)

    # Insert before </body> or </main>
    for target in ["</main>", "</article>", "</body>"]:
        if target in html:
            return html.replace(target, form_html + "\n" + target, 1)
    return html + form_html
