"""
Backlink outreach: personalized email sequences for link acquisition.
Tracks send status, follow-up cadence, reply detection, and conversion.
Uses the existing EmailSender for delivery.
"""
from __future__ import annotations
import json
import sqlite3
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger(__name__)

_DB = "data/storage/seo_engine.db"

# Outreach sequence: day offsets from first contact
SEQUENCE = [
    {"day": 0,  "template": "initial",   "subject_suffix": ""},
    {"day": 5,  "template": "followup1", "subject_suffix": " (quick follow-up)"},
    {"day": 12, "template": "followup2", "subject_suffix": " (last note)"},
]

MAX_EMAILS_PER_DAY = 20  # conservative warmup cap


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS outreach_log (
            id              TEXT PRIMARY KEY,
            business_id     TEXT NOT NULL,
            prospect_id     TEXT NOT NULL,
            template        TEXT NOT NULL,
            to_email        TEXT NOT NULL,
            subject         TEXT,
            body_preview    TEXT,
            sent_at         TEXT,
            status          TEXT DEFAULT 'queued',
            reply_detected  INTEGER DEFAULT 0,
            sequence_step   INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_ol_biz ON outreach_log(business_id);
        CREATE INDEX IF NOT EXISTS idx_ol_prospect ON outreach_log(prospect_id);

        CREATE TABLE IF NOT EXISTS outreach_daily_count (
            date        TEXT NOT NULL,
            business_id TEXT NOT NULL,
            count       INTEGER DEFAULT 0,
            PRIMARY KEY(date, business_id)
        );
    """)
    c.commit()
    return c


def _outreach_id(prospect_id: str, template: str) -> str:
    return hashlib.md5(f"{prospect_id}:{template}".encode()).hexdigest()[:12]


def _daily_count(business_id: str) -> int:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with _conn() as c:
        row = c.execute(
            "SELECT count FROM outreach_daily_count WHERE date=? AND business_id=?",
            [today, business_id]
        ).fetchone()
    return row["count"] if row else 0


def _increment_daily(business_id: str) -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with _conn() as c:
        c.execute("""
            INSERT INTO outreach_daily_count (date, business_id, count) VALUES (?,?,1)
            ON CONFLICT(date, business_id) DO UPDATE SET count=count+1
        """, [today, business_id])


def _build_email(template: str, prospect: dict, sender_name: str, your_domain: str, your_page: str) -> dict:
    """Build subject + body for a given template and prospect."""
    contact_name = prospect.get("contact_name", "there")
    first_name = contact_name.split()[0] if contact_name and contact_name != "there" else "there"
    target_domain = prospect.get("target_domain", prospect.get("target_url", "your site"))
    opportunity_type = prospect.get("opportunity_type", "resource")
    pitch = prospect.get("pitch_angle", "")
    page_title = prospect.get("page_title", "your page")
    your_page_url = your_page or f"https://{your_domain}/"

    if template == "initial":
        subject = f"Quick question about {target_domain}"
        if opportunity_type == "unlinked_mention":
            subject = f"You mentioned us on {target_domain} — thank you!"
        elif opportunity_type == "broken_link":
            subject = f"Found a broken link on {page_title}"
        elif opportunity_type == "local_citation":
            subject = f"Listing request for {your_domain}"

        body = _initial_body(first_name, target_domain, opportunity_type, your_page_url, your_domain, pitch, page_title)

    elif template == "followup1":
        subject = f"Re: Quick question about {target_domain}"
        body = (
            f"Hi {first_name},\n\n"
            f"I wanted to follow up on my note from last week. I know inboxes get busy — "
            f"I just wanted to make sure my message didn't slip through the cracks.\n\n"
            f"I think {your_page_url} could be a genuinely useful resource for your readers. "
            f"Happy to answer any questions or swap something of value if helpful.\n\n"
            f"Best,\n{sender_name}"
        )

    else:  # followup2
        subject = f"Last note — {target_domain}"
        body = (
            f"Hi {first_name},\n\n"
            f"I'll keep this brief — this is my last follow-up. "
            f"If there's ever a good time to discuss a link from {target_domain}, "
            f"my door's always open.\n\n"
            f"Thanks for your time,\n{sender_name}"
        )

    return {"subject": subject, "body": body}


def _initial_body(first_name, target_domain, opportunity_type, your_page_url, your_domain, pitch, page_title):
    if opportunity_type == "unlinked_mention":
        return (
            f"Hi {first_name},\n\n"
            f"I noticed you mentioned {your_domain} on {target_domain} — thank you so much for that!\n\n"
            f"I wanted to reach out to see if you'd be open to turning that mention into a link. "
            f"It would help your readers find us directly: {your_page_url}\n\n"
            f"No worries if not — we appreciate the mention either way!\n\nBest,"
        )
    elif opportunity_type == "broken_link":
        return (
            f"Hi {first_name},\n\n"
            f"I was reading {page_title} on {target_domain} and noticed a broken link. "
            f"I have a resource that covers the same topic and could be a good replacement: "
            f"{your_page_url}\n\n"
            f"Thought you'd want to know! Happy to share the exact URL that's broken if helpful.\n\nBest,"
        )
    elif opportunity_type == "local_citation":
        return (
            f"Hi {first_name},\n\n"
            f"I'd like to add {your_domain} to your directory. We're a local business serving the area "
            f"and believe we'd be a good fit for your listings.\n\n"
            f"Our website: {your_page_url}\n\nThank you,"
        )
    else:
        return (
            f"Hi {first_name},\n\n"
            f"I came across {page_title} on {target_domain} and thought it was great. "
            f"I've put together a resource that I think your readers would find valuable: "
            f"{your_page_url}\n\n"
            f"{pitch or 'Would you consider linking to it from your page?'}\n\n"
            f"Happy to return the favor — I'm open to any kind of collaboration.\n\nBest,"
        )


def send_outreach(
    business_id: str,
    prospect_id: str,
    template: str,
    sender_name: str,
    sender_email: str,
    your_domain: str,
    your_page: str = "",
    sequence_step: int = 0,
) -> dict:
    """Send one outreach email and log it. Respects daily cap."""
    if _daily_count(business_id) >= MAX_EMAILS_PER_DAY:
        log.info("send_outreach: daily cap reached  biz=%s", business_id)
        return {"status": "capped", "reason": "daily_limit_reached"}

    from core.backlink_prospector import get_prospect, update_prospect_status
    prospect = get_prospect(prospect_id)
    if not prospect:
        return {"status": "error", "reason": "prospect_not_found"}

    to_email = prospect.get("contact_email", "")
    if not to_email:
        return {"status": "skipped", "reason": "no_email"}

    email = _build_email(template, prospect, sender_name, your_domain, your_page)

    oid = _outreach_id(prospect_id, template)
    now = datetime.now(timezone.utc).isoformat()

    try:
        from core.email_sender import EmailSender
        sender = EmailSender(business_id)
        sender.send(
            to=to_email,
            subject=email["subject"],
            html_body=f"<p>{email['body'].replace(chr(10), '<br>')}</p>",
            from_name=sender_name,
        )
        status = "sent"
    except Exception:
        log.exception("send_outreach: email send failed  prospect=%s", prospect_id)
        status = "failed"

    with _conn() as c:
        c.execute("""
            INSERT INTO outreach_log
                (id,business_id,prospect_id,template,to_email,subject,body_preview,sent_at,status,sequence_step)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO NOTHING
        """, [oid, business_id, prospect_id, template, to_email,
              email["subject"], email["body"][:200], now, status, sequence_step])

    if status == "sent":
        _increment_daily(business_id)
        update_prospect_status(prospect_id, "contacted")
        log.info("send_outreach.sent  prospect=%s  template=%s  to=%s", prospect_id, template, to_email)

    return {"status": status, "to": to_email, "subject": email["subject"]}


def run_outreach_queue(business_id: str, sender_name: str, sender_email: str, your_domain: str) -> dict:
    """
    Process the outreach queue:
    1. Send initial emails to 'new' top-priority prospects (up to daily cap)
    2. Send follow-ups to 'contacted' prospects on schedule
    """
    from core.backlink_prospector import get_prospects

    sent = 0
    skipped = 0
    results = []

    # Initial outreach for new prospects
    new_prospects = get_prospects(business_id, status="new", limit=MAX_EMAILS_PER_DAY)
    for p in new_prospects:
        if _daily_count(business_id) >= MAX_EMAILS_PER_DAY:
            break
        if not p.get("contact_email"):
            skipped += 1
            continue
        r = send_outreach(
            business_id=business_id,
            prospect_id=p["id"],
            template="initial",
            sender_name=sender_name,
            sender_email=sender_email,
            your_domain=your_domain,
            your_page=p.get("your_page_to_link", ""),
            sequence_step=0,
        )
        results.append(r)
        if r.get("status") == "sent":
            sent += 1

    # Follow-ups for contacted prospects past their window
    contacted = get_prospects(business_id, status="contacted", limit=50)
    now = datetime.now(timezone.utc)
    for p in contacted:
        if _daily_count(business_id) >= MAX_EMAILS_PER_DAY:
            break
        with _conn() as c:
            last = c.execute(
                "SELECT sent_at, sequence_step FROM outreach_log WHERE prospect_id=? ORDER BY sent_at DESC LIMIT 1",
                [p["id"]]
            ).fetchone()
        if not last:
            continue
        last_sent = datetime.fromisoformat(last["sent_at"])
        step = last["sequence_step"]
        next_step = step + 1
        if next_step >= len(SEQUENCE):
            # sequence complete — mark exhausted
            from core.backlink_prospector import update_prospect_status
            update_prospect_status(p["id"], "exhausted")
            continue
        days_required = SEQUENCE[next_step]["day"] - SEQUENCE[step]["day"]
        if (now - last_sent).days < days_required:
            continue
        r = send_outreach(
            business_id=business_id,
            prospect_id=p["id"],
            template=SEQUENCE[next_step]["template"],
            sender_name=sender_name,
            sender_email=sender_email,
            your_domain=your_domain,
            your_page=p.get("your_page_to_link", ""),
            sequence_step=next_step,
        )
        results.append(r)
        if r.get("status") == "sent":
            sent += 1

    log.info("run_outreach_queue  biz=%s  sent=%d  skipped=%d", business_id, sent, skipped)
    return {"sent": sent, "skipped": skipped, "total_processed": len(results)}


def get_outreach_stats(business_id: str) -> dict:
    with _conn() as c:
        total_sent = c.execute(
            "SELECT COUNT(*) FROM outreach_log WHERE business_id=? AND status='sent'", [business_id]
        ).fetchone()[0]
        replies = c.execute(
            "SELECT COUNT(*) FROM outreach_log WHERE business_id=? AND reply_detected=1", [business_id]
        ).fetchone()[0]
        today_count = _daily_count(business_id)
    reply_rate = (replies / total_sent * 100) if total_sent > 0 else 0
    return {
        "total_sent": total_sent,
        "replies_detected": replies,
        "reply_rate_pct": round(reply_rate, 1),
        "sent_today": today_count,
        "daily_cap": MAX_EMAILS_PER_DAY,
    }
