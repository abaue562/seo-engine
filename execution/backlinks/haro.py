"""HARO (Help a Reporter Out / Connectively) ingestion and response system.

Parses HARO email digests, matches journalist queries to business expertise,
auto-drafts responses, and sends via SMTP.

HARO sends 3 digests per day: ~5:35am, 12:35pm, 5:35pm ET
Each digest contains journalist queries grouped by category.
"""

from __future__ import annotations

import asyncio
import imaplib
import json
import logging
import re
import smtplib
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class HAROQuery:
    category: str
    outlet: str
    query_text: str
    deadline: str
    email: str
    anonymous: bool = False
    keywords: list[str] = field(default_factory=list)


@dataclass
class HAROResponse:
    query: dict  # serialized HAROQuery
    business_name: str
    response_text: str
    sent_at: str
    status: str = "drafted"  # drafted | sent | responded


class HAROIngester:
    """Parses and responds to HARO journalist queries."""

    def parse_digest(self, email_body: str) -> list[HAROQuery]:
        """Parse raw HARO email text into structured queries."""
        queries: list[HAROQuery] = []

        # HARO queries are separated by lines of "=" or "-" characters
        # Each block has structured fields
        blocks = re.split(r"={3,}|-{3,}", email_body)

        for block in blocks:
            if len(block.strip()) < 50:
                continue

            # Extract fields
            category = self._extract_field(block, r"CATEGORY[:\s]+([^\n]+)")
            outlet = self._extract_field(block, r"(?:MEDIA OUTLET|OUTLET)[:\s]+([^\n]+)")
            query_text = self._extract_field(block, r"QUERY[:\s]+(.+?)(?=DEADLINE|EMAIL|$)", multiline=True)
            deadline = self._extract_field(block, r"DEADLINE[:\s]+([^\n]+)")
            email_match = re.search(r"[\w.+-]+@\w+\.\w+", block)
            email_addr = email_match.group(0) if email_match else ""
            anonymous = "anonymous" in block.lower()

            if not query_text or not email_addr:
                continue

            # Extract keywords from query
            words = re.findall(r"\b[A-Za-z]{4,}\b", query_text)
            keywords = list(set(w.lower() for w in words[:20]))

            queries.append(HAROQuery(
                category=category or "General",
                outlet=outlet or "Unknown Outlet",
                query_text=query_text.strip(),
                deadline=deadline or "ASAP",
                email=email_addr,
                anonymous=anonymous,
                keywords=keywords,
            ))

        log.info("haro.parsed  queries=%d", len(queries))
        return queries

    def _extract_field(self, text: str, pattern: str, multiline: bool = False) -> str:
        flags = re.IGNORECASE | (re.DOTALL if multiline else 0)
        m = re.search(pattern, text, flags)
        if m:
            return m.group(1).strip()[:500]
        return ""

    def match_to_business(
        self,
        queries: list[HAROQuery],
        business: dict,
        min_score: float = 0.3,
    ) -> list[tuple[HAROQuery, float]]:
        """Score and filter queries relevant to the business."""
        services = [s.lower() for s in business.get("services", [])]
        keywords = [k.lower() for k in business.get("keywords", [])]
        city = business.get("city", "").lower()
        category_hint = business.get("service_type", "").lower()
        all_terms = set(services + keywords + [city, category_hint])

        results: list[tuple[HAROQuery, float]] = []
        for query in queries:
            score = 0.0
            q_lower = query.query_text.lower()
            q_category = query.category.lower()

            # Keyword overlap in query text
            for term in all_terms:
                if term and term in q_lower:
                    score += 0.15

            # Category match
            if any(s in q_category for s in [category_hint, "home", "service", "contractor", "business"]):
                score += 0.25

            # Location mention
            if city and city in q_lower:
                score += 0.2

            # Cap at 1.0
            score = min(score, 1.0)

            if score >= min_score:
                results.append((query, round(score, 3)))

        results.sort(key=lambda x: x[1], reverse=True)
        return results

    async def draft_response(self, query: HAROQuery, business: dict) -> str:
        """Draft a HARO response using Claude."""
        try:
            from core.claude import call_claude
        except ImportError:
            return self._fallback_response(query, business)

        prompt = f"""You are a PR expert drafting a HARO response for a journalist query.

Business: {business.get('name', 'the business')}, {business.get('service_type', 'service company')} in {business.get('city', 'the area')}, {business.get('state', '')}.
Years in business: {business.get('years_in_business', 'over 5 years')}.
Services: {', '.join(business.get('services', []))}.
Any credentials/certifications: {business.get('credentials', 'Licensed and insured')}.

Journalist query: {query.query_text}
Category: {query.category}
Media outlet: {query.outlet}
Deadline: {query.deadline}

Write a HARO response that:
1. Opens with the most compelling insight or data point (not "Hi, I'm...")
2. Provides 2-3 specific, expert insights (not generic advice)
3. Includes one concrete statistic or observation from professional experience
4. Is 150-250 words total
5. Ends with: Full Name, Title, Business Name, Phone/Website

Write ONLY the response text. No subject line. No preamble."""

        try:
            response = await asyncio.to_thread(call_claude, prompt, max_tokens=500)
            return response or self._fallback_response(query, business)
        except Exception as e:
            log.warning("haro.draft_fail  err=%s", e)
            return self._fallback_response(query, business)

    def _fallback_response(self, query: HAROQuery, business: dict) -> str:
        return (
            f"As a {business.get('service_type', 'service')} professional with "
            f"{business.get('years_in_business', 'years of')} experience in {business.get('city', 'the area')}, "
            f"I can offer insight on this topic.\n\n"
            f"[Expert response to: {query.query_text[:100]}...]\n\n"
            f"-- {business.get('contact_name', 'Business Owner')}\n"
            f"{business.get('name', '')}\n"
            f"{business.get('website', '')}"
        )

    async def send_response(
        self,
        query: HAROQuery,
        response_text: str,
        sender_email: str,
        sender_name: str,
        smtp_config: dict,
    ) -> bool:
        """Send the HARO response email."""
        if not smtp_config.get("host") or not query.email:
            log.warning("haro.send_skip  no smtp config or query email")
            return False

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Re: {query.category} — {query.outlet}"
        msg["From"] = f"{sender_name} <{sender_email}>"
        msg["To"] = query.email
        msg.attach(MIMEText(response_text, "plain"))

        try:
            with smtplib.SMTP(smtp_config["host"], smtp_config.get("port", 587)) as server:
                server.starttls()
                if smtp_config.get("user") and smtp_config.get("pass"):
                    server.login(smtp_config["user"], smtp_config["pass"])
                server.sendmail(sender_email, [query.email], msg.as_string())
            log.info("haro.sent  outlet=%s  to=%s", query.outlet, query.email)
            return True
        except Exception as e:
            log.warning("haro.smtp_fail  err=%s  trying_aion_email", e)
            # Fallback: AION Email Sender service
            try:
                from core.aion_bridge import aion
                subject = f"Re: {query.category} — {query.outlet}"
                sent = aion.send_email(
                    to_email=query.email,
                    subject=subject,
                    body_html=f"<pre>{response_text}</pre>",
                    body_text=response_text,
                )
                if sent:
                    log.info("haro.sent_via_aion  outlet=%s  to=%s", query.outlet, query.email)
                    return True
            except Exception as e2:
                log.error("haro.aion_email_fail  err=%s", e2)
            return False

    def save_response(self, response: HAROResponse, storage_path: str = "data/storage/haro/"):
        """Persist HARO response to disk."""
        path = Path(storage_path)
        path.mkdir(parents=True, exist_ok=True)
        filename = f"response_{datetime.now(tz=timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        (path / filename).write_text(json.dumps(asdict(response), indent=2))

    def get_sent_today(self, storage_path: str = "data/storage/haro/") -> list[HAROResponse]:
        """Return responses already sent today (for rate limiting)."""
        today = date.today().isoformat()
        path = Path(storage_path)
        if not path.exists():
            return []
        results = []
        for f in path.glob("*.json"):
            data = json.loads(f.read_text())
            if data.get("sent_at", "").startswith(today) and data.get("status") == "sent":
                results.append(HAROResponse(**data))
        return results


class HAROEmailPoller:
    """Fetches HARO digest emails from an IMAP inbox."""

    HARO_SENDERS = [
        "haro@helpareporter.com",
        "noreply@connectively.us",
        "haro@cision.com",
    ]

    def __init__(self, imap_host: str, imap_user: str, imap_pass: str):
        self.host = imap_host
        self.user = imap_user
        self.password = imap_pass

    def fetch_latest_digest(self) -> str | None:
        """Connect via IMAP and return the latest HARO digest body."""
        if not self.host or not self.user:
            log.debug("haro.poller  no IMAP config")
            return None
        try:
            mail = imaplib.IMAP4_SSL(self.host)
            mail.login(self.user, self.password)
            mail.select("INBOX")

            # Search for HARO emails
            for sender in self.HARO_SENDERS:
                _, data = mail.search(None, f'FROM "{sender}" UNSEEN')
                ids = data[0].split()
                if ids:
                    latest_id = ids[-1]
                    _, msg_data = mail.fetch(latest_id, "(RFC822)")
                    raw = msg_data[0][1]
                    import email
                    msg = email.message_from_bytes(raw)
                    # Get plain text body
                    if msg.is_multipart():
                        for part in msg.walk():
                            if part.get_content_type() == "text/plain":
                                return part.get_payload(decode=True).decode("utf-8", errors="replace")
                    else:
                        return msg.get_payload(decode=True).decode("utf-8", errors="replace")

            mail.logout()
        except Exception as e:
            log.warning("haro.imap_fail  err=%s", e)
        return None
