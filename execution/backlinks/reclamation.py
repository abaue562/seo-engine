"""Link reclamation and broken link outreach system.

Two strategies:
  1. LinkReclaimer — finds unlinked brand mentions and requests a link
  2. BrokenLinkFinder — finds broken links on authority pages and pitches replacement
"""

from __future__ import annotations

import asyncio
import logging
import re
import smtplib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class MentionResult:
    url: str
    source_domain: str
    mention_snippet: str
    has_link: bool
    contact_email: str = ""
    outreach_sent: bool = False


@dataclass
class BrokenLinkResult:
    source_url: str
    broken_href: str
    anchor_text: str
    replacement_url: str = ""
    contact_email: str = ""
    outreach_sent: bool = False


@dataclass
class OutreachRecord:
    target_domain: str
    strategy: str  # "reclamation" | "broken_link"
    sent_at: str
    email: str
    subject: str
    status: str = "sent"


class LinkReclaimer:
    """Finds unlinked brand mentions and sends link reclamation outreach."""

    def __init__(self, dataforseo_login: str = "", dataforseo_password: str = ""):
        self.login = dataforseo_login
        self.password = dataforseo_password

    async def find_unlinked_mentions(
        self,
        brand_name: str,
        site_url: str,
        *,
        limit: int = 50,
    ) -> list[MentionResult]:
        """Search DataForSEO for brand mentions that don't link to our site."""
        if not self.login or not self.password:
            log.debug("reclamation.mentions  no DataForSEO credentials")
            return self._mock_mentions(brand_name, site_url)

        try:
            import httpx
            import base64

            auth = base64.b64encode(f"{self.login}:{self.password}".encode()).decode()
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    "https://api.dataforseo.com/v3/serp/google/organic/live/advanced",
                    headers={
                        "Authorization": f"Basic {auth}",
                        "Content-Type": "application/json",
                    },
                    json=[{
                        "keyword": f'"{brand_name}" -site:{site_url}',
                        "location_name": "United States",
                        "language_name": "English",
                        "device": "desktop",
                        "os": "windows",
                        "depth": limit,
                    }],
                )

            results: list[MentionResult] = []
            if resp.status_code == 200:
                data = resp.json()
                tasks = data.get("tasks", [])
                for task in tasks:
                    for res in task.get("result", []):
                        for item in res.get("items", []):
                            if item.get("type") == "organic":
                                url = item.get("url", "")
                                domain = re.sub(r"https?://(www\.)?", "", url).split("/")[0]
                                snippet = item.get("description", "")[:200]
                                if brand_name.lower() in snippet.lower():
                                    results.append(MentionResult(
                                        url=url,
                                        source_domain=domain,
                                        mention_snippet=snippet,
                                        has_link=False,
                                    ))
            log.info("reclamation.mentions  brand=%s  found=%d", brand_name, len(results))
            return results[:limit]

        except Exception as e:
            log.warning("reclamation.mentions_fail  err=%s", e)
            return []

    def _mock_mentions(self, brand_name: str, site_url: str) -> list[MentionResult]:
        """Return empty list when no credentials — no fake data in production."""
        return []

    async def find_contact_email(self, domain: str) -> str:
        """Try to find contact email for a domain via Hunter.io or header scraping."""
        try:
            import httpx
            # Try common contact page patterns
            for path in ["/contact", "/contact-us", "/about", "/"]:
                try:
                    async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
                        resp = await client.get(f"https://{domain}{path}")
                        if resp.status_code == 200:
                            emails = re.findall(
                                r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
                                resp.text,
                            )
                            # Filter out common noreply / script emails
                            filtered = [
                                e for e in emails
                                if not any(x in e.lower() for x in ["noreply", "example", "sentry", "@2x", ".png"])
                            ]
                            if filtered:
                                return filtered[0]
                except Exception:
                    continue
        except Exception as e:
            log.debug("reclamation.email_find_fail  domain=%s  err=%s", domain, e)
        return ""


class BrokenLinkFinder:
    """Finds broken external links on authority pages for link-building pitches."""

    def __init__(self, ahrefs_token: str = ""):
        self.ahrefs_token = ahrefs_token

    async def find_broken_backlinks(
        self,
        competitor_domain: str,
        *,
        limit: int = 30,
    ) -> list[BrokenLinkResult]:
        """Find broken backlinks pointing at a competitor via Ahrefs API."""
        if not self.ahrefs_token:
            log.debug("reclamation.broken  no Ahrefs token")
            return []

        try:
            import httpx
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    "https://apiv2.ahrefs.com",
                    params={
                        "token": self.ahrefs_token,
                        "from": "broken_backlinks",
                        "target": competitor_domain,
                        "mode": "domain",
                        "limit": limit,
                        "output": "json",
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    results = []
                    for item in data.get("refpages", []):
                        results.append(BrokenLinkResult(
                            source_url=item.get("url_from", ""),
                            broken_href=item.get("url_to", ""),
                            anchor_text=item.get("anchor", ""),
                        ))
                    log.info("reclamation.broken  domain=%s  found=%d", competitor_domain, len(results))
                    return results
        except Exception as e:
            log.warning("reclamation.broken_fail  err=%s", e)
        return []

    async def verify_broken(self, url: str) -> bool:
        """Confirm a URL is actually broken (4xx/5xx)."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
                resp = await client.head(url)
                return resp.status_code >= 400
        except Exception:
            return True  # Connection error = effectively broken


class OutreachSender:
    """Sends personalized outreach emails for link reclamation and broken link pitches."""

    def __init__(self, smtp_config: dict, sender_name: str, sender_email: str):
        self.smtp = smtp_config
        self.sender_name = sender_name
        self.sender_email = sender_email

    async def send_reclamation_email(
        self,
        mention: MentionResult,
        business: dict,
    ) -> bool:
        """Send a link reclamation request for an unlinked mention."""
        if not mention.contact_email:
            log.debug("reclamation.send_skip  no contact email for %s", mention.source_domain)
            return False

        subject = f"Quick note about your mention of {business.get('name', 'us')}"
        body = await self._draft_reclamation_email(mention, business)
        return await self._send(mention.contact_email, subject, body)

    async def send_broken_link_email(
        self,
        broken: BrokenLinkResult,
        business: dict,
    ) -> bool:
        """Send a broken link replacement pitch."""
        if not broken.contact_email:
            return False

        subject = f"Broken link on {broken.source_url.split('/')[2]} — free replacement"
        body = await self._draft_broken_link_email(broken, business)
        return await self._send(broken.contact_email, subject, body)

    async def _draft_reclamation_email(self, mention: MentionResult, business: dict) -> str:
        """Draft reclamation email via Claude or fallback."""
        try:
            from core.claude import call_claude
            prompt = f"""Write a short, friendly link reclamation email (under 120 words).

Context:
- Our business: {business.get('name')} — {business.get('service_type')} in {business.get('city')}, {business.get('state')}
- Our website: {business.get('website')}
- Their page mentioned us: {mention.url}
- Snippet: "{mention.mention_snippet}"

The email should:
1. Compliment their content briefly
2. Note you saw they mentioned us
3. Ask if they'd add a hyperlink to our site
4. Keep it under 4 sentences total
5. End with our name and website

Return only the email body text, no subject line."""

            result = await asyncio.to_thread(call_claude, prompt, max_tokens=300)
            return result or self._fallback_reclamation(mention, business)
        except Exception:
            return self._fallback_reclamation(mention, business)

    def _fallback_reclamation(self, mention: MentionResult, business: dict) -> str:
        return (
            f"Hi,\n\n"
            f"I came across your page at {mention.url} and noticed you mentioned "
            f"{business.get('name', 'our business')} — thank you!\n\n"
            f"Would you be open to adding a hyperlink to our website ({business.get('website', '')})? "
            f"It would help readers find us directly.\n\n"
            f"Thanks for considering it!\n\n"
            f"— {business.get('contact_name', 'The Team')}\n"
            f"{business.get('name', '')}\n"
            f"{business.get('website', '')}"
        )

    async def _draft_broken_link_email(self, broken: BrokenLinkResult, business: dict) -> str:
        """Draft broken link pitch via Claude or fallback."""
        try:
            from core.claude import call_claude
            prompt = f"""Write a short broken-link outreach email (under 130 words).

Context:
- Their page: {broken.source_url}
- The broken link: {broken.broken_href} (anchor: "{broken.anchor_text}")
- Our replacement page: {broken.replacement_url or business.get('website', '')}
- Our business: {business.get('name')} — {business.get('service_type')}

The email should:
1. Open by mentioning you found a broken link on their page
2. Specify the broken URL or anchor text
3. Offer our page as a helpful replacement (explain briefly why it fits)
4. Keep it friendly and brief — under 4 sentences
5. End with name and website

Return only the email body text, no subject line."""

            result = await asyncio.to_thread(call_claude, prompt, max_tokens=300)
            return result or self._fallback_broken(broken, business)
        except Exception:
            return self._fallback_broken(broken, business)

    def _fallback_broken(self, broken: BrokenLinkResult, business: dict) -> str:
        return (
            f"Hi,\n\n"
            f"I was reading your page at {broken.source_url} and noticed a broken link "
            f"(anchor: \"{broken.anchor_text}\") pointing to {broken.broken_href}.\n\n"
            f"We have a page that covers this topic well and might be a good replacement: "
            f"{broken.replacement_url or business.get('website', '')}\n\n"
            f"Happy to help if you'd like to swap it in!\n\n"
            f"— {business.get('contact_name', 'The Team')}\n"
            f"{business.get('name', '')}"
        )

    async def _send(self, to_email: str, subject: str, body: str) -> bool:
        """Send email via SMTP."""
        if not self.smtp.get("host") or not to_email:
            log.warning("reclamation.send_skip  no smtp or recipient")
            return False

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{self.sender_name} <{self.sender_email}>"
        msg["To"] = to_email
        msg.attach(MIMEText(body, "plain"))

        try:
            with smtplib.SMTP(self.smtp["host"], self.smtp.get("port", 587)) as server:
                server.starttls()
                if self.smtp.get("user") and self.smtp.get("pass"):
                    server.login(self.smtp["user"], self.smtp["pass"])
                server.sendmail(self.sender_email, [to_email], msg.as_string())
            log.info("reclamation.sent  to=%s  subject=%s", to_email, subject)
            return True
        except Exception as e:
            log.error("reclamation.send_fail  err=%s", e)
            return False

    def save_outreach(self, record: OutreachRecord, storage_path: str = "data/storage/outreach/"):
        """Persist outreach record to disk."""
        import json
        from dataclasses import asdict
        path = Path(storage_path)
        path.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"outreach_{record.strategy}_{ts}.json"
        (path / filename).write_text(json.dumps(asdict(record), indent=2))


async def run_reclamation_campaign(business: dict, smtp_config: dict) -> dict:
    """Run full link reclamation + broken link campaign for a business."""
    try:
        from config.settings import (
            DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD,
            AHREFS_API_TOKEN,
        )
    except ImportError:
        DATAFORSEO_LOGIN = ""
        DATAFORSEO_PASSWORD = ""
        AHREFS_API_TOKEN = ""

    reclaimer = LinkReclaimer(DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD)
    sender = OutreachSender(
        smtp_config=smtp_config,
        sender_name=business.get("contact_name", "The Team"),
        sender_email=business.get("owner_email", smtp_config.get("user", "")),
    )

    brand = business.get("name", "")
    site = business.get("website", "").replace("https://", "").replace("http://", "")

    # Find unlinked mentions
    mentions = await reclaimer.find_unlinked_mentions(brand, site)
    sent_reclamation = 0
    for mention in mentions[:10]:  # Cap at 10/run
        mention.contact_email = await reclaimer.find_contact_email(mention.source_domain)
        if mention.contact_email:
            ok = await sender.send_reclamation_email(mention, business)
            if ok:
                mention.outreach_sent = True
                sent_reclamation += 1
                sender.save_outreach(OutreachRecord(
                    target_domain=mention.source_domain,
                    strategy="reclamation",
                    sent_at=datetime.now(tz=timezone.utc).isoformat(),
                    email=mention.contact_email,
                    subject=f"Quick note about your mention of {brand}",
                ))

    log.info(
        "reclamation.campaign_done  brand=%s  mentions=%d  sent=%d",
        brand, len(mentions), sent_reclamation,
    )
    return {
        "mentions_found": len(mentions),
        "reclamation_sent": sent_reclamation,
    }
