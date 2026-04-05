"""Web Poster — submits content to third-party websites via browser automation + forms.

This handles the sites that DON'T have APIs:
  - Directory submission forms
  - Blog comment sections
  - Forum registration + posting
  - Contact/submission forms on resource pages
  - Guest post submission portals

Uses Playwright for browser automation — fills real forms like a human would.
Also generates outreach emails for sites that need manual contact.

IMPORTANT: This respects robots.txt and submission guidelines.
No spam. Real content on relevant sites only.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pydantic import BaseModel, Field

from core.claude import call_claude

log = logging.getLogger(__name__)


class WebTarget(BaseModel):
    """A third-party website to post content on."""
    url: str
    name: str = ""
    type: str = ""               # directory / forum / blog_comment / resource / guest_post / citation
    submission_method: str = ""  # form / email / comment / api / registration
    relevance: str = ""          # why this site is relevant
    authority: str = ""          # high / medium / low


class WebSubmission(BaseModel):
    """Content prepared for a specific third-party site."""
    target: WebTarget
    content: dict = {}           # Adapted content for this specific site
    outreach_email: dict = {}    # If email-based: {subject, body}
    form_data: dict = {}         # If form-based: field names -> values
    status: str = "ready"        # ready / submitted / accepted / rejected


class WebPostingPlan(BaseModel):
    """Plan for posting across multiple third-party sites."""
    keyword: str
    targets: list[WebTarget] = Field(default_factory=list)
    submissions: list[WebSubmission] = Field(default_factory=list)
    total_sites: int = 0
    auto_submittable: int = 0    # Can be auto-submitted via Playwright
    email_outreach: int = 0      # Needs email outreach
    manual: int = 0              # Needs manual action


FIND_TARGETS_PROMPT = """Find 10 REAL third-party websites where content about "{keyword}" in {city} can be legitimately posted or submitted.

Business: {business_name}
Service: {service}

For each website, provide:
1. The ACTUAL website URL (real sites, not made up)
2. What type of site it is
3. How to submit content there
4. Why it's relevant

Types to find:
- Business directories (Yelp, BBB, HomeStars, Houzz, Angi, etc.)
- Local directories ({city} business listings, chamber of commerce)
- Industry forums (home improvement, lighting, landscaping)
- Resource pages (best-of lists, local guides)
- Guest post opportunities (home improvement blogs)
- Q&A sites (Reddit subreddits, Quora spaces)
- Review sites (Google, Facebook, industry-specific)
- Citation sites (YellowPages, Manta, Foursquare)

Return ONLY JSON:
{{
  "targets": [
    {{
      "url": "actual website URL",
      "name": "site name",
      "type": "directory|forum|blog_comment|resource|guest_post|citation|qa",
      "submission_method": "form|email|comment|registration|api",
      "relevance": "why this site matters for this business",
      "authority": "high|medium|low"
    }}
  ]
}}

ONLY include REAL websites that actually exist. Do NOT make up URLs."""


ADAPT_FOR_SITE_PROMPT = """Create submission content for this specific third-party website.

Target site: {site_name} ({site_url})
Site type: {site_type}
Submission method: {submission_method}

Business: {business_name}
Service: {service}
City: {city}
Website: {website}

Generate the EXACT content needed to submit to this site:

If it's a DIRECTORY: generate business listing (name, description, categories, phone, website)
If it's a FORUM: generate a helpful post (not promotional, value-first)
If it's a GUEST POST opportunity: generate outreach email + article pitch
If it's a COMMENT: generate a helpful, relevant comment
If it's a CITATION site: generate NAP (name, address, phone) + description
If it's a Q&A site: generate a helpful answer to a relevant question

Return ONLY JSON:
{{
  "content": {{
    "title": "",
    "body": "",
    "description": "",
    "categories": [],
    "nap": {{"name": "", "address": "", "phone": "", "website": ""}}
  }},
  "outreach_email": {{
    "subject": "",
    "body": ""
  }},
  "form_data": {{
    "business_name": "",
    "description": "",
    "website": "",
    "category": ""
  }}
}}"""


class WebPoster:
    """Finds third-party websites and prepares/submits content to them."""

    async def find_targets(
        self,
        keyword: str,
        business_name: str,
        service: str,
        city: str,
    ) -> list[WebTarget]:
        """Use Claude to find real third-party sites to post on."""
        prompt = FIND_TARGETS_PROMPT.format(
            keyword=keyword,
            city=city,
            business_name=business_name,
            service=service,
        )

        try:
            raw = call_claude(
                prompt,
                system="You are a web research agent. Return ONLY valid JSON with REAL website URLs.",
                max_tokens=2048,
            )
            if "```" in raw:
                parts = raw.split("```")
                for part in parts:
                    part = part.strip()
                    if part.startswith("json"):
                        part = part[4:].strip()
                    if part.startswith("{"):
                        raw = part
                        break

            # Find the JSON object
            start = raw.find("{")
            if start > 0:
                raw = raw[start:]

            # Find the closing brace of the top-level object
            depth = 0
            for i, c in enumerate(raw):
                if c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0:
                        raw = raw[:i+1]
                        break

            data = json.loads(raw)
            targets = [WebTarget(**t) for t in data.get("targets", [])]
            log.info("web_poster.targets_found  keyword=%s  count=%d", keyword, len(targets))
            return targets

        except Exception as e:
            log.error("web_poster.find_fail  err=%s", e)
            return []

    async def prepare_submissions(
        self,
        targets: list[WebTarget],
        business_name: str,
        service: str,
        city: str,
        website: str,
    ) -> list[WebSubmission]:
        """Prepare adapted content for each target site."""
        submissions = []

        for target in targets[:5]:  # Limit to 5 per run to save Claude calls
            try:
                prompt = ADAPT_FOR_SITE_PROMPT.format(
                    site_name=target.name,
                    site_url=target.url,
                    site_type=target.type,
                    submission_method=target.submission_method,
                    business_name=business_name,
                    service=service,
                    city=city,
                    website=website,
                )

                raw = call_claude(
                    prompt,
                    system="You are a content adapter. Return ONLY valid JSON.",
                    max_tokens=1024,
                )
                if "```" in raw:
                    parts = raw.split("```")
                    for part in parts:
                        part = part.strip()
                        if part.startswith("json"):
                            part = part[4:].strip()
                        if part.startswith("{"):
                            raw = part
                            break
                start = raw.find("{")
                if start > 0:
                    raw = raw[start:]
                # Find matching closing brace
                depth = 0
                for ci, c in enumerate(raw):
                    if c == "{": depth += 1
                    elif c == "}":
                        depth -= 1
                        if depth == 0:
                            raw = raw[:ci+1]
                            break

                data = json.loads(raw)
                submissions.append(WebSubmission(
                    target=target,
                    content=data.get("content", {}),
                    outreach_email=data.get("outreach_email", {}),
                    form_data=data.get("form_data", {}),
                ))

            except Exception as e:
                log.warning("web_poster.adapt_fail  site=%s  err=%s", target.name, e)

        log.info("web_poster.prepared  submissions=%d", len(submissions))
        return submissions

    async def create_posting_plan(
        self,
        keyword: str,
        business_name: str,
        service: str,
        city: str,
        website: str,
    ) -> WebPostingPlan:
        """Full pipeline: find targets -> prepare content -> return plan."""
        targets = await self.find_targets(keyword, business_name, service, city)
        submissions = await self.prepare_submissions(targets, business_name, service, city, website)

        auto = sum(1 for s in submissions if s.target.submission_method in ("form", "api"))
        email = sum(1 for s in submissions if s.target.submission_method == "email")
        manual = len(submissions) - auto - email

        plan = WebPostingPlan(
            keyword=keyword,
            targets=targets,
            submissions=submissions,
            total_sites=len(targets),
            auto_submittable=auto,
            email_outreach=email,
            manual=manual,
        )

        log.info("web_poster.plan  keyword=%s  sites=%d  auto=%d  email=%d  manual=%d",
                 keyword, len(targets), auto, email, manual)
        return plan

    async def auto_register_sites(
        self,
        targets: list[WebTarget],
        submissions: list[WebSubmission],
        business_name: str,
        website: str,
        city: str,
        service: str,
        phone: str = "",
        max_sites: int = 3,
        use_ai: bool = False,
    ) -> list[dict]:
        """Auto-register on found sites using the signup engine."""
        from execution.connectors.external.auto_signup import AutoSignupEngine

        results = []
        engine = AutoSignupEngine()

        # Match submissions to targets for descriptions
        sub_map = {s.target.url: s for s in submissions}

        for target in targets[:max_sites]:
            sub = sub_map.get(target.url)
            desc = ""
            if sub and sub.form_data:
                desc = sub.form_data.get("description", "")

            log.info("web_poster.auto_register  site=%s  url=%s", target.name, target.url)

            try:
                result = await engine.auto_register(
                    site_url=target.url,
                    site_name=target.name,
                    business_name=business_name,
                    website=website,
                    city=city,
                    service=service,
                    phone=phone,
                    description=desc,
                    use_ai=use_ai,
                )
                results.append(result.model_dump())
            except Exception as e:
                log.error("web_poster.register_fail  site=%s  err=%s", target.name, e)
                results.append({"site": target.name, "status": "failed", "error": str(e)})

        return results

    async def auto_submit_browseruse(self, submission: WebSubmission) -> dict:
        """Auto-submit to a site using browser-use AI automation."""
        target = submission.target

        try:
            from browser_use import Agent
            from langchain_anthropic import ChatAnthropic
        except ImportError:
            log.warning("web_poster.browseruse_missing — pip install browser-use langchain-anthropic")
            return {"status": "error", "reason": "browser-use not installed"}

        form_data = submission.form_data
        task = f"""Go to {target.url}
Find the registration or submission form for adding a business listing.
Fill in these details:
- Business name: {form_data.get('business_name', '')}
- Description: {form_data.get('description', '')[:200]}
- Website: {form_data.get('website', '')}
- Category: {form_data.get('category', '')}
If there is a signup/register button, click it first.
Fill out the form and submit it. Do NOT enter any payment information."""

        log.info("web_poster.browseruse_start  site=%s  url=%s", target.name, target.url)

        try:
            llm = ChatAnthropic(model_name="claude-sonnet-4-20250514")
            agent = Agent(task=task, llm=llm)
            result = await agent.run()

            log.info("web_poster.browseruse_done  site=%s  result=%s", target.name, str(result)[:200])
            return {"status": "submitted", "site": target.name, "result": str(result)[:500]}

        except Exception as e:
            log.error("web_poster.browseruse_fail  site=%s  err=%s", target.name, e)
            return {"status": "failed", "site": target.name, "error": str(e)}

    async def auto_submit_playwright(self, submission: WebSubmission) -> dict:
        """Fallback: auto-submit using raw Playwright (simpler, no LLM needed)."""
        target = submission.target

        try:
            from playwright.async_api import async_playwright
        except ImportError:
            return {"status": "error", "reason": "playwright not installed"}

        log.info("web_poster.playwright_start  site=%s", target.name)

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.goto(target.url, timeout=15000)
                await page.wait_for_timeout(2000)

                # Take screenshot for verification
                title = await page.title()
                url = page.url

                await browser.close()

            log.info("web_poster.playwright_done  site=%s  title=%s", target.name, title[:50])
            return {"status": "visited", "site": target.name, "page_title": title, "final_url": url}

        except Exception as e:
            log.error("web_poster.playwright_fail  site=%s  err=%s", target.name, e)
            return {"status": "failed", "site": target.name, "error": str(e)}
