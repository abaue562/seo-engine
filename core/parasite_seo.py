"""
Parasite SEO layer: auto-publishes content to high-DA platforms to capture
rankings the main site can't win yet. Targets informational/cost queries.

Platforms (by DA, publish method):
  GitHub Pages  DA 95  — GitHub API, static HTML, best for AI citation
  Medium        DA 92  — Medium API or Playwright
  Reddit        DA 93  — PRAW or Playwright (careful, value-first)
  Quora         DA 92  — Playwright (no API)
  LinkedIn      DA 98  — LinkedIn API or Playwright
  Dev.to        DA 78  — Dev.to API (free, accepts markdown)
"""
from __future__ import annotations
import base64
import hashlib
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)
_DB = "data/storage/seo_engine.db"

PLATFORMS = {
    "github_pages": {"da": 95, "content_type": "html", "method": "api"},
    "medium":       {"da": 92, "content_type": "markdown", "method": "api"},
    "devto":        {"da": 78, "content_type": "markdown", "method": "api"},
    "reddit":       {"da": 93, "content_type": "text", "method": "playwright"},
    "quora":        {"da": 92, "content_type": "text", "method": "playwright"},
    "linkedin":     {"da": 98, "content_type": "html", "method": "playwright"},
}


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS parasite_pages (
            id              TEXT PRIMARY KEY,
            business_id     TEXT NOT NULL,
            platform        TEXT NOT NULL,
            title           TEXT NOT NULL,
            slug            TEXT NOT NULL,
            content         TEXT NOT NULL,
            published_url   TEXT DEFAULT '',
            target_keyword  TEXT DEFAULT '',
            target_intent   TEXT DEFAULT 'informational',
            da_score        INTEGER DEFAULT 0,
            status          TEXT DEFAULT 'draft',
            serp_position   INTEGER DEFAULT 0,
            clicks          INTEGER DEFAULT 0,
            backlinks_to_site INTEGER DEFAULT 0,
            created_at      TEXT,
            updated_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pp_biz ON parasite_pages(business_id, platform, status);
    """)
    c.commit()
    return c


def _slug(text: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', text.lower()).strip('-')[:80]


def _page_id(business_id: str, platform: str, slug: str) -> str:
    return hashlib.md5(f"{business_id}:{platform}:{slug}".encode()).hexdigest()[:12]


# ── Content Generators ─────────────────────────────────────────────────────────

def _generate_github_page(service: str, location: str, facts: list[dict],
                           business_name: str, domain: str) -> tuple[str, str]:
    """Returns (title, html_content)"""
    year = datetime.now().year
    title = f"{service} Cost Guide {year} — {location}"
    facts_str = "\n".join(
        f"- {f['claim']}: {f['value']} {f.get('unit','')}"
        for f in facts[:10]
    )

    prompt = f"""Write a complete, standalone HTML page titled: "{title}"

This is a cost and buying guide for {service} in {location}, published by {business_name} ({domain}).

Local pricing data:
{facts_str}

Requirements:
- Complete HTML document with <html>, <head>, <body>
- Meta description, og:title, og:description tags
- H1: "{title}"
- Table: "Cost Breakdown: {service} in {location}" — service type | low | high | typical (8+ rows)
- Section: "What Affects the Price?" with 5 numbered factors
- Section: "How to Find a Reliable Contractor in {location}" with 4 tips
- Section: "Frequently Asked Questions" with 5 Q&A pairs using <details><summary>
- Dataset JSON-LD schema in <script type="application/ld+json">
- Backlink: "Get quotes from <a href='https://{domain}'>{business_name}</a> — local {location} specialists"
- Clean CSS in <style> tag (readable, professional)
- Updated {datetime.now().strftime('%B %Y')}
- HTML only"""

    try:
        from core.aion_bridge import aion
        html = aion.brain_complete(prompt, model='groq', max_tokens=2500)
        html = re.sub(r'^```html\s*', '', html.strip())
        html = re.sub(r'```$', '', html.strip())
    except Exception:
        from core.claude import call_claude
        html = call_claude(prompt, max_tokens=2500)
        html = re.sub(r'^```html\s*', '', html.strip())
        html = re.sub(r'```$', '', html.strip())

    return title, html


def _generate_medium_article(service: str, location: str, facts: list[dict],
                              business_name: str, domain: str) -> tuple[str, str]:
    """Returns (title, markdown_content)"""
    year = datetime.now().year
    title = f"I Got 5 Quotes for {service} in {location} — Here's What I Paid"
    facts_str = "\n".join(f"- {f['claim']}: {f['value']} {f.get('unit','')}" for f in facts[:8])

    prompt = f"""Write a Medium-style first-person article in Markdown.

Title: "{title}"
Year: {year}

Local data:
{facts_str}

Tone: Helpful homeowner sharing their experience, not a contractor. Honest, specific.

Structure:
# {title}

Intro (2-3 paragraphs: why I needed the work, how confusing pricing was)

## What I Was Quoted (Real Numbers)

| Contractor | Service Scope | Quote |
|---|---|---|
(5 rows with realistic variation)

## Factors That Changed the Price

(3-4 paragraphs with specific details)

## What I Wish I Knew Before Getting Quotes

(5 bullet points, each actionable)

## The Bottom Line

(1 paragraph with final recommendation)

*If you're in {location}, [this guide from {business_name}](https://{domain}) has current pricing ranges that helped me understand what was fair.*

---
*Updated {datetime.now().strftime('%B %Y')}. Data based on quotes collected in {location}.*

Markdown only, no HTML tags"""

    try:
        from core.aion_bridge import aion
        md = aion.brain_complete(prompt, model='groq', max_tokens=1500)
    except Exception:
        from core.claude import call_claude
        md = call_claude(prompt, max_tokens=1500)

    return title, md


def _generate_devto_article(service: str, location: str, facts: list[dict],
                             business_name: str, domain: str) -> tuple[str, str]:
    """Returns (title, markdown with frontmatter)"""
    title = f"The Real Cost of {service} in {location}: {datetime.now().year} Data"
    tags = "homeimprovement, diy, realestate, canada"
    facts_str = "\n".join(f"- {f['claim']}: {f['value']} {f.get('unit','')}" for f in facts[:8])

    prompt = f"""Write a dev.to article in Markdown with frontmatter.

---
title: "{title}"
published: true
description: "Real pricing data for {service} in {location}. Updated {datetime.now().strftime('%B %Y')} with local contractor quotes."
tags: {tags}
---

Write the article body:
- Opening hook (surprising stat or common mistake)
- "## By the Numbers" section with a markdown table of cost ranges
- "## Key Factors That Affect Price" (4-5 items with specific numbers)
- "## How to Vet a Contractor in {location}" (3-4 practical tips)
- "## Quick Reference Table" with service / typical range / what's included
- Closing with link: "Full pricing breakdown at [{domain}](https://{domain})"

Local data:
{facts_str}

Tone: informative, practical, no fluff. Markdown only."""

    try:
        from core.aion_bridge import aion
        md = aion.brain_complete(prompt, model='groq', max_tokens=1200)
    except Exception:
        from core.claude import call_claude
        md = call_claude(prompt, max_tokens=1200)

    return title, md


def _generate_reddit_post(service: str, location: str, facts: list[dict],
                           domain: str) -> tuple[str, str]:
    """Returns (title, post_body). Value-first, no obvious promotion."""
    facts_str = "\n".join(f"- {f['claim']}: {f['value']} {f.get('unit','')}" for f in facts[:6])

    title = f"Finally got my {service} done in {location} — sharing what I paid and learned"

    prompt = f"""Write a Reddit post for r/HomeImprovement about {service} in {location}.

Title: "{title}"

Style: Authentic homeowner sharing experience. Helpful, specific, no obvious promotion.
- Intro: what prompted the project (1 paragraph)
- What I got quoted (3-4 paragraphs with specific numbers, what varied and why)
- Lessons learned (4-5 bullet points)
- "Edit: A few people DM'd asking — I found [this pricing breakdown](https://{domain}) helpful when comparing quotes"
- End with an open question to encourage comments

Local data for context:
{facts_str}

Plain text, no markdown headers. Natural Reddit writing style."""

    try:
        from core.aion_bridge import aion
        body = aion.brain_complete(prompt, model='groq', max_tokens=800)
    except Exception:
        from core.claude import call_claude
        body = call_claude(prompt, max_tokens=800)

    return title, body


def _generate_quora_answer(service: str, location: str, facts: list[dict],
                            business_name: str, domain: str) -> tuple[str, str]:
    """Returns (question, answer). Direct-answer format for AI citation."""
    year = datetime.now().year
    question = f"How much does {service} cost in {location} in {year}?"
    facts_str = "\n".join(f"- {f['claim']}: {f['value']} {f.get('unit','')}" for f in facts[:8])

    prompt = f"""Write a comprehensive Quora answer to: "{question}"

Format: Direct answer first, then detail.

Structure:
**Short answer:** [price range] depending on [2-3 key factors]

**In {location} specifically:** (2 paragraphs with local context and pricing)

**What affects the price:**
- Factor 1: [specific impact]
- Factor 2: [specific impact]
- Factor 3: [specific impact]
- Factor 4: [specific impact]

**Typical price ranges in {location}:**
| Service Type | Low | High | Notes |
(5-6 rows)

**Tips to get a fair price:**
1. [specific tip]
2. [specific tip]
3. [specific tip]

*[{business_name}]({domain}) publishes updated pricing ranges for {location} if you want a benchmark before getting quotes.*

Local data:
{facts_str}

Write as a knowledgeable local professional. Direct, specific, helpful."""

    try:
        from core.aion_bridge import aion
        answer = aion.brain_complete(prompt, model='groq', max_tokens=1000)
    except Exception:
        from core.claude import call_claude
        answer = call_claude(prompt, max_tokens=1000)

    return question, answer


# ── Publishers ─────────────────────────────────────────────────────────────────

def publish_github_page(
    repo_owner: str,
    repo_name: str,
    token: str,
    path: str,
    html: str,
    commit_msg: str = "Add content page",
) -> str:
    """Publish HTML to GitHub Pages repo. Returns URL or empty string."""
    import requests
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    # Check if file exists (need SHA for update)
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{path}"
    r = requests.get(url, headers=headers, timeout=10)
    sha = r.json().get("sha", "") if r.status_code == 200 else ""

    content_b64 = base64.b64encode(html.encode()).decode()
    payload: dict = {"message": commit_msg, "content": content_b64}
    if sha:
        payload["sha"] = sha

    r = requests.put(url, headers=headers, json=payload, timeout=15)
    if r.status_code in (200, 201):
        pub_url = f"https://{repo_owner}.github.io/{repo_name}/{path.replace('index.html','')}"
        log.info("github_pages.published  url=%s", pub_url)
        return pub_url
    else:
        log.warning("github_pages.failed  status=%d  body=%s", r.status_code, r.text[:200])
        return ""


def publish_devto(api_key: str, title: str, body_md: str, tags: list[str]) -> str:
    """Publish to Dev.to via API. Returns canonical URL or empty string."""
    import requests
    payload = {
        "article": {
            "title": title,
            "published": True,
            "body_markdown": body_md,
            "tags": tags[:4],
        }
    }
    r = requests.post(
        "https://dev.to/api/articles",
        headers={"api-key": api_key, "Content-Type": "application/json"},
        json=payload,
        timeout=15,
    )
    if r.status_code == 201:
        url = r.json().get("canonical_url", r.json().get("url", ""))
        log.info("devto.published  url=%s", url)
        return url
    log.warning("devto.failed  status=%d  body=%s", r.status_code, r.text[:200])
    return ""


def publish_medium(integration_token: str, title: str, content_md: str,
                   user_id: str = "") -> str:
    """Publish to Medium via Integration Token API. Returns URL or empty string."""
    import requests
    if not user_id:
        r = requests.get(
            "https://api.medium.com/v1/me",
            headers={"Authorization": f"Bearer {integration_token}"},
            timeout=10,
        )
        if r.status_code == 200:
            user_id = r.json().get("data", {}).get("id", "")
    if not user_id:
        log.warning("medium.publish: could not get user_id")
        return ""

    payload = {
        "title": title,
        "contentFormat": "markdown",
        "content": content_md,
        "publishStatus": "public",
    }
    r = requests.post(
        f"https://api.medium.com/v1/users/{user_id}/posts",
        headers={"Authorization": f"Bearer {integration_token}", "Content-Type": "application/json"},
        json=payload,
        timeout=15,
    )
    if r.status_code == 201:
        url = r.json().get("data", {}).get("url", "")
        log.info("medium.published  url=%s", url)
        return url
    log.warning("medium.failed  status=%d  body=%s", r.status_code, r.text[:200])
    return ""


def publish_via_playwright(platform: str, title: str, body: str,
                            credentials: dict) -> str:
    """
    Use Firecrawl/Playwright to publish to Reddit, Quora, or LinkedIn.
    Returns published URL or empty string.
    """
    try:
        from core.aion_bridge import aion

        if platform == "reddit":
            subreddit = credentials.get("subreddit", "HomeImprovement")
            script = f"""
Navigate to https://www.reddit.com/r/{subreddit}/submit
Fill in title field with: {title[:300]}
Fill in text field with: {body[:5000]}
Click submit button
Return the URL of the submitted post
"""
        elif platform == "quora":
            script = f"""
Navigate to https://www.quora.com/answer
Search for existing question: {title}
If found, click Add Answer
If not found, click Ask Question first then answer
Paste this answer: {body[:3000]}
Submit the answer
Return the URL
"""
        elif platform == "linkedin":
            script = f"""
Navigate to https://www.linkedin.com/pulse/new-story/
Click on the title field and type: {title}
Click on the content area and paste: {body[:5000]}
Click Publish
Return the URL of the published article
"""
        else:
            return ""

        # Playwright-based publishing queued — log script for manual/future automation
        log.info("playwright.queued  platform=%s  script_len=%d", platform, len(script))
        # Return empty to mark as pending_credentials until Playwright automation is wired
        return ""
    except Exception:
        log.exception("publish_via_playwright.error  platform=%s", platform)
    return ""


# ── Orchestrator ───────────────────────────────────────────────────────────────

def generate_parasite_content(
    business_id: str,
    platform: str,
    service: str,
    location: str,
    facts: list[dict],
    business_name: str = "",
    domain: str = "",
) -> dict:
    """Generate platform-optimized content. Saves draft, returns record."""
    if platform == "github_pages":
        title, content = _generate_github_page(service, location, facts, business_name, domain)
    elif platform == "medium":
        title, content = _generate_medium_article(service, location, facts, business_name, domain)
    elif platform == "devto":
        title, content = _generate_devto_article(service, location, facts, business_name, domain)
    elif platform == "reddit":
        title, content = _generate_reddit_post(service, location, facts, domain)
    elif platform == "quora":
        title, content = _generate_quora_answer(service, location, facts, business_name, domain)
    elif platform == "linkedin":
        title, content = _generate_medium_article(service, location, facts, business_name, domain)
    else:
        raise ValueError(f"Unknown platform: {platform}")

    slug = _slug(title)
    pid = _page_id(business_id, platform, slug)
    now = datetime.now(timezone.utc).isoformat()
    da = PLATFORMS.get(platform, {}).get("da", 0)

    with _conn() as c:
        c.execute("""
            INSERT INTO parasite_pages
                (id, business_id, platform, title, slug, content, target_keyword,
                 target_intent, da_score, status, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                content=excluded.content, title=excluded.title,
                updated_at=excluded.updated_at
        """, [pid, business_id, platform, title, slug, content,
              f"{service} {location}", "informational", da, "draft", now, now])

    log.info("parasite_content.generated  biz=%s  platform=%s  title=%s", business_id, platform, title[:50])
    return {"id": pid, "platform": platform, "title": title, "slug": slug,
            "da_score": da, "status": "draft", "content_len": len(content)}


def publish_parasite_page(page_id: str) -> dict:
    """
    Publish a draft parasite page using configured API keys / Playwright.
    Returns updated record with published_url.
    """
    with _conn() as c:
        row = c.execute("SELECT * FROM parasite_pages WHERE id=?", [page_id]).fetchone()
    if not row:
        return {"error": "page not found"}

    page = dict(row)
    platform = page["platform"]
    url = ""

    if platform == "github_pages":
        token = os.environ.get("GITHUB_TOKEN", "")
        owner = os.environ.get("GITHUB_PAGES_OWNER", "")
        repo = os.environ.get("GITHUB_PAGES_REPO", "local-guides")
        if token and owner:
            path = f"{page['slug']}/index.html"
            url = publish_github_page(owner, repo, token, path, page["content"],
                                      f"Add: {page['title']}")

    elif platform == "devto":
        api_key = os.environ.get("DEVTO_API_KEY", "")
        if api_key:
            tags = ["homeimprovement", "canada", "realestate", "diy"]
            url = publish_devto(api_key, page["title"], page["content"], tags)

    elif platform == "medium":
        token = os.environ.get("MEDIUM_TOKEN", "")
        if token:
            url = publish_medium(token, page["title"], page["content"])

    elif platform in ("reddit", "quora", "linkedin"):
        creds = {
            "subreddit": os.environ.get("REDDIT_SUBREDDIT", "HomeImprovement"),
            "reddit_user": os.environ.get("REDDIT_USER", ""),
            "reddit_pass": os.environ.get("REDDIT_PASS", ""),
        }
        url = publish_via_playwright(platform, page["title"], page["content"], creds)

    status = "published" if url else "pending_credentials"
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("""
            UPDATE parasite_pages SET published_url=?, status=?, updated_at=? WHERE id=?
        """, [url, status, now, page_id])

    log.info("parasite_page.publish  id=%s  platform=%s  status=%s  url=%s",
             page_id, platform, status, url)
    return {"id": page_id, "platform": platform, "status": status, "published_url": url}


def check_parasite_rankings(business_id: str) -> list[dict]:
    """Check SERP positions for all published parasite pages."""
    from core.serp_scraper import scrape_serp
    with _conn() as c:
        rows = c.execute("""
            SELECT id, platform, title, target_keyword, published_url
            FROM parasite_pages WHERE business_id=? AND status='published'
        """, [business_id]).fetchall()

    results = []
    for row in rows:
        page = dict(row)
        kw = page["target_keyword"]
        url = page["published_url"]
        if not kw or not url:
            continue
        try:
            serp = scrape_serp(kw, location="")
            position = 0
            for result in serp.get("organic", []):
                if url in result.get("url", "") or row["platform"] in result.get("url", ""):
                    position = result["position"]
                    break
            if position:
                with _conn() as c:
                    c.execute("UPDATE parasite_pages SET serp_position=? WHERE id=?",
                              [position, page["id"]])
            results.append({**page, "serp_position": position})
        except Exception:
            pass

    return results


def run_parasite_sweep(business_id: str, platforms: list[str] | None = None) -> list[dict]:
    """
    Generate parasite content for all configured platforms.
    Publishes immediately where API keys are available.
    """
    if platforms is None:
        platforms = ["github_pages", "medium", "devto", "reddit", "quora"]

    # Load business profile
    try:
        all_biz = json.loads(open("data/storage/businesses.json").read())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
        biz = next((b for b in biz_list
                    if b.get("id") == business_id or b.get("business_id") == business_id), {})
    except Exception:
        biz = {}

    business_name = biz.get("name", biz.get("business_name", "Local Business"))
    domain = biz.get("domain", "")
    location = biz.get("location", biz.get("city", ""))
    services = biz.get("services", biz.get("service_types", []))
    service = services[0] if services else "home services"

    if not location:
        log.warning("run_parasite_sweep: no location  biz=%s", business_id)
        return []

    # Load facts
    from core.citable_data import get_facts
    facts = get_facts(business_id, limit=20)

    results = []
    for platform in platforms:
        try:
            record = generate_parasite_content(
                business_id=business_id,
                platform=platform,
                service=service,
                location=location,
                facts=facts,
                business_name=business_name,
                domain=domain,
            )
            # Attempt publish immediately
            pub = publish_parasite_page(record["id"])
            record["published_url"] = pub.get("published_url", "")
            record["status"] = pub.get("status", "draft")
            results.append(record)
            log.info("parasite_sweep.done  platform=%s  status=%s", platform, record["status"])
        except Exception:
            log.exception("parasite_sweep.error  platform=%s  biz=%s", platform, business_id)

    return results


def get_parasite_pages(business_id: str, platform: str = "", status: str = "") -> list[dict]:
    with _conn() as c:
        q = "SELECT id, platform, title, slug, published_url, target_keyword, da_score, status, serp_position, created_at FROM parasite_pages WHERE business_id=?"
        params: list = [business_id]
        if platform:
            q += " AND platform=?"
            params.append(platform)
        if status:
            q += " AND status=?"
            params.append(status)
        q += " ORDER BY da_score DESC, created_at DESC"
        return [dict(r) for r in c.execute(q, params).fetchall()]


def get_parasite_stats(business_id: str) -> dict:
    with _conn() as c:
        rows = c.execute("""
            SELECT platform, status, COUNT(*) as n,
                   SUM(CASE WHEN serp_position>0 THEN 1 ELSE 0 END) as ranking,
                   AVG(CASE WHEN serp_position>0 THEN serp_position ELSE NULL END) as avg_pos
            FROM parasite_pages WHERE business_id=?
            GROUP BY platform, status
        """, [business_id]).fetchall()
        total = c.execute("SELECT COUNT(*) as n FROM parasite_pages WHERE business_id=?",
                          [business_id]).fetchone()["n"]
        published = c.execute(
            "SELECT COUNT(*) as n FROM parasite_pages WHERE business_id=? AND status='published'",
            [business_id]).fetchone()["n"]

    by_platform: dict[str, dict] = {}
    for r in rows:
        p = r["platform"]
        if p not in by_platform:
            by_platform[p] = {"total": 0, "published": 0, "ranking": 0}
        by_platform[p]["total"] += r["n"]
        if r["status"] == "published":
            by_platform[p]["published"] += r["n"]
        by_platform[p]["ranking"] += r["ranking"] or 0

    return {
        "business_id": business_id,
        "total_pages": total,
        "published": published,
        "pending_credentials": total - published,
        "by_platform": by_platform,
    }
