"""llms.txt Generator + AI Crawler Auditor.

llms.txt is an emerging standard (like robots.txt but for LLMs) that tells
AI systems what a site is about, its key pages, and how to cite it.

Also audits robots.txt for AI crawler access (14 crawlers across 3 tiers).
"""

from __future__ import annotations

import re
import logging
from urllib.parse import urlparse, urljoin

from models.business import BusinessContext

log = logging.getLogger(__name__)

# =====================================================================
# AI Crawler Definitions (14 crawlers, 3 tiers)
# =====================================================================

AI_CRAWLERS = {
    # Tier 1 — Critical (recommend ALLOW)
    "GPTBot": {"tier": 1, "org": "OpenAI", "weight": 10},
    "OAI-SearchBot": {"tier": 1, "org": "OpenAI", "weight": 10},
    "ChatGPT-User": {"tier": 1, "org": "OpenAI", "weight": 10},
    "ClaudeBot": {"tier": 1, "org": "Anthropic", "weight": 10},
    "PerplexityBot": {"tier": 1, "org": "Perplexity AI", "weight": 10},
    # Tier 2 — Important
    "Google-Extended": {"tier": 2, "org": "Google", "weight": 5},
    "GoogleOther": {"tier": 2, "org": "Google", "weight": 5},
    "Applebot-Extended": {"tier": 2, "org": "Apple", "weight": 5},
    "Amazonbot": {"tier": 2, "org": "Amazon", "weight": 5},
    "FacebookBot": {"tier": 2, "org": "Meta", "weight": 5},
    # Tier 3 — Training only
    "CCBot": {"tier": 3, "org": "Common Crawl", "weight": 2},
    "anthropic-ai": {"tier": 3, "org": "Anthropic", "weight": 2},
    "Bytespider": {"tier": 3, "org": "ByteDance", "weight": 1},
    "cohere-ai": {"tier": 3, "org": "Cohere", "weight": 2},
}

SECTION_RULES = {
    "Products & Services": ["/pricing", "/feature", "/product", "/solution", "/demo", "/service", "/shop"],
    "Resources & Blog": ["/blog", "/article", "/resource", "/guide", "/learn", "/docs", "/how-to", "/tips"],
    "Company": ["/about", "/team", "/career", "/contact", "/press", "/partner"],
    "Support": ["/help", "/support", "/faq", "/status"],
}


def generate_llms_txt(business: BusinessContext) -> str:
    """Generate llms.txt content for the business website."""
    lines = [
        f"# {business.business_name}",
        "",
        f"> {business.primary_service} services in {business.primary_city}",
        f"> Serving: {', '.join(business.service_areas) if business.service_areas else business.primary_city}",
        "",
        "## About",
        "",
        f"{business.business_name} is a {business.primary_service.lower()} company "
        f"based in {business.primary_city} with {business.years_active} years of experience. "
        f"We serve {', '.join(business.service_areas[:3]) if business.service_areas else business.primary_city}.",
        "",
        "## Services",
        "",
        f"- {business.primary_service}",
    ]

    for svc in business.secondary_services:
        lines.append(f"- {svc}")

    lines.extend([
        "",
        "## Service Area",
        "",
    ])
    for area in business.service_areas:
        lines.append(f"- {area}")

    if business.reviews_count > 0:
        lines.extend([
            "",
            "## Reputation",
            "",
            f"- Rating: {business.rating}/5",
            f"- Reviews: {business.reviews_count}",
        ])

    lines.extend([
        "",
        "## Contact",
        "",
        f"- Website: {business.website}",
    ])

    if business.gbp_url:
        lines.append(f"- Google Business: {business.gbp_url}")

    lines.extend([
        "",
        "## Key Pages",
        "",
        f"- [Home]({business.website})",
    ])

    for kw in business.primary_keywords[:5]:
        slug = kw.lower().replace(" ", "-")
        lines.append(f"- [{kw.title()}]({business.website}/{slug})")

    log.info("llms_txt.generated  biz=%s  lines=%d", business.business_name, len(lines))
    return "\n".join(lines)


def generate_llms_full_txt(business: BusinessContext, faqs: list[dict] | None = None) -> str:
    """Generate llms-full.txt with detailed content including FAQs."""
    base = generate_llms_txt(business)

    if faqs:
        faq_lines = [
            "",
            "## Frequently Asked Questions",
            "",
        ]
        for faq in faqs[:10]:
            q = faq.get("question", "")
            a = faq.get("direct_answer", faq.get("answer", ""))
            if q and a:
                faq_lines.append(f"### {q}")
                faq_lines.append("")
                faq_lines.append(a)
                faq_lines.append("")

        base += "\n".join(faq_lines)

    return base


# =====================================================================
# Website-crawl based llms.txt (fetches real pages)
# =====================================================================

def generate_llms_txt_from_url(url: str, max_per_section: int = 10) -> str:
    """Generate llms.txt by crawling the actual website.

    Fetches homepage, discovers internal links, classifies into sections.
    """
    import requests
    from bs4 import BeautifulSoup

    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0 (compatible; SEOEngine/1.0)"})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    site_name = soup.find("title").get_text(strip=True) if soup.find("title") else parsed.netloc
    meta = soup.find("meta", attrs={"name": "description"})
    meta_desc = meta.get("content", "").strip() if meta else ""

    # Collect internal links
    seen = set()
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        if href.startswith("/"):
            href = urljoin(base_url, href)
        elif not href.startswith("http"):
            continue
        lp = urlparse(href)
        if lp.netloc and lp.netloc != parsed.netloc:
            continue
        path = lp.path.rstrip("/")
        if not path or path in seen:
            continue
        if any(path.endswith(ext) for ext in [".jpg", ".png", ".pdf", ".css", ".js", ".svg"]):
            continue
        seen.add(path)
        links.append({"url": href, "path": path, "text": text or path.split("/")[-1].replace("-", " ").title()})

    # Classify
    sections: dict[str, list] = {}
    for link in links:
        path_lower = link["path"].lower()
        matched = "Main Pages"
        for section, keywords in SECTION_RULES.items():
            if any(kw in path_lower for kw in keywords):
                matched = section
                break
        sections.setdefault(matched, []).append(link)

    # Build output
    lines = [f"# {site_name}", f"> {meta_desc or f'Official website for {parsed.netloc}'}", ""]
    for section in ["Main Pages", "Products & Services", "Resources & Blog", "Company", "Support"]:
        slinks = sections.get(section, [])
        if not slinks:
            continue
        lines.append(f"## {section}")
        for sl in slinks[:max_per_section]:
            lines.append(f"- [{sl['text']}]({sl['url']})")
        lines.append("")

    lines.extend(["## Contact", f"- Website: {base_url}", f"- Email: contact@{parsed.netloc}", ""])
    log.info("llms_txt.generated_from_url  url=%s  links=%d", url, len(links))
    return "\n".join(lines)


# =====================================================================
# AI Crawler Audit
# =====================================================================

def audit_crawler_access(robots_txt: str) -> dict:
    """Audit robots.txt for AI crawler access.

    Returns per-crawler status, score (0-100), and recommendations.
    """
    results = {}
    total_score = 0
    max_score = sum(c["weight"] for c in AI_CRAWLERS.values())
    lines = robots_txt.lower().split("\n")

    for crawler, info in AI_CRAWLERS.items():
        blocked = False
        for i, line in enumerate(lines):
            if f"user-agent: {crawler.lower()}" in line:
                for j in range(i + 1, min(i + 5, len(lines))):
                    if lines[j].strip().startswith("disallow: /"):
                        blocked = True
                        break
                    if lines[j].strip().startswith("user-agent:"):
                        break
        # Check blanket block
        for i, line in enumerate(lines):
            if "user-agent: *" in line:
                for j in range(i + 1, min(i + 5, len(lines))):
                    if lines[j].strip() == "disallow: /":
                        blocked = True
                        break
                    if lines[j].strip().startswith("user-agent:"):
                        break

        if not blocked:
            total_score += info["weight"]
        results[crawler] = {"status": "blocked" if blocked else "allowed", "tier": info["tier"], "org": info["org"]}

    has_llms = "llms.txt" in robots_txt.lower()
    has_sitemap = "sitemap:" in robots_txt.lower()
    bonus = (5 if has_llms else 0) + (5 if has_sitemap else 0)
    final_score = min(100, int((total_score / max_score) * 90) + bonus)

    blocked_list = [n for n, r in results.items() if r["status"] == "blocked"]
    recommendations = []
    for name in blocked_list:
        info = AI_CRAWLERS[name]
        if info["tier"] == 1:
            recommendations.append(f"CRITICAL: Unblock {name} ({info['org']})")
        elif info["tier"] == 2:
            recommendations.append(f"IMPORTANT: Allow {name} ({info['org']})")
    if not has_llms:
        recommendations.append("Add llms.txt to help AI crawlers understand your site")

    return {
        "score": final_score,
        "crawlers": results,
        "blocked": blocked_list,
        "allowed": [n for n, r in results.items() if r["status"] == "allowed"],
        "has_llms_txt": has_llms,
        "has_sitemap": has_sitemap,
        "recommendations": recommendations,
    }


def audit_site_crawlers(url: str) -> dict:
    """Fetch robots.txt from a URL and audit it."""
    import requests
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    try:
        resp = requests.get(robots_url, timeout=10)
        if resp.status_code == 200:
            result = audit_crawler_access(resp.text)
            # Also check if llms.txt actually exists on the server
            try:
                llms_url = f"{parsed.scheme}://{parsed.netloc}/llms.txt"
                llms_resp = requests.get(llms_url, timeout=5)
                if llms_resp.status_code == 200:
                    result["has_llms_txt"] = True
            except Exception:
                pass
            return result
        return {
            "score": 100,
            "note": "No robots.txt found — all crawlers allowed by default",
            "crawlers": {},
            "blocked": [],
            "allowed": list(AI_CRAWLERS.keys()),
            "has_llms_txt": False,
            "has_sitemap": False,
            "recommendations": ["Add llms.txt to help AI crawlers understand your site"],
        }
    except Exception as e:
        return {"error": str(e)}


# =====================================================================
# New AI Crawler Setup Helpers
# =====================================================================

def generate_robots_txt_additions(site_url: str = "") -> str:
    """Return the lines to add to robots.txt to explicitly allow AI crawlers.

    These directives tell all major AI search crawlers they are welcome
    to index the site for inclusion in AI-generated responses.
    """
    lines = [
        "# Allow AI search crawlers",
        "User-agent: GPTBot",
        "Allow: /",
        "",
        "User-agent: ClaudeBot",
        "Allow: /",
        "",
        "User-agent: PerplexityBot",
        "Allow: /",
        "",
        "User-agent: Bingbot",
        "Allow: /",
        "",
        "User-agent: anthropic-ai",
        "Allow: /",
        "",
        "User-agent: OAI-SearchBot",
        "Allow: /",
        "",
        "User-agent: ChatGPT-User",
        "Allow: /",
        "",
        "User-agent: Google-Extended",
        "Allow: /",
        "",
        "User-agent: FacebookBot",
        "Allow: /",
    ]
    if site_url:
        parsed = urlparse(site_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        lines += [
            "",
            "# llms.txt for AI understanding",
            f"# {base}/llms.txt",
        ]
    return "\n".join(lines)


def validate_ai_crawler_access(site_url: str) -> dict:
    """Fetch robots.txt from site_url and check whether each AI crawler is allowed or blocked.

    Returns {gptbot: 'allowed'|'blocked'|'unknown', claudebot: ..., perplexitybot: ..., bingbot: ...}
    along with a full per-crawler status dict.
    """
    import requests as _req

    parsed = urlparse(site_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    try:
        resp = _req.get(robots_url, timeout=10, headers={"User-Agent": "SEOEngine/1.0"})
    except Exception as e:
        log.warning("validate_ai_crawlers.fetch_error  url=%s  err=%s", robots_url, e)
        return {
            "error": str(e),
            "gptbot": "unknown",
            "claudebot": "unknown",
            "perplexitybot": "unknown",
            "bingbot": "unknown",
            "anthropic_ai": "unknown",
        }

    if resp.status_code != 200:
        # No robots.txt = everything allowed by default
        return {
            "note": f"No robots.txt found (HTTP {resp.status_code}) — all crawlers allowed by default",
            "gptbot": "allowed",
            "claudebot": "allowed",
            "perplexitybot": "allowed",
            "bingbot": "allowed",
            "anthropic_ai": "allowed",
        }

    robots_text = resp.text
    audit = audit_crawler_access(robots_text)
    crawlers = audit.get("crawlers", {})

    def _status(name: str) -> str:
        info = crawlers.get(name)
        if info is None:
            # Not mentioned explicitly — blanket block check
            if "Disallow: /" in robots_text and "User-agent: *" in robots_text:
                return "blocked"
            return "allowed"
        return info.get("status", "unknown")

    return {
        "robots_txt_url": robots_url,
        "gptbot": _status("GPTBot"),
        "claudebot": _status("ClaudeBot"),
        "perplexitybot": _status("PerplexityBot"),
        "bingbot": _status("Bingbot"),
        "anthropic_ai": _status("anthropic-ai"),
        "oai_searchbot": _status("OAI-SearchBot"),
        "google_extended": _status("Google-Extended"),
        "full_audit": audit,
    }


def generate_sitemap_ai_xml(urls: list) -> str:
    """Generate a dedicated sitemap-ai.xml for AI-important pages.

    Each url entry dict may contain:
        loc           (required)
        lastmod       (defaults to today)
        changefreq    (defaults to 'monthly')
        priority      (0.0–1.0, defaults to 0.8)
        ai_priority   ('high' | 'medium' | 'low', defaults to 'medium')
        ai_content_type  (e.g. 'informational', 'service', 'faq')

    The sitemap includes a custom <ai:priority> extension tag.
    """
    from datetime import date as _date

    today = _date.today().isoformat()

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"',
        '        xmlns:ai="https://schema.org/ai-sitemap/1.0"',
        '        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
        '        xsi:schemaLocation="http://www.sitemaps.org/schemas/sitemap/0.9',
        '          http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd">',
        "  <!-- AI-priority sitemap: pages optimised for AI search citation -->",
    ]

    for entry in urls:
        loc = entry.get("loc", "")
        if not loc:
            continue

        lastmod = entry.get("lastmod", today)
        changefreq = entry.get("changefreq", "monthly")
        ai_priority = entry.get("ai_priority", "medium")
        ai_content_type = entry.get("ai_content_type", "informational")

        try:
            priority = max(0.0, min(1.0, float(entry.get("priority", 0.8))))
        except (TypeError, ValueError):
            priority = 0.8

        lines += [
            "  <url>",
            f"    <loc>{loc}</loc>",
            f"    <lastmod>{lastmod}</lastmod>",
            f"    <changefreq>{changefreq}</changefreq>",
            f"    <priority>{priority:.1f}</priority>",
            f"    <ai:priority>{ai_priority}</ai:priority>",
            f"    <ai:contentType>{ai_content_type}</ai:contentType>",
            "  </url>",
        ]

    lines.append("</urlset>")
    return "\n".join(lines)


def get_full_setup_checklist(site_url: str) -> dict:
    """Return a checklist of all AI search readiness items with pass/fail status.

    Checks: llms.txt, robots.txt AI access, sitemap, sitemap-ai.xml,
    schema markup, HTTPS, canonical tags, Open Graph, author markup.
    """
    import requests as _req
    import datetime as _dt

    parsed = urlparse(site_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    def _fetch(path: str, timeout: int = 8):
        try:
            return _req.get(
                f"{base}{path}", timeout=timeout,
                headers={"User-Agent": "SEOEngine/1.0"},
                allow_redirects=True,
            )
        except Exception:
            return None

    # Fetch key resources
    robots_resp = _fetch("/robots.txt")
    llms_resp = _fetch("/llms.txt")
    llms_full_resp = _fetch("/llms-full.txt")
    sitemap_resp = _fetch("/sitemap.xml")
    sitemap_ai_resp = _fetch("/sitemap-ai.xml")
    homepage_resp = _fetch("/")

    homepage_text = (
        homepage_resp.text if homepage_resp and homepage_resp.status_code == 200 else ""
    )

    # AI crawler access
    crawler_status = validate_ai_crawler_access(site_url)
    key_crawlers = ("gptbot", "claudebot", "perplexitybot", "bingbot", "anthropic_ai")
    all_allowed = all(crawler_status.get(k, "unknown") == "allowed" for k in key_crawlers)

    # Schema markup
    has_schema = "application/ld+json" in homepage_text or "schema.org" in homepage_text
    has_local_biz_schema = (
        '"LocalBusiness"' in homepage_text or '"Service"' in homepage_text
    )

    # Other signals
    has_https = base.startswith("https://")
    has_canonical = 'rel="canonical"' in homepage_text or "rel='canonical'" in homepage_text
    has_og = 'property="og:' in homepage_text or "property='og:" in homepage_text
    has_author = "author" in homepage_text.lower() and "Person" in homepage_text

    def _ok(resp) -> bool:
        return resp is not None and resp.status_code == 200

    checklist = {
        "site_url": site_url,
        "checked_at": _dt.datetime.utcnow().isoformat(),
        "items": {
            "llms_txt": {
                "label": "llms.txt file present",
                "status": "pass" if _ok(llms_resp) else "fail",
                "url": f"{base}/llms.txt",
                "importance": "critical",
            },
            "llms_full_txt": {
                "label": "llms-full.txt file present",
                "status": "pass" if _ok(llms_full_resp) else "fail",
                "url": f"{base}/llms-full.txt",
                "importance": "recommended",
            },
            "robots_txt_exists": {
                "label": "robots.txt accessible",
                "status": "pass" if _ok(robots_resp) else "fail",
                "url": f"{base}/robots.txt",
                "importance": "critical",
            },
            "ai_crawlers_allowed": {
                "label": "All major AI crawlers explicitly allowed in robots.txt",
                "status": "pass" if all_allowed else "warn",
                "detail": {k: crawler_status.get(k, "unknown") for k in key_crawlers},
                "importance": "critical",
            },
            "sitemap_xml": {
                "label": "sitemap.xml present",
                "status": "pass" if _ok(sitemap_resp) else "fail",
                "url": f"{base}/sitemap.xml",
                "importance": "critical",
            },
            "sitemap_ai_xml": {
                "label": "sitemap-ai.xml present (AI-focused sitemap)",
                "status": "pass" if _ok(sitemap_ai_resp) else "fail",
                "url": f"{base}/sitemap-ai.xml",
                "importance": "recommended",
            },
            "https": {
                "label": "Site uses HTTPS",
                "status": "pass" if has_https else "fail",
                "importance": "critical",
            },
            "schema_markup": {
                "label": "JSON-LD schema markup on homepage",
                "status": "pass" if has_schema else "fail",
                "importance": "critical",
            },
            "local_business_schema": {
                "label": "LocalBusiness or Service schema present",
                "status": "pass" if has_local_biz_schema else "fail",
                "importance": "high",
            },
            "canonical_tags": {
                "label": "Canonical tags present",
                "status": "pass" if has_canonical else "warn",
                "importance": "recommended",
            },
            "open_graph": {
                "label": "Open Graph meta tags present",
                "status": "pass" if has_og else "warn",
                "importance": "recommended",
            },
            "author_markup": {
                "label": "Author / Person schema on content pages",
                "status": "pass" if has_author else "fail",
                "importance": "high",
            },
        },
    }

    # Compute summary
    items = list(checklist["items"].values())
    passed = sum(1 for i in items if i["status"] == "pass")
    failed = sum(1 for i in items if i["status"] == "fail")
    warned = sum(1 for i in items if i["status"] == "warn")
    total = len(items)

    score = int((passed / total) * 100)
    if score >= 90:
        grade = "A"
    elif score >= 75:
        grade = "B"
    elif score >= 60:
        grade = "C"
    elif score >= 40:
        grade = "D"
    else:
        grade = "F"

    checklist["summary"] = {
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "total": total,
        "score": score,
        "grade": grade,
    }

    log.info(
        "llms_txt.checklist  url=%s  score=%d  grade=%s  passed=%d/%d",
        site_url, score, grade, passed, total,
    )
    return checklist
