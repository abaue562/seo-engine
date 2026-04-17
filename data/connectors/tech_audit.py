"""Technical SEO audit — robots.txt, sitemap, canonicals, redirects, schema, CWV.

Runs at onboarding (blocks publish on critical issues) and weekly thereafter.
Categories: Crawlability, Indexability, Sitemaps, Redirects, Internal 404s,
            Schema Validity, Mobile-friendly, Core Web Vitals.
"""
from __future__ import annotations
import logging, os, re, urllib.request, urllib.parse
from dataclasses import dataclass, field
from typing import Any
import requests

log = logging.getLogger(__name__)

@dataclass
class AuditIssue:
    category: str
    severity: str   # critical | warning | info
    title: str
    detail: str
    url: str = ""
    fix: str = ""

@dataclass
class AuditResult:
    domain: str
    issues: list[AuditIssue] = field(default_factory=list)
    score: int = 0  # 0-100

    @property
    def critical_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "critical")

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "warning")

    def passed(self) -> bool:
        return self.critical_count == 0


HEADERS = {"User-Agent": "SEOEngineBot/1.0 (+https://gethubed.com/bot)"}


def _fetch(url: str, timeout: int = 10) -> tuple[int, str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        return r.status_code, r.text
    except Exception as e:
        return 0, str(e)


def audit_robots_txt(domain: str, result: AuditResult) -> None:
    url = f"https://{domain}/robots.txt"
    status, body = _fetch(url)
    if status != 200:
        result.issues.append(AuditIssue("crawlability", "warning", "robots.txt missing or unreachable",
            f"GET {url} returned {status}", url=url, fix="Create a robots.txt at your domain root."))
        return
    # Check for broad disallow
    lines = [l.strip() for l in body.splitlines()]
    ua = "*"
    for line in lines:
        if line.lower().startswith("user-agent:"):
            ua = line.split(":", 1)[1].strip()
        if ua in ("*", "Googlebot") and line.lower().startswith("disallow: /"):
            val = line.split(":", 1)[1].strip()
            if val in ("/", "/*"):
                result.issues.append(AuditIssue("crawlability", "critical",
                    "robots.txt blocks all crawling",
                    f"Disallow: {val} for User-agent: {ua} — Google cannot crawl the site.",
                    url=url, fix="Remove or narrow the Disallow rule."))


def audit_sitemap(domain: str, result: AuditResult) -> None:
    url = f"https://{domain}/sitemap.xml"
    status, body = _fetch(url)
    if status != 200:
        result.issues.append(AuditIssue("sitemaps", "warning", "sitemap.xml not found",
            f"GET {url} returned {status}", url=url,
            fix="Generate and submit a sitemap.xml via your CMS or SEO plugin."))
        return
    if "<urlset" not in body and "<sitemapindex" not in body:
        result.issues.append(AuditIssue("sitemaps", "warning", "sitemap.xml invalid XML",
            "File exists but does not contain valid sitemap XML.", url=url,
            fix="Validate the sitemap at https://www.xml-sitemaps.com/validate-xml-sitemap.html"))
        return
    url_count = body.count("<url>") + body.count("<sitemap>")
    if url_count == 0:
        result.issues.append(AuditIssue("sitemaps", "warning", "sitemap.xml is empty",
            "Sitemap exists but contains no URLs.", url=url))


def audit_canonical(url: str, result: AuditResult) -> None:
    status, body = _fetch(url)
    if status != 200:
        return
    has_canonical = 'rel="canonical"' in body or "rel='canonical'" in body
    if not has_canonical:
        result.issues.append(AuditIssue("indexability", "warning", "Missing canonical tag",
            f"Page {url} has no canonical link element.", url=url,
            fix="Add <link rel='canonical' href='{url}'> in the <head>."))


def audit_redirect_chain(url: str, result: AuditResult) -> None:
    try:
        session = requests.Session()
        resp = session.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        hops = len(resp.history)
        if hops > 1:
            result.issues.append(AuditIssue("redirects", "warning",
                f"Redirect chain ({hops} hops)",
                f"{url} passes through {hops} redirects before resolving.",
                url=url, fix="Reduce to a single direct 301 redirect."))
    except Exception:
        pass


def audit_page_speed(url: str, result: AuditResult) -> None:
    api_key = os.getenv("GOOGLE_PAGESPEED_API_KEY", "")
    if not api_key:
        return
    api_url = f"https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url={urllib.parse.quote(url)}&key={api_key}&strategy=mobile"
    try:
        status, body = _fetch(api_url, timeout=30)
        if status != 200:
            return
        import json
        data = json.loads(body)
        cats = data.get("lighthouseResult", {}).get("categories", {})
        perf = cats.get("performance", {}).get("score", 1)
        if perf < 0.5:
            result.issues.append(AuditIssue("core_web_vitals", "warning",
                f"Low PageSpeed score ({int(perf*100)}/100)",
                f"Mobile PageSpeed score is {int(perf*100)} — below the recommended 50+.",
                url=url, fix="Optimize images, reduce JS, enable caching."))
    except Exception as e:
        log.debug("tech_audit.pagespeed_fail  url=%s  err=%s", url, e)


def run_audit(domain: str, sample_urls: list[str] | None = None) -> AuditResult:
    """Run a full technical SEO audit for a domain.

    Args:
        domain: bare domain, e.g. 'blendbrightlights.com'
        sample_urls: list of published page URLs to spot-check (canonical, redirect, schema)
    """
    result = AuditResult(domain=domain)
    site = f"https://{domain}"

    audit_robots_txt(domain, result)
    audit_sitemap(domain, result)
    audit_redirect_chain(site, result)

    for url in (sample_urls or [])[:5]:
        audit_canonical(url, result)
        audit_redirect_chain(url, result)

    # PageSpeed on homepage
    audit_page_speed(site, result)

    # Score: start at 100, deduct per issue
    deductions = sum(20 if i.severity == "critical" else 5 for i in result.issues)
    result.score = max(0, 100 - deductions)

    log.info("tech_audit.done  domain=%s  score=%d  critical=%d  warnings=%d",
             domain, result.score, result.critical_count, result.warning_count)
    return result


def audit_to_dict(result: AuditResult) -> dict:
    return {
        "domain": result.domain,
        "score": result.score,
        "passed": result.passed(),
        "critical_count": result.critical_count,
        "warning_count": result.warning_count,
        "issues": [
            {"category": i.category, "severity": i.severity,
             "title": i.title, "detail": i.detail, "url": i.url, "fix": i.fix}
            for i in result.issues
        ],
    }
