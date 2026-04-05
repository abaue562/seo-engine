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
