import json
import logging
import os
import sqlite3
from typing import List, Dict, Optional
import redis
import requests

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
DB_PATH = "data/storage/seo_engine.db"

def _get_top_pages(business_id: str, limit: int = 50) -> List[Dict]:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT p.url, p.title, p.keyword, r.position
        FROM published_urls p
        LEFT JOIN ranking_history r ON p.url = r.url AND p.business_id = r.business_id
        WHERE p.business_id = ? AND p.status = 'live'
        GROUP BY p.url
        ORDER BY COALESCE(r.position, 999) ASC
        LIMIT ?
    """, [business_id, limit]).fetchall()
    conn.close()
    return [{"url": r[0], "title": r[1] or r[2] or r[0], "keyword": r[2], "position": r[3]} for r in rows]

def _get_business_info(business_id: str) -> Dict:
    try:
        import json as j
        from pathlib import Path
        all_biz = j.loads(Path("data/storage/businesses.json").read_text())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
        for biz in biz_list:
            if biz.get("id") == business_id or biz.get("business_id") == business_id:
                return biz
    except Exception:
        pass
    return {"name": business_id, "description": "Local service business"}

def build_llms_txt(business_id: str) -> str:
    cache_key = f"llms_txt:{business_id}"
    cached = _redis.get(cache_key)
    if cached:
        return cached

    biz = _get_business_info(business_id)
    name = biz.get("name", business_id)
    description = biz.get("description", f"{name} provides local services.")
    city = biz.get("city", "")
    services = biz.get("services", [])
    domain = biz.get("domain", biz.get("wp_site_url", "")).rstrip("/")

    pages = _get_top_pages(business_id, limit=50)

    lines = [
        f"# {name}",
        "",
        f"> {description}{f' Based in {city}.' if city else ''}",
        "",
    ]

    if services:
        lines += ["## Services", ""]
        for s in services[:10]:
            lines.append(f"- {s}")
        lines.append("")

    if pages:
        lines += ["## Pages", ""]
        for p in pages[:30]:
            url = p["url"]
            title = p["title"] or p["keyword"] or url
            kw = p["keyword"] or ""
            pos = p["position"]
            pos_note = f" (currently ranking #{int(pos)})" if pos and pos <= 20 else ""
            desc = f"Covers {kw}{pos_note}." if kw else f"Service page."
            lines.append(f"- [{title}]({url}): {desc}")
        lines.append("")

    lines += [
        "## Notes",
        "",
        "- All content is original and specific to this business",
        "- Pages include structured data (JSON-LD) for LocalBusiness, FAQ, and HowTo schemas",
        f"- Content is regularly updated. Last generated: {__import__('datetime').date.today().isoformat()}",
        "",
    ]

    content = "\n".join(lines)
    _redis.setex(cache_key, 86400, content)
    log.info("llms_txt_builder.built  biz=%s  pages=%d", business_id, len(pages))
    return content

def deploy_llms_txt(business_id: str, output_path: Optional[str] = None) -> bool:
    content = build_llms_txt(business_id)
    if output_path:
        try:
            with open(output_path, "w") as f:
                f.write(content)
            log.info("llms_txt_builder.deployed  path=%s", output_path)
            return True
        except Exception as exc:
            log.exception("llms_txt_builder.deploy_error")
            return False

    # Try WP REST API deployment
    biz = _get_business_info(business_id)
    wp_url = biz.get("wp_site_url", "").rstrip("/")
    wp_user = biz.get("wp_username", "")
    wp_pass = biz.get("wp_app_password", "")
    if not all([wp_url, wp_user, wp_pass]):
        log.warning("llms_txt_builder: no WP credentials for %s", business_id)
        return False

    try:
        import base64
        token = base64.b64encode(f"{wp_user}:{wp_pass}".encode()).decode()
        # Create as a custom page slug llms-txt
        resp = requests.post(f"{wp_url}/wp-json/wp/v2/pages",
            headers={"Authorization": f"Basic {token}", "Content-Type": "application/json"},
            json={"title": "llms.txt", "slug": "llms-txt", "content": f"<pre>{content}</pre>", "status": "publish"},
            timeout=15)
        ok = resp.status_code in (200, 201)
        log.info("llms_txt_builder.wp_deploy  biz=%s  status=%d", business_id, resp.status_code)
        return ok
    except Exception as exc:
        log.exception("llms_txt_builder.wp_deploy_error")
        return False

def generate_platform_llms_txt() -> str:
    lines = [
        "# GetHubed — AI-Powered SEO Automation Platform",
        "",
        "> GetHubed is a multi-tenant SEO automation SaaS that generates, publishes, and optimizes content at scale using AI. It combines programmatic content generation, real-time SERP monitoring, and cross-tenant machine learning to compound SEO results over time.",
        "",
        "## What This Platform Does",
        "",
        "- Generates high-quality, schema-rich SEO content using a 4-pass LLM pipeline with LLM-as-judge quality validation",
        "- Publishes directly to WordPress with full schema markup (LocalBusiness, FAQ, HowTo, Speakable)",
        "- Tracks rankings via Google Search Console and DataForSEO, closing the loop from content to outcome",
        "- Runs 47+ automated tasks (content generation, indexing, competitor tracking, health monitoring)",
        "- Uses a cross-tenant signal layer with k-anonymity to share what works across similar businesses",
        "",
        "## Key Concepts",
        "",
        "- **Topical authority**: Content is organized into keyword clusters that build entity-level authority",
        "- **GEO optimization**: Every page is formatted for AI engine citation (direct answers, Speakable schema, entity chains)",
        "- **Closed-loop learning**: Content outcomes feed back into brief generation via Bayesian pattern updates",
        "",
        f"## Contact",
        "",
        "- Platform: GetHubed",
        f"- Generated: {__import__('datetime').date.today().isoformat()}",
        "",
    ]
    return "\n".join(lines)
