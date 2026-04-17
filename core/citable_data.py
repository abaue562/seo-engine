"""
Citable data manager: maintains a tenant-specific database of verifiable,
specific facts and statistics that AI engines can cite. Auto-generates
local market data using SERP signals, GSC data, and Claude synthesis.
"""
from __future__ import annotations
import hashlib
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)
_DB = "data/storage/seo_engine.db"


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS citable_facts (
            id              TEXT PRIMARY KEY,
            business_id     TEXT NOT NULL,
            category        TEXT NOT NULL,
            claim           TEXT NOT NULL,
            value           TEXT NOT NULL,
            unit            TEXT DEFAULT '',
            source          TEXT DEFAULT 'market_analysis',
            methodology     TEXT DEFAULT '',
            confidence      TEXT DEFAULT 'estimated',
            verified        INTEGER DEFAULT 0,
            keywords        TEXT DEFAULT '[]',
            cited_count     INTEGER DEFAULT 0,
            created_at      TEXT,
            updated_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_cf_biz ON citable_facts(business_id, category);

        CREATE TABLE IF NOT EXISTS citation_pages (
            id              TEXT PRIMARY KEY,
            business_id     TEXT NOT NULL,
            page_type       TEXT NOT NULL,
            title           TEXT NOT NULL,
            slug            TEXT NOT NULL,
            html_content    TEXT,
            schema_json     TEXT,
            published_url   TEXT,
            citation_score  INTEGER DEFAULT 0,
            ai_cited_count  INTEGER DEFAULT 0,
            created_at      TEXT,
            updated_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_cp_biz ON citation_pages(business_id);
    """)
    c.commit()
    return c


def _fact_id(business_id: str, claim: str) -> str:
    return hashlib.md5(f"{business_id}:{claim}".encode()).hexdigest()[:12]


def add_fact(
    business_id: str,
    category: str,
    claim: str,
    value: str,
    unit: str = "",
    source: str = "market_analysis",
    methodology: str = "",
    confidence: str = "estimated",
    keywords: list[str] | None = None,
) -> dict:
    fid = _fact_id(business_id, claim)
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("""
            INSERT INTO citable_facts
                (id,business_id,category,claim,value,unit,source,methodology,confidence,keywords,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                value=excluded.value, unit=excluded.unit, source=excluded.source,
                methodology=excluded.methodology, confidence=excluded.confidence,
                keywords=excluded.keywords, updated_at=excluded.updated_at
        """, [fid, business_id, category, claim, value, unit, source,
              methodology, confidence, json.dumps(keywords or []), now, now])
    return {"id": fid, "claim": claim, "value": value, "unit": unit}


def get_facts(business_id: str, category: str = "", limit: int = 50) -> list[dict]:
    with _conn() as c:
        if category:
            rows = c.execute(
                "SELECT * FROM citable_facts WHERE business_id=? AND category=? ORDER BY cited_count DESC LIMIT ?",
                [business_id, category, limit]
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT * FROM citable_facts WHERE business_id=? ORDER BY cited_count DESC LIMIT ?",
                [business_id, limit]
            ).fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d["keywords"] = json.loads(d.get("keywords") or "[]")
        result.append(d)
    return result


def record_citation(fact_id: str) -> None:
    with _conn() as c:
        c.execute("UPDATE citable_facts SET cited_count=cited_count+1 WHERE id=?", [fact_id])


def generate_local_facts(business_id: str) -> list[dict]:
    """
    Use Claude + SERP data to synthesize local market facts for the tenant.
    Generates pricing ranges, timelines, local statistics.
    """
    try:
        all_biz = json.loads(open("data/storage/businesses.json").read())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
        biz = next((b for b in biz_list
                    if b.get("id") == business_id or b.get("business_id") == business_id), {})
    except Exception:
        biz = {}

    business_name = biz.get("business_name", biz.get("name", "this business"))
    location = biz.get("city", biz.get("location", ""))
    services = biz.get("services", biz.get("service_types", []))
    niche = biz.get("niche", biz.get("service_type", "home services"))

    if not location or not services:
        log.warning("generate_local_facts: missing location or services  biz=%s", business_id)
        return []

    # Build SERP context for pricing signals
    serp_context = ""
    try:
        from core.serp_scraper import scrape_serp
        for svc in services[:2]:
            serp = scrape_serp(f"{svc} cost {location}", location=location)
            snippets = [r.get("snippet", "") for r in serp.get("organic", [])[:5] if r.get("snippet")]
            serp_context += f"\n{svc} SERP snippets: " + " | ".join(snippets[:3])
    except Exception:
        pass

    prompt = f"""You are a local market research analyst. Generate specific, verifiable facts and statistics
for {business_name}, a {niche} business in {location}.

Services: {', '.join(str(s) for s in services[:5])}
SERP Context (real search snippets for pricing): {serp_context or 'not available'}

Generate 15-20 specific, citable facts in this JSON format:
[
  {{
    "category": "pricing",
    "claim": "Average cost of [service] in {location}",
    "value": "$X-$Y",
    "unit": "per job",
    "methodology": "Based on analysis of local contractor quotes and regional cost indices",
    "confidence": "estimated"
  }},
  ...
]

Categories to cover: pricing, timeline, benefits, local_context, industry_stats
Rules:
- Every fact must have a specific number or range
- Local context facts should reference {location} specifically
- Industry stats should reference Canada or BC when possible
- Pricing must be realistic ranges (not single points)
- Include facts about ROI, savings, or before/after comparisons
- JSON array only, no explanation"""

    facts = []
    try:
        from core.claude import call_claude
        raw = call_claude(prompt, max_tokens=1500)
        m = re.search(r'\[[\s\S]*\]', raw)
        if m:
            items = json.loads(m.group())
            for item in items:
                if not isinstance(item, dict) or not item.get("claim") or not item.get("value"):
                    continue
                f = add_fact(
                    business_id=business_id,
                    category=item.get("category", "general"),
                    claim=item["claim"],
                    value=item["value"],
                    unit=item.get("unit", ""),
                    source="claude_market_synthesis",
                    methodology=item.get("methodology", ""),
                    confidence=item.get("confidence", "estimated"),
                    keywords=[niche, location] + [str(s) for s in services[:2]],
                )
                facts.append(f)
    except Exception:
        log.exception("generate_local_facts: Claude synthesis failed  biz=%s", business_id)

    log.info("generate_local_facts  biz=%s  facts=%d", business_id, len(facts))
    return facts
