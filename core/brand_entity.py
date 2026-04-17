"""
Brand Entity Manager: builds and publishes structured entity data for Google's
Knowledge Graph. Generates Organization/LocalBusiness schema, manages sameAs
links, pushes entities to AION Knowledge graph, and monitors entity strength.
"""
from __future__ import annotations
import json
import sqlite3
import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)
_DB = "data/storage/seo_engine.db"


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS brand_entities (
            id              TEXT PRIMARY KEY,
            business_id     TEXT NOT NULL UNIQUE,
            entity_name     TEXT NOT NULL,
            entity_type     TEXT DEFAULT 'LocalBusiness',
            description     TEXT,
            founding_year   INTEGER,
            location        TEXT,
            address         TEXT,
            phone           TEXT,
            email           TEXT,
            website         TEXT,
            logo_url        TEXT,
            same_as         TEXT DEFAULT '[]',
            schema_json     TEXT,
            kg_node_id      TEXT,
            wikidata_qid    TEXT,
            entity_score    INTEGER DEFAULT 0,
            last_published  TEXT,
            updated_at      TEXT
        );
        CREATE TABLE IF NOT EXISTS entity_same_as (
            id          TEXT PRIMARY KEY,
            business_id TEXT NOT NULL,
            platform    TEXT NOT NULL,
            url         TEXT NOT NULL,
            verified    INTEGER DEFAULT 0,
            added_at    TEXT,
            UNIQUE(business_id, platform)
        );
        CREATE TABLE IF NOT EXISTS entity_mentions (
            id          TEXT PRIMARY KEY,
            business_id TEXT NOT NULL,
            source_url  TEXT NOT NULL,
            source_domain TEXT,
            mention_text TEXT,
            has_link    INTEGER DEFAULT 0,
            discovered_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_be_biz ON brand_entities(business_id);
        CREATE INDEX IF NOT EXISTS idx_em_biz ON entity_mentions(business_id);
    """)
    c.commit()
    return c


# --- Core entity management ---

def upsert_brand_entity(
    business_id: str,
    entity_name: str,
    entity_type: str = "LocalBusiness",
    description: str = "",
    founding_year: int = 0,
    location: str = "",
    address: str = "",
    phone: str = "",
    email: str = "",
    website: str = "",
    logo_url: str = "",
) -> dict:
    eid = hashlib.md5(business_id.encode()).hexdigest()[:12]
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("""
            INSERT INTO brand_entities
                (id,business_id,entity_name,entity_type,description,founding_year,
                 location,address,phone,email,website,logo_url,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(business_id) DO UPDATE SET
                entity_name=excluded.entity_name, entity_type=excluded.entity_type,
                description=excluded.description, founding_year=excluded.founding_year,
                location=excluded.location, address=excluded.address,
                phone=excluded.phone, email=excluded.email,
                website=excluded.website, logo_url=excluded.logo_url,
                updated_at=excluded.updated_at
        """, [eid, business_id, entity_name, entity_type, description, founding_year,
              location, address, phone, email, website, logo_url, now])
    log.info("upsert_brand_entity  biz=%s  name=%s", business_id, entity_name)
    return get_brand_entity(business_id)


def get_brand_entity(business_id: str) -> dict:
    with _conn() as c:
        row = c.execute("SELECT * FROM brand_entities WHERE business_id=?", [business_id]).fetchone()
    if not row:
        return {}
    d = dict(row)
    d["same_as"] = json.loads(d.get("same_as") or "[]")
    return d


def add_same_as(business_id: str, platform: str, url: str, verified: bool = False) -> None:
    """Add a sameAs link (LinkedIn, Facebook, Yelp, Wikidata, etc.)."""
    sid = hashlib.md5(f"{business_id}:{platform}".encode()).hexdigest()[:12]
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("""
            INSERT INTO entity_same_as (id,business_id,platform,url,verified,added_at)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(business_id,platform) DO UPDATE SET url=excluded.url, verified=excluded.verified
        """, [sid, business_id, platform, url, int(verified), now])
    _sync_same_as_to_entity(business_id)


def _sync_same_as_to_entity(business_id: str) -> None:
    with _conn() as c:
        rows = c.execute(
            "SELECT url FROM entity_same_as WHERE business_id=?", [business_id]
        ).fetchall()
        urls = [r[0] for r in rows]
        c.execute("UPDATE brand_entities SET same_as=? WHERE business_id=?",
                  [json.dumps(urls), business_id])


def get_same_as(business_id: str) -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT platform, url, verified FROM entity_same_as WHERE business_id=? ORDER BY platform",
            [business_id]
        ).fetchall()
    return [dict(r) for r in rows]


# --- Schema generation ---

def build_organization_schema(business_id: str) -> dict:
    """Build schema.org/Organization or LocalBusiness JSON-LD for injection."""
    entity = get_brand_entity(business_id)
    if not entity:
        return {}
    same_as = entity.get("same_as", [])
    schema: dict = {
        "@context": "https://schema.org",
        "@type": entity.get("entity_type", "LocalBusiness"),
        "name": entity["entity_name"],
    }
    if entity.get("description"):
        schema["description"] = entity["description"]
    if entity.get("website"):
        schema["url"] = entity["website"]
    if entity.get("phone"):
        schema["telephone"] = entity["phone"]
    if entity.get("email"):
        schema["email"] = entity["email"]
    if entity.get("logo_url"):
        schema["logo"] = {"@type": "ImageObject", "url": entity["logo_url"]}
    if entity.get("founding_year"):
        schema["foundingDate"] = str(entity["founding_year"])
    if entity.get("address"):
        schema["address"] = {
            "@type": "PostalAddress",
            "streetAddress": entity["address"],
            "addressLocality": entity.get("location", ""),
        }
    elif entity.get("location"):
        schema["address"] = {"@type": "PostalAddress", "addressLocality": entity["location"]}
    if same_as:
        schema["sameAs"] = same_as
    # Cache schema back
    with _conn() as c:
        c.execute("UPDATE brand_entities SET schema_json=? WHERE business_id=?",
                  [json.dumps(schema), business_id])
    return schema


def inject_organization_schema(html: str, business_id: str) -> str:
    """Inject Organization schema JSON-LD into HTML."""
    schema = build_organization_schema(business_id)
    if not schema:
        return html
    tag = f'<script type="application/ld+json">{json.dumps(schema, indent=2)}</script>'
    # Insert in <head> if present, else prepend
    if "</head>" in html:
        return html.replace("</head>", f"{tag}\n</head>", 1)
    return tag + "\n" + html


# --- AION Knowledge Graph integration ---

def publish_to_knowledge_graph(business_id: str) -> dict:
    """
    Push brand entity + relationships into AION Knowledge graph.
    Creates nodes for: brand, location, services, competitors.
    Adds edges: brand→location (operates_in), brand→service (offers), brand→competitor (competes_with).
    """
    entity = get_brand_entity(business_id)
    if not entity:
        return {"status": "error", "reason": "no_entity"}

    try:
        from core.aion_bridge import aion
    except Exception:
        return {"status": "error", "reason": "aion_unavailable"}

    nodes_added = []
    edges_added = []

    # Brand node
    brand_node = aion.knowledge_add_node(
        label=entity["entity_name"],
        node_type=entity.get("entity_type", "LocalBusiness"),
        properties={
            "business_id": business_id,
            "description": entity.get("description", ""),
            "website": entity.get("website", ""),
            "phone": entity.get("phone", ""),
            "location": entity.get("location", ""),
            "founding_year": str(entity.get("founding_year", "")),
            "same_as": json.dumps(entity.get("same_as", [])),
        }
    )
    brand_id = (brand_node or {}).get("id", "")
    if brand_id:
        nodes_added.append({"type": "brand", "id": brand_id, "name": entity["entity_name"]})
        # Save kg_node_id
        with _conn() as c:
            c.execute("UPDATE brand_entities SET kg_node_id=? WHERE business_id=?",
                      [brand_id, business_id])

    # Location node
    if entity.get("location") and brand_id:
        loc_node = aion.knowledge_add_node(
            label=entity["location"],
            node_type="Place",
            properties={"address": entity.get("address", "")}
        )
        loc_id = (loc_node or {}).get("id", "")
        if loc_id:
            nodes_added.append({"type": "location", "id": loc_id})
            aion.knowledge_add_edge(brand_id, loc_id, "operates_in", weight=1.0)
            edges_added.append("operates_in")

    # Services from businesses.json
    try:
        from pathlib import Path
        all_biz = json.loads(Path("data/storage/businesses.json").read_text())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
        biz = next((b for b in biz_list
                    if b.get("id") == business_id or b.get("business_id") == business_id), {})
        services = biz.get("services", biz.get("service_types", []))
        competitors = biz.get("competitors", [])
    except Exception:
        services, competitors = [], []

    if brand_id:
        for svc in services[:5]:
            svc_node = aion.knowledge_add_node(label=str(svc), node_type="Service")
            svc_id = (svc_node or {}).get("id", "")
            if svc_id:
                aion.knowledge_add_edge(brand_id, svc_id, "offers", weight=1.0)
                edges_added.append(f"offers:{svc}")

        for comp in competitors[:5]:
            import re as _re
            comp_name = _re.sub(r'https?://|www\.', '', str(comp)).rstrip("/")
            comp_node = aion.knowledge_add_node(label=comp_name, node_type="Competitor")
            comp_id = (comp_node or {}).get("id", "")
            if comp_id:
                aion.knowledge_add_edge(brand_id, comp_id, "competes_with", weight=0.8)
                edges_added.append(f"competes_with:{comp_name}")

    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("UPDATE brand_entities SET last_published=? WHERE business_id=?", [now, business_id])

    log.info("publish_to_knowledge_graph  biz=%s  nodes=%d  edges=%d",
             business_id, len(nodes_added), len(edges_added))
    return {"status": "ok", "nodes_added": len(nodes_added), "edges_added": len(edges_added)}


# --- Entity strength scoring ---

def score_entity_strength(business_id: str) -> dict:
    """
    Score brand entity strength 0–100.
    Signals: schema present, sameAs count, KG published, description, mentions, Wikidata.
    """
    entity = get_brand_entity(business_id)
    if not entity:
        return {"total": 0, "passing": False, "scores": {}, "missing": ["no entity registered"]}

    scores: dict[str, int] = {}
    missing: list[str] = []
    same_as = entity.get("same_as", [])

    # Organization schema built (20 pts)
    scores["schema"] = 20 if entity.get("schema_json") else 0
    if not scores["schema"]:
        missing.append("Organization schema not yet built")

    # sameAs links — 5 pts each up to 20 pts
    same_as_pts = min(len(same_as) * 5, 20)
    scores["same_as"] = same_as_pts
    if same_as_pts < 20:
        missing.append(f"Add more sameAs links ({len(same_as)}/4+ platforms)")

    # Published to KG (15 pts)
    scores["kg_published"] = 15 if entity.get("kg_node_id") else 0
    if not scores["kg_published"]:
        missing.append("Not yet published to AION Knowledge Graph")

    # Description present (10 pts)
    scores["description"] = 10 if len(entity.get("description", "")) > 50 else 0
    if not scores["description"]:
        missing.append("Entity description missing or too short")

    # NAP complete (15 pts — Name, Address, Phone)
    nap_filled = sum(1 for f in ["entity_name", "address", "phone"] if entity.get(f))
    scores["nap"] = nap_filled * 5
    if scores["nap"] < 15:
        missing.append("NAP incomplete (name/address/phone)")

    # Mentions found (10 pts)
    with _conn() as c:
        mention_count = c.execute(
            "SELECT COUNT(*) FROM entity_mentions WHERE business_id=?", [business_id]
        ).fetchone()[0]
    scores["mentions"] = 10 if mention_count >= 5 else (5 if mention_count >= 1 else 0)
    if mention_count < 5:
        missing.append(f"Only {mention_count} entity mentions found (need 5+)")

    # Wikidata QID (10 pts)
    scores["wikidata"] = 10 if entity.get("wikidata_qid") else 0
    if not scores["wikidata"]:
        missing.append("No Wikidata entity (QID) linked")

    total = sum(scores.values())
    # Persist score
    with _conn() as c:
        c.execute("UPDATE brand_entities SET entity_score=? WHERE business_id=?", [total, business_id])

    return {"total": total, "passing": total >= 60, "scores": scores, "missing": missing}


# --- Entity mention monitoring ---

def find_entity_mentions(business_id: str, brand_name: str) -> list[dict]:
    """Use GPT-Researcher + Firecrawl to find web mentions of the brand."""
    try:
        from core.aion_bridge import aion
        import re, hashlib as _h
    except Exception:
        log.warning("find_entity_mentions: aion unavailable")
        return []

    mentions = []
    try:
        report = aion.gpt_research(f'"{brand_name}" reviews mentions site OR blog')
        if not report:
            return []
        urls = list(set(re.findall(r'https?://[^\s\)\]\'"<>]+', report)))[:15]
        now = datetime.now(timezone.utc).isoformat()
        import re as _re
        for url in urls:
            try:
                domain_m = _re.search(r'https?://([^/]+)', url)
                domain = domain_m.group(1) if domain_m else url
                md = aion.firecrawl_scrape(url)
                if not md or brand_name.lower() not in md.lower():
                    continue
                has_link = f"https://" in md and brand_name.lower() in md.lower()
                # Extract surrounding context
                idx = md.lower().find(brand_name.lower())
                context = md[max(0, idx-100):idx+200].strip() if idx >= 0 else ""
                mid = _h.md5(f"{business_id}:{url}".encode()).hexdigest()[:12]
                with _conn() as c:
                    c.execute("""
                        INSERT OR IGNORE INTO entity_mentions
                            (id,business_id,source_url,source_domain,mention_text,has_link,discovered_at)
                        VALUES (?,?,?,?,?,?,?)
                    """, [mid, business_id, url, domain, context[:500], int(has_link), now])
                mentions.append({"url": url, "domain": domain, "has_link": has_link})
            except Exception:
                pass
    except Exception:
        log.exception("find_entity_mentions  biz=%s", business_id)

    log.info("find_entity_mentions  biz=%s  found=%d", business_id, len(mentions))
    return mentions


def check_wikidata_presence(brand_name: str) -> Optional[str]:
    """Query Wikidata API to see if the brand has an entity. Returns QID or None."""
    import urllib.request, urllib.parse
    try:
        q = urllib.parse.quote_plus(brand_name)
        url = f"https://www.wikidata.org/w/api.php?action=wbsearchentities&search={q}&language=en&format=json&limit=3"
        req = urllib.request.Request(url, headers={"User-Agent": "SEOEngine/1.0"})
        data = json.loads(urllib.request.urlopen(req, timeout=10).read())
        for result in data.get("search", []):
            if brand_name.lower() in result.get("label", "").lower():
                return result.get("id")  # e.g. Q12345
    except Exception:
        log.exception("check_wikidata_presence  brand=%s", brand_name)
    return None


def run_entity_sweep(business_id: str) -> dict:
    """Full entity pipeline: build schema, push to KG, find mentions, score."""
    entity = get_brand_entity(business_id)
    if not entity:
        return {"status": "error", "reason": "no_entity_registered"}

    # Build/refresh schema
    schema = build_organization_schema(business_id)

    # Push to AION KG
    kg_result = publish_to_knowledge_graph(business_id)

    # Find mentions
    mentions = find_entity_mentions(business_id, entity["entity_name"])

    # Check Wikidata
    wikidata_qid = check_wikidata_presence(entity["entity_name"])
    if wikidata_qid:
        with _conn() as c:
            c.execute("UPDATE brand_entities SET wikidata_qid=? WHERE business_id=?",
                      [wikidata_qid, business_id])
        add_same_as(business_id, "wikidata", f"https://www.wikidata.org/wiki/{wikidata_qid}", verified=True)

    # Score
    score = score_entity_strength(business_id)

    log.info("run_entity_sweep  biz=%s  score=%d  mentions=%d  kg_nodes=%d",
             business_id, score["total"], len(mentions), kg_result.get("nodes_added", 0))
    return {
        "status": "ok",
        "entity_score": score["total"],
        "passing": score["passing"],
        "missing_signals": score["missing"],
        "kg_nodes_added": kg_result.get("nodes_added", 0),
        "mentions_found": len(mentions),
        "wikidata_qid": wikidata_qid,
    }
