"""9-step onboarding pipeline with SQLite job tracking."""
from __future__ import annotations
import json
import sqlite3
import uuid
import logging
from datetime import datetime
from typing import Optional

DB_PATH = "data/storage/seo_engine.db"
log = logging.getLogger(__name__)


def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS onboarding_jobs (
            id TEXT PRIMARY KEY,
            business_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            current_step INTEGER NOT NULL DEFAULT 0,
            steps_completed TEXT NOT NULL DEFAULT '[]',
            steps_total INTEGER NOT NULL DEFAULT 9,
            result_json TEXT NOT NULL DEFAULT '{}',
            error TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


def _update_job(conn, job_id: str, **kwargs):
    kwargs["updated_at"] = datetime.utcnow().isoformat()
    sets = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [job_id]
    conn.execute(f"UPDATE onboarding_jobs SET {sets} WHERE id=?", vals)
    conn.commit()


def start_onboarding(profile: dict) -> str:
    """Kick off onboarding in the current thread (call from Celery task)."""
    job_id = str(uuid.uuid4())
    business_id = profile.get("business_id", job_id[:8])

    conn = _db()
    conn.execute("""
        INSERT INTO onboarding_jobs (id, business_id, status, current_step, steps_completed, steps_total)
        VALUES (?, ?, 'running', 0, '[]', 9)
    """, [job_id, business_id])
    conn.commit()

    completed = []
    result = {}

    steps = [
        ("generate_local_facts", _step_facts),
        ("author_profile", _step_author),
        ("tracking_number", _step_tracking),
        ("cta_variants", _step_cta),
        ("entity_sweep", _step_entity),
        ("citation_content", _step_citation),
        ("parasite_sweep", _step_parasite),
        ("eeat_pipeline", _step_eeat),
        ("backlink_prospects", _step_backlinks),
    ]

    for i, (name, fn) in enumerate(steps):
        _update_job(conn, job_id, current_step=i + 1, status="running")
        try:
            out = fn(profile)
            completed.append({"step": name, "status": "ok", "data": out})
            result[name] = out
            log.info("onboarding.step.ok  job=%s  step=%s", job_id, name)
        except Exception as exc:
            log.warning("onboarding.step.fail  job=%s  step=%s  err=%s", job_id, name, exc)
            completed.append({"step": name, "status": "error", "error": str(exc)})
            result[name] = {"error": str(exc)}

        _update_job(conn, job_id, steps_completed=json.dumps(completed))

    _update_job(conn, job_id, status="done", result_json=json.dumps(result))
    conn.close()
    return job_id


def get_job_status(job_id: str) -> Optional[dict]:
    conn = _db()
    row = conn.execute("SELECT * FROM onboarding_jobs WHERE id=?", [job_id]).fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    d["steps_completed"] = json.loads(d["steps_completed"] or "[]")
    d["result_json"] = json.loads(d["result_json"] or "{}")
    return d


def list_jobs(business_id: str) -> list:
    conn = _db()
    rows = conn.execute(
        "SELECT * FROM onboarding_jobs WHERE business_id=? ORDER BY created_at DESC LIMIT 20",
        [business_id]
    ).fetchall()
    conn.close()
    result = []
    for row in rows:
        d = dict(row)
        d["steps_completed"] = json.loads(d["steps_completed"] or "[]")
        d["result_json"] = json.loads(d["result_json"] or "{}")
        result.append(d)
    return result


# ── Step implementations ────────────────────────────────────────────────────

def _step_facts(profile: dict) -> dict:
    from core.citable_data import generate_local_facts
    facts = generate_local_facts(profile["business_id"])
    return {"facts_generated": len(facts)}


def _step_author(profile: dict) -> dict:
    try:
        from core.author_profiles import upsert_author
        author = upsert_author(
            business_id=profile["business_id"],
            name=profile.get("owner_name", ""),
            role=profile.get("role", "Owner"),
            bio=profile.get("bio", ""),
            location=profile.get("location", ""),
        )
        return {"author_id": author.get("id", "")}
    except Exception:
        from core.author_profiles import get_default_author
        author = get_default_author(profile["business_id"])
        return {"author_id": author.get("id", "") if author else ""}


def _step_tracking(profile: dict) -> dict:
    from core.call_tracking import add_tracking_number
    phone = profile.get("phone", "")
    if not phone:
        return {"skipped": True}
    number = add_tracking_number(
        business_id=profile["business_id"],
        source="organic",
        display_number=phone,
        forward_to=phone,
    )
    return {"tracking_id": number.get("id", "")}


def _step_cta(profile: dict) -> dict:
    from core.cta_optimizer import generate_cta_variants
    services = profile.get("services", [])
    service = services[0] if services else "service"
    variants = generate_cta_variants(
        business_id=profile["business_id"],
        page_id="onboarding",
        service=service,
        location=profile.get("location", ""),
        intent="high_urgency",
        business_name=profile.get("business_name", ""),
        phone=profile.get("phone", ""),
    )
    return {"variants": len(variants) if isinstance(variants, list) else 0}


def _step_entity(profile: dict) -> dict:
    from core.brand_entity import run_entity_sweep
    results = run_entity_sweep(profile["business_id"])
    return {"mentions": len(results) if isinstance(results, list) else 0}


def _step_citation(profile: dict) -> dict:
    from core.citation_content import run_citation_content_sweep
    pages = run_citation_content_sweep(profile["business_id"])
    return {"citation_pages": len(pages) if isinstance(pages, list) else 0}


def _step_parasite(profile: dict) -> dict:
    from core.parasite_seo import run_parasite_sweep
    pages = run_parasite_sweep(profile["business_id"])
    published = sum(1 for p in pages if p.get("status") == "published")
    return {"parasite_pages": len(pages), "published": published}


def _step_eeat(profile: dict) -> dict:
    html = profile.get("homepage_html", "")
    if not html:
        return {"skipped": True}
    from core.eeat_pipeline import run_eeat_pipeline
    result = run_eeat_pipeline(html=html)
    return {"eeat_score": result.get("score", 0) if isinstance(result, dict) else 0}


def _step_backlinks(profile: dict) -> dict:
    from core.backlink_prospector import find_local_citation_opportunities
    prospects = find_local_citation_opportunities(
        business_id=profile["business_id"],
        niche=profile.get("niche", "home services"),
        location=profile.get("location", ""),
    )
    return {"prospects": len(prospects) if isinstance(prospects, list) else 0}
