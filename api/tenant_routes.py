"""Tenant-facing API routes (doc 05)."""
from __future__ import annotations
import logging, sqlite3, json, os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query

log = logging.getLogger(__name__)
router = APIRouter(prefix="/tenant", tags=["tenant"])

def _db():
    conn = sqlite3.connect(Path("data/storage/seo_engine.db"))
    conn.row_factory = sqlite3.Row
    return conn

def _now():
    return datetime.now(tz=timezone.utc)

def _week_ago():
    return (_now() - timedelta(days=7)).isoformat()

@router.get("/summary")
def weekly_summary(business_id: str = Query(...)):
    db = _db()
    week_ago = _week_ago()
    pub = [dict(r) for r in db.execute(
        "SELECT url, keyword, published_at FROM published_urls WHERE business_id=? AND published_at>=? ORDER BY published_at DESC",
        [business_id, week_ago]).fetchall()]
    ranks = db.execute(
        "SELECT keyword, position, recorded_at FROM ranking_history WHERE business_id=? AND recorded_at>=? ORDER BY keyword, recorded_at DESC",
        [business_id, week_ago]).fetchall()
    total_pub = db.execute("SELECT COUNT(*) as n FROM published_urls WHERE business_id=? AND status='live'", [business_id]).fetchone()
    idx_pending = db.execute("SELECT COUNT(*) as n FROM indexing_queue WHERE business_id=? AND status='pending'", [business_id]).fetchone()
    idx_failed  = db.execute("SELECT COUNT(*) as n FROM indexing_queue WHERE business_id=? AND status='failed'",  [business_id]).fetchone()
    idx_total   = db.execute("SELECT COUNT(*) as n FROM indexing_queue WHERE business_id=?", [business_id]).fetchone()
    db.close()
    kw_pos = {}
    for r in ranks:
        kw_pos.setdefault(r["keyword"], []).append(r["position"])
    rank_wins = []
    for kw, pos in kw_pos.items():
        if len(pos) >= 2 and pos[0] and pos[-1] and pos[0] < pos[-1] and pos[0] <= 10:
            rank_wins.append({"keyword": kw, "from": pos[-1], "to": pos[0], "delta": pos[-1]-pos[0]})
    wins = [f"Published: {p['keyword']}" for p in pub[:3]] + [f"{r['keyword']} moved {r['from']} to {r['to']}" for r in rank_wins[:2]]
    return {
        "business_id": business_id,
        "published_this_week": len(pub), "published_all_time": total_pub["n"] if total_pub else 0,
        "rank_wins": rank_wins,
        "indexing": {"indexed": (idx_total["n"] or 0)-(idx_pending["n"] or 0), "pending": idx_pending["n"] or 0, "failed": idx_failed["n"] or 0},
        "top_pages": pub[:10],
        "wins": wins or ["Your SEO engine is warming up — first content ships soon."],
    }

@router.get("/onboarding/{job_id}")
def onboarding_status(job_id: str):
    try:
        from core.pg import execute_one
        row = execute_one("SELECT id,tenant_id,status,current_step,checkpoint,error,created_at,updated_at FROM onboarding_jobs WHERE id=%s", [job_id])
        if not row:
            raise HTTPException(404, "Job not found")
        return dict(row)
    except ImportError:
        raise HTTPException(503, "Onboarding DB unavailable")

@router.get("/review-queue")
def review_queue(business_id: str = Query(...)):
    db = _db()
    rows = [dict(r) for r in db.execute(
        "SELECT url, keyword, published_at, status FROM published_urls WHERE business_id=? AND status='needs_review' ORDER BY published_at DESC",
        [business_id]).fetchall()]
    db.close()
    dead_dir = Path("data/storage/dead_letter")
    for item in rows:
        slug = item["url"].rstrip("/").split("/")[-1]
        for f in (dead_dir.glob(f"*{slug}*.json") if dead_dir.exists() else []):
            try:
                d = json.loads(f.read_text())
                item["failure_reason"] = d.get("reason", "unknown")
                item["missing"] = d.get("missing", [])
            except Exception:
                pass
    groups = {}
    for item in rows:
        groups.setdefault(item.get("failure_reason", "unknown"), []).append(item)
    return {"business_id": business_id, "total": len(rows), "groups": groups, "items": rows}

@router.get("/rankings")
def rankings(business_id: str = Query(...), days: int = Query(30)):
    db = _db()
    since = (_now() - timedelta(days=days)).isoformat()
    rows = db.execute(
        "SELECT keyword, position, recorded_at FROM ranking_history WHERE business_id=? AND recorded_at>=? ORDER BY keyword, recorded_at DESC",
        [business_id, since]).fetchall()
    db.close()
    kw_map = {}
    for r in rows:
        kw_map.setdefault(r["keyword"], []).append({"position": r["position"], "at": r["recorded_at"]})
    keywords = []
    for kw, history in kw_map.items():
        cur = history[0]["position"] if history else None
        wk  = next((h["position"] for h in history if h["at"] < (_now()-timedelta(days=7)).isoformat()), None)
        mo  = history[-1]["position"] if history else None
        keywords.append({"keyword": kw, "current_position": cur,
            "delta_7d": (wk-cur) if wk and cur else None,
            "delta_30d": (mo-cur) if mo and cur else None,
            "best_ever": min((h["position"] for h in history if h["position"]), default=None),
            "history": history[:30]})
    keywords.sort(key=lambda k: k["current_position"] or 999)
    return {"business_id": business_id, "days": days, "keywords": keywords}

@router.get("/indexing")
def indexing_status(business_id: str = Query(...)):
    db = _db()
    pubs = db.execute("SELECT url, keyword, published_at FROM published_urls WHERE business_id=? AND status='live'", [business_id]).fetchall()
    idxs = db.execute("SELECT url, status, submitted_at FROM indexing_queue WHERE business_id=?", [business_id]).fetchall()
    db.close()
    idx_map = {r["url"]: dict(r) for r in idxs}
    pages = []
    for pub in pubs:
        url = pub["url"]
        idx = idx_map.get(url, {})
        days_since = None
        try:
            days_since = (_now() - datetime.fromisoformat(pub["published_at"].replace("Z", "+00:00"))).days
        except Exception:
            pass
        status = idx.get("status", "not_submitted")
        pages.append({"url": url, "keyword": pub["keyword"], "published_at": pub["published_at"],
            "days_since_publish": days_since, "indexing_status": status,
            "submitted_at": idx.get("submitted_at"),
            "action": "resubmit" if status=="failed" else ("submit" if status=="not_submitted" else None)})
    pages.sort(key=lambda p: p["indexing_status"] or "z")
    total = len(pages)
    indexed = sum(1 for p in pages if p["indexing_status"] == "submitted")
    return {"business_id": business_id,
        "summary": {"total": total, "indexed": indexed,
                    "pct_indexed": round(100*indexed/total, 1) if total else 0,
                    "needs_action": sum(1 for p in pages if p["action"])},
        "pages": pages}

@router.get("/audit")
def audit_log(business_id: str = Query(...), limit: int = Query(50), offset: int = Query(0)):
    try:
        from core.pg import execute_many
        rows = execute_many(
            "SELECT event_type, entity_type, entity_id, summary, actor, created_at FROM audit_events WHERE tenant_id=%s ORDER BY created_at DESC LIMIT %s OFFSET %s",
            [business_id, limit, offset], tenant_id=business_id)
        return {"business_id": business_id, "events": [dict(r) for r in (rows or [])]}
    except Exception as e:
        return {"business_id": business_id, "events": [], "note": str(e)}

@router.get("/credentials")
def credential_status(business_id: str = Query(...)):
    biz_data = {}
    try:
        all_biz = json.loads(Path("data/storage/businesses.json").read_text())
        for b in (all_biz if isinstance(all_biz, list) else all_biz.values()):
            if b.get("id") == business_id or b.get("business_id") == business_id:
                biz_data = b
                break
    except Exception:
        pass
    return {"business_id": business_id, "integrations": [
        {"name": "WordPress", "key": "wordpress",
         "connected": bool(biz_data.get("wp_site_url") and biz_data.get("wp_app_password")),
         "detail": biz_data.get("wp_site_url", "Not configured")},
        {"name": "Google Search Console", "key": "gsc",
         "connected": Path("config/gsc_token.json").exists(),
         "detail": "OAuth token present" if Path("config/gsc_token.json").exists() else "Not connected"},
        {"name": "IndexNow", "key": "indexnow",
         "connected": bool(os.getenv("INDEXNOW_API_KEY")),
         "detail": "Key configured" if os.getenv("INDEXNOW_API_KEY") else "Missing"},
        {"name": "DataForSEO", "key": "dataforseo",
         "connected": bool(os.getenv("DATAFORSEO_LOGIN")),
         "detail": "Credentials set" if os.getenv("DATAFORSEO_LOGIN") else "Not configured"},
    ]}
