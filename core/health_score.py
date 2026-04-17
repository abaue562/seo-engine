import json, logging, sqlite3
from typing import Dict
import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
DB_PATH = "data/storage/seo_engine.db"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS tenant_health_scores (
        business_id TEXT PRIMARY KEY, score INTEGER, breakdown TEXT DEFAULT '{}',
        status TEXT DEFAULT 'healthy', updated_at TEXT DEFAULT (datetime('now'))
    )""")
    c.commit()
    return c

def _login_score(business_id: str) -> int:
    count = int(_redis.get(f"logins:30d:{business_id}") or 0)
    if count == 0: return 0
    if count < 3: return 5
    if count < 7: return 12
    return 25

def _utilization_score(business_id: str) -> int:
    try:
        from core.pricing import check_limit
        conn = sqlite3.connect(DB_PATH)
        pages = conn.execute("SELECT COUNT(*) FROM published_urls WHERE business_id=? AND status='live'", [business_id]).fetchone()[0]
        keywords = conn.execute("SELECT COUNT(DISTINCT keyword) FROM ranking_history WHERE business_id=?", [business_id]).fetchone()[0]
        conn.close()
        page_check = check_limit(business_id, "pages_per_month", pages)
        kw_check = check_limit(business_id, "keywords", keywords)
        avg_pct = (page_check["pct_used"] + kw_check["pct_used"]) / 2
        if avg_pct > 100: return 25
        if avg_pct >= 80: return 18
        if avg_pct >= 50: return 20
        if avg_pct >= 10: return 15
        return 5
    except Exception:
        return 10

def _results_score(business_id: str) -> int:
    try:
        conn = sqlite3.connect(DB_PATH)
        top10 = conn.execute("SELECT COUNT(DISTINCT keyword) FROM ranking_history WHERE business_id=? AND position <= 10", [business_id]).fetchone()[0]
        conn.close()
        if top10 == 0: return 0
        if top10 < 5: return 10
        if top10 < 20: return 18
        return 25
    except Exception:
        return 0

def _credential_score(business_id: str) -> int:
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute("SELECT count(*) FROM businesses WHERE (id=? OR business_id=?) AND wp_app_password IS NOT NULL AND gsc_credentials IS NOT NULL", [business_id, business_id]).fetchone()
        conn.close()
        if row and row[0] > 0:
            return 15
        return 8
    except Exception:
        return 8

def _engagement_score(business_id: str) -> int:
    actioned = int(_redis.get(f"review_queue:actioned:14d:{business_id}") or 0)
    if actioned == 0: return 0
    if actioned < 5: return 8
    return 15

def compute_health_score(business_id: str) -> Dict:
    cache_key = f"health:{business_id}"
    cached = _redis.get(cache_key)
    if cached:
        return json.loads(cached)

    breakdown = {
        "login_frequency": _login_score(business_id),
        "plan_utilization": _utilization_score(business_id),
        "ranking_results": _results_score(business_id),
        "credential_health": _credential_score(business_id),
        "review_engagement": _engagement_score(business_id),
    }
    score = sum(breakdown.values())
    if score >= 70: status = "healthy"
    elif score >= 50: status = "at_risk"
    else: status = "critical"

    result = {"business_id": business_id, "score": score, "breakdown": breakdown, "status": status}
    _redis.setex(cache_key, 86400, json.dumps(result))

    conn = _conn()
    conn.execute("INSERT OR REPLACE INTO tenant_health_scores (business_id, score, breakdown, status) VALUES (?,?,?,?)",
                 [business_id, score, json.dumps(breakdown), status])
    conn.commit()
    conn.close()
    log.info("health_score.computed  biz=%s  score=%d  status=%s", business_id, score, status)
    return result

def get_at_risk_tenants(limit: int = 20) -> list:
    conn = _conn()
    rows = conn.execute("""
        SELECT hs.business_id, hs.score, hs.status
        FROM tenant_health_scores hs
        WHERE hs.status IN ('at_risk','critical')
        ORDER BY hs.score ASC LIMIT ?
    """, [limit]).fetchall()
    conn.close()
    return [{"business_id": r[0], "score": r[1], "status": r[2]} for r in rows]

def batch_compute_health_scores() -> int:
    try:
        import json as j
        from pathlib import Path
        all_biz = j.loads(Path("data/storage/businesses.json").read_text())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
        for biz in biz_list:
            bid = biz.get("id") or biz.get("business_id")
            if bid:
                _redis.delete(f"health:{bid}")
                compute_health_score(bid)
        return len(biz_list)
    except Exception as exc:
        log.exception("health_score.batch_error")
        return 0
