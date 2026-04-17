import json, logging, sqlite3
from datetime import datetime, timedelta
from typing import Dict, List
import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
DB_PATH = "data/storage/seo_engine.db"

def detect_expansion_triggers(business_id: str) -> List[Dict]:
    triggers = []

    try:
        from core.pricing import check_limit
        conn = sqlite3.connect(DB_PATH)
        pages = conn.execute("SELECT COUNT(*) FROM published_urls WHERE business_id=? AND status='live'", [business_id]).fetchone()[0]
        keywords = conn.execute("SELECT COUNT(DISTINCT keyword) FROM ranking_history WHERE business_id=?", [business_id]).fetchone()[0]
        conn.close()

        for resource, used in [("pages_per_month", pages), ("keywords", keywords)]:
            chk = check_limit(business_id, resource, used)
            if chk["warn"]:
                triggers.append({
                    "type": "limit_80pct",
                    "resource": resource,
                    "pct_used": chk["pct_used"],
                    "message": f"You've used {chk['pct_used']:.0f}% of your {resource} limit — consider upgrading.",
                })
            if chk["over_limit"]:
                triggers.append({
                    "type": "over_limit",
                    "resource": resource,
                    "overage_cost": chk["overage_cost_usd"],
                    "message": f"Over {resource} limit — {chk['overage_units']} overage units this month.",
                })
    except Exception as exc:
        log.warning("expansion.limit_check_error  biz=%s  exc=%s", business_id, exc)

    try:
        conn = sqlite3.connect(DB_PATH)
        cutoff_90 = (datetime.utcnow() - timedelta(days=90)).isoformat()
        cutoff_180 = (datetime.utcnow() - timedelta(days=180)).isoformat()
        recent = conn.execute("SELECT COUNT(*) FROM published_urls WHERE business_id=? AND published_at > ?", [business_id, cutoff_90]).fetchone()[0]
        older = conn.execute("SELECT COUNT(*) FROM published_urls WHERE business_id=? AND published_at BETWEEN ? AND ?", [business_id, cutoff_180, cutoff_90]).fetchone()[0]
        conn.close()
        if older > 0 and recent >= older * 2:
            triggers.append({"type": "traffic_2x", "message": "Your organic growth has doubled in 90 days — great ROI signal for upgrading."})
    except Exception:
        pass

    try:
        conn = sqlite3.connect(DB_PATH)
        top3 = conn.execute("SELECT COUNT(DISTINCT keyword) FROM ranking_history WHERE business_id=? AND position <= 3", [business_id]).fetchone()[0]
        conn.close()
        prev_top3 = int(_redis.get(f"expansion:prev_top3:{business_id}") or 0)
        if top3 > 0 and prev_top3 == 0:
            triggers.append({"type": "first_top3", "message": f"You have {top3} keyword(s) ranking in top 3 — huge win!"})
        _redis.set(f"expansion:prev_top3:{business_id}", top3)
    except Exception:
        pass

    if triggers:
        _redis.setex(f"expansion:triggers:{business_id}", 86400 * 7, json.dumps(triggers))
    log.info("expansion.detected  biz=%s  triggers=%d", business_id, len(triggers))
    return triggers

def run_expansion_sweep() -> Dict:
    try:
        import json as j
        from pathlib import Path
        all_biz = j.loads(Path("data/storage/businesses.json").read_text())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
    except Exception:
        return {"status": "error", "reason": "could not load businesses"}

    total_triggers = 0
    tenants_with_triggers = 0
    for biz in biz_list:
        bid = biz.get("id") or biz.get("business_id")
        if not bid:
            continue
        triggers = detect_expansion_triggers(bid)
        if triggers:
            tenants_with_triggers += 1
            total_triggers += len(triggers)
            try:
                from core.slack_alerts import SlackAlerter
                alerter = SlackAlerter()
                for t in triggers:
                    alerter.alert_tenant(bid, "expansion", t["message"])
            except Exception:
                pass

    log.info("expansion.sweep_done  tenants_with_triggers=%d  total=%d", tenants_with_triggers, total_triggers)
    return {"tenants_checked": len(biz_list), "tenants_with_triggers": tenants_with_triggers, "total_triggers": total_triggers}
