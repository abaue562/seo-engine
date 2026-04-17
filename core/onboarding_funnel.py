import json, logging, sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

STAGES = {
    1: "signup", 2: "credentials_connected", 3: "first_publish",
    4: "first_indexed", 5: "first_ranking", 6: "first_top10", 7: "first_lead",
}
STAGE_NUDGE_HOURS = {
    2: 24,   # credentials: nudge if stuck 24h
    3: 72,   # first_publish: nudge if stuck 72h
    4: 168,  # first_indexed: nudge if stuck 7d
    5: 336,  # first_ranking: nudge if stuck 14d
}
NEXT_ACTIONS = {
    1: "Connect WordPress and Google Search Console",
    2: "Publish your first page",
    3: "Wait for Google to index your page (usually 1-7 days)",
    4: "Your page is indexed — wait for your first ranking (usually 7-14 days)",
    5: "You're ranking! Keep publishing to reach top 10",
    6: "Top 10 achieved! Track leads from your content",
    7: "Fully activated — maximize with more content",
}

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS onboarding_funnel (
        business_id TEXT PRIMARY KEY, stage INTEGER DEFAULT 1,
        stage_name TEXT DEFAULT 'signup',
        signup_at TEXT, credentials_at TEXT, first_publish_at TEXT,
        first_indexed_at TEXT, first_ranking_at TEXT, first_top10_at TEXT,
        first_lead_at TEXT, last_nudge_at TEXT, nudge_count INTEGER DEFAULT 0
    )""")
    c.commit()
    return c

MILESTONE_COLUMNS = {
    "signup": "signup_at", "credentials_connected": "credentials_at",
    "first_publish": "first_publish_at", "first_indexed": "first_indexed_at",
    "first_ranking": "first_ranking_at", "first_top10": "first_top10_at",
    "first_lead": "first_lead_at",
}
MILESTONE_STAGE = {v: k for k, v in STAGES.items()}

def record_milestone(business_id: str, milestone: str):
    col = MILESTONE_COLUMNS.get(milestone)
    if not col:
        log.warning("onboarding_funnel: unknown milestone %s", milestone)
        return
    stage = MILESTONE_STAGE.get(milestone, 1)
    conn = _conn()
    conn.execute("INSERT OR IGNORE INTO onboarding_funnel (business_id, signup_at) VALUES (?,datetime('now'))", [business_id])
    conn.execute(f"UPDATE onboarding_funnel SET {col}=datetime('now'), stage=MAX(stage,?), stage_name=? WHERE business_id=?",
                 [stage, milestone, business_id])
    conn.commit()
    conn.close()
    log.info("onboarding_funnel.milestone  biz=%s  milestone=%s  stage=%d", business_id, milestone, stage)

def get_funnel_stage(business_id: str) -> Dict:
    conn = _conn()
    row = conn.execute("SELECT stage, stage_name, signup_at, credentials_at, first_publish_at, first_indexed_at, first_ranking_at, first_top10_at, first_lead_at FROM onboarding_funnel WHERE business_id=?", [business_id]).fetchone()
    conn.close()
    if not row:
        return {"stage": 0, "stage_name": "not_started", "days_in_stage": 0, "next_action": NEXT_ACTIONS[1]}
    stage, stage_name = row[0], row[1]
    stage_ts = row[stage] if stage < len(row) else row[2]
    days_in = 0
    if stage_ts:
        try:
            days_in = (datetime.utcnow() - datetime.fromisoformat(stage_ts)).days
        except Exception:
            pass
    return {"stage": stage, "stage_name": stage_name, "days_in_stage": days_in,
            "next_action": NEXT_ACTIONS.get(stage + 1, "Fully activated")}

def get_stuck_tenants(stage: int, hours_threshold: int = 48) -> List[Dict]:
    col_map = {2: "signup_at", 3: "credentials_at", 4: "first_publish_at", 5: "first_indexed_at"}
    col = col_map.get(stage)
    if not col:
        return []
    cutoff = (datetime.utcnow() - timedelta(hours=hours_threshold)).isoformat()
    conn = _conn()
    rows = conn.execute(f"SELECT business_id, stage, {col} FROM onboarding_funnel WHERE stage=? AND {col} < ? AND {col} IS NOT NULL",
                        [stage - 1, cutoff]).fetchall()
    conn.close()
    return [{"business_id": r[0], "stage": r[1], "stuck_since": r[2]} for r in rows]

def should_nudge(business_id: str) -> Tuple[bool, str]:
    conn = _conn()
    row = conn.execute("SELECT stage, last_nudge_at, nudge_count FROM onboarding_funnel WHERE business_id=?", [business_id]).fetchone()
    conn.close()
    if not row:
        return False, ""
    stage, last_nudge, nudge_count = row
    if stage >= 7:
        return False, "fully_activated"
    threshold_hours = STAGE_NUDGE_HOURS.get(stage + 1, 999)
    if last_nudge:
        hours_since = (datetime.utcnow() - datetime.fromisoformat(last_nudge)).total_seconds() / 3600
        if hours_since < threshold_hours:
            return False, "nudged_recently"
    if nudge_count >= 3:
        return False, "max_nudges_reached"
    return True, NEXT_ACTIONS.get(stage + 1, "")

def mark_nudged(business_id: str):
    conn = _conn()
    conn.execute("UPDATE onboarding_funnel SET last_nudge_at=datetime('now'), nudge_count=nudge_count+1 WHERE business_id=?", [business_id])
    conn.commit()
    conn.close()
