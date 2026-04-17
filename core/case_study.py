import hashlib, json, logging, sqlite3
from datetime import datetime, timedelta
from typing import Dict, Optional

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS case_studies (
        id TEXT PRIMARY KEY, business_id TEXT UNIQUE NOT NULL,
        draft TEXT, status TEXT DEFAULT 'draft',
        approved_at TEXT, published_url TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")
    c.commit()
    return c

def check_case_study_eligible(business_id: str) -> Dict:
    conn = sqlite3.connect(DB_PATH)
    pages = conn.execute("SELECT COUNT(*) FROM published_urls WHERE business_id=? AND status='live'", [business_id]).fetchone()[0]
    top3 = conn.execute("SELECT COUNT(DISTINCT keyword) FROM ranking_history WHERE business_id=? AND position <= 3", [business_id]).fetchone()[0]

    onboard_row = conn.execute("SELECT signup_at FROM onboarding_funnel WHERE business_id=?", [business_id]).fetchone() if _table_exists(conn, "onboarding_funnel") else None
    conn.close()

    days_since_signup = 999
    if onboard_row and onboard_row[0]:
        try:
            days_since_signup = (datetime.utcnow() - datetime.fromisoformat(onboard_row[0])).days
        except Exception:
            pass

    eligible = days_since_signup >= 90 and (top3 >= 3 or pages >= 15)
    return {"business_id": business_id, "eligible": eligible, "pages": pages, "top3_rankings": top3, "days_since_signup": days_since_signup}

def _table_exists(conn, table_name: str) -> bool:
    return conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [table_name]).fetchone() is not None

def draft_case_study(business_id: str) -> Optional[Dict]:
    eligibility = check_case_study_eligible(business_id)
    if not eligibility["eligible"]:
        return None

    conn = sqlite3.connect(DB_PATH)
    existing = conn.execute("SELECT id, status FROM case_studies WHERE business_id=?", [business_id]).fetchone()
    conn.close()
    if existing and existing[1] != "draft":
        return {"status": "already_exists", "case_study_status": existing[1]}

    facts = f"""
Business: {business_id}
Pages published: {eligibility['pages']}
Top-3 rankings: {eligibility['top3_rankings']}
Days active: {eligibility['days_since_signup']}
"""
    prompt = f"""Write a concise SEO case study for a customer of an AI SEO automation platform.

Facts:
{facts}

Format:
- 2-3 sentence intro (problem they had)
- Results section: bullet points of key wins
- Quote placeholder: [CUSTOMER_QUOTE_HERE]
- 1-sentence CTA

Keep it under 300 words. Focus on concrete numbers."""

    draft_text = ""
    try:
        from core.llm_gateway import LLMGateway
        gw = LLMGateway(business_id=business_id)
        draft_text = gw.generate(prompt, complexity="fast")
    except Exception as exc:
        log.exception("case_study.llm_error  biz=%s", business_id)
        draft_text = f"[Auto-draft failed — please write manually]\n\nFacts:\n{facts}"

    uid = hashlib.sha256(business_id.encode()).hexdigest()[:16]
    conn = _conn()
    conn.execute("INSERT OR REPLACE INTO case_studies (id, business_id, draft, status) VALUES (?,?,?,'draft')", [uid, business_id, draft_text])
    conn.commit()
    conn.close()
    log.info("case_study.drafted  biz=%s  length=%d", business_id, len(draft_text))
    return {"business_id": business_id, "status": "draft", "draft": draft_text, "eligibility": eligibility}

def scan_for_eligible_tenants() -> int:
    try:
        import json as j
        from pathlib import Path
        all_biz = j.loads(Path("data/storage/businesses.json").read_text())
        biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
    except Exception:
        return 0
    drafted = 0
    for biz in biz_list:
        bid = biz.get("id") or biz.get("business_id")
        if not bid:
            continue
        result = draft_case_study(bid)
        if result and result.get("status") == "draft":
            drafted += 1
    log.info("case_study.scan_done  drafted=%d", drafted)
    return drafted
