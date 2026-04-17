import hashlib, json, logging, secrets, sqlite3
from typing import Dict, List, Optional
import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
DB_PATH = "data/storage/seo_engine.db"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS referrals (
        id TEXT PRIMARY KEY, referrer_business_id TEXT NOT NULL,
        code TEXT UNIQUE NOT NULL, referee_business_id TEXT,
        status TEXT DEFAULT 'pending', credit_months INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')), converted_at TEXT
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ref_code ON referrals(code)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ref_biz ON referrals(referrer_business_id)")
    c.commit()
    return c

def create_referral_code(business_id: str) -> str:
    conn = _conn()
    existing = conn.execute("SELECT code FROM referrals WHERE referrer_business_id=? AND status='pending'", [business_id]).fetchone()
    if existing:
        conn.close()
        return existing[0]
    code = secrets.token_urlsafe(8).upper()
    uid = hashlib.sha256(f"{business_id}:{code}".encode()).hexdigest()[:16]
    conn.execute("INSERT INTO referrals (id, referrer_business_id, code) VALUES (?,?,?)", [uid, business_id, code])
    conn.commit()
    conn.close()
    log.info("referral.code_created  biz=%s  code=%s", business_id, code)
    return code

def get_referral_stats(business_id: str) -> Dict:
    conn = _conn()
    rows = conn.execute("SELECT status, COUNT(*), SUM(credit_months) FROM referrals WHERE referrer_business_id=? GROUP BY status", [business_id]).fetchall()
    conn.close()
    stats = {"pending": 0, "converted": 0, "total_credit_months": 0, "code": None}
    for status, count, credits in rows:
        stats[status] = count
        stats["total_credit_months"] += (credits or 0)
    code_conn = _conn()
    code_row = code_conn.execute("SELECT code FROM referrals WHERE referrer_business_id=? LIMIT 1", [business_id]).fetchone()
    code_conn.close()
    if code_row:
        stats["code"] = code_row[0]
    credits_remaining = int(_redis.get(f"referral_credits:{business_id}") or 0)
    stats["credits_remaining_months"] = credits_remaining
    return stats

def record_conversion(code: str, referee_business_id: str) -> bool:
    conn = _conn()
    row = conn.execute("SELECT id, referrer_business_id, status FROM referrals WHERE code=?", [code]).fetchone()
    if not row or row[2] != "pending":
        conn.close()
        return False
    ref_id, referrer_biz, _ = row
    conn.execute("UPDATE referrals SET status='converted', referee_business_id=?, converted_at=datetime('now'), credit_months=2 WHERE id=?",
                 [referee_business_id, ref_id])
    conn.commit()
    conn.close()
    apply_credit(referrer_biz, months=2)
    log.info("referral.converted  code=%s  referrer=%s  referee=%s", code, referrer_biz, referee_business_id)
    return True

def apply_credit(business_id: str, months: int):
    _redis.incrby(f"referral_credits:{business_id}", months)
    log.info("referral.credit_applied  biz=%s  months=%d", business_id, months)

def get_extended_trial_days(code: Optional[str]) -> int:
    if not code:
        return 14
    conn = _conn()
    row = conn.execute("SELECT id FROM referrals WHERE code=? AND status='pending'", [code]).fetchone()
    conn.close()
    return 30 if row else 14

def get_top_referrers(limit: int = 10) -> List[Dict]:
    conn = _conn()
    rows = conn.execute("""
        SELECT referrer_business_id, COUNT(*) as conversions, SUM(credit_months) as credits
        FROM referrals WHERE status='converted'
        GROUP BY referrer_business_id ORDER BY conversions DESC LIMIT ?
    """, [limit]).fetchall()
    conn.close()
    return [{"business_id": r[0], "conversions": r[1], "credit_months": r[2]} for r in rows]
