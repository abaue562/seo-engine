import json, logging, sqlite3, hashlib
from typing import List, Dict, Optional
from datetime import datetime

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS seo_hypotheses (
        id TEXT PRIMARY KEY,
        hypothesis TEXT NOT NULL,
        metric TEXT DEFAULT 'rank_at_30d',
        test_group TEXT DEFAULT '[]',
        control_group TEXT DEFAULT '[]',
        status TEXT DEFAULT 'pending',
        promoted INTEGER DEFAULT 0,
        started_at TEXT DEFAULT (datetime('now')),
        completed_at TEXT,
        result TEXT DEFAULT '{}'
    )""")
    c.commit()
    return c

def generate_hypotheses(business_id: str) -> List[Dict]:
    conn = _conn()
    sample = conn.execute("""SELECT keyword, position FROM ranking_history WHERE business_id=? ORDER BY checked_at DESC LIMIT 50""", [business_id]).fetchall()
    conn.close()
    summary = f"Top ranking data (keyword, position): {sample[:10]}"

    prompt = f"""You are an SEO experimentation strategist. Based on site data:
{summary}

Propose exactly 3 testable SEO hypotheses as JSON array:
[{{"hypothesis": "...", "metric": "rank_at_30d|ctr|indexing_speed", "sample_size_needed": N, "duration_days": N}}]
Keep hypotheses specific and testable. Focus on schema markup, content length, publish timing, or internal linking."""

    try:
        from core.llm_gateway import LLMGateway
        gw = LLMGateway(business_id=business_id)
        raw = gw.generate(prompt, complexity="fast")
        hypotheses = json.loads(raw.strip().lstrip("```json").rstrip("```"))
        for h in hypotheses:
            register_hypothesis(h)
        log.info("hypothesis_engine.generated  biz=%s  count=%d", business_id, len(hypotheses))
        return hypotheses
    except Exception as exc:
        log.exception("hypothesis_engine.error  biz=%s", business_id)
        return []

def register_hypothesis(h: dict) -> str:
    uid = hashlib.sha256(h["hypothesis"].encode()).hexdigest()[:16]
    conn = _conn()
    conn.execute("INSERT OR IGNORE INTO seo_hypotheses (id, hypothesis, metric) VALUES (?,?,?)",
                 [uid, h["hypothesis"], h.get("metric", "rank_at_30d")])
    conn.commit()
    conn.close()
    return uid

def evaluate_hypothesis(hypothesis_id: str) -> Optional[Dict]:
    conn = _conn()
    row = conn.execute("SELECT hypothesis, test_group, control_group, metric FROM seo_hypotheses WHERE id=?", [hypothesis_id]).fetchone()
    if not row:
        conn.close()
        return None
    hyp, test_json, ctrl_json, metric = row
    test_urls = json.loads(test_json)
    ctrl_urls = json.loads(ctrl_json)
    if not test_urls or not ctrl_urls:
        conn.close()
        return {"status": "insufficient_data"}

    def get_positions(urls):
        placeholders = ",".join("?" * len(urls))
        return [r[0] for r in conn.execute(f"SELECT position FROM ranking_history WHERE url IN ({placeholders}) AND position IS NOT NULL", urls).fetchall()]

    test_pos = get_positions(test_urls)
    ctrl_pos = get_positions(ctrl_urls)
    conn.close()

    if not test_pos or not ctrl_pos:
        return {"status": "no_data"}

    test_mean = sum(test_pos) / len(test_pos)
    ctrl_mean = sum(ctrl_pos) / len(ctrl_pos)
    improvement = ctrl_mean - test_mean  # positive = test ranks higher (lower position number = better)

    significant = False
    try:
        from scipy import stats
        _, p_value = stats.ttest_ind(test_pos, ctrl_pos)
        significant = p_value < 0.05 and improvement > 0
    except ImportError:
        significant = improvement > 2 and len(test_pos) >= 5

    result = {"test_mean": round(test_mean, 1), "ctrl_mean": round(ctrl_mean, 1), "improvement": round(improvement, 1), "significant": significant}
    db = _conn()
    db.execute("UPDATE seo_hypotheses SET status='evaluated', result=?, completed_at=datetime('now'), promoted=? WHERE id=?",
               [json.dumps(result), 1 if significant else 0, hypothesis_id])
    db.commit()
    db.close()
    log.info("hypothesis_engine.evaluated  id=%s  improvement=%.1f  significant=%s", hypothesis_id, improvement, significant)
    return result

def promote_winning_hypotheses():
    conn = _conn()
    winners = conn.execute("SELECT id, hypothesis, result FROM seo_hypotheses WHERE promoted=1 AND status='evaluated'").fetchall()
    conn.close()
    for hid, hyp, result_json in winners:
        try:
            from core.learning_loop import record_outcome
            record_outcome(f"hypothesis:{hid}", success=True, meta={"hypothesis": hyp, "result": json.loads(result_json)})
        except Exception:
            pass
    log.info("hypothesis_engine.promoted  count=%d", len(winners))
    return len(winners)
