import json, logging, sqlite3
from typing import Dict

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"
PLATFORM_PRIORS = {"best_length_range": [800, 1500], "best_publish_day": "tuesday", "best_cadence": "steady"}

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS tenant_strategy (
        business_id TEXT PRIMARY KEY,
        strategy TEXT DEFAULT '{}',
        overrides TEXT DEFAULT '{}',
        updated_at TEXT DEFAULT (datetime('now'))
    )""")
    c.commit()
    return c

def get_strategy(business_id: str) -> Dict:
    conn = _conn()
    row = conn.execute("SELECT strategy, overrides FROM tenant_strategy WHERE business_id=?", [business_id]).fetchone()
    conn.close()
    if not row:
        return {**PLATFORM_PRIORS, "source": "platform_prior"}
    strategy = json.loads(row[0] or "{}")
    overrides = json.loads(row[1] or "{}")
    return {**strategy, **overrides, "source": "tenant_learned"}

def update_strategy(business_id: str):
    conn = _conn()
    rows = conn.execute("""
        SELECT p.url, p.word_count, r.position, strftime('%w', p.published_at) as dow
        FROM published_urls p
        LEFT JOIN ranking_history r ON p.url=r.url AND p.business_id=r.business_id
        WHERE p.business_id=? AND p.status='live'
        ORDER BY r.position ASC LIMIT 200
    """, [business_id]).fetchall()

    strategy = {**PLATFORM_PRIORS}
    if len(rows) >= 5:
        top = [r for r in rows if r[2] and r[2] <= 10]
        if top:
            lengths = [r[1] for r in top if r[1]]
            if lengths:
                avg_len = sum(lengths) // len(lengths)
                strategy["best_length_range"] = [int(avg_len * 0.8), int(avg_len * 1.2)]
            dow_counts = {}
            for r in top:
                d = r[3]
                if d:
                    dow_counts[d] = dow_counts.get(d, 0) + 1
            if dow_counts:
                dow_map = {"0":"sunday","1":"monday","2":"tuesday","3":"wednesday","4":"thursday","5":"friday","6":"saturday"}
                best_dow = max(dow_counts, key=lambda k: dow_counts[k])
                strategy["best_publish_day"] = dow_map.get(best_dow, "tuesday")
        weight_tenant = 0.7 if len(rows) >= 20 else 0.3
        strategy["confidence"] = round(weight_tenant, 2)
        strategy["sample_count"] = len(rows)

    conn.execute("INSERT OR REPLACE INTO tenant_strategy (business_id, strategy, updated_at) VALUES (?,?,datetime('now'))", [business_id, json.dumps(strategy)])
    conn.commit()
    conn.close()
    log.info("tenant_strategy.updated  biz=%s  samples=%d", business_id, len(rows))
    return strategy

def record_strategy_override(business_id: str, overrides: dict):
    conn = _conn()
    conn.execute("INSERT OR IGNORE INTO tenant_strategy (business_id) VALUES (?)", [business_id])
    conn.execute("UPDATE tenant_strategy SET overrides=? WHERE business_id=?", [json.dumps(overrides), business_id])
    conn.commit()
    conn.close()
