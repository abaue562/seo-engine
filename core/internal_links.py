import json, logging, re, sqlite3, hashlib
from typing import List, Dict

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("""CREATE TABLE IF NOT EXISTS link_suggestions (
        id TEXT PRIMARY KEY,
        business_id TEXT NOT NULL,
        target_url TEXT NOT NULL,
        source_url TEXT NOT NULL,
        anchor_text TEXT,
        relevance_score REAL DEFAULT 0.5,
        auto_applied INTEGER DEFAULT 0,
        status TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT (datetime('now'))
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_link_biz ON link_suggestions(business_id, status)")
    c.commit()
    return c

def _extract_paragraphs(html: str) -> List[str]:
    paras = re.findall(r'<p[^>]*>(.*?)</p>', html, re.S)
    clean = [re.sub(r'<[^>]+>', '', p).strip() for p in paras]
    return [p for p in clean if len(p) > 80]

def _keyword_overlap_score(text1: str, text2: str) -> float:
    words1 = set(text1.lower().split())
    words2 = set(text2.lower().split())
    stopwords = {"the","a","an","is","it","in","of","to","for","and","or","with","on","at","by"}
    w1 = words1 - stopwords
    w2 = words2 - stopwords
    if not w1 or not w2:
        return 0.0
    return len(w1 & w2) / min(len(w1), len(w2))

def find_link_opportunities(new_url: str, new_content: str, business_id: str, auto_apply_n: int = 3) -> List[Dict]:
    conn = _conn()
    existing = conn.execute("SELECT url FROM published_urls WHERE business_id=? AND status='live' AND url!=? LIMIT 100", [business_id, new_url]).fetchall()
    conn.close()

    new_paragraphs = _extract_paragraphs(new_content)
    if not new_paragraphs:
        return []

    suggestions = []
    use_embeddings = False
    try:
        from core.embeddings import find_similar
        use_embeddings = True
    except ImportError:
        pass

    if use_embeddings:
        for para in new_paragraphs[:5]:
            similar = find_similar(para, "content_paragraph", top_k=3)
            for art_id, score in similar:
                if score > 0.75:
                    source_url, snippet = art_id.split("|", 1) if "|" in art_id else (art_id, para[:100])
                    anchor = para.split(".")[0][:60]
                    suggestions.append({"source_url": source_url, "paragraph_snippet": snippet[:200], "anchor_text": anchor, "relevance_score": round(score, 2)})
    else:
        for url_row in existing[:30]:
            source_url = url_row[0]
            for para in new_paragraphs[:3]:
                score = _keyword_overlap_score(para, source_url)
                if score > 0.3:
                    suggestions.append({"source_url": source_url, "paragraph_snippet": para[:200], "anchor_text": para.split(".")[0][:60], "relevance_score": round(score, 2)})

    suggestions.sort(key=lambda x: x["relevance_score"], reverse=True)
    top = suggestions[:10]
    _save_suggestions(business_id, new_url, top, auto_apply_n)
    log.info("internal_links.found  biz=%s  target=%s  count=%d", business_id, new_url, len(top))
    return top

def _save_suggestions(business_id: str, target_url: str, suggestions: List[Dict], auto_apply_n: int):
    conn = _conn()
    for i, s in enumerate(suggestions):
        uid = hashlib.sha256(f"{target_url}:{s['source_url']}:{s['anchor_text']}".encode()).hexdigest()[:16]
        conn.execute("INSERT OR IGNORE INTO link_suggestions (id, business_id, target_url, source_url, anchor_text, relevance_score, auto_applied) VALUES (?,?,?,?,?,?,?)",
                     [uid, business_id, target_url, s["source_url"], s["anchor_text"], s["relevance_score"], 1 if i < auto_apply_n else 0])
    conn.commit()
    conn.close()

def get_link_suggestions(business_id: str, status: str = "pending") -> List[Dict]:
    conn = _conn()
    rows = conn.execute("SELECT id, target_url, source_url, anchor_text, relevance_score, auto_applied FROM link_suggestions WHERE business_id=? AND status=? ORDER BY relevance_score DESC LIMIT 50", [business_id, status]).fetchall()
    conn.close()
    return [{"id": r[0], "target_url": r[1], "source_url": r[2], "anchor_text": r[3], "relevance_score": r[4], "auto_applied": bool(r[5])} for r in rows]
