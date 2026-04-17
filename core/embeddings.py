import hashlib, json, logging, os, sqlite3, struct
from typing import List, Tuple, Optional
import numpy as np

log = logging.getLogger(__name__)
DB_PATH = "data/storage/seo_engine.db"

def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS embeddings (
        id TEXT PRIMARY KEY,
        artifact_type TEXT NOT NULL,
        artifact_id TEXT NOT NULL,
        embedding BLOB NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_emb_type ON embeddings(artifact_type)")
    conn.commit()
    return conn

def embed_text(text: str) -> Optional[List[float]]:
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        log.warning("embeddings: OPENAI_API_KEY not set, skipping")
        return None
    try:
        import openai
        client = openai.OpenAI(api_key=api_key)
        resp = client.embeddings.create(model="text-embedding-3-small", input=text[:8000])
        return resp.data[0].embedding
    except Exception as exc:
        log.exception("embed_text.error")
        return None

def _vec_to_blob(vec: List[float]) -> bytes:
    return struct.pack(f"{len(vec)}f", *vec)

def _blob_to_vec(blob: bytes) -> np.ndarray:
    n = len(blob) // 4
    return np.array(struct.unpack(f"{n}f", blob))

def store_embedding(artifact_type: str, artifact_id: str, text: str) -> bool:
    vec = embed_text(text)
    if vec is None:
        return False
    uid = hashlib.sha256(f"{artifact_type}:{artifact_id}".encode()).hexdigest()
    conn = _get_conn()
    conn.execute("INSERT OR REPLACE INTO embeddings (id, artifact_type, artifact_id, embedding) VALUES (?,?,?,?)",
                 [uid, artifact_type, artifact_id, _vec_to_blob(vec)])
    conn.commit()
    conn.close()
    log.debug("embeddings.store  type=%s  id=%s", artifact_type, artifact_id)
    return True

def find_similar(text: str, artifact_type: str, top_k: int = 5) -> List[Tuple[str, float]]:
    query_vec = embed_text(text)
    if query_vec is None:
        return []
    q = np.array(query_vec)
    conn = _get_conn()
    rows = conn.execute("SELECT artifact_id, embedding FROM embeddings WHERE artifact_type=?", [artifact_type]).fetchall()
    conn.close()
    if not rows:
        return []
    scores = []
    for artifact_id, blob in rows:
        v = _blob_to_vec(blob)
        cos = float(np.dot(q, v) / (np.linalg.norm(q) * np.linalg.norm(v) + 1e-9))
        scores.append((artifact_id, cos))
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores[:top_k]
