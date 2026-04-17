"""Per-tenant credential vault with optional Fernet encryption."""
from __future__ import annotations
import os
import sqlite3
import json
from typing import Optional

DB_PATH = "data/storage/seo_engine.db"

PLATFORM_KEYS = {
    "github_pages": ["GITHUB_TOKEN", "GITHUB_PAGES_OWNER", "GITHUB_PAGES_REPO"],
    "medium": ["MEDIUM_TOKEN"],
    "devto": ["DEVTO_API_KEY"],
    "reddit": ["REDDIT_USER", "REDDIT_PASS", "REDDIT_SUBREDDIT"],
    "quora": ["QUORA_EMAIL", "QUORA_PASS"],
    "linkedin": ["LINKEDIN_EMAIL", "LINKEDIN_PASS"],
    "resend": ["RESEND_API_KEY"],
    "wordpress": ["WP_SITE_URL", "WP_APP_PASSWORD"],
    "smtp": ["SMTP_HOST", "SMTP_USER", "SMTP_PASS", "SMTP_FROM", "SMTP_PORT"],
}


def _get_fernet():
    key = os.environ.get("VAULT_KEY", "")
    if not key:
        return None
    try:
        from cryptography.fernet import Fernet
        return Fernet(key.encode() if isinstance(key, str) else key)
    except Exception:
        return None


def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS credential_vault (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id TEXT NOT NULL,
            platform TEXT NOT NULL,
            key_name TEXT NOT NULL,
            key_value TEXT NOT NULL,
            encrypted INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(business_id, platform, key_name)
        )
    """)
    conn.commit()
    return conn


def set_credential(business_id: str, platform: str, key_name: str, value: str) -> dict:
    f = _get_fernet()
    encrypted = 0
    stored = value
    if f and value:
        stored = f.encrypt(value.encode()).decode()
        encrypted = 1
    conn = _db()
    conn.execute("""
        INSERT INTO credential_vault (business_id, platform, key_name, key_value, encrypted, updated_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(business_id, platform, key_name)
        DO UPDATE SET key_value=excluded.key_value, encrypted=excluded.encrypted, updated_at=excluded.updated_at
    """, [business_id, platform, key_name, stored, encrypted])
    conn.commit()
    conn.close()
    return {"status": "ok", "platform": platform, "key_name": key_name}


def get_credential(business_id: str, platform: str, key_name: str) -> Optional[str]:
    conn = _db()
    row = conn.execute(
        "SELECT key_value, encrypted FROM credential_vault WHERE business_id=? AND platform=? AND key_name=?",
        [business_id, platform, key_name]
    ).fetchone()
    conn.close()
    if not row:
        return None
    value = row["key_value"]
    if row["encrypted"]:
        f = _get_fernet()
        if f:
            try:
                value = f.decrypt(value.encode()).decode()
            except Exception:
                return None
    return value


def get_platform_credentials(business_id: str, platform: str) -> dict:
    keys = PLATFORM_KEYS.get(platform, [])
    result = {}
    for k in keys:
        val = get_credential(business_id, platform, k)
        result[k] = val or ""
    return result


def list_platforms(business_id: str) -> list:
    conn = _db()
    rows = conn.execute(
        "SELECT DISTINCT platform FROM credential_vault WHERE business_id=?", [business_id]
    ).fetchall()
    conn.close()
    configured = [r["platform"] for r in rows]
    result = []
    for platform, keys in PLATFORM_KEYS.items():
        conn2 = _db()
        count = conn2.execute(
            "SELECT COUNT(*) as c FROM credential_vault WHERE business_id=? AND platform=?",
            [business_id, platform]
        ).fetchone()["c"]
        conn2.close()
        result.append({
            "platform": platform,
            "required_keys": keys,
            "configured_count": count,
            "complete": count >= len(keys),
        })
    return result


def delete_credential(business_id: str, platform: str, key_name: str) -> dict:
    conn = _db()
    conn.execute(
        "DELETE FROM credential_vault WHERE business_id=? AND platform=? AND key_name=?",
        [business_id, platform, key_name]
    )
    conn.commit()
    conn.close()
    return {"status": "deleted"}


def inject_env_credentials(business_id: str, platform: str):
    """Load stored creds into os.environ for this process."""
    keys = PLATFORM_KEYS.get(platform, [])
    for k in keys:
        val = get_credential(business_id, platform, k)
        if val:
            os.environ[k] = val

# LinkedIn credentials added
PLATFORM_KEYS[linkedin] = [LINKEDIN_ACCESS_TOKEN, LINKEDIN_AUTHOR_URN, LINKEDIN_CLIENT_ID, LINKEDIN_CLIENT_SECRET]
# PRLog credentials added
PLATFORM_KEYS[prlog] = [PRLOG_EMAIL, PRLOG_PASSWORD]
