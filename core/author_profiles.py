"""
E-E-A-T: Author profile management and content injection.
Stores author credentials, injects bio blocks post-generation,
builds schema.org/Person markup for Google's author-aware ranking signals.
"""
from __future__ import annotations
import json
import sqlite3
import hashlib
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone

log = logging.getLogger(__name__)

_DB = "data/storage/seo_engine.db"


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB)
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE IF NOT EXISTS author_profiles (
            id          TEXT PRIMARY KEY,
            business_id TEXT NOT NULL,
            name        TEXT NOT NULL,
            slug        TEXT NOT NULL,
            title       TEXT,
            bio         TEXT,
            expertise   TEXT,      -- JSON list of topic areas
            credentials TEXT,      -- JSON list: ["MBA", "10 years experience", ...]
            photo_url   TEXT,
            linkedin_url TEXT,
            twitter_url TEXT,
            website_url TEXT,
            schema_json TEXT,      -- cached schema.org/Person JSON-LD
            is_default  INTEGER DEFAULT 0,
            created_at  TEXT,
            updated_at  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_author_biz ON author_profiles(business_id);

        CREATE TABLE IF NOT EXISTS author_assignments (
            id          TEXT PRIMARY KEY,
            business_id TEXT NOT NULL,
            content_url TEXT NOT NULL,
            author_id   TEXT NOT NULL,
            assigned_at TEXT,
            FOREIGN KEY(author_id) REFERENCES author_profiles(id)
        );
        CREATE INDEX IF NOT EXISTS idx_assign_biz ON author_assignments(business_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_assign_url ON author_assignments(business_id, content_url);
    """)
    c.commit()
    return c


def _new_id(business_id: str, name: str) -> str:
    return hashlib.md5(f"{business_id}:{name}".encode()).hexdigest()[:12]


def upsert_author(
    business_id: str,
    name: str,
    title: str = "",
    bio: str = "",
    expertise: list[str] | None = None,
    credentials: list[str] | None = None,
    photo_url: str = "",
    linkedin_url: str = "",
    twitter_url: str = "",
    website_url: str = "",
    is_default: bool = False,
) -> dict:
    """Create or update an author profile. Returns the profile dict."""
    author_id = _new_id(business_id, name)
    slug = name.lower().replace(" ", "-").replace("'", "")
    now = datetime.now(timezone.utc).isoformat()
    expertise_json = json.dumps(expertise or [])
    credentials_json = json.dumps(credentials or [])
    schema = _build_person_schema(
        name, title, bio, expertise or [], credentials or [],
        photo_url, linkedin_url, twitter_url, website_url
    )
    schema_json = json.dumps(schema)

    with _conn() as c:
        if is_default:
            c.execute("UPDATE author_profiles SET is_default=0 WHERE business_id=?", [business_id])
        c.execute("""
            INSERT INTO author_profiles
                (id,business_id,name,slug,title,bio,expertise,credentials,
                 photo_url,linkedin_url,twitter_url,website_url,schema_json,is_default,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                title=excluded.title, bio=excluded.bio, expertise=excluded.expertise,
                credentials=excluded.credentials, photo_url=excluded.photo_url,
                linkedin_url=excluded.linkedin_url, twitter_url=excluded.twitter_url,
                website_url=excluded.website_url, schema_json=excluded.schema_json,
                is_default=excluded.is_default, updated_at=excluded.updated_at
        """, [author_id, business_id, name, slug, title, bio, expertise_json, credentials_json,
              photo_url, linkedin_url, twitter_url, website_url, schema_json,
              int(is_default), now, now])
    log.info("upsert_author  id=%s  name=%s  default=%s", author_id, name, is_default)
    return get_author(author_id)


def get_author(author_id: str) -> dict:
    with _conn() as c:
        row = c.execute("SELECT * FROM author_profiles WHERE id=?", [author_id]).fetchone()
    if not row:
        return {}
    d = dict(row)
    d["expertise"] = json.loads(d.get("expertise") or "[]")
    d["credentials"] = json.loads(d.get("credentials") or "[]")
    d["schema_json"] = json.loads(d.get("schema_json") or "{}")
    return d


def get_default_author(business_id: str) -> dict:
    with _conn() as c:
        row = c.execute(
            "SELECT * FROM author_profiles WHERE business_id=? AND is_default=1 LIMIT 1",
            [business_id]
        ).fetchone()
        if not row:
            row = c.execute(
                "SELECT * FROM author_profiles WHERE business_id=? ORDER BY created_at LIMIT 1",
                [business_id]
            ).fetchone()
    if not row:
        return {}
    d = dict(row)
    d["expertise"] = json.loads(d.get("expertise") or "[]")
    d["credentials"] = json.loads(d.get("credentials") or "[]")
    d["schema_json"] = json.loads(d.get("schema_json") or "{}")
    return d


def list_authors(business_id: str) -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM author_profiles WHERE business_id=? ORDER BY is_default DESC, name",
            [business_id]
        ).fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d["expertise"] = json.loads(d.get("expertise") or "[]")
        d["credentials"] = json.loads(d.get("credentials") or "[]")
        d["schema_json"] = json.loads(d.get("schema_json") or "{}")
        result.append(d)
    return result


def assign_author(business_id: str, content_url: str, author_id: str) -> bool:
    import hashlib as _h
    aid = _h.md5(f"{business_id}:{content_url}".encode()).hexdigest()[:12]
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as c:
        c.execute("""
            INSERT INTO author_assignments (id,business_id,content_url,author_id,assigned_at)
            VALUES (?,?,?,?,?)
            ON CONFLICT(business_id,content_url) DO UPDATE SET author_id=excluded.author_id, assigned_at=excluded.assigned_at
        """, [aid, business_id, content_url, author_id, now])
    return True


def get_author_for_url(business_id: str, content_url: str) -> dict:
    with _conn() as c:
        row = c.execute("""
            SELECT ap.* FROM author_profiles ap
            JOIN author_assignments aa ON ap.id = aa.author_id
            WHERE aa.business_id=? AND aa.content_url=?
        """, [business_id, content_url]).fetchone()
    if not row:
        return get_default_author(business_id)
    d = dict(row)
    d["expertise"] = json.loads(d.get("expertise") or "[]")
    d["credentials"] = json.loads(d.get("credentials") or "[]")
    d["schema_json"] = json.loads(d.get("schema_json") or "{}")
    return d


def inject_author_bio(html: str, author: dict, show_schema: bool = True) -> str:
    """Append an 'About the Author' block with schema markup to HTML content."""
    if not author or not author.get("name"):
        return html

    name = author["name"]
    title = author.get("title", "")
    bio = author.get("bio", "")
    photo_url = author.get("photo_url", "")
    linkedin = author.get("linkedin_url", "")
    twitter = author.get("twitter_url", "")
    credentials = author.get("credentials", [])

    photo_html = (
        f'<img src="{photo_url}" alt="{name}" class="author-photo" width="80" height="80" loading="lazy">'
        if photo_url else ""
    )
    title_html = f'<span class="author-title">{title}</span>' if title else ""
    cred_html = ""
    if credentials:
        cred_items = "".join(f"<li>{c}</li>" for c in credentials)
        cred_html = f'<ul class="author-credentials">{cred_items}</ul>'
    social_html = ""
    links = []
    if linkedin:
        links.append(f'<a href="{linkedin}" rel="noopener noreferrer" target="_blank">LinkedIn</a>')
    if twitter:
        links.append(f'<a href="{twitter}" rel="noopener noreferrer" target="_blank">Twitter/X</a>')
    if links:
        social_html = '<div class="author-social">' + " · ".join(links) + "</div>"

    schema_tag = ""
    if show_schema and author.get("schema_json"):
        schema_str = json.dumps(author["schema_json"])
        schema_tag = f'<script type="application/ld+json">{schema_str}</script>'

    bio_block = f"""
<div class="author-bio-block" itemscope itemtype="https://schema.org/Person">
  {photo_html}
  <div class="author-info">
    <strong itemprop="name">{name}</strong>
    {title_html}
    <p itemprop="description">{bio}</p>
    {cred_html}
    {social_html}
  </div>
</div>
{schema_tag}
""".strip()

    return html + "\n" + bio_block


def _build_person_schema(
    name: str, title: str, bio: str, expertise: list[str],
    credentials: list[str], photo_url: str,
    linkedin_url: str, twitter_url: str, website_url: str
) -> dict:
    schema: dict = {
        "@context": "https://schema.org",
        "@type": "Person",
        "name": name,
    }
    if title:
        schema["jobTitle"] = title
    if bio:
        schema["description"] = bio
    if photo_url:
        schema["image"] = photo_url
    if website_url:
        schema["url"] = website_url
    same_as = [u for u in [linkedin_url, twitter_url, website_url] if u]
    if same_as:
        schema["sameAs"] = same_as
    if expertise:
        schema["knowsAbout"] = expertise
    if credentials:
        schema["hasCredential"] = [{"@type": "EducationalOccupationalCredential", "name": c} for c in credentials]
    return schema


def auto_inject_author(html: str, business_id: str, content_url: str = "") -> str:
    """Resolve author for URL (or default) and inject bio block."""
    author = get_author_for_url(business_id, content_url) if content_url else get_default_author(business_id)
    if not author:
        log.debug("auto_inject_author  no author found  biz=%s", business_id)
        return html
    return inject_author_bio(html, author)
