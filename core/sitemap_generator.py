"""Sitemap auto-generation for all tenants.

Sources (in priority order):
  1. brand_entities.website            → homepage (priority 1.0)
  2. citation_pages (published_url)    → authority pages (priority 0.9)
  3. parasite_pages (published_url)    → parasite SEO pages (priority 0.8)
  4. keyword_rankings (your_url)       → tracked ranking pages (priority 0.7)
  5. data/storage/published_urls_{biz_id}.json  → published blog posts (priority 0.8)
  6. crawled_pages (url, same domain)  → discovered site pages (priority 0.6)

Output:
  data/storage/sitemaps/{business_id}/sitemap.xml       ← main sitemap
  data/storage/sitemaps/{business_id}/sitemap_index.xml ← index if >500 URLs
  data/storage/sitemaps/{business_id}/sitemap_news.xml  ← recent 48h content

Pings Google and Bing after generation.
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

log = logging.getLogger(__name__)

_DB_PATH = Path("data/storage/seo_engine.db")
_STORAGE = Path("data/storage")
_SITEMAP_DIR = _STORAGE / "sitemaps"
_UA = "SEOEngine-SitemapBot/1.0 (+https://gethubed.com/bot)"

_PING_URLS = [
    "https://www.google.com/ping?sitemap={url}",
    "https://www.bing.com/ping?sitemap={url}",
]


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _now() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")


def _normalise_url(url: str) -> str:
    url = url.strip()
    if not url.startswith("http"):
        url = "https://" + url
    return url.rstrip("/")


def _domain_of(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return ""


def _xml_bytes(root: Element) -> bytes:
    raw = tostring(root, encoding="unicode")
    return minidom.parseString(raw).toprettyxml(indent="  ", encoding="utf-8")


def _url_element(parent: Element, loc: str, lastmod: str,
                 changefreq: str = "weekly", priority: str = "0.7") -> None:
    url_el = SubElement(parent, "url")
    SubElement(url_el, "loc").text = loc
    SubElement(url_el, "lastmod").text = lastmod
    SubElement(url_el, "changefreq").text = changefreq
    SubElement(url_el, "priority").text = priority


def _ping(sitemap_url: str) -> None:
    for tmpl in _PING_URLS:
        ping = tmpl.format(url=urllib.parse.quote(sitemap_url, safe=""))
        try:
            req = urllib.request.Request(ping, headers={"User-Agent": _UA})
            urllib.request.urlopen(req, timeout=8)
            log.info("sitemap.ping_ok  url=%s", ping[:80])
        except Exception as exc:
            log.warning("sitemap.ping_fail  url=%s  err=%s", ping[:60], exc)


class SitemapGenerator:
    """Generate XML sitemaps for a business and optionally ping search engines."""

    CHUNK_SIZE = 500  # URLs per sitemap file before splitting into index

    def __init__(self, business_id: str):
        self.business_id = business_id
        self.out_dir = _SITEMAP_DIR / business_id
        self.out_dir.mkdir(parents=True, exist_ok=True)

    # ── Public API ─────────────────────────────────────────────────────────────

    def generate(self, ping: bool = True) -> dict:
        """Build sitemap(s) for this business. Returns summary dict."""
        entries = self._collect_entries()
        if not entries:
            log.info("sitemap.no_urls  biz=%s", self.business_id)
            return {"business_id": self.business_id, "urls": 0, "status": "no_urls"}

        # Deduplicate by URL
        seen: set[str] = set()
        deduped = []
        for e in entries:
            loc = e["loc"]
            if loc not in seen:
                seen.add(loc)
                deduped.append(e)

        # Recent entries for news sitemap (last 48h)
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=48)).strftime("%Y-%m-%d")
        recent = [e for e in deduped if e.get("lastmod", "") >= cutoff]

        files_written = []

        if len(deduped) <= self.CHUNK_SIZE:
            path = self._write_sitemap(deduped, "sitemap.xml")
            files_written.append(path)
        else:
            chunk_paths = []
            for i in range(0, len(deduped), self.CHUNK_SIZE):
                chunk = deduped[i:i + self.CHUNK_SIZE]
                name = f"sitemap_{i // self.CHUNK_SIZE + 1}.xml"
                p = self._write_sitemap(chunk, name)
                chunk_paths.append(p)
                files_written.append(p)
            index_path = self._write_index(chunk_paths)
            files_written.append(index_path)

        if recent:
            news_path = self._write_news_sitemap(recent)
            files_written.append(news_path)

        if ping:
            primary = self.out_dir / "sitemap.xml"
            index = self.out_dir / "sitemap_index.xml"
            target = index if index.exists() else primary
            if target.exists():
                _ping(self._public_url(target.name))

        log.info("sitemap.done  biz=%s  urls=%d  files=%d  recent=%d",
                 self.business_id, len(deduped), len(files_written), len(recent))
        return {
            "business_id": self.business_id,
            "urls": len(deduped),
            "files": len(files_written),
            "recent_urls": len(recent),
            "status": "ok",
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        }

    # ── URL collection ─────────────────────────────────────────────────────────

    def _collect_entries(self) -> list[dict]:
        entries: list[dict] = []
        website = self._get_website()

        # 1. Homepage
        if website:
            entries.append({"loc": _normalise_url(website), "lastmod": _now(),
                            "changefreq": "daily", "priority": "1.0"})

        # 2. Citation pages
        entries.extend(self._from_citation_pages())

        # 3. Parasite pages
        entries.extend(self._from_parasite_pages())

        # 4. Keyword ranking tracked URLs
        entries.extend(self._from_keyword_rankings(website))

        # 5. Published URLs JSON files
        entries.extend(self._from_published_json())

        # 6. Crawled pages on same domain
        if website:
            entries.extend(self._from_crawled_pages(website))

        return entries

    def _get_website(self) -> str:
        try:
            conn = _db()
            row = conn.execute(
                "SELECT website FROM brand_entities WHERE business_id=? LIMIT 1",
                [self.business_id]
            ).fetchone()
            conn.close()
            return row["website"] if row and row["website"] else ""
        except Exception:
            return ""

    def _from_citation_pages(self) -> list[dict]:
        try:
            conn = _db()
            rows = conn.execute(
                "SELECT published_url, updated_at FROM citation_pages "
                "WHERE business_id=? AND published_url IS NOT NULL AND published_url != ''",
                [self.business_id]
            ).fetchall()
            conn.close()
            return [{"loc": _normalise_url(r["published_url"]),
                     "lastmod": (r["updated_at"] or _now())[:10],
                     "changefreq": "monthly", "priority": "0.9"}
                    for r in rows]
        except Exception as exc:
            log.warning("sitemap.citation_pages_err  err=%s", exc)
            return []

    def _from_parasite_pages(self) -> list[dict]:
        try:
            conn = _db()
            rows = conn.execute(
                "SELECT published_url, updated_at FROM parasite_pages "
                "WHERE business_id=? AND published_url IS NOT NULL AND published_url != '' "
                "AND status='published'",
                [self.business_id]
            ).fetchall()
            conn.close()
            return [{"loc": _normalise_url(r["published_url"]),
                     "lastmod": (r["updated_at"] or _now())[:10],
                     "changefreq": "monthly", "priority": "0.8"}
                    for r in rows]
        except Exception as exc:
            log.warning("sitemap.parasite_pages_err  err=%s", exc)
            return []

    def _from_keyword_rankings(self, website: str) -> list[dict]:
        if not website:
            return []
        site_domain = _domain_of(website)
        try:
            conn = _db()
            rows = conn.execute(
                "SELECT DISTINCT your_url, MAX(checked_at) as last_seen "
                "FROM keyword_rankings WHERE business_id=? AND your_url IS NOT NULL "
                "GROUP BY your_url",
                [self.business_id]
            ).fetchall()
            conn.close()
            results = []
            for r in rows:
                url = r["your_url"]
                if _domain_of(url) == site_domain:
                    results.append({"loc": _normalise_url(url),
                                    "lastmod": (r["last_seen"] or _now())[:10],
                                    "changefreq": "weekly", "priority": "0.7"})
            return results
        except Exception as exc:
            log.warning("sitemap.keyword_rankings_err  err=%s", exc)
            return []

    def _from_published_json(self) -> list[dict]:
        results = []
        today = _now()
        # Try UUID-based filename first, then fall back to scanning all published_urls_*.json.
        # File naming is slug-based (e.g. published_urls_bbl.json) not UUID-based,
        # so we scan all files in the storage dir.
        for json_file in _STORAGE.glob("published_urls_*.json"):
            try:
                items = json.loads(json_file.read_text(encoding="utf-8"))
                if not isinstance(items, list):
                    continue
                for item in items:
                    url = item.get("url", "")
                    if url:
                        results.append({"loc": _normalise_url(url),
                                        "lastmod": today,
                                        "changefreq": "monthly", "priority": "0.8"})
            except Exception as exc:
                log.warning("sitemap.published_json_err  file=%s  err=%s", json_file, exc)
        return results

    def _from_crawled_pages(self, website: str) -> list[dict]:
        site_domain = _domain_of(website)
        if not site_domain:
            return []
        try:
            conn = _db()
            rows = conn.execute(
                "SELECT url, crawled_at FROM crawled_pages "
                "WHERE domain=? LIMIT 2000",
                [site_domain]
            ).fetchall()
            conn.close()
            return [{"loc": _normalise_url(r["url"]),
                     "lastmod": (r["crawled_at"] or _now())[:10],
                     "changefreq": "monthly", "priority": "0.6"}
                    for r in rows if r["url"]]
        except Exception as exc:
            log.warning("sitemap.crawled_pages_err  err=%s", exc)
            return []

    # ── XML writers ────────────────────────────────────────────────────────────

    def _write_sitemap(self, entries: list[dict], filename: str) -> Path:
        root = Element("urlset")
        root.set("xmlns", "http://www.sitemaps.org/schemas/sitemap/0.9")
        for e in entries:
            _url_element(root, e["loc"], e.get("lastmod", _now()),
                         e.get("changefreq", "weekly"), e.get("priority", "0.7"))
        path = self.out_dir / filename
        path.write_bytes(_xml_bytes(root))
        log.info("sitemap.written  file=%s  urls=%d", path, len(entries))
        return path

    def _write_index(self, sitemap_paths: list[Path]) -> Path:
        root = Element("sitemapindex")
        root.set("xmlns", "http://www.sitemaps.org/schemas/sitemap/0.9")
        for p in sitemap_paths:
            sm = SubElement(root, "sitemap")
            SubElement(sm, "loc").text = self._public_url(p.name)
            SubElement(sm, "lastmod").text = _now()
        path = self.out_dir / "sitemap_index.xml"
        path.write_bytes(_xml_bytes(root))
        log.info("sitemap.index_written  file=%s  sitemaps=%d", path, len(sitemap_paths))
        return path

    def _write_news_sitemap(self, entries: list[dict]) -> Path:
        root = Element("urlset")
        root.set("xmlns", "http://www.sitemaps.org/schemas/sitemap/0.9")
        root.set("xmlns:news", "http://www.google.com/schemas/sitemap-news/0.9")
        for e in entries:
            url_el = SubElement(root, "url")
            SubElement(url_el, "loc").text = e["loc"]
            news_el = SubElement(url_el, "news:news")
            pub_el = SubElement(news_el, "news:publication")
            SubElement(pub_el, "news:name").text = "GetHubed SEO"
            SubElement(pub_el, "news:language").text = "en"
            SubElement(news_el, "news:publication_date").text = e.get("lastmod", _now())
            SubElement(news_el, "news:title").text = e["loc"].rstrip("/").split("/")[-1].replace("-", " ").title()
        path = self.out_dir / "sitemap_news.xml"
        path.write_bytes(_xml_bytes(root))
        log.info("sitemap.news_written  file=%s  urls=%d", path, len(entries))
        return path

    def _public_url(self, filename: str) -> str:
        return f"https://gethubed.com/sitemaps/{self.business_id}/{filename}"


# ── Module-level convenience ───────────────────────────────────────────────────

def generate_all_sitemaps(ping: bool = True) -> list[dict]:
    """Generate sitemaps for every business in businesses.json."""
    biz_file = _STORAGE / "businesses.json"
    if not biz_file.exists():
        log.warning("sitemap.no_businesses_file")
        return []

    businesses = json.loads(biz_file.read_text(encoding="utf-8"))
    results = []
    for biz in businesses:
        biz_id = biz.get("id") or biz.get("business_id", "")
        if not biz_id:
            continue
        try:
            gen = SitemapGenerator(biz_id)
            result = gen.generate(ping=ping)
            results.append(result)
        except Exception as exc:
            log.exception("sitemap.generate_error  biz=%s", biz_id)
            results.append({"business_id": biz_id, "status": "error", "error": str(exc)})

    log.info("sitemap.all_done  businesses=%d", len(results))
    return results
