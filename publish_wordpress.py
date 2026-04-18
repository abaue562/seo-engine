"""Publish BBL articles to self-hosted WordPress via REST API.

Run: cd /opt/seo-engine && .venv/bin/python publish_wordpress.py
"""
import json
import logging
import os
import re
import time
from pathlib import Path
from html.parser import HTMLParser

import requests
from requests.auth import HTTPBasicAuth

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("publish_wp")

WP_URL      = os.getenv("WP_URL", "http://204.168.184.50:8910")
WP_USER     = os.getenv("WP_USER", "abaue562")
WP_PASS     = os.getenv("WP_APP_PASSWORD", "")
ARTICLES_DIR = Path("data/storage/articles")
LOG_PATH     = Path("data/storage/wordpress_published.json")

BBL_DOMAIN = "https://blendbrightlights.com"


class HTMLToMarkdown(HTMLParser):
    def __init__(self):
        super().__init__()
        self.lines = []
        self.skip = False

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag in ("script", "style"):
            self.skip = True
        elif tag == "h2":
            self.lines.append("\n## ")
        elif tag == "h3":
            self.lines.append("\n### ")
        elif tag == "p":
            self.lines.append("\n\n")
        elif tag == "li":
            self.lines.append("\n- ")
        elif tag in ("strong", "b"):
            self.lines.append("**")
        elif tag in ("em", "i"):
            self.lines.append("*")
        elif tag == "a":
            href = attrs_dict.get("href", "")
            self.lines.append("[")
            self._pending_href = href
        elif tag == "br":
            self.lines.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.skip = False
        elif tag in ("h2", "h3"):
            self.lines.append("\n")
        elif tag in ("strong", "b"):
            self.lines.append("**")
        elif tag in ("em", "i"):
            self.lines.append("*")
        elif tag == "a":
            href = getattr(self, "_pending_href", "")
            self.lines.append(f"]({href})")
            self._pending_href = ""

    def handle_data(self, data):
        if not self.skip:
            self.lines.append(data)

    def result(self):
        text = "".join(self.lines)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()


def html_to_content(html: str) -> str:
    p = HTMLToMarkdown()
    p.feed(html)
    return p.result()


def load_log() -> dict:
    if LOG_PATH.exists():
        try:
            return json.loads(LOG_PATH.read_text())
        except Exception:
            pass
    return {}


def save_log(data: dict):
    LOG_PATH.write_text(json.dumps(data, indent=2))


def publish_post(title: str, content: str, slug: str, canonical_url: str) -> dict:
    auth = HTTPBasicAuth(WP_USER, WP_PASS)
    payload = {
        "title": title,
        "content": content,
        "status": "publish",
        "slug": slug,
        "meta": {"_yoast_wpseo_canonical": canonical_url},
    }
    r = requests.post(
        f"{WP_URL}/wp-json/wp/v2/posts",
        auth=auth,
        json=payload,
        timeout=30,
    )
    return {"status": r.status_code, "data": r.json()}


def run():
    if not WP_PASS:
        log.error("WP_APP_PASSWORD not set")
        return

    published = load_log()
    html_files = sorted(ARTICLES_DIR.glob("bbl_*.html"))

    if not html_files:
        log.error("No BBL HTML articles in %s", ARTICLES_DIR)
        return

    log.info("Found %d articles", len(html_files))
    results = []

    for html_file in html_files:
        slug = html_file.stem

        if slug in published:
            log.info("skip (already published): %s → %s", slug, published[slug].get("url"))
            continue

        html = html_file.read_text(encoding="utf-8")
        title_match = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.IGNORECASE | re.DOTALL)
        title = re.sub(r"<[^>]+>", "", title_match.group(1)).strip() if title_match else slug.replace("_", " ").title()

        content = html_to_content(html)
        content += f"\n\n---\n*Professional services by [BlendBright Lights]({BBL_DOMAIN}) — Kelowna, BC*"

        wp_slug = slug.replace("bbl_", "").replace("_", "-")
        canonical_url = f"{BBL_DOMAIN}/{wp_slug}/"

        log.info("Publishing: %s", title[:60])

        try:
            resp = publish_post(title=title, content=content, slug=wp_slug, canonical_url=canonical_url)
            if resp["status"] in (200, 201):
                post_url = resp["data"].get("link", "")
                published[slug] = {
                    "title": title,
                    "url": post_url,
                    "wp_id": resp["data"].get("id"),
                    "canonical": canonical_url,
                }
                save_log(published)
                log.info("✓ Published: %s → %s", title[:50], post_url)
                results.append({"slug": slug, "url": post_url, "status": "ok"})
            else:
                err = resp["data"].get("message", str(resp["data"]))
                log.error("✗ Failed: %s | status=%d | err=%s", title[:50], resp["status"], err)
                results.append({"slug": slug, "status": "error", "error": err})
        except Exception as e:
            log.exception("Exception publishing %s", slug)
            results.append({"slug": slug, "status": "error", "error": str(e)})

        time.sleep(2)

    ok = sum(1 for r in results if r.get("status") == "ok")
    log.info("Done. Published %d / %d", ok, len(results))
    return results


if __name__ == "__main__":
    run()
