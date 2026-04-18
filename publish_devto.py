"""Publish BBL articles to Dev.to using API key.

Converts local HTML articles to Markdown, publishes via Dev.to API.
Tags chosen for SEO + developer/homeowner crossover audience.
Canonical URL points back to blendbrightlights.com to pass link equity.

Run: .venv/bin/python publish_devto.py
"""
import json
import logging
import os
import re
import time
from pathlib import Path
from html.parser import HTMLParser

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("publish_devto")

API_KEY  = os.getenv("DEVTO_API_KEY", "")
API_BASE = "https://dev.to/api"
ARTICLES_DIR = Path("data/storage/articles")
STORAGE      = Path("data/storage")
LOG_PATH     = STORAGE / "devto_published.json"

BBL_DOMAIN = "https://blendbrightlights.com"

TAGS_MAP = {
    "landscape lighting": ["homeimprovement", "led", "outdoor", "diy"],
    "gutter":             ["homeimprovement", "maintenance", "diy", "canada"],
    "moss":               ["homeimprovement", "roofing", "diy", "canada"],
    "window":             ["homeimprovement", "cleaning", "diy", "canada"],
    "roof":               ["homeimprovement", "roofing", "canada", "diy"],
    "exterior":           ["homeimprovement", "outdoor", "canada", "diy"],
    "led":                ["led", "lighting", "homeimprovement", "outdoor"],
    "lighting":           ["lighting", "led", "homeimprovement", "outdoor"],
}

def _default_tags():
    return ["homeimprovement", "diy", "canada", "outdoor"]


class HTMLToMarkdown(HTMLParser):
    """Simple HTML → Markdown converter for article bodies."""
    def __init__(self):
        super().__init__()
        self.lines = []
        self.skip = False
        self._list_depth = 0
        self._in_li = False

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag in ("script", "style"):
            self.skip = True
        elif tag == "h1":
            self.lines.append("\n# ")
        elif tag == "h2":
            self.lines.append("\n## ")
        elif tag == "h3":
            self.lines.append("\n### ")
        elif tag == "h4":
            self.lines.append("\n#### ")
        elif tag == "p":
            self.lines.append("\n\n")
        elif tag in ("ul", "ol"):
            self._list_depth += 1
        elif tag == "li":
            self._in_li = True
            self.lines.append("\n- ")
        elif tag == "strong" or tag == "b":
            self.lines.append("**")
        elif tag == "em" or tag == "i":
            self.lines.append("*")
        elif tag == "a":
            href = attrs_dict.get("href", "")
            self.lines.append(f"[")
            self._pending_href = href
        elif tag == "br":
            self.lines.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.skip = False
        elif tag in ("h1", "h2", "h3", "h4"):
            self.lines.append("\n")
        elif tag in ("ul", "ol"):
            self._list_depth = max(0, self._list_depth - 1)
        elif tag == "li":
            self._in_li = False
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
        # Clean up excessive blank lines
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()


def html_to_markdown(html: str) -> str:
    p = HTMLToMarkdown()
    p.feed(html)
    return p.result()


def get_tags(title: str, keyword: str) -> list:
    combined = (title + " " + keyword).lower()
    for k, tags in TAGS_MAP.items():
        if k in combined:
            return tags
    return _default_tags()


def load_published_log() -> dict:
    if LOG_PATH.exists():
        try:
            return json.loads(LOG_PATH.read_text())
        except Exception:
            pass
    return {}


def save_published_log(log_data: dict):
    LOG_PATH.write_text(json.dumps(log_data, indent=2))


def publish_article(title: str, markdown: str, tags: list,
                    canonical_url: str = "", series: str = "") -> dict:
    """POST to Dev.to API. Returns response dict."""
    payload = {
        "article": {
            "title": title,
            "body_markdown": markdown,
            "published": True,
            "tags": tags[:4],
        }
    }
    if canonical_url:
        payload["article"]["canonical_url"] = canonical_url
    if series:
        payload["article"]["series"] = series

    r = requests.post(
        f"{API_BASE}/articles",
        headers={"api-key": API_KEY, "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    return {"status": r.status_code, "data": r.json()}


def run():
    if not API_KEY:
        log.error("DEVTO_API_KEY not set in environment")
        return

    published_log = load_published_log()
    html_files = sorted(ARTICLES_DIR.glob("bbl_*.html"))

    if not html_files:
        log.error("No BBL HTML articles found in %s", ARTICLES_DIR)
        return

    log.info("Found %d articles to publish", len(html_files))
    results = []

    for html_file in html_files:
        slug = html_file.stem  # e.g. bbl_landscape_lighting_kelowna_bc

        if slug in published_log:
            log.info("skip (already published): %s → %s",
                     slug, published_log[slug].get("url"))
            continue

        html = html_file.read_text(encoding="utf-8")

        # Extract title from H1
        title_match = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.IGNORECASE | re.DOTALL)
        title = re.sub(r"<[^>]+>", "", title_match.group(1)).strip() if title_match else slug.replace("_", " ").title()

        markdown = html_to_markdown(html)

        # Add attribution footer
        markdown += f"\n\n---\n*Professional services by [BlendBright Lights]({BBL_DOMAIN}) — Kelowna, BC*"

        # Keyword from filename
        keyword = slug.replace("bbl_", "").replace("_", " ")
        tags = get_tags(title, keyword)

        # Canonical URL — point to BBL domain page
        canonical_slug = slug.replace("bbl_", "").replace("_", "-")
        canonical_url = f"{BBL_DOMAIN}/{canonical_slug}/"

        log.info("Publishing: %s", title[:60])

        try:
            resp = publish_article(
                title=title,
                markdown=markdown,
                tags=tags,
                canonical_url=canonical_url,
                series="Kelowna Home Services Guide",
            )
            if resp["status"] in (200, 201):
                article_url = resp["data"].get("url", "")
                published_log[slug] = {
                    "title": title,
                    "url": article_url,
                    "devto_id": resp["data"].get("id"),
                    "canonical": canonical_url,
                    "published_at": resp["data"].get("published_at", ""),
                }
                save_published_log(published_log)
                log.info("✓ Published: %s → %s", title[:50], article_url)
                results.append({"slug": slug, "url": article_url, "status": "ok"})
            else:
                err = resp["data"].get("error", str(resp["data"]))
                log.error("✗ Failed: %s | status=%d | err=%s", title[:50], resp["status"], err)
                results.append({"slug": slug, "status": "error", "error": err})
        except Exception as e:
            log.exception("Exception publishing %s", slug)
            results.append({"slug": slug, "status": "error", "error": str(e)})

        # Rate limit — Dev.to allows ~10 req/min
        time.sleep(7)

    log.info("Done. Published: %d / %d",
             sum(1 for r in results if r.get("status") == "ok"), len(results))
    return results


if __name__ == "__main__":
    run()
