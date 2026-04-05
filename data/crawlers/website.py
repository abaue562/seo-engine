"""Website crawler — extracts page-level SEO signals.

Crawls pages and extracts: title, h1, meta description, headings, word count,
internal links, schema markup presence.
"""

from __future__ import annotations

import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from pydantic import BaseModel

log = logging.getLogger(__name__)


class PageData(BaseModel):
    url: str
    title: str = ""
    h1: str = ""
    meta_description: str = ""
    headings: dict[str, list[str]] = {}   # h2: [...], h3: [...]
    word_count: int = 0
    internal_links: list[str] = []
    has_schema: bool = False
    status_code: int = 0


class CrawlResult(BaseModel):
    website: str
    pages: list[PageData] = []
    fetched_at: datetime = datetime.utcnow()


def _extract_page(url: str, html: str) -> PageData:
    """Parse a single page's HTML into structured SEO data."""
    soup = BeautifulSoup(html, "lxml")

    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    h1_tag = soup.find("h1")
    h1 = h1_tag.get_text(strip=True) if h1_tag else ""
    meta_tag = soup.find("meta", attrs={"name": "description"})
    meta_desc = meta_tag.get("content", "").strip() if meta_tag else ""

    headings: dict[str, list[str]] = {}
    for level in ("h2", "h3", "h4"):
        tags = soup.find_all(level)
        if tags:
            headings[level] = [t.get_text(strip=True) for t in tags]

    body_text = soup.get_text(separator=" ", strip=True)
    word_count = len(body_text.split())

    # Internal links
    base_domain = urlparse(url).netloc
    internal_links = []
    for a in soup.find_all("a", href=True):
        href = urljoin(url, a["href"])
        if urlparse(href).netloc == base_domain:
            internal_links.append(href)

    has_schema = bool(soup.find("script", attrs={"type": "application/ld+json"}))

    return PageData(
        url=url,
        title=title,
        h1=h1,
        meta_description=meta_desc,
        headings=headings,
        word_count=word_count,
        internal_links=list(set(internal_links)),
        has_schema=has_schema,
    )


async def crawl_page(url: str) -> PageData:
    """Crawl a single page."""
    async with httpx.AsyncClient(follow_redirects=True, timeout=15) as client:
        resp = await client.get(url)
        page = _extract_page(url, resp.text)
        page.status_code = resp.status_code
    return page


async def crawl_website(
    website: str,
    max_pages: int = 20,
) -> CrawlResult:
    """Crawl a website starting from homepage, following internal links."""
    if not website.startswith("http"):
        website = f"https://{website}"

    visited: set[str] = set()
    queue = [website]
    pages: list[PageData] = []

    async with httpx.AsyncClient(follow_redirects=True, timeout=15) as client:
        while queue and len(pages) < max_pages:
            url = queue.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                resp = await client.get(url)
                page = _extract_page(url, resp.text)
                page.status_code = resp.status_code
                pages.append(page)

                # Discover more internal pages
                for link in page.internal_links:
                    if link not in visited and len(queue) < max_pages * 2:
                        queue.append(link)

                log.debug("crawl.page  url=%s  words=%d  links=%d", url, page.word_count, len(page.internal_links))

            except Exception as e:
                log.warning("crawl.fail  url=%s  err=%s", url, e)

    log.info("crawl.done  site=%s  pages=%d", website, len(pages))
    return CrawlResult(website=website, pages=pages, fetched_at=datetime.utcnow())


def crawl_to_prompt_block(result: CrawlResult) -> str:
    """Render crawl results as agent context."""
    lines = [
        f"WEBSITE CRAWL ({result.website}):",
        f"Pages crawled: {len(result.pages)}",
        "",
    ]
    for p in result.pages:
        schema_flag = " [SCHEMA]" if p.has_schema else ""
        lines.append(f"  {p.url}")
        lines.append(f"    title: {p.title[:80]}")
        lines.append(f"    h1: {p.h1[:80]}")
        lines.append(f"    meta: {p.meta_description[:80]}")
        lines.append(f"    words: {p.word_count}  links: {len(p.internal_links)}{schema_flag}")

    return "\n".join(lines)
