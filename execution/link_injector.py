"""WordPress Internal Link Injector.

After a new page is published, this module:
  1. Fetches the N most recent existing posts from WordPress.
  2. Scores each post for keyword relevance to the new page.
  3. For posts above the relevance threshold, finds the best anchor-text
     occurrence and PATCHes the post content via WordPress REST API to add
     a link to the new page.
  4. Also patches the new post itself with links to related existing posts
     (using the {{LINK:anchor text:relative/path}} placeholders injected by Claude).

Anchor text variation
---------------------
To avoid over-optimising exact-match anchors (Penguin risk), the injector
maintains an anchor text distribution target:

  branded     40%   — business name or brand variant
  partial     35%   — 1-2 words from the keyword
  exact       15%   — full keyword phrase
  generic     10%   — "learn more", "read more", "click here", etc.

The injector picks a variant type based on the current distribution in the
rank registry so that exact-match anchors never exceed the threshold.

Environment variables consumed:
  WP_URL, WP_USER, WP_APP_PASSWORD
  LINK_INJECT_MAX_POSTS            (default 20)
  LINK_INJECT_RELEVANCE_THRESHOLD  (default 0.3)
  LINK_ANCHOR_EXACT_MAX_PCT        (default 0.15)
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
from pathlib import Path
from typing import Any

import httpx

log = logging.getLogger(__name__)

_WP_URL   = lambda: os.getenv("WP_URL", "").rstrip("/")
_WP_AUTH  = lambda: (os.getenv("WP_USER", ""), os.getenv("WP_APP_PASSWORD", ""))
_MAX_POSTS  = lambda: int(os.getenv("LINK_INJECT_MAX_POSTS", "20"))
_THRESHOLD  = lambda: float(os.getenv("LINK_INJECT_RELEVANCE_THRESHOLD", "0.3"))
_EXACT_MAX  = lambda: float(os.getenv("LINK_ANCHOR_EXACT_MAX_PCT", "0.15"))

# HTML link pattern — detect if anchor already linked
_HREF_RE = re.compile(r'href=["\'][^"\']*["\']', re.IGNORECASE)

# Anchor text type registry path
_ANCHOR_LOG = Path("data/storage/anchor_distribution.json")

# Generic anchor variants
_GENERIC_ANCHORS = [
    "learn more", "find out more", "read more", "see details",
    "click here", "visit page", "explore options", "get started",
    "see how", "discover more",
]


class LinkInjector:
    """Injects internal links into a WordPress site after a new page is published."""

    def __init__(self):
        self.api  = f"{_WP_URL()}/wp-json/wp/v2"
        self.auth = _WP_AUTH()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def inject(
        self,
        new_url: str,
        new_keyword: str,
        new_post_id: str,
        page_data: dict,
        *,
        business_name: str = "",
    ) -> dict:
        """Run the full injection flow for a newly published page.

        Args:
            new_url:       Live URL of the new page.
            new_keyword:   Primary keyword of the new page.
            new_post_id:   WordPress post ID of the new page (string or int).
            page_data:     Full page dict from generate_content (may contain
                           {{LINK:anchor:path}} placeholders in content_html).
            business_name: Used for branded anchor text variants.

        Returns:
            dict: {links_injected: int, pages_updated: list[str],
                   anchor_distribution: dict}
        """
        if not _WP_URL() or not self.auth[0]:
            log.warning("link_injector.skip  reason=no WP credentials")
            return {"links_injected": 0, "pages_updated": [], "anchor_distribution": {}}

        pages_updated: list[str] = []
        links_injected = 0
        anchor_types_used: list[str] = []

        # Step 1: Resolve placeholders in the new page itself
        if new_post_id and page_data.get("content_html"):
            resolved, count = await self._resolve_placeholders(
                post_id=new_post_id,
                content=page_data["content_html"],
            )
            links_injected += count
            if count > 0:
                pages_updated.append(new_url)

        # Step 2: Find existing posts that should link to the new page
        existing_posts = await self._fetch_posts(limit=_MAX_POSTS())
        keyword_tokens = set(new_keyword.lower().split())

        # Load current anchor distribution to decide variant type
        distribution = self._load_anchor_distribution()

        for post in existing_posts:
            pid = str(post.get("id", ""))
            if pid == str(new_post_id):
                continue  # skip the new page itself

            post_content = post.get("content", {}).get("rendered", "")
            post_url     = post.get("link", "")

            score = self._relevance_score(post_content, keyword_tokens)
            if score < _THRESHOLD():
                continue

            # Choose anchor text variant based on distribution targets
            anchor_type, anchor = self._choose_anchor(
                content=post_content,
                keyword=new_keyword,
                business_name=business_name,
                distribution=distribution,
            )
            if not anchor:
                continue

            # Only inject if anchor not already a link
            patched, injected = self._inject_link(post_content, anchor, new_url)
            if not injected:
                continue

            success = await self._patch_post(pid, patched)
            if success:
                links_injected += 1
                pages_updated.append(post_url)
                anchor_types_used.append(anchor_type)
                distribution[anchor_type] = distribution.get(anchor_type, 0) + 1
                log.info(
                    "link_injector.injected  post_id=%s  anchor=%r  type=%s  target=%s",
                    pid, anchor[:40], anchor_type, new_url,
                )

        # Persist updated distribution
        if anchor_types_used:
            self._save_anchor_distribution(distribution)

        log.info(
            "link_injector.done  new_url=%s  links_injected=%d  pages_updated=%d",
            new_url, links_injected, len(pages_updated),
        )
        return {
            "links_injected":       links_injected,
            "pages_updated":        pages_updated,
            "anchor_distribution":  distribution,
        }

    # ------------------------------------------------------------------
    # Anchor text variation
    # ------------------------------------------------------------------

    def _choose_anchor(
        self,
        content: str,
        keyword: str,
        business_name: str,
        distribution: dict[str, int],
    ) -> tuple[str, str]:
        """Pick anchor type + text based on distribution targets.

        Returns:
            (anchor_type, anchor_text)  — anchor_text is empty string on failure.
        """
        total = max(sum(distribution.values()), 1)
        exact_pct    = distribution.get("exact", 0)    / total
        branded_pct  = distribution.get("branded", 0)  / total
        partial_pct  = distribution.get("partial", 0)  / total
        generic_pct  = distribution.get("generic", 0)  / total

        # Build candidate types in priority order based on how far below target each is
        targets = {
            "branded": 0.40,
            "partial": 0.35,
            "exact":   _EXACT_MAX(),
            "generic": 0.10,
        }
        current = {
            "branded": branded_pct,
            "partial": partial_pct,
            "exact":   exact_pct,
            "generic": generic_pct,
        }
        # Sort by largest deficit first
        ordered = sorted(targets.keys(), key=lambda t: targets[t] - current[t], reverse=True)

        plain = re.sub(r'<[^>]+>', ' ', content)

        for anchor_type in ordered:
            if anchor_type == "exact":
                if exact_pct >= _EXACT_MAX():
                    continue  # cap reached — skip exact
                anchor = self._find_best_anchor(content, keyword)
                if anchor:
                    return "exact", anchor

            elif anchor_type == "branded":
                if not business_name:
                    continue
                # Look for business name or first word of business name in content
                for brand_variant in _brand_variants(business_name):
                    m = re.search(re.escape(brand_variant), plain, re.IGNORECASE)
                    if m:
                        return "branded", m.group(0)

            elif anchor_type == "partial":
                # Use 1-2 content words from the keyword
                tokens = [t for t in keyword.split() if len(t) >= 4]
                random.shuffle(tokens)
                for token in tokens[:3]:
                    m = re.search(r'\b' + re.escape(token) + r'\b', plain, re.IGNORECASE)
                    if m:
                        return "partial", m.group(0)

            elif anchor_type == "generic":
                # Check if any generic phrase appears in the text (unlikely but correct)
                # Otherwise use any generic anchor with surrounding context
                for g in random.sample(_GENERIC_ANCHORS, min(3, len(_GENERIC_ANCHORS))):
                    m = re.search(r'\b' + re.escape(g) + r'\b', plain, re.IGNORECASE)
                    if m:
                        return "generic", m.group(0)
                # If no generic phrase found in text, fall through to exact as last resort
                anchor = self._find_best_anchor(content, keyword)
                if anchor:
                    return "generic", anchor

        return "", ""

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fetch_posts(self, limit: int = 20) -> list[dict]:
        """Fetch the most recent published posts from WordPress."""
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.get(
                    f"{self.api}/posts",
                    auth=self.auth,
                    params={
                        "per_page": min(limit, 100),
                        "status":   "publish",
                        "_fields":  "id,link,content,title",
                        "orderby":  "date",
                        "order":    "desc",
                    },
                )
                resp.raise_for_status()
                posts = resp.json()
                log.debug("link_injector.fetched_posts  count=%d", len(posts))
                return posts
        except Exception as e:
            log.warning("link_injector.fetch_fail  err=%s", e)
            return []

    async def _resolve_placeholders(self, post_id: str, content: str) -> tuple[str, int]:
        """Replace {{LINK:anchor text:relative/path}} placeholders with real <a> tags.

        Returns updated content and count of resolved placeholders.
        """
        base = _WP_URL()
        pattern = re.compile(r'\{\{LINK:([^:}]+):([^}]+)\}\}')
        count = 0

        def replacer(m: re.Match) -> str:
            nonlocal count
            anchor = m.group(1).strip()
            path   = m.group(2).strip().lstrip("/")
            href   = f"{base}/{path}"
            count += 1
            return f'<a href="{href}">{anchor}</a>'

        new_content = pattern.sub(replacer, content)

        if count > 0:
            await self._patch_post(post_id, new_content)

        return new_content, count

    async def _patch_post(self, post_id: str, new_content: str) -> bool:
        """PATCH a WordPress post's content via REST API."""
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.patch(
                    f"{self.api}/posts/{post_id}",
                    auth=self.auth,
                    json={"content": new_content},
                )
                resp.raise_for_status()
                return True
        except Exception as e:
            log.warning("link_injector.patch_fail  post_id=%s  err=%s", post_id, e)
            return False

    @staticmethod
    def _relevance_score(content: str, keyword_tokens: set[str]) -> float:
        """Score 0-1 based on what fraction of keyword tokens appear in content."""
        if not keyword_tokens:
            return 0.0
        text = re.sub(r'<[^>]+>', ' ', content).lower()
        hits = sum(1 for t in keyword_tokens if t in text)
        return hits / len(keyword_tokens)

    @staticmethod
    def _find_best_anchor(content: str, keyword: str) -> str:
        """Find the best anchor text occurrence in the post content.

        Strategy:
        - Try exact keyword match first.
        - Fall back to matching the longest single word from the keyword.

        Returns the matched text or empty string if nothing found.
        """
        plain = re.sub(r'<[^>]+>', ' ', content)

        # Exact keyword (case-insensitive)
        m = re.search(re.escape(keyword), plain, re.IGNORECASE)
        if m:
            return m.group(0)

        # Partial: longest word in keyword that appears in text
        tokens = sorted(keyword.split(), key=len, reverse=True)
        for token in tokens:
            if len(token) < 4:
                continue
            m = re.search(r'\b' + re.escape(token) + r'\b', plain, re.IGNORECASE)
            if m:
                return m.group(0)

        return ""

    @staticmethod
    def _inject_link(content: str, anchor: str, target_url: str) -> tuple[str, bool]:
        """Replace the FIRST occurrence of anchor in content with an <a> tag.

        Skips if the anchor is already inside an <a> tag.

        Returns:
            (updated_content, was_injected: bool)
        """
        pattern = re.compile(
            r'(?<!["\'>])(' + re.escape(anchor) + r')(?![^<]*>)',
            re.IGNORECASE,
        )
        new_content, n = pattern.subn(
            lambda m: f'<a href="{target_url}">{m.group(1)}</a>',
            content,
            count=1,
        )
        return new_content, n > 0

    # ------------------------------------------------------------------
    # Anchor distribution persistence
    # ------------------------------------------------------------------

    @staticmethod
    def _load_anchor_distribution() -> dict[str, int]:
        try:
            if _ANCHOR_LOG.exists():
                return json.loads(_ANCHOR_LOG.read_text(encoding="utf-8"))
        except Exception:
            pass
        return {"exact": 0, "branded": 0, "partial": 0, "generic": 0}

    @staticmethod
    def _save_anchor_distribution(dist: dict[str, int]) -> None:
        try:
            _ANCHOR_LOG.parent.mkdir(parents=True, exist_ok=True)
            _ANCHOR_LOG.write_text(json.dumps(dist, indent=2), encoding="utf-8")
        except Exception as e:
            log.warning("link_injector.save_dist_fail  err=%s", e)

    def get_anchor_report(self) -> dict:
        """Return current anchor text distribution as percentages."""
        dist = self._load_anchor_distribution()
        total = max(sum(dist.values()), 1)
        return {
            k: {"count": v, "pct": round(v / total * 100, 1)}
            for k, v in dist.items()
        }


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _brand_variants(business_name: str) -> list[str]:
    """Return branded anchor text candidates from a business name."""
    variants = [business_name]
    words = business_name.split()
    if len(words) >= 2:
        variants.append(words[0])              # first word only
        variants.append(" ".join(words[:2]))   # first two words
    return variants
