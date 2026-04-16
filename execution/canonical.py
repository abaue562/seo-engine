"""Canonical URL registry and cross-platform canonical enforcement.

Every piece of content published by the engine must be registered here.
Enforces that syndicated copies on Medium/Blogger/WP.com point canonical
back to the primary WordPress URL.

SimHash duplicate detection prevents near-duplicate content from being published.
"""

from __future__ import annotations

import hashlib
import logging
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data.db import SEODatabase

log = logging.getLogger(__name__)


class SimHashDuplicate(Exception):
    """Raised when content is too similar to an already-published piece."""
    def __init__(self, existing_url: str, similarity: float):
        super().__init__(f"Content {similarity:.0%} similar to {existing_url}")
        self.existing_url = existing_url
        self.similarity = similarity


def inject_canonical_meta(html: str, canonical_url: str) -> str:
    """Inject <link rel='canonical'> into <head> or prepend to HTML."""
    tag = f'<link rel="canonical" href="{canonical_url}">'
    if re.search(r"<head[^>]*>", html, re.IGNORECASE):
        return re.sub(
            r"(<head[^>]*>)",
            r"\1\n  " + tag,
            html,
            count=1,
            flags=re.IGNORECASE,
        )
    return tag + "\n" + html


class CanonicalRegistry:
    """Manages canonical URLs and duplicate detection across all published content."""

    def __init__(self, db: "SEODatabase" = None):
        self.db = db

    def register(
        self,
        business_id: str,
        primary_url: str,
        slug: str,
        keyword: str,
        platform: str = "wordpress",
    ) -> int:
        """Register the primary (canonical) URL for a piece of content."""
        if not self.db:
            log.warning("canonical.register  no db configured")
            return -1
        url_id = self.db.register_url(
            business_id=business_id,
            url=primary_url,
            platform=platform,
            canonical_url=primary_url,
            slug=slug,
            keyword=keyword,
        )
        log.info("canonical.registered  url=%s  id=%d", primary_url, url_id)
        return url_id

    def register_syndication(
        self,
        primary_url: str,
        syndicated_url: str,
        platform: str,
    ):
        """Register a syndicated copy pointing canonical to the primary URL."""
        if not self.db:
            return
        self.db.register_syndication(
            primary_url=primary_url,
            syndicated_url=syndicated_url,
            platform=platform,
            canonical_url=primary_url,
        )
        log.info(
            "canonical.syndication_registered  platform=%s  syndicated=%s  canonical=%s",
            platform, syndicated_url, primary_url,
        )

    def get_canonical(self, slug: str) -> str | None:
        """Return the primary canonical URL for a slug."""
        if not self.db:
            return None
        row = self.db.get_url_by_slug(slug)
        return row["canonical_url"] if row else None

    def get_all_syndications(self, primary_url: str) -> list[dict]:
        """List all syndicated copies for a primary URL."""
        if not self.db:
            return []
        return self.db.get_syndications(primary_url)

    def is_duplicate(self, content_html: str, threshold: float = 0.85) -> bool:
        """SimHash-based duplicate detection."""
        if not self.db:
            return False
        h = self._simhash(_strip_html(content_html))
        return self.db.simhash_exists(h, threshold)

    def store_hash(self, url_id: int, content_html: str):
        """Compute and store SimHash for a published URL."""
        if not self.db:
            return
        h = self._simhash(_strip_html(content_html))
        self.db.save_content_hash(url_id, h)

    def enforce_canonical_on_medium(
        self, post_html: str, canonical_url: str
    ) -> str:
        """Inject canonical link into Medium post HTML before publishing.

        Medium supports canonical URLs via their API's canonicalUrl field.
        This function also injects it into the HTML as a fallback.
        """
        return inject_canonical_meta(post_html, canonical_url)

    def generate_url_report(self, business_id: str) -> dict:
        """Summary of URL health for a business."""
        if not self.db:
            return {}
        urls = self.db.get_urls_by_business(business_id)
        orphans = self.db.get_orphan_urls(business_id)
        platforms: dict = {}
        for u in urls:
            p = u.get("platform", "unknown")
            platforms[p] = platforms.get(p, 0) + 1
        return {
            "total_urls": len(urls),
            "platforms": platforms,
            "orphan_count": len(orphans),
            "orphan_urls": [o["url"] for o in orphans[:10]],
        }

    # ----------------------------------------------------------------
    # SimHash implementation (stdlib only)
    # ----------------------------------------------------------------

    def _simhash(self, text: str) -> int:
        """Compute a 64-bit SimHash of text."""
        tokens = self._trigrams(text)
        if not tokens:
            return 0

        v = [0] * 64
        for token in tokens:
            h = int(hashlib.md5(token.encode()).hexdigest(), 16)
            for i in range(64):
                bit = (h >> i) & 1
                v[i] += 1 if bit else -1

        fingerprint = 0
        for i in range(64):
            if v[i] > 0:
                fingerprint |= 1 << i
        return fingerprint

    def _trigrams(self, text: str) -> list[str]:
        """Generate character 3-grams from text."""
        text = text.lower()
        words = re.findall(r"[a-z0-9]+", text)
        joined = " ".join(words)
        return [joined[i : i + 3] for i in range(len(joined) - 2)]

    @staticmethod
    def hamming_distance(h1: int, h2: int) -> int:
        """Count differing bits between two SimHash values."""
        return bin(h1 ^ h2).count("1")


def _strip_html(html: str) -> str:
    """Remove HTML tags and return plain text."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)
    return text.strip()
