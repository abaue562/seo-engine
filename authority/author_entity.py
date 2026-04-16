"""E-E-A-T Author Entity Manager.

Creates and maintains author entity profiles for all generated content.
Author entities are Google's primary E-E-A-T signal at the page level.

What this module does:
  1. Creates author profiles (name, bio, credentials, photo, social links)
  2. Generates Person schema markup for each author
  3. Creates/updates author bio pages on WordPress
  4. Injects author attribution into generated content HTML
  5. Maintains a Wikidata-style entity registry
  6. Enforces consistent attribution across all publishing channels

E-E-A-T signals managed:
  - Experience: Years of experience, service count, case studies
  - Expertise:  Credentials, certifications, licenses, specialisations
  - Authority:  Mentions, backlinks to author profile, social presence
  - Trust:      NAP consistency, verified reviews, guarantee statements

Usage:
    manager = AuthorEntityManager()
    author = manager.get_or_create_author(business)
    enriched_html = manager.inject_author_bio(content_html, author)
    schema = manager.build_person_schema(author)
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_AUTHORS_DIR = Path("data/storage/authors")


@dataclass
class AuthorProfile:
    id:             str
    name:           str
    slug:           str
    title:          str                      # e.g. "Master Plumber & Owner"
    bio:            str                      # 150-200 char bio
    bio_long:       str = ""                 # 400-600 char full bio for author page
    credentials:    list[str] = field(default_factory=list)  # ["Licensed Master Plumber", "EPA Certified"]
    years_exp:      int = 0
    service_count:  int = 0
    city:           str = ""
    phone:          str = ""
    email:          str = ""
    website:        str = ""
    linkedin:       str = ""
    google_profile: str = ""
    twitter:        str = ""
    photo_url:      str = ""
    wp_author_id:   int = 0                  # WordPress user ID after creation
    wikidata_id:    str = ""
    same_as:        list[str] = field(default_factory=list)  # Entity sameAs URLs
    business_id:    str = ""
    created_at:     str = ""
    updated_at:     str = ""


class AuthorEntityManager:
    """Creates and manages author entity profiles for E-E-A-T optimisation."""

    def __init__(self):
        _AUTHORS_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def get_or_create_author(self, business: dict | Any) -> AuthorProfile:
        """Get existing author for a business, or generate one via Claude.

        Args:
            business: BusinessContext instance or dict with business data.

        Returns:
            AuthorProfile ready for content injection.
        """
        biz = business if isinstance(business, dict) else (
            business.model_dump() if hasattr(business, "model_dump") else vars(business)
        )
        business_id = biz.get("id", biz.get("business_id", "unknown"))

        existing = self._load_author(business_id)
        if existing:
            log.debug("author_entity.loaded  business_id=%s  name=%s", business_id, existing.name)
            return existing

        author = self._generate_author(biz)
        self._save_author(author)
        log.info("author_entity.created  business_id=%s  name=%s", business_id, author.name)
        return author

    def inject_author_bio(
        self,
        content_html: str,
        author: AuthorProfile,
        *,
        position: str = "bottom",    # "top" | "bottom"
    ) -> str:
        """Inject author bio box into content HTML.

        Args:
            content_html: HTML body to inject into.
            author:       AuthorProfile to inject.
            position:     "top" = after H1, "bottom" = end of content.

        Returns:
            HTML with author bio injected.
        """
        bio_html = _build_author_bio_html(author)
        if position == "top":
            # Inject after the first </h1> or </h2>
            import re
            m = re.search(r'</h[12]>', content_html, re.IGNORECASE)
            if m:
                idx = m.end()
                return content_html[:idx] + bio_html + content_html[idx:]
        return content_html + bio_html

    def build_person_schema(self, author: AuthorProfile) -> dict:
        """Generate JSON-LD Person schema for an author.

        Returns:
            JSON-LD schema dict ready to inject via WordPress connector.
        """
        schema: dict[str, Any] = {
            "@context": "https://schema.org",
            "@type":    "Person",
            "name":     author.name,
            "jobTitle": author.title,
            "description": author.bio,
            "worksFor": {
                "@type": "LocalBusiness",
                "name":  author.name.split("—")[0].strip() if "—" in author.name else author.name,
                "url":   author.website,
            },
        }
        if author.website:
            schema["url"] = author.website
        if author.photo_url:
            schema["image"] = author.photo_url
        if author.email:
            schema["email"] = author.email
        if author.phone:
            schema["telephone"] = author.phone
        if author.linkedin:
            schema.setdefault("sameAs", []).append(author.linkedin)
        if author.twitter:
            schema.setdefault("sameAs", []).append(author.twitter)
        if author.google_profile:
            schema.setdefault("sameAs", []).append(author.google_profile)
        if author.same_as:
            schema.setdefault("sameAs", []).extend(author.same_as)
        if author.credentials:
            schema["hasCredential"] = [
                {"@type": "EducationalOccupationalCredential", "name": c}
                for c in author.credentials
            ]
        if author.wikidata_id:
            schema["sameAs"] = schema.get("sameAs", []) + [
                f"https://www.wikidata.org/wiki/{author.wikidata_id}"
            ]

        # Deduplicate sameAs
        if "sameAs" in schema:
            schema["sameAs"] = list(dict.fromkeys(schema["sameAs"]))

        return schema

    async def create_wp_author(
        self,
        author: AuthorProfile,
        wp_url: str,
        wp_user: str,
        wp_password: str,
    ) -> int:
        """Create or find the author as a WordPress user.

        Returns:
            WordPress user ID, or 0 on failure.
        """
        import httpx
        api = f"{wp_url.rstrip('/')}/wp-json/wp/v2"
        auth = (wp_user, wp_password)

        # Check if user already exists
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                search_resp = await client.get(
                    f"{api}/users",
                    auth=auth,
                    params={"search": author.slug[:20]},
                )
                search_resp.raise_for_status()
                users = search_resp.json()
                if users:
                    wp_id = users[0].get("id", 0)
                    log.info("author_entity.wp_found  name=%s  wp_id=%s", author.name, wp_id)
                    author.wp_author_id = wp_id
                    self._save_author(author)
                    return wp_id
        except Exception as e:
            log.warning("author_entity.wp_search_fail  err=%s", e)

        # Create new WordPress user
        try:
            user_data = {
                "username":    author.slug.replace("-", "_"),
                "name":        author.name,
                "first_name":  author.name.split()[0] if author.name else "",
                "last_name":   " ".join(author.name.split()[1:]) if len(author.name.split()) > 1 else "",
                "email":       author.email or f"{author.slug}@{_extract_domain(author.website)}",
                "description": author.bio_long or author.bio,
                "roles":       ["author"],
                "password":    _generate_temp_password(),
            }
            if author.url:
                user_data["url"] = author.website

            async with httpx.AsyncClient(timeout=20) as client:
                create_resp = await client.post(
                    f"{api}/users",
                    auth=auth,
                    json=user_data,
                )
                create_resp.raise_for_status()
                data = create_resp.json()
                wp_id = data.get("id", 0)
                log.info("author_entity.wp_created  name=%s  wp_id=%s", author.name, wp_id)
                author.wp_author_id = wp_id
                self._save_author(author)
                return wp_id
        except Exception as e:
            log.warning("author_entity.wp_create_fail  name=%s  err=%s", author.name, e)
            return 0

    def get_eeat_score(self, author: AuthorProfile) -> dict:
        """Score the author's E-E-A-T signals (0-10 per dimension)."""
        scores: dict[str, int] = {}

        # Experience
        exp = 0
        if author.years_exp >= 10:
            exp = 10
        elif author.years_exp >= 5:
            exp = 7
        elif author.years_exp >= 2:
            exp = 5
        if author.service_count >= 1000:
            exp = min(exp + 2, 10)
        scores["experience"] = exp

        # Expertise
        expertise = 0
        if author.credentials:
            expertise += min(len(author.credentials) * 2, 6)
        if author.bio_long:
            expertise += 2
        if author.title:
            expertise += 2
        scores["expertise"] = min(expertise, 10)

        # Authority (presence across web)
        authority = 0
        if author.linkedin:    authority += 2
        if author.twitter:     authority += 1
        if author.google_profile: authority += 2
        if author.wikidata_id: authority += 3
        if len(author.same_as) >= 5: authority += 2
        scores["authority"] = min(authority, 10)

        # Trust
        trust = 0
        if author.phone:   trust += 2
        if author.email:   trust += 2
        if author.website: trust += 2
        if author.city:    trust += 2
        if author.photo_url: trust += 2
        scores["trust"] = min(trust, 10)

        overall = sum(scores.values()) // 4
        scores["overall"] = overall

        missing = []
        if not author.credentials:      missing.append("credentials")
        if not author.wikidata_id:      missing.append("wikidata entry")
        if not author.linkedin:         missing.append("LinkedIn profile")
        if not author.photo_url:        missing.append("author photo")
        if not author.bio_long:         missing.append("extended bio")
        if author.years_exp == 0:       missing.append("years of experience")

        return {
            "scores":    scores,
            "missing":   missing,
            "next_step": missing[0] if missing else "All E-E-A-T signals present",
        }

    # ------------------------------------------------------------------
    # Private: generation
    # ------------------------------------------------------------------

    def _generate_author(self, biz: dict) -> AuthorProfile:
        """Generate an author profile from business data using Claude."""
        business_name  = biz.get("business_name", "Local Expert")
        primary_service = biz.get("primary_service", "service")
        city           = biz.get("primary_city", "")
        business_id    = biz.get("id", "unknown")
        website        = biz.get("website", "")
        phone          = biz.get("phone", "")
        email          = biz.get("email", "")

        try:
            from core.claude import call_claude
            prompt = f"""Generate a realistic, E-E-A-T optimised author profile for a local business expert.

Business: {business_name}
Service: {primary_service}
City: {city}

Requirements:
- Author name: realistic first + last name for the owner/expert
- Title: specific professional title (e.g. "Licensed Master Plumber & Owner")
- Bio (150 chars): punchy, first-person, credential-focused
- Bio long (500 chars): detailed background, experience, credentials
- Credentials: list of real industry certifications for this service type
- Years experience: realistic number (5-20)
- Service count: realistic number of completed jobs (hundreds to thousands)

Return ONLY valid JSON:
{{
  "name": "",
  "title": "",
  "bio": "",
  "bio_long": "",
  "credentials": ["", ""],
  "years_exp": 10,
  "service_count": 500
}}"""

            raw = call_claude(prompt, max_tokens=512)
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            data = json.loads(raw)
        except Exception as e:
            log.warning("author_entity.claude_fail  err=%s  using_defaults", e)
            data = {
                "name":         f"{business_name.split()[0]} Expert",
                "title":        f"Licensed {primary_service.title()} Professional",
                "bio":          f"Expert {primary_service} provider in {city} with years of experience.",
                "bio_long":     f"Specialising in {primary_service} services across {city}, our team brings deep expertise and a commitment to quality to every job.",
                "credentials":  [f"Licensed {primary_service.title()} Professional"],
                "years_exp":    10,
                "service_count": 500,
            }

        name    = data.get("name", business_name)
        slug    = _to_slug(name)
        now     = datetime.now(tz=timezone.utc).isoformat()

        same_as = []
        if website:
            same_as.append(website)

        return AuthorProfile(
            id=f"{business_id}-author",
            name=name,
            slug=slug,
            title=data.get("title", ""),
            bio=data.get("bio", ""),
            bio_long=data.get("bio_long", ""),
            credentials=data.get("credentials", []),
            years_exp=data.get("years_exp", 0),
            service_count=data.get("service_count", 0),
            city=city,
            phone=phone,
            email=email,
            website=website,
            same_as=same_as,
            business_id=business_id,
            created_at=now,
            updated_at=now,
        )

    # ------------------------------------------------------------------
    # Private: persistence
    # ------------------------------------------------------------------

    def _load_author(self, business_id: str) -> AuthorProfile | None:
        path = _AUTHORS_DIR / f"{business_id}.json"
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return AuthorProfile(**data)
        except Exception as e:
            log.warning("author_entity.load_fail  business_id=%s  err=%s", business_id, e)
            return None

    def _save_author(self, author: AuthorProfile) -> None:
        author.updated_at = datetime.now(tz=timezone.utc).isoformat()
        path = _AUTHORS_DIR / f"{author.business_id}.json"
        data = {
            "id":             author.id,
            "name":           author.name,
            "slug":           author.slug,
            "title":          author.title,
            "bio":            author.bio,
            "bio_long":       author.bio_long,
            "credentials":    author.credentials,
            "years_exp":      author.years_exp,
            "service_count":  author.service_count,
            "city":           author.city,
            "phone":          author.phone,
            "email":          author.email,
            "website":        author.website,
            "linkedin":       author.linkedin,
            "google_profile": author.google_profile,
            "twitter":        author.twitter,
            "photo_url":      author.photo_url,
            "wp_author_id":   author.wp_author_id,
            "wikidata_id":    author.wikidata_id,
            "same_as":        author.same_as,
            "business_id":    author.business_id,
            "created_at":     author.created_at,
            "updated_at":     author.updated_at,
        }
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# HTML / utility helpers
# ---------------------------------------------------------------------------

def _build_author_bio_html(author: AuthorProfile) -> str:
    """Build the author bio box HTML."""
    photo = (
        f'<img src="{author.photo_url}" alt="{author.name}" '
        f'style="width:80px;height:80px;border-radius:50%;object-fit:cover;margin-right:16px;">'
    ) if author.photo_url else (
        f'<div style="width:80px;height:80px;border-radius:50%;background:#3182ce;'
        f'display:flex;align-items:center;justify-content:center;color:#fff;'
        f'font-size:1.8rem;font-weight:700;margin-right:16px;">'
        f'{author.name[0].upper() if author.name else "A"}</div>'
    )

    creds_html = ""
    if author.credentials:
        creds_list = " &nbsp;·&nbsp; ".join(
            f'<span style="background:#ebf8ff;color:#2b6cb0;padding:2px 8px;border-radius:4px;font-size:0.8rem;">{c}</span>'
            for c in author.credentials[:4]
        )
        creds_html = f'<div style="margin-top:8px;">{creds_list}</div>'

    social_links = ""
    links = []
    if author.linkedin:
        links.append(f'<a href="{author.linkedin}" target="_blank" rel="noopener" style="color:#0077b5;">LinkedIn</a>')
    if author.twitter:
        links.append(f'<a href="{author.twitter}" target="_blank" rel="noopener" style="color:#1da1f2;">Twitter/X</a>')
    if links:
        social_links = ' &nbsp;·&nbsp; '.join(links)
        social_links = f'<div style="margin-top:6px;font-size:0.85rem;">{social_links}</div>'

    years_line = f'<span style="color:#718096;font-size:0.85rem;">{author.years_exp} years experience</span>' if author.years_exp else ""

    return f"""
<div class="seo-author-bio" itemscope itemtype="https://schema.org/Person"
     style="border-top:2px solid #e2e8f0;margin:32px 0 0;padding:24px 0;display:flex;align-items:flex-start;">
  {photo}
  <div style="flex:1;">
    <p style="margin:0;font-size:0.85rem;text-transform:uppercase;letter-spacing:0.05em;color:#718096;">Written by</p>
    <h3 itemprop="name" style="margin:4px 0;font-size:1.1rem;">
      {f'<a href="/author/{author.slug}" itemprop="url" style="color:inherit;">{author.name}</a>' if author.slug else author.name}
    </h3>
    <p itemprop="jobTitle" style="margin:0 0 4px;color:#4a5568;font-size:0.9rem;">{author.title}</p>
    {years_line}
    <p itemprop="description" style="margin:8px 0;color:#4a5568;font-size:0.9rem;">{author.bio}</p>
    {creds_html}
    {social_links}
  </div>
</div>"""


def _to_slug(name: str) -> str:
    import re
    slug = name.lower()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'\s+', '-', slug.strip())
    return slug


def _extract_domain(url: str) -> str:
    import re
    m = re.search(r'(?:https?://)?([^/]+)', url or "example.com")
    return m.group(1) if m else "example.com"


def _generate_temp_password() -> str:
    import secrets
    return secrets.token_urlsafe(24)
