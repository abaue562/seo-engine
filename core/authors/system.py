"""
Author entity management system.

Creates and manages author profiles for E-E-A-T compliance.
Each generated piece of content is attributed to a named author entity
with full schema markup and a dedicated author page.

The single biggest E-E-A-T gap for AI-generated content is anonymous authorship.
This system fixes it.

Usage:
    authors = AuthorSystem()
    author = authors.get_or_create_author(specialty="plumbing", business_name="Joe's Plumbing")
    schema = authors.get_author_schema(author)
    page_html = authors.render_author_page(author)
"""
import os
import json
import hashlib
import logging
import random
from datetime import datetime, date
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class Author:
    id: str                        # SHA-256 of name (first 12 chars)
    name: str                      # Full name (e.g., "James Carter")
    slug: str                      # URL slug (e.g., "james-carter")
    title: str                     # Professional title (e.g., "Licensed Master Plumber")
    bio: str                       # 2-3 sentence bio
    specialty: str                 # Primary topic specialty
    years_experience: int          # Years in field (5-25 range)
    credentials: list              # ["Licensed Master Plumber", "EPA Certified", ...]
    social_profiles: dict          # {"linkedin": url, "twitter": url}
    schema_url: str                # Author page URL
    created_at: str                # ISO date
    article_count: int = 0         # How many articles attributed


NAME_POOL = {
    "plumbing": ["James Carter", "Michael Torres", "David Walsh", "Robert Kim"],
    "hvac": ["Sarah Mitchell", "Thomas Reynolds", "Emily Chen", "Mark Peterson"],
    "electrical": ["Kevin Brooks", "Laura Martinez", "Daniel Scott", "Rachel Adams"],
    "roofing": ["Brian Johnson", "Amanda Clark", "Steven White", "Nicole Davis"],
    "landscaping": ["Tyler Green", "Melissa Brown", "Jason Hill", "Stephanie Moore"],
    "general": ["Alex Thompson", "Jordan Rivera", "Casey Morgan", "Taylor Hayes"],
}

CREDENTIAL_MAP = {
    "plumbing": ["Licensed Master Plumber", "State Certified Plumber", "EPA 608 Certified"],
    "hvac": ["NATE Certified Technician", "EPA 608 Universal Certified", "ACCA Member"],
    "electrical": ["Licensed Master Electrician", "NFPA Member", "OSHA 30 Certified"],
    "roofing": ["GAF Certified Installer", "OSHA 10 Certified", "NRCA Member"],
    "landscaping": ["ISA Certified Arborist", "PLANET Member", "Irrigation Association Certified"],
    "general": ["Industry Certified Professional", "Better Business Bureau Member"],
}

TITLE_MAP = {
    "plumbing": "Licensed Master Plumber",
    "hvac": "NATE Certified HVAC Technician",
    "electrical": "Licensed Master Electrician",
    "roofing": "GAF Certified Roofing Contractor",
    "landscaping": "ISA Certified Arborist & Landscape Professional",
    "general": "Certified Home Services Professional",
}

BIO_TEMPLATES = [
    (
        "{name} is a {title} with {years} years of hands-on experience serving homeowners across "
        "the {area} area{business_mention}. {pronoun} holds {credential} and has personally "
        "completed over {projects} {specialty} projects ranging from emergency repairs to full "
        "system installations. {first_name} regularly contributes expert advice on {specialty} "
        "maintenance and best practices."
    ),
    (
        "With {years} years in the {specialty} industry, {name} has built a reputation for clear, "
        "accurate guidance that homeowners can actually act on{business_mention}. {pronoun} is "
        "{credential} and draws on direct field experience to answer the questions customers "
        "ask most. {first_name}'s goal is to help every homeowner understand their home systems "
        "before a problem turns into an emergency."
    ),
    (
        "{name} brings {years} years of field experience to every article {pronoun_lower} writes "
        "on {specialty}{business_mention}. As a {title}, {pronoun_lower} has seen firsthand how "
        "proper maintenance prevents costly failures. {first_name} is {credential} and committed "
        "to translating professional knowledge into practical advice for everyday homeowners."
    ),
]


def _normalize_specialty(specialty: str) -> str:
    """Map a specialty string to a NAME_POOL key."""
    specialty_lower = specialty.lower()
    for key in NAME_POOL:
        if key in specialty_lower:
            return key
    # Fuzzy matching for common variations
    mappings = {
        "pipe": "plumbing", "drain": "plumbing", "water heater": "plumbing",
        "air conditioning": "hvac", "ac ": "hvac", "heat": "hvac", "furnace": "hvac",
        "wiring": "electrical", "electric": "electrical", "outlet": "electrical",
        "roof": "roofing", "shingle": "roofing", "gutter": "roofing",
        "lawn": "landscaping", "tree": "landscaping", "garden": "landscaping",
    }
    for kw, mapped in mappings.items():
        if kw in specialty_lower:
            return mapped
    return "general"


def _make_slug(name: str) -> str:
    """Convert 'James Carter' to 'james-carter'."""
    return name.lower().replace(" ", "-").replace("'", "").replace(".", "")


def _make_id(name: str) -> str:
    """SHA-256 of name, first 12 hex chars."""
    return hashlib.sha256(name.encode()).hexdigest()[:12]


class AuthorSystem:
    STORAGE_PATH = Path("data/storage/authors")

    def __init__(self, site_base_url: str = ""):
        self.storage = self.STORAGE_PATH
        self.storage.mkdir(parents=True, exist_ok=True)
        self.site_base_url = site_base_url or os.getenv("SITE_BASE_URL", "https://example.com")

    def get_or_create_author(self, specialty: str, business_name: str = "") -> Author:
        """Get existing author for specialty or create a new one.

        Each specialty gets one consistent author across all content.
        """
        normalized = _normalize_specialty(specialty)
        existing = self.load_author(normalized)
        if existing:
            log.info("authors.cache_hit  specialty=%s  author=%s", normalized, existing.name)
            return existing
        return self.create_author(specialty, business_name)

    def create_author(self, specialty: str, business_name: str = "") -> Author:
        """Create a new realistic author profile."""
        normalized = _normalize_specialty(specialty)

        # Pick name — avoid reusing if possible (check storage for taken names)
        taken_names = {a.name for a in self.list_all_authors()}
        name_candidates = NAME_POOL.get(normalized, NAME_POOL["general"])
        available = [n for n in name_candidates if n not in taken_names]
        name = random.choice(available) if available else random.choice(name_candidates)

        first_name = name.split()[0]
        slug = _make_slug(name)
        author_id = _make_id(name)

        title = TITLE_MAP.get(normalized, "Certified Home Services Professional")
        credentials = list(CREDENTIAL_MAP.get(normalized, CREDENTIAL_MAP["general"]))
        years_experience = random.randint(8, 22)

        # Gender-neutral pronoun selection: pick randomly per author
        pronoun = random.choice(["He", "She", "They"])
        pronoun_lower = pronoun.lower()

        schema_url = f"{self.site_base_url}/authors/{slug}"

        social_profiles = {
            "linkedin": f"https://www.linkedin.com/in/{slug}",
            "twitter": f"https://twitter.com/{slug.replace('-', '_')}",
        }

        # Build a stub Author first (bio needs the full object)
        author = Author(
            id=author_id,
            name=name,
            slug=slug,
            title=title,
            bio="",  # filled below
            specialty=normalized,
            years_experience=years_experience,
            credentials=credentials,
            social_profiles=social_profiles,
            schema_url=schema_url,
            created_at=date.today().isoformat(),
            article_count=0,
        )
        author.bio = self.generate_bio(author, business_name)

        self.save_author(author)
        log.info("authors.created  id=%s  name=%s  specialty=%s", author_id, name, normalized)
        return author

    def generate_bio(self, author: Author, business_name: str = "") -> str:
        """Generate a realistic 2-3 sentence professional bio using templates."""
        first_name = author.name.split()[0]
        pronoun = "He" if author.name.split()[0] in [
            "James", "Michael", "David", "Robert", "Thomas", "Mark",
            "Kevin", "Daniel", "Brian", "Steven", "Tyler", "Jason",
        ] else "She"
        pronoun_lower = pronoun.lower()

        # Business mention fragment
        business_mention = ""
        if business_name:
            business_mention = f" with {business_name}"

        # Credential string
        credential_str = author.credentials[0] if author.credentials else "industry certified"

        # Estimated projects (years * ~130 per year, adjusted by specialty)
        projects = (author.years_experience * random.randint(100, 180)) // 100 * 100

        # Area placeholder — overridden when business has a city
        area = "greater metropolitan"

        template = random.choice(BIO_TEMPLATES)
        bio = template.format(
            name=author.name,
            first_name=first_name,
            title=author.title,
            years=author.years_experience,
            area=area,
            business_mention=business_mention,
            pronoun=pronoun,
            pronoun_lower=pronoun_lower,
            credential=credential_str,
            projects=projects,
            specialty=author.specialty,
        )
        return bio

    def get_author_schema(self, author: Author) -> dict:
        """Generate schema.org Person schema for author."""
        schema = {
            "@context": "https://schema.org",
            "@type": "Person",
            "name": author.name,
            "url": author.schema_url,
            "description": author.bio,
            "jobTitle": author.title,
            "knowsAbout": [author.specialty.title(), "Home Maintenance", "DIY Repairs"],
            "hasCredential": [
                {
                    "@type": "EducationalOccupationalCredential",
                    "name": cred,
                    "credentialCategory": "Professional Certification",
                }
                for cred in author.credentials
            ],
            "sameAs": list(author.social_profiles.values()),
        }
        return schema

    def render_author_page(self, author: Author) -> str:
        """Generate HTML author bio page."""
        credentials_html = "".join(
            f'<li class="credential-item">{cred}</li>' for cred in author.credentials
        )
        social_links_html = ""
        if author.social_profiles.get("linkedin"):
            social_links_html += (
                f'<a class="social-link" href="{author.social_profiles["linkedin"]}" '
                f'rel="noopener noreferrer" target="_blank">LinkedIn</a>'
            )
        if author.social_profiles.get("twitter"):
            social_links_html += (
                f' <a class="social-link" href="{author.social_profiles["twitter"]}" '
                f'rel="noopener noreferrer" target="_blank">Twitter/X</a>'
            )

        import json as _json
        schema_json = _json.dumps(self.get_author_schema(author), indent=2, ensure_ascii=False)

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{author.name} — Expert Author Profile</title>
  <script type="application/ld+json">
{schema_json}
  </script>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f8f9fa; color: #212529; line-height: 1.6; }}
    .author-card {{ max-width: 760px; margin: 48px auto; background: #fff;
                    border-radius: 12px; box-shadow: 0 2px 12px rgba(0,0,0,.08);
                    padding: 40px; }}
    .author-header {{ display: flex; align-items: center; gap: 28px; margin-bottom: 28px; }}
    .author-avatar {{ width: 96px; height: 96px; border-radius: 50%;
                      background: linear-gradient(135deg, #667eea, #764ba2);
                      display: flex; align-items: center; justify-content: center;
                      font-size: 36px; color: #fff; font-weight: 700; flex-shrink: 0; }}
    .author-meta h1 {{ font-size: 1.75rem; font-weight: 700; margin-bottom: 4px; }}
    .author-meta .title {{ color: #6c757d; font-size: 1rem; margin-bottom: 6px; }}
    .author-meta .experience {{ font-size: 0.9rem; color: #495057; }}
    .section-label {{ font-size: 0.75rem; font-weight: 700; letter-spacing: 0.08em;
                      text-transform: uppercase; color: #6c757d; margin-bottom: 8px; }}
    .bio {{ font-size: 1.05rem; color: #343a40; margin-bottom: 28px;
            border-left: 3px solid #667eea; padding-left: 16px; }}
    .credentials-list {{ list-style: none; margin-bottom: 28px; }}
    .credential-item {{ padding: 8px 14px; background: #f1f3f5;
                        border-radius: 6px; margin-bottom: 6px; font-size: 0.95rem; }}
    .credential-item::before {{ content: "✓ "; color: #28a745; font-weight: 700; }}
    .stats-row {{ display: flex; gap: 24px; margin-bottom: 28px; }}
    .stat-box {{ flex: 1; text-align: center; background: #f8f9fa;
                 border-radius: 8px; padding: 16px; }}
    .stat-number {{ font-size: 1.75rem; font-weight: 700; color: #667eea; }}
    .stat-label {{ font-size: 0.8rem; color: #6c757d; margin-top: 2px; }}
    .social-link {{ display: inline-block; padding: 6px 16px; background: #667eea;
                    color: #fff; border-radius: 6px; text-decoration: none;
                    font-size: 0.875rem; margin-right: 8px; }}
    .social-link:hover {{ background: #5a6fd6; }}
  </style>
</head>
<body>
  <article class="author-card" itemscope itemtype="https://schema.org/Person">
    <div class="author-header">
      <div class="author-avatar" aria-hidden="true">{author.name[0]}</div>
      <div class="author-meta">
        <h1 itemprop="name">{author.name}</h1>
        <div class="title" itemprop="jobTitle">{author.title}</div>
        <div class="experience">{author.years_experience} years of professional experience</div>
      </div>
    </div>

    <div class="section-label">About</div>
    <p class="bio" itemprop="description">{author.bio}</p>

    <div class="section-label">Credentials &amp; Certifications</div>
    <ul class="credentials-list">
      {credentials_html}
    </ul>

    <div class="stats-row">
      <div class="stat-box">
        <div class="stat-number">{author.years_experience}</div>
        <div class="stat-label">Years Experience</div>
      </div>
      <div class="stat-box">
        <div class="stat-number">{author.article_count}</div>
        <div class="stat-label">Articles Published</div>
      </div>
      <div class="stat-box">
        <div class="stat-number">{len(author.credentials)}</div>
        <div class="stat-label">Certifications</div>
      </div>
    </div>

    <div class="section-label">Connect</div>
    {social_links_html}
  </article>
</body>
</html>"""

    def get_author_byline_html(self, author: Author) -> str:
        """Return compact author byline HTML for article headers."""
        return (
            f'<div class="author-byline">'
            f'By <a href="{author.schema_url}" rel="author" itemprop="author" '
            f'itemscope itemtype="https://schema.org/Person">'
            f'<span itemprop="name">{author.name}</span></a>, '
            f'<span class="author-title">{author.title}</span>'
            f' &middot; {author.years_experience} years experience'
            f'</div>'
        )

    def increment_article_count(self, author_id: str) -> None:
        """Increment article_count for author."""
        for author in self.list_all_authors():
            if author.id == author_id:
                author.article_count += 1
                self.save_author(author)
                log.info("authors.incremented  id=%s  count=%d", author_id, author.article_count)
                return
        log.warning("authors.increment_not_found  id=%s", author_id)

    def load_author(self, specialty: str) -> Optional[Author]:
        """Load author from storage by specialty (normalized key)."""
        specialty_file = self.storage / f"{specialty}.json"
        if not specialty_file.exists():
            return None
        try:
            data = json.loads(specialty_file.read_text(encoding="utf-8"))
            return Author(**data)
        except Exception as e:
            log.warning("authors.load_error  specialty=%s  err=%s", specialty, e)
            return None

    def save_author(self, author: Author) -> None:
        """Save author to storage keyed by specialty."""
        specialty_file = self.storage / f"{author.specialty}.json"
        data = asdict(author)
        specialty_file.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        log.debug("authors.saved  slug=%s  file=%s", author.slug, specialty_file)

    def list_all_authors(self) -> list:
        """Return all created author profiles."""
        authors = []
        for f in self.storage.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                authors.append(Author(**data))
            except Exception as e:
                log.warning("authors.list_error  file=%s  err=%s", f.name, e)
        return authors
