"""
E-E-A-T: Trust signal injection and policy page generation.
Generates Privacy Policy, Terms of Service, About page, and Editorial Policy.
Injects trust badges and review schema into content.
"""
from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

# --- Policy page generators ---

def generate_privacy_policy(business_name: str, domain: str, contact_email: str) -> str:
    year = datetime.now(timezone.utc).year
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Privacy Policy — {business_name}</title>
<meta name="description" content="Privacy policy for {business_name}. Learn how we collect, use, and protect your information.">
</head>
<body>
<article itemscope itemtype="https://schema.org/WebPage">
<h1 itemprop="name">Privacy Policy</h1>
<p><strong>Last updated:</strong> <time datetime="{datetime.now(timezone.utc).strftime('%Y-%m-%d')}">{datetime.now(timezone.utc).strftime('%B %d, %Y')}</time></p>

<h2>Who We Are</h2>
<p>{business_name} operates at <a href="https://{domain}">{domain}</a>. Contact us at <a href="mailto:{contact_email}">{contact_email}</a>.</p>

<h2>Information We Collect</h2>
<p>We collect information you provide directly (name, email, phone) when you request a quote, contact us, or use our services. We also collect usage data (IP address, browser type, pages visited) automatically via cookies and analytics tools.</p>

<h2>How We Use Your Information</h2>
<ul>
<li>To respond to your inquiries and provide services</li>
<li>To send service updates and promotional communications (with your consent)</li>
<li>To improve our website and services</li>
<li>To comply with legal obligations</li>
</ul>

<h2>Data Sharing</h2>
<p>We do not sell your personal information. We may share data with service providers who assist our operations (payment processors, email providers) under strict confidentiality agreements.</p>

<h2>Your Rights</h2>
<p>You have the right to access, correct, or delete your personal data. Email <a href="mailto:{contact_email}">{contact_email}</a> to exercise these rights.</p>

<h2>Cookies</h2>
<p>We use essential cookies for site functionality and analytics cookies to understand usage. You may disable non-essential cookies in your browser settings.</p>

<h2>Changes to This Policy</h2>
<p>We may update this policy periodically. The date at the top of this page reflects the most recent revision.</p>

<p>&copy; {year} {business_name}. All rights reserved.</p>
</article>
</body>
</html>"""


def generate_terms_of_service(business_name: str, domain: str, contact_email: str, state: str = "BC, Canada") -> str:
    year = datetime.now(timezone.utc).year
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Terms of Service — {business_name}</title>
<meta name="description" content="Terms of service for {business_name}. Read our terms before using our services.">
</head>
<body>
<article itemscope itemtype="https://schema.org/WebPage">
<h1 itemprop="name">Terms of Service</h1>
<p><strong>Last updated:</strong> <time datetime="{datetime.now(timezone.utc).strftime('%Y-%m-%d')}">{datetime.now(timezone.utc).strftime('%B %d, %Y')}</time></p>

<h2>Agreement to Terms</h2>
<p>By accessing or using {business_name}'s services at <a href="https://{domain}">{domain}</a>, you agree to be bound by these Terms of Service.</p>

<h2>Services</h2>
<p>{business_name} provides professional services as described on our website. We reserve the right to refuse service to anyone for any reason.</p>

<h2>Quotes and Pricing</h2>
<p>All quotes are valid for 30 days from issuance unless otherwise stated. Final pricing may vary based on actual conditions found during service delivery.</p>

<h2>Payment Terms</h2>
<p>Payment is due upon completion of services unless a written payment schedule has been agreed upon in advance. We accept major credit cards, e-transfer, and cash.</p>

<h2>Warranty and Liability</h2>
<p>We warrant our workmanship for 1 year from the date of service. Our liability is limited to the amount paid for services. We are not liable for pre-existing conditions.</p>

<h2>Cancellation</h2>
<p>Please provide 48 hours notice for cancellations. Late cancellations may incur a fee of up to $75.</p>

<h2>Governing Law</h2>
<p>These terms are governed by the laws of {state}.</p>

<h2>Contact</h2>
<p>Questions? Contact us at <a href="mailto:{contact_email}">{contact_email}</a>.</p>

<p>&copy; {year} {business_name}. All rights reserved.</p>
</article>
</body>
</html>"""


def generate_editorial_policy(business_name: str, domain: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Editorial Policy — {business_name}</title>
<meta name="description" content="How {business_name} creates, reviews, and updates content on {domain}.">
</head>
<body>
<article itemscope itemtype="https://schema.org/WebPage">
<h1 itemprop="name">Editorial Policy</h1>
<p>At {business_name}, we are committed to providing accurate, helpful, and trustworthy information. This policy explains how we create and maintain our content.</p>

<h2>Our Commitment to Accuracy</h2>
<p>All content published on {domain} is reviewed by experienced professionals in our industry before publication. We cite reputable sources and clearly distinguish between established facts and our professional opinion.</p>

<h2>Content Creation Process</h2>
<ol>
<li><strong>Research:</strong> Topics are researched using industry publications, regulatory guidelines, and hands-on field experience.</li>
<li><strong>Expert Review:</strong> Content is reviewed by a qualified team member with direct experience in the subject area.</li>
<li><strong>Fact-Check:</strong> Claims, statistics, and recommendations are verified before publication.</li>
<li><strong>Publication:</strong> Content is published with a clear author attribution and date stamp.</li>
</ol>

<h2>Content Updates</h2>
<p>We review and update our content regularly. Pages updated due to new information display a "Last updated" date. We do not silently alter factual claims — material changes are noted.</p>

<h2>Sponsored Content and Affiliate Links</h2>
<p>Any sponsored content or affiliate relationships are clearly disclosed. Our editorial opinions are never influenced by advertising relationships.</p>

<h2>Corrections Policy</h2>
<p>If you identify an error, please contact us. We will review and correct confirmed errors promptly, with a notation on the affected page.</p>

<h2>Expertise</h2>
<p>Our team brings direct, hands-on experience in the services we write about. Author credentials and backgrounds are displayed on each article.</p>

<h2>Contact</h2>
<p>Editorial questions: <a href="mailto:info@{domain}">info@{domain}</a></p>
</article>
</body>
</html>"""


def generate_about_page(
    business_name: str,
    domain: str,
    founding_year: int,
    location: str,
    description: str,
    team_members: list[dict] | None = None,
    review_count: int = 0,
    avg_rating: float = 0.0,
) -> str:
    """Generate a structured About page with LocalBusiness schema."""
    team_html = ""
    if team_members:
        cards = []
        for m in team_members:
            photo = f'<img src="{m.get("photo_url","")}" alt="{m["name"]}" width="80" height="80">' if m.get("photo_url") else ""
            cards.append(f"""<div class="team-member" itemscope itemtype="https://schema.org/Person">
  {photo}
  <strong itemprop="name">{m["name"]}</strong>
  <span itemprop="jobTitle">{m.get("title","")}</span>
  <p itemprop="description">{m.get("bio","")}</p>
</div>""")
        team_html = "<section><h2>Our Team</h2>" + "\n".join(cards) + "</section>"

    review_schema = ""
    if review_count > 0 and avg_rating > 0:
        review_schema = f"""
<script type="application/ld+json">
{{
  "@context": "https://schema.org",
  "@type": "LocalBusiness",
  "name": "{business_name}",
  "url": "https://{domain}",
  "foundingDate": "{founding_year}",
  "address": {{"@type": "PostalAddress", "addressLocality": "{location}"}},
  "aggregateRating": {{
    "@type": "AggregateRating",
    "ratingValue": "{avg_rating:.1f}",
    "reviewCount": "{review_count}",
    "bestRating": "5"
  }}
}}
</script>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>About {business_name} — {location}</title>
<meta name="description" content="{description[:155]}">
{review_schema}
</head>
<body>
<article itemscope itemtype="https://schema.org/LocalBusiness">
<h1>About <span itemprop="name">{business_name}</span></h1>
<p itemprop="description">{description}</p>

<ul class="trust-facts">
  <li>Serving <span itemprop="areaServed">{location}</span> since <span itemprop="foundingDate">{founding_year}</span></li>
  {"<li>⭐ " + str(avg_rating) + "/5 from " + str(review_count) + " verified reviews</li>" if review_count > 0 else ""}
  <li>Licensed and insured</li>
</ul>

{team_html}

<section>
<h2>Our Promise</h2>
<p>Every job is backed by our workmanship guarantee. We don't consider a job done until you're completely satisfied.</p>
</section>
</article>
</body>
</html>"""


# --- Trust signal injection ---

def inject_trust_badge(html: str, review_count: int, avg_rating: float, business_name: str) -> str:
    """Inject a compact trust badge with aggregate rating schema near the top of content."""
    if review_count < 1 or avg_rating <= 0:
        return html

    stars = "★" * round(avg_rating) + "☆" * (5 - round(avg_rating))
    badge = f"""<div class="trust-badge" itemscope itemtype="https://schema.org/AggregateRating" itemprop="aggregateRating">
  <span class="stars" aria-label="{avg_rating:.1f} out of 5 stars">{stars}</span>
  <span itemprop="ratingValue" content="{avg_rating:.1f}"></span>
  <strong>{avg_rating:.1f}/5</strong>
  from <span itemprop="reviewCount">{review_count}</span> verified reviews
  for <span itemprop="itemReviewed" itemscope itemtype="https://schema.org/LocalBusiness">
    <span itemprop="name">{business_name}</span>
  </span>
</div>"""

    # Insert after first <body> or opening <article> or at start
    for tag in ["<article", "<main", "<body"]:
        idx = html.find(tag)
        if idx != -1:
            end = html.find(">", idx) + 1
            return html[:end] + "\n" + badge + "\n" + html[end:]
    return badge + "\n" + html


def inject_review_schema(html: str, reviews: list[dict], business_name: str) -> str:
    """Inject Review schema JSON-LD for up to 5 reviews."""
    if not reviews:
        return html
    schema_reviews = []
    for r in reviews[:5]:
        schema_reviews.append({
            "@type": "Review",
            "author": {"@type": "Person", "name": r.get("author", "Verified Customer")},
            "reviewRating": {
                "@type": "Rating",
                "ratingValue": str(r.get("rating", 5)),
                "bestRating": "5"
            },
            "reviewBody": r.get("text", ""),
            "datePublished": r.get("date", ""),
            "itemReviewed": {"@type": "LocalBusiness", "name": business_name}
        })
    schema = {
        "@context": "https://schema.org",
        "@graph": schema_reviews
    }
    tag = f'<script type="application/ld+json">{json.dumps(schema, indent=2)}</script>'
    return html + "\n" + tag


def inject_breadcrumb_schema(html: str, breadcrumbs: list[dict]) -> str:
    """
    breadcrumbs: [{"name": "Home", "url": "https://..."}, ...]
    Injects BreadcrumbList schema and visible nav.
    """
    if not breadcrumbs:
        return html

    items = []
    nav_parts = []
    for i, crumb in enumerate(breadcrumbs, 1):
        items.append({
            "@type": "ListItem",
            "position": i,
            "name": crumb["name"],
            "item": crumb.get("url", "")
        })
        if crumb.get("url") and i < len(breadcrumbs):
            nav_parts.append(f'<a href="{crumb["url"]}">{crumb["name"]}</a>')
        else:
            nav_parts.append(f'<span>{crumb["name"]}</span>')

    schema = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": items
    }
    schema_tag = f'<script type="application/ld+json">{json.dumps(schema)}</script>'
    nav_html = f'<nav class="breadcrumb" aria-label="Breadcrumb">{" › ".join(nav_parts)}</nav>'

    return schema_tag + "\n" + nav_html + "\n" + html


def build_faq_schema(faqs: list[dict]) -> str:
    """
    faqs: [{"question": "...", "answer": "..."}, ...]
    Returns FAQ schema JSON-LD string.
    """
    entities = [
        {
            "@type": "Question",
            "name": f["question"],
            "acceptedAnswer": {"@type": "Answer", "text": f["answer"]}
        }
        for f in faqs if f.get("question") and f.get("answer")
    ]
    if not entities:
        return ""
    schema = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": entities
    }
    return f'<script type="application/ld+json">{json.dumps(schema, indent=2)}</script>'


def inject_faq_schema(html: str, faqs: list[dict]) -> str:
    """Extract FAQ Q&A pairs from HTML or supplied list, inject FAQ schema."""
    if not faqs:
        # Try to auto-extract from HTML
        import re
        questions = re.findall(r'<h[23][^>]*>([^<]{10,120}\?)</h[23]>', html, re.IGNORECASE)
        answers = re.findall(r'</h[23]>\s*<p>([^<]{20,})</p>', html, re.IGNORECASE)
        faqs = [{"question": q, "answer": a} for q, a in zip(questions, answers)]
    schema_tag = build_faq_schema(faqs)
    if schema_tag:
        return html + "\n" + schema_tag
    return html
