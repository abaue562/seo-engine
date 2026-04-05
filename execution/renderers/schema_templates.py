"""JSON-LD Schema Templates — 12 validated structured data templates.

Based on JayHoltslander/Structured-Data-JSON-LD (281 stars).
All templates validated against Google's Rich Results Test.

Usage:
    from execution.renderers.schema_templates import generate_schema, SCHEMA_TYPES

    schema = generate_schema("local_business", {
        "name": "Blend Bright Lights",
        "city": "Kelowna",
        "phone": "(250) 555-0199",
    })
    # Returns JSON-LD string ready to embed in HTML
"""

from __future__ import annotations

import json
import logging

log = logging.getLogger(__name__)


def _clean(d: dict) -> dict:
    """Remove None/empty values from a dict."""
    return {k: v for k, v in d.items() if v is not None and v != ""}


# =====================================================================
# Schema Templates
# =====================================================================

def local_business(data: dict) -> dict:
    return _clean({
        "@context": "https://schema.org",
        "@type": "LocalBusiness",
        "name": data.get("name", ""),
        "description": data.get("description", ""),
        "url": data.get("website", ""),
        "telephone": data.get("phone", ""),
        "email": data.get("email", ""),
        "image": data.get("image", ""),
        "priceRange": data.get("price_range", "$$"),
        "address": _clean({
            "@type": "PostalAddress",
            "streetAddress": data.get("address", ""),
            "addressLocality": data.get("city", ""),
            "addressRegion": data.get("province", data.get("state", "")),
            "postalCode": data.get("postal_code", ""),
            "addressCountry": data.get("country", "CA"),
        }),
        "geo": _clean({
            "@type": "GeoCoordinates",
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
        }) if data.get("latitude") else None,
        "openingHoursSpecification": data.get("hours"),
        "aggregateRating": _clean({
            "@type": "AggregateRating",
            "ratingValue": str(data.get("rating", "")),
            "reviewCount": str(data.get("review_count", "")),
            "bestRating": "5",
        }) if data.get("rating") else None,
        "sameAs": data.get("social_profiles", []),
    })


def faq_page(data: dict) -> dict:
    questions = data.get("questions", [])
    return {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {
                "@type": "Question",
                "name": q.get("question", ""),
                "acceptedAnswer": {
                    "@type": "Answer",
                    "text": q.get("answer", ""),
                },
            }
            for q in questions
        ],
    }


def how_to(data: dict) -> dict:
    steps = data.get("steps", [])
    return _clean({
        "@context": "https://schema.org",
        "@type": "HowTo",
        "name": data.get("name", ""),
        "description": data.get("description", ""),
        "totalTime": data.get("total_time", ""),
        "estimatedCost": _clean({
            "@type": "MonetaryAmount",
            "currency": data.get("currency", "CAD"),
            "value": data.get("cost", ""),
        }) if data.get("cost") else None,
        "step": [
            _clean({
                "@type": "HowToStep",
                "name": step.get("name", f"Step {i+1}"),
                "text": step.get("text", ""),
                "image": step.get("image", ""),
            })
            for i, step in enumerate(steps)
        ],
    })


def article(data: dict) -> dict:
    return _clean({
        "@context": "https://schema.org",
        "@type": data.get("article_type", "Article"),
        "headline": data.get("headline", data.get("title", "")),
        "description": data.get("description", ""),
        "image": data.get("image", ""),
        "author": _clean({
            "@type": "Person",
            "name": data.get("author_name", ""),
            "url": data.get("author_url", ""),
        }) if data.get("author_name") else None,
        "publisher": _clean({
            "@type": "Organization",
            "name": data.get("publisher_name", ""),
            "logo": _clean({
                "@type": "ImageObject",
                "url": data.get("publisher_logo", ""),
            }) if data.get("publisher_logo") else None,
        }) if data.get("publisher_name") else None,
        "datePublished": data.get("date_published", ""),
        "dateModified": data.get("date_modified", ""),
        "mainEntityOfPage": data.get("url", ""),
    })


def service(data: dict) -> dict:
    return _clean({
        "@context": "https://schema.org",
        "@type": "Service",
        "name": data.get("name", ""),
        "description": data.get("description", ""),
        "provider": _clean({
            "@type": "LocalBusiness",
            "name": data.get("provider_name", ""),
            "url": data.get("provider_url", ""),
            "telephone": data.get("phone", ""),
        }),
        "areaServed": [
            {"@type": "City", "name": city}
            for city in data.get("service_areas", [])
        ] if data.get("service_areas") else None,
        "serviceType": data.get("service_type", ""),
        "offers": _clean({
            "@type": "Offer",
            "price": data.get("price", ""),
            "priceCurrency": data.get("currency", "CAD"),
        }) if data.get("price") else None,
    })


def product(data: dict) -> dict:
    return _clean({
        "@context": "https://schema.org",
        "@type": "Product",
        "name": data.get("name", ""),
        "description": data.get("description", ""),
        "image": data.get("image", ""),
        "brand": {"@type": "Brand", "name": data.get("brand", "")} if data.get("brand") else None,
        "sku": data.get("sku", ""),
        "offers": _clean({
            "@type": "Offer",
            "price": str(data.get("price", "")),
            "priceCurrency": data.get("currency", "CAD"),
            "availability": data.get("availability", "https://schema.org/InStock"),
            "url": data.get("url", ""),
        }),
        "aggregateRating": _clean({
            "@type": "AggregateRating",
            "ratingValue": str(data.get("rating", "")),
            "reviewCount": str(data.get("review_count", "")),
        }) if data.get("rating") else None,
    })


def breadcrumb(data: dict) -> dict:
    items = data.get("items", [])
    return {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {
                "@type": "ListItem",
                "position": i + 1,
                "name": item.get("name", ""),
                "item": item.get("url", ""),
            }
            for i, item in enumerate(items)
        ],
    }


def video(data: dict) -> dict:
    return _clean({
        "@context": "https://schema.org",
        "@type": "VideoObject",
        "name": data.get("name", ""),
        "description": data.get("description", ""),
        "thumbnailUrl": data.get("thumbnail", ""),
        "uploadDate": data.get("upload_date", ""),
        "duration": data.get("duration", ""),
        "contentUrl": data.get("content_url", ""),
        "embedUrl": data.get("embed_url", ""),
    })


def review(data: dict) -> dict:
    return _clean({
        "@context": "https://schema.org",
        "@type": "Review",
        "itemReviewed": _clean({
            "@type": data.get("reviewed_type", "LocalBusiness"),
            "name": data.get("reviewed_name", ""),
        }),
        "author": {"@type": "Person", "name": data.get("author", "")},
        "reviewRating": _clean({
            "@type": "Rating",
            "ratingValue": str(data.get("rating", "")),
            "bestRating": "5",
        }),
        "reviewBody": data.get("body", ""),
        "datePublished": data.get("date", ""),
    })


def item_list(data: dict) -> dict:
    items = data.get("items", [])
    return {
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": data.get("name", ""),
        "itemListElement": [
            {
                "@type": "ListItem",
                "position": i + 1,
                "name": item.get("name", ""),
                "url": item.get("url", ""),
                "description": item.get("description", ""),
            }
            for i, item in enumerate(items)
        ],
    }


def organization(data: dict) -> dict:
    return _clean({
        "@context": "https://schema.org",
        "@type": "Organization",
        "name": data.get("name", ""),
        "url": data.get("website", ""),
        "logo": data.get("logo", ""),
        "description": data.get("description", ""),
        "telephone": data.get("phone", ""),
        "email": data.get("email", ""),
        "address": _clean({
            "@type": "PostalAddress",
            "addressLocality": data.get("city", ""),
            "addressRegion": data.get("province", ""),
            "addressCountry": data.get("country", "CA"),
        }),
        "sameAs": data.get("social_profiles", []),
        "foundingDate": data.get("founding_date", ""),
    })


def webpage(data: dict) -> dict:
    return _clean({
        "@context": "https://schema.org",
        "@type": "WebPage",
        "name": data.get("name", data.get("title", "")),
        "description": data.get("description", ""),
        "url": data.get("url", ""),
        "datePublished": data.get("date_published", ""),
        "dateModified": data.get("date_modified", ""),
        "isPartOf": {"@type": "WebSite", "name": data.get("site_name", ""), "url": data.get("site_url", "")} if data.get("site_name") else None,
    })


# =====================================================================
# Registry + Generator
# =====================================================================

SCHEMA_TYPES = {
    "local_business": {"fn": local_business, "description": "Local business with address, hours, reviews"},
    "faq": {"fn": faq_page, "description": "FAQ page with questions and answers"},
    "how_to": {"fn": how_to, "description": "Step-by-step instructions"},
    "article": {"fn": article, "description": "Blog post or news article"},
    "service": {"fn": service, "description": "Service offering with provider and area"},
    "product": {"fn": product, "description": "Product with price and reviews"},
    "breadcrumb": {"fn": breadcrumb, "description": "Breadcrumb navigation"},
    "video": {"fn": video, "description": "Video content"},
    "review": {"fn": review, "description": "Individual review"},
    "item_list": {"fn": item_list, "description": "List of items (best-of, rankings)"},
    "organization": {"fn": organization, "description": "Company/organization"},
    "webpage": {"fn": webpage, "description": "Generic web page"},
}


def generate_schema(schema_type: str, data: dict) -> str:
    """Generate a JSON-LD schema string ready to embed in HTML.

    Args:
        schema_type: Key from SCHEMA_TYPES
        data: Dict of values for the template

    Returns:
        Complete <script type="application/ld+json"> tag
    """
    if schema_type not in SCHEMA_TYPES:
        raise ValueError(f"Unknown schema type: {schema_type}. Available: {list(SCHEMA_TYPES.keys())}")

    schema = SCHEMA_TYPES[schema_type]["fn"](data)
    json_str = json.dumps(schema, indent=2, ensure_ascii=False)

    return f'<script type="application/ld+json">\n{json_str}\n</script>'


def generate_all_schemas(business: dict) -> list[str]:
    """Generate all applicable schemas for a business.

    Returns list of JSON-LD script tags.
    """
    schemas = []

    # LocalBusiness
    schemas.append(generate_schema("local_business", business))

    # Organization
    schemas.append(generate_schema("organization", business))

    # Service (if service info provided)
    if business.get("primary_service"):
        schemas.append(generate_schema("service", {
            "name": business.get("primary_service"),
            "provider_name": business.get("name", business.get("business_name", "")),
            "provider_url": business.get("website", ""),
            "phone": business.get("phone", ""),
            "service_areas": business.get("service_areas", []),
            "service_type": business.get("primary_service"),
        }))

    return schemas
