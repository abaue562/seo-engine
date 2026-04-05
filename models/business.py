from __future__ import annotations
from pydantic import BaseModel


class BusinessContext(BaseModel):
    """Permanent business context — injected into every Claude session."""

    # Identity
    business_name: str
    website: str
    gbp_url: str = ""
    years_active: int = 0

    # Services
    primary_service: str
    secondary_services: list[str] = []

    # Location
    primary_city: str
    service_areas: list[str] = []

    # Customer
    target_customer: str = ""
    avg_job_value: float = 0.0

    # SEO state
    primary_keywords: list[str] = []
    current_rankings: dict[str, int] = {}   # keyword -> position
    missing_keywords: list[str] = []

    # Performance
    reviews_count: int = 0
    rating: float = 0.0
    monthly_traffic: int = 0
    gbp_views: int = 0

    # Competitors
    competitors: list[str] = []

    def to_prompt_block(self) -> str:
        """Render as the context block Claude receives."""
        rankings = "\n".join(f"  {kw}: #{pos}" for kw, pos in self.current_rankings.items()) or "  (none loaded)"
        competitors = "\n".join(f"  - {c}" for c in self.competitors) or "  (none loaded)"

        return f"""BUSINESS:
Name: {self.business_name}
Website: {self.website}
GBP: {self.gbp_url}
Years Active: {self.years_active}

SERVICES:
Primary: {self.primary_service}
Secondary: {', '.join(self.secondary_services)}

LOCATIONS:
Primary City: {self.primary_city}
Service Areas: {', '.join(self.service_areas)}

CUSTOMER:
Target: {self.target_customer}
Avg Job Value: ${self.avg_job_value:,.0f}

SEO TARGETS:
Primary Keywords: {', '.join(self.primary_keywords)}
Current Rankings:
{rankings}
Missing Keywords: {', '.join(self.missing_keywords)}

CURRENT PERFORMANCE:
Reviews: {self.reviews_count} @ {self.rating}
Monthly Traffic: {self.monthly_traffic}
GBP Views: {self.gbp_views}

COMPETITORS:
{competitors}

CONSTRAINTS:
- Prioritize fast ROI
- Prefer actions under 30 days unless stated otherwise"""
