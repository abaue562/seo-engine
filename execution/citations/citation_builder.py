"""Local citation building for SEO Engine.

Manages NAP consistency and directory submissions across 20+ local directories.
High-value directories for Canadian home services businesses.
"""
from __future__ import annotations
import json
import logging
import urllib.request
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

DIRECTORIES = [
    {"name": "Google Business Profile", "url": "https://business.google.com", "da": 100, "api": "google_gbp", "priority": 1},
    {"name": "Yelp", "url": "https://biz.yelp.com", "da": 93, "api": "yelp_fusion", "priority": 1},
    {"name": "HomeStars", "url": "https://homestars.com", "da": 64, "api": "manual", "priority": 1},
    {"name": "Better Business Bureau", "url": "https://bbb.org", "da": 91, "api": "manual", "priority": 1},
    {"name": "Yellow Pages Canada", "url": "https://yellowpages.ca", "da": 72, "api": "manual", "priority": 1},
    {"name": "Houzz", "url": "https://houzz.com", "da": 91, "api": "manual", "priority": 2},
    {"name": "Angi (Angie's List)", "url": "https://angi.com", "da": 84, "api": "manual", "priority": 2},
    {"name": "Facebook Business", "url": "https://facebook.com/business", "da": 99, "api": "manual", "priority": 1},
    {"name": "Bing Places", "url": "https://bingplaces.com", "da": 100, "api": "manual", "priority": 1},
    {"name": "Apple Maps Connect", "url": "https://mapsconnect.apple.com", "da": 100, "api": "manual", "priority": 2},
    {"name": "Foursquare", "url": "https://foursquare.com/add-listing", "da": 92, "api": "manual", "priority": 3},
    {"name": "Cylex Canada", "url": "https://ca.cylex.com", "da": 52, "api": "manual", "priority": 3},
    {"name": "411.ca", "url": "https://411.ca", "da": 55, "api": "manual", "priority": 2},
    {"name": "Canada411", "url": "https://canada411.ca", "da": 58, "api": "manual", "priority": 2},
    {"name": "Local.com", "url": "https://local.com", "da": 52, "api": "manual", "priority": 3},
    {"name": "Manta", "url": "https://manta.com", "da": 67, "api": "manual", "priority": 3},
    {"name": "Alignable", "url": "https://alignable.com", "da": 51, "api": "manual", "priority": 3},
    {"name": "Nextdoor", "url": "https://nextdoor.com/business", "da": 77, "api": "manual", "priority": 2},
    {"name": "Thumbtack", "url": "https://thumbtack.com", "da": 76, "api": "manual", "priority": 2},
    {"name": "Townpost", "url": "https://townpost.ca", "da": 38, "api": "manual", "priority": 3},
]


@dataclass
class BusinessNAP:
    name: str
    address: str
    city: str
    province: str
    postal_code: str
    phone: str
    website: str
    email: str
    description: str
    services: list = field(default_factory=list)
    hours: str = "Mon-Sat: 8am-6pm"
    founded_year: int = 2019
    categories: list = field(default_factory=list)


class CitationBuilder:
    """Manages local directory citations for a business."""

    def __init__(self, storage_path: str = "data/storage/citations"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)

    def generate_submission_package(self, nap: BusinessNAP) -> dict:
        """Generate complete citation submission data for all 20 directories."""
        package = {
            "business": nap.name,
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "nap": {
                "name": nap.name,
                "address": nap.address,
                "city": nap.city,
                "province": nap.province,
                "postal_code": nap.postal_code,
                "phone": nap.phone,
                "website": nap.website,
                "email": nap.email,
            },
            "directories": [],
        }

        for directory in sorted(DIRECTORIES, key=lambda d: d["priority"]):
            entry = {
                "directory": directory["name"],
                "url": directory["url"],
                "da": directory["da"],
                "priority": directory["priority"],
                "status": "pending",
                "submission_data": self._format_for_directory(nap, directory),
            }
            package["directories"].append(entry)

        safe_name = nap.name.lower().replace(" ", "_")[:30]
        pkg_file = self.storage_path / f"{safe_name}_citations.json"
        pkg_file.write_text(json.dumps(package, indent=2))
        log.info("citations.package_saved  path=%s  directories=%d", pkg_file, len(DIRECTORIES))
        return package

    def _format_for_directory(self, nap: BusinessNAP, directory: dict) -> dict:
        return {
            "business_name": nap.name,
            "address": f"{nap.address}, {nap.city}, {nap.province} {nap.postal_code}",
            "phone": nap.phone,
            "website": nap.website,
            "email": nap.email,
            "description": nap.description[:500],
            "services": ", ".join(nap.services[:5]),
            "hours": nap.hours,
            "categories": nap.categories[:3],
            "submission_url": directory["url"],
        }

    def mark_submitted(self, business_name: str, directory_name: str, listing_url: str = "") -> None:
        safe_name = business_name.lower().replace(" ", "_")[:30]
        pkg_file = self.storage_path / f"{safe_name}_citations.json"
        if not pkg_file.exists():
            return
        package = json.loads(pkg_file.read_text())
        for d in package["directories"]:
            if d["directory"] == directory_name:
                d["status"] = "submitted"
                d["listing_url"] = listing_url
                d["submitted_at"] = datetime.now(tz=timezone.utc).isoformat()
        pkg_file.write_text(json.dumps(package, indent=2))
