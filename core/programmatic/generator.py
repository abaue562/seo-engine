"""Programmatic SEO page generator.

Generates location × service × modifier keyword matrices for bulk page creation.
Designed for local service businesses (plumbing, HVAC, dentistry, legal, etc.)

Scale target: 500–10,000 unique pages per business.

Usage:
    gen = ProgrammaticGenerator("acme-plumbing")
    locations = gen.load_locations(state="California", limit=30)
    pages = gen.generate_matrix(
        services=["emergency plumber", "drain cleaning"],
        locations=locations,
        modifiers=["cost", "near me", "best", "24/7"],
    )
    calendar = gen.to_publish_calendar(pages, pages_per_day=10)
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

log = logging.getLogger(__name__)

# Top 100 US cities with real population data
_US_CITIES: list[dict] = [
    {"city": "New York", "state": "New York", "state_abbr": "NY", "county": "New York County", "population": 8336817, "lat": 40.7128, "lon": -74.0060, "timezone": "America/New_York"},
    {"city": "Los Angeles", "state": "California", "state_abbr": "CA", "county": "Los Angeles County", "population": 3979576, "lat": 34.0522, "lon": -118.2437, "timezone": "America/Los_Angeles"},
    {"city": "Chicago", "state": "Illinois", "state_abbr": "IL", "county": "Cook County", "population": 2693976, "lat": 41.8781, "lon": -87.6298, "timezone": "America/Chicago"},
    {"city": "Houston", "state": "Texas", "state_abbr": "TX", "county": "Harris County", "population": 2304580, "lat": 29.7604, "lon": -95.3698, "timezone": "America/Chicago"},
    {"city": "Phoenix", "state": "Arizona", "state_abbr": "AZ", "county": "Maricopa County", "population": 1608139, "lat": 33.4484, "lon": -112.0740, "timezone": "America/Phoenix"},
    {"city": "Philadelphia", "state": "Pennsylvania", "state_abbr": "PA", "county": "Philadelphia County", "population": 1584064, "lat": 39.9526, "lon": -75.1652, "timezone": "America/New_York"},
    {"city": "San Antonio", "state": "Texas", "state_abbr": "TX", "county": "Bexar County", "population": 1434625, "lat": 29.4241, "lon": -98.4936, "timezone": "America/Chicago"},
    {"city": "San Diego", "state": "California", "state_abbr": "CA", "county": "San Diego County", "population": 1386932, "lat": 32.7157, "lon": -117.1611, "timezone": "America/Los_Angeles"},
    {"city": "Dallas", "state": "Texas", "state_abbr": "TX", "county": "Dallas County", "population": 1304379, "lat": 32.7767, "lon": -96.7970, "timezone": "America/Chicago"},
    {"city": "San Jose", "state": "California", "state_abbr": "CA", "county": "Santa Clara County", "population": 1013240, "lat": 37.3382, "lon": -121.8863, "timezone": "America/Los_Angeles"},
    {"city": "Austin", "state": "Texas", "state_abbr": "TX", "county": "Travis County", "population": 978908, "lat": 30.2672, "lon": -97.7431, "timezone": "America/Chicago"},
    {"city": "Jacksonville", "state": "Florida", "state_abbr": "FL", "county": "Duval County", "population": 949611, "lat": 30.3322, "lon": -81.6557, "timezone": "America/New_York"},
    {"city": "Fort Worth", "state": "Texas", "state_abbr": "TX", "county": "Tarrant County", "population": 918915, "lat": 32.7555, "lon": -97.3308, "timezone": "America/Chicago"},
    {"city": "Columbus", "state": "Ohio", "state_abbr": "OH", "county": "Franklin County", "population": 905748, "lat": 39.9612, "lon": -82.9988, "timezone": "America/New_York"},
    {"city": "Charlotte", "state": "North Carolina", "state_abbr": "NC", "county": "Mecklenburg County", "population": 885708, "lat": 35.2271, "lon": -80.8431, "timezone": "America/New_York"},
    {"city": "Indianapolis", "state": "Indiana", "state_abbr": "IN", "county": "Marion County", "population": 887642, "lat": 39.7684, "lon": -86.1581, "timezone": "America/Indiana/Indianapolis"},
    {"city": "San Francisco", "state": "California", "state_abbr": "CA", "county": "San Francisco County", "population": 883305, "lat": 37.7749, "lon": -122.4194, "timezone": "America/Los_Angeles"},
    {"city": "Seattle", "state": "Washington", "state_abbr": "WA", "county": "King County", "population": 753675, "lat": 47.6062, "lon": -122.3321, "timezone": "America/Los_Angeles"},
    {"city": "Denver", "state": "Colorado", "state_abbr": "CO", "county": "Denver County", "population": 715522, "lat": 39.7392, "lon": -104.9903, "timezone": "America/Denver"},
    {"city": "Nashville", "state": "Tennessee", "state_abbr": "TN", "county": "Davidson County", "population": 689447, "lat": 36.1627, "lon": -86.7816, "timezone": "America/Chicago"},
    {"city": "Oklahoma City", "state": "Oklahoma", "state_abbr": "OK", "county": "Oklahoma County", "population": 681054, "lat": 35.4676, "lon": -97.5164, "timezone": "America/Chicago"},
    {"city": "El Paso", "state": "Texas", "state_abbr": "TX", "county": "El Paso County", "population": 678815, "lat": 31.7619, "lon": -106.4850, "timezone": "America/Denver"},
    {"city": "Washington", "state": "District of Columbia", "state_abbr": "DC", "county": "District of Columbia", "population": 705749, "lat": 38.9072, "lon": -77.0369, "timezone": "America/New_York"},
    {"city": "Las Vegas", "state": "Nevada", "state_abbr": "NV", "county": "Clark County", "population": 641903, "lat": 36.1699, "lon": -115.1398, "timezone": "America/Los_Angeles"},
    {"city": "Louisville", "state": "Kentucky", "state_abbr": "KY", "county": "Jefferson County", "population": 633045, "lat": 38.2527, "lon": -85.7585, "timezone": "America/Kentucky/Louisville"},
    {"city": "Memphis", "state": "Tennessee", "state_abbr": "TN", "county": "Shelby County", "population": 633104, "lat": 35.1495, "lon": -90.0490, "timezone": "America/Chicago"},
    {"city": "Portland", "state": "Oregon", "state_abbr": "OR", "county": "Multnomah County", "population": 652503, "lat": 45.5051, "lon": -122.6750, "timezone": "America/Los_Angeles"},
    {"city": "Baltimore", "state": "Maryland", "state_abbr": "MD", "county": "Baltimore City", "population": 593490, "lat": 39.2904, "lon": -76.6122, "timezone": "America/New_York"},
    {"city": "Milwaukee", "state": "Wisconsin", "state_abbr": "WI", "county": "Milwaukee County", "population": 577222, "lat": 43.0389, "lon": -87.9065, "timezone": "America/Chicago"},
    {"city": "Albuquerque", "state": "New Mexico", "state_abbr": "NM", "county": "Bernalillo County", "population": 564559, "lat": 35.0844, "lon": -106.6504, "timezone": "America/Denver"},
    {"city": "Tucson", "state": "Arizona", "state_abbr": "AZ", "county": "Pima County", "population": 548073, "lat": 32.2226, "lon": -110.9747, "timezone": "America/Phoenix"},
    {"city": "Fresno", "state": "California", "state_abbr": "CA", "county": "Fresno County", "population": 542107, "lat": 36.7378, "lon": -119.7871, "timezone": "America/Los_Angeles"},
    {"city": "Sacramento", "state": "California", "state_abbr": "CA", "county": "Sacramento County", "population": 524943, "lat": 38.5816, "lon": -121.4944, "timezone": "America/Los_Angeles"},
    {"city": "Mesa", "state": "Arizona", "state_abbr": "AZ", "county": "Maricopa County", "population": 504258, "lat": 33.4152, "lon": -111.8315, "timezone": "America/Phoenix"},
    {"city": "Kansas City", "state": "Missouri", "state_abbr": "MO", "county": "Jackson County", "population": 495327, "lat": 39.0997, "lon": -94.5786, "timezone": "America/Chicago"},
    {"city": "Atlanta", "state": "Georgia", "state_abbr": "GA", "county": "Fulton County", "population": 498715, "lat": 33.7490, "lon": -84.3880, "timezone": "America/New_York"},
    {"city": "Omaha", "state": "Nebraska", "state_abbr": "NE", "county": "Douglas County", "population": 486051, "lat": 41.2565, "lon": -95.9345, "timezone": "America/Chicago"},
    {"city": "Colorado Springs", "state": "Colorado", "state_abbr": "CO", "county": "El Paso County", "population": 478961, "lat": 38.8339, "lon": -104.8214, "timezone": "America/Denver"},
    {"city": "Raleigh", "state": "North Carolina", "state_abbr": "NC", "county": "Wake County", "population": 467665, "lat": 35.7796, "lon": -78.6382, "timezone": "America/New_York"},
    {"city": "Long Beach", "state": "California", "state_abbr": "CA", "county": "Los Angeles County", "population": 466742, "lat": 33.7701, "lon": -118.1937, "timezone": "America/Los_Angeles"},
    {"city": "Virginia Beach", "state": "Virginia", "state_abbr": "VA", "county": "Virginia Beach City", "population": 459470, "lat": 36.8529, "lon": -75.9780, "timezone": "America/New_York"},
    {"city": "Minneapolis", "state": "Minnesota", "state_abbr": "MN", "county": "Hennepin County", "population": 429606, "lat": 44.9778, "lon": -93.2650, "timezone": "America/Chicago"},
    {"city": "Tampa", "state": "Florida", "state_abbr": "FL", "county": "Hillsborough County", "population": 399700, "lat": 27.9506, "lon": -82.4572, "timezone": "America/New_York"},
    {"city": "New Orleans", "state": "Louisiana", "state_abbr": "LA", "county": "Orleans Parish", "population": 383997, "lat": 29.9511, "lon": -90.0715, "timezone": "America/Chicago"},
    {"city": "Arlington", "state": "Texas", "state_abbr": "TX", "county": "Tarrant County", "population": 398854, "lat": 32.7357, "lon": -97.1081, "timezone": "America/Chicago"},
    {"city": "Bakersfield", "state": "California", "state_abbr": "CA", "county": "Kern County", "population": 383579, "lat": 35.3733, "lon": -119.0187, "timezone": "America/Los_Angeles"},
    {"city": "Honolulu", "state": "Hawaii", "state_abbr": "HI", "county": "Honolulu County", "population": 350964, "lat": 21.3069, "lon": -157.8583, "timezone": "Pacific/Honolulu"},
    {"city": "Anaheim", "state": "California", "state_abbr": "CA", "county": "Orange County", "population": 346824, "lat": 33.8366, "lon": -117.9143, "timezone": "America/Los_Angeles"},
    {"city": "Aurora", "state": "Colorado", "state_abbr": "CO", "county": "Arapahoe County", "population": 366623, "lat": 39.7294, "lon": -104.8319, "timezone": "America/Denver"},
    {"city": "Santa Ana", "state": "California", "state_abbr": "CA", "county": "Orange County", "population": 310227, "lat": 33.7455, "lon": -117.8677, "timezone": "America/Los_Angeles"},
    {"city": "Corpus Christi", "state": "Texas", "state_abbr": "TX", "county": "Nueces County", "population": 326586, "lat": 27.8006, "lon": -97.3964, "timezone": "America/Chicago"},
    {"city": "Riverside", "state": "California", "state_abbr": "CA", "county": "Riverside County", "population": 331360, "lat": 33.9533, "lon": -117.3962, "timezone": "America/Los_Angeles"},
    {"city": "Lexington", "state": "Kentucky", "state_abbr": "KY", "county": "Fayette County", "population": 323152, "lat": 38.0406, "lon": -84.5037, "timezone": "America/New_York"},
    {"city": "Pittsburgh", "state": "Pennsylvania", "state_abbr": "PA", "county": "Allegheny County", "population": 302971, "lat": 40.4406, "lon": -79.9959, "timezone": "America/New_York"},
    {"city": "Stockton", "state": "California", "state_abbr": "CA", "county": "San Joaquin County", "population": 320804, "lat": 37.9577, "lon": -121.2908, "timezone": "America/Los_Angeles"},
    {"city": "Cincinnati", "state": "Ohio", "state_abbr": "OH", "county": "Hamilton County", "population": 309317, "lat": 39.1031, "lon": -84.5120, "timezone": "America/New_York"},
    {"city": "St. Paul", "state": "Minnesota", "state_abbr": "MN", "county": "Ramsey County", "population": 308096, "lat": 44.9537, "lon": -93.0900, "timezone": "America/Chicago"},
    {"city": "Toledo", "state": "Ohio", "state_abbr": "OH", "county": "Lucas County", "population": 270871, "lat": 41.6528, "lon": -83.5379, "timezone": "America/New_York"},
    {"city": "Greensboro", "state": "North Carolina", "state_abbr": "NC", "county": "Guilford County", "population": 299035, "lat": 36.0726, "lon": -79.7920, "timezone": "America/New_York"},
    {"city": "Newark", "state": "New Jersey", "state_abbr": "NJ", "county": "Essex County", "population": 311549, "lat": 40.7357, "lon": -74.1724, "timezone": "America/New_York"},
    {"city": "Plano", "state": "Texas", "state_abbr": "TX", "county": "Collin County", "population": 288061, "lat": 33.0198, "lon": -96.6989, "timezone": "America/Chicago"},
    {"city": "Henderson", "state": "Nevada", "state_abbr": "NV", "county": "Clark County", "population": 320189, "lat": 36.0395, "lon": -114.9817, "timezone": "America/Los_Angeles"},
    {"city": "Orlando", "state": "Florida", "state_abbr": "FL", "county": "Orange County", "population": 307573, "lat": 28.5383, "lon": -81.3792, "timezone": "America/New_York"},
    {"city": "St. Louis", "state": "Missouri", "state_abbr": "MO", "county": "St. Louis City", "population": 302838, "lat": 38.6270, "lon": -90.1994, "timezone": "America/Chicago"},
    {"city": "Buffalo", "state": "New York", "state_abbr": "NY", "county": "Erie County", "population": 278349, "lat": 42.8864, "lon": -78.8784, "timezone": "America/New_York"},
    {"city": "Madison", "state": "Wisconsin", "state_abbr": "WI", "county": "Dane County", "population": 269840, "lat": 43.0731, "lon": -89.4012, "timezone": "America/Chicago"},
    {"city": "Lubbock", "state": "Texas", "state_abbr": "TX", "county": "Lubbock County", "population": 258862, "lat": 33.5779, "lon": -101.8552, "timezone": "America/Chicago"},
    {"city": "Chandler", "state": "Arizona", "state_abbr": "AZ", "county": "Maricopa County", "population": 261165, "lat": 33.3062, "lon": -111.8413, "timezone": "America/Phoenix"},
    {"city": "Scottsdale", "state": "Arizona", "state_abbr": "AZ", "county": "Maricopa County", "population": 258069, "lat": 33.4942, "lon": -111.9261, "timezone": "America/Phoenix"},
    {"city": "Reno", "state": "Nevada", "state_abbr": "NV", "county": "Washoe County", "population": 255601, "lat": 39.5296, "lon": -119.8138, "timezone": "America/Los_Angeles"},
    {"city": "Glendale", "state": "Arizona", "state_abbr": "AZ", "county": "Maricopa County", "population": 246709, "lat": 33.5387, "lon": -112.1860, "timezone": "America/Phoenix"},
    {"city": "Gilbert", "state": "Arizona", "state_abbr": "AZ", "county": "Maricopa County", "population": 254114, "lat": 33.3528, "lon": -111.7890, "timezone": "America/Phoenix"},
    {"city": "Norfolk", "state": "Virginia", "state_abbr": "VA", "county": "Norfolk City", "population": 238005, "lat": 36.8468, "lon": -76.2852, "timezone": "America/New_York"},
    {"city": "Boise", "state": "Idaho", "state_abbr": "ID", "county": "Ada County", "population": 235684, "lat": 43.6150, "lon": -116.2023, "timezone": "America/Boise"},
    {"city": "Birmingham", "state": "Alabama", "state_abbr": "AL", "county": "Jefferson County", "population": 212237, "lat": 33.5186, "lon": -86.8104, "timezone": "America/Chicago"},
    {"city": "Rochester", "state": "New York", "state_abbr": "NY", "county": "Monroe County", "population": 211328, "lat": 43.1566, "lon": -77.6088, "timezone": "America/New_York"},
    {"city": "Richmond", "state": "Virginia", "state_abbr": "VA", "county": "Richmond City", "population": 226610, "lat": 37.5407, "lon": -77.4360, "timezone": "America/New_York"},
    {"city": "Des Moines", "state": "Iowa", "state_abbr": "IA", "county": "Polk County", "population": 214133, "lat": 41.6005, "lon": -93.6091, "timezone": "America/Chicago"},
    {"city": "Spokane", "state": "Washington", "state_abbr": "WA", "county": "Spokane County", "population": 228989, "lat": 47.6588, "lon": -117.4260, "timezone": "America/Los_Angeles"},
    {"city": "Salt Lake City", "state": "Utah", "state_abbr": "UT", "county": "Salt Lake County", "population": 200544, "lat": 40.7608, "lon": -111.8910, "timezone": "America/Denver"},
    {"city": "Tacoma", "state": "Washington", "state_abbr": "WA", "county": "Pierce County", "population": 217827, "lat": 47.2529, "lon": -122.4443, "timezone": "America/Los_Angeles"},
    {"city": "Little Rock", "state": "Arkansas", "state_abbr": "AR", "county": "Pulaski County", "population": 202591, "lat": 34.7465, "lon": -92.2896, "timezone": "America/Chicago"},
    {"city": "Jackson", "state": "Mississippi", "state_abbr": "MS", "county": "Hinds County", "population": 153701, "lat": 32.2988, "lon": -90.1848, "timezone": "America/Chicago"},
    {"city": "Knoxville", "state": "Tennessee", "state_abbr": "TN", "county": "Knox County", "population": 190740, "lat": 35.9606, "lon": -83.9207, "timezone": "America/New_York"},
    {"city": "Worcester", "state": "Massachusetts", "state_abbr": "MA", "county": "Worcester County", "population": 185877, "lat": 42.2626, "lon": -71.8023, "timezone": "America/New_York"},
    {"city": "Providence", "state": "Rhode Island", "state_abbr": "RI", "county": "Providence County", "population": 179883, "lat": 41.8240, "lon": -71.4128, "timezone": "America/New_York"},
    {"city": "Tempe", "state": "Arizona", "state_abbr": "AZ", "county": "Maricopa County", "population": 192364, "lat": 33.4255, "lon": -111.9400, "timezone": "America/Phoenix"},
    {"city": "Fort Collins", "state": "Colorado", "state_abbr": "CO", "county": "Larimer County", "population": 169810, "lat": 40.5853, "lon": -105.0844, "timezone": "America/Denver"},
    {"city": "Cape Coral", "state": "Florida", "state_abbr": "FL", "county": "Lee County", "population": 194016, "lat": 26.5629, "lon": -81.9495, "timezone": "America/New_York"},
    {"city": "Moreno Valley", "state": "California", "state_abbr": "CA", "county": "Riverside County", "population": 208634, "lat": 33.9425, "lon": -117.2297, "timezone": "America/Los_Angeles"},
    {"city": "Eugene", "state": "Oregon", "state_abbr": "OR", "county": "Lane County", "population": 176654, "lat": 44.0521, "lon": -123.0868, "timezone": "America/Los_Angeles"},
    {"city": "Shreveport", "state": "Louisiana", "state_abbr": "LA", "county": "Caddo Parish", "population": 179858, "lat": 32.5252, "lon": -93.7502, "timezone": "America/Chicago"},
    {"city": "Akron", "state": "Ohio", "state_abbr": "OH", "county": "Summit County", "population": 190469, "lat": 41.0814, "lon": -81.5190, "timezone": "America/New_York"},
    {"city": "Yonkers", "state": "New York", "state_abbr": "NY", "county": "Westchester County", "population": 211569, "lat": 40.9312, "lon": -73.8988, "timezone": "America/New_York"},
    {"city": "Columbus", "state": "Georgia", "state_abbr": "GA", "county": "Muscogee County", "population": 206922, "lat": 32.4610, "lon": -84.9877, "timezone": "America/New_York"},
    {"city": "Chattanooga", "state": "Tennessee", "state_abbr": "TN", "county": "Hamilton County", "population": 181099, "lat": 35.0456, "lon": -85.3097, "timezone": "America/New_York"},
    {"city": "Oceanside", "state": "California", "state_abbr": "CA", "county": "San Diego County", "population": 174068, "lat": 33.1959, "lon": -117.3795, "timezone": "America/Los_Angeles"},
    {"city": "Fort Lauderdale", "state": "Florida", "state_abbr": "FL", "county": "Broward County", "population": 182437, "lat": 26.1224, "lon": -80.1373, "timezone": "America/New_York"},
    {"city": "Rancho Cucamonga", "state": "California", "state_abbr": "CA", "county": "San Bernardino County", "population": 177751, "lat": 34.1064, "lon": -117.5931, "timezone": "America/Los_Angeles"},
]


@dataclass
class Location:
    city: str
    state: str
    state_abbr: str
    county: str
    population: int
    lat: float
    lon: float
    timezone: str


@dataclass
class ProgrammaticPage:
    keyword: str
    slug: str
    title: str
    meta_description: str
    h1: str
    page_type: str  # service_page | location_page | faq_page
    intent: str     # transactional | commercial | informational
    target_words: int
    city: str
    state: str
    state_abbr: str
    service: str
    modifier: str = ""
    priority_score: float = 0.0
    status: str = "pending"  # pending | queued | published | failed
    url: str = ""


class ProgrammaticGenerator:
    """Generates location × service × modifier keyword matrices."""

    _INTENT_WEIGHTS = {"transactional": 1.0, "commercial": 0.8, "informational": 0.6}
    _WORD_TARGETS = {"transactional": 900, "commercial": 1200, "informational": 1500}

    def __init__(self, business_id: str, storage_path: str = "data/storage/programmatic/"):
        self.business_id = business_id
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self._matrix_file = self.storage_path / f"{business_id}_matrix.json"
        self._matrix: list[ProgrammaticPage] = self._load()

    def _load(self) -> list[ProgrammaticPage]:
        if self._matrix_file.exists():
            data = json.loads(self._matrix_file.read_text())
            return [ProgrammaticPage(**p) for p in data]
        return []

    def _save(self):
        self._matrix_file.write_text(
            json.dumps([asdict(p) for p in self._matrix], indent=2)
        )

    def load_locations(
        self,
        state: str = None,
        min_population: int = 10_000,
        limit: int = 100,
    ) -> list[Location]:
        """Return US cities filtered by state and minimum population."""
        results = []
        for c in _US_CITIES:
            if state and c["state"].lower() != state.lower() and c["state_abbr"].lower() != state.lower():
                continue
            if c["population"] < min_population:
                continue
            results.append(Location(**c))
        results.sort(key=lambda l: l.population, reverse=True)
        return results[:limit]

    def generate_matrix(
        self,
        services: list[str],
        locations: list[Location],
        modifiers: list[str] = None,
        page_types: list[str] = None,
    ) -> list[ProgrammaticPage]:
        """Generate all service × location combinations."""
        modifiers = modifiers or []
        existing_slugs: set[str] = {p.slug for p in self._matrix}
        new_pages: list[ProgrammaticPage] = []

        for service in services:
            for loc in locations:
                # Base: "{service} {city}" — transactional
                combos = [
                    (f"{service} {loc.city}", "transactional", ""),
                    (f"{service} {loc.city} {loc.state_abbr}", "transactional", ""),
                ]
                # Modifier variants
                for mod in modifiers:
                    if mod in ("near me", "24/7", "emergency", "same day", "best", "top"):
                        combos.append((f"{mod} {service} {loc.city}", "transactional", mod))
                    elif mod in ("cost", "price", "how much", "fee", "quote", "estimate"):
                        combos.append((f"{service} cost {loc.city}", "commercial", mod))
                        combos.append((f"how much does {service} cost in {loc.city}", "commercial", mod))
                    else:
                        combos.append((f"{service} {mod} {loc.city}", "transactional", mod))

                for keyword, intent, modifier in combos:
                    slug = self.generate_slug(keyword)
                    if slug in existing_slugs or self.check_duplicate(slug, existing_slugs):
                        continue
                    existing_slugs.add(slug)
                    priority = (loc.population / 1_000_000) * self._INTENT_WEIGHTS.get(intent, 0.5)
                    page = ProgrammaticPage(
                        keyword=keyword,
                        slug=slug,
                        title=self.generate_title(service, loc.city, loc.state, modifier),
                        meta_description=self.generate_meta(service, loc.city, loc.state, modifier),
                        h1=f"{service.title()} in {loc.city}, {loc.state_abbr}",
                        page_type="service_page" if intent == "transactional" else "location_page",
                        intent=intent,
                        target_words=self._WORD_TARGETS.get(intent, 900),
                        city=loc.city,
                        state=loc.state,
                        state_abbr=loc.state_abbr,
                        service=service,
                        modifier=modifier,
                        priority_score=round(priority, 4),
                        status="pending",
                    )
                    new_pages.append(page)

        new_pages.sort(key=lambda p: p.priority_score, reverse=True)
        self._matrix.extend(new_pages)
        self._save()
        log.info("programmatic.matrix_generated  new=%d  total=%d", len(new_pages), len(self._matrix))
        return new_pages

    def generate_slug(self, keyword: str) -> str:
        slug = keyword.lower()
        slug = re.sub(r"[^a-z0-9\s-]", "", slug)
        slug = re.sub(r"\s+", "-", slug.strip())
        slug = re.sub(r"-+", "-", slug)
        return slug[:70]

    def generate_title(self, service: str, city: str, state: str, modifier: str = "") -> str:
        if modifier in ("cost", "price", "how much", "fee"):
            title = f"{service.title()} Cost in {city} | Free Estimates"
        elif modifier in ("emergency", "24/7", "same day"):
            title = f"24/7 {service.title()} in {city}, {state}"
        elif modifier == "best":
            title = f"Best {service.title()} in {city} | Top-Rated Experts"
        else:
            title = f"{service.title()} in {city}, {state} | Expert Service"
        return title[:60]

    def generate_meta(self, service: str, city: str, state: str, modifier: str = "") -> str:
        if modifier in ("cost", "price", "how much"):
            meta = f"Find out how much {service} costs in {city}, {state}. Get free quotes from licensed experts. Transparent pricing, no surprises."
        else:
            meta = f"Need {service} in {city}? Licensed, insured experts serving {city} and surrounding {state} communities. Call for same-day service."
        return meta[:155]

    def to_publish_calendar(
        self, pages: list[ProgrammaticPage], pages_per_day: int = 10
    ) -> list[dict]:
        sorted_pages = sorted(pages, key=lambda p: p.priority_score, reverse=True)
        calendar = []
        today = date.today()
        for i, page in enumerate(sorted_pages):
            day = today + timedelta(days=i // pages_per_day)
            if not calendar or calendar[-1]["date"] != day.isoformat():
                calendar.append({"date": day.isoformat(), "pages": []})
            calendar[-1]["pages"].append(asdict(page))
        return calendar

    def mark_published(self, slug: str, url: str):
        for page in self._matrix:
            if page.slug == slug:
                page.status = "published"
                page.url = url
                break
        self._save()

    def mark_failed(self, slug: str):
        for page in self._matrix:
            if page.slug == slug:
                page.status = "failed"
                break
        self._save()

    def get_pending(self, limit: int = 50) -> list[ProgrammaticPage]:
        pending = [p for p in self._matrix if p.status == "pending"]
        pending.sort(key=lambda p: p.priority_score, reverse=True)
        return pending[:limit]

    def get_stats(self) -> dict:
        stats: dict = {"total": len(self._matrix), "by_status": {}, "by_state": {}, "by_service": {}}
        for page in self._matrix:
            stats["by_status"][page.status] = stats["by_status"].get(page.status, 0) + 1
            stats["by_state"][page.state_abbr] = stats["by_state"].get(page.state_abbr, 0) + 1
            stats["by_service"][page.service] = stats["by_service"].get(page.service, 0) + 1
        for k in ("pending", "queued", "published", "failed"):
            stats[k] = stats["by_status"].get(k, 0)
        return stats

    def check_duplicate(self, slug: str, existing_slugs: set) -> bool:
        return slug in existing_slugs
