"""Wikidata entity creation and sameAs validation pipeline.

Creates structured entity entries for local businesses on Wikidata
using QuickStatements batch format. Also validates existing sameAs
references across GBP, LinkedIn, Yelp, BBB, and Wikidata.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

log = logging.getLogger(__name__)


@dataclass
class WikidataEntity:
    qid: str = ""           # Wikidata QID (e.g. Q12345678)
    label: str = ""         # Business name
    description: str = ""   # Short description
    instance_of: str = "Q4830453"  # business (default)
    country: str = "Q30"    # United States
    city_qid: str = ""      # Wikidata QID for city
    website: str = ""
    phone: str = ""
    founded: str = ""
    address: str = ""
    same_as: list[str] = field(default_factory=list)
    created_at: str = ""
    validated: bool = False


# Wikidata QIDs for common US cities
CITY_QIDS = {
    "new york": "Q60",
    "los angeles": "Q65",
    "chicago": "Q1297",
    "houston": "Q16555",
    "phoenix": "Q16556",
    "philadelphia": "Q1345",
    "san antonio": "Q975",
    "san diego": "Q16561",
    "dallas": "Q16557",
    "san jose": "Q16553",
    "austin": "Q16559",
    "jacksonville": "Q16575",
    "fort worth": "Q128306",
    "columbus": "Q16564",
    "charlotte": "Q16562",
    "san francisco": "Q62",
    "indianapolis": "Q6346",
    "seattle": "Q5083",
    "denver": "Q16554",
    "nashville": "Q23197",
    "oklahoma city": "Q34404",
    "el paso": "Q16567",
    "washington": "Q61",
    "las vegas": "Q16563",
    "louisville": "Q40231",
    "memphis": "Q34404",
    "portland": "Q6106",
    "baltimore": "Q5092",
    "milwaukee": "Q37836",
    "albuquerque": "Q34404",
    "tucson": "Q16566",
    "fresno": "Q34404",
    "mesa": "Q35794",
    "sacramento": "Q16568",
    "atlanta": "Q23556",
    "kansas city": "Q41816",
    "omaha": "Q22812",
    "colorado springs": "Q49258",
    "raleigh": "Q34404",
    "long beach": "Q34404",
    "virginia beach": "Q34404",
    "minneapolis": "Q36091",
    "tampa": "Q49297",
    "new orleans": "Q34404",
    "arlington": "Q34404",
    "bakersfield": "Q34404",
    "honolulu": "Q18094",
    "anaheim": "Q34404",
    "aurora": "Q34404",
    "santa ana": "Q34404",
    "corpus christi": "Q34404",
    "riverside": "Q34404",
    "lexington": "Q34404",
    "st. louis": "Q38022",
    "pittsburg": "Q34404",
    "pittsburgh": "Q34404",
    "anchorage": "Q34404",
    "stockton": "Q34404",
    "cincinnati": "Q43196",
    "st. paul": "Q34404",
    "greensboro": "Q34404",
    "toledo": "Q34404",
    "newark": "Q34404",
    "plano": "Q34404",
    "henderson": "Q34404",
    "orlando": "Q34404",
    "lincoln": "Q34404",
    "jersey city": "Q34404",
    "chandler": "Q34404",
    "fort wayne": "Q34404",
    "buffalo": "Q40435",
    "durham": "Q34404",
    "st. petersburg": "Q34404",
    "irvine": "Q34404",
    "laredo": "Q34404",
    "lubbock": "Q34404",
    "madison": "Q34404",
    "gilbert": "Q34404",
    "garland": "Q34404",
    "glendale": "Q34404",
    "reno": "Q34404",
    "hialeah": "Q34404",
    "baton rouge": "Q34404",
    "richmond": "Q34404",
    "boise": "Q34404",
    "spokane": "Q34404",
    "des moines": "Q34404",
    "tacoma": "Q34404",
    "san bernardino": "Q34404",
    "fremont": "Q34404",
    "modesto": "Q34404",
    "fontana": "Q34404",
    "santa clarita": "Q34404",
    "moreno valley": "Q34404",
    "fayetteville": "Q34404",
    "glendale az": "Q34404",
    "masterston": "Q34404",
    "akron": "Q34404",
    "yonkers": "Q34404",
    "huntington beach": "Q34404",
    "little rock": "Q34404",
    "salt lake city": "Q23337",
    "tallahassee": "Q34404",
    "worcester": "Q34404",
}

# Instance-of QIDs for common business types
BUSINESS_TYPE_QIDS = {
    "plumber": "Q15893660",
    "plumbing": "Q15893660",
    "hvac": "Q179826",
    "electrician": "Q165029",
    "electrical": "Q165029",
    "roofing": "Q34404",
    "roofer": "Q34404",
    "landscaping": "Q34404",
    "pest control": "Q34404",
    "cleaning": "Q34404",
    "cleaning service": "Q34404",
    "painting": "Q34404",
    "contractor": "Q34404",
    "construction": "Q34404",
    "restaurant": "Q11707",
    "hotel": "Q27686",
    "dentist": "Q27349",
    "doctor": "Q39631",
    "law firm": "Q613142",
    "lawyer": "Q40348",
    "real estate": "Q44628",
}


class WikidataBuilder:
    """Creates and manages Wikidata entity entries for local businesses."""

    def __init__(self, wikidata_username: str = "", wikidata_password: str = ""):
        self.username = wikidata_username
        self.password = wikidata_password

    def build_entity(self, business: dict) -> WikidataEntity:
        """Build a WikidataEntity from business config dict."""
        name = business.get("name", "")
        service_type = business.get("service_type", "").lower()
        city = business.get("city", "").lower()
        state = business.get("state", "")
        website = business.get("website", "")
        phone = business.get("phone", "")
        founded = str(business.get("founded_year", ""))

        # Get instance_of QID
        instance_of = "Q4830453"  # generic business
        for key, qid in BUSINESS_TYPE_QIDS.items():
            if key in service_type:
                instance_of = qid
                break

        # Get city QID
        city_qid = CITY_QIDS.get(city, "")

        # Build description
        description = (
            f"{service_type.title()} company in {city.title()}, {state}"
            if city and state else f"{service_type.title()} company"
        )

        # Collect sameAs URLs from business config
        same_as = []
        for key in ["gbp_url", "linkedin_url", "yelp_url", "bbb_url", "facebook_url"]:
            val = business.get(key, "")
            if val:
                same_as.append(val)

        return WikidataEntity(
            label=name,
            description=description,
            instance_of=instance_of,
            country="Q30",
            city_qid=city_qid,
            website=website,
            phone=phone,
            founded=founded,
            same_as=same_as,
        )

    def to_quickstatements(self, entity: WikidataEntity) -> str:
        """Generate QuickStatements v2 batch format for entity creation."""
        lines = []
        qid = entity.qid or "CREATE"

        # If creating new entity
        if not entity.qid:
            lines.append("CREATE")
            qid = "LAST"

        # Label (English)
        if entity.label:
            lines.append(f'{qid}|Len|"{entity.label}"')

        # Description (English)
        if entity.description:
            lines.append(f'{qid}|Den|"{entity.description}"')

        # instance of (P31)
        if entity.instance_of:
            lines.append(f"{qid}|P31|{entity.instance_of}")

        # country (P17)
        if entity.country:
            lines.append(f"{qid}|P17|{entity.country}")

        # located in (P131) — city
        if entity.city_qid and entity.city_qid != "Q34404":  # skip generic placeholder
            lines.append(f"{qid}|P131|{entity.city_qid}")

        # official website (P856)
        if entity.website:
            lines.append(f'{qid}|P856|"{entity.website}"')

        # phone number (P1329)
        if entity.phone:
            phone_clean = re.sub(r"[^\d+]", "", entity.phone)
            lines.append(f'{qid}|P1329|"{phone_clean}"')

        # inception date (P571)
        if entity.founded and entity.founded.isdigit():
            lines.append(f"{qid}|P571|+{entity.founded}-00-00T00:00:00Z/9")

        # same as (P2888) — exact match
        for url in entity.same_as:
            lines.append(f'{qid}|P2888|"{url}"')

        return "\n".join(lines)

    async def check_exists(self, business_name: str, city: str) -> str | None:
        """Search Wikidata SPARQL for existing entity by name + city."""
        query = f"""
SELECT ?item WHERE {{
  ?item rdfs:label "{business_name}"@en .
  ?item wdt:P31 wd:Q4830453 .
}}
LIMIT 5
"""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.get(
                    "https://query.wikidata.org/sparql",
                    params={"query": query, "format": "json"},
                    headers={"User-Agent": "SEOEngine/1.0 (https://github.com/seo-engine)"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    bindings = data.get("results", {}).get("bindings", [])
                    if bindings:
                        qid_url = bindings[0]["item"]["value"]
                        qid = qid_url.split("/")[-1]
                        log.info("wikidata.exists  name=%s  qid=%s", business_name, qid)
                        return qid
        except Exception as e:
            log.warning("wikidata.check_fail  err=%s", e)
        return None

    async def check_notability(self, business: dict) -> tuple[bool, str]:
        """Assess if business meets basic Wikidata notability criteria."""
        reasons: list[str] = []
        points = 0

        # Years in business
        years = business.get("years_in_business", 0)
        if isinstance(years, str) and years.isdigit():
            years = int(years)
        if years >= 10:
            points += 2
            reasons.append(f"10+ years in business ({years} years)")
        elif years >= 5:
            points += 1
            reasons.append(f"5+ years in business ({years} years)")

        # Has website
        if business.get("website"):
            points += 1
            reasons.append("has official website")

        # Has GBP
        if business.get("gbp_url"):
            points += 1
            reasons.append("has Google Business Profile")

        # Has BBB
        if business.get("bbb_url"):
            points += 2
            reasons.append("listed on BBB")

        # Has press mentions or media coverage
        if business.get("press_mentions", 0) > 0:
            points += 2
            reasons.append(f"{business.get('press_mentions')} press mentions")

        # Has employee count
        employees = business.get("employee_count", 0)
        if employees >= 10:
            points += 1
            reasons.append(f"{employees}+ employees")

        notable = points >= 3
        reason_str = "; ".join(reasons) if reasons else "insufficient notability signals"
        return notable, reason_str

    def save_entity(self, entity: WikidataEntity, storage_path: str = "data/storage/wikidata/"):
        """Save entity data and QuickStatements to disk."""
        path = Path(storage_path)
        path.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

        # Save entity JSON
        entity_dict = {
            "qid": entity.qid,
            "label": entity.label,
            "description": entity.description,
            "instance_of": entity.instance_of,
            "country": entity.country,
            "city_qid": entity.city_qid,
            "website": entity.website,
            "phone": entity.phone,
            "founded": entity.founded,
            "same_as": entity.same_as,
            "created_at": entity.created_at,
            "validated": entity.validated,
        }
        (path / f"entity_{ts}.json").write_text(json.dumps(entity_dict, indent=2))

        # Save QuickStatements
        qs = self.to_quickstatements(entity)
        (path / f"quickstatements_{ts}.txt").write_text(qs)
        log.info("wikidata.saved  label=%s  qs_lines=%d", entity.label, len(qs.splitlines()))


class SameAsValidator:
    """Validates sameAs references across platforms for schema.org structured data."""

    PLATFORMS = {
        "google_business": r"business\.google\.com|maps\.google\.com/maps/place",
        "yelp": r"yelp\.com/biz/",
        "bbb": r"bbb\.org/",
        "linkedin": r"linkedin\.com/company/",
        "facebook": r"facebook\.com/",
        "instagram": r"instagram\.com/",
        "twitter": r"twitter\.com/|x\.com/",
        "angi": r"angi\.com|angie",
        "houzz": r"houzz\.com/",
        "homeadvisor": r"homeadvisor\.com/",
    }

    async def validate_urls(self, urls: list[str]) -> dict[str, dict]:
        """Check each sameAs URL is reachable and returns 200."""
        results = {}
        tasks = [self._check_url(url) for url in urls]
        checks = await asyncio.gather(*tasks, return_exceptions=True)

        for url, check in zip(urls, checks):
            platform = self._detect_platform(url)
            if isinstance(check, Exception):
                results[url] = {"platform": platform, "reachable": False, "status": 0, "error": str(check)}
            else:
                results[url] = {"platform": platform, "reachable": check[0], "status": check[1]}

        return results

    async def _check_url(self, url: str) -> tuple[bool, int]:
        try:
            import httpx
            async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
                resp = await client.head(url)
                return resp.status_code < 400, resp.status_code
        except Exception as e:
            return False, 0

    def _detect_platform(self, url: str) -> str:
        for platform, pattern in self.PLATFORMS.items():
            if re.search(pattern, url, re.IGNORECASE):
                return platform
        return "unknown"

    def build_schema_same_as(self, valid_urls: list[str]) -> list[str]:
        """Return filtered list of valid sameAs URLs for schema.org markup."""
        return [u for u in valid_urls if u.startswith("http")]

    def generate_schema_block(self, business: dict, same_as_urls: list[str]) -> dict:
        """Generate schema.org LocalBusiness JSON-LD with sameAs."""
        schema = {
            "@context": "https://schema.org",
            "@type": "LocalBusiness",
            "name": business.get("name", ""),
            "url": business.get("website", ""),
            "telephone": business.get("phone", ""),
            "address": {
                "@type": "PostalAddress",
                "addressLocality": business.get("city", ""),
                "addressRegion": business.get("state", ""),
                "addressCountry": "US",
            },
        }
        if same_as_urls:
            schema["sameAs"] = same_as_urls
        if business.get("description"):
            schema["description"] = business["description"]
        if business.get("gbp_rating"):
            schema["aggregateRating"] = {
                "@type": "AggregateRating",
                "ratingValue": business["gbp_rating"],
                "reviewCount": business.get("review_count", 1),
            }
        return schema



# ---------------------------------------------------------------------------
# Wikidata MediaWiki API — auto-creation (requires bot account credentials)
# ---------------------------------------------------------------------------

class WikidataAPI:
    """Submit entities directly to Wikidata via MediaWiki API.

    Requires a Wikidata bot account:
      WIKIDATA_USERNAME=MyBot@my-bot-password-name
      WIKIDATA_PASSWORD=the-bot-password

    Set these in config/.env or via credential_vault.
    """

    API = "https://www.wikidata.org/w/api.php"
    UA  = "SEOEngine/1.0 (https://gethubed.com; contact@gethubed.com)"

    def __init__(self, username: str = "", password: str = ""):
        import os
        self.username = username or os.getenv("WIKIDATA_USERNAME", "")
        self.password = password or os.getenv("WIKIDATA_PASSWORD", "")
        self._session = None
        self._csrf_token = None

    def is_configured(self) -> bool:
        return bool(self.username and self.password)

    def _get_session(self):
        import requests
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({"User-Agent": self.UA})
        return self._session

    def login(self) -> bool:
        """Authenticate and store session cookies + CSRF token."""
        sess = self._get_session()
        try:
            # Step 1: get login token
            r = sess.get(self.API, params={
                "action": "query", "meta": "tokens",
                "type": "login", "format": "json"
            }, timeout=15)
            r.raise_for_status()
            login_token = r.json()["query"]["tokens"]["logintoken"]

            # Step 2: login
            r = sess.post(self.API, data={
                "action": "login",
                "lgname": self.username,
                "lgpassword": self.password,
                "lgtoken": login_token,
                "format": "json",
            }, timeout=15)
            r.raise_for_status()
            result = r.json().get("login", {}).get("result", "")
            if result != "Success":
                log.error("wikidata_api.login_fail  result=%s", result)
                return False

            # Step 3: get CSRF token for edits
            r = sess.get(self.API, params={
                "action": "query", "meta": "tokens", "format": "json"
            }, timeout=15)
            r.raise_for_status()
            self._csrf_token = r.json()["query"]["tokens"]["csrftoken"]
            log.info("wikidata_api.login_ok  user=%s", self.username)
            return True
        except Exception as exc:
            log.error("wikidata_api.login_error  err=%s", exc)
            return False

    def create_item(self, entity: "WikidataEntity") -> str:
        """Create a new Wikidata item. Returns QID string (e.g. 'Q12345678') or ''."""
        if not self._csrf_token:
            if not self.login():
                return ""

        import json as _json
        sess = self._get_session()

        # Build item data with labels, descriptions, and core claims
        item_data: dict = {
            "labels": {"en": {"language": "en", "value": entity.label}},
            "descriptions": {"en": {"language": "en", "value": entity.description}},
            "claims": {},
        }

        def _item_claim(prop: str, qid: str) -> dict:
            return {
                "mainsnak": {
                    "snaktype": "value", "property": prop,
                    "datavalue": {"type": "wikibase-entityid",
                                  "value": {"entity-type": "item", "id": qid}},
                },
                "type": "statement", "rank": "normal",
            }

        def _string_claim(prop: str, value: str) -> dict:
            return {
                "mainsnak": {
                    "snaktype": "value", "property": prop,
                    "datavalue": {"type": "string", "value": value},
                },
                "type": "statement", "rank": "normal",
            }

        def _url_claim(prop: str, url: str) -> dict:
            return {
                "mainsnak": {
                    "snaktype": "value", "property": prop,
                    "datavalue": {"type": "string", "value": url},
                },
                "type": "statement", "rank": "normal",
            }

        # P31 — instance of
        if entity.instance_of:
            item_data["claims"]["P31"] = [_item_claim("P31", entity.instance_of)]

        # P17 — country (US)
        item_data["claims"]["P17"] = [_item_claim("P17", entity.country or "Q30")]

        # P131 — located in (city) — skip generic Q34404 placeholder
        if entity.city_qid and entity.city_qid != "Q34404":
            item_data["claims"]["P131"] = [_item_claim("P131", entity.city_qid)]

        # P856 — official website
        if entity.website:
            website = entity.website if entity.website.startswith("http") else "https://" + entity.website
            item_data["claims"]["P856"] = [_url_claim("P856", website)]

        # P1329 — phone number
        if entity.phone:
            import re as _re
            phone_clean = _re.sub(r"[^\d+]", "", entity.phone)
            if phone_clean:
                item_data["claims"]["P1329"] = [_string_claim("P1329", phone_clean)]

        # P2888 — exact match / sameAs
        if entity.same_as:
            item_data["claims"]["P2888"] = [_url_claim("P2888", url) for url in entity.same_as[:5]]

        try:
            r = sess.post(self.API, data={
                "action": "wbeditentity",
                "new": "item",
                "data": _json.dumps(item_data),
                "token": self._csrf_token,
                "format": "json",
                "summary": "Entity created by SEOEngine automated pipeline",
            }, timeout=30)
            r.raise_for_status()
            resp = r.json()
            if "error" in resp:
                log.error("wikidata_api.create_error  code=%s  info=%s",
                          resp["error"].get("code"), resp["error"].get("info"))
                return ""
            qid = resp.get("entity", {}).get("id", "")
            log.info("wikidata_api.created  label=%s  qid=%s", entity.label, qid)
            return qid
        except Exception as exc:
            log.error("wikidata_api.create_fail  label=%s  err=%s", entity.label, exc)
            return ""

    def add_inception_date(self, qid: str, year: str) -> bool:
        """Add P571 (inception/founded) date claim to existing item."""
        if not year or not year.isdigit() or not self._csrf_token:
            return False
        sess = self._get_session()
        import json as _json
        claim_data = {
            "mainsnak": {
                "snaktype": "value", "property": "P571",
                "datavalue": {
                    "type": "time",
                    "value": {
                        "time": "+" + year + "-00-00T00:00:00Z",
                        "timezone": 0, "before": 0, "after": 0,
                        "precision": 9, "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
                    },
                },
            },
            "type": "statement", "rank": "normal",
        }
        try:
            r = sess.post(self.API, data={
                "action": "wbcreateclaim",
                "entity": qid,
                "snaktype": "value",
                "property": "P571",
                "value": _json.dumps(claim_data["mainsnak"]["datavalue"]["value"]),
                "token": self._csrf_token,
                "format": "json",
            }, timeout=15)
            r.raise_for_status()
            return "claim" in r.json()
        except Exception as exc:
            log.warning("wikidata_api.inception_fail  qid=%s  err=%s", qid, exc)
            return False


def _store_qid(business_id: str, qid: str) -> None:
    """Persist Wikidata QID back to brand_entities SQLite table."""
    import sqlite3
    from datetime import datetime as _dt
    db_path = "data/storage/seo_engine.db"
    try:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "UPDATE brand_entities SET wikidata_qid=?, updated_at=? WHERE business_id=?",
            [qid, _dt.utcnow().isoformat(), business_id],
        )
        conn.commit()
        conn.close()
        log.info("wikidata.qid_stored  business_id=%s  qid=%s", business_id, qid)
    except Exception as exc:
        log.warning("wikidata.qid_store_fail  err=%s", exc)


async def run_entity_pipeline(business: dict) -> dict:
    """Full Wikidata entity creation pipeline for a business.

    Flow:
      1. Notability check
      2. Check if already exists on Wikidata (SPARQL search)
      3. If credentials set: auto-create via MediaWiki API → get QID
      4. If no credentials: generate QuickStatements file for manual submission
      5. Store QID in brand_entities table
    """
    builder = WikidataBuilder()
    api = WikidataAPI()

    # 1. Notability check
    notable, reason = await builder.check_notability(business)
    if not notable:
        log.info("wikidata.skip  name=%s  reason=%s", business.get("name"), reason)
        return {"created": False, "reason": reason}

    # 2. Check if already exists
    existing_qid = await builder.check_exists(
        business.get("name", ""), business.get("city", "")
    )
    if existing_qid:
        _store_qid(business.get("business_id", ""), existing_qid)
        log.info("wikidata.exists  name=%s  qid=%s", business.get("name"), existing_qid)
        return {"created": False, "qid": existing_qid, "reason": "already exists"}

    # 3. Build entity + validate sameAs
    entity = builder.build_entity(business)
    entity.created_at = datetime.now(tz=timezone.utc).isoformat()
    validator = SameAsValidator()
    if entity.same_as:
        validation = await validator.validate_urls(entity.same_as)
        entity.same_as = [u for u, info in validation.items() if info.get("reachable")]

    qid = ""

    # 4a. Auto-create via API if credentials configured
    if api.is_configured():
        log.info("wikidata.api_create  name=%s", entity.label)
        qid = api.create_item(entity)
        if qid and entity.founded:
            api.add_inception_date(qid, entity.founded)
        entity.qid = qid

    # 4b. Always save QS file as audit trail / fallback
    builder.save_entity(entity)
    qs = builder.to_quickstatements(entity)

    # 5. Store QID
    if qid:
        _store_qid(business.get("business_id", ""), qid)

    method = "api" if qid else "quickstatements_file"
    log.info(
        "wikidata.pipeline_done  name=%s  qid=%s  method=%s  same_as=%d",
        entity.label, qid or "pending_manual", method, len(entity.same_as),
    )
    return {
        "created": bool(qid),
        "qid": qid,
        "method": method,
        "label": entity.label,
        "same_as_count": len(entity.same_as),
        "quickstatements_lines": len(qs.splitlines()),
        "notable_reason": reason,
        "instructions": "" if qid else "Submit QS file at https://quickstatements.toolforge.org",
    }
