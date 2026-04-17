import hashlib, json, logging, re
from datetime import date
from typing import Dict, List

import redis

log = logging.getLogger(__name__)
_redis = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)

REQUIRED_FIELDS = {
    "Article": ["headline", "author", "datePublished", "description"],
    "LocalBusiness": ["name", "address", "telephone"],
    "FAQPage": ["mainEntity"],
    "HowTo": ["name", "step"],
    "Product": ["name", "offers"],
    "BreadcrumbList": ["itemListElement"],
    "WebPage": ["name", "url"],
}

def extract_jsonld(html: str) -> List[Dict]:
    schemas = []
    pattern = re.compile(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', re.S | re.I)
    for match in pattern.finditer(html):
        try:
            obj = json.loads(match.group(1).strip())
            if isinstance(obj, list):
                schemas.extend(obj)
            else:
                schemas.append(obj)
        except json.JSONDecodeError:
            pass
    return schemas

def check_required_fields(schema_obj: Dict) -> List[str]:
    schema_type = schema_obj.get("@type", "")
    if isinstance(schema_type, list):
        schema_type = schema_type[0]
    required = REQUIRED_FIELDS.get(schema_type, [])
    return [f for f in required if f not in schema_obj]

def auto_fix_trivial(schema_obj: Dict) -> Dict:
    result = dict(schema_obj)
    schema_type = result.get("@type", "")
    if "dateModified" not in result and schema_type in ("Article", "WebPage", "BlogPosting"):
        result["dateModified"] = date.today().isoformat()
    if "inLanguage" not in result and schema_type in ("Article", "WebPage", "BlogPosting"):
        result["inLanguage"] = "en"
    if "@context" not in result:
        result["@context"] = "https://schema.org"
    return result

def validate_schema(html_content: str, url: str = "") -> Dict:
    cache_key = f"schema_val:{hashlib.sha256(html_content[:2000].encode()).hexdigest()[:16]}"
    cached = _redis.get(cache_key)
    if cached:
        return json.loads(cached)

    schemas = extract_jsonld(html_content)
    errors = []
    warnings = []
    fixed_schemas = []

    for i, schema in enumerate(schemas):
        schema_type = schema.get("@type", "unknown")
        missing = check_required_fields(schema)
        if missing:
            errors.append({"schema_index": i, "type": schema_type, "missing_fields": missing})
        fixed = auto_fix_trivial(schema)
        if fixed != schema:
            warnings.append({"schema_index": i, "type": schema_type, "auto_fixed": True})
        fixed_schemas.append(fixed)

    result = {
        "url": url,
        "schema_count": len(schemas),
        "errors": errors,
        "warnings": warnings,
        "passed": len(errors) == 0,
        "fixed_schemas": fixed_schemas,
    }
    _redis.setex(cache_key, 86400, json.dumps(result))
    log.info("schema_validator.validate  url=%s  schemas=%d  errors=%d", url[:60], len(schemas), len(errors))
    return result
