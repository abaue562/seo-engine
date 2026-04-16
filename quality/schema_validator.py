"""
Schema.org JSON-LD validation using Google's Rich Results Test API.

Usage:
    validator = SchemaValidator()
    result = validator.validate_page(url="https://mysite.com/page")
    result = validator.validate_json_ld(json_ld_string)
"""
import json
import logging
import re
from datetime import date
from typing import Union

import httpx

log = logging.getLogger(__name__)

# Required fields per Schema.org @type
_REQUIRED: dict[str, list[str]] = {
    "LocalBusiness":  ["name", "@type"],
    "Organization":   ["name", "url"],
    "FAQPage":        ["mainEntity"],
    "Article":        ["headline", "datePublished"],
    "HowTo":          ["name", "step"],
    "BreadcrumbList": ["itemListElement"],
    "Product":        ["name"],
    "Service":        ["name"],
    "Event":          ["name", "startDate"],
    "Person":         ["name"],
    "WebPage":        ["name"],
    "WebSite":        ["name", "url"],
    "Review":         ["reviewRating", "author"],
}

# Valid Schema.org @context values
_VALID_CONTEXTS = {
    "https://schema.org",
    "http://schema.org",
    "https://schema.org/",
    "http://schema.org/",
}


class SchemaValidator:
    """Validates Schema.org JSON-LD markup both locally and via Google APIs."""

    # Google Rich Results Test API (unofficial but stable)
    RICH_RESULTS_URL = (
        "https://searchconsole.googleapis.com/v1/urlTestingTools/mobileFriendlyTest:run"
    )

    def __init__(self):
        self.client = httpx.Client(timeout=20)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_json_ld(self, json_ld: Union[str, dict]) -> dict:
        """Parse and validate a JSON-LD block locally.

        Checks:
          - Valid JSON
          - @context is schema.org
          - @type is present
          - Required fields for the declared @type
          - sameAs is a list of strings (URLs)

        Returns:
            {
                valid:       bool,
                errors:      list[str],
                warnings:    list[str],
                types_found: list[str],
            }
        """
        errors:   list[str] = []
        warnings: list[str] = []
        types_found: list[str] = []

        # -- Parse --
        if isinstance(json_ld, str):
            try:
                data = json.loads(json_ld)
            except json.JSONDecodeError as exc:
                return {
                    "valid":       False,
                    "errors":      [f"Invalid JSON: {exc}"],
                    "warnings":    [],
                    "types_found": [],
                }
        else:
            data = json_ld

        # Handle @graph (array of entities inside a wrapper)
        items_to_check: list[dict] = []
        if isinstance(data, list):
            items_to_check = data
        elif isinstance(data, dict):
            if "@graph" in data:
                items_to_check = data["@graph"] if isinstance(data["@graph"], list) else [data]
            else:
                items_to_check = [data]
        else:
            return {
                "valid":       False,
                "errors":      ["JSON-LD root must be an object or array"],
                "warnings":    [],
                "types_found": [],
            }

        for block in items_to_check:
            if not isinstance(block, dict):
                errors.append(f"Non-object element in JSON-LD: {type(block).__name__}")
                continue

            # @context check
            ctx = block.get("@context", "")
            if not ctx:
                errors.append("Missing @context")
            elif str(ctx).rstrip("/") not in {c.rstrip("/") for c in _VALID_CONTEXTS}:
                warnings.append(f"Unexpected @context value: {ctx!r}")

            # @type check
            schema_type = block.get("@type", "")
            if not schema_type:
                errors.append("Missing @type")
            else:
                if isinstance(schema_type, list):
                    types_found.extend(schema_type)
                else:
                    types_found.append(schema_type)

                # Required-field check for each type
                all_types = schema_type if isinstance(schema_type, list) else [schema_type]
                for t in all_types:
                    missing = self.check_required_fields(t, block)
                    for field in missing:
                        errors.append(f"[{t}] missing required field: {field!r}")

                # Type-specific deep checks
                for t in all_types:
                    self._deep_check(t, block, errors, warnings)

            # sameAs must be a list of strings
            same_as = block.get("sameAs")
            if same_as is not None:
                if isinstance(same_as, str):
                    warnings.append("sameAs should be a list, not a bare string")
                elif isinstance(same_as, list):
                    for entry in same_as:
                        if not isinstance(entry, str):
                            errors.append(f"sameAs contains non-string value: {entry!r}")
                else:
                    errors.append(f"sameAs has unexpected type: {type(same_as).__name__}")

        valid = len(errors) == 0
        return {
            "valid":       valid,
            "errors":      errors,
            "warnings":    warnings,
            "types_found": list(dict.fromkeys(types_found)),  # deduplicated, order-preserved
        }

    def validate_schema_types(self, page_html: str) -> dict:
        """Extract every JSON-LD block from raw HTML and validate each one.

        Returns:
            {
                blocks_found: int,
                valid:        int,
                invalid:      int,
                errors:       list[str],
                types:        list[str],
            }
        """
        pattern = re.compile(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            re.DOTALL | re.IGNORECASE,
        )
        blocks = pattern.findall(page_html)

        total_valid   = 0
        total_invalid = 0
        all_errors:   list[str] = []
        all_types:    list[str] = []

        for idx, block_text in enumerate(blocks):
            result = self.validate_json_ld(block_text.strip())
            all_types.extend(result.get("types_found", []))

            if result["valid"]:
                total_valid += 1
            else:
                total_invalid += 1
                for err in result["errors"]:
                    all_errors.append(f"Block {idx + 1}: {err}")

        return {
            "blocks_found": len(blocks),
            "valid":        total_valid,
            "invalid":      total_invalid,
            "errors":       all_errors,
            "types":        list(dict.fromkeys(all_types)),
        }

    def check_required_fields(self, schema_type: str, data: dict) -> list[str]:
        """Return a list of required field names that are absent from *data*."""
        required = _REQUIRED.get(schema_type, [])
        return [field for field in required if field not in data or data[field] in (None, "", [])]

    def inject_missing_fields(self, schema: dict, business_data: dict) -> dict:
        """Auto-fill common missing Schema.org fields from *business_data*.

        Fields added when absent:
          - dateModified   → today (ISO date)
          - inLanguage     → "en-US"
          - publisher      → {"@type": "Organization", "name": business_name}
          - url            → from business_data["url"] or business_data["site_url"]
          - author         → {"@type": "Organization", "name": business_name}
          - image          → from business_data["logo"] or business_data["image"]

        Returns the mutated schema dict (also mutates in place for convenience).
        """
        today = date.today().isoformat()
        biz_name = (
            business_data.get("business_name")
            or business_data.get("name")
            or ""
        )
        biz_url = (
            business_data.get("url")
            or business_data.get("site_url")
            or ""
        )

        if "dateModified" not in schema:
            schema["dateModified"] = today

        if "inLanguage" not in schema:
            schema["inLanguage"] = "en-US"

        if "publisher" not in schema and biz_name:
            publisher: dict = {"@type": "Organization", "name": biz_name}
            if biz_url:
                publisher["url"] = biz_url
            logo = business_data.get("logo") or business_data.get("logo_url")
            if logo:
                publisher["logo"] = {"@type": "ImageObject", "url": logo}
            schema["publisher"] = publisher

        if "author" not in schema and biz_name:
            schema["author"] = {"@type": "Organization", "name": biz_name}

        if "url" not in schema and biz_url:
            schema["url"] = biz_url

        image_url = business_data.get("image") or business_data.get("logo")
        if "image" not in schema and image_url:
            schema["image"] = image_url

        return schema

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _deep_check(
        self,
        schema_type: str,
        data: dict,
        errors: list[str],
        warnings: list[str],
    ) -> None:
        """Run type-specific validations beyond required-field presence."""

        if schema_type == "FAQPage":
            main_entity = data.get("mainEntity", [])
            if not isinstance(main_entity, list):
                main_entity = [main_entity]
            for i, q in enumerate(main_entity):
                if not isinstance(q, dict):
                    errors.append(f"FAQPage.mainEntity[{i}] must be an object")
                    continue
                if q.get("@type") != "Question":
                    warnings.append(
                        f"FAQPage.mainEntity[{i}] @type should be 'Question', got {q.get('@type')!r}"
                    )
                answer = q.get("acceptedAnswer", {})
                if not isinstance(answer, dict):
                    errors.append(f"FAQPage.mainEntity[{i}].acceptedAnswer must be an object")
                elif answer.get("@type") != "Answer":
                    warnings.append(
                        f"FAQPage.mainEntity[{i}].acceptedAnswer @type should be 'Answer'"
                    )
                if not answer.get("text"):
                    errors.append(
                        f"FAQPage.mainEntity[{i}].acceptedAnswer.text is missing or empty"
                    )

        elif schema_type == "Article":
            headline = data.get("headline", "")
            if isinstance(headline, str) and len(headline) > 110:
                warnings.append(
                    f"Article.headline is {len(headline)} chars; Google recommends ≤110"
                )
            date_pub = data.get("datePublished", "")
            if date_pub and not re.match(r"\d{4}-\d{2}-\d{2}", str(date_pub)):
                warnings.append(
                    f"Article.datePublished {date_pub!r} should use ISO 8601 (YYYY-MM-DD)"
                )

        elif schema_type == "HowTo":
            steps = data.get("step", [])
            if not isinstance(steps, list):
                steps = [steps]
            for i, step in enumerate(steps):
                if not isinstance(step, dict):
                    errors.append(f"HowTo.step[{i}] must be an object")
                    continue
                if step.get("@type") not in ("HowToStep", "HowToSection"):
                    warnings.append(
                        f"HowTo.step[{i}] @type should be HowToStep or HowToSection"
                    )
                if not step.get("text") and not step.get("name"):
                    errors.append(f"HowTo.step[{i}] must have 'name' or 'text'")

        elif schema_type == "LocalBusiness":
            address = data.get("address")
            if address and isinstance(address, dict):
                if "@type" not in address:
                    warnings.append("LocalBusiness.address should have @type: PostalAddress")
            telephone = data.get("telephone")
            if telephone and not re.match(r"[\+\d\s\-\(\)]{7,}", str(telephone)):
                warnings.append(
                    f"LocalBusiness.telephone {telephone!r} may not be a valid phone number"
                )

        elif schema_type == "BreadcrumbList":
            items = data.get("itemListElement", [])
            if not isinstance(items, list) or not items:
                errors.append("BreadcrumbList.itemListElement must be a non-empty list")
            else:
                for i, item in enumerate(items):
                    if not isinstance(item, dict):
                        errors.append(f"BreadcrumbList.itemListElement[{i}] must be an object")
                        continue
                    if item.get("@type") != "ListItem":
                        warnings.append(
                            f"BreadcrumbList.itemListElement[{i}] @type should be ListItem"
                        )
                    if "position" not in item:
                        errors.append(
                            f"BreadcrumbList.itemListElement[{i}] is missing 'position'"
                        )
                    if "name" not in item and "item" not in item:
                        errors.append(
                            f"BreadcrumbList.itemListElement[{i}] must have 'name' or 'item'"
                        )
