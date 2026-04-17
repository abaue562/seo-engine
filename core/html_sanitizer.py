"""HTML sanitizer for content published to WordPress.

Prevents XSS payloads from LLM output or prompt-injected competitor content
reaching the customer's WordPress site.

Uses a strict allowlist approach built on stdlib html.parser -- no external
dependencies, full control over what passes through.

Usage:
    from core.html_sanitizer import sanitize_html, sanitize_schema_json

    clean_html = sanitize_html(raw_html)
    clean_schema = sanitize_schema_json(raw_schema_dict)
"""
from __future__ import annotations

import json
import logging
import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlparse

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Allowlist configuration
# ---------------------------------------------------------------------------

# Tags whose content is kept but the tag itself is stripped (unwrap)
_UNWRAP_TAGS = frozenset({"html", "head", "body", "main", "article", "section", "header", "footer"})

# Tags that are fully allowed (attributes filtered separately)
_ALLOWED_TAGS = frozenset({
    "h1", "h2", "h3", "h4", "h5", "h6",
    "p", "br", "hr",
    "ul", "ol", "li", "dl", "dt", "dd",
    "strong", "em", "b", "i", "u", "s", "mark", "small", "sub", "sup",
    "blockquote", "q", "cite", "abbr", "acronym",
    "pre", "code",
    "a",            # href filtered
    "img",          # src, alt filtered
    "table", "thead", "tbody", "tfoot", "tr", "th", "td", "caption",
    "div", "span",
    "figure", "figcaption",
})

# Tags whose entire subtree is dropped (including children)
_DROP_TAGS = frozenset({
    "script", "style", "iframe", "object", "embed", "applet",
    "form", "input", "button", "select", "textarea", "label",
    "meta", "link", "base",
    "noscript", "canvas", "svg", "math",
    "details", "summary",  # can hide content
})

# Allowed attributes per tag (* = any allowed tag)
_ALLOWED_ATTRS: dict[str, frozenset[str]] = {
    "*":     frozenset({"class", "id"}),
    "a":     frozenset({"href", "title", "rel", "target"}),
    "img":   frozenset({"src", "alt", "title", "width", "height", "loading"}),
    "td":    frozenset({"colspan", "rowspan"}),
    "th":    frozenset({"colspan", "rowspan", "scope"}),
    "ol":    frozenset({"type", "start"}),
    "div":   frozenset({"class", "id", "style"}),  # style filtered separately
    "span":  frozenset({"class", "id", "style"}),
    "table": frozenset({"class", "id"}),
    "blockquote": frozenset({"cite"}),
}

# Allowed CSS properties in inline style (very narrow)
_ALLOWED_STYLES = frozenset({
    "color", "background-color", "background", "font-weight", "font-style",
    "text-align", "text-decoration", "padding", "margin",
    "border-radius", "border", "line-height", "font-size",
})

# Allowed URL schemes for href/src
_ALLOWED_SCHEMES = frozenset({"https", "http", "mailto"})

# Event handler attribute pattern
_EVENT_ATTR_RE = re.compile(r"^on[a-z]+$", re.IGNORECASE)
# Dangerous URL pattern
_DANGEROUS_URL_RE = re.compile(r"^(javascript|vbscript|data)\s*:", re.IGNORECASE)


def _clean_url(url: str) -> str:
    """Return url if safe scheme, else empty string."""
    url = url.strip()
    if _DANGEROUS_URL_RE.match(url):
        return ""
    try:
        parsed = urlparse(url)
        if parsed.scheme and parsed.scheme.lower() not in _ALLOWED_SCHEMES:
            return ""
    except Exception:
        return ""
    return url


def _clean_style(style: str) -> str:
    """Return filtered inline CSS -- only allow safe properties."""
    safe_parts = []
    for declaration in style.split(";"):
        declaration = declaration.strip()
        if ":" not in declaration:
            continue
        prop, _, val = declaration.partition(":")
        prop = prop.strip().lower()
        val = val.strip()
        # Block url() calls in CSS (can load external resources)
        if "url(" in val.lower() or "expression(" in val.lower():
            continue
        if prop in _ALLOWED_STYLES:
            safe_parts.append(f"{prop}: {val}")
    return "; ".join(safe_parts)


def _allowed_attrs(tag: str, attr: str, val: str) -> str | None:
    """Return cleaned attribute value, or None to drop the attribute."""
    # Block all event handlers
    if _EVENT_ATTR_RE.match(attr):
        return None
    # Check allowlist
    tag_allowed = _ALLOWED_ATTRS.get(tag, frozenset())
    global_allowed = _ALLOWED_ATTRS.get("*", frozenset())
    if attr not in tag_allowed and attr not in global_allowed:
        return None
    # URL filtering
    if attr in ("href", "src"):
        cleaned = _clean_url(val)
        if not cleaned:
            return None
        return cleaned
    # Style filtering
    if attr == "style":
        cleaned = _clean_style(val)
        return cleaned if cleaned else None
    # rel attribute: only allow safe values
    if attr == "rel":
        allowed_rels = {"noopener", "noreferrer", "nofollow", "ugc", "sponsored", "external", "author"}
        parts = [r for r in val.split() if r.lower() in allowed_rels]
        return " ".join(parts) if parts else None
    return val


# ---------------------------------------------------------------------------
# HTML parser / sanitizer
# ---------------------------------------------------------------------------

class _Sanitizer(HTMLParser):
    """Stateful HTML sanitizer that builds clean output."""

    def __init__(self):
        super().__init__(convert_charrefs=False)
        self._output: list[str] = []
        self._drop_depth = 0   # depth of a subtree being dropped
        self._script_data = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if self._drop_depth > 0:
            if tag not in _DROP_TAGS:
                self._drop_depth += 1
            return
        if tag in _DROP_TAGS:
            self._drop_depth = 1
            return
        if tag in _UNWRAP_TAGS:
            return  # emit nothing, children flow through
        if tag not in _ALLOWED_TAGS:
            return  # unknown tag: silently drop

        clean_attrs = []
        for attr, val in attrs:
            attr = attr.lower()
            val = val or ""
            result = _allowed_attrs(tag, attr, val)
            if result is not None:
                clean_attrs.append(f' {attr}="{_escape_attr(result)}"')

        void_tags = {"br", "hr", "img"}
        slash = " /" if tag in void_tags else ""
        self._output.append(f"<{tag}{''.join(clean_attrs)}{slash}>")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self._drop_depth > 0:
            self._drop_depth -= 1
            return
        if tag in _DROP_TAGS or tag in _UNWRAP_TAGS:
            return
        if tag not in _ALLOWED_TAGS:
            return
        void_tags = {"br", "hr", "img"}
        if tag not in void_tags:
            self._output.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        if self._drop_depth > 0:
            return
        self._output.append(_escape_text(data))

    def handle_entityref(self, name: str) -> None:
        if self._drop_depth == 0:
            self._output.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        if self._drop_depth == 0:
            self._output.append(f"&#{name};")

    def get_result(self) -> str:
        return "".join(self._output)


def _escape_attr(val: str) -> str:
    return val.replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;").replace(">", "&gt;")


def _escape_text(val: str) -> str:
    return val.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def sanitize_html(html: str) -> str:
    """Sanitize HTML using a strict allowlist. Returns clean HTML string.

    Strips all script/style/iframe/event-handler content. Allowlists tags and
    attributes. Filters URL schemes. Safe to publish to WordPress.
    """
    if not html:
        return ""
    # Preserve JSON-LD script blocks (they are not executable JS)
    # Extract them before sanitization, re-inject after
    ld_blocks: list[str] = []
    ld_pattern = re.compile(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        re.DOTALL | re.IGNORECASE,
    )

    def _extract_ld(m: re.Match) -> str:
        try:
            # Validate it's actually JSON before preserving
            json.loads(m.group(1))
            placeholder = f"__LD_BLOCK_{len(ld_blocks)}__"
            ld_blocks.append(m.group(0))
            return placeholder
        except (json.JSONDecodeError, Exception):
            return ""  # Drop malformed JSON-LD

    html_without_ld = ld_pattern.sub(_extract_ld, html)

    sanitizer = _Sanitizer()
    sanitizer.feed(html_without_ld)
    result = sanitizer.get_result()

    # Re-inject preserved JSON-LD blocks
    for i, block in enumerate(ld_blocks):
        result = result.replace(f"__LD_BLOCK_{i}__", block)

    return result


# ---------------------------------------------------------------------------
# Schema JSON sanitizer
# ---------------------------------------------------------------------------

# Allowed schema types (re-serialize from typed structure, drop unknown)
_ALLOWED_SCHEMA_TYPES = frozenset({
    "LocalBusiness", "Organization", "Article", "BlogPosting", "WebPage",
    "FAQPage", "Question", "Answer", "HowTo", "HowToStep",
    "BreadcrumbList", "ListItem", "Service", "Product", "Review",
    "AggregateRating", "Person", "PostalAddress", "GeoCoordinates",
    "ImageObject", "VideoObject", "SiteNavigationElement",
})


def sanitize_schema_json(schema: dict | None) -> dict | None:
    """Parse and re-serialize schema JSON to prevent injection.

    Validates the @type is in the allowlist. Strips unknown/dangerous fields.
    Returns None if schema is invalid or type is not allowed.
    """
    if not schema:
        return None
    schema_type = schema.get("@type", "")
    if schema_type not in _ALLOWED_SCHEMA_TYPES:
        log.warning("html_sanitizer.schema_type_blocked  type=%s", schema_type)
        return None
    # Re-serialize via JSON parse/dump to strip any non-JSON-serialisable objects
    try:
        clean = json.loads(json.dumps(schema, default=str))
        # Strip any keys that look like script injection
        _sanitize_dict_recursive(clean)
        return clean
    except Exception as e:
        log.warning("html_sanitizer.schema_sanitize_fail  err=%s", e)
        return None


_DANGEROUS_VALUE_RE = re.compile(r"<script|javascript:|vbscript:|data:text/html", re.IGNORECASE)


def _sanitize_dict_recursive(obj: Any) -> None:
    """In-place removal of dangerous values from nested dicts/lists."""
    if isinstance(obj, dict):
        bad_keys = [k for k in obj if not isinstance(k, str) or _DANGEROUS_VALUE_RE.search(k)]
        for k in bad_keys:
            del obj[k]
        for k, v in list(obj.items()):
            if isinstance(v, str) and _DANGEROUS_VALUE_RE.search(v):
                obj[k] = ""
            else:
                _sanitize_dict_recursive(v)
    elif isinstance(obj, list):
        for item in obj:
            _sanitize_dict_recursive(item)
