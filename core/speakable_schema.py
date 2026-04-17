import json
import logging
import re
from typing import Dict, List, Optional

log = logging.getLogger(__name__)

DEFAULT_SELECTORS = [
    ".seo-direct-answer",
    ".key-takeaways",
    ".faq-answer",
    "h1 + p",
    ".entry-summary",
]

def get_default_selectors() -> List[str]:
    return DEFAULT_SELECTORS

def generate_speakable_schema(page_url: str, css_selectors: Optional[List[str]] = None) -> Dict:
    selectors = css_selectors or DEFAULT_SELECTORS
    return {
        "@context": "https://schema.org",
        "@type": "WebPage",
        "url": page_url,
        "speakable": {
            "@type": "SpeakableSpecification",
            "cssSelector": selectors,
        }
    }

def extract_speakable_sections(html: str) -> List[Dict]:
    sections = []
    patterns = {
        ".seo-direct-answer": re.compile(r'<div[^>]+class="[^"]*seo-direct-answer[^"]*"[^>]*>(.*?)</div>', re.S | re.I),
        ".key-takeaways": re.compile(r'<div[^>]+class="[^"]*key-takeaways[^"]*"[^>]*>(.*?)</div>', re.S | re.I),
        "h1": re.compile(r'<h1[^>]*>(.*?)</h1>', re.S | re.I),
        "first-p": re.compile(r'<p[^>]*>(.*?)</p>', re.S | re.I),
    }
    for selector, pattern in patterns.items():
        match = pattern.search(html)
        if match:
            text = re.sub(r'<[^>]+>', '', match.group(1)).strip()
            if len(text) > 20:
                sections.append({
                    'selector': selector,
                    'text_preview': text[:120],
                    'word_count': len(text.split()),
                })
    return sections

def inject_speakable_into_html(html: str, page_url: str) -> str:
    if 'SpeakableSpecification' in html:
        return html  # already present

    sections = extract_speakable_sections(html)
    if not sections:
        selectors = DEFAULT_SELECTORS
    else:
        selectors = [s['selector'] for s in sections if s['word_count'] >= 10]
        if not selectors:
            selectors = DEFAULT_SELECTORS

    schema = generate_speakable_schema(page_url, selectors)
    script_tag = f'\n<script type="application/ld+json">\n{json.dumps(schema, indent=2)}\n</script>\n'

    # Inject before </head> or at top
    if '</head>' in html:
        return html.replace('</head>', script_tag + '</head>', 1)
    if '<body' in html:
        idx = html.index('<body')
        return html[:idx] + script_tag + html[idx:]
    return script_tag + html
