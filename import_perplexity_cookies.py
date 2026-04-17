"""Convert browser-extension Perplexity cookie export to Playwright storage_state.

Usage: paste your exported cookies JSON into RAW_COOKIES below and run:
    python import_perplexity_cookies.py

This writes /opt/seo-engine/data/storage/browser_sessions/perplexity.json
After that, call_perplexity() will use your logged-in session and bypass Cloudflare.

To export Perplexity cookies:
  1. Open perplexity.ai and log in
  2. Use "Cookie-Editor" or "EditThisCookie" browser extension
  3. Export → paste into RAW_COOKIES below
"""

import json
from pathlib import Path

# ---- PASTE YOUR PERPLEXITY COOKIES HERE ----
RAW_COOKIES = []  # e.g. [{"domain": ".perplexity.ai", "name": "...", ...}]
# --------------------------------------------

if not RAW_COOKIES:
    print("No cookies provided. Export perplexity.ai cookies and paste into RAW_COOKIES.")
    raise SystemExit(1)

SAMESITE_MAP = {
    "no_restriction": "None",
    "lax":            "Lax",
    "strict":         "Strict",
    "unspecified":    "None",
}

cookies = []
for c in RAW_COOKIES:
    if c.get("session") and c["name"] not in ("_pplx_auth",):
        continue  # skip pure session cookies
    cookie = {
        "name":     c["name"],
        "value":    c["value"],
        "domain":   c["domain"],
        "path":     c.get("path", "/"),
        "httpOnly": c.get("httpOnly", False),
        "secure":   c.get("secure", False),
        "sameSite": SAMESITE_MAP.get(c.get("sameSite", "unspecified"), "None"),
        "expires":  int(c["expirationDate"]) if c.get("expirationDate") else -1,
    }
    cookies.append(cookie)

out = Path("/opt/seo-engine/data/storage/browser_sessions/perplexity.json")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps({"cookies": cookies, "origins": []}, indent=2))
print(f"Written {len(cookies)} cookies to {out}")
for c in cookies:
    print(f"  {c['name']:40s}  domain={c['domain']}")
