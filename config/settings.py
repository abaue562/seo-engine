import os
from pathlib import Path
from dotenv import load_dotenv

_config_dir = Path(__file__).parent
load_dotenv(_config_dir / ".env")
# NOTE: .env.example is NOT loaded here — it's documentation only.
# Copy it to .env and fill in real values.

# ── AI ───────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL = os.getenv("MODEL", "claude-sonnet-4-20250514")

# ── Server ───────────────────────────────────────────────────────────────────
PORT = int(os.getenv("PORT", "8900"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "info")
SITE_BASE_URL = os.getenv("SITE_BASE_URL", "")
SITE_HOST = os.getenv("SITE_HOST", "")

# ── Task queue (Celery + Redis) ───────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CELERY_CONCURRENCY = int(os.getenv("CELERY_CONCURRENCY", "4"))
CELERY_LOGLEVEL = os.getenv("CELERY_LOGLEVEL", "info")

# ── Google ───────────────────────────────────────────────────────────────────
GSC_CREDENTIALS_PATH = os.getenv("GSC_CREDENTIALS_PATH", "config/gsc_credentials.json")
GSC_TOKEN_PATH = os.getenv("GSC_TOKEN_PATH", "config/gsc_token.json")
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "")
GA4_CREDENTIALS_PATH = os.getenv("GA4_CREDENTIALS_PATH", "config/ga4_credentials.json")
GOOGLE_PAGESPEED_API_KEY = os.getenv("GOOGLE_PAGESPEED_API_KEY", "")

# ── Bing ─────────────────────────────────────────────────────────────────────
BING_WEBMASTER_API_KEY = os.getenv("BING_WEBMASTER_API_KEY", "")

# ── DataForSEO ───────────────────────────────────────────────────────────────
DATAFORSEO_LOGIN = os.getenv("DATAFORSEO_LOGIN", "")
DATAFORSEO_PASSWORD = os.getenv("DATAFORSEO_PASSWORD", "")

# ── Ahrefs ───────────────────────────────────────────────────────────────────
AHREFS_API_TOKEN = os.getenv("AHREFS_API_TOKEN", "")

# ── Content quality ──────────────────────────────────────────────────────────
ORIGINALITY_API_KEY = os.getenv("ORIGINALITY_API_KEY", "")
AI_SCORE_THRESHOLD = float(os.getenv("AI_SCORE_THRESHOLD", "0.8"))

# ── IndexNow ─────────────────────────────────────────────────────────────────
INDEXNOW_API_KEY = os.getenv("INDEXNOW_API_KEY", "")

# ── Brand monitoring ─────────────────────────────────────────────────────────
BRANDMENTIONS_API_KEY = os.getenv("BRANDMENTIONS_API_KEY", "")

# ── Alerts / webhooks ────────────────────────────────────────────────────────
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")  # Slack / Discord / Telegram compatible

# ── Database (Supabase) ──────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# ── Data freshness thresholds (days) ─────────────────────────────────────────
FRESHNESS_RANKINGS = int(os.getenv("FRESHNESS_RANKINGS", "30"))
FRESHNESS_REVIEWS = int(os.getenv("FRESHNESS_REVIEWS", "90"))
FRESHNESS_TRAFFIC = int(os.getenv("FRESHNESS_TRAFFIC", "90"))

# ── Autonomous runner ─────────────────────────────────────────────────────────
AUTONOMOUS_MODE = os.getenv("AUTONOMOUS_MODE", "shadow")       # shadow | assisted | autonomous
CONFIDENCE_THRESHOLD = float(os.getenv("CONFIDENCE_THRESHOLD", "0.7"))
MAX_DAILY_EXECUTIONS = int(os.getenv("MAX_DAILY_EXECUTIONS", "5"))
RUNNER_LOOP_HOURS = int(os.getenv("RUNNER_LOOP_HOURS", "24"))

# ── Publishing — WordPress (primary) ─────────────────────────────────────────
# WP_APP_PASSWORD: Settings → Users → Application Passwords in WP admin
WP_URL = os.getenv("WP_URL", "")
WP_USER = os.getenv("WP_USER", "")
WP_APP_PASSWORD = os.getenv("WP_APP_PASSWORD", "")
# draft | publish  — set 'publish' to auto-publish, 'draft' to review first
WP_PUBLISH_STATUS = os.getenv("WP_PUBLISH_STATUS", "draft")

# ── Publishing — Medium ───────────────────────────────────────────────────────
# Get at: medium.com/me/settings/security → Integration Tokens
MEDIUM_TOKEN = os.getenv("MEDIUM_TOKEN", "")
MEDIUM_PUBLISH_STATUS = os.getenv("MEDIUM_PUBLISH_STATUS", "draft")  # draft | public

# ── Publishing — Blogger ─────────────────────────────────────────────────────
BLOGGER_BLOG_ID = os.getenv("BLOGGER_BLOG_ID", "")

# ── Publishing — WordPress.com ───────────────────────────────────────────────
WP_COM_SITE = os.getenv("WP_COM_SITE", "")
WP_COM_TOKEN = os.getenv("WP_COM_TOKEN", "")

# ── Link injection ────────────────────────────────────────────────────────────
# Max existing posts to scan and patch with inbound links per new page
LINK_INJECT_MAX_POSTS = int(os.getenv("LINK_INJECT_MAX_POSTS", "20"))
# Minimum word match score (0-1) to consider a post eligible for link injection
LINK_INJECT_RELEVANCE_THRESHOLD = float(os.getenv("LINK_INJECT_RELEVANCE_THRESHOLD", "0.3"))
# Max % of injected anchors that can be exact-match (Penguin protection)
LINK_ANCHOR_EXACT_MAX_PCT = float(os.getenv("LINK_ANCHOR_EXACT_MAX_PCT", "0.15"))

# ── Content quality gates ─────────────────────────────────────────────────────
MIN_WORD_COUNT = int(os.getenv("MIN_WORD_COUNT", "700"))
SCHEMA_VALIDATE = os.getenv("SCHEMA_VALIDATE", "true")

# ── AI Citation monitoring ────────────────────────────────────────────────────
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY", "")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")     # for ChatGPT citation testing
GA4_MEASUREMENT_ID = os.getenv("GA4_MEASUREMENT_ID", "") # G-XXXXXXXXXX format

# ── Conversion layer ──────────────────────────────────────────────────────────
# Default phone/email injected into CTAs if not set on individual business
DEFAULT_PHONE = os.getenv("DEFAULT_PHONE", "")
DEFAULT_EMAIL = os.getenv("DEFAULT_EMAIL", "")
