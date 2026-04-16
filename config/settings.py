import os
from pathlib import Path
from dotenv import load_dotenv

_config_dir = Path(__file__).parent
load_dotenv(_config_dir / ".env")
load_dotenv(_config_dir / ".env.example")

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
