import os
from pathlib import Path
from dotenv import load_dotenv

_config_dir = Path(__file__).parent
load_dotenv(_config_dir / ".env")
load_dotenv(_config_dir / ".env.example")

# AI
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL = os.getenv("MODEL", "claude-sonnet-4-20250514")

# Server
PORT = int(os.getenv("PORT", "8900"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "info")

# Google
GSC_CREDENTIALS_PATH = os.getenv("GSC_CREDENTIALS_PATH", "config/gsc_credentials.json")
GSC_TOKEN_PATH = os.getenv("GSC_TOKEN_PATH", "config/gsc_token.json")

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# Data freshness thresholds (days)
FRESHNESS_RANKINGS = int(os.getenv("FRESHNESS_RANKINGS", "30"))
FRESHNESS_REVIEWS = int(os.getenv("FRESHNESS_REVIEWS", "90"))
FRESHNESS_TRAFFIC = int(os.getenv("FRESHNESS_TRAFFIC", "90"))

# DataForSEO — keyword volume, SERP, backlinks
DATAFORSEO_LOGIN    = os.getenv("DATAFORSEO_LOGIN", "")
DATAFORSEO_PASSWORD = os.getenv("DATAFORSEO_PASSWORD", "")

# Google Analytics 4
GA4_PROPERTY_ID       = os.getenv("GA4_PROPERTY_ID", "")
GA4_CREDENTIALS_PATH  = os.getenv("GA4_CREDENTIALS_PATH", "config/ga4_credentials.json")

# PageSpeed Insights (optional — increases quota beyond 25K/day free tier)
PSI_API_KEY = os.getenv("PSI_API_KEY", "")
