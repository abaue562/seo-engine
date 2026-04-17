# GetHubed SEO Engine

Multi-tenant SEO automation SaaS. Generates, publishes, and optimises content across every signal layer — from SERP rankings to AI citations to Wikidata entities — with zero paid API dependencies.

---

## VPS Access

| Item | Value |
|------|-------|
| IP | 204.168.184.50 |
| User | root |
| SSH key | `~/.ssh/aion_vps` |
| Engine path | `/opt/seo-engine` |
| Port | 8900 |
| Python | `.venv/bin/python` (always use this, not system python) |

**Connect:**
```bash
ssh -i ~/.ssh/aion_vps root@204.168.184.50
cd /opt/seo-engine
```

**Services:**
```bash
systemctl status seo-api seo-beat seo-worker
```

---

## Stack

| Layer | Tech |
|-------|------|
| API | FastAPI (port 8900) |
| Task queue | Celery + Redis |
| Scheduler | Celery Beat (61 scheduled tasks) |
| Storage | SQLite (`data/storage/seo_engine.db`) + PostgreSQL (RLS multi-tenant) |
| LLM | Grok via AION Brain :9082 (OpenAI-compatible router) |
| Scraping | Firecrawl :3002 (self-hosted, JS-aware) |
| Embeddings | Ollama + nomic-embed-text :11434 |
| Browser automation | Playwright + persistent session cookies |
| Email | Amazon SES SMTP (STARTTLS, port 587) |

---

## AION Brain Services (all self-hosted on same VPS)

| Service | Port | Purpose |
|---------|------|---------|
| Grok LLM router | :9082 | Primary LLM — content, summaries, prompts |
| Firecrawl | :3002 | JS-aware scraper → clean markdown |
| GPT-Researcher | :8170 | Deep research tasks |
| Knowledge bridge | :9091 | Knowledge retrieval |
| Ollama | :11434 | nomic-embed-text embeddings |
| Twitter Intel | :8195 | Trending signals |
| Listmonk | :9001 | Email list management |
| AION Email | :9280 | Email routing |

---

## Architecture — Module Map

### `core/` — 79 modules
Key modules:
- `pipeline.py` — master content pipeline orchestrator
- `llm_gateway.py` — LLM routing with circuit breaker
- `aion_bridge.py` — AION Brain HTTP client
- `browser_llm.py` — Playwright browser: Perplexity + Grok via session cookies (no API key)
- `browser_image_gen.py` — Image generation via Grok Create Images (Playwright)
- `sitemap_generator.py` — XML sitemap generation + Google/Bing pings
- `email_sender.py` — Amazon SES SMTP outreach
- `serp_scraper.py` — Bing SERP via Firecrawl (replaces DataForSEO)
- `backlink_crawler.py` — Real-time backlink discovery via Firecrawl
- `backlink_prospector.py` — Competitor gap analysis via Common Crawl
- `parasite_seo.py` — High-DA platform publishing (WordPress, Medium, LinkedIn, Reddit)
- `press_release.py` — Press release generation + distribution
- `geo_optimizer.py` — GEO/AEO optimisation for AI answer engines
- `llms_txt_builder.py` — llms.txt generation for LLM citation signals
- `ai_answer_monitor.py` — Monitor AI citation appearances
- `semantic_linker.py` — Internal link injection with anchor diversity enforcement
- `eeat_pipeline.py` — E-E-A-T signal generation
- `keyword_clustering.py` — Topical cluster builder
- `cta_optimizer.py` — CTA variant testing
- `lead_capture.py` — Lead form injection
- `call_tracking.py` — Dynamic phone number swapping
- `credential_vault.py` — Secure API key storage
- `onboarding_orchestrator.py` — 9-step tenant onboarding

### `data/connectors/` — self-hosted data pipeline (zero paid APIs)
- `rank_checker.py` — Keyword rank tracking via Bing/Firecrawl (replaces DataForSEO rank)
- `serp_volume_estimator.py` — Keyword volume + difficulty from SERP signals (replaces DataForSEO keyword)
- `common_crawl.py` — Backlink intelligence via CC CDX API + Wayback Machine (replaces Ahrefs)
- `dataforseo.py` — Legacy wrapper; now routes to self-hosted alternatives

### `authority/`
- `wikidata.py` — Wikidata entity pipeline: notability check → SPARQL search → MediaWiki API auto-create
- `WikidataAPI` — login/create_item/add_inception_date via wbeditentity

### `execution/`
- `handlers/` — Per-task execution handlers (content, backlinks, GBP, citations, etc.)
- `connectors/external/` — WordPress, Playwright web poster, auto-signup

### `taskq/`
- `tasks.py` — 61 Celery beat tasks covering all pipeline layers
- `celery_app.py` — Beat schedule, task routing, dead letter queue

### `core/crawlers/`
- `js_crawler.py` — Firecrawl-backed JS crawler with CWV extraction

---

## Scheduled Tasks (61 total — runs automatically)

Key weekly/daily tasks:
| Task | Schedule | Purpose |
|------|----------|---------|
| `run_wikidata_sync` | Weekly | Auto-create Wikidata entities for all businesses |
| `run_sitemap_sync` | Weekly | Generate XML sitemaps + ping Google/Bing |
| `run_llms_txt_deploy` | Weekly | Deploy llms.txt for AI citation signals |
| `run_parasite_seo` | Weekly | Publish content to high-DA platforms |
| `run_press_release` | Weekly | Generate + distribute press releases |
| `run_haro_check` | Daily | Monitor HARO for backlink opportunities |
| `run_link_reclamation` | Weekly | Reclaim broken/unlinked brand mentions |
| `run_gbp_posts` | Weekly | Google Business Profile posts |
| `scan_content_decay` | Weekly | Detect + refresh decaying content |
| `run_topical_gap_check` | Weekly | Find + queue topical gap content |
| `run_ai_answer_monitor` | Daily | Track AI engine citations |
| `run_geo_optimization` | Weekly | GEO/AEO content optimisation |
| `inject_content_freshness` | Weekly | Freshness signals + dateModified bumps |
| `run_rank_check` | Weekly | Check keyword rankings |
| `run_programmatic_batch` | Weekly | Programmatic page generation |
| `run_competitor_gap` | Weekly | Competitor backlink gap analysis |

---

## Image Generation Pipeline

Uses Grok's "Create Images" feature via Playwright browser session (no API key):

```bash
cd /opt/seo-engine
.venv/bin/python run_image_pipeline.py --biz-id <UUID> --max-keywords 20
```

Flow:
1. Load businesses + keywords from `data/storage/`
2. Grok text → engineers a professional image prompt per keyword
3. Browser navigates `x.com/i/grok` with saved session → Create Images
4. Downloads generated PNG (403 handled via browser context fetch with cookies)
5. Saves to `data/storage/images/{business_id}/`
6. Updates `published_urls_*.json` with `image_path` field

Session file: `data/storage/browser_sessions/grok.json`

---

## Self-Hosted Replacements (zero paid APIs)

| Paid Tool | Cost | Replacement | Status |
|-----------|------|-------------|--------|
| DataForSEO SERP | ~$50/mo | `core/serp_scraper.py` (Bing + Firecrawl) | ✅ Live |
| DataForSEO rank tracking | ~$50/mo | `data/connectors/rank_checker.py` | ✅ Live |
| DataForSEO keyword volume | ~$50/mo | `data/connectors/serp_volume_estimator.py` | ✅ Live |
| Ahrefs backlinks | ~$100/mo | `data/connectors/common_crawl.py` (CC CDX + Wayback) | ✅ Live |
| Screaming Frog | ~$20/mo | Firecrawl :3002 (self-hosted) | ✅ Live |
| Perplexity API | ~$20/mo | `core/browser_llm.py` (browser session) | ✅ Live |
| DALL-E / Midjourney | ~$20/mo | `core/browser_image_gen.py` (Grok browser) | ✅ Live |

**Total saved: ~$310/mo**

---

## LLM Access Strategy (no API keys needed)

| Tool | Access method |
|------|--------------|
| Grok | AION Brain :9082 + browser session (grok.json) |
| Perplexity | Browser session (perplexity.json) |
| Claude | CLI access |

---

## Environment Setup

Copy `config/.env.example` to `config/.env` and fill in:

```env
# Amazon SES (email outreach — already configured)
SMTP_HOST=email-smtp.us-east-2.amazonaws.com
SMTP_PORT=587
SMTP_USER=<SES SMTP username>
SMTP_PASS=<SES SMTP password>
SMTP_FROM=noreply@gethubed.com

# Wikidata auto-create (bot account)
WIKIDATA_USERNAME=Abaue562@123456
WIKIDATA_PASSWORD=<bot password from Special:BotPasswords>

# Platform publishing (unlock DA 78-98 backlink channels)
REDDIT_CLIENT_ID=
REDDIT_CLIENT_SECRET=
MEDIUM_TOKEN=
LINKEDIN_ACCESS_TOKEN=
GITHUB_TOKEN=
DEVTO_API_KEY=

# X/Twitter (for Grok browser session setup)
TWITTER_USERNAME=
TWITTER_PASSWORD=
```

---

## Restore from Scratch

If the VPS is lost:

```bash
# 1. Clone repo
git clone https://github.com/abaue562/seo-engine.git /opt/seo-engine
cd /opt/seo-engine

# 2. Create virtualenv + install
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
playwright install chromium

# 3. Restore config
cp config/.env.example config/.env
# Fill in credentials from password manager

# 4. Restore browser sessions (grok.json, perplexity.json)
# Copy from backup to data/storage/browser_sessions/

# 5. Start services
systemctl start redis
.venv/bin/celery -A taskq.celery_app worker -Q analysis,execution &
.venv/bin/celery -A taskq.celery_app beat &
uvicorn api.main:app --port 8900 &
```

---

## Current Status

**Score: 8.2/10** (target: 10/10)

### Completed
- ✅ GEO/AEO layer
- ✅ E-E-A-T signals
- ✅ Backlink acquisition pipeline
- ✅ Brand entity / Knowledge Graph
- ✅ LLM citation content
- ✅ Conversion layer (CTA, lead capture, call tracking)
- ✅ Parasite SEO (WordPress, Medium, Reddit, LinkedIn, Dev.to)
- ✅ Credential vault + onboarding wizard
- ✅ LinkedIn connector (DA 98)
- ✅ Press release pipeline
- ✅ Email outreach (Amazon SES SMTP — LIVE)
- ✅ Dead letter queue + circuit breaker
- ✅ Self-hosted rank tracker (replaces DataForSEO)
- ✅ Self-hosted SERP volume estimator (replaces DataForSEO keyword)
- ✅ Self-hosted backlink intelligence (replaces Ahrefs via Common Crawl)
- ✅ Wikidata auto-create pipeline (MediaWiki API)
- ✅ Sitemap auto-generation + search engine pings
- ✅ Image generation pipeline (Grok via Playwright)

### To reach 10/10
1. Set platform OAuth keys (Reddit, Medium, LinkedIn, Dev.to, GitHub) → activates DA 78-98 publishing
2. Schedule llms.txt beat task (module built, needs wiring)
3. Onboard first real paying tenant

---

## Git History (recent)

```
25d0e15 fix: browser_image_gen — 403 via context fetch + extended wait
bedf31c feat: browser image generation via Grok (Playwright session)
398fc89 feat: sitemap auto-generation pipeline
5c442d8 feat: Wikidata auto-create pipeline via MediaWiki API
55ab613 feat: self-hosted data scrapers — replace DataForSEO + Ahrefs
8eece15 fix: email_sender — SES SMTP replaces Resend API (outreach LIVE)
5cda556 feat: LinkedIn connector (DA 98) + press release distribution
af1ced5 feat: credential vault, onboarding wizard, dashboard UI
```

---

*Built by Abaue562 + Claude Code — GetHubed.com*
