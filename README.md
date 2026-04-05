# SEO Engine

An autonomous SEO operating system powered by Claude AI. It analyzes local service businesses, generates ranked SEO tasks, and executes them across Google, content platforms, and the web — with minimal human oversight.

---

## What It Does

Give it a business (name, services, location, keywords, current rankings). It:

1. Pulls live data from Google Search Console, GBP, GA4, and competitors
2. Sends all context to Claude, which identifies the fastest path to top 3 rankings
3. Scores and prioritizes tasks by impact, speed, and confidence
4. Executes approved tasks across websites, Google Business Profile, blog networks, and social platforms
5. Learns from results, detects winning patterns, and evolves its own strategy over time

The system can run fully autonomously on a schedule, or in manual/assisted modes where a human approves actions.

---

## Architecture

```
BUSINESS CONTEXT
    ↓
[DATA PIPELINE] ← GSC, GBP, GA4, Website Crawl, Competitors, Keywords
    ↓
[CLAUDE BRAIN] → Single or Multi-agent Analysis
    ↓
[TASK GENERATION] → JSON array of SEO tasks
    ↓
[SCORING ENGINE] → Filter, rank, tier (PRIMARY / SUPPORTING / EXPERIMENTAL)
    ↓
[EXECUTION ROUTER] → Safety gate → Handler → Connector → Execution
    ↓
[VERIFICATION] → Confirm changes applied
    ↓
[LEARNING LOOP] → Analyze results, detect patterns, evolve strategy
    ↓
[AUTONOMOUS MODE] → Repeat on schedule
```

---

## Modules

### `core/` — The Brain
The decision-making layer. All strategy runs through here.

| File | Purpose |
|------|---------|
| `agents/brain.py` | Single-agent mode: one Claude call → scored task list |
| `agents/orchestrator.py` | Multi-agent pipeline: Data → Analysis → Strategy → Execution agents |
| `agents/prompts.py` | System prompts for each agent role |
| `prompts/system.py` | Master system prompt — aggressive, page-2-first ranking strategy |
| `scoring/engine.py` | Weighted task scorer: impact 35%, speed 30%, confidence 20%, ease 15% |
| `claude.py` | Unified Claude caller — auto-detects CLI mode vs API key mode |
| `full_power.py` | Activates all advanced modules together in one analysis pass |

---

### `models/` — Data Structures
| File | Purpose |
|------|---------|
| `business.py` | `BusinessContext` — immutable business profile; renders as Claude prompt block |
| `task.py` | `SEOTask` with type, execution mode, impact level, score; `TaskBatch` wrapper |

---

### `api/` — FastAPI Backend (port 8900)
All endpoints the UI and CLI communicate with.

| Endpoint | What it does |
|----------|-------------|
| `POST /analyze` | Single-brain analysis |
| `POST /orchestrate` | Multi-agent pipeline |
| `POST /full-run` | Ingest → Analyze → Execute → Learn |
| `POST /ingest` | Refresh all data connectors |
| `POST /execute` | Run a task through the execution router |
| `POST /learn` | Trigger weekly/monthly learning cycle |
| `POST /campaign` | Build a 21-day phased ranking campaign |
| `POST /simulate` | Simulate strategy outcomes before committing |
| `POST /aic` | Attention → Intent → Conversion funnel analysis |
| `POST /influence` | Perception and narrative control tools |
| `POST /personas` | Generate content in multiple voices |
| `POST /evolve` | Trigger self-evolution cycle |
| `POST /web-post` | Generate third-party web posting plan |
| `GET /health` | Health check |

---

### `execution/` — The Hands
Turns Claude's decisions into real-world actions.

| File | Purpose |
|------|---------|
| `router.py` | Routes tasks to correct handler; enforces SafetyGate; supports LIVE, SHADOW, APPROVAL_QUEUE modes |
| `safety.py` | Blocks dangerous actions (blackhat, brand-unsafe); enforces rate limits |
| `handlers/` | GBP, Website, Content, Authority — each handles its task type |
| `connectors/external/gbp.py` | Google Business Profile automation |
| `connectors/external/wordpress.py` | Direct WordPress integration |
| `connectors/external/web_poster.py` | Auto-submit to third-party websites |
| `connectors/external/blogger.py` | Blogger auto-posting |
| `connectors/external/medium.py` | Medium article syndication |
| `connectors/external/reddit.py` | Reddit community posting |
| `connectors/external/free_blogs.py` | Auto-signup and post to free blog networks |
| `publisher.py` | Content publication logic |
| `verification.py` | Confirms changes were applied after execution |
| `renderers/` | Template-based HTML/schema content generation |

---

### `data/` — The Sensors
Pulls live data from all sources before analysis.

| File | Purpose |
|------|---------|
| `pipeline.py` | `IngestionPipeline` — orchestrates all connectors in sequence |
| `connectors/gsc.py` | Google Search Console rankings and search analytics |
| `connectors/keywords.py` | Keyword volume and difficulty |
| `connectors/gbp.py` | GBP listing data |
| `connectors/ga4.py` | Google Analytics 4 traffic and behavior |
| `connectors/dataforseo.py` | SERP data, backlinks, competitor analysis |
| `crawlers/website.py` | Full on-page SEO site crawl |
| `crawlers/competitors.py` | Competitor benchmarking |
| `crawlers/gbp.py` | GBP profile scraper |
| `events.py` | Detects ranking changes, review spikes, traffic anomalies |
| `freshness.py` | Decides if cached data needs refreshing |
| `storage/database.py` | Local SQLite/JSON store for caching and learning history |

---

### `orchestration/` — Campaign Management
| File | Purpose |
|------|---------|
| `campaign.py` | Builds 21-day phased campaigns: Foundation → Distribution → Amplification → Reinforcement |

---

### `learning/` — Continuous Improvement
The system learns from every action and improves over time.

| File | Purpose |
|------|---------|
| `loops.py` | Weekly/monthly learning cycles — analyzes results, spots patterns |
| `patterns.py` | `PatternMemory` — stores what worked, feeds it into future runs |
| `evolution.py` | Self-evolution — mutates strategy parameters, evolves its own prompts |
| `attribution.py` | Attribution model for understanding which actions caused ranking changes |
| `reflection/engine.py` | Post-execution reflection and analysis |

---

### `prediction/` — Outcome Forecasting
Predicts what will happen before committing to execution.

| File | Purpose |
|------|---------|
| `ctr.py` | CTR Dominator — predicts and optimizes click-through rates |
| `serp_hijack.py` | Featured snippet and PAA box targeting strategy |
| `rapid_update.py` | Predicts how fast new content will rank |
| `acceleration.py` | Finds ranking acceleration opportunities |
| `authority_gap.py` | Identifies and closes authority gaps vs competitors |
| `competitor_reaction.py` | Models likely competitor responses |

---

### `strategy/` — Long-term Autonomous Strategy
| File | Purpose |
|------|---------|
| `autonomous.py` | `AutonomousRunner` — manages scheduled runs, approval queues, confidence thresholds, rate limits |
| `domination.py` | Market domination playbook generation |
| `cross_business.py` | Multi-location / multi-brand strategy |
| `evolution.py` | Strategy evolution over time |

---

### `aic/` — Attention → Intent → Conversion
| File | Purpose |
|------|---------|
| `engine.py` | Funnel analysis — finds where the drop-off is across the SERP funnel |
| `influence.py` | `InfluenceOS` — narrative control, consensus building, demand creation |
| `perception/engine.py` | Detects competitor narratives; chooses countermeasures |

---

### `signals/` — SEO Signal Generation
| File | Purpose |
|------|---------|
| `burst.py` | Burst signals — coordinated activity spikes to trigger ranking jumps |
| `demand.py` | Creates authentic search demand signals |
| `pressure.py` | Competitive pressure tactics |
| `suppression.py` | Negative signal strategies for competitors |
| `behavioral.py` | User behavior signal generation |

---

### `authority/` — Link Building
| File | Purpose |
|------|---------|
| `swarm.py` | Coordinated multi-channel link building (web 2.0, contextual, local citations) |

---

### `channels/` — Content Distribution
| File | Purpose |
|------|---------|
| `distribution.py` | Distributes content across owned, earned, and paid channels |
| `flywheel.py` | Models how one action triggers downstream amplification |
| `multiplier.py` | Maximizes reach through strategic channel sequencing |

---

### `entity/` — Brand & Entity Building
| File | Purpose |
|------|---------|
| `dominance.py` | Builds topical authority and entity recognition across the web |

---

### `personas/` — Multi-voice Content
| File | Purpose |
|------|---------|
| `system.py` | Generates content in multiple voices — formal brand, expert authority, community advocate, etc. |

---

### `simulation/` — Strategy Testing
| File | Purpose |
|------|---------|
| `engine.py` | Runs multiple strategy simulations showing predicted outcomes before execution |

---

### `ai_visibility/` — AI Search Optimization
| File | Purpose |
|------|---------|
| `eeat_scorer.py` | Scores E-E-A-T (Expertise, Experience, Authoritativeness, Trustworthiness) |
| `ai_detector.py` | Detects AI-generated content and suggests humanization |
| `llms_txt.py` | Generates `.llms.txt` files for AI model accessibility |
| `citability.py` | Structures content to be cited by AI tools |
| `answers.py` | Targets answer boxes and featured snippets |
| `geo_strategies.py` | Geographic-specific optimization |

---

### `ui/` — Dashboard (Next.js)
Frontend dashboard at `localhost:3000`.

**Business input form** → enter business details, services, location, keywords, competitors

**Analysis modes:**
- **Analyze** — Single Claude call, fastest
- **Orchestrate** — 4-agent pipeline, most thorough
- **Full-Power** — All edge tools activated (AIC, Simulation, Personas, Entity)

**Operation modes:**
- **Manual** — Approve every task
- **Assisted** — Auto-execute low-risk, approve high-risk
- **Autonomous** — Fully automated within safety thresholds

**Other panels:** Task list, execution results, activity feed, system status, edge tools toggles

---

## Entry Points

```bash
# Command-line (no server needed)
python run_mvp.py --mode analyze       # One Claude call → 5 ranked tasks
python run_mvp.py --mode orchestrate   # 4-agent pipeline
python run_mvp.py --mode execute       # Analyze + execute top task
python run_mvp.py --mode shadow        # Full run, no publishing

# Autonomous loop
python scheduler.py                    # Run once
python scheduler.py --loop             # Run every 24h
python scheduler.py --loop --hours 6   # Run every 6h
python scheduler.py --shadow           # Dry run, no publishing

# Focused single-keyword attack
python ranking_win.py

# API server
python -m api.server                   # Starts on :8900
```

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp config/.env.example config/.env
```

Edit `config/.env`:
```env
ANTHROPIC_API_KEY=your-key-here
MODEL=claude-sonnet-4-20250514
PORT=8900
DATAFORSEO_LOGIN=username
DATAFORSEO_PASSWORD=password
GSC_CREDENTIALS_PATH=config/gsc_credentials.json
GA4_PROPERTY_ID=G-xxxxx
```

> **Note:** If you have Claude Code CLI installed, `ANTHROPIC_API_KEY` is optional — the system auto-detects and uses your CLI subscription.

### 3. Run
```bash
python run_mvp.py --mode analyze
```

---

## Docker

```bash
# API + UI
docker-compose up

# API only
docker build -t seo-engine .
docker run -p 8900:8900 --env-file config/.env seo-engine
```

Services:
- API → `localhost:8900`
- UI → `localhost:3000`

---

## External Integrations

| Service | Purpose |
|---------|---------|
| Google Search Console | Rankings and search analytics |
| Google Business Profile | Local listing management |
| Google Analytics 4 | Traffic and behavior data |
| DataForSEO | SERP data, keyword volumes, backlinks |
| Claude API / CLI | All decision-making |
| WordPress | Direct site management |
| Blogger, Medium | Content syndication |
| Reddit | Community posting |
| Supabase (optional) | Cloud database |

---

## Key Design Principles

- **Claude-native** — Claude makes all decisions; no hardcoded rule trees
- **Autonomous-first** — Designed to run without human input; human is supervisor, not operator
- **Fast results** — Prioritizes 30-day ranking wins, targets page 2 keywords (positions 5–15)
- **Safety-gated** — Blocks blackhat and brand-unsafe actions automatically
- **Learning-enabled** — Analyzes every result and evolves its own prompts over time
- **Fully traceable** — Every decision logged with reasoning
