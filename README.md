# SEO Engine

Autonomous SEO operating system powered by Claude.

Completely standalone — plugs into external AI APIs but has zero dependencies on any other project.

## Structure

```
seo-engine/
├── core/               # Brain: prompts, agents, scoring
│   ├── prompts/        # All system prompts (versioned)
│   ├── agents/         # Multi-agent orchestration
│   └── scoring/        # Task scoring + priority algorithm
├── models/             # Data models (business, tasks, competitors)
├── api/                # FastAPI backend
├── config/             # Settings, env template
└── tests/
```

## Quick Start

```bash
pip install -r requirements.txt
cp config/.env.example config/.env   # Add your API key
python -m api.server                 # Start API on :8900
```
