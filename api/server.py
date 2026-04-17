"""SEO Engine API v7 — full autonomous SEO domination system."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from config.settings import PORT, LOG_LEVEL
from core.agents.brain import SEOBrain
from core.agents.orchestrator import AgentOrchestrator
from core.scoring.engine import score_and_rank
from data.storage.database import Database
from data.pipeline import IngestionPipeline
from execution.router import ExecutionRouter
from execution.models import ExecResult
from learning.loops import LearningEngine, LearningReport
from learning.patterns import PatternMemory
from core.claude import get_mode
from models.business import BusinessContext
from models.task import SEOTask, TaskBatch

logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO))
log = logging.getLogger(__name__)

# Attach lead capture router (CRM + contact form)
try:
    from conversion.crm import lead_router
    if lead_router is not None:
        from fastapi import APIRouter  # noqa — already imported below
except ImportError:
    lead_router = None

brain: SEOBrain | None = None
orchestrator: AgentOrchestrator | None = None
db: Database | None = None
pipeline: IngestionPipeline | None = None
executor: ExecutionRouter | None = None
learner: LearningEngine | None = None
patterns: PatternMemory | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global brain, orchestrator, db, pipeline, executor, learner, patterns
    db = Database()
    brain = SEOBrain()
    orchestrator = AgentOrchestrator()
    pipeline = IngestionPipeline(db)
    executor = ExecutionRouter(db)
    learner = LearningEngine(db)
    patterns = PatternMemory(db)

    # Register publishing connectors — must happen before any task publishes
    from execution.startup import init_publisher
    init_publisher()

    log.info("SEO Engine v5 initialized  model=%s", brain.model)
    yield


app = FastAPI(
    title="SEO Engine",
    version="1.0.0",
    description="Autonomous SEO domination system — analyze, decide, execute, learn, predict, signal, dominate",
    lifespan=lifespan,
)

# Mount lead capture router at /conversion
if lead_router is not None:
    app.include_router(lead_router, prefix="/conversion")


from api.tenant_routes import router as tenant_router
app.include_router(tenant_router)
from api.growth_routes import router as growth_router
app.include_router(growth_router)
from api.geo_routes import router as geo_router
app.include_router(geo_router)
from api.eeat_routes import router as eeat_router
app.include_router(eeat_router)
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.responses import Response
import hashlib, time, json

class _ResponseCacheMiddleware(BaseHTTPMiddleware):
    """Per-tenant GET response cache. TTL 30-60s. Invalidated on writes."""
    _CACHEABLE = {'/stats', '/content', '/keywords', '/health'}
    _TTL = 45  # seconds — warm but not stale

    async def dispatch(self, request: StarletteRequest, call_next):
        if request.method != 'GET':
            return await call_next(request)
        path = request.url.path
        if not any(path.endswith(p) or path.startswith('/businesses') for p in self._CACHEABLE):
            return await call_next(request)
        try:
            import redis as _redis
            r = _redis.from_url('redis://localhost:6379/0', decode_responses=True, socket_timeout=1)
            ck = 'api_cache:' + hashlib.sha256((path + '?' + str(request.query_params)).encode()).hexdigest()
            cached = r.get(ck)
            if cached:
                data = json.loads(cached)
                return Response(content=cached, media_type='application/json',
                                headers={'X-Cache': 'HIT', 'X-Cache-TTL': str(self._TTL)})
        except Exception:
            return await call_next(request)
        resp = await call_next(request)
        if resp.status_code == 200:
            body = b''
            async for chunk in resp.body_iterator:
                body += chunk
            try:
                r.setex(ck, self._TTL, body.decode())
            except Exception:
                pass
            return Response(content=body, status_code=resp.status_code,
                            headers=dict(resp.headers) | {'X-Cache': 'MISS'})
        return resp

app.add_middleware(_ResponseCacheMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3900", "http://127.0.0.1:3900"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---- Request / Response models ----

class AnalyzeRequest(BaseModel):
    business: BusinessContext
    input_type: Literal["GBP", "WEBSITE", "CONTENT", "AUTHORITY", "FULL"] = "FULL"
    max_actions: int = Field(default=5, ge=1, le=20)


class OrchestrateRequest(BaseModel):
    business: BusinessContext
    input_type: Literal["GBP", "WEBSITE", "CONTENT", "AUTHORITY", "FULL"] = "FULL"
    disagreement_mode: bool = False


class OrchestrateResponse(BaseModel):
    tasks: TaskBatch
    pipeline_log: dict


class IngestRequest(BaseModel):
    business: BusinessContext
    business_id: str
    skip_gsc: bool = False
    skip_gbp: bool = False
    skip_crawl: bool = False
    skip_competitors: bool = False
    skip_keywords: bool = False


class IngestResponse(BaseModel):
    agent_context: str
    freshness: str
    events: list[dict]
    data_sources: dict


class ExecuteRequest(BaseModel):
    """Execute scored tasks."""
    tasks: list[SEOTask]
    business: BusinessContext
    business_id: str
    shadow_mode: bool = False     # True = generate but don't publish


class ExecuteResponse(BaseModel):
    results: list[dict]
    executed: int
    queued: int
    skipped: int
    failed: int


class FullRunRequest(BaseModel):
    """Full pipeline: ingest → analyze → execute → learn."""
    business: BusinessContext
    business_id: str
    input_type: Literal["GBP", "WEBSITE", "CONTENT", "AUTHORITY", "FULL"] = "FULL"
    disagreement_mode: bool = False
    auto_execute: bool = False     # Execute AUTO tasks immediately
    shadow_mode: bool = False      # Shadow mode for execution
    skip_gsc: bool = False
    skip_gbp: bool = False
    skip_crawl: bool = False
    skip_competitors: bool = False
    skip_keywords: bool = False


class FullRunResponse(BaseModel):
    tasks: TaskBatch
    execution_results: list[dict]
    events: list[dict]
    freshness: str
    pipeline_log: dict


class ApproveRequest(BaseModel):
    task_id: str
    business_id: str


class RollbackRequest(BaseModel):
    task_id: str


class LearnRequest(BaseModel):
    business_id: str
    cycle: Literal["weekly", "monthly"] = "weekly"


class ScoreRequest(BaseModel):
    tasks: list[SEOTask]
    apply_filters: bool = True


class ScoreResponse(BaseModel):
    tasks: list[SEOTask]
    filtered_count: int


# ---- Endpoints ----

@app.get("/health")
async def health():
    return {"status": "ok", "service": "seo-engine", "version": "1.0.0", "claude_mode": get_mode()}


# --- AIC Engine ---

class AICRequest(BaseModel):
    keyword: str
    business: BusinessContext


@app.post("/aic")
async def aic_funnel(req: AICRequest):
    """Generate complete Attention -> Intent -> Conversion funnel for one keyword."""
    from aic.engine import AICEngine
    engine = AICEngine()
    result = await engine.generate(req.keyword, req.business)
    return result.model_dump()


@app.post("/influence")
async def influence_strategy(req: AICRequest):
    """Influence OS: perception map + narrative control + consensus + demand creation."""
    from aic.influence import InfluenceOS
    ios = InfluenceOS()
    result = await ios.generate(req.keyword, req.business)
    return result.model_dump()


@app.post("/perception")
async def perception_cycle_endpoint(req: AICRequest):
    """Perception Engine: detect narratives, choose strategy, deploy messaging."""
    from aic.perception.engine import run_perception_cycle
    result = await run_perception_cycle(
        keyword=req.keyword,
        business_name=req.business.business_name,
        service=req.business.primary_service,
        city=req.business.primary_city,
        reviews=req.business.reviews_count,
        competitors=req.business.competitors,
    )
    return result.model_dump()


# --- Simulation ---

class SimulateRequest(BaseModel):
    keyword: str
    current_position: int = 10
    current_ctr: float = 0.025
    current_traffic: int = 100
    current_authority: float = 25


@app.post("/simulate")
async def simulate(req: SimulateRequest):
    """Simulate multiple strategies and pick the best one BEFORE executing."""
    from simulation.engine import run_simulation
    result = run_simulation(
        keyword=req.keyword,
        current_position=req.current_position,
        current_ctr=req.current_ctr,
        current_traffic=req.current_traffic,
        current_authority=req.current_authority,
    )
    return result.model_dump()


# --- Self-Evolution ---

class EvolveRequest(BaseModel):
    business_id: str = "default"


@app.post("/evolve")
async def self_evolve_endpoint(req: EvolveRequest):
    """Run one self-evolution cycle: assess health, mutate strategy, evolve prompts."""
    from learning.evolution import self_evolve, StrategyParams, PromptModifier
    from core.prompts.system import MASTER_SYSTEM_PROMPT

    params = StrategyParams()
    modifiers: list[PromptModifier] = []

    new_params, evolved_prompt, new_mods, health = await self_evolve(
        db=db,
        business_id=req.business_id,
        params=params,
        base_prompt=MASTER_SYSTEM_PROMPT,
        modifiers=modifiers,
    )

    return {
        "health": health.model_dump(),
        "strategy_mutations": {
            "aggressiveness": new_params.aggressiveness,
            "content_depth": new_params.content_depth,
            "link_velocity": new_params.link_velocity,
            "update_frequency_days": new_params.update_frequency_days,
            "min_confidence": new_params.min_confidence,
        },
        "prompt_modifiers": len([m for m in new_mods if m.active]),
        "prompt_preview": evolved_prompt[-200:] if len(evolved_prompt) > 200 else evolved_prompt,
    }


@app.get("/health/system")
async def system_health():
    """Get overall system health metrics."""
    from learning.evolution import assess_health, StrategyParams, PromptModifier
    from learning.patterns import PatternMemory

    patterns = PatternMemory(db)
    all_patterns = await patterns.get_all_patterns()
    health = assess_health(all_patterns, StrategyParams(), [], [])
    return health.model_dump()


# --- Campaign Orchestrator ---

class CampaignRequest(BaseModel):
    keyword: str
    business: BusinessContext
    duration_days: int = 21


@app.post("/campaign")
async def create_campaign(req: CampaignRequest):
    """Create a 21-day phased campaign: foundation → distribution → amplification → reinforcement."""
    from orchestration.campaign import CampaignOrchestrator
    orch = CampaignOrchestrator()
    campaign = await orch.create_campaign(req.keyword, req.business, req.duration_days)
    return orch.campaign_summary(campaign)


# --- Personas ---

class PersonaRequest(BaseModel):
    keyword: str
    business: BusinessContext
    max_personas: int = 3


@app.post("/personas")
async def persona_campaign(req: PersonaRequest):
    """Generate multi-persona content — different voices for different channels."""
    from personas.system import PersonaSystem
    system = PersonaSystem()
    campaign = await system.generate_campaign(
        keyword=req.keyword,
        business_name=req.business.business_name,
        city=req.business.primary_city,
        max_personas=req.max_personas,
    )
    return campaign.model_dump()


@app.get("/personas/list")
async def list_personas():
    """List all available personas and their stats."""
    from personas.system import PersonaSystem
    system = PersonaSystem()
    return system.get_summary()


# --- External Network ---

@app.post("/web-post")
async def web_posting_plan(req: AICRequest):
    """Find REAL third-party websites and prepare submissions for each one."""
    from execution.connectors.external.web_poster import WebPoster
    poster = WebPoster()
    plan = await poster.create_posting_plan(
        keyword=req.keyword,
        business_name=req.business.business_name,
        service=req.business.primary_service,
        city=req.business.primary_city,
        website=req.business.website,
    )
    return plan.model_dump()


class AutoSignupRequest(BaseModel):
    keyword: str
    business: BusinessContext
    max_sites: int = 3
    use_ai_browser: bool = False    # True = browser-use AI, False = Playwright


@app.post("/auto-signup")
async def auto_signup(req: AutoSignupRequest):
    """Find sites + auto-register on them (disposable email + form filling)."""
    from execution.connectors.external.web_poster import WebPoster
    poster = WebPoster()

    # Step 1: Find targets
    targets = await poster.find_targets(
        req.keyword, req.business.business_name,
        req.business.primary_service, req.business.primary_city,
    )
    if not targets:
        return {"status": "no_targets", "results": []}

    # Step 2: Prepare content
    submissions = await poster.prepare_submissions(
        targets, req.business.business_name, req.business.primary_service,
        req.business.primary_city, req.business.website,
    )

    # Step 3: Auto-register
    results = await poster.auto_register_sites(
        targets=targets,
        submissions=submissions,
        business_name=req.business.business_name,
        website=req.business.website,
        city=req.business.primary_city,
        service=req.business.primary_service,
        max_sites=req.max_sites,
        use_ai=req.use_ai_browser,
    )

    return {
        "status": "completed",
        "sites_found": len(targets),
        "signups_attempted": len(results),
        "results": results,
    }


@app.post("/create-email")
async def create_disposable_email():
    """Create a disposable email address with real inbox (for testing)."""
    from execution.connectors.external.auto_signup import AutoSignupEngine
    engine = AutoSignupEngine()
    email, password = await engine.create_email()
    return {"email": email, "password": password}


@app.post("/external-network")
async def external_network(req: AICRequest):
    """Generate platform-adapted content for Medium, Reddit, Quora, directories."""
    from execution.connectors.external.network import ExternalNetwork
    network = ExternalNetwork()
    plan = await network.plan_distribution(
        keyword=req.keyword,
        business_name=req.business.business_name,
        service=req.business.primary_service,
        city=req.business.primary_city,
        target_page=f"{req.business.website}/{req.keyword.replace(' ', '-')}",
    )
    return plan.model_dump()


# --- Publish ---

class PublishRequest(BaseModel):
    aic_result: dict = {}
    dry_run: bool = True


@app.get("/publish/status")
async def publish_status():
    """Show which publishing channels are connected and which need setup."""
    import os
    return {
        "channels": {
            "wordpress": {
                "status": "ready" if os.getenv("WP_URL") and os.getenv("WP_USER") else "needs_setup",
                "setup": "Add WP_URL, WP_USER, WP_APP_PASSWORD to config/.env",
                "auto_publish": True,
            },
            "medium": {
                "status": "ready" if os.getenv("MEDIUM_TOKEN") else "needs_setup",
                "setup": "Get token at medium.com/me/settings/security -> Add MEDIUM_TOKEN to config/.env",
                "auto_publish": True,
            },
            "blogger": {
                "status": "ready" if os.getenv("BLOGGER_BLOG_ID") else "needs_setup",
                "setup": "Create free blog at blogger.com -> Add BLOGGER_BLOG_ID to config/.env",
                "auto_publish": True,
                "owns": False,
            },
            "wordpress_com": {
                "status": "ready" if os.getenv("WP_COM_TOKEN") else "needs_setup",
                "setup": "Create free blog at wordpress.com -> Get OAuth token -> Add WP_COM_SITE + WP_COM_TOKEN to config/.env",
                "auto_publish": True,
                "owns": False,
            },
            "tumblr": {
                "status": "ready" if os.getenv("TUMBLR_TOKEN") else "needs_setup",
                "setup": "Create blog at tumblr.com -> Register app -> Add TUMBLR_* to config/.env",
                "auto_publish": True,
                "owns": False,
            },
            "pinterest": {
                "status": "ready" if os.getenv("PINTEREST_TOKEN") else "needs_setup",
                "setup": "Create business account -> Get API token -> Add PINTEREST_TOKEN + PINTEREST_BOARD to config/.env",
                "auto_publish": True,
                "owns": False,
            },
            "social": {
                "status": "active",
                "setup": "Content generated and queued. Post manually or connect via n8n.",
                "auto_publish": False,
            },
            "tiktok": {
                "status": "active",
                "setup": "Scripts generated. Film and post. No direct API available.",
                "auto_publish": False,
            },
            "gbp": {
                "status": "active",
                "setup": "Posts generated. Copy to GBP dashboard or use Playwright automation.",
                "auto_publish": False,
            },
            "reddit": {
                "status": "active",
                "setup": "Discussion posts generated. Post manually (recommended for trust building).",
                "auto_publish": False,
            },
            "quora": {
                "status": "active",
                "setup": "Answers generated. Post manually.",
                "auto_publish": False,
            },
            "directories": {
                "status": "active",
                "setup": "Listing content generated. Submit manually to each directory.",
                "auto_publish": False,
            },
        },
        "auto_publishable": ["wordpress", "medium", "blogger", "wordpress_com", "tumblr", "pinterest"],
        "content_ready": ["social", "tiktok", "gbp", "reddit", "quora", "directories"],
    }


@app.post("/publish")
async def publish_content(req: PublishRequest):
    """Publish AIC result across all connected channels (dry_run=True by default)."""
    import os
    from execution.publisher import MultiChannelPublisher
    from execution.connectors.social import SocialConnector, TikTokConnector, GBPConnector
    publisher = MultiChannelPublisher()
    publisher.register("social", SocialConnector())
    publisher.register("tiktok", TikTokConnector())
    publisher.register("gbp", GBPConnector())

    # Auto-register WordPress if configured
    wp_url = os.getenv("WP_URL")
    wp_user = os.getenv("WP_USER")
    wp_pass = os.getenv("WP_APP_PASSWORD")
    if wp_url and wp_user and wp_pass:
        from execution.connectors.wordpress import WordPressConnector
        publisher.register("wordpress", WordPressConnector(wp_url, wp_user, wp_pass))

    # Auto-register Medium if configured
    medium_token = os.getenv("MEDIUM_TOKEN")
    if medium_token:
        from execution.connectors.external.medium import MediumConnector
        publisher.register("medium", MediumConnector(medium_token))
    # WordPress only if configured
    report = await publisher.publish_aic_result(req.aic_result, dry_run=req.dry_run)
    return report.model_dump()


# --- Full Power ---

class FullPowerRequest(BaseModel):
    business: BusinessContext
    business_id: str = "default"


@app.post("/full-power")
async def full_power(req: FullPowerRequest):
    """Run ALL system capabilities — analysis + CTR + SERP hijack + rapid updates + competitor reaction + signal burst + authority gap + suppression + AI visibility."""
    from core.full_power import run_full_power
    report = await run_full_power(req.business, req.business_id)
    return report.model_dump()


# --- Analysis ---

@app.post("/analyze", response_model=TaskBatch)
async def analyze(req: AnalyzeRequest):
    """Single-brain mode: 1 Claude call → scored tasks."""
    return await brain.analyze(
        business=req.business,
        input_type=req.input_type,
        max_actions=req.max_actions,
    )


@app.post("/orchestrate", response_model=OrchestrateResponse)
async def orchestrate(req: OrchestrateRequest):
    """Multi-agent mode: 4-5 chained Claude calls → deep analysis."""
    batch, plog = await orchestrator.run(
        business=req.business,
        input_type=req.input_type,
        disagreement_mode=req.disagreement_mode,
    )
    return OrchestrateResponse(tasks=batch, pipeline_log=plog.to_dict())


# --- Data ---

@app.post("/ingest", response_model=IngestResponse)
async def ingest(req: IngestRequest):
    """Pull live data from all sources."""
    data = await pipeline.run_full(
        business=req.business,
        business_id=req.business_id,
        skip_gsc=req.skip_gsc,
        skip_gbp=req.skip_gbp,
        skip_crawl=req.skip_crawl,
        skip_competitors=req.skip_competitors,
        skip_keywords=req.skip_keywords,
    )
    return IngestResponse(
        agent_context=data.to_agent_context(),
        freshness=data.freshness.to_prompt_block(),
        events=[e.model_dump() for e in data.events],
        data_sources={
            name: {"confidence": src.confidence, "freshness": src.freshness.value}
            for name, src in data.freshness.sources.items()
        },
    )


# --- Execution ---

@app.post("/execute", response_model=ExecuteResponse)
async def execute(req: ExecuteRequest):
    """Execute a batch of tasks (respects execution_mode + safety)."""
    results = await executor.execute_batch(
        tasks=req.tasks,
        business=req.business,
        business_id=req.business_id,
        force_shadow=req.shadow_mode,
    )

    executed = sum(1 for r in results if r.status.value == "success")
    queued = sum(1 for r in results if r.status.value == "queued")
    skipped = sum(1 for r in results if r.status.value == "skipped")
    failed = sum(1 for r in results if r.status.value == "failed")

    return ExecuteResponse(
        results=[r.model_dump() for r in results],
        executed=executed,
        queued=queued,
        skipped=skipped,
        failed=failed,
    )


@app.post("/approve")
async def approve(req: ApproveRequest):
    """Approve a queued ASSISTED task for execution."""
    result = await executor.approve_task(req.task_id, req.business_id)
    return result.model_dump()


@app.post("/rollback")
async def rollback(req: RollbackRequest):
    """Roll back a previously executed task."""
    result = await executor.rollback(req.task_id)
    return result.model_dump()


# --- Learning ---

@app.post("/learn", response_model=LearningReport)
async def learn(req: LearnRequest):
    """Run a learning cycle (weekly or monthly)."""
    if req.cycle == "weekly":
        return await learner.weekly_cycle(req.business_id)
    else:
        return await learner.monthly_cycle(req.business_id)


@app.get("/patterns")
async def get_patterns():
    """Get all learned action patterns."""
    all_patterns = await patterns.get_all_patterns()
    return [p.model_dump() for p in all_patterns]


# --- Full Pipeline ---

@app.post("/run", response_model=FullRunResponse)
async def full_run(req: FullRunRequest):
    """Full pipeline: ingest → agents → execute → store.
    The primary production endpoint."""
    # 1. Ingest live data
    data = await pipeline.run_full(
        business=req.business,
        business_id=req.business_id,
        skip_gsc=req.skip_gsc,
        skip_gbp=req.skip_gbp,
        skip_crawl=req.skip_crawl,
        skip_competitors=req.skip_competitors,
        skip_keywords=req.skip_keywords,
    )

    # 2. Multi-agent analysis
    batch, plog = await orchestrator.run(
        business=req.business,
        input_type=req.input_type,
        disagreement_mode=req.disagreement_mode,
    )

    # 3. Apply freshness penalty
    penalty = data.freshness.confidence_penalty()
    if penalty > 0:
        for task in batch.tasks:
            task.confidence_score = max(1.0, task.confidence_score - penalty)

    # 4. Apply pattern learning adjustments
    for task in batch.tasks:
        pattern = await patterns.get_pattern(task.action, task.type.value)
        adj = patterns.confidence_adjustment(pattern)
        if adj != 0:
            task.confidence_score = max(1.0, min(10.0, task.confidence_score + adj))

    # 5. Execute (if enabled)
    exec_results = []
    if req.auto_execute:
        results = await executor.execute_batch(
            tasks=batch.tasks,
            business=req.business,
            business_id=req.business_id,
            force_shadow=req.shadow_mode,
        )
        exec_results = [r.model_dump() for r in results]

    # 6. Save tasks
    await db.save_tasks(req.business_id, [t.model_dump() for t in batch.tasks])

    return FullRunResponse(
        tasks=batch,
        execution_results=exec_results,
        events=[e.model_dump() for e in data.events],
        freshness=data.freshness.overall_confidence(),
        pipeline_log=plog.to_dict(),
    )


# --- Utility ---

@app.post("/score", response_model=ScoreResponse)
async def score(req: ScoreRequest):
    """Score/re-score tasks server-side without calling Claude."""
    ranked, filtered = score_and_rank(req.tasks, apply_filters=req.apply_filters)
    return ScoreResponse(tasks=ranked, filtered_count=filtered)


@app.post("/context-preview")
async def context_preview(business: BusinessContext):
    """Preview the prompt block that gets sent to Claude."""
    return {"prompt_block": business.to_prompt_block()}


# --- Prediction ---

class PredictRequest(BaseModel):
    url: str
    keyword: str
    current_rank: int = 0
    word_count: int = 0
    keyword_in_title: bool = False
    keyword_in_h1: bool = False
    heading_count: int = 0
    backlink_count: int = 0
    domain_authority: float = 0
    ctr: float = 0
    days_since_update: int = 30
    competitor_avg_authority: float = 0
    competitor_avg_words: int = 0


@app.post("/predict")
async def predict_rank(req: PredictRequest):
    """Predict ranking position and generate gap analysis."""
    from prediction.scoring import score_page, analyze_gap, build_timeline
    ps = score_page(**req.model_dump())
    gap = analyze_gap(ps, {
        "avg_word_count": req.competitor_avg_words or 1200,
        "avg_backlinks": 5,
        "page_backlinks": req.backlink_count,
        "page_word_count": req.word_count,
    })
    tl = build_timeline(ps, gap)
    return {
        "page_score": ps.model_dump(),
        "gap": gap.model_dump(),
        "timeline": tl.model_dump(),
    }


# --- Channels ---

class MultiplyRequest(BaseModel):
    keyword: str
    business: BusinessContext


@app.post("/multiply")
async def multiply_content(req: MultiplyRequest):
    """Generate a full content bundle (5 formats) from one keyword."""
    from channels.multiplier import ContentMultiplier
    multiplier = ContentMultiplier()
    bundle = await multiplier.multiply(req.keyword, req.business)
    return bundle.model_dump()


# --- Market Domination ---

class DominationRequest(BaseModel):
    keyword: str
    business: BusinessContext
    existing_pages: list[str] = []


@app.post("/dominate")
async def dominate_market(req: DominationRequest):
    """Plan full market domination for a keyword cluster."""
    from strategy.domination import MarketDominator
    dominator = MarketDominator()
    plan = await dominator.analyze_market(
        keyword=req.keyword,
        business_name=req.business.business_name,
        service=req.business.primary_service,
        city=req.business.primary_city,
        existing_pages=req.existing_pages,
    )
    return plan.model_dump()


# --- Cross-Business Learning ---

class CrossLearnRequest(BaseModel):
    business_ids: list[str]


@app.post("/cross-learn")
async def cross_learn(req: CrossLearnRequest):
    """Aggregate learnings across all managed businesses."""
    from strategy.cross_business import CrossBusinessLearner
    learner = CrossBusinessLearner(db)
    report = await learner.aggregate_patterns(req.business_ids)
    return report.model_dump()


# --- Signal Burst ---

class BurstRequest(BaseModel):
    keyword: str
    page_url: str
    position: int
    business: BusinessContext
    intensity: Literal["low", "medium", "high"] | None = None


@app.post("/burst")
async def signal_burst(req: BurstRequest):
    """Plan a signal burst campaign — controlled spike of activity for ranking push."""
    from signals.burst import SignalBurstEngine, BurstIntensity
    engine = SignalBurstEngine()

    if not engine.should_burst(req.position):
        return {"eligible": False, "reason": f"Position #{req.position} not in burst range (4-10)"}

    intensity = BurstIntensity(req.intensity) if req.intensity else None
    plan = await engine.plan_burst(
        keyword=req.keyword,
        page_url=req.page_url,
        position=req.position,
        business_name=req.business.business_name,
        city=req.business.primary_city,
        intensity=intensity,
    )
    return {"eligible": True, "plan": plan.model_dump()}


# --- Authority Gap ---

class AuthorityGapRequest(BaseModel):
    keyword: str
    our_da: float
    our_links: int
    competitor_name: str
    competitor_da: float
    competitor_links: int
    business: BusinessContext


@app.post("/authority-gap")
async def authority_gap(req: AuthorityGapRequest):
    """Calculate authority gap and generate a link building plan to close it."""
    from prediction.authority_gap import AuthorityGapAccelerator
    accel = AuthorityGapAccelerator()

    gap = accel.calculate_gap(
        keyword=req.keyword,
        our_da=req.our_da,
        our_links=req.our_links,
        competitor_name=req.competitor_name,
        competitor_da=req.competitor_da,
        competitor_links=req.competitor_links,
    )

    plan = await accel.generate_plan(
        gap=gap,
        business_name=req.business.business_name,
        city=req.business.primary_city,
        keyword=req.keyword,
    )

    return {
        "gap": gap.model_dump(),
        "plan": plan.model_dump(),
        "quick_recommendation": accel.recommend_strategy(gap),
    }


# --- Rapid Update ---

class RapidUpdateRequest(BaseModel):
    page_url: str
    keyword: str
    business: BusinessContext
    position: int = 0
    update_number: int = 1
    previous_updates: list[str] = []


@app.post("/rapid-update")
async def rapid_update(req: RapidUpdateRequest):
    """Generate incremental page updates for freshness-driven ranking."""
    from prediction.rapid_update import RapidUpdateEngine
    engine = RapidUpdateEngine(db)
    plan = await engine.generate_updates(
        page_url=req.page_url,
        keyword=req.keyword,
        business_name=req.business.business_name,
        city=req.business.primary_city,
        position=req.position,
        update_number=req.update_number,
        previous_updates=req.previous_updates,
    )
    return plan.model_dump()


class StagnantRequest(BaseModel):
    rankings: dict[str, int]


@app.post("/rapid-update/detect")
async def detect_stagnant(req: StagnantRequest):
    """Find stagnant pages that need rapid updates."""
    from prediction.rapid_update import RapidUpdateEngine
    engine = RapidUpdateEngine(db)
    return engine.find_stagnant_pages(req.rankings)


# --- Competitor Reaction ---

class CompetitorReactionRequest(BaseModel):
    our_rankings: dict[str, int]
    previous_rankings: dict[str, int] = {}
    competitor_rankings: dict[str, dict[str, int]] = {}
    previous_competitor_rankings: dict[str, dict[str, int]] = {}
    business: BusinessContext


@app.post("/competitor-react")
async def competitor_react(req: CompetitorReactionRequest):
    """Detect competitor moves and generate counter-actions."""
    from prediction.competitor_reaction import CompetitorReactor
    reactor = CompetitorReactor()

    moves = reactor.detect_moves(
        our_rankings=req.our_rankings,
        previous_rankings=req.previous_rankings,
        competitor_rankings=req.competitor_rankings,
        previous_competitor_rankings=req.previous_competitor_rankings,
    )

    if not moves:
        return {"moves": [], "reactions": []}

    plans = await reactor.react_to_all(
        moves=moves,
        business_name=req.business.business_name,
        city=req.business.primary_city,
    )

    return {
        "moves": [m.model_dump() for m in moves],
        "reactions": [p.model_dump() for p in plans],
    }


# --- CTR Domination ---

class CTRTestRequest(BaseModel):
    page_url: str
    keyword: str
    current_title: str = ""
    current_meta: str = ""
    current_ctr: float = 0.0
    position: int = 0
    impressions: int = 0
    business: BusinessContext


@app.post("/ctr/generate")
async def ctr_generate(req: CTRTestRequest):
    """Generate 3 CTR-optimized title/meta variants for A/B testing."""
    from prediction.ctr import CTRDominator
    ctr = CTRDominator(db)
    variants = await ctr.generate_variants(
        page_url=req.page_url,
        keyword=req.keyword,
        current_title=req.current_title,
        current_meta=req.current_meta,
        current_ctr=req.current_ctr,
        position=req.position,
        impressions=req.impressions,
        business_name=req.business.business_name,
        city=req.business.primary_city,
        reviews=req.business.reviews_count,
    )
    return [v.model_dump() for v in variants]


class CTRDetectRequest(BaseModel):
    gsc_data: list[dict]
    min_impressions: int = 100


@app.post("/ctr/detect")
async def ctr_detect(req: CTRDetectRequest):
    """Detect low-CTR pages that should be tested."""
    from prediction.ctr import CTRDominator
    ctr = CTRDominator(db)
    return ctr.detect_low_ctr_pages(req.gsc_data, req.min_impressions)


# --- SERP Hijack ---

class SERPHijackRequest(BaseModel):
    keyword: str
    business: BusinessContext
    current_position: int = 0


@app.post("/serp-hijack")
async def serp_hijack(req: SERPHijackRequest):
    """Plan a SERP hijack cluster — multiple pages targeting one keyword."""
    from prediction.serp_hijack import SERPHijacker
    hijacker = SERPHijacker()
    cluster = await hijacker.plan_cluster(
        keyword=req.keyword,
        business_name=req.business.business_name,
        service=req.business.primary_service,
        city=req.business.primary_city,
        current_position=req.current_position,
    )
    return cluster.model_dump()


# --- Signal + Suppression ---

class SuppressionRequest(BaseModel):
    our_keywords: dict[str, int]
    competitor_keywords: dict[str, dict]
    competitor_links: dict[str, int] = {}
    our_link_count: int = 0


@app.post("/suppression")
async def suppression_analysis(req: SuppressionRequest):
    """Analyze competitive suppression opportunities."""
    from signals.suppression import analyze_suppression_opportunities
    actions = analyze_suppression_opportunities(
        our_keywords=req.our_keywords,
        competitor_keywords=req.competitor_keywords,
        competitor_links=req.competitor_links,
        our_link_count=req.our_link_count,
    )
    return [a.model_dump() for a in actions]


class PressureRequest(BaseModel):
    keyword: str
    cluster_keywords: list[str] = []
    intensity: Literal["standard", "aggressive", "blitz"] = "standard"


@app.post("/pressure")
async def pressure_campaign(req: PressureRequest):
    """Plan a multi-channel pressure campaign for a keyword."""
    from signals.pressure import PressureEngine
    engine = PressureEngine()
    campaign = engine.plan_campaign(req.keyword, req.cluster_keywords, None, req.intensity)
    return campaign.model_dump()


class DemandRequest(BaseModel):
    keyword: str
    business: BusinessContext


@app.post("/demand")
async def demand_campaign(req: DemandRequest):
    """Generate a demand generation campaign (branded search driving)."""
    from signals.demand import DemandEngine
    engine = DemandEngine()
    campaign = await engine.create_campaign(req.keyword, req.business)
    return campaign.model_dump()


# --- Entity ---

@app.post("/entity-audit")
async def entity_audit(business: BusinessContext):
    """Run entity dominance audit — find gaps in how Google sees the brand."""
    from entity.dominance import EntityEngine
    engine = EntityEngine()
    profile, actions = await engine.audit(business)
    return {"profile": profile.model_dump(), "actions": [a.model_dump() for a in actions]}


@app.post("/entity-schema")
async def entity_schema(business: BusinessContext):
    """Generate comprehensive schema.org JSON-LD markup."""
    from entity.dominance import EntityEngine
    return EntityEngine.generate_schema_markup(business)


# --- Autonomous ---

class AutonomousRunRequest(BaseModel):
    business: BusinessContext
    business_id: str
    shadow_mode: bool = True
    max_auto_executions: int = 3
    min_confidence: float = 7.0


@app.post("/autonomous")
async def autonomous_cycle(req: AutonomousRunRequest):
    """Run one full autonomous cycle — data → agents → execute → learn."""
    from strategy.autonomous import AutonomousRunner, AutonomousConfig
    config = AutonomousConfig(
        business_id=req.business_id,
        business=req.business,
        shadow_mode=req.shadow_mode,
        max_auto_executions_per_day=req.max_auto_executions,
        min_confidence_for_auto=req.min_confidence,
    )
    runner = AutonomousRunner(config)
    result = await runner.run_cycle()
    return result.model_dump()


# --- Cognitive System ---

class CognitiveRequest(BaseModel):
    business: BusinessContext
    business_id: str
    goal_keyword: str | None = None
    target_position: int = 3
    execute: bool = False


@app.post("/cognitive")
async def cognitive_cycle(req: CognitiveRequest):
    """Run one cognitive cycle: perceive → plan → execute → reflect → learn."""
    from core.cognitive import CognitiveSystem
    system = CognitiveSystem(db)
    result = await system.run_cycle(
        business=req.business,
        business_id=req.business_id,
        goal_keyword=req.goal_keyword,
        target_position=req.target_position,
        execute=req.execute,
    )
    return result.model_dump()


@app.post("/plan")
async def create_plan(req: CognitiveRequest):
    """Create a multi-step plan with dependencies (no execution)."""
    from core.planner.engine import PlanningEngine
    from core.world_model.state import WorldModel
    planner = PlanningEngine()
    world = WorldModel(db)
    state = await world.get_state(req.business_id)
    world_block = world.to_prompt_block(state)

    keyword = req.goal_keyword or (req.business.primary_keywords[0] if req.business.primary_keywords else req.business.primary_service)
    position = req.business.current_rankings.get(keyword, 0)

    plan = await planner.create_plan(
        keyword=keyword,
        current_position=position,
        target_position=req.target_position,
        business_name=req.business.business_name,
        city=req.business.primary_city,
        world_state_block=world_block,
    )
    return plan.model_dump()


# --- Rank Tracking ---

class RankCheckRequest(BaseModel):
    domain: str
    keywords: list[str]
    location_code: int = 2840


@app.post("/rank-check")
async def rank_check(req: RankCheckRequest):
    """Check current rankings for all keywords via DataForSEO SERP API."""
    from data.connectors.rank_tracker import RankTracker
    tracker = RankTracker()
    results = tracker.check_rankings(req.domain, req.keywords, req.location_code)
    return {"domain": req.domain, "results": results, "count": len(results)}


@app.post("/rank-delta")
async def rank_delta(req: RankCheckRequest):
    """Get ranking changes over the last 7 days."""
    from data.connectors.rank_tracker import RankTracker
    tracker = RankTracker()
    report = tracker.get_summary_report(req.domain, req.keywords)
    return report


@app.get("/rank-history/{domain}")
async def rank_history(domain: str, keyword: str, days: int = 90):
    """Get full rank history for a keyword."""
    from data.connectors.rank_tracker import RankTracker
    tracker = RankTracker()
    history = tracker.get_rank_history(domain, keyword, days)
    return {"domain": domain, "keyword": keyword, "history": history}


# --- Content Decay ---

class DecayRequest(BaseModel):
    domain: str


@app.post("/content-decay")
async def content_decay_scan(req: DecayRequest):
    """Scan all pages for content decay — find what's losing traffic."""
    from data.analyzers.content_decay import ContentDecayDetector
    detector = ContentDecayDetector()
    report = detector.generate_decay_report(req.domain)
    return report


@app.get("/content-decay/queue/{domain}")
async def decay_refresh_queue(domain: str, limit: int = 10):
    """Get the top pages that need refreshing most urgently."""
    from data.analyzers.content_decay import ContentDecayDetector
    detector = ContentDecayDetector()
    queue = detector.get_refresh_priority_queue(domain, limit)
    return {"domain": domain, "refresh_queue": queue}


# --- IndexNow ---

class IndexNowRequest(BaseModel):
    urls: list[str]
    host: str = ""
    key: str = ""


@app.post("/indexnow")
async def submit_indexnow(req: IndexNowRequest):
    """Submit URLs to IndexNow for instant indexing across Google, Bing, Yandex."""
    from execution.indexnow import IndexNow
    indexnow = IndexNow(key=req.key or None, host=req.host or None)
    if not indexnow.is_configured():
        return {"error": "IndexNow not configured. Set INDEXNOW_API_KEY and SITE_HOST in config/.env"}
    result = indexnow.submit_batch(req.urls)
    return result


@app.post("/indexnow/sitemap")
async def ping_sitemap(sitemap_url: str):
    """Ping Google and Bing about a sitemap update."""
    from execution.indexnow import IndexNow
    indexnow = IndexNow()
    result = indexnow.submit_sitemap(sitemap_url)
    return result


# --- Content Quality ---

class QualityCheckRequest(BaseModel):
    content: str
    title: str = ""


@app.post("/quality/check")
async def quality_check(req: QualityCheckRequest):
    """Check content for AI generation and originality."""
    from quality.originality import OriginalityChecker
    checker = OriginalityChecker()
    result = checker.check(req.content, req.title)
    return result


@app.post("/quality/schema-validate")
async def schema_validate(req: QualityCheckRequest):
    """Validate JSON-LD schema markup in HTML or raw JSON string."""
    from quality.schema_validator import SchemaValidator
    validator = SchemaValidator()
    if req.content.strip().startswith("<"):
        result = validator.validate_schema_types(req.content)
    else:
        result = validator.validate_json_ld(req.content)
    return result


# --- Author Entity ---

class AuthorRequest(BaseModel):
    specialty: str
    business_name: str = ""


@app.post("/authors/get-or-create")
async def get_or_create_author(req: AuthorRequest):
    """Get or create an author profile for a specialty."""
    from core.authors.system import AuthorSystem
    authors = AuthorSystem()
    author = authors.get_or_create_author(req.specialty, req.business_name)
    return {
        "author": author.__dict__,
        "schema": authors.get_author_schema(author),
        "byline_html": authors.get_author_byline_html(author),
    }


@app.get("/authors")
async def list_authors():
    """List all author profiles."""
    from core.authors.system import AuthorSystem
    authors = AuthorSystem()
    return [a.__dict__ for a in authors.list_all_authors()]


@app.get("/authors/{specialty}/page")
async def author_page_html(specialty: str):
    """Get the HTML author page for a specialty."""
    from core.authors.system import AuthorSystem
    from fastapi.responses import HTMLResponse
    authors = AuthorSystem()
    author = authors.load_author(specialty)
    if not author:
        return {"error": f"No author found for specialty: {specialty}"}
    return HTMLResponse(content=authors.render_author_page(author))


# --- Entity sameAs Registry ---

class SameAsRequest(BaseModel):
    business_id: str
    platform: str
    url: str


@app.post("/entity/same-as/add")
async def add_same_as(req: SameAsRequest):
    """Add an external entity URL (GBP, Yelp, BBB, LinkedIn, etc.) to the sameAs registry."""
    from core.entity.same_as import SameAsRegistry
    registry = SameAsRegistry()
    success = registry.add_entity(req.business_id, req.platform, req.url)
    return {"added": success, "business_id": req.business_id, "platform": req.platform}


@app.get("/entity/same-as/{business_id}")
async def get_same_as(business_id: str):
    """Get all sameAs URLs and entity strength score for a business."""
    from core.entity.same_as import SameAsRegistry
    registry = SameAsRegistry()
    urls = registry.get_same_as_urls(business_id)
    score = registry.get_entity_strength_score(business_id)
    return {"business_id": business_id, "same_as_urls": urls, "strength": score}


class AutoDiscoverRequest(BaseModel):
    business_id: str
    business_name: str
    location: str = ""


@app.post("/entity/same-as/discover")
async def discover_same_as(req: AutoDiscoverRequest):
    """Auto-discover entity URLs by probing known platform patterns."""
    from core.entity.same_as import SameAsRegistry
    registry = SameAsRegistry()
    result = registry.auto_discover(req.business_name, req.business_id, req.location)
    return result


# --- AI Citation Monitoring ---

class CitationMonitorRequest(BaseModel):
    brand: str
    domain: str = ""
    location: str = ""


@app.post("/monitoring/citations")
async def monitor_citations(req: CitationMonitorRequest):
    """Check if brand is being cited in AI search responses and web mentions."""
    from monitoring.brand_mentions import BrandMentionMonitor
    monitor = BrandMentionMonitor()
    report = monitor.generate_citation_report(req.brand, req.domain)
    return report


@app.post("/monitoring/perplexity")
async def check_perplexity(req: CitationMonitorRequest):
    """Check if brand appears in Perplexity AI search results."""
    from monitoring.brand_mentions import BrandMentionMonitor
    monitor = BrandMentionMonitor()
    mentions = monitor.check_perplexity_citation(req.brand)
    return {"brand": req.brand, "perplexity_mentions": [m.__dict__ for m in mentions]}


# --- AI Search Setup ---

class AISetupRequest(BaseModel):
    site_url: str


@app.post("/ai-search/setup-checklist")
async def ai_setup_checklist(req: AISetupRequest):
    """Full AI search readiness checklist — llms.txt, robots.txt, AI crawlers, schema."""
    from ai_visibility.llms_txt import get_full_setup_checklist
    return get_full_setup_checklist(req.site_url)


@app.post("/ai-search/robots-additions")
async def robots_additions(req: AISetupRequest):
    """Get the robots.txt lines needed to allow AI crawlers."""
    from ai_visibility.llms_txt import generate_robots_txt_additions
    return {"additions": generate_robots_txt_additions(req.site_url)}


@app.post("/ai-search/sitemap-xml")
async def ai_sitemap(urls: list[dict]):
    """Generate a sitemap-ai.xml for AI search engines."""
    from ai_visibility.llms_txt import generate_sitemap_ai_xml
    xml = generate_sitemap_ai_xml(urls)
    from fastapi.responses import Response
    return Response(content=xml, media_type="application/xml")


# --- Queue Status ---

@app.get("/queue/status")
async def queue_status():
    """Get Celery queue stats — pending tasks, active workers, results."""
    try:
        from taskq.celery_app import app as celery_app
        inspect = celery_app.control.inspect(timeout=3)
        active = inspect.active() or {}
        reserved = inspect.reserved() or {}
        stats = inspect.stats() or {}
        return {
            "status": "connected",
            "active_tasks": sum(len(v) for v in active.values()),
            "queued_tasks": sum(len(v) for v in reserved.values()),
            "workers": list(active.keys()),
            "worker_count": len(active),
        }
    except Exception as e:
        return {"status": "unavailable", "error": str(e), "note": "Start Redis + Celery worker to enable background tasks"}


class QueueTaskRequest(BaseModel):
    business_id: str
    business_data: dict
    mode: str = "analyze"


@app.post("/queue/submit")
async def queue_task(req: QueueTaskRequest):
    """Submit a business analysis task to the Celery queue (non-blocking)."""
    try:
        from taskq.tasks import analyze_business, orchestrate_business
        if req.mode == "orchestrate":
            task = orchestrate_business.delay(req.business_id, req.business_data)
        else:
            task = analyze_business.delay(req.business_id, req.business_data)
        return {"task_id": task.id, "status": "queued", "mode": req.mode}
    except Exception as e:
        return {"error": str(e), "note": "Celery worker not running. Start with: celery -A taskq.celery_app worker"}


@app.get("/queue/result/{task_id}")
async def queue_result(task_id: str):
    """Get the result of a queued task."""
    try:
        from taskq.celery_app import app as celery_app
        result = celery_app.AsyncResult(task_id)
        return {
            "task_id": task_id,
            "status": result.status,
            "result": result.result if result.ready() else None,
        }
    except Exception as e:
        return {"error": str(e)}


# --- Autonomous Approval Queue ---

@app.get("/autonomous/approval-queue/{business_id}")
async def get_approval_queue(business_id: str):
    """Get all tasks pending human approval for a business."""
    from strategy.autonomous import AutonomousRunner
    runner = AutonomousRunner.__new__(AutonomousRunner)
    return runner.get_approval_queue(business_id)


class ApprovalActionRequest(BaseModel):
    business_id: str
    task_id: str


@app.post("/autonomous/approve-task")
async def approve_autonomous_task(req: ApprovalActionRequest):
    """Approve a queued autonomous task for execution."""
    from strategy.autonomous import AutonomousRunner
    runner = AutonomousRunner.__new__(AutonomousRunner)
    return runner.approve_task(req.business_id, req.task_id)


@app.post("/autonomous/reject-task")
async def reject_autonomous_task(req: ApprovalActionRequest):
    """Reject and discard a queued autonomous task."""
    from strategy.autonomous import AutonomousRunner
    runner = AutonomousRunner.__new__(AutonomousRunner)
    return runner.reject_task(req.business_id, req.task_id)


# --- CWV Analysis ---

class CWVRequest(BaseModel):
    url: str
    strategy: str = "mobile"


@app.post("/cwv")
async def cwv_analysis(req: CWVRequest):
    """Analyze Core Web Vitals via PageSpeed Insights API."""
    from data.analyzers.cwv import CWVAnalyzer
    analyzer = CWVAnalyzer()
    result = analyzer.analyze(req.url, req.strategy)
    return {
        "url": result.url,
        "grade": result.overall_grade,
        "performance_score": result.performance_score,
        "lcp_ms": result.lcp_ms,
        "cls_score": result.cls_score,
        "fcp_ms": result.fcp_ms,
        "ttfb_ms": result.ttfb_ms,
        "quick_wins": analyzer.get_quick_wins(result),
        "opportunities": result.opportunities,
    }


# ── Content Pipeline ──────────────────────────────────────────────────────────

class PipelineRunRequest(BaseModel):
    business: BusinessContext
    keyword: str
    page_type: str = "service_page"   # service_page | blog_post | location_page
    async_run: bool = True             # True = queue in Celery; False = run inline


class PipelineStatusRequest(BaseModel):
    task_id: str


@app.post("/pipeline/run")
async def pipeline_run(req: PipelineRunRequest):
    """Trigger the full content pipeline: generate → publish → link inject → index → track.

    With async_run=True (default) the pipeline is queued in Celery and returns immediately.
    With async_run=False the generate step runs inline (slower, for testing).
    """
    business_data = req.business.model_dump()

    if req.async_run:
        from taskq.tasks import run_content_pipeline
        task = run_content_pipeline.apply_async(
            args=[business_data, req.keyword, req.page_type],
        )
        return {
            "status": "queued",
            "task_id": task.id,
            "keyword": req.keyword,
            "page_type": req.page_type,
            "message": f"Pipeline queued. Poll /pipeline/status/{task.id} for progress.",
        }
    else:
        # Inline: run generate + publish synchronously (no Celery)
        from execution.handlers.content import ContentHandler
        from execution.startup import get_publisher
        from execution.publisher import ContentPackage

        handler = ContentHandler()
        result = await handler.create_article(
            task_id="inline",
            target=req.keyword,
            action=f"create {req.page_type}",
            business=req.business,
        )
        return {
            "status": result.status.value,
            "output": result.output,
            "keyword": req.keyword,
        }


@app.get("/pipeline/status/{task_id}")
async def pipeline_status(task_id: str):
    """Poll the status of a queued pipeline task."""
    from taskq.celery_app import app as celery_app
    from celery.result import AsyncResult

    result = AsyncResult(task_id, app=celery_app)
    state = result.state
    info: dict = {}

    if state == "SUCCESS":
        info = result.result or {}
    elif state == "FAILURE":
        info = {"error": str(result.info)}
    elif state == "PENDING":
        info = {"message": "Task is waiting to be picked up by a worker."}

    return {"task_id": task_id, "state": state, "info": info}


@app.post("/pipeline/run-batch")
async def pipeline_run_batch(businesses: list[dict], keywords: list[str]):
    """Queue a content pipeline for every business × keyword combination.

    Body example:
      businesses: [{...BusinessContext...}]
      keywords: ["emergency plumber nyc", "drain cleaning manhattan"]
    """
    from taskq.tasks import run_content_pipeline
    queued = []
    for biz in businesses:
        for kw in keywords:
            task = run_content_pipeline.apply_async(args=[biz, kw, "service_page"])
            queued.append({"task_id": task.id, "keyword": kw,
                           "business": biz.get("business_name", "")})

    return {"queued": len(queued), "tasks": queued}


# ── Feedback loop trigger ─────────────────────────────────────────────────────

@app.post("/pipeline/feedback/{business_id}")
async def trigger_feedback_loop(business_id: str):
    """Manually trigger the feedback + learning loop for a business."""
    from taskq.tasks import run_feedback_loop
    task = run_feedback_loop.apply_async(args=[business_id])
    return {"task_id": task.id, "business_id": business_id, "status": "queued"}


# ── Publisher health ──────────────────────────────────────────────────────────

@app.get("/publisher/status")
async def publisher_status():
    """Show which publishing connectors are currently registered."""
    from execution.startup import get_publisher
    pub = get_publisher()
    return {
        "connectors_registered": list(pub.connectors.keys()),
        "count": len(pub.connectors),
    }


# ── Programmatic SEO ─────────────────────────────────────────────────────────

class ProgrammaticRequest(BaseModel):
    business_id: str
    services: list[str] = []
    pages_per_day: int = 10


@app.post("/programmatic/generate")
async def programmatic_generate(req: ProgrammaticRequest):
    """Generate programmatic SEO page matrix (location × service × modifier)."""
    from core.programmatic.generator import ProgrammaticGenerator
    import json
    from pathlib import Path

    biz_file = Path("data/storage/businesses.json")
    business = {}
    if biz_file.exists():
        all_biz = json.loads(biz_file.read_text())
        business = next((b for b in all_biz if b.get("id") == req.business_id), {})

    gen = ProgrammaticGenerator()
    services = req.services or business.get("services", ["service"])
    matrix = gen.generate_matrix(services=services)
    calendar = gen.to_publish_calendar(matrix, pages_per_day=req.pages_per_day)

    return {
        "business_id": req.business_id,
        "matrix_size": len(matrix),
        "calendar_pages": len(calendar),
        "preview": calendar[:5],
    }


@app.post("/programmatic/queue")
async def programmatic_queue(req: ProgrammaticRequest):
    """Queue programmatic page batch for today via Celery."""
    from taskq.tasks import run_programmatic_batch
    task = run_programmatic_batch.apply_async(
        args=[req.business_id, req.pages_per_day],
    )
    return {"task_id": task.id, "status": "queued", "business_id": req.business_id}


# ── Deep health check ─────────────────────────────────────────────────────────

@app.get("/health/deep")
async def deep_health():
    """Full system health check — Claude CLI, Redis, queues, disk, dead-letter, AION services."""
    from monitoring.health import SystemHealthMonitor
    monitor = SystemHealthMonitor()
    report = await monitor.run_checks()
    result = report.to_dict()

    # Append AION infrastructure health
    try:
        from core.aion_bridge import aion
        result["aion_services"] = aion.health()
    except Exception as e:
        result["aion_services"] = {"error": str(e)}

    return result


# ── Indexing management ───────────────────────────────────────────────────────

class IndexUrlRequest(BaseModel):
    url: str


@app.post("/indexing/submit")
async def indexing_submit(req: IndexUrlRequest):
    """Submit a single URL to Google Indexing API + Bing IndexNow + GSC."""
    from execution.indexing import submit_url
    result = await submit_url(req.url)
    return {
        "url": result.url,
        "google_api": result.google_api,
        "google_sitemap": result.google_sitemap_ping,
        "gsc_request": result.gsc_request,
        "bing_indexnow": result.bing_indexnow,
        "any_success": result.any_success,
        "errors": result.errors,
    }


@app.post("/indexing/verify")
async def indexing_verify(req: IndexUrlRequest):
    """Check if URL is indexed in Google via GSC URL Inspection API."""
    from execution.indexing import IndexingSystem
    system = IndexingSystem()
    indexed = await system.verify_indexed(req.url)
    return {"url": req.url, "indexed": indexed}


# ── Wikidata entity pipeline ──────────────────────────────────────────────────

@app.post("/entity/wikidata/build")
async def wikidata_build(business: BusinessContext):
    """Run full Wikidata entity creation pipeline for a business."""
    from authority.wikidata import run_entity_pipeline
    biz_dict = business.model_dump()
    result = await run_entity_pipeline(biz_dict)
    return result


@app.post("/entity/wikidata/quickstatements")
async def wikidata_quickstatements(business: BusinessContext):
    """Generate Wikidata QuickStatements for manual import."""
    from authority.wikidata import WikidataBuilder
    builder = WikidataBuilder()
    entity = builder.build_entity(business.model_dump())
    qs = builder.to_quickstatements(entity)
    return {
        "label": entity.label,
        "description": entity.description,
        "quickstatements": qs,
        "lines": len(qs.splitlines()),
    }


# ── HARO management ───────────────────────────────────────────────────────────

@app.post("/backlinks/haro/trigger")
async def haro_trigger(business_id: str = "default"):
    """Manually trigger HARO check and response sending."""
    from taskq.tasks import run_haro_check
    task = run_haro_check.apply_async(args=[business_id])
    return {"task_id": task.id, "status": "queued", "business_id": business_id}


@app.post("/backlinks/reclamation/trigger")
async def reclamation_trigger(business_id: str = "default"):
    """Manually trigger link reclamation campaign."""
    from taskq.tasks import run_link_reclamation
    task = run_link_reclamation.apply_async(args=[business_id])
    return {"task_id": task.id, "status": "queued", "business_id": business_id}


# ── Content Gate ──────────────────────────────────────────────────────────────

class ContentGateRequest(BaseModel):
    content_html: str
    keyword: str
    intent: str = "informational"
    title: str = ""
    meta_description: str = ""
    humanise_if_needed: bool = False


@app.post("/quality/gate")
async def content_gate(req: ContentGateRequest):
    """Run the full content quality gate (word count, AI score, direct answer, FAQ, headers)."""
    from execution.validators.content_gate import ContentGate
    try:
        from config.settings import ORIGINALITY_API_KEY
    except ImportError:
        ORIGINALITY_API_KEY = ""

    gate = ContentGate(originality_api_key=ORIGINALITY_API_KEY)

    if req.humanise_if_needed:
        result = await gate.check_and_humanise(
            req.content_html, req.keyword,
            intent=req.intent, title=req.title,
            meta_description=req.meta_description,
        )
    else:
        result = await gate.check(
            req.content_html, req.keyword,
            intent=req.intent, title=req.title,
            meta_description=req.meta_description,
        )

    return {
        "passed": result.passed,
        "blocking_failures": result.blocking_failures,
        "warnings": result.warnings,
        "scores": result.scores,
        "humanised": result.humanised_html is not None,
    }


# ── Canonical registry ────────────────────────────────────────────────────────

class CanonicalRequest(BaseModel):
    url: str
    content: str = ""
    business_id: str = "default"


@app.post("/canonical/register")
async def canonical_register(req: CanonicalRequest):
    """Register a URL in the canonical registry and check for duplicates."""
    from execution.canonical import CanonicalRegistry, SimHashDuplicate
    registry = CanonicalRegistry()
    try:
        registry.register(req.url, req.content, req.business_id)
        return {"registered": True, "url": req.url, "duplicate": False}
    except SimHashDuplicate as e:
        return {"registered": False, "url": req.url, "duplicate": True, "original_url": str(e).split(": ")[-1]}


# ── AION bridge endpoints ─────────────────────────────────────────────────────

class ContentBriefRequest(BaseModel):
    keyword: str
    competitor_urls: list[str] = Field(default_factory=list)
    max_competitors: int = Field(default=4, ge=1, le=8)
    include_youtube: bool = True


@app.post("/content/brief")
async def content_brief(req: ContentBriefRequest):
    """Generate a content brief by scraping competitors via AION Firecrawl + Brain.

    Scrapes competitor URLs, extracts headings/word-counts, searches YouTube,
    then uses AION Brain to synthesize recommended H2s, FAQs, and content gaps.
    """
    from core.crawlers.competitor_scraper import CompetitorScraper
    scraper = CompetitorScraper()
    brief = scraper.generate_brief(
        keyword=req.keyword,
        competitor_urls=req.competitor_urls,
        max_competitors=req.max_competitors,
        include_youtube=req.include_youtube,
    )
    return scraper.brief_to_dict(brief)


class ScrapeRequest(BaseModel):
    url: str


@app.post("/content/scrape")
async def scrape_url(req: ScrapeRequest):
    """Scrape a URL to clean markdown via AION Firecrawl (JS-aware)."""
    from core.aion_bridge import aion
    meta = aion.firecrawl_scrape_meta(req.url)
    return meta


class SignalsRequest(BaseModel):
    source: str | None = None
    limit: int = Field(default=20, ge=1, le=100)


@app.get("/content/signals")
async def content_signals(source: str | None = None, limit: int = 20):
    """Get trending signals from AION Research Aggregator (HN, Reddit, news).

    Use to discover trending topics for content calendar.
    """
    from core.aion_bridge import aion
    signals = aion.get_signals(source=source, limit=limit)
    return {"count": len(signals), "signals": signals}


class YouTubeResearchRequest(BaseModel):
    topic: str
    max_videos: int = Field(default=3, ge=1, le=10)


@app.post("/content/youtube-research")
async def youtube_research(req: YouTubeResearchRequest):
    """Search YouTube and return video list + transcripts for a topic.

    Use for FAQ enrichment, topic gap analysis, content differentiation.
    """
    from core.aion_bridge import aion
    results = aion.youtube_research(req.topic, max_videos=req.max_videos)
    return {"topic": req.topic, "count": len(results), "videos": results}



# ── Business Onboarding ──────────────────────────────────────────────────────

class BusinessRegisterRequest(BaseModel):
    name: str
    website: str
    domain: str = ""
    primary_service: str = ""
    secondary_services: list = []
    primary_city: str = ""
    state: str = ""
    service_areas: list = []
    primary_keywords: list = []
    competitors: list = []
    owner_email: str = ""
    contact_name: str = ""
    target_customer: str = ""
    gbp_url: str = ""
    years_active: int = 0
    avg_job_value: float = 0.0
    reviews_count: int = 0
    monthly_traffic: int = 0


@app.post("/businesses")
async def register_business(req: BusinessRegisterRequest):
    """Register a new business for automated SEO. Immediately starts task pipeline."""
    import uuid
    from data.db import get_db

    domain = req.domain or req.website.replace("https://", "").replace("http://", "").rstrip("/")
    business_id = str(uuid.uuid4())

    config = {
        "website": req.website,
        "domain": domain,
        "primary_service": req.primary_service,
        "secondary_services": req.secondary_services,
        "primary_city": req.primary_city,
        "state": req.state,
        "service_areas": req.service_areas,
        "primary_keywords": req.primary_keywords,
        "competitors": req.competitors,
        "owner_email": req.owner_email,
        "contact_name": req.contact_name,
        "target_customer": req.target_customer,
        "gbp_url": req.gbp_url,
        "years_active": req.years_active,
        "avg_job_value": req.avg_job_value,
        "reviews_count": req.reviews_count,
        "monthly_traffic": req.monthly_traffic,
        "status": "active",
    }

    _db = get_db()
    _db.add_business(business_id=business_id, name=req.name, domain=domain, config=config)

    # Kick off initial analysis immediately
    try:
        from taskq.tasks import analyze_business
        analyze_business.delay(business_id, {"name": req.name, **config})
    except Exception:
        pass

    return {
        "business_id": business_id,
        "name": req.name,
        "domain": domain,
        "status": "registered",
        "message": "Business registered. Initial analysis queued.",
    }


@app.get("/businesses")
async def list_businesses():
    """List all registered businesses."""
    from data.db import get_db
    return {"businesses": get_db().get_businesses()}


@app.delete("/businesses/{business_id}")
async def delete_business(business_id: str):
    """Remove a business from the SEO engine."""
    from data.db import get_db
    removed = get_db().remove_business(business_id)
    if removed:
        return {"status": "removed", "business_id": business_id}
    return {"status": "not_found", "business_id": business_id}


@app.get("/businesses/{business_id}/status")
async def business_status(business_id: str):
    """Get task + ranking status for a specific business."""
    import json as _json
    import sqlite3
    conn = sqlite3.connect("data/seo_engine.db")
    conn.row_factory = sqlite3.Row

    biz = conn.execute("SELECT * FROM businesses WHERE id = ?", (business_id,)).fetchone()
    if not biz:
        conn.close()
        return {"error": "not found"}

    config = _json.loads(biz["config_json"] or "{}")
    domain = biz["domain"] or config.get("domain", "")

    try:
        rankings = conn.execute(
            "SELECT keyword, position, checked_at FROM ranking_history WHERE domain = ? ORDER BY checked_at DESC LIMIT 20",
            (domain,)
        ).fetchall()
    except Exception:
        rankings = []

    try:
        published = conn.execute(
            "SELECT url, published_at FROM published_urls WHERE domain = ? ORDER BY published_at DESC LIMIT 10",
            (domain,)
        ).fetchall() if domain else []
    except Exception:
        published = []

    conn.close()

    return {
        "business_id": business_id,
        "name": biz["name"],
        "domain": domain,
        "config": config,
        "rankings": [dict(r) for r in rankings],
        "published_urls": [dict(p) for p in published],
    }


# ---- Run ----

if __name__ == "__main__":
    import uvicorn

# Dashboard & Static Files
import os as _os
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse as _HTMLResponse

_static_dir = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), '..', 'static')
if _os.path.exists(_static_dir):
    app.mount('/static', StaticFiles(directory=_static_dir), name='static')

@app.get('/', response_class=_HTMLResponse, include_in_schema=False)
async def dashboard():
    _p = _os.path.join(_static_dir, 'dashboard.html')
    with open(_p, encoding='utf-8') as _f:
        return _f.read()

@app.get('/signup', response_class=_HTMLResponse, include_in_schema=False)
async def signup_page():
    _p = _os.path.join(_static_dir, 'signup.html')
    with open(_p, encoding='utf-8') as _f:
        return _f.read()
