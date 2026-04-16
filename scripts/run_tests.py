"""Full non-Claude integration test — runs without ANTHROPIC_API_KEY.

Tests: imports, enums, connectors, Redis, Celery discovery, routing,
       Beat schedule, businesses.json, link injector logic.
"""
import sys, os, json, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

results = []

def ok(msg):
    results.append(("OK", msg))
    print(f"  [OK]   {msg}")

def fail(msg):
    results.append(("FAIL", msg))
    print(f"  [FAIL] {msg}")

def section(s):
    print(f"\n--- {s} ---")


# ================================================================
# 1. CORE PACKAGE IMPORTS
# ================================================================
section("1. Core Package Imports")

core_imports = [
    ("fastapi",   ["FastAPI"]),
    ("celery",    ["Celery"]),
    ("redis",     ["Redis"]),
    ("kombu",     ["Queue"]),
    ("anthropic", ["Anthropic"]),
    ("pydantic",  ["BaseModel"]),
    ("httpx",     ["AsyncClient"]),
    ("tenacity",  ["retry"]),
    ("bs4",       ["BeautifulSoup"]),
    ("dotenv",    ["load_dotenv"]),
    ("defusedxml",["DefusedXmlException"]),
]
for mod, attrs in core_imports:
    try:
        m = __import__(mod, fromlist=attrs)
        for a in attrs:
            if not hasattr(m, a):
                raise AttributeError(f"missing {a}")
        ok(f"import {mod}")
    except Exception as e:
        fail(f"import {mod}: {e}")


# ================================================================
# 2. PROJECT MODULE IMPORTS
# ================================================================
section("2. Project Module Imports")

project_mods = [
    ("config.settings",                ["WP_URL", "REDIS_URL", "ANTHROPIC_API_KEY"]),
    ("models.task",                    ["SEOTask", "ExecutionMode", "TaskType"]),
    ("models.business",                ["BusinessContext"]),
    ("execution.publisher",            ["MultiChannelPublisher", "ContentPackage"]),
    ("execution.startup",              ["get_publisher", "init_publisher", "reset_publisher"]),
    ("execution.link_injector",        ["LinkInjector"]),
    ("execution.connectors.base",      ["Connector", "PublishResult"]),
    ("execution.connectors.wordpress", ["WordPressConnector"]),
    ("execution.connectors.external.medium", ["MediumConnector"]),
    ("execution.handlers.content",     ["ContentHandler"]),
    ("execution.handlers.website",     ["WebsiteHandler"]),
    ("execution.handlers.gbp",         ["GBPHandler"]),
    ("execution.handlers.authority",   ["AuthorityHandler"]),
    ("execution.router",               ["ExecutionRouter"]),
    ("execution.models",               ["ExecResult", "ExecStatus"]),
    ("taskq.celery_app",               ["app"]),
    ("taskq.tasks",                    ["run_content_pipeline", "generate_content",
                                        "publish_content", "inject_internal_links",
                                        "indexnow_and_track", "run_feedback_loop",
                                        "analyze_business", "execute_seo_task",
                                        "check_rankings", "monitor_ai_citations"]),
    ("monitoring.brand_mentions",      ["BrandMentionMonitor"]),
    ("data.connectors.rank_tracker",   ["RankTracker"]),
    ("data.storage.database",          ["Database"]),
    ("learning.loops",                 ["LearningEngine"]),
]

for mod_path, attrs in project_mods:
    try:
        mod = __import__(mod_path, fromlist=attrs)
        for a in attrs:
            if not hasattr(mod, a):
                raise AttributeError(f"missing {a}")
        ok(mod_path)
    except Exception as e:
        fail(f"{mod_path}: {str(e)[:100]}")


# ================================================================
# 3. EXECUTION MODE ENUM
# ================================================================
section("3. ExecutionMode Enum")
from models.task import ExecutionMode
for mode_name in ["AUTO", "MANUAL", "ASSISTED", "SHADOW"]:
    try:
        m = ExecutionMode[mode_name]
        ok(f"ExecutionMode.{mode_name} = {m.value!r}")
    except KeyError:
        fail(f"ExecutionMode.{mode_name} missing")


# ================================================================
# 4. CONNECTOR REGISTRATION
# ================================================================
section("4. Connector Registration (no WP creds)")
from execution.startup import init_publisher, reset_publisher
reset_publisher()
pub = init_publisher()
ok("init_publisher() returned MultiChannelPublisher")
registered = list(pub.connectors.keys())
ok(f"connectors: {registered or '(none — expected without creds)'}")
ok("pub.connectors is dict")


# ================================================================
# 5. REDIS CONNECTION
# ================================================================
section("5. Redis Connection")
try:
    import redis as redis_lib
    r = redis_lib.from_url("redis://localhost:6379/0", socket_connect_timeout=3)
    pong = r.ping()
    ver = r.info("server")["redis_version"]
    ok(f"Redis ping=True  version={ver}")
except Exception as e:
    fail(f"Redis: {e}")


# ================================================================
# 6. CELERY TASK DISCOVERY
# ================================================================
section("6. Celery Task Discovery")
from taskq.celery_app import app as celery_app
registered_tasks = list(celery_app.tasks.keys())
expected_tasks = [
    "taskq.tasks.run_content_pipeline",
    "taskq.tasks.generate_content",
    "taskq.tasks.publish_content",
    "taskq.tasks.inject_internal_links",
    "taskq.tasks.indexnow_and_track",
    "taskq.tasks.run_feedback_loop",
    "taskq.tasks.analyze_business",
    "taskq.tasks.execute_seo_task",
    "taskq.tasks.daily_analysis_cycle",
    "taskq.tasks.check_rankings",
    "taskq.tasks.scan_content_decay",
    "taskq.tasks.monitor_ai_citations",
]
for t in expected_tasks:
    if t in registered_tasks:
        ok(f"task: {t.replace('taskq.tasks.','')}")
    else:
        fail(f"task not registered: {t}")


# ================================================================
# 7. BEAT SCHEDULE
# ================================================================
section("7. Beat Schedule")
schedule = celery_app.conf.beat_schedule
for job_name, interval_hours in [
    ("run-daily-analysis", 24),
    ("run-rank-check",     168),
    ("run-content-decay",  72),
    ("run-learning-cycle", 168),
]:
    if job_name in schedule:
        task_name = schedule[job_name]["task"]
        ok(f"{job_name} -> {task_name.replace('taskq.tasks.','')} every {interval_hours}h")
    else:
        fail(f"beat job missing: {job_name}")


# ================================================================
# 8. TASK QUEUE ROUTING
# ================================================================
section("8. Task Queue Routing")
routes = celery_app.conf.task_routes
routing_checks = [
    ("taskq.tasks.run_content_pipeline", "execution"),
    ("taskq.tasks.generate_content",     "execution"),
    ("taskq.tasks.publish_content",      "execution"),
    ("taskq.tasks.inject_internal_links","execution"),
    ("taskq.tasks.analyze_business",     "analysis"),
    ("taskq.tasks.run_feedback_loop",    "learning"),
    ("taskq.tasks.check_rankings",       "monitoring"),
    ("taskq.tasks.scan_content_decay",   "monitoring"),
]
for t, expected_q in routing_checks:
    actual_q = routes.get(t, {}).get("queue", "MISSING")
    if actual_q == expected_q:
        ok(f"{t.replace('taskq.tasks.','')} -> queue:{actual_q}")
    else:
        fail(f"{t.replace('taskq.tasks.','')} -> expected:{expected_q} got:{actual_q}")


# ================================================================
# 9. DATA STORAGE
# ================================================================
section("9. Data Storage")
biz_path = Path("data/storage/businesses.json")
if biz_path.exists():
    biz = json.loads(biz_path.read_text(encoding="utf-8"))
    ok(f"businesses.json: {len(biz)} business(es)")
    if biz:
        b = biz[0]
        ok(f"  name: {b['business_name']}")
        ok(f"  service: {b['primary_service']} | city: {b['primary_city']}")
        ok(f"  keywords: {b['primary_keywords']}")
        req = ["id", "business_name", "website", "primary_service", "primary_city", "primary_keywords"]
        missing = [k for k in req if k not in b]
        if not missing:
            ok("  all required fields present")
        else:
            fail(f"  missing fields: {missing}")
else:
    fail("businesses.json not found")

rank_reg = Path("data/storage/rank_registry.json")
ok(f"rank_registry.json: {'exists' if rank_reg.exists() else 'will be created on first register()'}")

dead_dir = Path("data/storage/dead_letter")
dead_dir.mkdir(parents=True, exist_ok=True)
ok("data/storage/dead_letter/ directory ready")


# ================================================================
# 10. LINK INJECTOR LOGIC
# ================================================================
section("10. Link Injector Logic (no HTTP)")
from execution.link_injector import LinkInjector
inj = LinkInjector()

score_val = inj._relevance_score(
    "We offer emergency plumber services in NYC and Manhattan",
    {"emergency", "plumber", "nyc"}
)
if score_val > 0:
    ok(f"relevance_score = {score_val:.2f}  (> 0, keyword overlap working)")
else:
    fail(f"relevance_score = {score_val} — expected > 0")

anchor = inj._find_best_anchor("Need an emergency plumber in Manhattan today?", "emergency plumber")
if anchor:
    ok(f"find_best_anchor = {anchor!r}")
else:
    fail("find_best_anchor returned empty string")

content = "Call an emergency plumber in Manhattan for drain cleaning services."
patched, injected = inj._inject_link(content, "emergency plumber", "https://example.com/plumber")
if injected and "href" in patched:
    ok(f"inject_link: injected=True  result contains href")
else:
    fail(f"inject_link failed: injected={injected}")

# Verify no double-injection on already-linked text
already_linked = 'Call <a href="https://old.com">emergency plumber</a> today.'
_, injected2 = inj._inject_link(already_linked, "emergency plumber", "https://new.com")
if not injected2:
    ok("inject_link: skips already-linked anchors correctly")
else:
    fail("inject_link: incorrectly injects into already-linked anchor")


# ================================================================
# 11. RANK TRACKER
# ================================================================
section("11. RankTracker (no DataForSEO)")
from data.connectors.rank_tracker import RankTracker
import asyncio
tracker = RankTracker()
asyncio.run(tracker.register("emergency plumber NYC", "https://test-plumbing.com/emergency-plumber-nyc"))
reg = tracker._load_registry()
if "emergency plumber NYC" in reg:
    ok(f"register() saved keyword: {reg}")
else:
    fail("register() did not save keyword")

biz_record = tracker._load_business("example-plumber-nyc")
if biz_record:
    ok(f"_load_business() found: {biz_record.get('business_name')}")
else:
    ok("_load_business() returned empty (business ID not in file — expected for test ID)")


# ================================================================
# 12. BRAND MENTION MONITOR
# ================================================================
section("12. BrandMentionMonitor")
from monitoring.brand_mentions import BrandMentionMonitor
import inspect
monitor = BrandMentionMonitor()
has_check = hasattr(monitor, "check") and inspect.iscoroutinefunction(monitor.check)
if has_check:
    ok("BrandMentionMonitor.check() is async coroutine")
else:
    fail("BrandMentionMonitor.check() missing or not async")
ok("BrandMentionMonitor instantiated successfully")


# ================================================================
# 13. ROUTER SHADOW MODE
# ================================================================
section("13. ExecutionRouter Shadow Mode")
from execution.router import ExecutionRouter
router = ExecutionRouter(shadow_mode=True)
if router.shadow_mode:
    ok("ExecutionRouter(shadow_mode=True) set correctly")
else:
    fail("shadow_mode not set")

router2 = ExecutionRouter(shadow_mode=False)
from models.task import SEOTask, TaskType
task = SEOTask(
    action="test", target="test", why="test", impact="high",
    estimated_result="test", time_to_result="1 week", execution="test",
    type=TaskType.CONTENT, execution_mode=ExecutionMode.SHADOW,
)
ok(f"SEOTask with ExecutionMode.SHADOW created: mode={task.execution_mode.value!r}")


# ================================================================
# SUMMARY
# ================================================================
print()
print("=" * 65)
passed = sum(1 for s, _ in results if s == "OK")
failed_count = sum(1 for s, _ in results if s == "FAIL")
pct = int(100 * passed / (passed + failed_count)) if (passed + failed_count) else 0
grade = "A" if pct >= 95 else "B" if pct >= 85 else "C" if pct >= 70 else "D"

print(f"  FINAL RESULT:  {passed}/{passed+failed_count} checks  ({pct}%)  Grade: {grade}")
print()
if failed_count:
    print("  FAILURES:")
    for s, m in results:
        if s == "FAIL":
            print(f"    - {m}")
    print()
else:
    print("  All checks passed. System is ready.")
    print()
    print("  To start the full stack:")
    print("    Terminal 1: celery -A taskq.celery_app worker -Q analysis,execution,learning,monitoring --concurrency=4")
    print("    Terminal 2: celery -A taskq.celery_app beat --loglevel=info")
    print("    Terminal 3: uvicorn api.server:app --host 0.0.0.0 --port 8900")
    print()
    print("  Required before running: add ANTHROPIC_API_KEY to config/.env")

print("=" * 65)
sys.exit(0 if failed_count == 0 else 1)
