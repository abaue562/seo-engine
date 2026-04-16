"""Final system audit script — scores the SEO engine across 7 categories."""
import ast, sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

score = {}
issues = []

def audit(category, item, passed, note=""):
    score.setdefault(category, {"pass": 0, "fail": 0, "items": []})
    if passed:
        score[category]["pass"] += 1
    else:
        score[category]["fail"] += 1
        issues.append(f"[{category}] {item}: {note}")
    score[category]["items"].append({"item": item, "pass": passed, "note": note})

def read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()

# ── Category 1: Execution Chain ──────────────────────────────────────────────
cat = "1. Execution Chain"
tasks_src = read("taskq/tasks.py")
for t in ["generate_content", "publish_content", "inject_internal_links",
          "indexnow_and_track", "run_content_pipeline", "run_feedback_loop"]:
    audit(cat, f"task:{t}", f"def {t}(" in tasks_src)
audit(cat, "chain wiring",
      "celery_chain(" in tasks_src and "generate_content.s(" in tasks_src)
audit(cat, "publish receives generate output",
      "publish_content.s(business_data)" in tasks_src)
audit(cat, "inject receives publish output",
      "inject_internal_links.s(business_data)" in tasks_src)

# ── Category 2: Connector Registration ───────────────────────────────────────
cat = "2. Connector Registration"
startup_src = read("execution/startup.py")
server_src = read("api/server.py")
audit(cat, "get_publisher() singleton", "def get_publisher(" in startup_src)
audit(cat, "init_publisher() builds publisher", "def init_publisher(" in startup_src)
audit(cat, "WordPress registered", "WordPressConnector" in startup_src)
audit(cat, "Medium registered", "MediumConnector" in startup_src)
audit(cat, "skip on missing creds (no crash)", "log.warning" in startup_src)
audit(cat, "init_publisher at API startup", "init_publisher" in server_src)
audit(cat, "reset_publisher() for tests", "def reset_publisher(" in startup_src)

# ── Category 3: Bug Fixes ─────────────────────────────────────────────────────
cat = "3. Bug Fixes"
models_src = read("models/task.py")
settings_src = read("config/settings.py")
bm_src = read("monitoring/brand_mentions.py")
rt_src = read("data/connectors/rank_tracker.py")
ch_src = read("execution/handlers/content.py")
router_src = read("execution/router.py")

audit(cat, "ExecutionMode.SHADOW", "SHADOW" in models_src)
bad = [l for l in settings_src.splitlines()
       if ".env.example" in l and not l.strip().startswith("#")]
audit(cat, "settings.py no .env.example load", len(bad) == 0)
audit(cat, "BrandMentionMonitor.check() exists", "async def check(" in bm_src)
audit(cat, "BrandMentionChecker removed from monitor", "BrandMentionChecker" not in bm_src)
audit(cat, "BrandMentionChecker removed from tasks", "BrandMentionChecker" not in tasks_src)
audit(cat, "execute_task passes business_id",
      "router.execute_task(seo_task, business, business_id" in tasks_src)
audit(cat, "tasks.py uses get_summary_by_id()", "get_summary_by_id(" in tasks_src)
audit(cat, "RankTracker.register() added", "async def register(" in rt_src)
audit(cat, "RankTracker.get_summary_by_id() added", "def get_summary_by_id(" in rt_src)
audit(cat, "ContentHandler validates content_html", "content_html" in ch_src)
audit(cat, "router handles ExecutionMode.SHADOW", "ExecutionMode.SHADOW" in router_src)

# ── Category 4: Link Injection ────────────────────────────────────────────────
cat = "4. Link Injection"
li_src = read("execution/link_injector.py")
audit(cat, "LinkInjector class", "class LinkInjector" in li_src)
audit(cat, "fetches existing WP posts", "_fetch_posts" in li_src)
audit(cat, "PATCHes posts via REST", "_patch_post" in li_src)
audit(cat, "relevance scoring", "_relevance_score" in li_src)
audit(cat, "LINK:anchor:path placeholders", "_resolve_placeholders" in li_src)
audit(cat, "link injection wired to task", "injector.inject(" in tasks_src)

# ── Category 5: Config & Data ─────────────────────────────────────────────────
cat = "5. Config & Data"
audit(cat, "WP_URL in settings", "WP_URL" in settings_src)
audit(cat, "WP_APP_PASSWORD in settings", "WP_APP_PASSWORD" in settings_src)
audit(cat, "WP_PUBLISH_STATUS in settings", "WP_PUBLISH_STATUS" in settings_src)
audit(cat, "LINK_INJECT_MAX_POSTS in settings", "LINK_INJECT_MAX_POSTS" in settings_src)
audit(cat, "businesses.json exists", os.path.exists("data/storage/businesses.json"))
if os.path.exists("data/storage/businesses.json"):
    try:
        biz = json.loads(read("data/storage/businesses.json"))
        audit(cat, "businesses.json has entries", len(biz) >= 1)
        if biz:
            req = ["id","business_name","website","primary_service","primary_city","primary_keywords"]
            audit(cat, "business entry has required fields", all(k in biz[0] for k in req))
    except Exception as e:
        audit(cat, "businesses.json valid JSON", False, str(e))
else:
    audit(cat, "businesses.json has entries", False, "file missing")

# ── Category 6: Task Queue ────────────────────────────────────────────────────
cat = "6. Task Queue"
celery_src = read("taskq/celery_app.py")
audit(cat, "4 named queues", all(q in celery_src for q in ["analysis","execution","learning","monitoring"]))
audit(cat, "dead_letter queue", "dead_letter" in celery_src)
audit(cat, "task_failure dead-letter handler", "task_failure" in celery_src)
audit(cat, "Beat: daily analysis", "daily_analysis_cycle" in celery_src)
audit(cat, "Beat: rank check", "check_rankings" in celery_src)
audit(cat, "Beat: content decay", "scan_content_decay" in celery_src)
audit(cat, "Beat: learning cycle", "run_learning" in celery_src)
for t in ["run_content_pipeline","generate_content","publish_content",
          "inject_internal_links","run_feedback_loop"]:
    audit(cat, f"route:{t}", f"taskq.tasks.{t}" in celery_src)

# ── Category 7: API Endpoints ─────────────────────────────────────────────────
cat = "7. API Endpoints"
for ep in ["/pipeline/run", "/pipeline/status", "/pipeline/run-batch",
           "/pipeline/feedback", "/publisher/status", "/analyze",
           "/execute", "/health", "/cwv", "/autonomous", "/ingest"]:
    audit(cat, f"endpoint:{ep}", ep in server_src)

# ── Print results ──────────────────────────────────────────────────────────────
print()
print("=" * 65)
print("  SEO ENGINE -- FINAL SYSTEM AUDIT")
print("=" * 65)
total_pass = 0
total_fail = 0
for cat_name, cat_data in score.items():
    p = cat_data["pass"]
    f = cat_data["fail"]
    total_pass += p
    total_fail += f
    cat_pct = int(100 * p / (p + f)) if (p + f) else 0
    status = "PASS" if f == 0 else f"PARTIAL ({f} failing)"
    print(f"  {cat_name:<35} {p:>2}/{p+f:<2}  ({cat_pct:>3}%)  [{status}]")

print()
total = total_pass + total_fail
overall_pct = int(100 * total_pass / total) if total else 0
grade = "A" if overall_pct >= 95 else "B" if overall_pct >= 85 else "C" if overall_pct >= 70 else "D"
print(f"  OVERALL SCORE : {total_pass}/{total} ({overall_pct}%)  Grade: {grade}")
print()

if issues:
    print("  REMAINING ISSUES:")
    for iss in issues:
        print(f"    - {iss}")
    print()

print("=" * 65)
