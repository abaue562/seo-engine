#!/usr/bin/env python3
"""
Appends GEO/AEO Celery tasks to taskq/tasks.py and beat schedule entries to celery_app.py.
Run from /opt/seo-engine.
"""
import os
import sys

GEO_TASKS = '''

# GEO/AEO tasks

@app.task(bind=True, queue="monitoring", max_retries=1, name="taskq.tasks.run_ai_answer_monitor")
def run_ai_answer_monitor(self, business_id: str = "") -> dict:
    log.info("run_ai_answer_monitor.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from core.ai_answer_monitor import run_keyword_monitor
        import json
        from pathlib import Path
        if business_id:
            result = run_keyword_monitor(business_id)
        else:
            all_biz = json.loads(Path("data/storage/businesses.json").read_text())
            biz_list = all_biz if isinstance(all_biz, list) else list(all_biz.values())
            total_gaps = 0
            total_wins = 0
            for biz in biz_list:
                bid = biz.get("id") or biz.get("business_id")
                if bid:
                    r = run_keyword_monitor(bid, max_keywords=10)
                    total_gaps += r.get("citation_gaps", 0)
                    total_wins += r.get("you_cited", 0)
            result = {"status": "success", "total_gaps": total_gaps, "total_wins": total_wins}
        result["task_id"] = self.request.id
        _save_result(self.request.id, result)
        log.info("run_ai_answer_monitor.done  gaps=%s  wins=%s", result.get("citation_gaps", result.get("total_gaps")), result.get("you_cited", result.get("total_wins")))
        return result
    except Exception as exc:
        log.exception("run_ai_answer_monitor.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="execution", max_retries=1, name="taskq.tasks.run_geo_optimization_sweep")
def run_geo_optimization_sweep(self, business_id: str = "") -> dict:
    log.info("run_geo_optimization_sweep.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from core.geo_optimizer import score_geo_readiness
        import sqlite3, requests as req_lib
        conn = sqlite3.connect("data/storage/seo_engine.db")
        urls = [(r[0], r[1], r[2]) for r in conn.execute(
            "SELECT url, keyword, business_id FROM published_urls WHERE business_id=? AND status=\'live\' LIMIT 20",
            [business_id]).fetchall()]
        conn.close()
        scores = []
        failing = 0
        for url, keyword, bid in urls:
            try:
                html = req_lib.get(url, timeout=10).text
                score = score_geo_readiness(html)
                scores.append({"url": url, "keyword": keyword, "score": score["score"], "passing": score["passing"]})
                if not score["passing"]:
                    failing += 1
            except Exception:
                pass
        avg_score = round(sum(s["score"] for s in scores) / max(len(scores), 1), 1)
        result = {"status": "success", "pages_checked": len(scores), "avg_geo_score": avg_score, "failing_geo": failing, "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_geo_optimization_sweep.done  pages=%d  avg_score=%.1f  failing=%d", len(scores), avg_score, failing)
        return result
    except Exception as exc:
        log.exception("run_geo_optimization_sweep.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}


@app.task(bind=True, queue="execution", max_retries=1, name="taskq.tasks.run_llms_txt_deploy")
def run_llms_txt_deploy(self, business_id: str = "") -> dict:
    log.info("run_llms_txt_deploy.start  task_id=%s  biz=%s", self.request.id, business_id)
    try:
        from core.llms_txt_builder import deploy_llms_txt, build_llms_txt
        from core.geo_prompts import register_geo_prompts
        register_geo_prompts()
        if business_id:
            content = build_llms_txt(business_id)
            ok = deploy_llms_txt(business_id, output_path=f"public/{business_id}_llms.txt")
            result = {"status": "success" if ok else "partial", "business_id": business_id, "content_length": len(content), "task_id": self.request.id}
        else:
            from core.llms_txt_builder import generate_platform_llms_txt
            content = generate_platform_llms_txt()
            with open("public/llms.txt", "w") as f:
                f.write(content)
            result = {"status": "success", "type": "platform", "content_length": len(content), "task_id": self.request.id}
        _save_result(self.request.id, result)
        log.info("run_llms_txt_deploy.done  length=%d", result.get("content_length", 0))
        return result
    except Exception as exc:
        log.exception("run_llms_txt_deploy.error  task_id=%s", self.request.id)
        return {"status": "error", "error": str(exc), "task_id": self.request.id}
'''

BEAT_ENTRIES = '''    "ai-answer-monitor-weekly": {
        "task": "taskq.tasks.run_ai_answer_monitor",
        "schedule": crontab(hour=8, minute=0, day_of_week=1),
        "kwargs": {"business_id": ""},
    },
    "geo-optimization-sweep-weekly": {
        "task": "taskq.tasks.run_geo_optimization_sweep",
        "schedule": crontab(hour=9, minute=0, day_of_week=1),
        "kwargs": {"business_id": ""},
    },
    "llms-txt-deploy-weekly": {
        "task": "taskq.tasks.run_llms_txt_deploy",
        "schedule": crontab(hour=10, minute=0, day_of_week=1),
        "kwargs": {"business_id": ""},
    },'''

def append_tasks():
    tasks_path = "taskq/tasks.py"
    with open(tasks_path, "r") as f:
        content = f.read()
    if "run_ai_answer_monitor" in content:
        print("GEO tasks already present in tasks.py — skipping")
        return False
    with open(tasks_path, "a") as f:
        f.write(GEO_TASKS)
    print(f"Appended GEO tasks to {tasks_path}")
    return True

def update_beat_schedule():
    celery_path = "celery_app.py"
    with open(celery_path, "r") as f:
        content = f.read()
    if "ai-answer-monitor-weekly" in content:
        print("Beat schedule entries already present — skipping")
        return False
    # Find the closing brace of beat_schedule dict
    # Look for the pattern: last entry before the closing }
    import re
    # Find beat_schedule = { ... }
    match = re.search(r'(beat_schedule\s*=\s*\{)(.*?)(\})', content, re.S)
    if not match:
        print("ERROR: Could not find beat_schedule in celery_app.py")
        return False
    # Insert new entries before the closing brace
    new_block = match.group(1) + match.group(2) + BEAT_ENTRIES + '\n' + match.group(3)
    new_content = content[:match.start()] + new_block + content[match.end():]
    with open(celery_path, "w") as f:
        f.write(new_content)
    print(f"Updated beat schedule in {celery_path}")
    return True

if __name__ == "__main__":
    os.chdir("/opt/seo-engine")
    t = append_tasks()
    b = update_beat_schedule()
    print(f"Done: tasks={'added' if t else 'skipped'}, beat={'updated' if b else 'skipped'}")
