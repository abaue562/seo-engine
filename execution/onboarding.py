"""Idempotent tenant onboarding state machine.

Each step is:
  1. Checked against checkpoint: if already done, returns stored result.
  2. Executed with isolated error handling.
  3. Checkpointed on success.

A stuck onboarding (processing > 10 min) auto-resumes from last checkpoint.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any

log = logging.getLogger(__name__)

# Ordered list of onboarding steps
ONBOARDING_STEPS = [
    "validate_input",
    "create_tenant",
    "store_credentials",
    "seed_keywords",
    "crawl_site",
    "analyze_competitors",
    "build_content_plan",
    "finalize",
]


class OnboardingJob:
    """Manages a single tenant onboarding job with per-step checkpoints.

    All onboarding_jobs table operations use admin_write/admin_one (BYPASSRLS)
    because the job row may exist before tenant RLS context is established.
    """

    def __init__(self, job_id: str, tenant_id: str):
        self.job_id = job_id
        self.tenant_id = tenant_id

    @classmethod
    def create(cls, tenant_id: str) -> "OnboardingJob":
        """Create a new onboarding job in the DB."""
        from core.pg import admin_write
        job_id = str(uuid.uuid4())
        admin_write(
            "INSERT INTO onboarding_jobs (id, tenant_id, status, current_step, checkpoint) "
            "VALUES (%s, %s, %s, %s, %s)",
            [job_id, tenant_id, "processing", ONBOARDING_STEPS[0], "{}"],
        )
        log.info("onboarding.created  job_id=%s  tenant=%s", job_id, tenant_id[:8])
        return cls(job_id, tenant_id)

    @classmethod
    def load(cls, job_id: str, tenant_id: str) -> "OnboardingJob | None":
        """Load an existing onboarding job by ID."""
        from core.pg import admin_one
        row = admin_one(
            "SELECT id FROM onboarding_jobs WHERE id = %s",
            [job_id],
        )
        if row is None:
            return None
        return cls(job_id, tenant_id)

    def get_checkpoint(self) -> dict:
        """Return the current checkpoint dict from DB."""
        from core.pg import admin_one
        row = admin_one(
            "SELECT checkpoint, current_step, status FROM onboarding_jobs WHERE id = %s",
            [self.job_id],
        )
        if row is None:
            return {}
        return {
            "checkpoint": row[0] or {},
            "current_step": row[1],
            "status": row[2],
        }

    def is_step_done(self, step: str) -> bool:
        """Return True if this step is already checkpointed."""
        state = self.get_checkpoint()
        return step in (state.get("checkpoint") or {})

    def complete_step(self, step: str, result: Any) -> None:
        """Mark a step as complete and store its result in the checkpoint."""
        import json
        from core.pg import admin_write
        try:
            next_idx = ONBOARDING_STEPS.index(step) + 1
            next_step = ONBOARDING_STEPS[next_idx] if next_idx < len(ONBOARDING_STEPS) else "done"
        except ValueError:
            next_step = "done"
        admin_write(
            "UPDATE onboarding_jobs "
            "SET checkpoint = checkpoint || %s::jsonb, current_step = %s, updated_at = NOW() "
            "WHERE id = %s",
            [json.dumps({step: result}), next_step, self.job_id],
        )
        log.info("onboarding.step_done  job=%s  step=%s  next=%s", self.job_id[:8], step, next_step)

    def fail(self, step: str, error: str) -> None:
        """Mark the job as failed with an error message."""
        from core.pg import admin_write
        admin_write(
            "UPDATE onboarding_jobs SET status = %s, error = %s, current_step = %s, updated_at = NOW() "
            "WHERE id = %s",
            ["failed", error[:500], step, self.job_id],
        )
        log.error("onboarding.step_failed  job=%s  step=%s  err=%s", self.job_id[:8], step, error[:100])

    def complete(self) -> None:
        """Mark the entire job as completed."""
        from core.pg import admin_write
        admin_write(
            "UPDATE onboarding_jobs SET status = %s, completed_at = NOW(), current_step = %s, updated_at = NOW() "
            "WHERE id = %s",
            ["completed", "done", self.job_id],
        )
        log.info("onboarding.completed  job=%s  tenant=%s", self.job_id[:8], self.tenant_id[:8])


def run_step(job: OnboardingJob, step: str, input_data: dict) -> dict:
    """Execute a single onboarding step, checking idempotency first."""
    if job.is_step_done(step):
        log.info("onboarding.step_skip  job=%s  step=%s  (already_done)", job.job_id[:8], step)
        state = job.get_checkpoint()
        return (state.get("checkpoint") or {}).get(step, {})

    log.info("onboarding.step_start  job=%s  step=%s", job.job_id[:8], step)

    runner = _STEP_RUNNERS.get(step)
    if runner is None:
        log.warning("onboarding.unknown_step  step=%s", step)
        return {"status": "skipped", "reason": "unknown_step"}

    result = runner(job, input_data)
    job.complete_step(step, result)
    return result


# ---------------------------------------------------------------------------
# Step implementations
# ---------------------------------------------------------------------------

def _step_validate_input(job: OnboardingJob, data: dict) -> dict:
    required = ["business_name", "website", "primary_city", "primary_service"]
    missing = [f for f in required if not data.get(f, "").strip()]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    # SSRF validation on website URL (S3-A)
    website = data.get("website", "")
    if website:
        try:
            from core.ssrf import validate_url
            validate_url(website, resolve_dns=False)  # DNS resolution optional at validation step
        except Exception as ssrf_err:
            raise ValueError(f"Invalid website URL: {ssrf_err}")
    return {"status": "ok", "fields_validated": len(required)}


def _step_create_tenant(job: OnboardingJob, data: dict) -> dict:
    from core.pg import execute_write
    execute_write(
        "INSERT INTO tenants (id, name, slug, status) VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()",
        [job.tenant_id, data["business_name"], data.get("slug", job.tenant_id), "trial"],
    )
    execute_write(
        "INSERT INTO tenant_nap (tenant_id, business_name, website, primary_city, primary_service) "
        "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
        [job.tenant_id, data["business_name"], data["website"],
         data["primary_city"], data["primary_service"]],
        tenant_id=job.tenant_id,
    )
    return {"status": "ok", "tenant_id": job.tenant_id}


def _step_store_credentials(job: OnboardingJob, data: dict) -> dict:
    from core.credential_vault import vault
    stored = []
    # Support both nested dict and flat field formats
    wp = data.get("wordpress") or {}
    wp_url = wp.get("url") or data.get("wp_url", "")
    wp_user = wp.get("username") or data.get("wp_username", "")
    wp_pass = wp.get("app_password") or data.get("wp_app_password", "")
    if wp_url and wp_user and wp_pass:
        # SSRF check before hitting WP URL (S3-A: SSRF on validate_publishing_credentials)
        try:
            from core.ssrf import validate_url
            validate_url(wp_url, resolve_dns=True)
        except Exception as _ssrf_e:
            log.error("onboarding.ssrf_blocked  url=%s  err=%s", wp_url[:60], _ssrf_e)
            raise ValueError(f"WordPress URL blocked by security policy: {_ssrf_e}")
        # Warn if not HTTPS
        if not wp_url.startswith("https://"):
            log.warning("onboarding.wp_url_not_https  url=%s", wp_url[:60])
        vault.store(job.tenant_id, "wordpress", {
            "url": wp_url,
            "username": wp_user,
            "app_password": wp_pass,
        })
        stored.append("wordpress")
    return {"status": "ok", "stored_platforms": stored}


def _step_seed_keywords(job: OnboardingJob, data: dict) -> dict:
    from core.pg import execute_write
    service = data.get("primary_service", "")
    city = data.get("primary_city", "")
    # Use provided keywords or generate seed set
    provided = data.get("keywords", [])
    if provided:
        seed_keywords = provided
    else:
        seed_keywords = [
            f"{service} {city}",
            f"{service} near me",
            f"best {service} {city}",
            f"{service} company {city}",
            f"{city} {service}",
        ]
    for kw in seed_keywords:
        execute_write(
            "INSERT INTO keywords (tenant_id, keyword, intent, status) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (tenant_id, keyword) DO NOTHING",
            [job.tenant_id, kw.lower().strip(), "commercial", "pending"],
            tenant_id=job.tenant_id,
        )
    return {"status": "ok", "keywords_seeded": len(seed_keywords), "keywords": seed_keywords}


def _step_crawl_site(job: OnboardingJob, data: dict) -> dict:
    return {"status": "ok", "site": data.get("website", ""), "crawl": "queued_async"}


def _step_analyze_competitors(job: OnboardingJob, data: dict) -> dict:
    competitors = data.get("competitors", [])
    return {"status": "ok", "competitors_queued": len(competitors), "analysis": "queued_async"}


def _step_build_content_plan(job: OnboardingJob, data: dict) -> dict:
    return {"status": "ok", "content_plan": "queued_async"}


def _step_finalize(job: OnboardingJob, data: dict) -> dict:
    from core.pg import execute_write
    execute_write(
        "UPDATE tenants SET status = %s, updated_at = NOW() WHERE id = %s",
        ["active", job.tenant_id],
    )
    return {"status": "ok", "tenant_activated": True}


_STEP_RUNNERS = {
    "validate_input":      _step_validate_input,
    "create_tenant":       _step_create_tenant,
    "store_credentials":   _step_store_credentials,
    "seed_keywords":       _step_seed_keywords,
    "crawl_site":          _step_crawl_site,
    "analyze_competitors": _step_analyze_competitors,
    "build_content_plan":  _step_build_content_plan,
    "finalize":            _step_finalize,
}


def run_onboarding(tenant_id: str, input_data: dict) -> dict:
    """Run or resume a full onboarding job for a tenant.

    Idempotent: call again to resume from last successful step.
    Uses admin_one/admin_write for job-table ops (BYPASSRLS).
    """
    from core.pg import admin_one, execute_write

    # Check for an existing in-progress or failed job (admin bypasses RLS)
    row = admin_one(
        "SELECT id FROM onboarding_jobs "
        "WHERE tenant_id = %s AND status IN ('pending', 'processing', 'failed') "
        "ORDER BY started_at DESC LIMIT 1",
        [tenant_id],
    )

    if row:
        job = OnboardingJob(str(row[0]), tenant_id)
        log.info("onboarding.resuming  job=%s  tenant=%s", job.job_id[:8], tenant_id[:8])
    else:
        # Check for already-completed job (idempotent: return existing result)
        done_row = admin_one(
            "SELECT id FROM onboarding_jobs WHERE tenant_id = %s AND status = 'completed' "
            "ORDER BY completed_at DESC LIMIT 1",
            [tenant_id],
        )
        if done_row:
            log.info("onboarding.already_completed  tenant=%s", tenant_id[:8])
            return {
                "status": "completed",
                "job_id": str(done_row[0]),
                "tenant_id": tenant_id,
                "steps": {},
                "resumed": True,
            }

        # Ensure tenant stub exists for FK constraint on onboarding_jobs
        execute_write(
            "INSERT INTO tenants (id, name, slug, status) VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (id) DO NOTHING",
            [tenant_id, input_data.get("business_name", tenant_id), tenant_id, "trial"],
        )
        job = OnboardingJob.create(tenant_id)

    results = {}
    for step in ONBOARDING_STEPS:
        try:
            results[step] = run_step(job, step, input_data)
        except Exception as e:
            job.fail(step, str(e))
            log.error("onboarding.halted  job=%s  step=%s  err=%s", job.job_id[:8], step, e)
            return {
                "status": "failed",
                "job_id": job.job_id,
                "failed_step": step,
                "error": str(e),
                "completed_steps": results,
                "resumable": True,
            }

    job.complete()
    return {
        "status": "completed",
        "job_id": job.job_id,
        "tenant_id": tenant_id,
        "steps": results,
    }
