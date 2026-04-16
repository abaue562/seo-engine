"""System health monitoring — Claude CLI auth, queue depth, task failures.

Runs as a Celery task every 15 minutes. Sends webhook alerts for critical issues.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class HealthReport:
    timestamp: str
    claude_cli_ok: bool = False
    redis_ok: bool = False
    queue_depth: dict = field(default_factory=dict)
    task_failure_rate_24h: float = 0.0
    disk_usage_pct: float = 0.0
    dead_letter_count: int = 0
    alerts_fired: list[str] = field(default_factory=list)
    overall_status: str = "ok"  # ok | warning | critical

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "claude_cli_ok": self.claude_cli_ok,
            "redis_ok": self.redis_ok,
            "queue_depth": self.queue_depth,
            "task_failure_rate_24h": self.task_failure_rate_24h,
            "disk_usage_pct": self.disk_usage_pct,
            "dead_letter_count": self.dead_letter_count,
            "alerts_fired": self.alerts_fired,
            "overall_status": self.overall_status,
        }


class SystemHealthMonitor:
    """Checks all critical system components and fires alerts as needed."""

    def __init__(self, webhook_url: str = ""):
        try:
            from config.settings import ALERT_WEBHOOK_URL
            self.webhook_url = webhook_url or ALERT_WEBHOOK_URL
        except ImportError:
            self.webhook_url = webhook_url

    async def run_checks(self) -> HealthReport:
        report = HealthReport(timestamp=datetime.now(tz=timezone.utc).isoformat())

        # Run all checks concurrently
        results = await asyncio.gather(
            self._check_claude_cli(),
            self._check_redis(),
            self._get_queue_depths(),
            self._get_failure_rate(),
            self._check_disk(),
            self._count_dead_letter(),
            return_exceptions=True,
        )

        report.claude_cli_ok  = results[0] if not isinstance(results[0], Exception) else False
        report.redis_ok       = results[1] if not isinstance(results[1], Exception) else False
        report.queue_depth    = results[2] if not isinstance(results[2], Exception) else {}
        report.task_failure_rate_24h = results[3] if not isinstance(results[3], Exception) else 0.0
        report.disk_usage_pct = results[4] if not isinstance(results[4], Exception) else 0.0
        report.dead_letter_count = results[5] if not isinstance(results[5], Exception) else 0

        # Fire alerts
        await self._fire_alerts(report)

        # Set overall status
        if any(a.startswith("CRITICAL") for a in report.alerts_fired):
            report.overall_status = "critical"
        elif report.alerts_fired:
            report.overall_status = "warning"
        else:
            report.overall_status = "ok"

        log.info(
            "health.check  status=%s  claude=%s  redis=%s  failures=%.1f%%  disk=%.0f%%  dead_letter=%d",
            report.overall_status,
            report.claude_cli_ok,
            report.redis_ok,
            report.task_failure_rate_24h * 100,
            report.disk_usage_pct,
            report.dead_letter_count,
        )
        return report

    async def _check_claude_cli(self) -> bool:
        """Verify Claude CLI is installed and authenticated."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "claude", "--print", "--model", "haiku", "--max-turns", "1",
                "--message", "Reply with: OK",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=20)
            except asyncio.TimeoutError:
                proc.kill()
                return False
            return proc.returncode == 0 and len(stdout.strip()) > 0
        except FileNotFoundError:
            # Try common paths
            for path in ["/usr/local/bin/claude", "/usr/bin/claude"]:
                if os.path.exists(path):
                    return True
            return False
        except Exception as e:
            log.warning("health.claude_check_fail  err=%s", e)
            return False

    async def _check_redis(self) -> bool:
        """Verify Redis is reachable."""
        try:
            import redis as redis_lib
            from config.settings import REDIS_URL
            r = redis_lib.from_url(REDIS_URL, socket_connect_timeout=5)
            return r.ping()
        except Exception as e:
            log.warning("health.redis_fail  err=%s", e)
            return False

    async def _get_queue_depths(self) -> dict:
        """Get Celery queue depths via Redis LLEN."""
        try:
            import redis as redis_lib
            from config.settings import REDIS_URL
            r = redis_lib.from_url(REDIS_URL, socket_connect_timeout=5)
            queues = ["analysis", "execution", "learning", "monitoring", "dead_letter"]
            depths = {}
            for q in queues:
                try:
                    depths[q] = r.llen(q)
                except Exception:
                    depths[q] = -1
            return depths
        except Exception as e:
            log.warning("health.queue_depth_fail  err=%s", e)
            return {}

    async def _get_failure_rate(self) -> float:
        """Calculate 24h task failure rate from dead-letter count vs total."""
        from datetime import timedelta
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=24)
        cutoff_ts = cutoff.timestamp()

        dead_dir = Path("data/storage/dead_letter")
        task_dir = Path("data/storage/task_results")

        dead_count = 0
        total_count = 0

        for directory, is_dead in [(dead_dir, True), (task_dir, False)]:
            if directory.exists():
                for f in directory.iterdir():
                    if f.suffix == ".json" and f.stat().st_mtime > cutoff_ts:
                        total_count += 1
                        if is_dead:
                            dead_count += 1

        return dead_count / max(total_count, 1)

    async def _check_disk(self) -> float:
        """Return disk usage percentage."""
        total, used, free = shutil.disk_usage("/")
        return used / total * 100

    async def _count_dead_letter(self) -> int:
        """Count files in dead-letter queue."""
        dead_dir = Path("data/storage/dead_letter")
        if not dead_dir.exists():
            return 0
        return len(list(dead_dir.glob("*.json")))

    async def _fire_alerts(self, report: HealthReport):
        """Check thresholds and fire webhook alerts for violations."""
        alerts = []

        if not report.claude_cli_ok:
            alerts.append(("CRITICAL", "Claude CLI auth failed — all AI tasks will fail. Run: claude auth login"))

        if not report.redis_ok:
            alerts.append(("CRITICAL", "Redis is unreachable — Celery cannot process any tasks"))

        for queue_name, depth in report.queue_depth.items():
            if depth > 100:
                alerts.append(("WARNING", f"Queue '{queue_name}' has {depth} backed-up tasks"))

        if report.task_failure_rate_24h > 0.2:
            alerts.append(("WARNING", f"Task failure rate {report.task_failure_rate_24h:.0%} in last 24h (threshold: 20%)"))

        if report.disk_usage_pct > 85:
            alerts.append(("WARNING", f"Disk usage at {report.disk_usage_pct:.0f}% — clean up old files"))

        if report.dead_letter_count > 10:
            alerts.append(("WARNING", f"{report.dead_letter_count} tasks in dead-letter queue — investigate failures"))

        for level, message in alerts:
            report.alerts_fired.append(f"{level}: {message}")
            await self._send_alert(message, level)

    async def _send_alert(self, message: str, level: str):
        """Send alert to webhook."""
        if not self.webhook_url:
            log.warning("health.alert  [%s] %s", level, message)
            return
        try:
            from execution.notify import notify, AlertLevel
            level_map = {
                "CRITICAL": AlertLevel.CRITICAL,
                "WARNING": AlertLevel.WARNING,
                "INFO": AlertLevel.INFO,
            }
            await asyncio.to_thread(
                notify,
                title=f"System Health: {level}",
                message=message,
                level=level_map.get(level, AlertLevel.WARNING),
            )
        except Exception as e:
            log.warning("health.alert_fail  err=%s", e)
