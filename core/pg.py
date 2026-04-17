"""PostgreSQL connection module with tenant RLS middleware.

Every connection automatically sets app.current_tenant GUC before
any queries run, enforcing Row-Level Security at the DB layer.

Usage:
    from core.pg import get_conn, tenant_cursor

    # Transaction with tenant isolation
    with get_conn(tenant_id="abc-123") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM keywords")
            rows = cur.fetchall()

    # Or use the helper context manager
    with tenant_cursor(tenant_id="abc-123") as cur:
        cur.execute("INSERT INTO keywords (tenant_id, keyword) VALUES (%s, %s)",
                    [tenant_id, "my keyword"])
"""
from __future__ import annotations

import contextlib
import logging
import os
from typing import Generator, Optional

log = logging.getLogger(__name__)

# Connection string -- override via DATABASE_URL env var
_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://seo_app:seo_Bbl_2026_secure@localhost:5432/seo_engine"
)

# Module-level connection pool (lazy initialised)
_pool = None
_pool_lock = None


def _get_pool():
    """Return the module-level connection pool, creating it if needed."""
    global _pool, _pool_lock
    if _pool is not None:
        return _pool
    import threading
    if _pool_lock is None:
        _pool_lock = threading.Lock()
    with _pool_lock:
        if _pool is None:
            try:
                from psycopg2 import pool as pg_pool
                _pool = pg_pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=int(os.getenv("PG_POOL_MAX", "10")),
                    dsn=_DATABASE_URL,
                    connect_timeout=5,
                )
                log.info("pg.pool_created  dsn=%s", _DATABASE_URL.split("@")[-1])
            except Exception as e:
                log.error("pg.pool_create_fail  err=%s", e)
                raise
    return _pool


@contextlib.contextmanager
def get_conn(tenant_id: Optional[str] = None) -> Generator:
    """Context manager that yields a psycopg2 connection with tenant GUC set.

    Args:
        tenant_id: UUID string of the tenant.  If None, no GUC is set
                   (use only for platform_admin operations).

    Yields:
        psycopg2 connection.  Auto-commits on clean exit, rolls back on exception.
    """
    pool = _get_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            if tenant_id:
                cur.execute("SELECT set_tenant_context(%s)", [tenant_id])
            else:
                # Explicitly clear context so RLS blocks everything
                cur.execute('RESET app.current_tenant')
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


@contextlib.contextmanager
def tenant_cursor(tenant_id: str) -> Generator:
    """Shortcut context manager that yields a cursor with tenant GUC set.

    Usage:
        with tenant_cursor("abc-123") as cur:
            cur.execute("SELECT * FROM keywords")
    """
    with get_conn(tenant_id=tenant_id) as conn:
        with conn.cursor() as cur:
            yield cur


def execute_one(
    sql: str,
    params: list | tuple | None = None,
    tenant_id: Optional[str] = None,
) -> Optional[tuple]:
    """Execute a query and return the first row, or None."""
    with get_conn(tenant_id=tenant_id) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


def execute_many(
    sql: str,
    params: list | tuple | None = None,
    tenant_id: Optional[str] = None,
) -> list[tuple]:
    """Execute a query and return all rows."""
    with get_conn(tenant_id=tenant_id) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() or []


def execute_write(
    sql: str,
    params: list | tuple | None = None,
    tenant_id: Optional[str] = None,
) -> int:
    """Execute an INSERT/UPDATE/DELETE and return rowcount."""
    with get_conn(tenant_id=tenant_id) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount


def health_check() -> dict:
    """Check database connectivity and return status dict."""
    try:
        row = execute_one("SELECT version(), NOW()")
        return {
            "status": "ok",
            "version": row[0].split(" ")[0] if row else "unknown",
            "server_time": str(row[1]) if row else "unknown",
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# =============================================================================
# platform_admin connection pool (BYPASSRLS for cross-tenant operations)
# =============================================================================
_ADMIN_DATABASE_URL = os.getenv(
    "ADMIN_DATABASE_URL",
    "postgresql://platform_admin:admin_password_change_me@localhost:5432/seo_engine"
)
_admin_pool = None
_admin_pool_lock = None


def _get_admin_pool():
    """Return the module-level admin connection pool, creating it if needed."""
    global _admin_pool, _admin_pool_lock
    if _admin_pool is not None:
        return _admin_pool
    import threading
    if _admin_pool_lock is None:
        _admin_pool_lock = threading.Lock()
    with _admin_pool_lock:
        if _admin_pool is None:
            try:
                from psycopg2 import pool as pg_pool
                _admin_pool = pg_pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=int(os.getenv("PG_ADMIN_POOL_MAX", "5")),
                    dsn=_ADMIN_DATABASE_URL,
                    connect_timeout=5,
                )
                log.info("pg.admin_pool_created")
            except Exception as e:
                log.error("pg.admin_pool_create_fail  err=%s", e)
                raise
    return _admin_pool


@contextlib.contextmanager
def platform_admin_conn():
    """Context manager for cross-tenant operations. Uses BYPASSRLS platform_admin role."""
    pool = _get_admin_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = False
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def admin_write(sql: str, params=None) -> int:
    """Execute INSERT/UPDATE/DELETE as platform_admin (bypasses RLS). Returns rowcount."""
    with platform_admin_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount


def admin_one(sql: str, params=None) -> Optional[tuple]:
    """Execute a query as platform_admin and return the first row, or None."""
    with platform_admin_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


def admin_many(sql: str, params=None) -> list:
    """Execute a query as platform_admin and return all rows."""
    with platform_admin_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() or []
