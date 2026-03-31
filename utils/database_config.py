"""Database URL and engine helpers for OpenAlgo.

This module centralizes the runtime database selection logic so the app can:
- Prefer PostgreSQL profiles from .env
- Keep backward compatibility with explicit DATABASE_URL overrides
- Enforce PostgreSQL-only runtime connections
"""

from __future__ import annotations

import logging
import os
import threading
from urllib.parse import quote_plus

import psycopg2
from psycopg2 import errors as pg_errors
from psycopg2 import sql as pg_sql
from sqlalchemy import create_engine, event
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

from utils.timezone import configure_process_timezone

DEFAULT_ENV_VALUE = "local"
PG_DRIVER = "psycopg2"
logger = logging.getLogger(__name__)
configure_process_timezone()
POOL_CONFIG = {
    "pool_size": 10,
    "max_overflow": 20,
    "pool_timeout": 30,
    "pool_recycle": 1800,
    "pool_pre_ping": True,
}
SMALL_POOL_CONFIG = {
    "pool_size": 2,
    "max_overflow": 3,
    "pool_timeout": 5,
    "pool_recycle": 1800,
    "pool_pre_ping": True,
}
SANDBOX_POOL_CONFIG = {
    "pool_size": 3,
    "max_overflow": 5,
    "pool_timeout": 10,
    "pool_recycle": 1800,
    "pool_pre_ping": True,
}

_ENGINE_CACHE: dict[tuple[str, tuple[tuple[str, object], ...]], object] = {}
_ENGINE_CACHE_LOCK = threading.Lock()


def _cap_pool_config(pool_config: dict[str, object]) -> dict[str, object]:
    """Cap aggressive pool settings in production to avoid exhausting remote DBs."""
    capped = dict(pool_config)

    if get_runtime_environment() == "production":
        pool_size_cap = int(os.getenv("DB_POOL_SIZE_CAP", "10"))
        overflow_cap = int(os.getenv("DB_MAX_OVERFLOW_CAP", "5"))
        timeout_floor = int(os.getenv("DB_POOL_TIMEOUT_FLOOR", "5"))

        capped["pool_size"] = min(int(capped.get("pool_size", POOL_CONFIG["pool_size"])), pool_size_cap)
        capped["max_overflow"] = min(
            int(capped.get("max_overflow", POOL_CONFIG["max_overflow"])), overflow_cap
        )
        capped["pool_timeout"] = max(int(capped.get("pool_timeout", POOL_CONFIG["pool_timeout"])), timeout_floor)

    return capped


def get_runtime_environment() -> str:
    """Return the active environment name used for DB profile selection."""
    env = (os.getenv("ENV") or os.getenv("FLASK_ENV") or DEFAULT_ENV_VALUE).strip().lower()
    if env in {"prod", "production"}:
        return "production"
    return "local"


def _profile_prefix() -> str:
    return "PROD" if get_runtime_environment() == "production" else "LOCAL"


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _build_postgres_url(profile_prefix: str, db_prefix: str) -> str | None:
    """Build a PostgreSQL URL from env vars for the selected profile."""
    explicit_url = _first_env(f"{profile_prefix}_{db_prefix}_URL")
    if explicit_url:
        return explicit_url

    host = _first_env(
        f"{profile_prefix}_{db_prefix}_HOST",
        f"{profile_prefix}_DB_HOST",
    )
    port = _first_env(
        f"{profile_prefix}_{db_prefix}_PORT",
        f"{profile_prefix}_DB_PORT",
    )
    database = _first_env(
        f"{profile_prefix}_{db_prefix}_NAME",
        f"{profile_prefix}_DB_NAME",
    )
    username = _first_env(
        f"{profile_prefix}_{db_prefix}_USER",
        f"{profile_prefix}_DB_USER",
    )
    password = _first_env(
        f"{profile_prefix}_{db_prefix}_PASSWORD",
        f"{profile_prefix}_DB_PASSWORD",
    )

    if not all([host, port, database, username, password]):
        return None

    driver = _first_env(
        f"{profile_prefix}_{db_prefix}_DRIVER",
        f"{profile_prefix}_DB_DRIVER",
    )
    if not driver:
        driver = PG_DRIVER

    sslmode = _first_env(
        f"{profile_prefix}_{db_prefix}_SSLMODE",
        f"{profile_prefix}_DB_SSLMODE",
    )
    connect_timeout = _first_env(
        f"{profile_prefix}_{db_prefix}_CONNECT_TIMEOUT",
        f"{profile_prefix}_DB_CONNECT_TIMEOUT",
    )

    url = f"postgresql+{driver}://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/{database}"

    params: list[str] = []
    if sslmode:
        params.append(f"sslmode={quote_plus(sslmode)}")
    if connect_timeout:
        params.append(f"connect_timeout={quote_plus(connect_timeout)}")
    if params:
        url = f"{url}?{'&'.join(params)}"

    return url


def resolve_database_url(
    env_var_name: str,
    *,
    default_prefix: str = "DB",
    fallback_url: str | None = None,
) -> str:
    """Resolve a database URL from env vars.

    Priority:
    1. Explicit env var like DATABASE_URL or LOGS_DATABASE_URL
    2. Profile-specific full URL like LOCAL_DB_URL / PROD_DB_URL
    3. Profile-specific connection parts
    4. Fallback URL, if provided
    """
    explicit = os.getenv(env_var_name)
    if explicit:
        return explicit

    profile_prefix = _profile_prefix()

    # Try a dedicated profile URL first.
    resolved = _build_postgres_url(profile_prefix, default_prefix)
    if resolved:
        return resolved

    # If a dedicated DB profile is not present, fall back to the main DB profile.
    if default_prefix != "DB":
        resolved = _build_postgres_url(profile_prefix, "DB")
        if resolved:
            return resolved

    if fallback_url is not None:
        return fallback_url

    return explicit or ""


def create_engine_from_env(
    env_var_name: str,
    *,
    default_prefix: str = "DB",
    fallback_url: str | None = None,
    echo: bool = False,
    pool_size: int = 50,
    max_overflow: int = 100,
    pool_timeout: int = 10,
    pool_recycle: int = 3600,
    pool_config: dict[str, object] | None = None,
):
    """Create a SQLAlchemy engine using profile-aware database selection."""
    database_url = resolve_database_url(
        env_var_name,
        default_prefix=default_prefix,
        fallback_url=fallback_url,
    )

    if not database_url:
        raise RuntimeError(
            f"Could not resolve a database URL for {env_var_name}. "
            f"Set {env_var_name} directly or provide {get_runtime_environment().upper()}_{default_prefix}_* profile values."
        )

    if not database_url.startswith("postgresql"):
        raise RuntimeError(
            f"Runtime database access must use PostgreSQL. "
            f"Set {env_var_name} or the {get_runtime_environment().upper()} profile to PostgreSQL."
        )

    effective_pool_config = pool_config
    if effective_pool_config is None:
        if env_var_name in {"LOGS_DATABASE_URL", "LATENCY_DATABASE_URL", "HEALTH_DATABASE_URL"} or default_prefix in {
            "LOGS_DB",
            "LATENCY_DB",
            "HEALTH_DB",
        }:
            effective_pool_config = SMALL_POOL_CONFIG
        else:
            effective_pool_config = POOL_CONFIG

    effective_pool_config = _cap_pool_config(dict(effective_pool_config))

    return get_engine(database_url, echo=echo, pool_config=effective_pool_config)


def get_engine(
    database_url: str,
    *,
    echo: bool = False,
    pool_config: dict[str, object] | None = None,
):
    """Return a shared SQLAlchemy engine for the given PostgreSQL URL."""
    if not database_url:
        raise RuntimeError("Cannot create a database engine from an empty URL.")

    effective_pool_config = dict(POOL_CONFIG)
    if pool_config:
        effective_pool_config.update(pool_config)

    effective_pool_config = _cap_pool_config(effective_pool_config)

    cache_key = (database_url, tuple(sorted(effective_pool_config.items())))

    cached = _ENGINE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    with _ENGINE_CACHE_LOCK:
        cached = _ENGINE_CACHE.get(cache_key)
        if cached is not None:
            return cached

        engine = create_engine(
            database_url,
            echo=echo,
            **effective_pool_config,
        )

        def _set_ist_timezone(dbapi_connection, connection_record) -> None:  # noqa: ARG001
            try:
                cursor = dbapi_connection.cursor()
                cursor.execute("SET TIME ZONE 'Asia/Kolkata'")
                cursor.close()
            except Exception:
                logger.debug("Could not set PostgreSQL session timezone to Asia/Kolkata")

        event.listen(engine, "connect", _set_ist_timezone)
        _ENGINE_CACHE[cache_key] = engine
        return engine


def get_resolved_database_urls() -> dict[str, str]:
    """Return the resolved runtime URLs for all PostgreSQL-backed databases."""
    main_url = resolve_database_url("DATABASE_URL", default_prefix="DB")
    return {
        "DATABASE_URL": main_url,
        "LATENCY_DATABASE_URL": resolve_database_url(
            "LATENCY_DATABASE_URL", default_prefix="LATENCY_DB", fallback_url=main_url
        ),
        "LOGS_DATABASE_URL": resolve_database_url(
            "LOGS_DATABASE_URL", default_prefix="LOGS_DB", fallback_url=main_url
        ),
        "SANDBOX_DATABASE_URL": resolve_database_url(
            "SANDBOX_DATABASE_URL", default_prefix="SANDBOX_DB", fallback_url=main_url
        ),
        "HEALTH_DATABASE_URL": resolve_database_url(
            "HEALTH_DATABASE_URL", default_prefix="HEALTH_DB", fallback_url=main_url
        ),
    }


def _resolve_database_admin_credentials(database_url: str) -> dict[str, object]:
    """Resolve admin credentials for database bootstrap if explicitly configured.

    Falls back to the application credentials embedded in the target URL if no
    dedicated admin credentials are present. This keeps local bootstrap working
    while allowing production deployments to supply a true admin account.
    """
    parsed = make_url(database_url)
    return {
        "host": _first_env("DATABASE_ADMIN_HOST", "PG_ADMIN_HOST") or parsed.host,
        "port": int(_first_env("DATABASE_ADMIN_PORT", "PG_ADMIN_PORT") or parsed.port or 5432),
        "user": _first_env("DATABASE_ADMIN_USER", "PG_ADMIN_USER") or parsed.username,
        "password": _first_env("DATABASE_ADMIN_PASSWORD", "PG_ADMIN_PASSWORD") or parsed.password,
    }


def ensure_postgres_database_exists(database_url: str) -> bool:
    """Create the target PostgreSQL database if it does not already exist."""
    if not database_url:
        return False

    parsed = make_url(database_url)
    if parsed.get_backend_name() != "postgresql":
        return False

    database_name = parsed.database
    if not database_name:
        return False

    host = parsed.host
    port = parsed.port or 5432
    username = parsed.username
    password = parsed.password

    if not all([host, username, password]):
        raise RuntimeError(
            f"Cannot auto-create PostgreSQL database '{database_name}' because host/user/password are incomplete."
        )

    admin_creds = _resolve_database_admin_credentials(database_url)
    admin_host = admin_creds["host"] or host
    admin_port = int(admin_creds["port"] or port)
    admin_user = admin_creds["user"] or username
    admin_password = admin_creds["password"] or password

    try:
        conn = psycopg2.connect(
            host=admin_host,
            port=admin_port,
            user=admin_user,
            password=admin_password,
            dbname="postgres",
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (database_name,),
            )
            exists = cur.fetchone() is not None
            if exists:
                return False

            create_sql = pg_sql.SQL("CREATE DATABASE {} OWNER {}").format(
                pg_sql.Identifier(database_name),
                pg_sql.Identifier(username),
            )
            cur.execute(create_sql)
            return True
    except Exception as exc:
        pgcode = getattr(exc, "pgcode", None)
        if isinstance(exc, pg_errors.InsufficientPrivilege) or pgcode == "42501":
            raise RuntimeError(
                f"Unable to auto-create PostgreSQL database '{database_name}' at {host}:{port} "
                f"because user '{admin_user}' does not have CREATEDB privilege. "
                f"Create the database manually or supply DATABASE_ADMIN_* / PG_ADMIN_* credentials."
            ) from exc
        raise RuntimeError(
            f"Unable to auto-create PostgreSQL database '{database_name}' at {host}:{port} "
            f"for user '{admin_user}'. Create it manually or grant CREATEDB privileges."
        ) from exc
    finally:
        try:
            conn.close()  # type: ignore[name-defined]
        except Exception:
            pass


def ensure_postgres_databases_exist(database_urls: dict[str, str]) -> list[str]:
    """Ensure all resolved PostgreSQL databases exist, creating missing local DBs when possible."""
    created: list[str] = []
    seen: set[tuple[str | None, int | None, str | None, str | None]] = set()

    for database_url in database_urls.values():
        if not database_url:
            continue

        parsed = make_url(database_url)
        if parsed.get_backend_name() != "postgresql":
            continue

        identity = (parsed.host, parsed.port, parsed.username, parsed.database)
        if identity in seen:
            continue
        seen.add(identity)

        if ensure_postgres_database_exists(database_url):
            created.append(parsed.database or "")

    return [name for name in created if name]


def ensure_runtime_postgres_databases_exist() -> list[str]:
    """Create missing runtime PostgreSQL databases for local development if needed."""
    if get_runtime_environment() != "local":
        return []

    return ensure_postgres_databases_exist(get_resolved_database_urls())


def configure_database_environment() -> None:
    """Resolve and export database URLs for the current process."""
    resolved_urls = get_resolved_database_urls()
    for env_var, resolved in resolved_urls.items():
        if resolved:
            os.environ[env_var] = resolved


def get_pool_status() -> dict[str, dict[str, object]]:
    """Return live SQLAlchemy pool stats for all resolved runtime engines."""
    resolved_urls = get_resolved_database_urls()
    status: dict[str, dict[str, object]] = {}
    warn_threshold = POOL_CONFIG["pool_size"] + int(POOL_CONFIG["max_overflow"] * 0.8)

    for env_var, database_url in resolved_urls.items():
        if not database_url or not database_url.startswith("postgresql"):
            continue

        engine = get_engine(database_url)
        pool = engine.pool

        checked_out = int(pool.checkedout()) if hasattr(pool, "checkedout") else 0
        checked_in = int(pool.checkedin()) if hasattr(pool, "checkedin") else 0
        overflow = int(pool.overflow()) if hasattr(pool, "overflow") else 0
        size = int(pool.size()) if hasattr(pool, "size") else POOL_CONFIG["pool_size"]
        max_overflow = int(getattr(pool, "_max_overflow", POOL_CONFIG["max_overflow"]))
        warn_threshold = size + max(1, int(max_overflow * 0.8))

        url_info = make_url(database_url)
        status[env_var] = {
            "database": url_info.database or "",
            "host": url_info.host or "",
            "port": int(url_info.port or 0),
            "user": url_info.username or "",
            "pool_size": size,
            "checked_out": checked_out,
            "checked_in": checked_in,
            "overflow": overflow,
            "max_overflow": max_overflow,
            "warn_threshold": warn_threshold,
            "warning": checked_out > warn_threshold,
        }

    return status


def log_pool_status() -> dict[str, dict[str, object]]:
    """Log live pool status for all engines and return the snapshot."""
    status = get_pool_status()
    for env_var, pool_info in status.items():
        message = (
            f"{env_var} pool - db={pool_info['database']} "
            f"size={pool_info['pool_size']} checked_out={pool_info['checked_out']} "
            f"checked_in={pool_info['checked_in']} overflow={pool_info['overflow']}"
        )
        if pool_info["warning"]:
            logger.warning(
                message
                + f" WARNING: checked_out exceeds {pool_info['warn_threshold']} connections"
            )
        else:
            logger.info(message)
    return status


def bulk_insert_mappings_chunked(
    engine,
    model,
    records,
    *,
    chunk_size: int = 1000,
    logger: logging.Logger | None = None,
    label: str = "bulk insert",
    progress_every: int = 10,
) -> int:
    """Insert mappings in small transactions so each chunk releases its connection."""
    if not records:
        if logger:
            logger.info("No new records to insert.")
        return 0

    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    total = len(records)
    inserted = 0
    effective_chunk_size = chunk_size
    if label.lower().startswith("master contract"):
        env_chunk_size = os.getenv("MASTER_CONTRACT_INSERT_CHUNK_SIZE")
        if env_chunk_size:
            effective_chunk_size = max(1, int(env_chunk_size))
        else:
            effective_chunk_size = max(chunk_size, int(os.getenv("MASTER_CONTRACT_INSERT_CHUNK_SIZE_DEFAULT", "15000")))

    if logger:
        logger.info(f"Starting {label} of {total} records in chunks of {effective_chunk_size}")

    for start in range(0, total, effective_chunk_size):
        chunk = records[start : start + effective_chunk_size]
        session = session_factory()
        try:
            session.bulk_insert_mappings(model, chunk)
            session.commit()
            inserted += len(chunk)
            if logger and ((start // effective_chunk_size + 1) % progress_every == 0 or inserted == total):
                logger.info(f"Inserted {inserted}/{total} records")
        except Exception as exc:
            session.rollback()
            if logger:
                logger.exception(
                    f"{label} chunk insert failed at chunk {start // effective_chunk_size + 1}: {exc}"
                )
            raise
        finally:
            session.close()

    if logger:
        logger.info(f"{label} completed successfully with {inserted} records.")
    return inserted
