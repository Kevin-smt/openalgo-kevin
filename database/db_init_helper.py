"""
Helper module for database initialization with better logging.

This keeps the initialization path idempotent and avoids redundant metadata
inspection work when the same database bootstrap function is called more than
once during a process lifetime.

Neon / remote-PG optimization
------------------------------
create_all() internally runs many pg_catalog round trips (one per table) to
check whether each table already exists. On a high-latency connection such as
Neon serverless (India → US-East, ~300-350 ms RTT) this adds up to 15-20 s of
blocking startup time.

The fix: do a *single* pg_catalog query (get_table_names) to decide whether the
schema has been bootstrapped. If at least one table exists we assume the schema is
already in place and skip create_all() entirely. This reduces the per-database
overhead from ~10 round-trips to 1 on every warm restart.
"""

from __future__ import annotations

import threading

_INITIALIZED_DATABASES: set[tuple[int, int]] = set()
_INIT_LOCK = threading.Lock()


def init_db_with_logging(base, engine, db_name, logger):
    """
    Initialize database tables with detailed logging.

    On Neon / high-latency PostgreSQL: skips create_all() when tables already
    exist (detected with a single pg_catalog query) to avoid 10+ unnecessary
    round trips per database module at startup.

    Args:
        base: SQLAlchemy Base (declarative_base)
        engine: SQLAlchemy engine
        db_name: Name of the database (for logging)
        logger: Logger instance

    Returns:
        tuple: (tables_created, tables_verified)
    """
    cache_key = (id(base.metadata), id(engine))
    with _INIT_LOCK:
        if cache_key in _INITIALIZED_DATABASES:
            return 0, len(base.metadata.tables)

    # --- Neon / remote-PG fast path ---
    # One round-trip to check which tables already exist, then create only the
    # missing ones. This avoids re-checking every table and still bootstraps a
    # partial schema correctly.
    try:
        from sqlalchemy import inspect as sa_inspect

        inspector = sa_inspect(engine)
        existing_tables = set(inspector.get_table_names())
        metadata_tables = set(base.metadata.tables.keys())
        missing_tables = [base.metadata.tables[name] for name in metadata_tables - existing_tables]

        if not missing_tables:
            # Schema already bootstrapped – nothing to do.
            with _INIT_LOCK:
                _INITIALIZED_DATABASES.add(cache_key)
            logger.debug(
                f"{db_name}: schema already exists ({len(existing_tables)} tables), "
                "skipping create_all()"
            )
            return 0, len(base.metadata.tables)

        logger.debug(
            f"{db_name}: schema missing {len(missing_tables)} table(s), creating only the missing ones"
        )
    except Exception as exc:
        # If the inspection fails for any reason, fall through to create_all()
        # so we don't leave the database un-bootstrapped.
        logger.debug(f"{db_name}: table inspection failed ({exc}), falling back to create_all()")
        missing_tables = None

    # Create tables (first-run: DB is empty, partial schema, or inspection failed)
    if missing_tables is None:
        base.metadata.create_all(bind=engine)
    else:
        base.metadata.create_all(bind=engine, tables=missing_tables)

    with _INIT_LOCK:
        _INITIALIZED_DATABASES.add(cache_key)

    tables_created = len(missing_tables) if missing_tables is not None else len(base.metadata.tables)
    tables_verified = len(base.metadata.tables)

    # Log appropriately
    logger.debug(f"{db_name}: initialization completed ({tables_verified} table(s) ready)")

    return tables_created, tables_verified
