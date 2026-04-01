from __future__ import annotations

import os
import threading
import logging
from contextlib import contextmanager

from sqlalchemy.orm import DeclarativeBase, scoped_session, sessionmaker

from utils.database_config import get_engine, get_resolved_database_urls

logger = logging.getLogger(__name__)

resolved_urls = get_resolved_database_urls()
DATABASE_URL = os.getenv("DATABASE_URL") or resolved_urls["DATABASE_URL"]

engine = get_engine(DATABASE_URL, db_name="openalgo")

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)
Session = scoped_session(SessionLocal)

_SESSION_LOCK = threading.Lock()
_ACTIVE_SESSION_COUNT = 0


def _log_session_count(delta: int) -> int:
    global _ACTIVE_SESSION_COUNT
    with _SESSION_LOCK:
        _ACTIVE_SESSION_COUNT = max(0, _ACTIVE_SESSION_COUNT + delta)
        return _ACTIVE_SESSION_COUNT


@contextmanager
def session_scope():
    """Provide a short-lived session for one unit of work."""
    session = SessionLocal()
    active = _log_session_count(1)
    logger.debug("[DB-SESSION] opened active_sessions=%s", active)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        remaining = _log_session_count(-1)
        logger.debug("[DB-SESSION] closed active_sessions=%s", remaining)


class Base(DeclarativeBase):
    pass


Base.query = Session.query_property()
