import json
import logging
import os
import threading
from datetime import date, timedelta
from contextlib import contextmanager

from cachetools import TTLCache
from sqlalchemy import Boolean, Column, Date, DateTime, Integer, String, Text, inspect, text

from database.db import Base, Session, engine
from database.db_init_helper import init_db_with_logging
from utils.timezone import ensure_ist, now_ist


def _now_ist_aware():
    return now_ist()
logger = logging.getLogger(__name__)

# If a download stays in 'downloading' state longer than this, treat it as stuck/failed
DOWNLOAD_TIMEOUT_MINUTES = 5

# Get the database path from environment variable or use default
DB_PATH = os.getenv("DATABASE_URL", "")
SessionLocal = Session
_SYM_TOKEN_SCHEMA_READY = False
_status_cache = TTLCache(maxsize=256, ttl=int(os.getenv("MASTER_CONTRACT_STATUS_TTL", "10")))
_cache_lock = threading.Lock()


def _cache_get(key):
    with _cache_lock:
        return _status_cache.get(key)


def _cache_set(key, value):
    with _cache_lock:
        _status_cache[key] = value


def _cache_clear():
    with _cache_lock:
        _status_cache.clear()


def _ensure_symtoken_table() -> bool:
    """Create the master contract symbol table(s) if missing."""
    global _SYM_TOKEN_SCHEMA_READY
    if _SYM_TOKEN_SCHEMA_READY:
        return True

    try:
        if inspect(engine).has_table("symtoken"):
            _SYM_TOKEN_SCHEMA_READY = True
            return True
    except Exception as exc:
        logger.debug(f"Could not inspect symtoken table; will try to initialize it: {exc}")

    try:
        from database.symbol import init_db as init_symbol_db

        init_symbol_db()
        _SYM_TOKEN_SCHEMA_READY = True
        logger.info("Created missing symtoken table(s) via master contract DB init")
        return True
    except Exception as exc:
        logger.exception(f"Failed to create missing symtoken table(s): {exc}")
        return False


class MasterContractStatus(Base):
    __tablename__ = "master_contract_status"

    broker = Column(String, primary_key=True)
    status = Column(String, default="pending")  # pending, downloading, success, error
    message = Column(String)
    last_updated = Column(DateTime(timezone=True), default=_now_ist_aware)
    total_symbols = Column(String, default="0")
    is_ready = Column(Boolean, default=False)

    # Smart download tracking columns
    last_download_time = Column(DateTime(timezone=True), nullable=True)  # When download completed successfully
    download_date = Column(Date, nullable=True)           # Trading day of the download
    exchange_stats = Column(Text, nullable=True)          # JSON: {"NSE": 2500, "NFO": 85000, ...}
    download_duration_seconds = Column(Integer, nullable=True)  # How long download took


def init_db():
    """Initialize the master contract status table."""
    init_db_with_logging(Base, engine, "Master Contract Status DB", logger)


def init_broker_status(broker):
    """Initialize status for a broker when they login"""
    session = SessionLocal()
    try:
        # Check if status already exists
        existing = session.query(MasterContractStatus).filter_by(broker=broker).first()

        if existing:
            # Update existing status
            existing.status = "pending"
            existing.message = "Master contract download pending"
            existing.last_updated = _now_ist_aware()
            existing.is_ready = False
        else:
            # Create new status
            status = MasterContractStatus(
                broker=broker,
                status="pending",
                message="Master contract download pending",
                last_updated=_now_ist_aware(),
                is_ready=False,
            )
            session.add(status)

        session.commit()
        _cache_clear()
        logger.info(f"Initialized master contract status for {broker}")

    except Exception as e:
        logger.exception(f"Error initializing status for {broker}: {str(e)}")
        session.rollback()
    finally:
        session.close()


def update_status(broker, status, message, total_symbols=None):
    """Update the download status for a broker"""
    session = SessionLocal()
    try:
        broker_status = session.query(MasterContractStatus).filter_by(broker=broker).first()

        if broker_status:
            broker_status.status = status
            broker_status.message = message
            broker_status.last_updated = _now_ist_aware()
            broker_status.is_ready = status == "success"

            if total_symbols is not None:
                broker_status.total_symbols = str(total_symbols)
        else:
            # Create new status if it doesn't exist
            broker_status = MasterContractStatus(
                broker=broker,
                status=status,
                message=message,
                last_updated=_now_ist_aware(),
                is_ready=(status == "success"),
                total_symbols=str(total_symbols) if total_symbols else "0",
            )
            session.add(broker_status)

        session.commit()
        _cache_clear()
        logger.info(f"Updated master contract status for {broker}: {status}")

    except Exception as e:
        logger.exception(f"Error updating status for {broker}: {str(e)}")
        session.rollback()
    finally:
        session.close()


def get_status(broker):
    """Get the current status for a broker"""
    cached = _cache_get(("status", broker))
    if cached is not None:
        return cached

    session = SessionLocal()
    try:
        status = session.query(MasterContractStatus).filter_by(broker=broker).first()

        if status:
            # Detect stuck downloads: if status is 'downloading' but last_updated
            # is older than the timeout, auto-transition to 'error'
            if (
                status.status == "downloading"
                and status.last_updated
                and now_ist() - ensure_ist(status.last_updated) > timedelta(minutes=DOWNLOAD_TIMEOUT_MINUTES)
            ):
                logger.warning(
                    f"Download for {broker} stuck for >{DOWNLOAD_TIMEOUT_MINUTES}min, marking as error"
                )
                status.status = "error"
                status.message = (
                    f"Download timed out (stuck for >{DOWNLOAD_TIMEOUT_MINUTES} minutes). "
                    "Click Force Download to retry."
                )
                status.last_updated = _now_ist_aware()
                status.is_ready = False
                session.commit()

            # Parse exchange_stats JSON if present
            exchange_stats = None
            if status.exchange_stats:
                try:
                    exchange_stats = json.loads(status.exchange_stats)
                except json.JSONDecodeError:
                    exchange_stats = None

            result = {
                "broker": status.broker,
                "status": status.status,
                "message": status.message,
                "last_updated": ensure_ist(status.last_updated).isoformat() if status.last_updated else None,
                "total_symbols": status.total_symbols,
                "is_ready": status.is_ready,
                # Smart download fields
                "last_download_time": ensure_ist(status.last_download_time).isoformat() if status.last_download_time else None,
                "download_date": status.download_date.isoformat() if status.download_date else None,
                "exchange_stats": exchange_stats,
                "download_duration_seconds": status.download_duration_seconds,
            }
            _cache_set(("status", broker), result)
            return result
        else:
            result = {
                "broker": broker,
                "status": "unknown",
                "message": "No status available",
                "last_updated": None,
                "total_symbols": "0",
                "is_ready": False,
                "last_download_time": None,
                "download_date": None,
                "exchange_stats": None,
                "download_duration_seconds": None,
            }
            _cache_set(("status", broker), result)
            return result
    except Exception as e:
        logger.exception(f"Error getting status for {broker}: {str(e)}")
        result = {
            "broker": broker,
            "status": "error",
            "message": f"Error retrieving status: {str(e)}",
            "last_updated": None,
            "total_symbols": "0",
            "is_ready": False,
            "last_download_time": None,
            "download_date": None,
            "exchange_stats": None,
            "download_duration_seconds": None,
        }
        return result
    finally:
        session.close()


def check_if_ready(broker):
    """Check if master contracts are ready for a broker"""
    cached = _cache_get(("ready", broker))
    if cached is not None:
        return cached

    session = SessionLocal()
    try:
        status = session.query(MasterContractStatus).filter_by(broker=broker).first()
        result = status.is_ready if status else False
        _cache_set(("ready", broker), result)
        return result
    except Exception as e:
        logger.exception(f"Error checking if ready for {broker}: {str(e)}")
        return False
    finally:
        session.close()


def get_last_download_time(broker):
    """Get the last successful download time for a broker"""
    cached = _cache_get(("last_download", broker))
    if cached is not None:
        return cached

    session = SessionLocal()
    try:
        status = session.query(MasterContractStatus).filter_by(broker=broker).first()
        result = ensure_ist(status.last_download_time) if status and status.last_download_time else None
        _cache_set(("last_download", broker), result)
        return result
    except Exception as e:
        logger.exception(f"Error getting last download time for {broker}: {str(e)}")
        return None
    finally:
        session.close()


def update_download_stats(broker, duration_seconds, exchange_stats=None):
    """Update download statistics after successful download"""
    session = SessionLocal()
    try:
        status = session.query(MasterContractStatus).filter_by(broker=broker).first()
        if status:
            status.last_download_time = _now_ist_aware()
            status.download_date = now_ist().date()
            status.download_duration_seconds = duration_seconds
            if exchange_stats:
                status.exchange_stats = json.dumps(exchange_stats)
            session.commit()
            _cache_clear()
            logger.info(f"Updated download stats for {broker}: {duration_seconds}s")
    except Exception as e:
        logger.exception(f"Error updating download stats for {broker}: {str(e)}")
        session.rollback()
    finally:
        session.close()


def mark_status_ready_without_download(broker):
    """Mark master contract as ready without downloading (using existing data)"""
    session = SessionLocal()
    try:
        status = session.query(MasterContractStatus).filter_by(broker=broker).first()
        if status and status.last_download_time:
            status.is_ready = True
            status.status = "success"
            status.message = "Using cached master contract"
            status.last_updated = _now_ist_aware()
            session.commit()
            _cache_clear()
            logger.info(f"Marked existing master contract as ready for {broker}")
            return True
        return False
    except Exception as e:
        logger.exception(f"Error marking status ready for {broker}: {str(e)}")
        session.rollback()
        return False
    finally:
        session.close()


def get_exchange_stats_from_db():
    """Get exchange-wise symbol counts from symtoken table"""
    try:
        cached = _cache_get(("exchange_stats", "symtoken"))
        if cached is not None:
            return cached

        if not _ensure_symtoken_table():
            return {}

        # Query symtoken table directly using raw SQL
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT
                        exchange,
                        COUNT(*) as total
                    FROM symtoken
                    GROUP BY exchange
                    ORDER BY total DESC
                    """
                )
            ).fetchall()

            stats = {row[0]: row[1] for row in result}
            _cache_set(("exchange_stats", "symtoken"), stats)
            return stats
    except Exception as e:
        logger.exception(f"Error getting exchange stats: {str(e)}")
        return {}
