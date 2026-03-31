import json
import logging
import os
import threading
from contextlib import contextmanager
from datetime import timedelta

from cachetools import TTLCache
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.sql import func

from database.settings_db import get_security_settings
from database.db import Base, Session, engine
from utils.async_db_logger import async_log, get_queue_status as get_async_queue_status
from utils.timezone import ensure_ist, now_ist

logger = logging.getLogger(__name__)

logs_session = Session
LogBase = Base
logs_engine = engine

_BAN_CACHE = TTLCache(maxsize=1000, ttl=60)
_RECENT_LOGS_CACHE = TTLCache(maxsize=16, ttl=10)
_TRAFFIC_STATS_CACHE = TTLCache(maxsize=16, ttl=30)
_BAN_LIST_CACHE = TTLCache(maxsize=16, ttl=60)
_BAN_CACHE_LOCK = threading.Lock()
_REPORT_CACHE_LOCK = threading.Lock()


@contextmanager
def _logs_session_scope():
    session = logs_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
def _set_ban_cache(ip_address: str, value: bool) -> None:
    with _BAN_CACHE_LOCK:
        _BAN_CACHE[ip_address] = value


def _get_ban_cache(ip_address: str) -> tuple[bool, bool]:
    with _BAN_CACHE_LOCK:
        if ip_address in _BAN_CACHE:
            return True, bool(_BAN_CACHE[ip_address])
    return False, False


def _invalidate_traffic_read_caches() -> None:
    with _REPORT_CACHE_LOCK:
        _RECENT_LOGS_CACHE.clear()
        _TRAFFIC_STATS_CACHE.clear()


def _invalidate_ban_list_cache() -> None:
    with _REPORT_CACHE_LOCK:
        _BAN_LIST_CACHE.clear()


def _write_traffic_log(session, event: dict[str, object]) -> None:
    session.add(
        TrafficLog(
            client_ip=event["client_ip"],
            method=event["method"],
            path=event["path"],
            status_code=event["status_code"],
            duration_ms=event["duration_ms"],
            host=event.get("host"),
            error=event.get("error"),
            user_id=event.get("user_id"),
        )
    )


def _write_404_tracker(session, event: dict[str, object]) -> None:
    ip_address = str(event["ip_address"])
    path = str(event["path"])
    now = now_ist()

    tracker = session.query(Error404Tracker).filter_by(ip_address=ip_address).first()
    if tracker:
        if (now - ensure_ist(tracker.first_error_at)).days >= 1:
            tracker.error_count = 1
            tracker.first_error_at = now
            tracker.paths_attempted = json.dumps([path])
        else:
            tracker.error_count += 1
            paths = json.loads(tracker.paths_attempted or "[]")
            if path not in paths:
                paths.append(path)
                tracker.paths_attempted = json.dumps(paths[-50:])
        tracker.last_error_at = now
    else:
        session.add(
            Error404Tracker(ip_address=ip_address, error_count=1, paths_attempted=json.dumps([path]))
        )


def _write_invalid_api_key_tracker(session, event: dict[str, object]) -> None:
    ip_address = str(event["ip_address"])
    api_key_hash = event.get("api_key_hash")
    api_key_hash = str(api_key_hash) if api_key_hash else None
    now = now_ist()

    tracker = session.query(InvalidAPIKeyTracker).filter_by(ip_address=ip_address).first()
    if tracker:
        if (now - ensure_ist(tracker.first_attempt_at)).days >= 1:
            tracker.attempt_count = 1
            tracker.first_attempt_at = now
            tracker.api_keys_tried = json.dumps([api_key_hash] if api_key_hash else [])
        else:
            tracker.attempt_count += 1
            if api_key_hash:
                keys_tried = json.loads(tracker.api_keys_tried or "[]")
                if api_key_hash not in keys_tried:
                    keys_tried.append(api_key_hash)
                    tracker.api_keys_tried = json.dumps(keys_tried[-20:])
        tracker.last_attempt_at = now
    else:
        session.add(
            InvalidAPIKeyTracker(
                ip_address=ip_address,
                attempt_count=1,
                api_keys_tried=json.dumps([api_key_hash] if api_key_hash else []),
            )
        )


def get_queue_status() -> dict[str, object]:
    """Return current traffic log queue health."""
    return get_async_queue_status()


def log_queue_status() -> dict[str, object]:
    """Log queue depth and warn when it becomes congested."""
    status = get_queue_status()
    message = (
        f"Log queue depth: {status['log_queue_depth']} / {status['log_queue_maxsize']} "
        f"(worker_alive={status['worker_alive']})"
    )
    if status["log_queue_pct"] >= 0.8:
        logger.warning(message)
    else:
        logger.info(message)
    return status


class TrafficLog(LogBase):
    """Model for traffic logging"""

    __tablename__ = "traffic_logs"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    client_ip = Column(String(50), nullable=False)
    method = Column(String(10), nullable=False)
    path = Column(String(500), nullable=False)
    status_code = Column(Integer, nullable=False)
    duration_ms = Column(Float, nullable=False)
    host = Column(String(500))
    error = Column(String(500))
    user_id = Column(Integer)  # No foreign key since it's a separate database

    # Performance indexes for common query patterns
    __table_args__ = (
        Index("idx_traffic_timestamp", "timestamp"),
        Index("idx_traffic_client_ip", "client_ip"),
        Index("idx_traffic_status_code", "status_code"),
        Index("idx_traffic_user_id", "user_id"),
        Index("idx_traffic_ip_timestamp", "client_ip", "timestamp"),
    )

    @staticmethod
    def log_request(
        client_ip, method, path, status_code, duration_ms, host=None, error=None, user_id=None
    ):
        """Queue a request log so request threads never block on writes."""
        try:
            async_log(
                TrafficLog,
                {
                    "client_ip": client_ip,
                    "method": method,
                    "path": path,
                    "status_code": status_code,
                    "duration_ms": duration_ms,
                    "host": host,
                    "error": error,
                    "user_id": user_id,
                },
            )
            return True
        except Exception:
            try:
                with _logs_session_scope() as session:
                    _write_traffic_log(
                        session,
                        {
                            "client_ip": client_ip,
                            "method": method,
                            "path": path,
                            "status_code": status_code,
                            "duration_ms": duration_ms,
                            "host": host,
                            "error": error,
                            "user_id": user_id,
                        },
                    )
                _invalidate_traffic_read_caches()
                return True
            except Exception as e:
                logger.exception(f"Error logging traffic: {e}")
                return False

    @staticmethod
    def get_recent_logs(limit=100):
        """Get recent traffic logs ordered by timestamp."""
        try:
            cache_key = f"recent:{limit}"
            with _REPORT_CACHE_LOCK:
                cached = _RECENT_LOGS_CACHE.get(cache_key)
            if cached is not None:
                return cached
            with _logs_session_scope() as session:
                logs = (
                    session.query(TrafficLog)
                    .order_by(TrafficLog.timestamp.desc())
                    .limit(limit)
                    .all()
                )
            with _REPORT_CACHE_LOCK:
                _RECENT_LOGS_CACHE[cache_key] = logs
            return logs
        except Exception as e:
            logger.exception(f"Error getting recent logs: {str(e)}")
            return []

    @staticmethod
    def get_stats():
        """Get basic traffic statistics."""
        try:
            cache_key = "traffic_stats"
            with _REPORT_CACHE_LOCK:
                cached = _TRAFFIC_STATS_CACHE.get(cache_key)
            if cached is not None:
                return cached
            from sqlalchemy import func

            with _logs_session_scope() as session:
                total_requests = session.query(TrafficLog).count()
                error_requests = session.query(TrafficLog).filter(TrafficLog.status_code >= 400).count()
                avg_duration = session.query(func.avg(TrafficLog.duration_ms)).scalar() or 0

            result = {
                "total_requests": total_requests,
                "error_requests": error_requests,
                "avg_duration": round(float(avg_duration), 2),
            }
            with _REPORT_CACHE_LOCK:
                _TRAFFIC_STATS_CACHE[cache_key] = result
            return result
        except Exception as e:
            logger.exception(f"Error getting traffic stats: {str(e)}")
            return {"total_requests": 0, "error_requests": 0, "avg_duration": 0}


class IPBan(LogBase):
    """Model for banned IPs"""

    __tablename__ = "ip_bans"

    id = Column(Integer, primary_key=True)
    ip_address = Column(String(50), unique=True, nullable=False, index=True)
    ban_reason = Column(String(200))
    ban_count = Column(Integer, default=1)
    banned_at = Column(DateTime(timezone=True), server_default=func.now())
    expires_at = Column(DateTime(timezone=True))
    is_permanent = Column(Boolean, default=False)
    created_by = Column(String(50), default="system")

    @staticmethod
    def is_ip_banned(ip_address):
        """Check if an IP is currently banned."""
        try:
            cached, cached_value = _get_ban_cache(ip_address)
            if cached:
                return cached_value

            with _logs_session_scope() as session:
                ban = session.query(IPBan).filter_by(ip_address=ip_address).first()

            if not ban:
                _set_ban_cache(ip_address, False)
                return False

            if ban.is_permanent:
                _set_ban_cache(ip_address, True)
                return True

            if ban.expires_at:
                if now_ist() < ensure_ist(ban.expires_at):
                    _set_ban_cache(ip_address, True)
                    return True

                with _logs_session_scope() as session:
                    expired_ban = session.query(IPBan).filter_by(ip_address=ip_address).first()
                    if expired_ban:
                        session.delete(expired_ban)
                _set_ban_cache(ip_address, False)
                _invalidate_ban_list_cache()
                return False

            _set_ban_cache(ip_address, False)
            return False
        except Exception as e:
            logger.exception(f"Error checking IP ban status: {e}")
            return False

    @staticmethod
    def ban_ip(ip_address, reason, duration_hours=24, permanent=False, created_by="system"):
        """Ban an IP address."""
        try:
            if ip_address in ["127.0.0.1", "::1", "localhost"]:
                logger.warning(f"Attempted to ban localhost IP {ip_address} - ignoring")
                return False

            security_settings = get_security_settings()
            repeat_limit = security_settings["repeat_offender_limit"]

            with _logs_session_scope() as session:
                existing_ban = session.query(IPBan).filter_by(ip_address=ip_address).first()

                if existing_ban:
                    existing_ban.ban_count += 1
                    existing_ban.ban_reason = reason
                    existing_ban.banned_at = now_ist()

                    if existing_ban.ban_count >= repeat_limit:
                        existing_ban.is_permanent = True
                        existing_ban.expires_at = None
                        logger.warning(
                            f"IP {ip_address} permanently banned after {existing_ban.ban_count} offenses"
                        )
                    else:
                        existing_ban.is_permanent = permanent
                        existing_ban.expires_at = (
                            None if permanent else now_ist() + timedelta(hours=duration_hours)
                        )
                else:
                    session.add(
                        IPBan(
                            ip_address=ip_address,
                            ban_reason=reason,
                            is_permanent=permanent,
                            expires_at=None
                            if permanent
                            else now_ist() + timedelta(hours=duration_hours),
                            created_by=created_by,
                        )
                    )

            _set_ban_cache(ip_address, True)
            _invalidate_ban_list_cache()
            logger.info(f"IP {ip_address} banned: {reason}")
            return True
        except Exception as e:
            logger.exception(f"Error banning IP {ip_address}: {e}")
            return False

    @staticmethod
    def unban_ip(ip_address):
        """Remove IP ban."""
        try:
            with _logs_session_scope() as session:
                ban = session.query(IPBan).filter_by(ip_address=ip_address).first()
                if ban:
                    session.delete(ban)
                    _set_ban_cache(ip_address, False)
                    _invalidate_ban_list_cache()
                    logger.info(f"IP {ip_address} unbanned")
                    return True
            return False
        except Exception as e:
            logger.exception(f"Error unbanning IP: {e}")
            return False

    @staticmethod
    def get_all_bans():
        """Get all current IP bans."""
        try:
            cache_key = "ban_list"
            with _REPORT_CACHE_LOCK:
                cached = _BAN_LIST_CACHE.get(cache_key)
            if cached is not None:
                return cached
            with _logs_session_scope() as session:
                expired = session.query(IPBan).filter(
                IPBan.is_permanent == False, IPBan.expires_at < now_ist()
                ).all()

                for ban in expired:
                    session.delete(ban)
                    _set_ban_cache(ban.ip_address, False)
                if expired:
                    _invalidate_ban_list_cache()

                bans = session.query(IPBan).all()
            with _REPORT_CACHE_LOCK:
                _BAN_LIST_CACHE[cache_key] = bans
            return bans
        except Exception as e:
            logger.exception(f"Error getting IP bans: {e}")
            return []


class Error404Tracker(LogBase):
    """Track 404 errors per IP for bot detection."""

    __tablename__ = "error_404_tracker"

    id = Column(Integer, primary_key=True)
    ip_address = Column(String(50), nullable=False, index=True)
    error_count = Column(Integer, default=1)
    first_error_at = Column(DateTime(timezone=True), server_default=func.now())
    last_error_at = Column(DateTime(timezone=True), server_default=func.now())
    paths_attempted = Column(Text)

    __table_args__ = (
        Index("idx_404_error_count", "error_count"),
        Index("idx_404_first_error_at", "first_error_at"),
    )

    @staticmethod
    def track_404(ip_address, path):
        """Queue a 404 tracking event so it never blocks the request path."""
        try:
            if IPBan.is_ip_banned(ip_address):
                return False
            with _logs_session_scope() as session:
                _write_404_tracker(session, {"ip_address": ip_address, "path": path})
            _invalidate_traffic_read_caches()
            return True
        except Exception as e:
            logger.exception(f"Error tracking 404: {e}")
            return False

    @staticmethod
    def get_suspicious_ips(min_errors=5):
        """Get IPs with suspicious 404 activity."""
        try:
            cutoff = now_ist() - timedelta(days=1)
            with _logs_session_scope() as session:
                old_entries = session.query(Error404Tracker).filter(
                    Error404Tracker.first_error_at < cutoff
                ).all()

                for entry in old_entries:
                    session.delete(entry)

                return (
                    session.query(Error404Tracker)
                    .filter(Error404Tracker.error_count >= min_errors)
                    .order_by(Error404Tracker.error_count.desc())
                    .all()
                )
        except Exception as e:
            logger.exception(f"Error getting suspicious IPs: {e}")
            return []


class InvalidAPIKeyTracker(LogBase):
    """Track invalid API key attempts per IP."""

    __tablename__ = "invalid_api_key_tracker"

    id = Column(Integer, primary_key=True)
    ip_address = Column(String(50), nullable=False, index=True)
    attempt_count = Column(Integer, default=1)
    first_attempt_at = Column(DateTime(timezone=True), server_default=func.now())
    last_attempt_at = Column(DateTime(timezone=True), server_default=func.now())
    api_keys_tried = Column(Text)

    __table_args__ = (
        Index("idx_api_tracker_attempt_count", "attempt_count"),
        Index("idx_api_tracker_first_attempt_at", "first_attempt_at"),
    )

    @staticmethod
    def track_invalid_api_key(ip_address, api_key_hash=None):
        """Queue an invalid API key tracking event."""
        try:
            if IPBan.is_ip_banned(ip_address):
                return False
            with _logs_session_scope() as session:
                _write_invalid_api_key_tracker(
                    session,
                    {
                        "ip_address": ip_address,
                        "api_key_hash": api_key_hash,
                    },
                )
            _invalidate_traffic_read_caches()
            return True
        except Exception as e:
            logger.exception(f"Error tracking invalid API key: {e}")
            return False

    @staticmethod
    def get_suspicious_api_users(min_attempts=3):
        """Get IPs with suspicious API key activity."""
        try:
            cutoff = now_ist() - timedelta(days=1)
            with _logs_session_scope() as session:
                old_entries = session.query(InvalidAPIKeyTracker).filter(
                    InvalidAPIKeyTracker.first_attempt_at < cutoff
                ).all()

                for entry in old_entries:
                    session.delete(entry)

                return (
                    session.query(InvalidAPIKeyTracker)
                    .filter(InvalidAPIKeyTracker.attempt_count >= min_attempts)
                    .order_by(InvalidAPIKeyTracker.attempt_count.desc())
                    .all()
                )
        except Exception as e:
            logger.exception(f"Error getting suspicious API users: {e}")
            return []


def init_logs_db():
    """Initialize the logs database."""
    from database.db_init_helper import init_db_with_logging

    init_db_with_logging(LogBase, logs_engine, "Traffic Logs DB", logger)
