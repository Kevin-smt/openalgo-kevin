# database/analyzer_db.py

import json
import os
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import Column, DateTime, Index, Integer, String, Text
from sqlalchemy.sql import func

from database.db import Base, Session, engine
from utils.async_db_logger import async_log
from utils.logging import get_logger
from utils.timezone import ensure_ist, now_ist

logger = get_logger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

db_session = Session


class AnalyzerLog(Base):
    __tablename__ = "analyzer_logs"
    id = Column(Integer, primary_key=True)
    api_type = Column(String(50), nullable=False, index=True)  # placeorder, cancelorder, etc.
    request_data = Column(Text, nullable=False)
    response_data = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=func.now(), index=True)

    # Performance indexes for analyzer queries
    __table_args__ = (
        Index("idx_analyzer_api_type", "api_type"),  # Speeds up filtering by API type
        Index(
            "idx_analyzer_created_at", "created_at"
        ),  # Speeds up time-based queries and log retrieval
        Index(
            "idx_analyzer_type_time", "api_type", "created_at"
        ),  # Composite for API type + time range queries
    )

    def to_dict(self):
        """Convert log entry to dictionary"""
        try:
            request_data = (
                json.loads(self.request_data)
                if isinstance(self.request_data, str)
                else self.request_data
            )
            response_data = (
                json.loads(self.response_data)
                if isinstance(self.response_data, str)
                else self.response_data
            )
        except json.JSONDecodeError:
            request_data = self.request_data
            response_data = self.response_data

        return {
            "id": self.id,
            "api_type": self.api_type,
            "request_data": request_data,
            "response_data": response_data,
            "created_at": ensure_ist(self.created_at).isoformat() if self.created_at else None,
        }


def init_db():
    """Initialize the analyzer table"""
    from database.db_init_helper import init_db_with_logging

    init_db_with_logging(Base, engine, "Analyzer DB", logger)


# Executor for asynchronous tasks
executor = ThreadPoolExecutor(10)  # Increased from 2 to 10 for better concurrency


def async_log_analyzer(request_data, response_data, api_type="placeorder"):
    """Asynchronously log analyzer request"""
    try:
        async_log(
            AnalyzerLog,
            {
                "api_type": api_type,
                "request_data": json.dumps(request_data),
                "response_data": json.dumps(response_data),
                "created_at": now_ist(),
            },
        )
    except Exception as e:
        logger.exception(f"Error queueing analyzer log: {e}")
        try:
            analyzer_log = AnalyzerLog(
                api_type=api_type,
                request_data=json.dumps(request_data),
                response_data=json.dumps(response_data),
                created_at=now_ist(),
            )
            db_session.add(analyzer_log)
            db_session.commit()
        except Exception as sync_error:
            logger.exception(f"Error saving analyzer log synchronously: {sync_error}")
        finally:
            db_session.remove()
