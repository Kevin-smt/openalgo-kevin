# database/apilog_db.py

import json
import os
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import Column, DateTime, Integer, Text
from sqlalchemy.sql import func

from database.db import Base, Session, engine
from utils.async_db_logger import async_log
from utils.logging import get_logger
from utils.timezone import now_ist

logger = get_logger(__name__)


DATABASE_URL = os.getenv("DATABASE_URL")  # Resolved by the PostgreSQL env profile

db_session = Session


class OrderLog(Base):
    __tablename__ = "order_logs"
    id = Column(Integer, primary_key=True)
    api_type = Column(Text, nullable=False, index=True)
    request_data = Column(Text, nullable=False)
    response_data = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=func.now(), index=True)


def init_db():
    from database.db_init_helper import init_db_with_logging

    init_db_with_logging(Base, engine, "API Log DB", logger)


# Executor for asynchronous tasks
executor = ThreadPoolExecutor(10)  # Increased from 2 to 10 for better concurrency


def async_log_order(api_type, request_data, response_data):
    try:
        # Serialize JSON data for storage
        async_log(
            OrderLog,
            {
                "api_type": api_type,
                "request_data": json.dumps(request_data),
                "response_data": json.dumps(response_data),
                "created_at": now_ist(),
            },
        )
    except Exception as e:
        logger.exception(f"Error queueing order log: {e}")
        try:
            order_log = OrderLog(
                api_type=api_type,
                request_data=json.dumps(request_data),
                response_data=json.dumps(response_data),
                created_at=now_ist(),
            )
            db_session.add(order_log)
            db_session.commit()
        except Exception as sync_error:
            logger.exception(f"Error saving order log synchronously: {sync_error}")
        finally:
            db_session.remove()
