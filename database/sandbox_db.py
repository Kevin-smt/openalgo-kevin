# database/sandbox_db.py

import os
import threading
from contextlib import contextmanager
from datetime import datetime

from dotenv import load_dotenv
from cachetools import TTLCache
from sqlalchemy import (
    DECIMAL,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from database.db import Base, Session, SessionLocal, engine
from utils.logging import get_logger
from utils.timezone import now_ist

# Initialize logger
logger = get_logger(__name__)

# Load environment variables
load_dotenv()

# Sandbox database URL - separate database for isolation
# Get from environment variable or use default path in /db directory
SANDBOX_DATABASE_URL = os.getenv("SANDBOX_DATABASE_URL", "")
logger.info(f"[LEDGER DEBUG] Sandbox DB engine URL: {engine.url}")
db_session = Session

_config_cache = TTLCache(
    maxsize=128, ttl=int(os.getenv("SANDBOX_CONFIG_CACHE_TTL", "30"))
)
_config_cache_lock = threading.Lock()


@contextmanager
def _session_scope():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _invalidate_config_cache():
    with _config_cache_lock:
        _config_cache.clear()


def _get_cached_config(config_key):
    with _config_cache_lock:
        return _config_cache.get(config_key)


def _set_cached_config(config_key, value):
    with _config_cache_lock:
        _config_cache[config_key] = value


class SandboxOrders(Base):
    """Sandbox orders table - all virtual orders"""

    __tablename__ = "sandbox_orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    orderid = Column(String(50), unique=True, nullable=False, index=True)
    user_id = Column(String(50), nullable=False, index=True)
    strategy = Column(String(100), nullable=True)
    symbol = Column(String(50), nullable=False, index=True)
    exchange = Column(String(20), nullable=False, index=True)
    action = Column(String(10), nullable=False)  # BUY or SELL
    quantity = Column(Integer, nullable=False)
    price = Column(DECIMAL(10, 2), nullable=True)  # Null for market orders
    trigger_price = Column(DECIMAL(10, 2), nullable=True)  # For SL and SL-M orders
    price_type = Column(String(20), nullable=False)  # MARKET, LIMIT, SL, SL-M
    product = Column(String(20), nullable=False)  # CNC, NRML, MIS
    order_status = Column(
        String(20), nullable=False, default="open", index=True
    )  # open, complete, cancelled, rejected
    average_price = Column(DECIMAL(10, 2), nullable=True)  # Filled price
    filled_quantity = Column(Integer, default=0)  # Always 0 or quantity (no partial fills)
    pending_quantity = Column(Integer, nullable=False)  # Remaining quantity
    rejection_reason = Column(Text, nullable=True)
    margin_blocked = Column(
        DECIMAL(10, 2), nullable=True, default=0.00
    )  # Margin blocked at order placement
    order_timestamp = Column(DateTime(timezone=True), nullable=False, default=now_ist)
    update_timestamp = Column(DateTime(timezone=True), nullable=False, default=now_ist, onupdate=now_ist)

    __table_args__ = (
        Index("idx_sandbox_user_status", "user_id", "order_status"),
        Index("idx_sandbox_symbol_exchange", "symbol", "exchange"),
        CheckConstraint(
            "order_status IN ('open', 'complete', 'cancelled', 'rejected')",
            name="sandbox_check_order_status",
        ),
        CheckConstraint("action IN ('BUY', 'SELL')", name="sandbox_check_action"),
        CheckConstraint(
            "price_type IN ('MARKET', 'LIMIT', 'SL', 'SL-M')",
            name="sandbox_check_price_type",
        ),
        CheckConstraint("product IN ('CNC', 'NRML', 'MIS')", name="sandbox_check_product"),
    )


class SandboxTrades(Base):
    """Sandbox trades table - executed trades"""

    __tablename__ = "sandbox_trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tradeid = Column(String(50), unique=True, nullable=False, index=True)
    orderid = Column(String(50), nullable=False, index=True)
    user_id = Column(String(50), nullable=False, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    exchange = Column(String(20), nullable=False, index=True)
    action = Column(String(10), nullable=False)  # BUY or SELL
    quantity = Column(Integer, nullable=False)
    price = Column(DECIMAL(10, 2), nullable=False)  # Execution price
    product = Column(String(20), nullable=False)  # CNC, NRML, MIS
    strategy = Column(String(100), nullable=True)
    trade_timestamp = Column(DateTime(timezone=True), nullable=False, default=now_ist)

    __table_args__ = (
        Index("idx_sandbox_user_symbol", "user_id", "symbol"),
        Index("idx_sandbox_orderid", "orderid"),
    )


class SandboxPositions(Base):
    """Sandbox positions table - open positions"""

    __tablename__ = "sandbox_positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(50), nullable=False, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    exchange = Column(String(20), nullable=False, index=True)
    product = Column(String(20), nullable=False)  # CNC, NRML, MIS
    quantity = Column(Integer, nullable=False)  # Net quantity (can be negative for short)
    average_price = Column(DECIMAL(10, 2), nullable=False)  # Average entry price

    # MTM tracking
    ltp = Column(DECIMAL(10, 2), nullable=True)  # Last traded price
    pnl = Column(
        DECIMAL(10, 2), default=0.00
    )  # Current P&L (unrealized for open, realized for closed)
    pnl_percent = Column(DECIMAL(10, 4), default=0.00)  # P&L percentage
    accumulated_realized_pnl = Column(
        DECIMAL(10, 2), default=0.00
    )  # Accumulated realized P&L (all-time for this position)
    today_realized_pnl = Column(
        DECIMAL(10, 2), default=0.00
    )  # Today's realized P&L only (resets daily)

    # Margin tracking - stores exact margin blocked for this position
    # This prevents margin release bugs when execution price differs from order placement price
    margin_blocked = Column(DECIMAL(15, 2), default=0.00)  # Total margin blocked for this position

    # Timestamps
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_ist)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=now_ist, onupdate=now_ist)

    __table_args__ = (
        UniqueConstraint(
            "user_id", "symbol", "exchange", "product", name="unique_sandbox_position"
        ),
        Index("idx_sandbox_user_product", "user_id", "product"),
    )


class SandboxHoldings(Base):
    """Sandbox holdings table - T+1 settled CNC positions"""

    __tablename__ = "sandbox_holdings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(50), nullable=False, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    exchange = Column(String(20), nullable=False, index=True)
    quantity = Column(Integer, nullable=False)  # Total holdings quantity
    average_price = Column(DECIMAL(10, 2), nullable=False)  # Average buy price

    # MTM tracking
    ltp = Column(DECIMAL(10, 2), nullable=True)  # Last traded price
    pnl = Column(DECIMAL(10, 2), default=0.00)  # Unrealized P&L
    pnl_percent = Column(DECIMAL(10, 4), default=0.00)  # P&L percentage

    # Settlement tracking
    settlement_date = Column(Date, nullable=False)  # Date when position was settled to holdings

    # Timestamps
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_ist)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=now_ist, onupdate=now_ist)

    __table_args__ = (
        UniqueConstraint("user_id", "symbol", "exchange", name="unique_sandbox_holding"),
    )


class SandboxFunds(Base):
    """Sandbox funds table - simulated capital and margin tracking"""

    __tablename__ = "sandbox_funds"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(50), unique=True, nullable=False, index=True)

    # Fund balances
    total_capital = Column(DECIMAL(15, 2), default=10000000.00)  # ₹1 Crore starting capital
    available_balance = Column(DECIMAL(15, 2), default=10000000.00)  # Available for trading
    used_margin = Column(DECIMAL(15, 2), default=0.00)  # Margin blocked in positions

    # P&L tracking
    realized_pnl = Column(
        DECIMAL(15, 2), default=0.00
    )  # Realized profit/loss from closed positions (all-time)
    today_realized_pnl = Column(
        DECIMAL(15, 2), default=0.00
    )  # Today's realized P&L only (resets daily)
    unrealized_pnl = Column(DECIMAL(15, 2), default=0.00)  # Unrealized P&L from open positions
    total_pnl = Column(DECIMAL(15, 2), default=0.00)  # Total P&L (realized + unrealized)

    # Reset tracking
    last_reset_date = Column(DateTime(timezone=True), nullable=False, default=now_ist)
    reset_count = Column(Integer, default=0)  # Number of times reset has occurred

    # Timestamps
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_ist)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=now_ist, onupdate=now_ist)


class SandboxDailyPnL(Base):
    """Sandbox daily P&L snapshots - tracks end-of-day P&L for historical reporting"""

    __tablename__ = "sandbox_daily_pnl"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String(50), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)  # Trading date

    # Realized P&L (from closed positions/trades)
    realized_pnl = Column(DECIMAL(15, 2), default=0.00)

    # Unrealized P&L (from open positions + holdings at EOD)
    positions_unrealized_pnl = Column(DECIMAL(15, 2), default=0.00)  # Open positions MTM
    holdings_unrealized_pnl = Column(DECIMAL(15, 2), default=0.00)  # Holdings MTM

    # Total MTM = Realized + Unrealized
    total_mtm = Column(DECIMAL(15, 2), default=0.00)

    # Portfolio value at EOD
    available_balance = Column(DECIMAL(15, 2), default=0.00)
    used_margin = Column(DECIMAL(15, 2), default=0.00)
    portfolio_value = Column(DECIMAL(15, 2), default=0.00)  # Total value including positions

    # Metadata
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_ist)

    __table_args__ = (
        UniqueConstraint("user_id", "date", name="unique_sandbox_user_daily_pnl"),
        Index("idx_sandbox_user_date", "user_id", "date"),
    )


class SandboxConfig(Base):
    """Sandbox configuration table - all configurable settings"""

    __tablename__ = "sandbox_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    config_key = Column(String(100), unique=True, nullable=False, index=True)
    config_value = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=now_ist, onupdate=now_ist)


def init_sandbox_db():
    """Initialize sandbox database and tables"""
    from database.db_init_helper import init_db_with_logging

    init_db_with_logging(Base, engine, "Sandbox DB", logger)
    logger.info(
        "Sandbox DB initialized — tables: sandbox_orders, sandbox_trades, sandbox_positions, sandbox_holdings, sandbox_funds, sandbox_daily_pnl, sandbox_config"
    )

    # Initialize default configuration
    init_default_config()


def init_db():
    """Backward-compatible alias for sandbox DB initialization."""
    init_sandbox_db()


def init_default_config():
    """Initialize default sandbox configuration"""
    from sqlalchemy.exc import IntegrityError

    default_configs = [
        {
            "config_key": "starting_capital",
            "config_value": "10000000.00",
            "description": "Starting sandbox capital in INR (₹1 Crore) - Min: ₹1000",
        },
        {
            "config_key": "reset_day",
            "config_value": "Never",
            "description": "Day of week for automatic fund reset (Never = disabled)",
        },
        {
            "config_key": "reset_time",
            "config_value": "00:00",
            "description": "Time for automatic fund reset (IST)",
        },
        {
            "config_key": "order_check_interval",
            "config_value": "5",
            "description": "Interval in seconds to check pending orders - Range: 1-30 seconds",
        },
        {
            "config_key": "mtm_update_interval",
            "config_value": "5",
            "description": "Interval in seconds to update MTM - Range: 0-60 seconds (0 = manual only)",
        },
        {
            "config_key": "nse_bse_square_off_time",
            "config_value": "15:15",
            "description": "Square-off time for NSE/BSE MIS positions (IST)",
        },
        {
            "config_key": "cds_bcd_square_off_time",
            "config_value": "16:45",
            "description": "Square-off time for CDS/BCD MIS positions (IST)",
        },
        {
            "config_key": "mcx_square_off_time",
            "config_value": "23:30",
            "description": "Square-off time for MCX MIS positions (IST)",
        },
        {
            "config_key": "ncdex_square_off_time",
            "config_value": "17:00",
            "description": "Square-off time for NCDEX MIS positions (IST)",
        },
        {
            "config_key": "equity_mis_leverage",
            "config_value": "5",
            "description": "Leverage multiplier for equity MIS (NSE/BSE) - Range: 1-50x",
        },
        {
            "config_key": "equity_cnc_leverage",
            "config_value": "1",
            "description": "Leverage multiplier for equity CNC (NSE/BSE) - Range: 1-50x",
        },
        {
            "config_key": "futures_leverage",
            "config_value": "10",
            "description": "Leverage multiplier for all futures (NFO/BFO/CDS/BCD/MCX/NCDEX) - Range: 1-50x",
        },
        {
            "config_key": "option_buy_leverage",
            "config_value": "1",
            "description": "Leverage for buying options (full premium) - Range: 1-50x",
        },
        {
            "config_key": "option_sell_leverage",
            "config_value": "1",
            "description": "Leverage for selling options (same as buying - full premium) - Range: 1-50x",
        },
        {
            "config_key": "order_rate_limit",
            "config_value": "10",
            "description": "Maximum orders per second - Range: 1-100 orders/sec (for future use)",
        },
        {
            "config_key": "api_rate_limit",
            "config_value": "50",
            "description": "Maximum API calls per second - Range: 1-1000 calls/sec (for future use)",
        },
        {
            "config_key": "smart_order_rate_limit",
            "config_value": "2",
            "description": "Maximum smart orders per second - Range: 1-50 orders/sec (for future use)",
        },
        {
            "config_key": "smart_order_delay",
            "config_value": "0.5",
            "description": "Delay between multi-leg smart orders - Range: 0.1-10 seconds (for future use)",
        },
    ]

    try:
        with _session_scope() as session:
            existing_keys = {
                row[0]
                for row in session.query(SandboxConfig.config_key)
                .filter(SandboxConfig.config_key.in_([cfg["config_key"] for cfg in default_configs]))
                .all()
            }

            missing = [SandboxConfig(**cfg) for cfg in default_configs if cfg["config_key"] not in existing_keys]
            if missing:
                session.add_all(missing)
                logger.debug(f"Added {len(missing)} default sandbox config row(s)")
    except IntegrityError:
        logger.debug("Sandbox config defaults already exist")
    except Exception as e:
        logger.exception(f"Error initializing sandbox defaults: {e}")
    finally:
        _invalidate_config_cache()


def get_config(config_key, default=None):
    """Get configuration value by key"""
    try:
        cached = _get_cached_config(config_key)
        if cached is not None:
            return cached

        with SessionLocal() as session:
            config = session.query(SandboxConfig).filter_by(config_key=config_key).first()
            if config:
                _set_cached_config(config_key, config.config_value)
                return config.config_value
            return default
    except Exception as e:
        logger.exception(f"Error fetching config {config_key}: {e}")
        return default


def set_config(config_key, config_value, description=None):
    """Set configuration value"""
    try:
        with _session_scope() as session:
            config = session.query(SandboxConfig).filter_by(config_key=config_key).first()
            if config:
                config.config_value = str(config_value)
                if description:
                    config.description = description
            else:
                config = SandboxConfig(
                    config_key=config_key, config_value=str(config_value), description=description
                )
                session.add(config)
        logger.info(f"Updated config: {config_key} = {config_value}")
        _set_cached_config(config_key, str(config_value))
        return True
    except Exception as e:
        logger.exception(f"Error setting config {config_key}: {e}")
        return False


def get_all_configs():
    """Get all configuration values"""
    try:
        with SessionLocal() as session:
            configs = session.query(SandboxConfig).all()
            result = {
                config.config_key: {"value": config.config_value, "description": config.description}
                for config in configs
            }
        with _config_cache_lock:
            for key, value in result.items():
                _config_cache[key] = value["value"]
        return result
    except Exception as e:
        logger.exception(f"Error fetching all configs: {e}")
        return {}
