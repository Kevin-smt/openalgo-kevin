"""Live trading ledger tables and helper functions.

This module mirrors broker-side trading state into PostgreSQL without
replacing the broker as the source of truth.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from database.db_init_helper import init_db_with_logging
from utils.database_config import POOL_CONFIG, create_engine_from_env
from utils.timezone import ensure_ist, ist_midnight, now_ist

logger = logging.getLogger(__name__)

engine = create_engine_from_env(
    "DATABASE_URL",
    default_prefix="DB",
    pool_config=POOL_CONFIG,
)

TradingSession = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)
TradingBase = declarative_base()
TradingBase.query = TradingSession.query_property()


def _utcnow() -> datetime:
    return now_ist()


def _safe_decimal(value: Any, default: float | None = None) -> Decimal | None:
    if value is None or value == "":
        return Decimal(str(default)) if default is not None else None
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(str(default)) if default is not None else None


def _safe_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except Exception:
        return default


def _safe_datetime(value: Any, default: datetime | None = None) -> datetime:
    if isinstance(value, datetime):
        return ensure_ist(value) or now_ist()
    if isinstance(value, str):
        try:
            normalized = value.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            return ensure_ist(parsed) or now_ist()
        except Exception:
            pass
    return default or _utcnow()


def _safe_date(value: Any, default: date | None = None) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            normalized = value.split("T", 1)[0]
            return date.fromisoformat(normalized)
        except Exception:
            pass
    return default or _utcnow().date()


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _jsonable(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return (ensure_ist(value) or value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _row_to_dict(row: Any, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for column in row.__table__.columns:  # type: ignore[attr-defined]
        value = getattr(row, column.name)
        data[column.name] = _jsonable(value)
    if extra:
        data.update(extra)
    return data


def _resolve_ledger_user_id(
    user_id: str | None = None,
    api_key: str | None = None,
) -> str | None:
    """Resolve the user id using all safe fallbacks available to the process."""
    if user_id:
        return user_id

    if api_key:
        try:
            from database.auth_db import verify_api_key

            resolved_user_id = verify_api_key(api_key)
            if resolved_user_id:
                return resolved_user_id
        except Exception:
            logger.exception("Failed to resolve ledger user id from api_key")

    try:
        from flask import has_request_context, session as flask_session

        if has_request_context():
            session_user = flask_session.get("user")
            if session_user:
                return str(session_user)
    except Exception:
        logger.debug("Ledger user id fallback to Flask session was unavailable")

    return None


@contextmanager
def _trading_session_scope():
    session = TradingSession()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def mirror_live_order(
    *,
    order_data: dict[str, Any],
    broker: str,
    api_key: str | None,
    broker_order_id: str | None,
    order_status: str,
    broker_response: dict[str, Any] | None,
    rejection_reason: str | None = None,
    user_id: str | None = None,
    api_source: str = "placeorder",
) -> bool:
    """Best-effort live order mirror write that never affects request flow."""
    try:
        ledger_user_id = _resolve_ledger_user_id(user_id=user_id, api_key=api_key)
        logger.info(
            "[LEDGER DEBUG] mirror_live_order called: user=%s broker=%s symbol=%s api_source=%s",
            ledger_user_id,
            broker,
            order_data.get("symbol"),
            api_source,
        )

        if not ledger_user_id:
            logger.warning(
                "Skipping live order mirror because ledger user id could not be resolved "
                f"(broker={broker}, symbol={order_data.get('symbol')}, api_source={api_source})"
            )
            return

        resolved_broker_order_id = _extract_broker_order_id(
            order_data, broker_order_id, broker_response
        )

        local_order_id = save_order(
            {
                "user_id": ledger_user_id,
                "broker": broker,
                "strategy": order_data.get("strategy"),
                "broker_order_id": resolved_broker_order_id,
                "exchange": order_data.get("exchange"),
                "symbol": order_data.get("symbol"),
                "action": order_data.get("action"),
                "quantity": order_data.get("quantity"),
                "filled_quantity": order_data.get("filled_quantity", 0),
                "price": order_data.get("price"),
                "trigger_price": order_data.get("trigger_price"),
                "average_price": order_data.get("average_price"),
                "pricetype": order_data.get("pricetype"),
                "product": order_data.get("product"),
                "order_status": order_status,
                "rejection_reason": rejection_reason,
                "placed_at": now_ist(),
                "updated_at": now_ist(),
                "order_type": "live",
                "api_source": api_source,
                "api_key": api_key,
            }
        )

        if local_order_id:
            event_type = "submitted" if order_status == "open" else order_status
            save_order_event(
                local_order_id,
                event_type,
                order_data.get("quantity"),
                order_data.get("price"),
                broker_response or {},
            )
            logger.info(
                "[LEDGER DEBUG] mirror_live_order completed successfully: local_order_id=%s, broker_order_id=%s",
                local_order_id,
                resolved_broker_order_id,
            )
            return True
        logger.warning(
            "Live order mirror did not persist an order row "
            f"(broker={broker}, symbol={order_data.get('symbol')}, api_source={api_source})"
        )
        return False
    except Exception as exc:
        logger.exception("Order DB mirror failed (non-critical)")
        return False


class Order(TradingBase):
    __tablename__ = "orders"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(String(255), nullable=False, index=True)
    broker = Column(String(50), nullable=False, index=True)
    strategy = Column(String(255), nullable=True, index=True)
    broker_order_id = Column(String(255), nullable=True, index=True, unique=True)
    exchange = Column(String(20), nullable=True, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    action = Column(String(10), nullable=False)
    quantity = Column(Integer, nullable=False)
    filled_quantity = Column(Integer, nullable=False, default=0)
    price = Column(Numeric(12, 4), nullable=True)
    trigger_price = Column(Numeric(12, 4), nullable=True)
    average_price = Column(Numeric(12, 4), nullable=True)
    pricetype = Column(String(20), nullable=True)
    product = Column(String(20), nullable=True)
    order_status = Column(String(20), nullable=False, default="open", index=True)
    rejection_reason = Column(Text, nullable=True)
    placed_at = Column(DateTime(timezone=True), nullable=False, default=now_ist, index=True)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=now_ist, onupdate=now_ist, index=True)
    order_type = Column(String(20), nullable=False, default="live", index=True)
    api_source = Column(String(50), nullable=True, index=True)

    __table_args__ = (
        Index("idx_orders_user_broker", "user_id", "broker"),
        Index("idx_orders_user_status", "user_id", "order_status"),
        Index("idx_orders_broker_symbol", "broker", "symbol"),
        Index("idx_orders_broker_order_id", "broker_order_id"),
        Index("idx_orders_placed_at", "placed_at"),
        Index("idx_orders_updated_at", "updated_at"),
    )


class OrderEvent(TradingBase):
    __tablename__ = "order_events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    order_id = Column(BigInteger, ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, index=True)
    broker_order_id = Column(String(255), nullable=True, index=True)
    event_type = Column(String(50), nullable=False, index=True)
    quantity = Column(Integer, nullable=True)
    price = Column(Numeric(12, 4), nullable=True)
    event_time = Column(DateTime(timezone=True), nullable=False, default=now_ist, index=True)
    raw_broker_response = Column(JSONB, nullable=True)

    __table_args__ = (
        Index("idx_order_events_order_time", "order_id", "event_time"),
        Index("idx_order_events_broker_time", "broker_order_id", "event_time"),
    )


class Trade(TradingBase):
    __tablename__ = "trades"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    order_id = Column(BigInteger, ForeignKey("orders.id", ondelete="SET NULL"), nullable=True, index=True)
    user_id = Column(String(255), nullable=False, index=True)
    broker = Column(String(50), nullable=False, index=True)
    broker_order_id = Column(String(255), nullable=True, index=True)
    broker_trade_id = Column(String(255), nullable=True, index=True, unique=True)
    exchange = Column(String(20), nullable=True, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    action = Column(String(10), nullable=False)
    quantity = Column(Integer, nullable=False)
    price = Column(Numeric(12, 4), nullable=True)
    trade_time = Column(DateTime(timezone=True), nullable=False, default=now_ist, index=True)
    product = Column(String(20), nullable=True)

    __table_args__ = (
        Index("idx_trades_user_broker", "user_id", "broker"),
        Index("idx_trades_broker_order_id", "broker_order_id"),
        Index("idx_trades_broker_symbol", "broker", "symbol"),
        Index("idx_trades_trade_time", "trade_time"),
    )


class Position(TradingBase):
    __tablename__ = "positions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(String(255), nullable=False, index=True)
    broker = Column(String(50), nullable=False, index=True)
    exchange = Column(String(20), nullable=True, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    product = Column(String(20), nullable=True, index=True)
    net_quantity = Column(Integer, nullable=False, default=0)
    buy_quantity = Column(Integer, nullable=False, default=0)
    sell_quantity = Column(Integer, nullable=False, default=0)
    average_price = Column(Numeric(12, 4), nullable=True)
    last_price = Column(Numeric(12, 4), nullable=True)
    pnl = Column(Numeric(12, 4), nullable=True)
    day_pnl = Column(Numeric(12, 4), nullable=True)
    last_synced_at = Column(DateTime(timezone=True), nullable=False, default=now_ist, index=True)

    __table_args__ = (
        Index("idx_positions_user_broker", "user_id", "broker"),
        Index(
            "idx_positions_user_broker_exchange_symbol_product",
            "user_id",
            "broker",
            "exchange",
            "symbol",
            "product",
        ),
        Index("idx_positions_user_symbol", "user_id", "symbol"),
        Index("idx_positions_broker_symbol", "broker", "symbol"),
        Index("idx_positions_synced_at", "last_synced_at"),
    )


class Holding(TradingBase):
    __tablename__ = "holdings"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(String(255), nullable=False, index=True)
    broker = Column(String(50), nullable=False, index=True)
    exchange = Column(String(20), nullable=True, index=True)
    symbol = Column(String(50), nullable=False, index=True)
    isin = Column(String(20), nullable=True, index=True)
    quantity = Column(Integer, nullable=False, default=0)
    average_price = Column(Numeric(12, 4), nullable=True)
    last_price = Column(Numeric(12, 4), nullable=True)
    pnl = Column(Numeric(12, 4), nullable=True)
    last_synced_at = Column(DateTime(timezone=True), nullable=False, default=now_ist, index=True)

    __table_args__ = (
        Index("idx_holdings_user_broker", "user_id", "broker"),
        Index(
            "idx_holdings_user_broker_exchange_symbol",
            "user_id",
            "broker",
            "exchange",
            "symbol",
        ),
        Index("idx_holdings_user_symbol", "user_id", "symbol"),
        Index("idx_holdings_broker_symbol", "broker", "symbol"),
        Index("idx_holdings_synced_at", "last_synced_at"),
    )


class PnlSnapshot(TradingBase):
    __tablename__ = "pnl_snapshots"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(String(255), nullable=False, index=True)
    broker = Column(String(50), nullable=False, index=True)
    snapshot_date = Column(Date, nullable=False, index=True)
    realized_pnl = Column(Numeric(12, 4), nullable=True)
    unrealized_pnl = Column(Numeric(12, 4), nullable=True)
    total_pnl = Column(Numeric(12, 4), nullable=True)
    total_charges = Column(Numeric(12, 4), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=now_ist, index=True)

    __table_args__ = (
        Index("idx_pnl_snapshots_user_broker", "user_id", "broker"),
        Index("idx_pnl_snapshots_user_date", "user_id", "snapshot_date"),
        Index("idx_pnl_snapshots_broker_date", "broker", "snapshot_date"),
        Index("idx_pnl_snapshots_created_at", "created_at"),
    )


def init_trading_db() -> None:
    """Initialize the live trading ledger tables."""
    logger.info(
        f"[LEDGER DEBUG] Trading DB engine URL: {engine.url}"
    )
    init_db_with_logging(TradingBase, engine, "Trading DB", logger)
    logger.info(
        "Trading DB initialized — tables: orders, order_events, trades, positions, holdings, pnl_snapshots"
    )


ensure_trading_tables_exists = init_trading_db


def _order_payload(order_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "user_id": order_data.get("user_id"),
        "broker": order_data.get("broker"),
        "strategy": order_data.get("strategy"),
        "broker_order_id": order_data.get("broker_order_id") or order_data.get("orderid"),
        "exchange": order_data.get("exchange"),
        "symbol": order_data.get("symbol"),
        "action": str(order_data.get("action", "")).upper(),
        "quantity": _safe_int(order_data.get("quantity"), 0),
        "filled_quantity": _safe_int(order_data.get("filled_quantity"), 0),
        "price": _safe_decimal(order_data.get("price")),
        "trigger_price": _safe_decimal(order_data.get("trigger_price")),
        "average_price": _safe_decimal(order_data.get("average_price")),
        "pricetype": order_data.get("pricetype"),
        "product": order_data.get("product"),
        "order_status": order_data.get("order_status", "open"),
        "rejection_reason": order_data.get("rejection_reason"),
        "placed_at": _safe_datetime(order_data.get("placed_at"), _utcnow()),
        "updated_at": _safe_datetime(order_data.get("updated_at"), _utcnow()),
        "order_type": order_data.get("order_type", "live"),
        "api_source": order_data.get("api_source"),
    }


def _extract_broker_order_id(
    order_data: dict[str, Any] | None,
    broker_order_id: str | None,
    broker_response: dict[str, Any] | None,
) -> str | None:
    """Extract a broker order id from the most reliable source available."""
    if broker_order_id:
        return str(broker_order_id)

    sources: list[dict[str, Any]] = []
    if isinstance(order_data, dict):
        sources.append(order_data)
    if isinstance(broker_response, dict):
        sources.append(broker_response)
        data = broker_response.get("data")
        if isinstance(data, dict):
            sources.append(data)

    candidate_keys = ("orderid", "order_id", "orderId", "norenordno", "orderNumber", "id")
    for source in sources:
        for key in candidate_keys:
            value = source.get(key)
            if value not in (None, ""):
                return str(value)

    return None


def save_order(order_data: dict[str, Any]) -> int | None:
    """Insert a live order row and return the local id."""
    if not order_data:
        return None
    try:
        resolved_user_id = _resolve_ledger_user_id(
            user_id=order_data.get("user_id"),
            api_key=order_data.get("api_key"),
        )
        logger.info(
            "[LEDGER DEBUG] save_order called: user=%s, symbol=%s, broker_order_id=%s",
            resolved_user_id,
            order_data.get("symbol"),
            order_data.get("broker_order_id") or order_data.get("orderid"),
        )
        payload = _order_payload(order_data)
        payload["user_id"] = resolved_user_id
        if not payload["user_id"] or not payload["broker"] or not payload["symbol"]:
            logger.warning(
                "save_order skipped because required identity fields are missing "
                f"(user_id={payload['user_id']}, broker={payload['broker']}, symbol={payload['symbol']})"
            )
            return None

        with _trading_session_scope() as session:
            broker_order_id = payload.get("broker_order_id")
            if broker_order_id:
                stmt = pg_insert(Order).values(**payload)
                update_values = {
                    key: value
                    for key, value in payload.items()
                    if key not in {"broker_order_id", "placed_at"}
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=[Order.broker_order_id],
                    set_=update_values,
                ).returning(Order.id)
                result = session.execute(stmt)
                row_id = result.scalar_one_or_none()
                if row_id is not None:
                    logger.info(
                        "[LEDGER DEBUG] orders row persisted: id=%s user=%s broker=%s symbol=%s broker_order_id=%s status=%s",
                        row_id,
                        payload["user_id"],
                        payload["broker"],
                        payload["symbol"],
                        broker_order_id,
                        payload["order_status"],
                    )
                return int(row_id) if row_id is not None else None

            order = Order(**payload)
            session.add(order)
            session.flush()
            logger.info(
                "[LEDGER DEBUG] orders row persisted: id=%s user=%s broker=%s symbol=%s broker_order_id=%s status=%s",
                order.id,
                payload["user_id"],
                payload["broker"],
                payload["symbol"],
                broker_order_id,
                payload["order_status"],
            )
            return int(order.id)
    except Exception as exc:
        logger.exception("Error saving order")
        return None


def update_order_status(
    broker_order_id: str | None,
    status: str,
    filled_qty: int | None,
    avg_price: Any,
    rejection_reason: str | None = None,
    broker: str | None = None,
    user_id: str | None = None,
) -> bool:
    """Update a mirrored order status using the broker order id."""
    if not broker_order_id:
        return False

    try:
        filters = [Order.broker_order_id == str(broker_order_id)]
        if broker:
            filters.append(Order.broker == broker)
        if user_id:
            filters.append(Order.user_id == user_id)

        values: dict[Any, Any] = {
            Order.order_status: status,
            Order.updated_at: _utcnow(),
        }
        if filled_qty is not None:
            values[Order.filled_quantity] = _safe_int(filled_qty, 0)
        if avg_price is not None:
            values[Order.average_price] = _safe_decimal(avg_price)
        if rejection_reason is not None:
            values[Order.rejection_reason] = rejection_reason

        with _trading_session_scope() as session:
            updated = session.query(Order).filter(*filters).update(
                values, synchronize_session=False
            )
            if updated == 0:
                logger.debug(
                    "update_order_status skipped because no matching order was found "
                    f"for broker_order_id={broker_order_id}"
                )
                return False
            return True
    except Exception as exc:
        logger.exception(f"Error updating order status for {broker_order_id}")
        return False


def _orders_query_base(session, user_id: str, broker: str, date_value: date | str | None = None):
    query = session.query(Order).filter(Order.user_id == user_id, Order.broker == broker)
    if date_value:
        day = _safe_date(date_value)
        start = ist_midnight(day)
        end = start + timedelta(days=1)
        query = query.filter(Order.placed_at >= start, Order.placed_at < end)
    return query


def get_orders_by_user(
    user_id: str,
    broker: str,
    date: date | str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return mirrored orders for a user/broker, newest first."""
    try:
        limit = max(1, min(int(limit or 100), 500))
        with _trading_session_scope() as session:
            rows = (
                _orders_query_base(session, user_id, broker, date)
                .order_by(Order.placed_at.desc())
                .limit(limit)
                .all()
            )
            return [
                _row_to_dict(
                    row,
                    extra={
                        "orderid": row.broker_order_id,
                        "timestamp": _jsonable(row.placed_at),
                    },
                )
                for row in rows
            ]
    except Exception as exc:
        logger.exception(f"Error fetching orders for user={user_id}, broker={broker}")
        return []


def get_order_by_broker_id(broker_order_id: str) -> dict[str, Any] | None:
    """Return a single mirrored order by broker order id."""
    if not broker_order_id:
        return None
    try:
        with _trading_session_scope() as session:
            row = (
                session.query(Order)
                .filter(Order.broker_order_id == str(broker_order_id))
                .order_by(Order.placed_at.desc())
                .first()
            )
            if not row:
                return None
            return _row_to_dict(row, extra={"orderid": row.broker_order_id, "timestamp": _jsonable(row.placed_at)})
    except Exception as exc:
        logger.exception(f"Error fetching order {broker_order_id}")
        return None


def save_order_event(
    order_id: int | None,
    event_type: str,
    qty: int | None,
    price: Any,
    broker_response: Any | None,
) -> int | None:
    """Insert a new order event row."""
    if not order_id:
        return None

    try:
        with _trading_session_scope() as session:
            order = session.query(Order).filter(Order.id == int(order_id)).first()
            event = OrderEvent(
                order_id=int(order_id),
                broker_order_id=order.broker_order_id if order else None,
                event_type=event_type,
                quantity=_safe_int(qty, 0) if qty is not None else None,
                price=_safe_decimal(price),
                event_time=_utcnow(),
                raw_broker_response=_jsonable(broker_response or {}),
            )
            session.add(event)
            session.flush()
            logger.info(
                "[LEDGER DEBUG] order_event persisted: id=%s order_id=%s broker_order_id=%s event_type=%s",
                event.id,
                order_id,
                event.broker_order_id,
                event.event_type,
            )
            return int(event.id)
    except Exception as exc:
        logger.exception(f"Error saving order event for order_id={order_id}")
        return None


def _trade_payload(trade_data: dict[str, Any], user_id: str, broker: str) -> dict[str, Any]:
    broker_order_id = trade_data.get("broker_order_id") or trade_data.get("orderid") or trade_data.get("order_id")
    trade_time = _safe_datetime(trade_data.get("trade_time") or trade_data.get("timestamp"), _utcnow())
    return {
        "user_id": user_id,
        "broker": broker,
        "order_id": _resolve_order_id(trade_data, broker_order_id),
        "broker_order_id": str(broker_order_id) if broker_order_id is not None else None,
        "broker_trade_id": trade_data.get("broker_trade_id")
        or trade_data.get("tradeid")
        or trade_data.get("trade_id"),
        "exchange": trade_data.get("exchange"),
        "symbol": trade_data.get("symbol"),
        "action": str(trade_data.get("action", "")).upper(),
        "quantity": _safe_int(trade_data.get("quantity"), 0),
        "price": _safe_decimal(trade_data.get("price") or trade_data.get("average_price")),
        "trade_time": trade_time,
        "product": trade_data.get("product"),
    }


def _resolve_order_id(trade_data: dict[str, Any], broker_order_id: Any) -> int | None:
    if trade_data.get("order_id"):
        return _safe_int(trade_data.get("order_id"))
    if not broker_order_id:
        return None
    try:
        with _trading_session_scope() as session:
            order = (
                session.query(Order)
                .filter(Order.broker_order_id == str(broker_order_id))
                .order_by(Order.placed_at.desc())
                .first()
            )
            return int(order.id) if order else None
    except Exception:
        return None


def save_trade(trade_data: dict[str, Any]) -> int | None:
    """Insert a mirrored trade row."""
    if not trade_data:
        return None

    user_id = _resolve_ledger_user_id(
        user_id=trade_data.get("user_id"), api_key=trade_data.get("api_key")
    )
    broker = trade_data.get("broker")
    if not user_id or not broker or not trade_data.get("symbol"):
        logger.warning(
            "save_trade skipped because required identity fields are missing "
            f"(user_id={user_id}, broker={broker}, symbol={trade_data.get('symbol')})"
        )
        return None

    try:
        payload = _trade_payload(trade_data, user_id, broker)
        with _trading_session_scope() as session:
            broker_trade_id = payload.get("broker_trade_id")
            if broker_trade_id:
                stmt = pg_insert(Trade).values(**payload)
                update_values = {
                    key: value
                    for key, value in payload.items()
                    if key not in {"broker_trade_id", "trade_time"}
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=[Trade.broker_trade_id],
                    set_=update_values,
                ).returning(Trade.id)
                result = session.execute(stmt)
                row_id = result.scalar_one_or_none()
                if row_id is not None:
                    logger.info(
                        "[LEDGER DEBUG] trades row persisted: id=%s user=%s broker=%s symbol=%s broker_trade_id=%s order_id=%s",
                        row_id,
                        payload["user_id"],
                        payload["broker"],
                        payload["symbol"],
                        broker_trade_id,
                        payload.get("order_id"),
                    )
                return int(row_id) if row_id is not None else None

            trade = Trade(**payload)
            session.add(trade)
            session.flush()
            logger.info(
                "[LEDGER DEBUG] trades row persisted: id=%s user=%s broker=%s symbol=%s broker_trade_id=%s order_id=%s",
                trade.id,
                payload["user_id"],
                payload["broker"],
                payload["symbol"],
                broker_trade_id,
                payload.get("order_id"),
            )
            return int(trade.id)
    except Exception as exc:
        logger.exception(f"Error saving trade for user={user_id}, broker={broker}")
        return None


def get_trades_by_user(
    user_id: str,
    broker: str,
    date: date | str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return mirrored trades for a user/broker, newest first."""
    try:
        limit = max(1, min(int(limit or 100), 500))
        with _trading_session_scope() as session:
            query = session.query(Trade).filter(Trade.user_id == user_id, Trade.broker == broker)
            if date:
                day = _safe_date(date)
                start = ist_midnight(day)
                end = start + timedelta(days=1)
                query = query.filter(Trade.trade_time >= start, Trade.trade_time < end)
            rows = query.order_by(Trade.trade_time.desc()).limit(limit).all()
            return [
                _row_to_dict(
                    row,
                    extra={
                        "orderid": row.broker_order_id,
                        "tradeid": row.broker_trade_id,
                        "timestamp": _jsonable(row.trade_time),
                    },
                )
                for row in rows
            ]
    except Exception as exc:
        logger.exception(f"Error fetching trades for user={user_id}, broker={broker}")
        return []


def _normalize_position_row(user_id: str, broker: str, position: dict[str, Any]) -> dict[str, Any]:
    return {
        "user_id": user_id,
        "broker": broker,
        "exchange": position.get("exchange"),
        "symbol": position.get("symbol"),
        "product": position.get("product") or position.get("product_type"),
        "net_quantity": _safe_int(
            position.get("net_quantity")
            or position.get("quantity")
            or position.get("netqty")
            or position.get("net_qty")
        ),
        "buy_quantity": _safe_int(position.get("buy_quantity") or position.get("buyqty") or 0),
        "sell_quantity": _safe_int(position.get("sell_quantity") or position.get("sellqty") or 0),
        "average_price": _safe_decimal(position.get("average_price") or position.get("avg_price")),
        "last_price": _safe_decimal(position.get("last_price") or position.get("ltp")),
        "pnl": _safe_decimal(position.get("pnl")),
        "day_pnl": _safe_decimal(position.get("day_pnl") or position.get("today_pnl")),
        "last_synced_at": _utcnow(),
    }


def upsert_positions(
    user_id: str,
    broker: str,
    positions_list: list[dict[str, Any]],
    api_key: str | None = None,
) -> bool:
    """Full replace of mirrored positions for a user/broker."""
    try:
        resolved_user_id = _resolve_ledger_user_id(user_id=user_id, api_key=api_key)
        if not resolved_user_id:
            logger.warning(
                "upsert_positions skipped because required identity fields are missing "
                f"(user_id={user_id}, broker={broker})"
            )
            return False
        with _trading_session_scope() as session:
            deleted = session.query(Position).filter(
                Position.user_id == resolved_user_id,
                Position.broker == broker,
            ).delete(synchronize_session=False)

            if positions_list:
                rows = [
                    _normalize_position_row(resolved_user_id, broker, position)
                    for position in positions_list
                    if position.get("symbol")
                ]
                if rows:
                    session.bulk_insert_mappings(Position, rows)
            logger.info(
                "[LEDGER DEBUG] positions rows persisted: user=%s broker=%s deleted=%s inserted=%s",
                resolved_user_id,
                broker,
                deleted,
                len(rows) if positions_list else 0,
            )
        return True
    except Exception as exc:
        logger.exception(f"Error upserting positions for user={user_id}, broker={broker}")
        return False


def get_positions_by_user(user_id: str, broker: str) -> list[dict[str, Any]]:
    """Return mirrored positions for a user/broker."""
    try:
        with _trading_session_scope() as session:
            rows = (
                session.query(Position)
                .filter(Position.user_id == user_id, Position.broker == broker)
                .order_by(Position.symbol.asc())
                .all()
            )
            return [_row_to_dict(row) for row in rows]
    except Exception as exc:
        logger.exception(f"Error fetching positions for user={user_id}, broker={broker}")
        return []


def _normalize_holding_row(user_id: str, broker: str, holding: dict[str, Any]) -> dict[str, Any]:
    return {
        "user_id": user_id,
        "broker": broker,
        "exchange": holding.get("exchange"),
        "symbol": holding.get("symbol"),
        "isin": holding.get("isin"),
        "quantity": _safe_int(holding.get("quantity") or holding.get("qty") or 0),
        "average_price": _safe_decimal(holding.get("average_price") or holding.get("avg_price")),
        "last_price": _safe_decimal(holding.get("last_price") or holding.get("ltp")),
        "pnl": _safe_decimal(holding.get("pnl")),
        "last_synced_at": _utcnow(),
    }


def upsert_holdings(
    user_id: str,
    broker: str,
    holdings_list: list[dict[str, Any]],
    api_key: str | None = None,
) -> bool:
    """Full replace of mirrored holdings for a user/broker."""
    try:
        resolved_user_id = _resolve_ledger_user_id(user_id=user_id, api_key=api_key)
        if not resolved_user_id:
            logger.warning(
                "upsert_holdings skipped because required identity fields are missing "
                f"(user_id={user_id}, broker={broker})"
            )
            return False
        with _trading_session_scope() as session:
            deleted = session.query(Holding).filter(
                Holding.user_id == resolved_user_id,
                Holding.broker == broker,
            ).delete(synchronize_session=False)

            if holdings_list:
                rows = [
                    _normalize_holding_row(resolved_user_id, broker, holding)
                    for holding in holdings_list
                    if holding.get("symbol")
                ]
                if rows:
                    session.bulk_insert_mappings(Holding, rows)
            logger.info(
                "[LEDGER DEBUG] holdings rows persisted: user=%s broker=%s deleted=%s inserted=%s",
                resolved_user_id,
                broker,
                deleted,
                len(rows) if holdings_list else 0,
            )
        return True
    except Exception as exc:
        logger.exception(f"Error upserting holdings for user={user_id}, broker={broker}")
        return False


def get_holdings_by_user(user_id: str, broker: str) -> list[dict[str, Any]]:
    """Return mirrored holdings for a user/broker."""
    try:
        with _trading_session_scope() as session:
            rows = (
                session.query(Holding)
                .filter(Holding.user_id == user_id, Holding.broker == broker)
                .order_by(Holding.symbol.asc())
                .all()
            )
            return [_row_to_dict(row) for row in rows]
    except Exception as exc:
        logger.exception(f"Error fetching holdings for user={user_id}, broker={broker}")
        return []


def save_pnl_snapshot(
    user_id: str,
    broker: str,
    pnl_data: dict[str, Any],
    api_key: str | None = None,
) -> int | None:
    """Insert a mirrored PnL snapshot."""
    resolved_user_id = _resolve_ledger_user_id(user_id=user_id, api_key=api_key)
    if not resolved_user_id or not broker:
        return None
    try:
        snapshot = PnlSnapshot(
            user_id=resolved_user_id,
            broker=broker,
            snapshot_date=_safe_date(pnl_data.get("snapshot_date") or pnl_data.get("date")),
            realized_pnl=_safe_decimal(pnl_data.get("realized_pnl")),
            unrealized_pnl=_safe_decimal(pnl_data.get("unrealized_pnl")),
            total_pnl=_safe_decimal(pnl_data.get("total_pnl")),
            total_charges=_safe_decimal(pnl_data.get("total_charges")),
            created_at=_safe_datetime(pnl_data.get("created_at"), _utcnow()),
        )
        with _trading_session_scope() as session:
            session.add(snapshot)
            session.flush()
            logger.info(
                "[LEDGER DEBUG] pnl snapshot persisted: id=%s user=%s broker=%s snapshot_date=%s total_pnl=%s",
                snapshot.id,
                resolved_user_id,
                broker,
                snapshot.snapshot_date,
                snapshot.total_pnl,
            )
            return int(snapshot.id)
    except Exception as exc:
        logger.exception(f"Error saving pnl snapshot for user={resolved_user_id}, broker={broker}")
        return None


def get_pnl_history(user_id: str, broker: str, days: int = 30) -> list[dict[str, Any]]:
    """Return mirrored PnL snapshots for a user/broker."""
    try:
        cutoff = _utcnow().date() - timedelta(days=days)
        with _trading_session_scope() as session:
            rows = (
                session.query(PnlSnapshot)
                .filter(
                    PnlSnapshot.user_id == user_id,
                    PnlSnapshot.broker == broker,
                    PnlSnapshot.snapshot_date >= cutoff,
                )
                .order_by(PnlSnapshot.snapshot_date.desc(), PnlSnapshot.created_at.desc())
                .all()
            )
            return [_row_to_dict(row) for row in rows]
    except Exception as exc:
        logger.exception(f"Error fetching pnl history for user={user_id}, broker={broker}")
        return []
