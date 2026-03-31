import os
from datetime import datetime

from flask import jsonify, make_response, request
from flask_restx import Namespace, Resource
from marshmallow import Schema, ValidationError, fields, validate

from database.auth_db import Auth, verify_api_key
from database.trading_db import get_orders_by_user
from limiter import limiter
from services.orderbook_service import format_order_data, format_statistics
from utils.logging import get_logger

API_RATE_LIMIT = os.getenv("API_RATE_LIMIT", "10 per second")
api = Namespace("local_orderbook", description="Local Order Book API")

logger = get_logger(__name__)


class LocalOrderbookSchema(Schema):
    apikey = fields.Str(required=True, validate=validate.Length(min=1, max=256))
    date = fields.Str(required=False, allow_none=True)
    strategy = fields.Str(required=False, allow_none=True)
    status = fields.Str(required=False, allow_none=True)
    limit = fields.Int(
        required=False,
        load_default=100,
        validate=validate.Range(min=1, max=500),
    )


local_orderbook_schema = LocalOrderbookSchema()


def _normalize_local_order(order: dict) -> dict:
    return {
        "orderid": order.get("broker_order_id"),
        "broker_order_id": order.get("broker_order_id"),
        "symbol": order.get("symbol"),
        "exchange": order.get("exchange"),
        "action": order.get("action"),
        "quantity": order.get("quantity"),
        "filled_quantity": order.get("filled_quantity"),
        "price": order.get("price"),
        "trigger_price": order.get("trigger_price"),
        "average_price": order.get("average_price"),
        "pricetype": order.get("pricetype"),
        "product": order.get("product"),
        "order_status": order.get("order_status"),
        "rejection_reason": order.get("rejection_reason"),
        "timestamp": order.get("placed_at") or order.get("timestamp"),
        "placed_at": order.get("placed_at"),
        "updated_at": order.get("updated_at"),
        "strategy": order.get("strategy"),
        "order_type": order.get("order_type"),
        "api_source": order.get("api_source"),
        "user_id": order.get("user_id"),
        "broker": order.get("broker"),
    }


def _build_local_statistics(orders: list[dict]) -> dict[str, int]:
    stats = {
        "total_orders": len(orders),
        "open_orders": 0,
        "complete_orders": 0,
        "cancelled_orders": 0,
        "rejected_orders": 0,
        "buy_orders": 0,
        "sell_orders": 0,
    }
    for order in orders:
        status = str(order.get("order_status") or "").lower()
        action = str(order.get("action") or "").upper()
        if status == "open":
            stats["open_orders"] += 1
        elif status == "complete":
            stats["complete_orders"] += 1
        elif status == "cancelled":
            stats["cancelled_orders"] += 1
        elif status == "rejected":
            stats["rejected_orders"] += 1
        if action == "BUY":
            stats["buy_orders"] += 1
        elif action == "SELL":
            stats["sell_orders"] += 1
    return stats


def _parse_date(date_value: str | None):
    if not date_value:
        return None
    try:
        return datetime.strptime(date_value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _resolve_user_and_broker(api_key: str) -> tuple[str | None, str | None]:
    user_id = verify_api_key(api_key)
    if not user_id:
        return None, None

    try:
        auth_obj = Auth.query.filter_by(name=user_id).first()
        if auth_obj:
            return user_id, auth_obj.broker
    except Exception as exc:
        logger.exception(f"Failed to resolve broker for local orderbook: {exc}")
    return user_id, None


@api.route("/", strict_slashes=False)
class LocalOrderbook(Resource):
    @limiter.limit(API_RATE_LIMIT)
    def get(self):
        """Return the locally mirrored orderbook from PostgreSQL."""
        try:
            payload = {
                "apikey": request.args.get("apikey"),
                "date": request.args.get("date"),
                "strategy": request.args.get("strategy"),
                "status": request.args.get("status"),
                "limit": request.args.get("limit", 100),
            }

            try:
                args = local_orderbook_schema.load(payload)
            except ValidationError as err:
                return make_response(jsonify({"status": "error", "message": err.messages}), 400)

            api_key = args["apikey"]
            date_filter = _parse_date(args.get("date"))
            strategy_filter = (args.get("strategy") or "").strip().lower() or None
            status_filter = (args.get("status") or "").strip().lower() or None
            limit = args.get("limit", 100)

            user_id, broker = _resolve_user_and_broker(api_key)
            if not user_id or not broker:
                return make_response(
                    jsonify({"status": "error", "message": "Invalid openalgo apikey"}), 403
                )

            orders = get_orders_by_user(user_id, broker, date=date_filter, limit=limit)
            if strategy_filter:
                orders = [
                    order
                    for order in orders
                    if str(order.get("strategy") or "").strip().lower() == strategy_filter
                ]
            if status_filter:
                orders = [
                    order
                    for order in orders
                    if str(order.get("order_status") or order.get("status") or "").lower()
                    == status_filter
                ]

            normalized_orders = format_order_data([
                _normalize_local_order(order) for order in orders
            ])
            statistics = format_statistics(_build_local_statistics(normalized_orders))

            return make_response(
                jsonify(
                    {
                        "status": "success",
                        "data": {"orders": normalized_orders, "statistics": statistics},
                    }
                ),
                200,
            )
        except Exception as exc:
            logger.exception(f"Unexpected error in local orderbook endpoint: {exc}")
            return make_response(
                jsonify({"status": "error", "message": "An unexpected error occurred"}), 500
            )
