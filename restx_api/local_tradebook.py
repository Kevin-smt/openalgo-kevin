import os

from flask import jsonify, make_response, request
from flask_restx import Namespace, Resource
from marshmallow import ValidationError

from database.auth_db import Auth, verify_api_key
from database.trading_db import get_trades_by_user
from limiter import limiter
from utils.logging import get_logger

from .account_schema import TradebookSchema

API_RATE_LIMIT = os.getenv("API_RATE_LIMIT", "10 per second")
api = Namespace("local_tradebook", description="Local Trade Book API")

logger = get_logger(__name__)

tradebook_schema = TradebookSchema()


def _safe_float(value):
    try:
        return float(value) if value is not None else None
    except Exception:
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
        logger.exception(f"Failed to resolve broker for local tradebook: {exc}")
    return user_id, None


def _normalize_local_trade(trade: dict) -> dict:
    price = _safe_float(trade.get("price")) or 0.0
    quantity = int(_safe_float(trade.get("quantity")) or 0)
    return {
        "symbol": trade.get("symbol"),
        "exchange": trade.get("exchange"),
        "action": trade.get("action"),
        "quantity": quantity,
        "average_price": price,
        "trade_value": round(price * quantity, 2),
        "product": trade.get("product"),
        "orderid": trade.get("broker_order_id") or trade.get("orderid"),
        "tradeid": trade.get("broker_trade_id") or trade.get("tradeid"),
        "timestamp": trade.get("trade_time") or trade.get("timestamp"),
    }


@api.route("/", strict_slashes=False)
class LocalTradebook(Resource):
    @limiter.limit(API_RATE_LIMIT)
    def post(self):
        """Return the locally mirrored trade book from PostgreSQL."""
        try:
            payload = tradebook_schema.load(request.json)
            api_key = payload["apikey"]

            user_id, broker = _resolve_user_and_broker(api_key)
            if not user_id or not broker:
                return make_response(
                    jsonify({"status": "error", "message": "Invalid openalgo apikey"}), 403
                )

            trades = get_trades_by_user(user_id, broker)
            normalized_trades = [_normalize_local_trade(trade) for trade in trades]

            return make_response(
                jsonify({"status": "success", "data": normalized_trades}),
                200,
            )
        except ValidationError as err:
            return make_response(jsonify({"status": "error", "message": err.messages}), 400)
        except Exception as exc:
            logger.exception(f"Unexpected error in local tradebook endpoint: {exc}")
            return make_response(
                jsonify({"status": "error", "message": "An unexpected error occurred"}), 500
            )
