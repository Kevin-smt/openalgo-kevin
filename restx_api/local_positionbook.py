import os

from flask import jsonify, make_response, request
from flask_restx import Namespace, Resource
from marshmallow import ValidationError

from database.auth_db import Auth, verify_api_key
from database.trading_db import get_positions_by_user
from limiter import limiter
from utils.logging import get_logger

from .account_schema import PositionbookSchema

API_RATE_LIMIT = os.getenv("API_RATE_LIMIT", "10 per second")
api = Namespace("local_positionbook", description="Local Position Book API")

logger = get_logger(__name__)

positionbook_schema = PositionbookSchema()


def _safe_float(value):
    try:
        return float(value) if value is not None else None
    except Exception:
        return None


def _normalize_local_position(position: dict) -> dict:
    quantity = _safe_float(position.get("net_quantity"))
    quantity_value = int(quantity) if quantity is not None else 0
    average_price = _safe_float(position.get("average_price"))
    last_price = _safe_float(position.get("last_price"))
    pnl = _safe_float(position.get("pnl")) or 0.0
    pnl_percent = 0.0

    if average_price not in (None, 0) and quantity_value != 0:
        investment = abs(float(average_price) * quantity_value)
        pnl_percent = (pnl / investment * 100) if investment else 0.0

    return {
        "symbol": position.get("symbol"),
        "exchange": position.get("exchange"),
        "product": position.get("product"),
        "quantity": quantity_value,
        "average_price": average_price if average_price is not None else 0.0,
        "ltp": last_price if last_price is not None else 0.0,
        "pnl": pnl,
        "pnlpercent": round(pnl_percent, 2),
    }


def _resolve_user_and_broker(api_key: str) -> tuple[str | None, str | None]:
    user_id = verify_api_key(api_key)
    if not user_id:
        return None, None

    try:
        auth_obj = Auth.query.filter_by(name=user_id).first()
        if auth_obj:
            return user_id, auth_obj.broker
    except Exception as exc:
        logger.exception(f"Failed to resolve broker for local positionbook: {exc}")
    return user_id, None


@api.route("/", strict_slashes=False)
class LocalPositionbook(Resource):
    @limiter.limit(API_RATE_LIMIT)
    def post(self):
        """Return the locally mirrored position book from PostgreSQL."""
        try:
            payload = positionbook_schema.load(request.json)
            api_key = payload["apikey"]

            user_id, broker = _resolve_user_and_broker(api_key)
            if not user_id or not broker:
                return make_response(
                    jsonify({"status": "error", "message": "Invalid openalgo apikey"}), 403
                )

            positions = get_positions_by_user(user_id, broker)
            normalized_positions = [_normalize_local_position(position) for position in positions]

            return make_response(
                jsonify({"status": "success", "data": normalized_positions}),
                200,
            )
        except ValidationError as err:
            return make_response(jsonify({"status": "error", "message": err.messages}), 400)
        except Exception as exc:
            logger.exception(f"Unexpected error in local positionbook endpoint: {exc}")
            return make_response(
                jsonify({"status": "error", "message": "An unexpected error occurred"}), 500
            )
