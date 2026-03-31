import os

from flask import jsonify, make_response, request
from flask_restx import Namespace, Resource
from marshmallow import ValidationError

from database.auth_db import Auth, verify_api_key
from database.trading_db import get_holdings_by_user
from limiter import limiter
from utils.logging import get_logger

from .account_schema import HoldingsSchema

API_RATE_LIMIT = os.getenv("API_RATE_LIMIT", "10 per second")
api = Namespace("local_holdings", description="Local Holdings API")

logger = get_logger(__name__)

holdings_schema = HoldingsSchema()


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
        logger.exception(f"Failed to resolve broker for local holdings: {exc}")
    return user_id, None


def _normalize_local_holding(holding: dict) -> dict:
    quantity = int(_safe_float(holding.get("quantity")) or 0)
    average_price = _safe_float(holding.get("average_price")) or 0.0
    last_price = _safe_float(holding.get("last_price"))
    ltp = last_price if last_price is not None else average_price
    pnl = _safe_float(holding.get("pnl")) or 0.0
    pnl_percent = 0.0
    if average_price not in (None, 0):
        investment = abs(float(average_price) * quantity)
        pnl_percent = (pnl / investment * 100) if investment else 0.0

    return {
        "symbol": holding.get("symbol"),
        "exchange": holding.get("exchange"),
        "quantity": quantity,
        "product": holding.get("product"),
        "average_price": average_price,
        "ltp": ltp,
        "pnl": pnl,
        "pnlpercent": round(pnl_percent, 2),
    }


def _build_statistics(holdings: list[dict]) -> dict[str, float]:
    total_holding_value = 0.0
    total_inv_value = 0.0
    total_pnl = 0.0

    for holding in holdings:
        quantity = int(holding.get("quantity") or 0)
        average_price = float(holding.get("average_price") or 0.0)
        ltp = float(holding.get("ltp") or average_price or 0.0)
        pnl = float(holding.get("pnl") or 0.0)

        total_holding_value += ltp * quantity
        total_inv_value += average_price * quantity
        total_pnl += pnl

    total_pnl_percentage = (total_pnl / total_inv_value * 100) if total_inv_value else 0.0
    return {
        "totalholdingvalue": round(total_holding_value, 2),
        "totalinvvalue": round(total_inv_value, 2),
        "totalprofitandloss": round(total_pnl, 2),
        "totalpnlpercentage": round(total_pnl_percentage, 2),
    }


@api.route("/", strict_slashes=False)
class LocalHoldings(Resource):
    @limiter.limit(API_RATE_LIMIT)
    def post(self):
        """Return the locally mirrored holdings from PostgreSQL."""
        try:
            payload = holdings_schema.load(request.json)
            api_key = payload["apikey"]

            user_id, broker = _resolve_user_and_broker(api_key)
            if not user_id or not broker:
                return make_response(
                    jsonify({"status": "error", "message": "Invalid openalgo apikey"}), 403
                )

            holdings = get_holdings_by_user(user_id, broker)
            normalized_holdings = [_normalize_local_holding(holding) for holding in holdings]
            statistics = _build_statistics(normalized_holdings)

            return make_response(
                jsonify(
                    {
                        "status": "success",
                        "data": {"holdings": normalized_holdings, "statistics": statistics},
                    }
                ),
                200,
            )
        except ValidationError as err:
            return make_response(jsonify({"status": "error", "message": err.messages}), 400)
        except Exception as exc:
            logger.exception(f"Unexpected error in local holdings endpoint: {exc}")
            return make_response(
                jsonify({"status": "error", "message": "An unexpected error occurred"}), 500
            )
