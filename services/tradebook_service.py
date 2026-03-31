import importlib
import traceback
from typing import Any, Dict, List, Optional, Tuple, Union

from database.auth_db import get_auth_token_broker
from database.trading_db import _resolve_ledger_user_id, save_trade
from utils.logging import get_logger

# Initialize logger
logger = get_logger(__name__)


def format_decimal(value):
    """Format numeric value to 2 decimal places"""
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    return value


def format_trade_data(trade_data):
    """Format all numeric values in trade data to 2 decimal places, except quantity fields"""
    # Fields that should remain as integers
    quantity_fields = {
        "quantity",
        "qty",
        "tradedqty",
        "filledqty",
        "filled_quantity",
        "traded_quantity",
    }

    if isinstance(trade_data, list):
        return [
            {
                key: (int(value) if value == int(value) else value)
                if (key.lower() in quantity_fields and isinstance(value, (int, float)))
                else (format_decimal(value) if isinstance(value, (int, float)) else value)
                for key, value in item.items()
            }
            for item in trade_data
        ]
    return trade_data


def import_broker_module(broker_name: str) -> dict[str, Any] | None:
    """
    Dynamically import the broker-specific tradebook modules.

    Args:
        broker_name: Name of the broker

    Returns:
        Dictionary of broker functions or None if import fails
    """
    try:
        # Import API module
        api_module = importlib.import_module(f"broker.{broker_name}.api.order_api")
        # Import mapping module
        mapping_module = importlib.import_module(f"broker.{broker_name}.mapping.order_data")
        return {
            "get_trade_book": api_module.get_trade_book,
            "map_trade_data": mapping_module.map_trade_data,
            "transform_tradebook_data": mapping_module.transform_tradebook_data,
        }
    except (ImportError, AttributeError) as error:
        logger.error(f"Error importing broker modules: {error}")
        return None


def get_tradebook_with_auth(
    auth_token: str,
    broker: str,
    original_data: dict[str, Any] = None,
    user_id: str | None = None,
) -> tuple[bool, dict[str, Any], int]:
    """
    Get trade book details using provided auth token.

    Args:
        auth_token: Authentication token for the broker API
        broker: Name of the broker
        original_data: Original request data (for sandbox mode, optional for internal calls)

    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)
    """
    # If in analyze mode AND we have original_data (API call), route to sandbox
    # If original_data is None (internal call), use live broker
    from database.settings_db import get_analyze_mode

    if get_analyze_mode() and original_data:
        from services.sandbox_service import sandbox_get_tradebook

        api_key = original_data.get("apikey")
        if not api_key:
            return (
                False,
                {
                    "status": "error",
                    "message": "API key required for sandbox mode",
                    "mode": "analyze",
                },
                400,
            )

        return sandbox_get_tradebook(api_key, original_data)

    broker_funcs = import_broker_module(broker)
    if broker_funcs is None:
        return False, {"status": "error", "message": "Broker-specific module not found"}, 404

    try:
        # Get tradebook data using broker's implementation
        trade_data = broker_funcs["get_trade_book"](auth_token)

        if "status" in trade_data and trade_data["status"] == "error":
            return (
                False,
                {
                    "status": "error",
                    "message": trade_data.get("message", "Error fetching trade data"),
                },
                500,
            )

        # Transform data using mapping functions
        trade_data = broker_funcs["map_trade_data"](trade_data=trade_data)
        trade_data = broker_funcs["transform_tradebook_data"](trade_data)

        # Format numeric values to 2 decimal places
        formatted_trades = format_trade_data(trade_data)
        logger.info(
            "[LEDGER DEBUG] tradebook fetch complete: broker=%s trades=%s api_key_present=%s user_id=%s",
            broker,
            len(formatted_trades) if isinstance(formatted_trades, list) else "n/a",
            bool(original_data.get("apikey") if original_data else None),
            user_id,
        )

        try:
            api_key = original_data.get("apikey") if original_data else None
            ledger_user_id = _resolve_ledger_user_id(user_id=user_id, api_key=api_key)
            logger.info(
                "[LEDGER DEBUG] tradebook mirror start: resolved_user_id=%s broker=%s",
                ledger_user_id,
                broker,
            )

            if ledger_user_id:
                for trade in formatted_trades:
                    trade_payload = trade.copy()
                    trade_payload["user_id"] = ledger_user_id
                    trade_payload["broker"] = broker
                    trade_payload["api_key"] = api_key
                    trade_payload["broker_order_id"] = (
                        trade.get("broker_order_id")
                        or trade.get("orderid")
                        or trade.get("order_id")
                    )
                    trade_payload["broker_trade_id"] = (
                        trade.get("broker_trade_id")
                        or trade.get("tradeid")
                        or trade.get("trade_id")
                    )
                    save_trade(trade_payload)
                    logger.info(
                        "[LEDGER DEBUG] trade mirrored: user=%s broker=%s symbol=%s broker_trade_id=%s broker_order_id=%s",
                        ledger_user_id,
                        broker,
                        trade_payload.get("symbol"),
                        trade_payload.get("broker_trade_id"),
                        trade_payload.get("broker_order_id"),
                    )
        except Exception as exc:
            logger.warning(f"Tradebook mirror sync failed (non-critical): {exc}")

        return True, {"status": "success", "data": formatted_trades}, 200
    except Exception as e:
        logger.error(f"Error processing trade data: {e}")
        traceback.print_exc()
        return False, {"status": "error", "message": str(e)}, 500


def get_tradebook(
    api_key: str | None = None,
    auth_token: str | None = None,
    broker: str | None = None,
    user_id: str | None = None,
) -> tuple[bool, dict[str, Any], int]:
    """
    Get trade book details.
    Supports both API-based authentication and direct internal calls.

    Args:
        api_key: OpenAlgo API key (for API-based calls)
        auth_token: Direct broker authentication token (for internal calls)
        broker: Direct broker name (for internal calls)

    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)
    """
    # Case 1: API-based authentication
    if api_key and not (auth_token and broker):
        AUTH_TOKEN, broker_name = get_auth_token_broker(api_key)
        if AUTH_TOKEN is None:
            return False, {"status": "error", "message": "Invalid openalgo apikey"}, 403
        original_data = {"apikey": api_key}
        return get_tradebook_with_auth(AUTH_TOKEN, broker_name, original_data, user_id=user_id)

    # Case 2: Direct internal call with auth_token and broker
    elif auth_token and broker:
        return get_tradebook_with_auth(auth_token, broker, None, user_id=user_id)

    # Case 3: Invalid parameters
    else:
        return (
            False,
            {
                "status": "error",
                "message": "Either api_key or both auth_token and broker must be provided",
            },
            400,
        )
