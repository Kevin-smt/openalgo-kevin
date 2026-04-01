# services/pending_order_execution_service.py

import json
import time
from typing import Any, Dict, Tuple

from database.action_center_db import get_pending_order_by_id, update_broker_status
from database.auth_db import get_api_key_for_tradingview, get_auth_token
from extensions import socketio
from utils.logging import get_logger

# Initialize logger
logger = get_logger(__name__)


def _log_timing(label: str, started_at: float, **fields: Any) -> None:
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    extras = " ".join(f"{key}={value}" for key, value in fields.items() if value is not None)
    if extras:
        logger.info(f"[TIMING] pending_order_execution | stage={label} | elapsed_ms={elapsed_ms} | {extras}")
    else:
        logger.info(f"[TIMING] pending_order_execution | stage={label} | elapsed_ms={elapsed_ms}")


def _refresh_pending_order_status(
    pending_order_id: int,
    broker_order_id: str,
    api_key: str,
    auth_token: str,
    broker: str,
    user_id: str,
) -> None:
    """Best-effort background reconciliation for approved orders."""
    try:
        from services.orderstatus_service import get_order_status

        status_success, status_response, _ = get_order_status(
            status_data={"orderid": broker_order_id},
            api_key=api_key,
            auth_token=auth_token,
            broker=broker,
            user_id=user_id,
        )

        if status_success and isinstance(status_response, dict) and "data" in status_response:
            actual_status = status_response["data"].get("status", "open")
            update_broker_status(pending_order_id, broker_order_id, actual_status)
            logger.info(
                f"Order status refreshed in background: pending_order_id={pending_order_id}, "
                f"broker_order_id={broker_order_id}, status={actual_status}"
            )
        else:
            update_broker_status(pending_order_id, broker_order_id, "open")
            logger.info(
                f"Order status refresh did not return data; kept open: pending_order_id={pending_order_id}, "
                f"broker_order_id={broker_order_id}"
            )
    except Exception as exc:
        logger.exception(f"Background order status refresh failed: {exc}")
        update_broker_status(pending_order_id, broker_order_id, "open")


def execute_approved_order(pending_order_id: int) -> tuple[bool, dict[str, Any], int]:
    """
    Execute an approved pending order

    Args:
        pending_order_id: ID of the pending order to execute

    Returns:
        Tuple containing:
        - Success status (bool)
        - Response data (dict)
        - HTTP status code (int)
    """
    try:
        started_at = time.perf_counter()
        # Get the pending order
        pending_order = get_pending_order_by_id(pending_order_id)
        _log_timing("pending_order_loaded", started_at, pending_order_id=pending_order_id)

        if not pending_order:
            logger.error(f"Pending order {pending_order_id} not found")
            return False, {"status": "error", "message": "Pending order not found"}, 404

        if pending_order.status != "approved":
            logger.error(
                f"Cannot execute pending order {pending_order_id}: status is '{pending_order.status}', not 'approved'"
            )
            return (
                False,
                {
                    "status": "error",
                    "message": f"Order cannot be executed (status: {pending_order.status})",
                },
                400,
            )

        # Parse order data
        order_data = json.loads(pending_order.order_data)
        api_type = pending_order.api_type
        user_id = pending_order.user_id

        # Get the user's API key (needed for order_data validation and broker functions)
        api_key = get_api_key_for_tradingview(user_id)
        _log_timing("api_key_loaded", started_at, user_id=user_id)

        # Get auth token and broker (to skip routing check and authenticate)
        auth_token = get_auth_token(user_id)
        _log_timing("auth_token_loaded", started_at, user_id=user_id)

        # Get broker from auth table
        from database.auth_db import Auth

        auth_obj = Auth.query.filter_by(name=user_id).first()
        broker = auth_obj.broker if auth_obj else None
        _log_timing("broker_loaded", started_at, user_id=user_id, broker=broker)

        if not api_key or not auth_token or not broker:
            logger.error(
                f"Cannot execute pending order {pending_order_id}: missing api_key, auth_token, or broker"
            )
            update_broker_status(pending_order_id, None, "rejected")
            return False, {"status": "error", "message": "Authentication failed"}, 403

        # Route to appropriate service based on api_type
        success = False
        response_data = {}
        status_code = 500

        logger.info(
            f"Executing approved order: pending_order_id={pending_order_id}, api_type={api_type}, user={user_id}"
        )
        logger.debug(f"Order data keys: {list(order_data.keys())}")
        logger.debug(f"Has apikey in order_data: {'apikey' in order_data}")

        try:
            # Pass api_key, auth_token, and broker to:
            # 1. Include apikey in order_data for validation and broker functions
            # 2. Skip routing check (because auth_token and broker are present)
            # 3. Execute order with proper authentication

            if api_type == "placeorder":
                from services.place_order_service import place_order

                place_started_at = time.perf_counter()
                success, response_data, status_code = place_order(
                    order_data=order_data, api_key=api_key, auth_token=auth_token, broker=broker
                )
                _log_timing(
                    "place_order_complete",
                    place_started_at,
                    pending_order_id=pending_order_id,
                    success=success,
                    status_code=status_code,
                )

            elif api_type == "smartorder":
                from services.place_smart_order_service import place_smart_order

                success, response_data, status_code = place_smart_order(
                    order_data=order_data, api_key=api_key, auth_token=auth_token, broker=broker
                )

            elif api_type == "basketorder":
                from services.basket_order_service import place_basket_order

                success, response_data, status_code = place_basket_order(
                    basket_data=order_data, api_key=api_key, auth_token=auth_token, broker=broker
                )

            elif api_type == "splitorder":
                from services.split_order_service import split_order

                logger.info(
                    f"Calling split_order with api_key={api_key[:8]}..., auth_token={'present' if auth_token else 'None'}, broker={broker}"
                )
                success, response_data, status_code = split_order(
                    split_data=order_data, api_key=api_key, auth_token=auth_token, broker=broker
                )
                logger.info(
                    f"Split order result: success={success}, status={status_code}, response={response_data}"
                )

            elif api_type == "optionsorder":
                from services.place_options_order_service import place_options_order

                logger.info(
                    f"Calling place_options_order with api_key={api_key[:8]}..., auth_token={'present' if auth_token else 'None'}, broker={broker}"
                )
                success, response_data, status_code = place_options_order(
                    options_data=order_data, api_key=api_key, auth_token=auth_token, broker=broker
                )
                logger.info(
                    f"Options order result: success={success}, status={status_code}, response={response_data}"
                )

            else:
                logger.error(f"Unknown api_type: {api_type}")
                update_broker_status(pending_order_id, None, "rejected")
                return False, {"status": "error", "message": f"Unknown order type: {api_type}"}, 400

            # Update pending order with broker response
            if success and "orderid" in response_data:
                broker_order_id = response_data["orderid"]

                # Persist the execution immediately, then reconcile the final broker
                # status in the background to keep the approval response fast.
                update_broker_status(pending_order_id, broker_order_id, "open")
                _log_timing(
                    "pending_order_status_updated",
                    started_at,
                    pending_order_id=pending_order_id,
                    broker_order_id=broker_order_id,
                )
                logger.info(
                    f"Order executed successfully: pending_order_id={pending_order_id}, "
                    f"broker_order_id={broker_order_id}"
                )

                socketio.start_background_task(
                    _refresh_pending_order_status,
                    pending_order_id,
                    broker_order_id,
                    api_key,
                    auth_token,
                    broker,
                    user_id,
                )
                _log_timing(
                    "background_status_refresh_queued",
                    started_at,
                    pending_order_id=pending_order_id,
                    broker_order_id=broker_order_id,
                )

            elif not success:
                # Broker rejected the order
                update_broker_status(pending_order_id, None, "rejected")
                logger.warning(f"Order rejected by broker: pending_order_id={pending_order_id}")
                _log_timing(
                    "broker_rejected",
                    started_at,
                    pending_order_id=pending_order_id,
                    status_code=status_code,
                )

            return success, response_data, status_code

        except Exception as e:
            logger.exception(f"Error executing order via service: {e}")
            update_broker_status(pending_order_id, None, "rejected")
            return False, {"status": "error", "message": f"Order execution failed: {str(e)}"}, 500

    except Exception as e:
        logger.exception(f"Error in execute_approved_order: {e}")
        return False, {"status": "error", "message": f"Failed to execute order: {str(e)}"}, 500
