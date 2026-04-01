import os
import traceback

from flask import jsonify, make_response, request
from flask_restx import Namespace, Resource
from marshmallow import ValidationError

from database.settings_db import get_analyze_mode
from events import OrderFailedEvent
from utils.event_bus import bus
from limiter import limiter
from restx_api.schemas import CancelOrderSchema
from services.cancel_order_service import cancel_order, emit_analyzer_error
from utils.api_timer import APITimer
from utils.logging import get_logger

ORDER_RATE_LIMIT = os.getenv("ORDER_RATE_LIMIT", "10 per second")
api = Namespace("cancel_order", description="Cancel Order API")

# Initialize logger
logger = get_logger(__name__)

# Initialize schema
cancel_order_schema = CancelOrderSchema()


@api.route("/", strict_slashes=False)
class CancelOrder(Resource):
    @limiter.limit(ORDER_RATE_LIMIT)
    def post(self):
        """Cancel an existing order"""
        timer = APITimer("cancelorder")
        try:
            data = request.json

            # Validate and deserialize input
            try:
                order_data = cancel_order_schema.load(data)
            except ValidationError as err:
                error_message = str(err.messages)
                if get_analyze_mode():
                    timer.finish(status="error")
                    return make_response(jsonify(emit_analyzer_error(data, error_message)), 400)
                error_response = {"status": "error", "message": error_message}
                bus.publish(OrderFailedEvent(
                    mode="live",
                    api_type="cancelorder",
                    request_data=data,
                    response_data=error_response,
                    error_message=error_message,
                ))
                timer.finish(status="error")
                return make_response(jsonify(error_response), 400)
            timer.checkpoint("request_validation")

            # Extract API key and order ID
            api_key = order_data.pop("apikey", None)
            orderid = order_data.get("orderid")
            timer.checkpoint("auth_and_preflight_db")

            # Call the service function to cancel the order
            success, response_data, status_code = cancel_order(orderid=orderid, api_key=api_key)

            timer.finish(status="success" if success else "error")
            return make_response(jsonify(response_data), status_code)

        except KeyError as e:
            missing_field = str(e)
            logger.error(f"KeyError: Missing field {missing_field}")
            error_message = f"A required field is missing: {missing_field}"
            if get_analyze_mode():
                timer.finish(status="error")
                return make_response(jsonify(emit_analyzer_error(data, error_message)), 400)
            error_response = {"status": "error", "message": error_message}
            bus.publish(OrderFailedEvent(
                mode="live",
                api_type="cancelorder",
                request_data=data,
                response_data=error_response,
                error_message=error_message,
            ))
            timer.finish(status="error")
            return make_response(jsonify(error_response), 400)

        except Exception:
            logger.error("An unexpected error occurred in CancelOrder endpoint.")
            traceback.print_exc()
            error_message = "An unexpected error occurred"
            if get_analyze_mode():
                timer.finish(status="error")
                return make_response(jsonify(emit_analyzer_error(data, error_message)), 500)
            error_response = {"status": "error", "message": error_message}
            bus.publish(OrderFailedEvent(
                mode="live",
                api_type="cancelorder",
                request_data=data,
                response_data=error_response,
                error_message=error_message,
            ))
            timer.finish(status="error")
            return make_response(jsonify(error_response), 500)
