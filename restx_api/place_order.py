import os

from flask import jsonify, make_response, request
from flask_restx import Namespace, Resource, fields
from marshmallow import ValidationError

from database.db import Session
from limiter import limiter
from restx_api.schemas import OrderSchema
from services.place_order_service import place_order
from services.preflight import get_trade_preflight
from utils.api_timer import APITimer
from utils.logging import get_logger

ORDER_RATE_LIMIT = os.getenv("ORDER_RATE_LIMIT", "10 per second")
api = Namespace("place_order", description="Place Order API")

# Initialize logger
logger = get_logger(__name__)

# Initialize schema
order_schema = OrderSchema()


@api.route("/", strict_slashes=False)
class PlaceOrder(Resource):
    @limiter.limit(ORDER_RATE_LIMIT)
    def post(self):
        """Place an order with the broker"""
        timer = APITimer("placeorder")
        try:
            data = request.json

            # Validate and deserialize input
            try:
                order_data = order_schema.load(data)
            except ValidationError as err:
                error_response = {"status": "error", "message": str(err.messages)}
                timer.finish(status="error")
                return make_response(jsonify(error_response), 400)
            timer.checkpoint("request_validation")

            # Extract API key
            api_key = order_data.get("apikey", None)

            with Session() as session:
                with session.begin():
                    preflight = get_trade_preflight(api_key, session)
            timer.checkpoint("auth_and_preflight_db")

            if not preflight:
                error_response = {"status": "error", "message": "Invalid openalgo apikey"}
                timer.finish(status="error")
                return make_response(jsonify(error_response), 403)

            if preflight["action_center_mode"] == "semi_auto":
                from services.order_router_service import queue_order

                timer.checkpoint("dedup_check_db")
                success, response_data, status_code = queue_order(
                    api_key, order_data, "placeorder", user_id=preflight["user_id"]
                )
                timer.finish(status="success" if success else "error")
                return make_response(jsonify(response_data), status_code)

            # Call the service function to place the order using preflight data
            success, response_data, status_code = place_order(
                order_data=order_data,
                api_key=api_key,
                auth_token=preflight["access_token"],
                broker=preflight["broker"],
                user_id=preflight["user_id"],
                analyze_mode=preflight["analyze_mode"],
                timer=timer,
            )

            timer.finish(status="success" if success else "error")
            return make_response(jsonify(response_data), status_code)

        except Exception:
            logger.exception("An unexpected error occurred in PlaceOrder endpoint.")
            error_response = {
                "status": "error",
                "message": "An unexpected error occurred in the API endpoint",
            }
            timer.finish(status="error")
            return make_response(jsonify(error_response), 500)
