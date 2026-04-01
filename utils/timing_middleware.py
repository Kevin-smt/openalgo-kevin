from __future__ import annotations

import logging
import time
import uuid

logger = logging.getLogger(__name__)


def register_timing_middleware(app) -> None:
    """Register request timing middleware for `/api/v1/` routes."""

    @app.before_request
    def _api_timing_start():
        from flask import request

        if not request.path.startswith("/api/v1/"):
            return

        from utils.database_config import flask_g_safe

        g = flask_g_safe()
        request_id = uuid.uuid4().hex[:8]
        g.request_id = request_id
        g.request_started_at = time.perf_counter()
        logger.info(
            f"[API] START | req={request_id} | method={request.method} | path={request.path} | ip={request.remote_addr or 'unknown'}"
        )

    @app.after_request
    def _api_timing_end(response):
        from flask import request

        if not request.path.startswith("/api/v1/"):
            return response

        from utils.database_config import flask_g_safe

        g = flask_g_safe()
        started_at = getattr(g, "request_started_at", None)
        request_id = getattr(g, "request_id", "no-req")
        total_ms = int((time.perf_counter() - started_at) * 1000) if started_at else 0
        logger.info(
            f"[API] END   | req={request_id} | method={request.method} | path={request.path} | status={response.status_code} | total={total_ms}ms"
        )
        if total_ms > 1000:
            logger.warning(
                f"[API] SLOW  | req={request_id} | path={request.path} | total={total_ms}ms | EXCEEDED_THRESHOLD"
            )
        return response
