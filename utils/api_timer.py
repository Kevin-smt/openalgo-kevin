from __future__ import annotations

import logging
import time
import uuid

logger = logging.getLogger(__name__)


def _request_id_from_flask() -> str | None:
    try:
        from flask import has_request_context

        if not has_request_context():
            return None
        from utils.database_config import flask_g_safe

        request_id = getattr(flask_g_safe(), "request_id", None)
        return str(request_id) if request_id else None
    except Exception:
        return None


class APITimer:
    """
    API execution timer with checkpoint logging.
    Wraps any API handler and logs time spent at each stage.
    """

    def __init__(self, api_name: str, request_id: str | None = None):
        self.api_name = api_name
        self.request_id = request_id or _request_id_from_flask() or uuid.uuid4().hex[:8]
        self.started_at = time.perf_counter()
        self._last_checkpoint = self.started_at
        self._finished = False
        self._checkpoints: list[tuple[str, float]] = []

    def checkpoint(self, label: str) -> None:
        now = time.perf_counter()
        since_start = int((now - self.started_at) * 1000)
        since_last = int((now - self._last_checkpoint) * 1000)
        self._last_checkpoint = now
        self._checkpoints.append((label, since_last))
        logger.info(
            f"[TIMING] {self.api_name} | req={self.request_id} | checkpoint={label} "
            f"| since_start={since_start}ms | since_last={since_last}ms"
        )

    def finish(self, status: str = "success") -> None:
        if self._finished:
            return
        self._finished = True
        total_ms = int((time.perf_counter() - self.started_at) * 1000)
        if self._checkpoints:
            slowest_label, slowest_ms = max(self._checkpoints, key=lambda item: item[1])
        else:
            slowest_label, slowest_ms = "none", 0
        logger.info(
            f"[TIMING] {self.api_name} | req={self.request_id} | TOTAL={total_ms}ms "
            f"| status={status} | slowest_checkpoint={slowest_label}({slowest_ms}ms)"
        )
