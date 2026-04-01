"""EventBus subscriber that mirrors live trading state into PostgreSQL."""

from __future__ import annotations

from services.trading_sync_service import sync_live_trading_state
from utils.logging import get_logger

logger = get_logger(__name__)


def on_live_trading_state_refresh(event) -> None:
    """Refresh mirrored order/trade/position/holding state after trade events."""
    try:
        if getattr(event, "mode", "live") != "live":
            return

        api_key = getattr(event, "api_key", "")
        if not api_key:
            logger.debug(
                f"Skipping live trading state refresh for {getattr(event, 'topic', 'unknown')} "
                "because api_key is empty"
            )
            return

        sync_live_trading_state(api_key, reason=getattr(event, "topic", ""))
    except Exception as exc:
        logger.exception(f"Trading sync subscriber failed: {exc}")
