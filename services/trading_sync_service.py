"""Background live-trading mirror synchronization helpers."""

from __future__ import annotations

from utils.logging import get_logger

logger = get_logger(__name__)


def sync_live_trading_state(api_key: str | None, reason: str = "") -> None:
    """
    Best-effort background refresh of live trading state.

    The broker remains the source of truth. This helper simply invokes the live
    read services so their PostgreSQL mirror writes stay warm even if the user
    does not manually open the orderbook/positions/holdings pages.
    """
    api_key = (api_key or "").strip()
    if not api_key:
        logger.debug("Skipping trading state sync because api_key is empty")
        return

    logger.debug(f"Starting live trading state sync ({reason or 'unknown reason'})")

    # Import locally to keep the service layer dependency one-way and avoid
    # importing the heavy live-read modules unless a sync is actually needed.
    from services.holdings_service import get_holdings
    from services.orderbook_service import get_orderbook
    from services.positionbook_service import get_positionbook
    from services.tradebook_service import get_tradebook

    sync_steps = (
        ("orderbook", get_orderbook),
        ("tradebook", get_tradebook),
        ("positionbook", get_positionbook),
        ("holdings", get_holdings),
    )

    for name, fn in sync_steps:
        try:
            success, response, status_code = fn(api_key=api_key)
            if not success:
                message = response.get("message", "unknown error") if isinstance(response, dict) else "unknown error"
                logger.debug(
                    f"Live trading state sync step '{name}' returned non-success "
                    f"(status={status_code}): {message}"
                )
        except Exception as exc:
            logger.exception(f"Live trading state sync step '{name}' failed: {exc}")

    logger.debug(f"Completed live trading state sync ({reason or 'unknown reason'})")
