"""Shared IST timezone helpers for runtime code.

These helpers make the application explicit about Asia/Kolkata handling and
avoid relying on the server default timezone.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime
from typing import Literal

try:
    from zoneinfo import ZoneInfo

    IST = ZoneInfo("Asia/Kolkata")
except Exception:  # pragma: no cover - fallback for older environments
    import pytz

    IST = pytz.timezone("Asia/Kolkata")

IST_NAME = "Asia/Kolkata"


def configure_process_timezone() -> None:
    """Force the current process to use IST for local-time operations."""
    if os.environ.get("TZ") != IST_NAME:
        os.environ["TZ"] = IST_NAME
        if hasattr(time, "tzset"):
            try:
                time.tzset()
            except Exception:
                pass


def now_ist() -> datetime:
    """Return the current time in IST as an aware datetime."""
    return datetime.now(IST)


def ensure_ist(value: datetime | None) -> datetime | None:
    """Normalize a datetime to IST, preserving None."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=IST)
    return value.astimezone(IST)


def normalize_datetime(
    value: datetime | None,
    *,
    tz: object = IST,
    strategy: Literal["aware", "naive"] = "aware",
) -> datetime | None:
    """
    Normalize a datetime for safe comparison.

    Args:
        value: Input datetime.
        tz: Target timezone for aware normalization.
        strategy: "aware" returns a timezone-aware datetime in `tz`;
                  "naive" strips tzinfo after conversion to `tz`.
    """
    if value is None:
        return None

    normalized = value if value.tzinfo is not None else value.replace(tzinfo=IST)
    normalized = normalized.astimezone(tz)
    if strategy == "naive":
        return normalized.replace(tzinfo=None)
    return normalized


def normalize_ist(value: datetime | None, *, strategy: Literal["aware", "naive"] = "aware") -> datetime | None:
    """Normalize a datetime to IST using the shared app timezone."""
    return normalize_datetime(value, tz=IST, strategy=strategy)


def compare_aware_datetimes(left: datetime | None, right: datetime | None) -> int | None:
    """
    Compare two datetimes after normalizing both to aware IST values.

    Returns:
        1 if left > right, 0 if equal, -1 if left < right, None if either value is None.
    """
    if left is None or right is None:
        return None

    left_norm = normalize_ist(left)
    right_norm = normalize_ist(right)
    if left_norm > right_norm:
        return 1
    if left_norm < right_norm:
        return -1
    return 0


def utc_to_ist(value: datetime | None) -> datetime | None:
    """Convert a UTC datetime to IST if possible."""
    if value is None:
        return None
    return ensure_ist(value)


def ist_midnight(day: date) -> datetime:
    """Return the IST midnight for a calendar date."""
    return datetime(day.year, day.month, day.day, tzinfo=IST)


def today_ist() -> date:
    """Return today's calendar date in IST."""
    return now_ist().date()


def to_ist_iso(value: datetime | None) -> str | None:
    """Return an ISO-8601 string in IST."""
    normalized = ensure_ist(value)
    return normalized.isoformat() if normalized else None
