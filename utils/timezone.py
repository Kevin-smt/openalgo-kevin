"""Shared IST timezone helpers for runtime code.

These helpers make the application explicit about Asia/Kolkata handling and
avoid relying on the server default timezone.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime

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
