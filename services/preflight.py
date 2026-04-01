from __future__ import annotations

import hashlib
import time

from argon2.exceptions import VerifyMismatchError
from sqlalchemy import text

from database.auth_db import PEPPER, decrypt_token, ph


def get_trade_preflight(api_key: str, session) -> dict | None:
    """
    Returns all data needed before placing a trade in a single DB round trip.
    Fields: user_id, broker, access_token, is_active, analyze_mode, action_center_mode
    Returns None if api_key is invalid or inactive.
    """

    if not api_key:
        return None

    started_at = time.perf_counter()
    api_key_sha256 = hashlib.sha256(api_key.encode()).hexdigest()

    query = text(
        """
        SELECT
            ak.user_id AS user_id,
            ak.api_key_hash AS api_key_hash,
            a.broker AS broker,
            a.auth AS encrypted_access_token,
            CASE WHEN a.is_revoked THEN 0 ELSE 1 END AS is_active,
            s.analyze_mode AS analyze_mode,
            COALESCE(ak.order_mode, 'auto') AS action_center_mode
        FROM api_keys ak
        JOIN auth a ON a.name = ak.user_id
        JOIN (
            SELECT analyze_mode
            FROM settings
            ORDER BY id
            LIMIT 1
        ) s ON 1 = 1
        WHERE ak.api_key_sha256 = :api_key_sha256
          AND a.is_revoked IS FALSE
        LIMIT 1
        """
    )

    row = session.execute(query, {"api_key_sha256": api_key_sha256}).mappings().first()
    if not row:
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        from utils.logging import get_logger

        get_logger(__name__).info(f"[TIMING] preflight lookup failed | elapsed_ms={elapsed_ms}")
        return None

    if not bool(row["is_active"]):
        return None

    try:
        ph.verify(row["api_key_hash"], api_key + PEPPER)
    except VerifyMismatchError:
        return None

    access_token = decrypt_token(row["encrypted_access_token"])
    if not access_token:
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        from utils.logging import get_logger

        get_logger(__name__).info(f"[TIMING] preflight decrypt failed | elapsed_ms={elapsed_ms}")
        return None

    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    from utils.logging import get_logger

    get_logger(__name__).info(
        "[TIMING] preflight lookup complete | elapsed_ms=%s | user_id=%s | broker=%s | order_mode=%s",
        elapsed_ms,
        row["user_id"],
        row["broker"],
        row["action_center_mode"],
    )

    return {
        "user_id": row["user_id"],
        "broker": row["broker"],
        "access_token": access_token,
        "is_active": bool(row["is_active"]),
        "analyze_mode": bool(row["analyze_mode"]),
        "action_center_mode": row["action_center_mode"],
    }
