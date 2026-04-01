"""
Run this ONCE after deployment to add missing performance indexes.
Uses CONCURRENTLY so it does not lock the table.
Safe to run on a live database.
"""

import os

from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

indexes = [
    # Master contract - hit on every order for symbol/token lookup
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mc_exchange_symbol ON master_contract (exchange, symbol)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mc_token ON master_contract (token)",
    # API key validation
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_apikeys_key ON api_keys (api_key)",
    # Orders lookup by user
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_user_created ON orders (user_id, created_at DESC)",
    # Latency log - queried by timestamp for monitoring
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_latency_ts ON latency_log (timestamp DESC)",
    # Traffic log
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_traffic_ts ON traffic_log (timestamp DESC)",
    # Auth tokens
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_auth_tokens_user ON auth_tokens (user_id)",
]

# NOTE: CONCURRENTLY cannot run inside a transaction block
with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
    for idx_sql in indexes:
        try:
            conn.execute(text(idx_sql))
            print(f"OK: {idx_sql[:60]}...")
        except Exception as e:
            print(f"SKIP (may already exist): {e}")

print("Done. All indexes applied.")
