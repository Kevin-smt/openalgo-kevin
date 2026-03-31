#!/usr/bin/env python3
"""Concurrent PostgreSQL pool stress test for OpenAlgo.

Runs a burst of parallel `get_symbol_info` lookups to verify:
- pooled engine reuse
- no session leaks in hot-path lookups
- low latency under concurrent access
- zero connection exhaustion errors
"""

from __future__ import annotations

import argparse
import math
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy.exc import SQLAlchemyError


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[int(rank)]
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def main() -> int:
    parser = argparse.ArgumentParser(description="Stress test OpenAlgo PostgreSQL pools.")
    parser.add_argument("--threads", type=int, default=50, help="Concurrent worker threads")
    parser.add_argument(
        "--queries-per-thread", type=int, default=100, help="Queries executed by each worker"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=200,
        help="How many symbols to sample from the database before the benchmark",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    load_dotenv(project_root / ".env", override=False)

    from utils.database_config import configure_database_environment, get_pool_status

    configure_database_environment()

    from database.symbol import SymToken, db_session
    from database.token_db_enhanced import clear_symbol_query_caches, get_symbol_info

    clear_symbol_query_caches()

    try:
        sample_rows = (
            db_session.query(SymToken.symbol, SymToken.exchange)
            .order_by(SymToken.symbol.asc())
            .limit(args.sample_size)
            .all()
        )
    finally:
        db_session.remove()

    if not sample_rows:
        print("No symbols found. Seed the symtoken table before running this test.")
        return 1

    sample_pairs = [(row.symbol, row.exchange) for row in sample_rows]
    latencies: list[float] = []
    latencies_lock = threading.Lock()
    connection_errors = 0
    misses = 0

    def worker(worker_id: int) -> None:
        nonlocal connection_errors, misses
        local_latencies: list[float] = []
        local_errors = 0
        local_misses = 0

        for _ in range(args.queries_per_thread):
            symbol, exchange = random.choice(sample_pairs)
            started = time.perf_counter()
            try:
                result = get_symbol_info(symbol, exchange)
                if result is None:
                    local_misses += 1
            except SQLAlchemyError:
                local_errors += 1
            except Exception:
                local_errors += 1
            finally:
                local_latencies.append((time.perf_counter() - started) * 1000.0)
                db_session.remove()

        with latencies_lock:
            latencies.extend(local_latencies)
            connection_errors += local_errors
            misses += local_misses

    started_at = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = [executor.submit(worker, i) for i in range(args.threads)]
        for future in as_completed(futures):
            future.result()
    elapsed = time.perf_counter() - started_at

    p50 = percentile(latencies, 50)
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)

    print("OpenAlgo DB pool stress test")
    print(f"Threads: {args.threads}")
    print(f"Queries per thread: {args.queries_per_thread}")
    print(f"Total queries: {len(latencies)}")
    print(f"Elapsed seconds: {elapsed:.2f}")
    print(f"qps: {len(latencies) / elapsed:.2f}" if elapsed else "qps: n/a")
    print(f"p50 latency (ms): {p50:.2f}")
    print(f"p95 latency (ms): {p95:.2f}")
    print(f"p99 latency (ms): {p99:.2f}")
    print(f"Connection errors: {connection_errors}")
    print(f"Cache misses / no-result responses: {misses}")

    pool_status = get_pool_status()
    print("Final pool stats:")
    for env_var, info in pool_status.items():
        print(
            f"  {env_var}: db={info['database']} size={info['pool_size']} "
            f"checked_out={info['checked_out']} checked_in={info['checked_in']} overflow={info['overflow']}"
        )

    return 0 if connection_errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
