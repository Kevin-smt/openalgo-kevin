#!/usr/bin/env python3
"""Measure end-to-end basket order latency against a running OpenAlgo instance."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class Snapshot:
    pool: dict[str, Any]
    queue: dict[str, Any]


def _stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {"min": 0.0, "max": 0.0, "p50": 0.0, "p95": 0.0}
    ordered = sorted(values)
    p50 = statistics.median(ordered)
    if len(ordered) == 1:
        p95 = ordered[0]
    else:
        idx_95 = max(0, min(len(ordered) - 1, round((len(ordered) - 1) * 0.95)))
        p95 = ordered[idx_95]
    return {
        "min": min(ordered),
        "max": max(ordered),
        "p50": float(p50),
        "p95": float(p95),
    }


def _get_json(url: str, timeout: float = 5.0) -> dict[str, Any]:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _snapshot(base_url: str) -> Snapshot:
    pool = _get_json(f"{base_url}/system/pool-status")
    queue = _get_json(f"{base_url}/system/queue-status")
    return Snapshot(pool=pool, queue=queue)


def _basket_payload(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "apikey": args.api_key,
        "strategy": args.strategy,
        "orders": [
            {
                "exchange": args.exchange,
                "symbol": args.symbol_1,
                "action": args.action_1,
                "quantity": args.quantity_1,
                "pricetype": args.pricetype,
                "product": args.product,
                "price": args.price,
                "trigger_price": args.trigger_price,
                "disclosed_quantity": args.disclosed_quantity,
            },
            {
                "exchange": args.exchange,
                "symbol": args.symbol_2,
                "action": args.action_2,
                "quantity": args.quantity_2,
                "pricetype": args.pricetype,
                "product": args.product,
                "price": args.price,
                "trigger_price": args.trigger_price,
                "disclosed_quantity": args.disclosed_quantity,
            },
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.getenv("OPENALGO_BASE_URL", "http://127.0.0.1:5000"))
    parser.add_argument("--api-key", default=os.getenv("OPENALGO_API_KEY"), required=False)
    parser.add_argument("--strategy", default=os.getenv("OPENALGO_BASKET_STRATEGY", "Latency Test"))
    parser.add_argument("--exchange", default=os.getenv("OPENALGO_BASKET_EXCHANGE", "NSE"))
    parser.add_argument("--symbol-1", default=os.getenv("OPENALGO_BASKET_SYMBOL_1", "RELIANCE"))
    parser.add_argument("--symbol-2", default=os.getenv("OPENALGO_BASKET_SYMBOL_2", "TCS"))
    parser.add_argument("--action-1", default=os.getenv("OPENALGO_BASKET_ACTION_1", "BUY"))
    parser.add_argument("--action-2", default=os.getenv("OPENALGO_BASKET_ACTION_2", "SELL"))
    parser.add_argument("--quantity-1", type=float, default=float(os.getenv("OPENALGO_BASKET_QTY_1", "1")))
    parser.add_argument("--quantity-2", type=float, default=float(os.getenv("OPENALGO_BASKET_QTY_2", "1")))
    parser.add_argument("--pricetype", default=os.getenv("OPENALGO_BASKET_PRICETYPE", "MARKET"))
    parser.add_argument("--product", default=os.getenv("OPENALGO_BASKET_PRODUCT", "MIS"))
    parser.add_argument("--price", type=float, default=float(os.getenv("OPENALGO_BASKET_PRICE", "0")))
    parser.add_argument("--trigger-price", type=float, default=float(os.getenv("OPENALGO_BASKET_TRIGGER_PRICE", "0")))
    parser.add_argument(
        "--disclosed-quantity",
        type=int,
        default=int(os.getenv("OPENALGO_BASKET_DISCLOSED_QTY", "0")),
    )
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    if not args.api_key:
        print("Missing API key. Set OPENALGO_API_KEY or pass --api-key.", file=sys.stderr)
        return 2

    url = f"{args.base_url.rstrip('/')}/api/v1/basketorder"
    payload = _basket_payload(args)

    print("Basket payload:")
    print(json.dumps(payload, indent=2))

    before = _snapshot(args.base_url.rstrip("/"))
    print("Pool before:")
    print(json.dumps(before.pool, indent=2))
    print("Queue before:")
    print(json.dumps(before.queue, indent=2))

    latencies: list[float] = []
    over_one_second = 0
    failures = 0

    session = requests.Session()
    for i in range(args.count):
        start = time.perf_counter()
        try:
            response = session.post(url, json=payload, timeout=args.timeout)
            elapsed = (time.perf_counter() - start) * 1000
            latencies.append(elapsed)
            if elapsed > 1000:
                over_one_second += 1
            print(f"Request {i + 1}: {response.status_code} {elapsed:.2f} ms")
            if response.status_code >= 400:
                failures += 1
                print(response.text)
        except Exception as exc:
            elapsed = (time.perf_counter() - start) * 1000
            latencies.append(elapsed)
            over_one_second += 1
            failures += 1
            print(f"Request {i + 1}: ERROR after {elapsed:.2f} ms -> {exc}")

    after = _snapshot(args.base_url.rstrip("/"))
    print("Pool after:")
    print(json.dumps(after.pool, indent=2))
    print("Queue after:")
    print(json.dumps(after.queue, indent=2))

    summary = _stats(latencies)
    print("Latency summary:")
    print(json.dumps(summary, indent=2))
    print(f"Requests over 1s: {over_one_second}")
    print(f"Failures: {failures}")

    return 0 if failures == 0 and over_one_second == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
