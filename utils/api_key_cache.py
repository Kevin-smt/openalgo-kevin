import threading
import time
from typing import Any, Dict, Optional

_cache: Dict[str, Dict] = {}
_lock = threading.Lock()
TTL_SECONDS = 300  # 5 minutes


def get_cached_api_key(key_hash: str) -> Optional[Dict[str, Any]]:
    with _lock:
        entry = _cache.get(key_hash)
        if entry and (time.time() - entry["cached_at"]) < TTL_SECONDS:
            return entry["data"]
        if entry:
            del _cache[key_hash]
        return None


def set_cached_api_key(key_hash: str, data: Dict[str, Any]) -> None:
    with _lock:
        _cache[key_hash] = {"data": data, "cached_at": time.time()}


def invalidate_api_key(key_hash: str) -> None:
    with _lock:
        _cache.pop(key_hash, None)


def clear_all() -> None:
    with _lock:
        _cache.clear()
