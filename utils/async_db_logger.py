import logging
import queue
import threading

logger = logging.getLogger(__name__)

_log_queue: queue.Queue = queue.Queue(maxsize=50000)
_worker_started = False
_worker_lock = threading.Lock()


def _worker():
    from database.db import Session

    while True:
        try:
            item = _log_queue.get(timeout=2)
            if item is None:
                break
            table_class, data = item
            try:
                session = Session()
                session.add(table_class(**data))
                session.commit()
            except Exception as e:
                logger.debug(f"Async log write failed (non-critical): {e}")
                try:
                    Session.remove()
                except Exception:
                    pass
        except queue.Empty:
            continue


def ensure_worker_running():
    global _worker_started
    if _worker_started:
        return
    with _worker_lock:
        if not _worker_started:
            t = threading.Thread(target=_worker, name="AsyncDBLogger", daemon=True)
            t.start()
            _worker_started = True


def async_log(table_class, data: dict):
    """
    Queue a DB write. Never blocks. Safe to call from any request thread.
    If queue is full (system overloaded), the log entry is silently dropped.
    A dropped log is always preferable to blocking a trade.
    """
    ensure_worker_running()
    try:
        _log_queue.put_nowait((table_class, data))
    except queue.Full:
        pass


def get_queue_status() -> dict:
    return {
        "log_queue_depth": _log_queue.qsize(),
        "log_queue_maxsize": _log_queue.maxsize,
        "log_queue_pct": round(_log_queue.qsize() / _log_queue.maxsize, 4)
        if _log_queue.maxsize
        else 0.0,
        "worker_alive": _worker_started,
    }
