"""Background thread that periodically checks service availability."""

import threading
import time
from datetime import datetime


def start_poller(
    load_configs_fn,
    checkers: dict,
    service_status: dict,
    status_lock: threading.Lock,
    broadcast_fn,
    interval: int = 30,
    initial_delay: int = 5,
) -> None:
    def _run() -> None:
        time.sleep(initial_delay)
        while True:
            try:
                _poll_once(load_configs_fn, checkers, service_status, status_lock, broadcast_fn)
            except Exception as exc:
                print(f"[status_poller] error: {exc}")
            time.sleep(interval)

    threading.Thread(target=_run, daemon=True).start()


def _poll_once(load_configs_fn, checkers, service_status, status_lock, broadcast_fn) -> None:
    configs = load_configs_fn()
    for svc, checker in checkers.items():
        try:
            status, message = checker(configs.get(svc, {}))
        except Exception as exc:
            status, message = "down", str(exc)[:150]
        with status_lock:
            service_status[svc] = {"status": status, "message": message}
        broadcast_fn({
            "type":    "service_status",
            "service": svc,
            "status":  status,
            "message": message,
            "ts":      datetime.utcnow().isoformat() + "Z",
        })
