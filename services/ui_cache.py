import threading
import time
import traceback


class UICache:
    def __init__(self):
        self._lock = threading.Lock()
        self._data = {}
        self._meta = {}

    def set(self, key, value):
        with self._lock:
            self._data[key] = value
            self._meta[key] = {
                "updated_at": int(time.time()),
                "source": "background-cache",
            }

    def get(self, key, max_age=None):
        with self._lock:
            if key not in self._data:
                return None

            meta = self._meta.get(key, {})
            updated_at = meta.get("updated_at", 0)

            if max_age is not None:
                age = int(time.time()) - int(updated_at)
                if age > max_age:
                    return None

            return {
                "value": self._data[key],
                "meta": meta,
            }

    def get_any_age(self, key):
        with self._lock:
            if key not in self._data:
                return None
            return {
                "value": self._data[key],
                "meta": self._meta.get(key, {}),
            }


ui_cache = UICache()


def safe_call(fn, default=None):
    try:
        return fn()
    except Exception:
        traceback.print_exc()
        return default


def start_ui_cache_worker(
    get_portfolio_snapshot_fn,
    portfolio_summary_fn,
    get_rebalance_preview_fn,
    get_valid_products_fn,
    interval_sec=20,
):
    def worker():
        while True:
            try:
                snapshot = safe_call(get_portfolio_snapshot_fn, default={}) or {}
                summary = safe_call(lambda: portfolio_summary_fn(snapshot), default={}) or {}
                rebalance = safe_call(get_rebalance_preview_fn, default={}) or {}
                valid_products = safe_call(get_valid_products_fn, default=[]) or []

                ui_cache.set("portfolio_snapshot", snapshot)
                ui_cache.set("portfolio_summary", summary)
                ui_cache.set("rebalance_preview", rebalance)
                ui_cache.set("valid_products", valid_products)

            except Exception:
                traceback.print_exc()

            time.sleep(interval_sec)

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    return t
