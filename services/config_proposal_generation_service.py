import os
import threading
import time

from env_runtime import load_runtime_env
from portfolio import load_asset_config
from services.config_proposal_service import (
    generate_config_proposal,
    get_config_proposal_automation_settings,
)


load_runtime_env(override=True)

AUTO_INTERVAL_SEC = max(30, int(float(os.getenv("CONFIG_PROPOSAL_AUTO_INTERVAL_SEC", "300") or "300")))

_WORKER_STARTED = False
_WORKER_LOCK = threading.Lock()
_LAST_LOG_KEY = None


def _log_worker(message):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [config_proposal_worker] {message}")


def _log_once(key, message):
    global _LAST_LOG_KEY
    if key == _LAST_LOG_KEY:
        return
    _LAST_LOG_KEY = key
    _log_worker(message)


def _worker_loop():
    while True:
        try:
            config = load_asset_config() or {}
            settings = get_config_proposal_automation_settings(config)
            if settings.get("generation_mode") != "auto":
                _log_once("manual_mode", "skipped generation because mode is manual")
            else:
                result = generate_config_proposal()
                status = str(result.get("status", "unknown") or "unknown").strip().lower()
                if status == "created":
                    _log_once(f"created:{result.get('proposal_id')}", f"created proposal {result.get('proposal_id')}")
                elif status == "deduped":
                    _log_once(f"deduped:{result.get('proposal_id')}", "skipped generation because matching pending proposal already exists")
                elif status == "noop":
                    _log_once("noop", "skipped generation because no new allowlisted changes qualified")
                else:
                    _log_once(f"status:{status}", f"generation returned status {status}")
        except Exception as exc:
            _log_once(f"error:{exc}", f"generation loop error: {exc}")

        time.sleep(AUTO_INTERVAL_SEC)


def start_config_proposal_generation_worker():
    global _WORKER_STARTED

    with _WORKER_LOCK:
        if _WORKER_STARTED:
            return

        thread = threading.Thread(target=_worker_loop, daemon=True)
        thread.start()
        _WORKER_STARTED = True
        _log_worker(f"started with interval {AUTO_INTERVAL_SEC}s")
