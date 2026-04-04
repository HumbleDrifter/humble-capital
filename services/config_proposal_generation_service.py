import os
import time

from env_runtime import load_runtime_env
from portfolio import load_asset_config
from services.config_proposal_service import (
    generate_config_proposal,
    get_config_proposal_automation_settings,
)


load_runtime_env(override=True)

AUTO_INTERVAL_SEC = max(30, int(float(os.getenv("CONFIG_PROPOSAL_AUTO_INTERVAL_SEC", "300") or "300")))

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


def run_config_proposal_generation_cycle():
    config = load_asset_config() or {}
    settings = get_config_proposal_automation_settings(config)

    if settings.get("generation_mode") != "auto":
        _log_once("manual_mode", "skipped generation because mode is manual")
        return {"ok": True, "status": "skipped_manual_mode"}

    result = generate_config_proposal(min_confidence=settings.get("min_confidence"))
    status = str(result.get("status", "unknown") or "unknown").strip().lower()

    if status == "created":
        _log_once(f"created:{result.get('proposal_id')}", f"created proposal {result.get('proposal_id')}")
    elif status == "deduped":
        _log_once(f"deduped:{result.get('proposal_id')}", "skipped generation because matching pending proposal already exists")
    elif status == "skipped_low_confidence":
        _log_once(
            f"low_confidence:{result.get('confidence')}:{result.get('required_confidence')}",
            f"skipped generation because confidence {result.get('confidence', 'low')} is below required {result.get('required_confidence', 'high')}",
        )
    elif status == "noop":
        _log_once("noop", "skipped generation because no new allowlisted changes qualified")
    else:
        _log_once(f"status:{status}", f"generation returned status {status}")

    return result


def run_config_proposal_generation_loop(interval_sec=None):
    interval = max(30, int(float(interval_sec or AUTO_INTERVAL_SEC)))
    _log_worker(f"started with interval {interval}s")

    while True:
        try:
            run_config_proposal_generation_cycle()
        except Exception as exc:
            _log_once(f"error:{type(exc).__name__}:{exc}", f"generation loop error: {exc}")

        time.sleep(interval)


def main():
    run_config_proposal_generation_loop()
