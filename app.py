from flask import Flask
from dotenv import load_dotenv
from datetime import timedelta
import os
import threading

from routes.public import public_bp
from routes.webhook import webhook_bp
from routes.api import api_bp
from routes.api_options import api_options_bp
from routes.dashboard import dashboard_bp

from rebalancer import run_trailing_exit_sweep, run_defensive_regime_check
from signal_scanner import run_scanner_sweep
from workers.execution_queue import start_execution_worker
from storage import init_db, init_user_table

load_dotenv("/root/tradingbot/.env", override=True)


def _trailing_exit_loop():
    import time as _time
    _time.sleep(30)  # wait 30s after startup before first sweep
    while True:
        try:
            result = run_trailing_exit_sweep()
            exits = result.get("exits", [])
            if exits:
                print(f"[trailing_exit_loop] sweep completed exits={len(exits)}")

            # Defensive regime selling/rebuying
            try:
                defensive_result = run_defensive_regime_check()
                defensive_actions = defensive_result.get("actions", [])
                if defensive_actions:
                    print(f"[defensive_regime] {len(defensive_actions)} actions regime={defensive_result.get('regime')}")
            except Exception as exc:
                print(f"[defensive_regime] error: {exc}")
        except Exception as exc:
            print(f"[trailing_exit_loop] error: {exc}")
        _time.sleep(300)  # 5 minutes


def _signal_scanner_loop():
    import time as _time
    _time.sleep(60)  # wait 1 min after startup
    while True:
        try:
            now = _time.localtime()
            # Run at :02 past each hour to let candles finalize
            if now.tm_min == 2:
                result = run_scanner_sweep()
                core = len(result.get("core_signals", []))
                sat = len(result.get("satellite_signals", []))
                scanned = result.get("products_scanned", 0)
                print(f"[signal_scanner_loop] sweep done scanned={scanned} core={core} satellite={sat}")
                _time.sleep(120)  # avoid re-trigger at :02
            else:
                _time.sleep(30)
        except Exception as exc:
            print(f"[signal_scanner_loop] error: {exc}")
            _time.sleep(60)


def create_app():
    app = Flask(__name__)

    app.secret_key = os.getenv("APP_SESSION_SECRET", "CHANGE_ME_TO_A_LONG_RANDOM_SECRET")

    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = os.getenv("SESSION_COOKIE_SECURE", "1") == "1"
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)

    init_db()
    init_user_table()
    start_execution_worker()
    _exit_thread = threading.Thread(target=_trailing_exit_loop, daemon=True, name="trailing_exit_sweep")
    _exit_thread.start()
    _scanner_thread = threading.Thread(target=_signal_scanner_loop, daemon=True, name="signal_scanner")
    _scanner_thread.start()

    app.register_blueprint(public_bp)
    app.register_blueprint(webhook_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(api_options_bp)
    app.register_blueprint(dashboard_bp)

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
