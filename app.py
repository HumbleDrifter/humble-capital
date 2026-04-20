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
from signal_scanner import run_scanner_sweep, run_dip_detector
from futures.executor import run_futures_scan_and_execute, run_futures_position_monitor
from options.executor import run_options_scan_and_execute, run_options_position_monitor
from stocks.executor import run_stock_scan_and_execute, run_stock_position_monitor
from services.config_proposal_service import (
    evaluate_auto_draft_review_proposals,
    approve_config_proposal,
)
from storage import list_pending_config_proposals
from workers.execution_queue import start_execution_worker
from storage import init_db, init_user_table

load_dotenv("/root/tradingbot/.env", override=True)


def _trailing_exit_loop():
    import time as _time
    print("[trailing_exit_loop] thread started", flush=True)
    _time.sleep(30)  # wait 30s after startup before first sweep
    while True:
        try:
            result = run_trailing_exit_sweep()
            exits = result.get("exits", [])
            if exits:
                print(f"[trailing_exit_loop] sweep completed exits={len(exits)}", flush=True)

            # Defensive regime selling/rebuying
            try:
                defensive_result = run_defensive_regime_check()
                defensive_actions = defensive_result.get("actions", [])
                if defensive_actions:
                    print(f"[defensive_regime] {len(defensive_actions)} actions regime={defensive_result.get('regime')}", flush=True)
            except Exception as exc:
                print(f"[defensive_regime] error: {exc}", flush=True)

            # Dip buy detector
            try:
                dip_result = run_dip_detector()
                dip_actions = dip_result.get("actions", [])
                if dip_actions:
                    for da in dip_actions:
                        print(
                            f"[dip_detector] BUY {da['product_id']} drop={da['change_24h']}% "
                            f"rsi={da['rsi']} amount=${da['amount']:.2f}",
                            flush=True,
                        )
            except Exception as exc:
                print(f"[dip_detector] error: {exc}", flush=True)

            # Futures position monitor (stop loss / take profit)
            try:
                futures_monitor = run_futures_position_monitor()
                futures_closes = futures_monitor.get("actions", [])
                if futures_closes:
                    for fc in futures_closes:
                        print(
                            f"[futures_monitor] CLOSE {fc['side'].upper()} {fc['product_id']} "
                            f"reason={fc['reason']} pnl={fc['pnl']:.2f}",
                            flush=True,
                        )
            except Exception as exc:
                print(f"[futures_monitor] error: {exc}", flush=True)

            # Options position monitor (profit targets / stop losses)
            try:
                options_monitor = run_options_position_monitor()
                options_closes = options_monitor.get("actions", [])
                if options_closes:
                    for oc in options_closes:
                        print(
                            f"[options_monitor] CLOSE {oc['symbol']} "
                            f"reason={oc['reason']} pnl={oc['pnl']:.2f} ({oc['pnl_pct']:.1f}%)",
                            flush=True,
                        )
            except Exception as exc:
                print(f"[options_monitor] error: {exc}", flush=True)

            # Stock position monitor (stop loss / take profit)
            try:
                stock_monitor = run_stock_position_monitor()
                stock_closes = stock_monitor.get("actions", [])
                if stock_closes:
                    for sc in stock_closes:
                        print(
                            f"[stock_monitor] CLOSE {sc['symbol']} "
                            f"reason={sc['reason']} pnl={sc['pnl_pct']:.1f}%",
                            flush=True,
                        )
            except Exception as exc:
                print(f"[stock_monitor] error: {exc}", flush=True)
        except Exception as exc:
            print(f"[trailing_exit_loop] error: {exc}", flush=True)
        _time.sleep(300)  # 5 minutes


def _signal_scanner_loop():
    import time as _time
    print("[signal_scanner_loop] thread started", flush=True)
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
                print(f"[signal_scanner_loop] sweep done scanned={scanned} core={core} satellite={sat}", flush=True)
                _time.sleep(120)  # avoid re-trigger at :02
            else:
                _time.sleep(30)
        except Exception as exc:
            print(f"[signal_scanner_loop] error: {exc}", flush=True)
            _time.sleep(60)


def _config_proposal_loop():
    import time as _time
    print("[config_proposal_loop] thread started", flush=True)
    _time.sleep(300)  # wait 5 min after startup
    while True:
        try:
            # Config proposal auto-draft disabled — APEX agent handles proposals
            _auto_draft = True
            try:
                import json as _j
                with open("/root/tradingbot/asset_config.json") as _f:
                    _cfg = _j.load(_f)
                _auto_draft = bool(_cfg.get("auto_trading", {}).get("auto_draft_config_proposals", False))
            except Exception:
                pass

            if not _auto_draft:
                _time.sleep(21600)
                continue

            result = evaluate_auto_draft_review_proposals()
            status = result.get("status", "")
            if status == "manual_mode":
                _time.sleep(3600)
                continue
            if status == "drafted":
                print(f"[config_proposal_loop] new proposal drafted", flush=True)

            # Auto-approve pending proposals — check config for auto_apply setting
            _auto_apply = True
            try:
                import json as _j
                with open("/root/tradingbot/asset_config.json") as _f:
                    _cfg = _j.load(_f)
                _auto_apply = bool(_cfg.get("auto_trading", {}).get("auto_apply_config_proposals", True))
            except Exception:
                pass

            if not _auto_apply:
                _time.sleep(21600)
                continue

            pending = list_pending_config_proposals() or []
            for proposal in pending:
                proposal_id = str(proposal.get("proposal_id") or "").strip()
                proposal_type = str(proposal.get("proposal_type") or "").strip()
                if not proposal_id:
                    continue

                approve_result = approve_config_proposal(
                    proposal_id=proposal_id,
                    actor="auto_loop",
                )
                if approve_result.get("ok"):
                    auto_applied = approve_result.get("auto_apply_ok", False)
                    print(
                        f"[config_proposal_loop] approved proposal={proposal_id} "
                        f"auto_applied={auto_applied} "
                        f"config_changed={approve_result.get('config_changed', False)}",
                        flush=True,
                    )
                else:
                    print(
                        f"[config_proposal_loop] approve failed proposal={proposal_id} "
                        f"reason={approve_result.get('reason')}",
                        flush=True,
                    )

        except Exception as exc:
            print(f"[config_proposal_loop] error: {exc}", flush=True)

        # Run every 6 hours
        _time.sleep(21600)



def _telegram_polling_loop():
    """Telegram polling — handles APPROVE/REJECT and commands."""
    from telegram_bot import run_polling_loop
    run_polling_loop()


def _agent_loop():
    """AI agent loop — runs analysis on schedule during market hours."""
    import time as _time
    from datetime import datetime, timezone, timedelta
    print("[agent_loop] thread started", flush=True)
    _time.sleep(120)  # wait 2 min after startup
    while True:
        try:
            from agent import _is_enabled, run_agent_cycle, _agent_config
            if _is_enabled():
                et_offset = timedelta(hours=-4)
                now = datetime.now(timezone(et_offset))
                # Only run during market hours Mon-Fri 9:30-16:00 ET
                is_market = (
                    now.weekday() < 5 and
                    (now.hour > 9 or (now.hour == 9 and now.minute >= 30)) and
                    now.hour < 16
                )
                if is_market:
                    run_agent_cycle()
                else:
                    print("[agent_loop] market closed — skipping", flush=True)
            else:
                print("[agent_loop] agent disabled — sleeping", flush=True)
        except Exception as exc:
            print(f"[agent_loop] error: {exc}", flush=True)
        # Sleep for configured interval (default 30 min)
        try:
            from agent import _agent_config
            mins = int(_agent_config().get("schedule_minutes", 30))
        except Exception:
            mins = 30
        _time.sleep(mins * 60)


def _daily_summary_loop():
    """Fires daily at 4:05 PM ET on weekdays."""
    import time as _time
    from datetime import datetime, timezone, timedelta
    print("[daily_summary_loop] thread started", flush=True)
    _time.sleep(60)
    _last_sent_date = None
    while True:
        try:
            et_offset = timedelta(hours=-4)
            now = datetime.now(timezone(et_offset))
            if (now.weekday() < 5 and
                now.hour == 16 and now.minute == 5 and
                now.date() != _last_sent_date):
                from notify import send_daily_summary
                send_daily_summary()
                _last_sent_date = now.date()
                print(f"[daily_summary_loop] summary sent for {_last_sent_date}", flush=True)
        except Exception as exc:
            print(f"[daily_summary_loop] error: {exc}", flush=True)
        _time.sleep(30)

def _stocks_execution_loop():
    import time as _time
    print("[stocks_execution_loop] thread started", flush=True)
    _time.sleep(150)  # wait 2.5 min after startup
    while True:
        try:
            now = _time.localtime()
            # Run at :47 past each hour (offset: crypto :02, options :17, futures :32)
            if now.tm_min == 47:
                result = run_stock_scan_and_execute()
                executed = result.get("executed", [])
                skipped = result.get("skipped", False)
                if skipped:
                    print(f"[stocks_execution_loop] skipped reason={result.get('reason')}", flush=True)
                elif executed:
                    for trade in executed:
                        print(
                            f"[stocks_execution_loop] BUY {trade['symbol']} "
                            f"score={trade['score']:.1f} qty={trade['qty']} "
                            f"price=${trade['price']:.2f} order_id={trade['order_id']}",
                            flush=True,
                        )
                else:
                    print(
                        f"[stocks_execution_loop] scan done executed=0 "
                        f"scanned={result.get('opportunities_scanned',0)} regime={result.get('regime')}",
                        flush=True,
                    )
                _time.sleep(120)  # avoid re-trigger at :47
            else:
                _time.sleep(30)
        except Exception as exc:
            print(f"[stocks_execution_loop] error: {exc}", flush=True)
            _time.sleep(60)


def _options_execution_loop():
    import time as _time
    print("[options_execution_loop] thread started", flush=True)
    _time.sleep(120)  # wait 2 min after startup
    while True:
        try:
            now = _time.localtime()
            # Run at :17 past each hour (offset: crypto :02, futures :32)
            if now.tm_min == 17:
                result = run_options_scan_and_execute()
                reg = result.get("executed_regular", [])
                sqz = result.get("executed_squeeze", [])
                skipped = result.get("skipped", False)
                if skipped:
                    print(f"[options_execution_loop] skipped reason={result.get('reason')}", flush=True)
                else:
                    for trade in reg:
                        print(
                            f"[options_execution_loop] EXECUTED {trade['strategy'].upper()} "
                            f"{trade['symbol']} score={trade['score']:.1f} "
                            f"qty={trade['qty']} order_id={trade['order_id']}",
                            flush=True,
                        )
                    for trade in sqz:
                        print(
                            f"[options_execution_loop] SQUEEZE CALL {trade['symbol']} "
                            f"conviction={trade['squeeze_conviction']:.2f} "
                            f"capital={trade['capital_pct']:.0%} "
                            f"qty={trade['qty']} order_id={trade['order_id']}",
                            flush=True,
                        )
                    if not reg and not sqz:
                        print(
                            f"[options_execution_loop] scan done executed=0 regime={result.get('regime')}",
                            flush=True,
                        )
                _time.sleep(120)  # avoid re-trigger at :17
            else:
                _time.sleep(30)
        except Exception as exc:
            print(f"[options_execution_loop] error: {exc}", flush=True)
            _time.sleep(60)


def _futures_execution_loop():
    import time as _time
    print("[futures_execution_loop] thread started", flush=True)
    _time.sleep(90)  # wait 90s after startup
    while True:
        try:
            now = _time.localtime()
            if now.tm_min == 32:
                result = run_futures_scan_and_execute()
                executed = result.get("executed", [])
                skipped = result.get("skipped", False)
                if skipped:
                    print(f"[futures_execution_loop] skipped reason={result.get('reason')}", flush=True)
                elif executed:
                    for trade in executed:
                        print(
                            f"[futures_execution_loop] EXECUTED {trade['direction'].upper()} "
                            f"{trade['product_id']} score={trade['score']:.1f} "
                            f"leverage={trade['leverage']}x order_id={trade['order_id']}",
                            flush=True,
                        )
                else:
                    print(
                        f"[futures_execution_loop] scan done opportunities={result.get('opportunities_scanned', 0)} "
                        f"executed=0 regime={result.get('regime')}",
                        flush=True,
                    )
                _time.sleep(120)
            else:
                _time.sleep(30)
        except Exception as exc:
            print(f"[futures_execution_loop] error: {exc}", flush=True)
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
    _futures_thread = threading.Thread(target=_futures_execution_loop, daemon=True, name="futures_execution")
    _futures_thread.start()
    _options_thread = threading.Thread(target=_options_execution_loop, daemon=True, name="options_execution")
    _options_thread.start()
    _stocks_thread = threading.Thread(target=_stocks_execution_loop, daemon=True, name="stocks_execution")
    _stocks_thread.start()
    _proposal_thread = threading.Thread(target=_config_proposal_loop, daemon=True, name="config_proposal")
    _proposal_thread.start()
    _summary_thread = threading.Thread(target=_daily_summary_loop, daemon=True, name="daily_summary")
    _summary_thread.start()
    # Only run polling/agent in one worker to avoid duplicate messages
    import os as _os
    _worker_id = _os.getpid()
    # Use a lock file to ensure only one worker runs these
    _lock_file = "/tmp/hc_primary_worker.lock"
    _is_primary = False
    try:
        import fcntl
        _lock_fd = open(_lock_file, "w")
        fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_fd.write(str(_worker_id))
        _lock_fd.flush()
        _is_primary = True
    except (IOError, OSError):
        _is_primary = False

    if _is_primary:
        print(f"[startup] primary worker {_worker_id} — starting Telegram + agent", flush=True)
        _telegram_thread = threading.Thread(target=_telegram_polling_loop, daemon=True, name="telegram_polling")
        _telegram_thread.start()
        _agent_thread = threading.Thread(target=_agent_loop, daemon=True, name="agent_loop")
        _agent_thread.start()
    else:
        print(f"[startup] secondary worker {_worker_id} — skipping Telegram + agent", flush=True)

    app.register_blueprint(public_bp)
    app.register_blueprint(webhook_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(api_options_bp)
    app.register_blueprint(dashboard_bp)

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
