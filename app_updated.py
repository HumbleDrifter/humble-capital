from flask import Flask, request, jsonify, Response
from dotenv import load_dotenv
import os
import time
import json
import socket
import threading
import atexit
from datetime import datetime, timezone

from storage import init_db, get_total_realized_pnl, get_all_positions
from portfolio import get_portfolio_snapshot, portfolio_summary
from rebalancer import (
    get_rebalance_plan,
    execute_satellite_signal,
    execute_buy,
    execute_trim,
)
from reconcile import reconcile_positions
from notify import send_telegram

ENV_PATH = "/root/tradingbot/.env"
load_dotenv(ENV_PATH)

HOSTNAME = socket.gethostname()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SHARED_SECRET")
STATUS_SECRET = os.getenv("STATUS_SECRET")
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "false").lower() == "true"
MAX_ALERT_AGE_SEC = int(os.getenv("MAX_ALERT_AGE_SEC", "0"))
DAILY_LOSS_LIMIT_USD = float(os.getenv("DAILY_LOSS_LIMIT_USD", "50"))
DAILY_STATE_PATH = "/root/tradingbot/daily_state.json"
AUTO_RECONCILE = os.getenv("AUTO_RECONCILE", "true").lower() == "true"
RECONCILE_COOLDOWN_SEC = int(os.getenv("RECONCILE_COOLDOWN_SEC", "15"))

recent_orders = {}
state_lock = threading.Lock()
reconcile_lock = threading.Lock()
last_reconcile_ts = 0.0

last_webhook_info = {
    "ts": None,
    "order_id": None,
    "action": None,
    "signal_type": None,
    "product_id": None,
    "status": None,
    "error": None,
    "result": None,
}

app = Flask(__name__)
init_db()

print("Loaded .env from:", ENV_PATH)
print("Loaded webhook secret:", WEBHOOK_SECRET)
print("Loaded status secret:", STATUS_SECRET)
print("Trading enabled:", TRADING_ENABLED)
print("Auto reconcile:", AUTO_RECONCILE)


def send_startup_heartbeat():
    try:
        send_telegram(
            "🟢 Trading bot started\n"
            f"Host: {HOSTNAME}\n"
            f"Trading enabled: {TRADING_ENABLED}\n"
            f"Auto reconcile: {AUTO_RECONCILE}\n"
            f"Max alert age sec: {MAX_ALERT_AGE_SEC}\n"
            f"Daily loss limit: ${DAILY_LOSS_LIMIT_USD}"
        )
    except Exception as e:
        print("Startup heartbeat failed:", str(e))



def send_shutdown_heartbeat():
    try:
        send_telegram(f"🔴 Trading bot stopped\nHost: {HOSTNAME}")
    except Exception as e:
        print("Shutdown heartbeat failed:", str(e))


send_startup_heartbeat()
atexit.register(send_shutdown_heartbeat)


def utc_day():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")



def load_daily_state():
    if not os.path.exists(DAILY_STATE_PATH):
        return None
    with open(DAILY_STATE_PATH, "r") as f:
        return json.load(f)



def save_daily_state(state):
    with open(DAILY_STATE_PATH, "w") as f:
        json.dump(state, f)



def ensure_daily_state():
    today = utc_day()
    state = load_daily_state()

    if not state or state.get("day") != today:
        state = {
            "day": today,
            "baseline_realized_pnl": get_total_realized_pnl(),
        }
        save_daily_state(state)

    return state



def get_today_realized_pnl():
    state = ensure_daily_state()
    baseline = float(state.get("baseline_realized_pnl", 0.0) or 0.0)
    current = get_total_realized_pnl()
    return current - baseline



def buy_allowed():
    pnl = get_today_realized_pnl()
    return pnl > -DAILY_LOSS_LIMIT_USD, pnl



def update_last_webhook(**kwargs):
    with state_lock:
        for k, v in kwargs.items():
            last_webhook_info[k] = v
        last_webhook_info["ts"] = int(time.time())



def parse_timestamp(value):
    if value is None:
        return None
    try:
        ts = float(value)
        if ts > 1e12:
            ts = ts / 1000.0
        return ts
    except Exception:
        return None



def normalize_signal(data):
    action = str(data.get("action", "")).upper().strip()
    signal_type = str(data.get("signal_type", "")).upper().strip()
    product_id = str(data.get("product_id", "")).upper().strip()
    return action, signal_type, product_id



def maybe_reconcile(force=False, reason="unknown"):
    global last_reconcile_ts

    if not AUTO_RECONCILE and not force:
        return {"ok": True, "skipped": True, "reason": "auto_reconcile_disabled"}

    now = time.time()
    with reconcile_lock:
        if not force and (now - last_reconcile_ts) < RECONCILE_COOLDOWN_SEC:
            return {
                "ok": True,
                "skipped": True,
                "reason": "cooldown_active",
                "seconds_since_last": round(now - last_reconcile_ts, 2),
            }

        result = reconcile_positions()
        last_reconcile_ts = time.time()
        return {
            "ok": True,
            "forced": force,
            "reason": reason,
            "result": result,
            "ts": int(last_reconcile_ts),
        }



def _safe_refresh_snapshot(reason):
    try:
        maybe_reconcile(reason=reason)
    except Exception as e:
        print(f"Reconcile failed during {reason}: {e}")

    snapshot = get_portfolio_snapshot()
    summary = portfolio_summary(snapshot)
    return snapshot, summary



def process_alert(data):
    order_id = str(data.get("order_id", "")).strip()
    action, signal_type, product_id = normalize_signal(data)

    try:
        try:
            maybe_reconcile(reason=f"process_alert:{signal_type}:{product_id}")
        except Exception as e:
            print("Pre-trade reconcile failed:", str(e))

        allowed, today_pnl = buy_allowed()

        if signal_type in {
            "CORE_BUY_WINDOW",
            "SATELLITE_BUY",
            "SATELLITE_BUY_EARLY",
            "SATELLITE_BUY_HEAVY",
        } and not allowed:
            result = {"ok": False, "reason": f"daily_loss_limit_reached pnl={today_pnl}"}
            update_last_webhook(
                order_id=order_id,
                action=action,
                signal_type=signal_type,
                product_id=product_id,
                status="kill_switch_blocked",
                result=result,
            )
            send_telegram(
                f"🛑 Buy blocked by daily loss limit\n"
                f"Signal: {signal_type}\n"
                f"Product: {product_id}\n"
                f"PnL today: ${today_pnl:.2f}"
            )
            return

        if signal_type in {"SATELLITE_BUY_EARLY", "SATELLITE_BUY", "SATELLITE_BUY_HEAVY"}:
            result = execute_satellite_signal(product_id, signal_type=signal_type)
            update_last_webhook(
                order_id=order_id,
                action=action,
                signal_type=signal_type,
                product_id=product_id,
                status="processed" if result.get("ok") else "skipped",
                result=result,
                error=None if result.get("ok") else result.get("reason"),
            )
            return

        if signal_type == "CORE_BUY_WINDOW":
            snapshot, summary = _safe_refresh_snapshot("core_buy_window")
            amount = float(summary.get("assets", {}).get(product_id, {}).get("allowed_buy_usd", 0.0) or 0.0)
            result = execute_buy(product_id, amount, signal_type=signal_type)
            update_last_webhook(
                order_id=order_id,
                action=action,
                signal_type=signal_type,
                product_id=product_id,
                status="processed" if result.get("ok") else "skipped",
                result=result,
                error=None if result.get("ok") else result.get("reason"),
            )
            return

        if signal_type == "TRIM_TO_TARGET":
            snapshot, summary = _safe_refresh_snapshot("trim_to_target")
            trim_usd = float(summary.get("assets", {}).get(product_id, {}).get("required_trim_usd", 0.0) or 0.0)
            result = execute_trim(product_id, trim_usd, snapshot)
            update_last_webhook(
                order_id=order_id,
                action=action,
                signal_type=signal_type,
                product_id=product_id,
                status="processed" if result.get("ok") else "skipped",
                result=result,
                error=None if result.get("ok") else result.get("reason"),
            )
            return

        if signal_type == "SELL":
            sell_pct = float(data.get("sell_pct", 0.0) or 0.0)
            snapshot, _ = _safe_refresh_snapshot("sell_signal")
            current_value = float(snapshot["positions"].get(product_id, {}).get("value_total_usd", 0.0) or 0.0)
            trim_usd = current_value * sell_pct
            result = execute_trim(product_id, trim_usd, snapshot)
            update_last_webhook(
                order_id=order_id,
                action=action,
                signal_type=signal_type,
                product_id=product_id,
                status="processed" if result.get("ok") else "skipped",
                result=result,
                error=None if result.get("ok") else result.get("reason"),
            )
            return

        update_last_webhook(
            order_id=order_id,
            action=action,
            signal_type=signal_type,
            product_id=product_id,
            status="invalid_signal_type",
            error="unsupported signal type",
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        send_telegram(
            f"🔥 Background processing error\n"
            f"Order ID: {order_id}\n"
            f"Signal: {signal_type}\n"
            f"Product: {product_id}\n"
            f"Error: {str(e)}"
        )
        update_last_webhook(
            order_id=order_id,
            action=action,
            signal_type=signal_type,
            product_id=product_id,
            status="exception",
            error=str(e),
        )


@app.route("/status", methods=["GET"])
def status():
    secret = request.args.get("secret")
    if not STATUS_SECRET or secret != STATUS_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 403

    try:
        maybe_reconcile(reason="status")
    except Exception as e:
        print("Status reconcile failed:", str(e))

    snapshot = get_portfolio_snapshot()
    summary = portfolio_summary(snapshot)
    pnl = get_today_realized_pnl()
    buy_ok, _ = buy_allowed()

    return jsonify({
        "ok": True,
        "host": HOSTNAME,
        "utc_time": datetime.now(timezone.utc).isoformat(),
        "trading_enabled": TRADING_ENABLED,
        "auto_reconcile": AUTO_RECONCILE,
        "buy_allowed": buy_ok,
        "today_realized_pnl": pnl,
        "daily_loss_limit_usd": DAILY_LOSS_LIMIT_USD,
        "positions": get_all_positions(),
        "portfolio": summary,
        "last_webhook": last_webhook_info,
    })


@app.route("/reconcile", methods=["GET", "POST"])
def reconcile_now():
    secret = request.args.get("secret")
    if not STATUS_SECRET or secret != STATUS_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 403

    try:
        result = maybe_reconcile(force=True, reason="manual_endpoint")
        return jsonify({"ok": True, "status": "reconciled", "result": result}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/dashboard")
def dashboard():
    secret = request.args.get("secret")
    if not STATUS_SECRET or secret != STATUS_SECRET:
        return Response("unauthorized", status=403)

    try:
        maybe_reconcile(reason="dashboard")
    except Exception as e:
        print("Dashboard reconcile failed:", str(e))

    snapshot = get_portfolio_snapshot()
    summary = portfolio_summary(snapshot)
    plan = get_rebalance_plan()

    pnl = get_today_realized_pnl()
    buy_ok, _ = buy_allowed()

    total_value = float(summary.get("total_value_usd", 0.0) or 0.0)
    usd_cash = float(summary.get("usd_cash", 0.0) or 0.0)
    cash_weight = float(summary.get("cash_weight", 0.0) or 0.0)
    core_weight = float(summary.get("core_weight", 0.0) or 0.0)
    satellite_weight = float(summary.get("satellite_weight", 0.0) or 0.0)
    market_regime = summary.get("market_regime", "n/a")

    drawdown = float(snapshot.get("portfolio_drawdown", 0.0) or 0.0)
    peak = float(snapshot.get("portfolio_peak", total_value) or total_value)

    cash_breakdown = summary.get("cash_breakdown", {})
    usd_cash_only = float(cash_breakdown.get("USD", 0.0) or 0.0)
    usdc_cash = float(cash_breakdown.get("USDC", 0.0) or 0.0)
    usdt_cash = float(cash_breakdown.get("USDT", 0.0) or 0.0)
    dai_cash = float(cash_breakdown.get("DAI", 0.0) or 0.0)

    def badge_class(value, kind):
        if kind == "bool":
            return "good" if value else "bad"
        if kind == "regime":
            if value == "bull":
                return "good"
            if value == "neutral":
                return "warn"
            return "bad"
        if kind == "pnl":
            return "good" if value >= 0 else "bad"
        if kind == "drawdown":
            if value >= 0.20:
                return "bad"
            if value >= 0.10:
                return "warn"
            return "good"
        return "neutral"

    assets_html = ""
    for product_id, asset in sorted(summary.get("assets", {}).items()):
        cls = asset.get("class", "")
        value_total = float(asset.get("value_total_usd", 0.0) or 0.0)
        value_liquid = float(asset.get("value_liquid_usd", 0.0) or 0.0)
        value_locked = float(asset.get("value_locked_usd", 0.0) or 0.0)
        weight_total = float(asset.get("weight_total", 0.0) or 0.0)
        price_usd = float(asset.get("price_usd", 0.0) or 0.0)
        is_locked = bool(asset.get("is_locked_or_staked", False))

        details = []
        if is_locked:
            details.append("locked/staked")
        if "target_weight" in asset:
            details.append(f"target={float(asset['target_weight']):.3f}")
        if "max_weight" in asset:
            details.append(f"max={float(asset['max_weight']):.3f}")
        if "volatility_bucket" in asset:
            details.append(f"bucket={asset['volatility_bucket']}")
        if "bucket_source" in asset:
            details.append(f"source={asset['bucket_source']}")
        if asset.get("realized_volatility") is not None:
            details.append(f"vol={float(asset['realized_volatility']):.4f}")
        if "allowed_buy_usd" in asset:
            details.append(f"buy=${float(asset['allowed_buy_usd']):.2f}")
        if "required_trim_usd" in asset:
            details.append(f"trim=${float(asset['required_trim_usd']):.2f}")

        row_class = "row-core" if cls == "core" else "row-sat" if cls == "satellite_active" else "row-other"

        assets_html += f"""
        <tr class=\"{row_class}\">
            <td>{product_id}</td>
            <td>{cls}</td>
            <td>${value_total:,.2f}</td>
            <td>${value_liquid:,.2f}</td>
            <td>${value_locked:,.2f}</td>
            <td>{weight_total:.3f}</td>
            <td>${price_usd:,.6f}</td>
            <td>{" | ".join(details)}</td>
        </tr>
        """

    buy_rows = ""
    for item in plan.get("buys", []):
        buy_rows += f"""
        <tr>
            <td>{item['product_id']}</td>
            <td>{item.get('signal_type', 'BUY')}</td>
            <td>${float(item.get('amount_usd', 0.0)):.2f}</td>
            <td>{float(item.get('current_weight', 0.0)):.3f}</td>
            <td>{float(item.get('target_weight', 0.0)):.3f}</td>
        </tr>
        """

    trim_rows = ""
    for item in plan.get("trims", []):
        max_wt_display = f"{float(item.get('max_weight', 0.0)):.3f}" if "max_weight" in item else ""
        trim_rows += f"""
        <tr>
            <td>{item['product_id']}</td>
            <td>{item.get('tier', 'trim')}</td>
            <td>${float(item.get('amount_usd', 0.0)):.2f}</td>
            <td>{float(item.get('current_weight', 0.0)):.3f}</td>
            <td>{max_wt_display}</td>
        </tr>
        """

    if not buy_rows:
        buy_rows = "<tr><td colspan='5'>No buy opportunities right now.</td></tr>"
    if not trim_rows:
        trim_rows = "<tr><td colspan='5'>No trim opportunities right now.</td></tr>"

    lw = last_webhook_info

    html = f"""
    <html>
    <head>
        <title>Trading Bot Dashboard</title>
        <meta http-equiv=\"refresh\" content=\"10\">
        <style>
            body {{ font-family: Arial, sans-serif; margin: 24px; background: #111827; color: #f3f4f6; }}
            h1, h2 {{ margin-bottom: 8px; }}
            .sub {{ color: #9ca3af; margin-bottom: 18px; }}
            .grid {{ display: grid; grid-template-columns: repeat(4, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }}
            .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }}
            .card {{ background: #1f2937; border-radius: 12px; padding: 16px; box-shadow: 0 2px 8px rgba(0,0,0,0.25); }}
            .label {{ font-size: 12px; color: #9ca3af; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.04em; }}
            .value {{ font-size: 24px; font-weight: bold; }}
            .badge {{ display: inline-block; padding: 5px 10px; border-radius: 999px; font-size: 12px; font-weight: bold; }}
            .good {{ background: rgba(16,185,129,0.18); color: #34d399; }}
            .warn {{ background: rgba(245,158,11,0.18); color: #fbbf24; }}
            .bad {{ background: rgba(239,68,68,0.18); color: #f87171; }}
            .neutral {{ background: rgba(148,163,184,0.18); color: #cbd5e1; }}
            .bar-wrap {{ background: #0f172a; border-radius: 999px; overflow: hidden; height: 22px; margin-top: 8px; }}
            .bar {{ height: 100%; float: left; text-align: center; font-size: 11px; line-height: 22px; font-weight: bold; color: white; }}
            .bar-cash {{ background: #475569; }}
            .bar-core {{ background: #2563eb; }}
            .bar-sat {{ background: #7c3aed; }}
            table {{ width: 100%; border-collapse: collapse; background: #1f2937; border-radius: 12px; overflow: hidden; margin-bottom: 24px; }}
            th, td {{ text-align: left; padding: 10px 12px; border-bottom: 1px solid #374151; vertical-align: top; }}
            th {{ background: #111827; color: #d1d5db; font-size: 13px; }}
            .row-core td:first-child {{ color: #93c5fd; font-weight: bold; }}
            .row-sat td:first-child {{ color: #c4b5fd; font-weight: bold; }}
            .mono {{ font-family: Consolas, monospace; white-space: pre-wrap; }}
        </style>
    </head>
    <body>
        <h1>Trading Bot Dashboard</h1>
        <div class=\"sub\">Host: {HOSTNAME} | UTC: {datetime.now(timezone.utc).isoformat()}</div>

        <div class=\"grid\">
            <div class=\"card\"><div class=\"label\">Trading Enabled</div><div class=\"value\"><span class=\"badge {badge_class(TRADING_ENABLED, 'bool')}\">{TRADING_ENABLED}</span></div></div>
            <div class=\"card\"><div class=\"label\">Buy Allowed</div><div class=\"value\"><span class=\"badge {badge_class(buy_ok, 'bool')}\">{buy_ok}</span></div></div>
            <div class=\"card\"><div class=\"label\">Market Regime</div><div class=\"value\"><span class=\"badge {badge_class(market_regime, 'regime')}\">{market_regime}</span></div></div>
            <div class=\"card\"><div class=\"label\">Today Realized PnL</div><div class=\"value\"><span class=\"badge {badge_class(pnl, 'pnl')}\">${pnl:,.2f}</span></div></div>
            <div class=\"card\"><div class=\"label\">Portfolio Value</div><div class=\"value\">${total_value:,.2f}</div></div>
            <div class=\"card\"><div class=\"label\">Cash</div><div class=\"value\">${usd_cash:,.2f}</div></div>
            <div class=\"card\"><div class=\"label\">Portfolio Peak</div><div class=\"value\">${peak:,.2f}</div></div>
            <div class=\"card\"><div class=\"label\">Drawdown</div><div class=\"value\"><span class=\"badge {badge_class(drawdown, 'drawdown')}\">{drawdown:.2%}</span></div></div>
        </div>

        <div class=\"two-col\">
            <div class=\"card\">
                <h2 style=\"margin-bottom:6px;\">Allocation Mix</h2>
                <div class=\"sub\" style=\"margin-bottom:8px;\">Cash / Core / Satellite portfolio weights</div>
                <div>Cash: {cash_weight:.3f} | Core: {core_weight:.3f} | Satellite: {satellite_weight:.3f}</div>
                <div class=\"bar-wrap\">
                    <div class=\"bar bar-cash\" style=\"width:{cash_weight*100:.2f}%;\">Cash</div>
                    <div class=\"bar bar-core\" style=\"width:{core_weight*100:.2f}%;\">Core</div>
                    <div class=\"bar bar-sat\" style=\"width:{satellite_weight*100:.2f}%;\">Satellite</div>
                </div>
                <div style=\"margin-top:14px;\">Cash USD: ${usd_cash:,.2f}</div>
            </div>

            <div class=\"card mono\">
                <h2 style=\"font-family:Arial,sans-serif;margin-bottom:8px;\">Cash & Stablecoins</h2>
USD : ${usd_cash_only:,.2f}
USDC: ${usdc_cash:,.2f}
USDT: ${usdt_cash:,.2f}
DAI : ${dai_cash:,.2f}
TOTAL: ${float(summary.get('usd_cash', 0.0)):,.2f}
            </div>
        </div>

        <div class=\"two-col\">
            <div class=\"card mono\">
                <h2 style=\"font-family:Arial,sans-serif;margin-bottom:8px;\">Last Webhook</h2>
order_id: {lw.get('order_id')}
action: {lw.get('action')}
signal_type: {lw.get('signal_type')}
product_id: {lw.get('product_id')}
status: {lw.get('status')}
error: {lw.get('error')}
result: {lw.get('result')}
ts: {lw.get('ts')}
            </div>

            <div class=\"card\">
                <h2 style=\"margin-bottom:10px;\">Buy Candidates</h2>
                <table style=\"margin-bottom:0;\">
                    <thead><tr><th>Product</th><th>Type</th><th>Amount</th><th>Current Wt</th><th>Target Wt</th></tr></thead>
                    <tbody>{buy_rows}</tbody>
                </table>
            </div>
        </div>

        <div class=\"card\" style=\"margin-bottom:24px;\">
            <h2 style=\"margin-bottom:10px;\">Trim Candidates</h2>
            <table style=\"margin-bottom:0;\">
                <thead><tr><th>Product</th><th>Tier</th><th>Amount</th><th>Current Wt</th><th>Max Wt</th></tr></thead>
                <tbody>{trim_rows}</tbody>
            </table>
        </div>

        <h2 style=\"margin-bottom:10px;\">Assets</h2>
        <table>
            <thead>
                <tr>
                    <th>Product</th><th>Class</th><th>Total Value</th><th>Liquid Value</th><th>Locked/Staked Value</th><th>Weight</th><th>Price</th><th>Details</th>
                </tr>
            </thead>
            <tbody>{assets_html}</tbody>
        </table>
    </body>
    </html>
    """

    return Response(html, mimetype="text/html")


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        raw = request.get_data(as_text=True)
        print("RAW BODY:", raw)

        data = request.get_json(silent=True)
        if not data:
            update_last_webhook(status="invalid_json", error="invalid or empty JSON")
            return jsonify({"ok": False, "error": "invalid JSON"}), 400

        if data.get("secret") != WEBHOOK_SECRET:
            send_telegram("🚨 Invalid webhook secret received")
            update_last_webhook(status="invalid_secret", error="invalid secret")
            return jsonify({"ok": False, "error": "invalid secret"}), 403

        order_id = str(data.get("order_id", "")).strip()
        action, signal_type, product_id = normalize_signal(data)

        if not order_id:
            update_last_webhook(status="missing_order_id", error="missing order_id")
            return jsonify({"ok": False, "error": "missing order_id"}), 400

        now = time.time()
        with state_lock:
            if order_id in recent_orders and (now - recent_orders[order_id]) < 60:
                update_last_webhook(
                    order_id=order_id,
                    action=action,
                    signal_type=signal_type,
                    product_id=product_id,
                    status="duplicate",
                )
                return jsonify({"ok": True, "status": "duplicate"}), 200
            recent_orders[order_id] = now

        alert_ts = parse_timestamp(data.get("timestamp"))
        if MAX_ALERT_AGE_SEC > 0 and alert_ts is not None:
            age = now - alert_ts
            if age > MAX_ALERT_AGE_SEC:
                update_last_webhook(
                    order_id=order_id,
                    action=action,
                    signal_type=signal_type,
                    product_id=product_id,
                    status="stale_alert",
                    error=f"age={age:.2f}",
                )
                return jsonify({"ok": False, "error": "stale_alert"}), 400

        update_last_webhook(
            order_id=order_id,
            action=action,
            signal_type=signal_type,
            product_id=product_id,
            status="accepted",
        )

        threading.Thread(target=process_alert, args=(data,), daemon=True).start()

        return jsonify({
            "ok": True,
            "status": "accepted",
            "order_id": order_id,
            "action": action,
            "signal_type": signal_type,
            "product_id": product_id,
        }), 200

    except Exception as e:
        import traceback
        traceback.print_exc()
        update_last_webhook(status="exception", error=str(e))
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/")
def home():
    return Response(
        "Trading bot online. Use /status?secret=... , /dashboard?secret=... , /reconcile?secret=...",
        mimetype="text/plain",
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
