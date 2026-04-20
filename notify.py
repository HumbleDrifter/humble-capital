import os
import sqlite3
from decimal import Decimal
from typing import Optional

import requests
from env_runtime import load_runtime_env

load_runtime_env(override=True)

DB_PATH = str(os.getenv("TRADINGBOT_DB_PATH", "/root/tradingbot/trading.db") or "").strip() or "/root/tradingbot/trading.db"
TELEGRAM_TIMEOUT_SEC = float(os.getenv("TELEGRAM_TIMEOUT_SEC", "10") or "10")


def _env_bool(name, default=False):
    v = str(os.getenv(name, str(default)) or "").lower().strip()
    return v in {"1", "true", "yes", "on"}


def _enabled():
    return _env_bool("TELEGRAM_ENABLED", True)


def _legacy_enabled():
    # Keep old compatibility function importable, but do not let it spam duplicates by default.
    return _env_bool("LEGACY_TELEGRAM_ENABLED", False)


def _token():
    load_runtime_env(override=True)
    return str(os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()


def _chat():
    load_runtime_env(override=True)
    return str(os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()


def _fmt_usd(v):
    try:
        value = float(v)
        if value < 0:
            return f"-${abs(value):,.2f}"
        return f"${value:,.2f}"
    except Exception:
        return "$0.00"


def _fmt_num(v):
    try:
        return format(Decimal(str(v)), "f").rstrip("0").rstrip(".")
    except Exception:
        return "0"


def _fmt_score(score, band):
    try:
        return f"{int(float(score or 0))} ({str(band or 'Moderate Risk').strip() or 'Moderate Risk'})"
    except Exception:
        return f"0 ({str(band or 'Moderate Risk').strip() or 'Moderate Risk'})"


def _send(text):
    if not _enabled():
        return False

    token = _token()
    chat = _chat()

    if not token or not chat:
        print("[notify] missing telegram config")
        return False

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat,
                "text": text,
                "disable_web_page_preview": True,
            },
            timeout=TELEGRAM_TIMEOUT_SEC,
        ).raise_for_status()
        return True
    except Exception as e:
        print("telegram error:", e)
        return False


def _has_fill(result):
    if not isinstance(result, dict):
        return False

    for a in result.get("attempts", []):
        try:
            if float(a.get("filled_base", 0) or 0) > 0:
                return True
        except Exception:
            pass

        try:
            if float(a.get("avg_fill_price", 0) or 0) > 0:
                return True
        except Exception:
            pass

        raw = a.get("raw") or {}
        status = str(raw.get("status") or a.get("status") or "").upper()
        if status == "FILLED":
            return True

    return False


def _first_filled_attempt(result):
    if not isinstance(result, dict):
        return None

    for a in result.get("attempts", []):
        try:
            if float(a.get("filled_base", 0) or 0) > 0:
                return a
        except Exception:
            pass

    for a in result.get("attempts", []):
        raw = a.get("raw") or {}
        status = str(raw.get("status") or a.get("status") or "").upper()
        if status == "FILLED":
            return a

    return None


def _latest_realized_pnl(product_id: str) -> Optional[float]:
    if not product_id or not os.path.exists(DB_PATH):
        return None

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT pnl_usd
            FROM realized_pnl
            WHERE product_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (str(product_id).upper(),),
        ).fetchone()
        conn.close()

        if not row:
            return None

        return float(row["pnl_usd"] or 0.0)
    except Exception as exc:
        print(f"[notify] latest realized pnl lookup failed for {product_id}: {exc}")
        return None


def notify_execution_result(
    product_id,
    side,
    signal_type,
    requested_usd=None,
    requested_base=None,
    result_wrapper=None,
):
    if not _has_fill(result_wrapper):
        return False

    attempt = _first_filled_attempt(result_wrapper)
    if not attempt:
        return False

    try:
        filled = float(attempt.get("filled_base", 0) or 0)
    except Exception:
        filled = 0.0

    try:
        price = float(attempt.get("avg_fill_price", 0) or 0)
    except Exception:
        price = 0.0

    notional = filled * price if filled > 0 and price > 0 else 0.0

    side = str(side or "").upper().strip()
    product_id = str(product_id or "").upper().strip()
    signal_type = str(signal_type or "").upper().strip()

    emoji = "🟢" if side == "BUY" else "🔴"

    msg = [
        f"{emoji} {side} FILLED",
        product_id,
        f"Signal: {signal_type}",
    ]

    if requested_usd:
        msg.append(f"Requested: {_fmt_usd(requested_usd)}")
    elif requested_base:
        msg.append(f"Requested Base: {_fmt_num(requested_base)}")

    if filled:
        msg.append(f"Filled: {_fmt_num(filled)}")

    if price:
        msg.append(f"Price: {_fmt_usd(price)}")

    if notional:
        msg.append(f"Value: {_fmt_usd(notional)}")

    if side == "SELL":
        pnl = _latest_realized_pnl(product_id)
        if pnl is not None:
            msg.append(f"PnL: {_fmt_usd(pnl)}")

    return _send("\n".join(msg))


def notify_execution_error(*args, **kwargs):
    return False


def notify_signal_received(*args, **kwargs):
    return False


def notify_signal_rejected(*args, **kwargs):
    return False


def render_config_proposal_text(proposal_record):
    proposal_record = proposal_record if isinstance(proposal_record, dict) else {}
    proposal = proposal_record.get("proposal", {}) if isinstance(proposal_record.get("proposal"), dict) else {}
    source = proposal.get("source", {}) if isinstance(proposal.get("source"), dict) else {}
    simulation = proposal.get("simulation", {}) if isinstance(proposal.get("simulation"), dict) else {}
    changes = proposal.get("changes", []) if isinstance(proposal.get("changes"), list) else []
    candidates = proposal.get("candidates", []) if isinstance(proposal.get("candidates"), list) else []
    order = proposal.get("order", {}) if isinstance(proposal.get("order"), dict) else {}
    proposal_type = str(proposal.get("proposal_type") or proposal_record.get("proposal_type") or "config_guardrail").strip()

    proposal_id = str(proposal_record.get("id") or proposal.get("proposal_id") or "").strip() or "CFG-UNKNOWN"
    summary = str(proposal.get("summary") or proposal_record.get("summary_text") or "Config proposal ready for review.").strip()
    confidence = str(source.get("confidence", "low") or "low").strip().lower()
    expires_at = str(proposal_record.get("expires_at", "") or "").strip() or "not set"

    if proposal_type == "satellite_enable_recommendation":
        lines = [
            "🛰️ Satellite Enable Recommendation",
            f"ID: {proposal_id}",
            summary,
            "",
            f"Confidence: {confidence}",
            f"Candidates: {len(candidates)}",
        ]
    elif proposal_type == "options_order_recommendation":
        lines = [
            "🧾 Options Order Recommendation",
            f"ID: {proposal_id}",
            summary,
            "",
            f"Broker: {str(order.get('broker', 'ibkr') or 'ibkr').strip()}",
            f"Underlying: {str(order.get('underlying', '') or '').strip().upper() or 'unknown'}",
            f"Strategy: {str(order.get('strategy', '') or '').strip() or 'unknown'}",
            f"Order Type: {str(order.get('order_type', 'LIMIT') or 'LIMIT').strip().upper()}",
            f"Limit: {_fmt_usd(order.get('limit_price', 0.0))}",
        ]
    else:
        lines = [
            "⚙️ Config Proposal",
            f"ID: {proposal_id}",
            summary,
            "",
            f"Confidence: {confidence}",
            f"Current Risk: {_fmt_score(source.get('risk_score', 0), source.get('risk_band', 'Moderate Risk'))}",
            f"Projected Risk: {_fmt_score(simulation.get('projected_score', 0), simulation.get('projected_band', 'Moderate Risk'))}",
        ]

    if proposal_type == "satellite_enable_recommendation" and candidates:
        lines.append("")
        lines.append("Recommended Candidates")
        for item in candidates[:6]:
            item = item if isinstance(item, dict) else {}
            product_id = str(item.get("product_id", "Candidate") or "Candidate").strip()
            score = float(item.get("net_score", 0.0) or 0.0)
            confidence_band = str(item.get("confidence_band", "") or "").strip() or "unknown"
            liquidity_bucket = str(item.get("liquidity_bucket", "") or "").strip() or "unknown"
            volatility_bucket = str(item.get("volatility_bucket", "") or "").strip() or "unknown"
            lines.append(f"• {product_id}: {score:.1f} · {confidence_band} confidence · {liquidity_bucket} liquidity · {volatility_bucket} volatility")
    elif proposal_type == "options_order_recommendation" and order:
        lines.append("")
        lines.append("Order Legs")
        for leg in (order.get("legs") or [])[:6]:
            leg = leg if isinstance(leg, dict) else {}
            lines.append(
                f"• {str(leg.get('side', '') or '').strip().upper()} "
                f"{_fmt_num(leg.get('quantity', 0))} "
                f"{str(leg.get('expiry', '') or '').strip()} "
                f"{_fmt_num(leg.get('strike', 0))} "
                f"{str(leg.get('right_code', leg.get('right', '')) or '').strip().upper()}"
            )
    elif changes:
        lines.append("")
        lines.append("Changed Controls")
        for item in changes[:6]:
            item = item if isinstance(item, dict) else {}
            label = str(item.get("label", item.get("key", "Control")) or "Control").strip()
            current_value = item.get("current_value")
            proposed_value = item.get("proposed_value")
            lines.append(f"• {label}: {_fmt_num(current_value)} → {_fmt_num(proposed_value)}")

    lines.extend(
        [
            "",
            f"Expires: {expires_at}",
            f"APPROVE {proposal_id}",
            f"REJECT {proposal_id}",
        ]
    )

    return "\n".join(lines).strip()


def notify_config_proposal(proposal_record):
    return _send(render_config_proposal_text(proposal_record))


def render_config_proposal_status_text(proposal_id, result, proposal_record=None):
    result = result if isinstance(result, dict) else {}
    proposal_record = proposal_record if isinstance(proposal_record, dict) else {}
    proposal = proposal_record.get("proposal") if isinstance(proposal_record.get("proposal"), dict) else {}
    proposal_type = str(proposal.get("proposal_type") or proposal_record.get("proposal_type") or "config_guardrail").strip().lower()

    if not result.get("ok"):
        return (
            f"❌ Config proposal command failed\n"
            f"ID: {proposal_id}\n"
            f"Reason: {result.get('reason')}\n"
            f"Status: {result.get('current_status', 'n/a')}"
        )

    summary = str(
        (proposal.get("summary"))
        or proposal_record.get("summary_text")
        or ""
    ).strip()

    title = "Config Proposal"
    if proposal_type == "satellite_enable_recommendation":
        title = "Satellite Enable Recommendation"
    elif proposal_type == "options_order_recommendation":
        title = "Options Order Recommendation"

    if result.get("status") == "approved":
        lines = [
            f"✅ {title} Approved",
            f"ID: {proposal_id}",
            f"Approved At: {result.get('approved_at')}",
        ]
        if result.get("approved_by"):
            lines.append(f"Approved By: {result.get('approved_by')}")
    elif result.get("status") == "applied":
        lines = [
            f"✅ {title} Applied",
            f"ID: {proposal_id}",
            f"Applied At: {result.get('applied_at')}",
        ]
        if result.get("applied_by"):
            lines.append(f"Applied By: {result.get('applied_by')}")
        if result.get("config_changed") is not None:
            lines.append(f"Config Changed: {'yes' if bool(result.get('config_changed')) else 'no'}")
    else:
        lines = [
            f"🛑 {title} Rejected",
            f"ID: {proposal_id}",
            f"Rejected At: {result.get('rejected_at')}",
        ]
        if result.get("rejected_by"):
            lines.append(f"Rejected By: {result.get('rejected_by')}")

    if summary:
        lines.extend(["", summary])

    if result.get("auto_apply_attempted") and not result.get("auto_apply_ok"):
        lines.extend([
            "",
            f"Automatic apply after approval did not complete.",
            f"Apply Reason: {result.get('auto_apply_reason', 'unknown')}",
            f"Apply Status: {result.get('auto_apply_status', 'n/a')}",
        ])

    lines.append("")
    if result.get("status") == "applied":
        lines.append("Config changes have been applied.")
    else:
        lines.append("No config changes have been applied yet.")
    return "\n".join(lines).strip()


def send_daily_summary():
    """Send end-of-day summary at market close (4:05 PM ET)."""
    try:
        import sqlite3, json, os
        from datetime import datetime, timezone, timedelta

        # Get today's crypto fills
        db_path = DB_PATH
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Today's orders (ET timezone)
        et_offset = timedelta(hours=-4)
        now_et = datetime.now(timezone(et_offset))
        today_start = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        today_ts = int(today_start.timestamp())

        cur.execute("""
            SELECT product_id, side, base_size, price, status
            FROM orders
            WHERE created_at >= ? AND status = 'FILLED'
            ORDER BY created_at DESC
        """, (today_ts,))
        fills = [dict(r) for r in cur.fetchall()]
        conn.close()

        # Get portfolio snapshot
        portfolio_val = 0.0
        day_pnl = 0.0
        positions = 0
        try:
            from portfolio import get_portfolio_snapshot
            snap = get_portfolio_snapshot()
            portfolio_val = float(snap.get("total_value_usd") or 0)
            day_pnl = float(snap.get("day_pnl_usd") or 0)
            positions = len(snap.get("positions") or {})
        except Exception:
            pass

        # Get options positions
        opt_summary = []
        try:
            from brokers.webull_adapter import WebullAdapter
            info = WebullAdapter().get_account_info()
            opts = [p for p in (info.get("positions") or [])
                    if str(p.get("asset_type","")).lower() == "option"]
            for o in opts:
                pnl = float(o.get("unrealized_pnl") or 0)
                opt_summary.append(
                    f"  {o.get('symbol')} {o.get('option_type','').upper()} "
                    f"${o.get('strike')} · {float(o.get('unrealized_pnl_pct',0)):.1f}% "
                    f"({'+'if pnl>=0 else ''}{_fmt_usd(pnl)})"
                )
        except Exception:
            pass

        # Get executor logs for today
        exec_actions = []
        try:
            from options.executor import get_executor_log
            for entry in get_executor_log(limit=100):
                msg = str(entry.get("msg",""))
                if any(k in msg for k in ["executed","closed","sold","EXIT signal","stop_loss"]):
                    exec_actions.append(f"  {msg[:80]}")
        except Exception:
            pass

        # Build message
        pnl_emoji = "🟢" if day_pnl >= 0 else "🔴"
        lines = [
            f"📊 Daily Summary — {now_et.strftime('%b %d, %Y')}",
            f"",
            f"{pnl_emoji} Day P&L: {'+' if day_pnl>=0 else ''}{_fmt_usd(day_pnl)}",
            f"💼 Portfolio: {_fmt_usd(portfolio_val)}",
            f"📍 Positions: {positions}",
        ]

        if fills:
            lines.append(f"")
            lines.append(f"📋 Today's Fills ({len(fills)}):")
            buys = [f for f in fills if f["side"]=="BUY"]
            sells = [f for f in fills if f["side"]=="SELL"]
            if buys:
                lines.append(f"  Buys: {', '.join(f['product_id'] for f in buys[:5])}")
            if sells:
                lines.append(f"  Sells: {', '.join(f['product_id'] for f in sells[:5])}")
        else:
            lines.append(f"")
            lines.append(f"📋 No crypto fills today")

        if opt_summary:
            lines.append(f"")
            lines.append(f"⚡ Options Positions ({len(opt_summary)}):")
            lines.extend(opt_summary[:6])

        if exec_actions:
            lines.append(f"")
            lines.append(f"🤖 Bot Actions Today:")
            lines.extend(exec_actions[:5])

        return _send("\n".join(lines))
    except Exception as e:
        return _send(f"📊 Daily Summary unavailable: {e}")


def send_telegram(text):
    # Compatibility shim for older code paths like services/execution_service.py.
    # Disabled by default to prevent duplicate order alerts.
    if not _legacy_enabled():
        return False
    return _send(str(text or "").strip())
