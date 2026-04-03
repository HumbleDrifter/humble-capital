import os
import sqlite3
from decimal import Decimal
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv("/root/tradingbot/.env", override=True)

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
    load_dotenv("/root/tradingbot/.env", override=True)
    return str(os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()


def _chat():
    load_dotenv("/root/tradingbot/.env", override=True)
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


def send_telegram(text):
    # Compatibility shim for older code paths like services/execution_service.py.
    # Disabled by default to prevent duplicate order alerts.
    if not _legacy_enabled():
        return False
    return _send(str(text or "").strip())
