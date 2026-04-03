import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv("/root/tradingbot/.env", override=True)

DB_PATH = str(os.getenv("TRADINGBOT_DB_PATH", "/root/tradingbot/trading.db") or "").strip() or "/root/tradingbot/trading.db"
BASE_DIR = Path("/root/tradingbot")
ASSET_CONFIG_PATH = BASE_DIR / "asset_config.json"
EXPORT_DIR = BASE_DIR / "advisor_export"
SYSTEM_STATUS_PATH = EXPORT_DIR / "system_status.json"
PORTFOLIO_SNAPSHOT_PATH = EXPORT_DIR / "portfolio_snapshot.json"
MEME_ROTATION_PATH = BASE_DIR / "meme_rotation.json"

TELEGRAM_TIMEOUT_SEC = float(os.getenv("TELEGRAM_TIMEOUT_SEC", "10") or "10")


def _env_bool(name: str, default: bool = False) -> bool:
    value = str(os.getenv(name, str(default)) or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _telegram_enabled() -> bool:
    return _env_bool("TELEGRAM_ENABLED", True)


def _telegram_token() -> str:
    load_dotenv("/root/tradingbot/.env", override=True)
    return str(os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()


def _telegram_chat_id() -> str:
    load_dotenv("/root/tradingbot/.env", override=True)
    return str(os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()


def _fmt_usd(value) -> str:
    try:
        v = float(value)
        if v < 0:
            return f"-${abs(v):,.2f}"
        return f"${v:,.2f}"
    except Exception:
        return "$0.00"


def _send_telegram(text: str) -> bool:
    if not _telegram_enabled():
        return False

    token = _telegram_token()
    chat_id = _telegram_chat_id()

    if not token or not chat_id:
        print("[daily_report] missing telegram config")
        return False

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": True,
            },
            timeout=TELEGRAM_TIMEOUT_SEC,
        ).raise_for_status()
        return True
    except Exception as exc:
        print(f"[daily_report] telegram error: {exc}")
        return False


def _load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _today_utc_prefix() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _load_core_assets() -> set[str]:
    cfg = _load_json(ASSET_CONFIG_PATH, {})
    if not isinstance(cfg, dict):
        return set()

    core_assets = cfg.get("core_assets", []) or []
    return {
        str(x).upper().strip()
        for x in core_assets
        if str(x or "").strip()
    }


def _fetch_db_summary():
    if not os.path.exists(DB_PATH):
        return {
            "db_exists": False,
            "realized_pnl_today": 0.0,
            "trade_count_today": 0,
            "buy_count_today": 0,
            "sell_count_today": 0,
            "buy_notional_today": 0.0,
            "sell_notional_today": 0.0,
            "top_products": [],
            "best_product": None,
            "worst_product": None,
        }

    today = _today_utc_prefix()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        realized_row = conn.execute(
            """
            SELECT COALESCE(SUM(pnl_usd), 0) AS total_pnl
            FROM realized_pnl
            WHERE substr(created_at, 1, 10) = ?
            """,
            (today,),
        ).fetchone()

        orders = conn.execute(
            """
            SELECT product_id, side, price, base_size, status, created_at
            FROM orders
            WHERE datetime(created_at, 'unixepoch') >= date('now')
            ORDER BY created_at DESC
            """
        ).fetchall()

        trade_count_today = len(orders)
        buy_count_today = 0
        sell_count_today = 0
        buy_notional_today = 0.0
        sell_notional_today = 0.0

        product_counts = {}
        for row in orders:
            pid = str(row["product_id"] or "UNKNOWN").upper()
            product_counts[pid] = product_counts.get(pid, 0) + 1

            side = str(row["side"] or "").upper()
            try:
                price = float(row["price"] or 0.0)
            except Exception:
                price = 0.0
            try:
                base_size = float(row["base_size"] or 0.0)
            except Exception:
                base_size = 0.0

            notional = price * base_size

            if side == "BUY":
                buy_count_today += 1
                buy_notional_today += notional
            elif side == "SELL":
                sell_count_today += 1
                sell_notional_today += notional

        top_products = sorted(product_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        pnl_by_product_rows = conn.execute(
            """
            SELECT product_id, COALESCE(SUM(pnl_usd), 0) AS total_pnl
            FROM realized_pnl
            WHERE substr(created_at, 1, 10) = ?
            GROUP BY product_id
            ORDER BY total_pnl DESC
            """,
            (today,),
        ).fetchall()

        best_product = None
        worst_product = None

        if pnl_by_product_rows:
            best_row = pnl_by_product_rows[0]
            worst_row = pnl_by_product_rows[-1]

            best_product = {
                "product_id": str(best_row["product_id"] or "UNKNOWN").upper(),
                "pnl_usd": float(best_row["total_pnl"] or 0.0),
            }
            worst_product = {
                "product_id": str(worst_row["product_id"] or "UNKNOWN").upper(),
                "pnl_usd": float(worst_row["total_pnl"] or 0.0),
            }

        return {
            "db_exists": True,
            "realized_pnl_today": float(realized_row["total_pnl"] or 0.0),
            "trade_count_today": trade_count_today,
            "buy_count_today": buy_count_today,
            "sell_count_today": sell_count_today,
            "buy_notional_today": buy_notional_today,
            "sell_notional_today": sell_notional_today,
            "top_products": top_products,
            "best_product": best_product,
            "worst_product": worst_product,
        }
    finally:
        conn.close()


def _classify_position(product_id: str, pos: dict, configured_core_assets: set[str]) -> str:
    product_id = str(product_id or "").upper().strip()
    asset_class = str(pos.get("class", "") or "").strip().lower()

    if product_id in configured_core_assets:
        return "core"

    if asset_class == "core":
        return "core"

    if asset_class in {"satellite", "satellite_active", "volatile"}:
        return "satellite"

    if asset_class == "dust":
        return "dust"

    return "satellite"


def _fetch_portfolio_summary():
    snapshot = _load_json(PORTFOLIO_SNAPSHOT_PATH, {})
    if not isinstance(snapshot, dict):
        snapshot = {}

    configured_core_assets = _load_core_assets()

    total_value_usd = float(snapshot.get("total_value_usd", 0) or 0)
    usd_cash = float(snapshot.get("usd_cash", 0) or 0)
    cash_weight = float(snapshot.get("cash_weight", 0) or 0)

    positions = snapshot.get("positions", {}) or {}

    top_asset = "unknown"
    top_asset_value = -1.0
    core_weight = 0.0
    satellite_weight = 0.0

    for product_id, pos in positions.items():
        if not isinstance(pos, dict):
            continue

        try:
            value_total_usd = float(pos.get("value_total_usd", 0) or 0)
        except Exception:
            value_total_usd = 0.0

        try:
            weight_total = float(pos.get("weight_total", 0) or 0)
        except Exception:
            weight_total = 0.0

        classification = _classify_position(product_id, pos, configured_core_assets)

        if value_total_usd > top_asset_value:
            top_asset_value = value_total_usd
            top_asset = str(product_id or "unknown").upper()

        if classification == "core":
            core_weight += weight_total
        elif classification == "satellite":
            satellite_weight += weight_total

    core_weight = max(0.0, min(core_weight, 1.0))
    satellite_weight = max(0.0, min(satellite_weight, 1.0))

    if cash_weight >= 0.70:
        market_regime = "risk_off"
    elif satellite_weight >= 0.25:
        market_regime = "bull"
    else:
        market_regime = "neutral"

    return {
        "total_value_usd": total_value_usd,
        "usd_cash": usd_cash,
        "cash_weight": cash_weight,
        "core_weight": core_weight,
        "satellite_weight": satellite_weight,
        "market_regime": market_regime,
        "top_asset": top_asset,
        "configured_core_assets": sorted(configured_core_assets),
        "exported_at": snapshot.get("timestamp"),
    }


def _fetch_system_status():
    status = _load_json(SYSTEM_STATUS_PATH, {})
    if not isinstance(status, dict):
        status = {}

    return {
        "time": status.get("time"),
        "trading_enabled": str(status.get("trading_enabled", "unknown")),
        "db_path": status.get("db_path"),
    }


def _fetch_scanner_summary():
    data = _load_json(MEME_ROTATION_PATH, {})
    if not isinstance(data, dict):
        data = {}

    candidates = data.get("candidates", []) or []
    top_candidates = []

    for c in candidates[:10]:
        if not isinstance(c, dict):
            continue

        score_breakdown = c.get("score_breakdown") or {}

        top_candidates.append({
            "product_id": str(c.get("product_id", "UNKNOWN") or "UNKNOWN").upper(),
            "score": float(c.get("score", 0) or 0),
            "momentum_tag": str(c.get("momentum_tag", "unknown") or "unknown"),
            "volume_tag": str(c.get("volume_tag", "unknown") or "unknown"),
            "meme_boost_bonus": float(score_breakdown.get("meme_boost_bonus", 0) or 0),
        })

    return {
        "candidate_count": int(data.get("candidate_count", 0) or 0),
        "meme_bias_mode": str(data.get("meme_bias_mode", "unknown") or "unknown"),
        "top_candidates": top_candidates,
    }


def _build_suggested_focus(scanner: dict):
    if not isinstance(scanner, dict):
        return {
            "high_conviction": [],
            "watchlist": [],
            "meme_momentum": [],
        }

    high_conviction = []
    watchlist = []
    meme_momentum = []

    for c in scanner.get("top_candidates", []) or []:
        if not isinstance(c, dict):
            continue

        product_id = str(c.get("product_id", "UNKNOWN") or "UNKNOWN").upper()
        score = float(c.get("score", 0) or 0)
        momentum_tag = str(c.get("momentum_tag", "unknown") or "unknown").lower()
        volume_tag = str(c.get("volume_tag", "unknown") or "unknown").lower()
        meme_boost = float(c.get("meme_boost_bonus", 0) or 0)

        if score >= 24 or (momentum_tag == "surging" and volume_tag in {"hot", "explosive"}):
            high_conviction.append(product_id)
        elif score >= 16 or volume_tag in {"hot", "explosive"}:
            watchlist.append(product_id)

        if meme_boost > 0:
            meme_momentum.append(product_id)

    def _uniq(seq):
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    return {
        "high_conviction": _uniq(high_conviction)[:3],
        "watchlist": _uniq(watchlist)[:4],
        "meme_momentum": _uniq(meme_momentum)[:4],
    }


def _render_report() -> str:
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    db = _fetch_db_summary()
    portfolio = _fetch_portfolio_summary()
    system_status = _fetch_system_status()
    scanner = _fetch_scanner_summary()
    suggested_focus = _build_suggested_focus(scanner)

    pnl = db["realized_pnl_today"]
    pnl_str = _fmt_usd(pnl)

    lines = [
        "📊 Humble Capital Daily Bot Report",
        f"Generated: {now_utc}",
        "",
        "━━━ PERFORMANCE ━━━",
        f"PnL Today: {pnl_str}",
        f"Trades Today: {db['trade_count_today']}",
        f"🟢 Buys: {db['buy_count_today']} ({_fmt_usd(db['buy_notional_today'])})",
        f"🔴 Sells: {db['sell_count_today']} ({_fmt_usd(db['sell_notional_today'])})",
    ]

    if db["best_product"]:
        best_pnl = db["best_product"]["pnl_usd"]
        lines.append(f"Best Product: {db['best_product']['product_id']} ({_fmt_usd(best_pnl)})")

    if db["worst_product"]:
        worst_pnl = db["worst_product"]["pnl_usd"]
        lines.append(f"Worst Product: {db['worst_product']['product_id']} ({_fmt_usd(worst_pnl)})")

    lines.extend([
        "",
        "━━━ PORTFOLIO ━━━",
        f"Total Value: {_fmt_usd(portfolio['total_value_usd'])}",
        f"Cash: {_fmt_usd(portfolio['usd_cash'])}",
        f"Market Regime: {portfolio['market_regime']}",
        f"Top Asset: {portfolio['top_asset']}",
        f"Core Weight: {portfolio['core_weight'] * 100:.1f}%",
        f"Satellite Weight: {portfolio['satellite_weight'] * 100:.1f}%",
        f"Cash Weight: {portfolio['cash_weight'] * 100:.1f}%",
        "",
        "━━━ SYSTEM ━━━",
        f"Trading Enabled: {system_status['trading_enabled']}",
        f"Snapshot Time: {system_status['time'] or 'unknown'}",
        "",
        "━━━ SCANNER ━━━",
        f"Mode: {scanner['meme_bias_mode']}",
        f"Candidate Count: {scanner['candidate_count']}",
    ])

    if scanner["top_candidates"]:
        lines.append("Top Candidates")
        for c in scanner["top_candidates"][:5]:
            boost = c["meme_boost_bonus"]
            boost_txt = f", meme+{boost:.0f}" if boost > 0 else ""
            lines.append(
                f"• {c['product_id']}  |  {c['score']:.1f}  |  "
                f"{c['momentum_tag']}/{c['volume_tag']}{boost_txt}"
            )

    if (
        suggested_focus["high_conviction"]
        or suggested_focus["watchlist"]
        or suggested_focus["meme_momentum"]
    ):
        lines.append("Suggested Focus")

        if suggested_focus["high_conviction"]:
            lines.append("🔥 High Conviction")
            for x in suggested_focus["high_conviction"]:
                lines.append(f"  • {x}")

        if suggested_focus["watchlist"]:
            lines.append("👀 Watchlist")
            for x in suggested_focus["watchlist"]:
                lines.append(f"  • {x}")

        if suggested_focus["meme_momentum"]:
            lines.append("🚀 Meme Momentum")
            for x in suggested_focus["meme_momentum"]:
                lines.append(f"  • {x}")

    if portfolio["configured_core_assets"]:
        lines.append("")
        lines.append("Configured Core Assets")
        lines.append(", ".join(portfolio["configured_core_assets"]))

    if db["top_products"]:
        lines.append("")
        lines.append("Most Active Products")
        for product_id, count in db["top_products"]:
            lines.append(f"• {product_id}  ({count} trades)")

    return "\n".join(lines).strip()


def main():
    report = _render_report()
    print(report)
    ok = _send_telegram(report)
    if ok:
        print("[daily_report] sent to Telegram")
    else:
        print("[daily_report] telegram send failed")


if __name__ == "__main__":
    main()
