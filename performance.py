import math
import os
import sqlite3
import time
from datetime import datetime, timezone

DB_PATH = os.getenv("TRADING_DB_PATH", "/root/tradingbot/trading.db")

_ROUND_TRIP_CACHE_TTL_SEC = 60
_ROUND_TRIP_CACHE = {"ts": 0, "value": []}
_PORTFOLIO_HISTORY_TIME_COL = None


def _log(message):
    print(f"[performance] {message}")


def _now():
    return int(time.time())


def _db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _safe_float(value, default=0.0):
    try:
        return float(value or 0.0)
    except Exception:
        return float(default)


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return int(default)


def _utc_date(ts):
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return ""


def _portfolio_history_time_column():
    global _PORTFOLIO_HISTORY_TIME_COL
    if _PORTFOLIO_HISTORY_TIME_COL:
        return _PORTFOLIO_HISTORY_TIME_COL

    try:
        with _db_conn() as conn:
            rows = conn.execute("PRAGMA table_info(portfolio_history)").fetchall()
        cols = {str(row["name"] or "").strip().lower() for row in rows}
        if "timestamp" in cols:
            _PORTFOLIO_HISTORY_TIME_COL = "timestamp"
        elif "created_at" in cols:
            _PORTFOLIO_HISTORY_TIME_COL = "created_at"
        else:
            _PORTFOLIO_HISTORY_TIME_COL = None
    except Exception as exc:
        _log(f"failed to inspect portfolio_history schema error={exc}")
        _PORTFOLIO_HISTORY_TIME_COL = None

    return _PORTFOLIO_HISTORY_TIME_COL


def _load_filled_orders():
    try:
        with _db_conn() as conn:
            rows = conn.execute(
                """
                SELECT order_id, product_id, side, base_size, price, status, created_at
                FROM orders
                WHERE UPPER(COALESCE(status, '')) = 'FILLED'
                ORDER BY created_at ASC, rowid ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]
    except Exception as exc:
        _log(f"failed to load orders error={exc}")
        return []


def _consume_lots(lots, qty_to_close):
    remaining = float(qty_to_close or 0.0)
    opened_at = None

    while remaining > 1e-12 and lots:
        lot = lots[0]
        lot_qty = _safe_float(lot.get("qty"), 0.0)
        lot_ts = _safe_int(lot.get("ts"), 0)

        if opened_at is None:
            opened_at = lot_ts

        consume = min(remaining, lot_qty)
        lot_qty -= consume
        remaining -= consume

        if lot_qty <= 1e-12:
            lots.pop(0)
        else:
            lot["qty"] = lot_qty

    return opened_at


def _compute_round_trips_uncached():
    orders = _load_filled_orders()
    if not orders:
        return []

    states = {}
    round_trips = []

    for row in orders:
        product_id = str(row.get("product_id") or "").strip().upper()
        side = str(row.get("side") or "").strip().upper()
        qty = _safe_float(row.get("base_size"), 0.0)
        price = _safe_float(row.get("price"), 0.0)
        created_at = _safe_int(row.get("created_at"), 0)

        if not product_id or qty <= 0 or price <= 0 or side not in {"BUY", "SELL"}:
            continue

        state = states.setdefault(
            product_id,
            {
                "qty": 0.0,
                "avg_entry_price": 0.0,
                "lots": [],
            },
        )

        if side == "BUY":
            current_qty = _safe_float(state["qty"], 0.0)
            current_avg = _safe_float(state["avg_entry_price"], 0.0)
            new_qty = current_qty + qty
            if new_qty > 0:
                state["avg_entry_price"] = (
                    ((current_qty * current_avg) + (qty * price)) / new_qty
                )
            else:
                state["avg_entry_price"] = 0.0
            state["qty"] = new_qty
            state["lots"].append({"qty": qty, "ts": created_at})
            continue

        current_qty = _safe_float(state["qty"], 0.0)
        avg_entry_price = _safe_float(state["avg_entry_price"], 0.0)
        qty_to_close = min(qty, current_qty)

        if qty_to_close <= 1e-12 or avg_entry_price <= 0:
            continue

        opened_at = _consume_lots(state["lots"], qty_to_close)
        pnl_usd = (price - avg_entry_price) * qty_to_close
        pnl_pct = ((price - avg_entry_price) / avg_entry_price) * 100.0 if avg_entry_price > 0 else 0.0

        if opened_at is None:
            opened_at = created_at

        round_trips.append(
            {
                "product_id": product_id,
                "side": "SELL",
                "entry_price": round(avg_entry_price, 8),
                "exit_price": round(price, 8),
                "qty": round(qty_to_close, 8),
                "notional_usd": round(price * qty_to_close, 2),
                "pnl_usd": round(pnl_usd, 2),
                "pnl_pct": round(pnl_pct, 4),
                "is_win": bool(pnl_usd > 0),
                "opened_at": opened_at,
                "closed_at": created_at,
                "hold_duration_sec": max(0, created_at - opened_at) if opened_at and created_at else 0,
            }
        )

        remaining_qty = max(0.0, current_qty - qty_to_close)
        state["qty"] = remaining_qty
        if remaining_qty <= 1e-12:
            state["avg_entry_price"] = 0.0
            state["lots"] = []

    round_trips.sort(key=lambda row: _safe_int(row.get("closed_at"), 0), reverse=True)
    _log(f"computed round trips count={len(round_trips)}")
    return round_trips


def _get_cached_round_trips():
    now = _now()
    if (now - _safe_int(_ROUND_TRIP_CACHE.get("ts"), 0)) < _ROUND_TRIP_CACHE_TTL_SEC:
        return list(_ROUND_TRIP_CACHE.get("value") or [])

    value = _compute_round_trips_uncached()
    _ROUND_TRIP_CACHE["ts"] = now
    _ROUND_TRIP_CACHE["value"] = list(value)
    return list(value)


def get_round_trips(product_id: str = None, limit: int = 500) -> list:
    try:
        rows = _get_cached_round_trips()
        product_filter = str(product_id or "").strip().upper()
        if product_filter:
            rows = [row for row in rows if str(row.get("product_id") or "").upper() == product_filter]

        limit = max(0, _safe_int(limit, 500))
        if limit:
            rows = rows[:limit]
        return rows
    except Exception as exc:
        _log(f"get_round_trips failed error={exc}")
        return []


def get_performance_summary(days: int = None) -> dict:
    try:
        rows = get_round_trips(product_id=None, limit=1000000)
        if days is not None:
            cutoff = _now() - (int(days) * 86400)
            rows = [row for row in rows if _safe_int(row.get("closed_at"), 0) >= cutoff]

        if not rows:
            return {
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0.0,
                "total_pnl_usd": 0.0,
                "avg_pnl_usd": 0.0,
                "avg_win_usd": 0.0,
                "avg_loss_usd": 0.0,
                "largest_win_usd": 0.0,
                "largest_loss_usd": 0.0,
                "profit_factor": 0.0,
                "avg_hold_duration_sec": 0.0,
                "best_product": "",
                "worst_product": "",
                "current_streak": {"type": "none", "count": 0},
                "generated_at": _now(),
            }

        total_trades = len(rows)
        wins = [row for row in rows if _safe_float(row.get("pnl_usd"), 0.0) > 0]
        losses = [row for row in rows if _safe_float(row.get("pnl_usd"), 0.0) < 0]
        total_pnl = sum(_safe_float(row.get("pnl_usd"), 0.0) for row in rows)
        gross_wins = sum(_safe_float(row.get("pnl_usd"), 0.0) for row in wins)
        gross_losses = sum(_safe_float(row.get("pnl_usd"), 0.0) for row in losses)
        avg_hold = sum(_safe_int(row.get("hold_duration_sec"), 0) for row in rows) / total_trades if total_trades else 0.0

        by_product = {}
        for row in rows:
            product_id = str(row.get("product_id") or "").strip().upper()
            by_product[product_id] = by_product.get(product_id, 0.0) + _safe_float(row.get("pnl_usd"), 0.0)

        sorted_by_product = sorted(by_product.items(), key=lambda item: item[1], reverse=True)
        best_product = sorted_by_product[0][0] if sorted_by_product else ""
        worst_product = sorted_by_product[-1][0] if sorted_by_product else ""

        streak_type = "none"
        streak_count = 0
        if rows:
            first_is_win = _safe_float(rows[0].get("pnl_usd"), 0.0) > 0
            streak_type = "win" if first_is_win else "loss"
            for row in rows:
                is_win = _safe_float(row.get("pnl_usd"), 0.0) > 0
                if is_win == first_is_win:
                    streak_count += 1
                else:
                    break

        return {
            "total_trades": total_trades,
            "winning_trades": len(wins),
            "losing_trades": len(losses),
            "win_rate": (len(wins) / total_trades) if total_trades else 0.0,
            "total_pnl_usd": round(total_pnl, 2),
            "avg_pnl_usd": round(total_pnl / total_trades, 2) if total_trades else 0.0,
            "avg_win_usd": round(gross_wins / len(wins), 2) if wins else 0.0,
            "avg_loss_usd": round(sum(_safe_float(row.get("pnl_usd"), 0.0) for row in losses) / len(losses), 2) if losses else 0.0,
            "largest_win_usd": round(max((_safe_float(row.get("pnl_usd"), 0.0) for row in wins), default=0.0), 2),
            "largest_loss_usd": round(min((_safe_float(row.get("pnl_usd"), 0.0) for row in losses), default=0.0), 2),
            "profit_factor": round(gross_wins / abs(gross_losses), 4) if gross_losses < 0 else 0.0,
            "avg_hold_duration_sec": round(avg_hold, 2),
            "best_product": best_product,
            "worst_product": worst_product,
            "current_streak": {"type": streak_type, "count": streak_count},
            "generated_at": _now(),
        }
    except Exception as exc:
        _log(f"get_performance_summary failed error={exc}")
        return {
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "win_rate": 0.0,
            "total_pnl_usd": 0.0,
            "avg_pnl_usd": 0.0,
            "avg_win_usd": 0.0,
            "avg_loss_usd": 0.0,
            "largest_win_usd": 0.0,
            "largest_loss_usd": 0.0,
            "profit_factor": 0.0,
            "avg_hold_duration_sec": 0.0,
            "best_product": "",
            "worst_product": "",
            "current_streak": {"type": "none", "count": 0},
            "generated_at": _now(),
        }


def _load_portfolio_history_rows():
    time_col = _portfolio_history_time_column()
    if not time_col:
        return []

    try:
        with _db_conn() as conn:
            rows = conn.execute(
                f"""
                SELECT total_value_usd, {time_col} AS ts
                FROM portfolio_history
                WHERE total_value_usd IS NOT NULL
                ORDER BY ts ASC, id ASC
                """
            ).fetchall()
        cleaned = []
        for row in rows:
            ts = _safe_int(row["ts"], 0)
            value = _safe_float(row["total_value_usd"], 0.0)
            if ts > 0:
                cleaned.append({"ts": ts, "value": value})
        return cleaned
    except Exception as exc:
        _log(f"failed to load portfolio history error={exc}")
        return []


def get_equity_analytics(days: int = 30) -> dict:
    try:
        all_points = _load_portfolio_history_rows()
        if not all_points:
            return {
                "high_water_mark_usd": 0.0,
                "current_value_usd": 0.0,
                "max_drawdown_usd": 0.0,
                "max_drawdown_pct": 0.0,
                "current_drawdown_usd": 0.0,
                "current_drawdown_pct": 0.0,
                "days_since_hwm": 0,
                "equity_points": [],
                "generated_at": _now(),
            }

        current_ts = all_points[-1]["ts"]
        if days is not None:
            cutoff = current_ts - (int(days) * 86400)
            points = [row for row in all_points if row["ts"] >= cutoff]
        else:
            points = list(all_points)

        global_hwm = 0.0
        global_hwm_ts = 0
        running_peak = 0.0
        max_dd_usd = 0.0
        max_dd_pct = 0.0

        for row in all_points:
            value = _safe_float(row["value"], 0.0)
            if value >= global_hwm:
                global_hwm = value
                global_hwm_ts = row["ts"]

            if value > running_peak:
                running_peak = value

            drawdown_usd = max(0.0, running_peak - value)
            drawdown_pct = (drawdown_usd / running_peak) * 100.0 if running_peak > 0 else 0.0
            if drawdown_usd > max_dd_usd:
                max_dd_usd = drawdown_usd
            if drawdown_pct > max_dd_pct:
                max_dd_pct = drawdown_pct

        equity_points = []
        rolling_hwm = 0.0
        for row in points:
            value = _safe_float(row["value"], 0.0)
            if value > rolling_hwm:
                rolling_hwm = value
            drawdown_pct = ((rolling_hwm - value) / rolling_hwm) * 100.0 if rolling_hwm > 0 else 0.0
            equity_points.append(
                {
                    "ts": row["ts"],
                    "value": round(value, 2),
                    "hwm": round(rolling_hwm, 2),
                    "drawdown_pct": round(drawdown_pct, 4),
                }
            )

        current_value = _safe_float(all_points[-1]["value"], 0.0)
        current_drawdown_usd = max(0.0, global_hwm - current_value)
        current_drawdown_pct = (current_drawdown_usd / global_hwm) * 100.0 if global_hwm > 0 else 0.0
        days_since_hwm = max(0, int((current_ts - global_hwm_ts) / 86400)) if global_hwm_ts > 0 else 0

        return {
            "high_water_mark_usd": round(global_hwm, 2),
            "current_value_usd": round(current_value, 2),
            "max_drawdown_usd": round(max_dd_usd, 2),
            "max_drawdown_pct": round(max_dd_pct, 4),
            "current_drawdown_usd": round(current_drawdown_usd, 2),
            "current_drawdown_pct": round(current_drawdown_pct, 4),
            "days_since_hwm": days_since_hwm,
            "equity_points": equity_points,
            "generated_at": _now(),
        }
    except Exception as exc:
        _log(f"get_equity_analytics failed error={exc}")
        return {
            "high_water_mark_usd": 0.0,
            "current_value_usd": 0.0,
            "max_drawdown_usd": 0.0,
            "max_drawdown_pct": 0.0,
            "current_drawdown_usd": 0.0,
            "current_drawdown_pct": 0.0,
            "days_since_hwm": 0,
            "equity_points": [],
            "generated_at": _now(),
        }


def get_daily_pnl(days: int = 30) -> list:
    try:
        rows = get_round_trips(product_id=None, limit=1000000)
        if days is not None:
            cutoff = _now() - (int(days) * 86400)
            rows = [row for row in rows if _safe_int(row.get("closed_at"), 0) >= cutoff]

        by_day = {}
        for row in rows:
            date_key = _utc_date(_safe_int(row.get("closed_at"), 0))
            if not date_key:
                continue
            bucket = by_day.setdefault(
                date_key,
                {"date": date_key, "pnl_usd": 0.0, "trade_count": 0, "win_count": 0},
            )
            pnl = _safe_float(row.get("pnl_usd"), 0.0)
            bucket["pnl_usd"] += pnl
            bucket["trade_count"] += 1
            if pnl > 0:
                bucket["win_count"] += 1

        out = list(by_day.values())
        out.sort(key=lambda row: row["date"])
        for row in out:
            row["pnl_usd"] = round(_safe_float(row["pnl_usd"]), 2)
        return out
    except Exception as exc:
        _log(f"get_daily_pnl failed error={exc}")
        return []


def get_product_breakdown() -> list:
    try:
        rows = get_round_trips(product_id=None, limit=1000000)
        if not rows:
            return []

        grouped = {}
        for row in rows:
            product_id = str(row.get("product_id") or "").strip().upper()
            pnl = _safe_float(row.get("pnl_usd"), 0.0)
            notional = _safe_float(row.get("notional_usd"), 0.0)

            bucket = grouped.setdefault(
                product_id,
                {
                    "product_id": product_id,
                    "trade_count": 0,
                    "win_count": 0,
                    "total_pnl_usd": 0.0,
                    "total_notional_usd": 0.0,
                },
            )
            bucket["trade_count"] += 1
            bucket["total_pnl_usd"] += pnl
            bucket["total_notional_usd"] += notional
            if pnl > 0:
                bucket["win_count"] += 1

        out = []
        for bucket in grouped.values():
            trade_count = int(bucket["trade_count"])
            total_pnl = _safe_float(bucket["total_pnl_usd"], 0.0)
            out.append(
                {
                    "product_id": bucket["product_id"],
                    "trade_count": trade_count,
                    "win_rate": (int(bucket["win_count"]) / trade_count) if trade_count else 0.0,
                    "total_pnl_usd": round(total_pnl, 2),
                    "avg_pnl_usd": round(total_pnl / trade_count, 2) if trade_count else 0.0,
                    "total_notional_usd": round(_safe_float(bucket["total_notional_usd"], 0.0), 2),
                }
            )

        out.sort(key=lambda row: _safe_float(row.get("total_pnl_usd"), 0.0), reverse=True)
        return out
    except Exception as exc:
        _log(f"get_product_breakdown failed error={exc}")
        return []
