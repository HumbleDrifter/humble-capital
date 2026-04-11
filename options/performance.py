import json
import os
import time
from datetime import datetime, timedelta

from brokers.webull_adapter import WebullAdapter
from options.earnings import EarningsCalendar


def _log(message):
    print(f"[options_performance] {message}")


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return int(default)


def _parse_date(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    for candidate in (text, text[:10]):
        try:
            return datetime.fromisoformat(candidate)
        except Exception:
            continue
    return None


def _period_cutoff(period):
    period = str(period or "all").strip().lower()
    now = datetime.utcnow()
    if period in {"7d", "7"}:
      return now - timedelta(days=7)
    if period in {"30d", "30", "1m"}:
      return now - timedelta(days=30)
    if period in {"90d", "90", "3m"}:
      return now - timedelta(days=90)
    if period in {"365d", "365", "1y", "ytd"}:
      return now - timedelta(days=365)
    return None


class OptionsPerformance:
    def __init__(self):
        self.trades_file = os.path.join("data", "options_trades.json")

    def _ensure_store(self):
        folder = os.path.dirname(self.trades_file)
        if folder:
            os.makedirs(folder, exist_ok=True)
        if not os.path.exists(self.trades_file):
            with open(self.trades_file, "w", encoding="utf-8") as handle:
                json.dump([], handle)

    def _load_trades(self):
        self._ensure_store()
        try:
            with open(self.trades_file, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            return data if isinstance(data, list) else []
        except Exception as exc:
            _log(f"load_trades_failed error={exc}")
            return []

    def _save_trades(self, trades):
        self._ensure_store()
        with open(self.trades_file, "w", encoding="utf-8") as handle:
            json.dump(trades, handle, indent=2)

    def record_trade(self, trade_data):
        try:
            trades = self._load_trades()
            payload = {
                "symbol": str(trade_data.get("symbol") or "").upper().strip(),
                "strategy": str(trade_data.get("strategy") or "unknown").strip().lower(),
                "entry_date": str(trade_data.get("entry_date") or ""),
                "exit_date": str(trade_data.get("exit_date") or ""),
                "entry_price": _safe_float(trade_data.get("entry_price"), 0.0),
                "exit_price": _safe_float(trade_data.get("exit_price"), 0.0),
                "contracts": _safe_int(trade_data.get("contracts"), 0),
                "pnl": _safe_float(trade_data.get("pnl"), 0.0),
                "premium_collected": _safe_float(trade_data.get("premium_collected"), 0.0),
                "assignment": bool(trade_data.get("assignment")),
                "fees": _safe_float(trade_data.get("fees"), 0.0),
                "recorded_at": int(time.time()),
            }
            trades.append(payload)
            self._save_trades(trades)
            return {"ok": True, "trade": payload}
        except Exception as exc:
            _log(f"record_trade_failed error={exc}")
            return {"ok": False, "error": str(exc)}

    def _filtered_trades(self, period="all"):
        trades = self._load_trades()
        cutoff = _period_cutoff(period)
        if cutoff is None:
            return trades
        out = []
        for trade in trades:
            exit_dt = _parse_date(trade.get("exit_date")) or _parse_date(trade.get("entry_date"))
            if exit_dt and exit_dt >= cutoff:
                out.append(trade)
        return out

    def get_performance_summary(self, period="all") -> dict:
        trades = self._filtered_trades(period)
        winning = [t for t in trades if _safe_float(t.get("pnl"), 0.0) > 0]
        losing = [t for t in trades if _safe_float(t.get("pnl"), 0.0) < 0]
        gross_wins = sum(_safe_float(t.get("pnl"), 0.0) for t in winning)
        gross_losses = sum(_safe_float(t.get("pnl"), 0.0) for t in losing)
        total_pnl = sum(_safe_float(t.get("pnl"), 0.0) for t in trades)
        total_premium = sum(_safe_float(t.get("premium_collected"), 0.0) for t in trades)
        total_fees = sum(_safe_float(t.get("fees"), 0.0) for t in trades)

        monthly_buckets = {}
        strategy_breakdown = {}
        by_symbol = {}
        total_days = 0.0

        for trade in trades:
            exit_dt = _parse_date(trade.get("exit_date")) or _parse_date(trade.get("entry_date"))
            entry_dt = _parse_date(trade.get("entry_date"))
            month_key = exit_dt.strftime("%Y-%m") if exit_dt else "unknown"
            pnl = _safe_float(trade.get("pnl"), 0.0)
            strategy = str(trade.get("strategy") or "unknown").strip().lower()
            symbol = str(trade.get("symbol") or "UNKNOWN").upper().strip()

            bucket = monthly_buckets.setdefault(month_key, {"month": month_key, "pnl": 0.0, "trades": 0, "wins": 0})
            bucket["pnl"] += pnl
            bucket["trades"] += 1
            if pnl > 0:
                bucket["wins"] += 1

            strat = strategy_breakdown.setdefault(strategy, {"strategy": strategy, "trades": 0, "wins": 0, "pnl": 0.0, "premium_collected": 0.0})
            strat["trades"] += 1
            strat["pnl"] += pnl
            strat["premium_collected"] += _safe_float(trade.get("premium_collected"), 0.0)
            if pnl > 0:
                strat["wins"] += 1

            sym = by_symbol.setdefault(symbol, {"symbol": symbol, "trades": 0, "wins": 0, "pnl": 0.0, "premium_collected": 0.0})
            sym["trades"] += 1
            sym["pnl"] += pnl
            sym["premium_collected"] += _safe_float(trade.get("premium_collected"), 0.0)
            if pnl > 0:
                sym["wins"] += 1

            if entry_dt and exit_dt:
                total_days += max(0.0, (exit_dt - entry_dt).total_seconds() / 86400.0)

        monthly_returns = []
        for key in sorted(monthly_buckets.keys()):
            row = monthly_buckets[key]
            monthly_returns.append(
                {
                    "month": row["month"],
                    "pnl": round(row["pnl"], 2),
                    "trades": row["trades"],
                    "win_rate": round((row["wins"] / row["trades"]) if row["trades"] else 0.0, 4),
                }
            )

        for row in strategy_breakdown.values():
            trades_count = row["trades"]
            row["win_rate"] = round((row["wins"] / trades_count) if trades_count else 0.0, 4)
            row["pnl"] = round(row["pnl"], 2)
            row["premium_collected"] = round(row["premium_collected"], 2)

        for row in by_symbol.values():
            trades_count = row["trades"]
            row["win_rate"] = round((row["wins"] / trades_count) if trades_count else 0.0, 4)
            row["pnl"] = round(row["pnl"], 2)
            row["premium_collected"] = round(row["premium_collected"], 2)

        return {
            "total_trades": len(trades),
            "win_rate": round((len(winning) / len(trades)) if trades else 0.0, 4),
            "profit_factor": round(gross_wins / abs(gross_losses), 4) if gross_losses < 0 else 0.0,
            "total_pnl": round(total_pnl, 2),
            "total_premium_collected": round(total_premium, 2),
            "total_fees": round(total_fees, 2),
            "avg_days_in_trade": round((total_days / len(trades)) if trades else 0.0, 2),
            "best_trade": max((_safe_float(t.get("pnl"), 0.0) for t in trades), default=0.0),
            "worst_trade": min((_safe_float(t.get("pnl"), 0.0) for t in trades), default=0.0),
            "monthly_returns": monthly_returns,
            "strategy_breakdown": dict(sorted(strategy_breakdown.items(), key=lambda item: item[1]["pnl"], reverse=True)),
            "by_symbol": dict(sorted(by_symbol.items(), key=lambda item: item[1]["pnl"], reverse=True)),
        }

    def get_open_positions_risk(self) -> dict:
        adapter = WebullAdapter()
        earnings = EarningsCalendar()
        positions = [row for row in (adapter.get_positions() or []) if str(row.get("asset_type") or "").lower() == "option"]
        total_exposure = sum(abs(_safe_float(row.get("market_value"), 0.0)) for row in positions)
        max_possible_loss = total_exposure

        portfolio_delta = sum(_safe_float(row.get("delta"), 0.0) for row in positions)
        portfolio_theta = sum(_safe_float(row.get("theta"), 0.0) for row in positions)
        portfolio_vega = sum(_safe_float(row.get("vega"), 0.0) for row in positions)

        concentration = []
        earnings_exposure = []
        expiring_soon = []
        for row in positions:
            symbol = str(row.get("symbol") or "").upper().strip()
            exposure = abs(_safe_float(row.get("market_value"), 0.0))
            if total_exposure > 0 and exposure / total_exposure > 0.15:
                concentration.append(symbol)
            if symbol and earnings.is_earnings_within(symbol, days=14):
                earnings_exposure.append(symbol)
            expiration = _parse_date(row.get("expiration") or row.get("expiry"))
            if expiration is not None and (expiration.date() - datetime.utcnow().date()).days < 7:
                expiring_soon.append(symbol)

        return {
            "total_capital_at_risk": round(total_exposure, 2),
            "max_possible_loss": round(max_possible_loss, 2),
            "portfolio_delta": round(portfolio_delta, 4),
            "portfolio_theta": round(portfolio_theta, 4),
            "portfolio_vega": round(portfolio_vega, 4),
            "concentration_risk": sorted({s for s in concentration if s}),
            "earnings_exposure": sorted({s for s in earnings_exposure if s}),
            "expiring_soon": sorted({s for s in expiring_soon if s}),
        }
