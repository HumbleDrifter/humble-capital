import time
from datetime import datetime, timedelta


def _log(message):
    print(f"[options_earnings] {message}")


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _to_date_string(value):
    if value is None:
        return ""
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if hasattr(value, "date"):
        try:
            return value.date().isoformat()
        except Exception:
            pass
    try:
        return str(value)[:10]
    except Exception:
        return ""


class EarningsCalendar:
    def __init__(self):
        self.cache = {}
        self.cache_ttl = 86400
        self.watchlist = [
            "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AMD",
            "SPY", "QQQ", "IWM", "NFLX", "DIS", "BA", "JPM", "GS", "BAC",
            "XOM", "CVX", "PFE", "JNJ", "V", "MA", "COST", "WMT",
            "HD", "LOW", "CRM", "ORCL", "ADBE", "INTC", "MU", "QCOM",
            "COIN", "MARA", "RIOT", "SQ", "PYPL", "UBER", "ABNB",
            "PLTR", "SOFI", "NIO", "RIVN", "F", "GM", "T", "VZ", "KO", "PEP",
        ]

    def _cache_get(self, key):
        entry = self.cache.get(key)
        if not isinstance(entry, dict):
            return None
        if (time.time() - _safe_float(entry.get("ts"), 0.0)) > self.cache_ttl:
            return None
        return entry.get("data")

    def _cache_set(self, key, data):
        self.cache[key] = {"ts": time.time(), "data": data}
        return data

    def _get_ticker(self, symbol):
        import yfinance as yf

        return yf.Ticker(str(symbol or "").upper().strip())

    def _fetch_history(self, symbol, start_date, end_date):
        try:
            ticker = self._get_ticker(symbol)
            df = ticker.history(start=start_date, end=end_date, auto_adjust=False)
            if df is None or getattr(df, "empty", True):
                return []
            rows = []
            for idx, row in df.iterrows():
                try:
                    rows.append({
                        "date": _to_date_string(idx),
                        "open": _safe_float(row.get("Open"), 0.0),
                        "high": _safe_float(row.get("High"), 0.0),
                        "low": _safe_float(row.get("Low"), 0.0),
                        "close": _safe_float(row.get("Close"), 0.0),
                        "volume": int(_safe_float(row.get("Volume"), 0.0)),
                    })
                except Exception:
                    continue
            return rows
        except Exception as exc:
            _log(f"history fetch failed symbol={symbol} error={exc}")
            return []

    def _earnings_dates_raw(self, symbol):
        key = ("earnings_dates_raw", str(symbol or "").upper().strip())
        cached = self._cache_get(key)
        if cached is not None:
            return cached
        try:
            ticker = self._get_ticker(symbol)
            df = ticker.earnings_dates
            rows = []
            if df is not None and not getattr(df, "empty", True):
                index = getattr(df, "index", [])
                for idx, (_, row) in enumerate(df.iterrows()):
                    event_dt = index[idx] if idx < len(index) else None
                    rows.append(
                        {
                            "date": _to_date_string(event_dt),
                            "eps_estimate": _safe_float(row.get("EPS Estimate"), 0.0),
                            "eps_actual": _safe_float(row.get("Reported EPS"), 0.0),
                            "surprise_pct": _safe_float(row.get("Surprise(%)"), 0.0),
                        }
                    )
            return self._cache_set(key, rows)
        except Exception as exc:
            _log(f"earnings dates failed symbol={symbol} error={exc}")
            return self._cache_set(key, [])

    def _next_earnings_date(self, symbol):
        key = ("next_earnings", str(symbol or "").upper().strip())
        cached = self._cache_get(key)
        if cached is not None:
            return cached
        try:
            ticker = self._get_ticker(symbol)
            cal = ticker.calendar
            event_date = None
            if hasattr(cal, "loc"):
                for label in ("Earnings Date", "Earnings Dates"):
                    try:
                        value = cal.loc[label]
                        if hasattr(value, "__iter__") and not isinstance(value, (str, bytes)):
                            for item in value:
                                date_str = _to_date_string(item)
                                if date_str:
                                    event_date = date_str
                                    break
                        else:
                            event_date = _to_date_string(value)
                        if event_date:
                            break
                    except Exception:
                        continue
            return self._cache_set(key, event_date or "")
        except Exception as exc:
            _log(f"next earnings failed symbol={symbol} error={exc}")
            return self._cache_set(key, "")

    def get_upcoming_earnings(self, days_ahead=14) -> list:
        key = ("upcoming", int(days_ahead))
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        now = datetime.utcnow().date()
        out = []
        for symbol in self.watchlist:
            try:
                date_str = self._next_earnings_date(symbol)
                if not date_str:
                    continue
                earnings_date = datetime.fromisoformat(date_str).date()
                days_until = (earnings_date - now).days
                if 0 <= days_until <= int(days_ahead):
                    out.append(
                        {
                            "symbol": symbol,
                            "earnings_date": date_str,
                            "days_until": days_until,
                            "historical_move_avg": round(self.get_historical_earnings_move(symbol), 4),
                        }
                    )
            except Exception:
                continue

        out.sort(key=lambda row: (row.get("days_until", 9999), row.get("symbol", "")))
        return self._cache_set(key, out)

    def get_earnings_dates(self, symbol, lookback_quarters=8) -> list:
        key = ("history", str(symbol or "").upper().strip(), int(lookback_quarters))
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        raw = self._earnings_dates_raw(symbol)
        if not raw:
            return self._cache_set(key, [])

        start_date = (datetime.utcnow() - timedelta(days=800)).date().isoformat()
        end_date = (datetime.utcnow() + timedelta(days=10)).date().isoformat()
        history = self._fetch_history(symbol, start_date, end_date)
        history_by_date = {row["date"]: row for row in history}
        ordered_dates = [row["date"] for row in history]

        results = []
        for row in raw[: max(1, int(lookback_quarters))]:
            date_str = row.get("date") or ""
            stock_move_pct = 0.0
            if date_str and date_str in history_by_date:
                idx = ordered_dates.index(date_str)
                prev_close = _safe_float(history[idx - 1]["close"], 0.0) if idx > 0 else 0.0
                next_close = _safe_float(history[idx + 1]["close"], 0.0) if idx + 1 < len(history) else _safe_float(history[idx]["close"], 0.0)
                if prev_close > 0 and next_close > 0:
                    stock_move_pct = ((next_close - prev_close) / prev_close) * 100.0
            results.append(
                {
                    "date": date_str,
                    "eps_estimate": row.get("eps_estimate", 0.0),
                    "eps_actual": row.get("eps_actual", 0.0),
                    "surprise_pct": row.get("surprise_pct", 0.0),
                    "stock_move_pct": round(stock_move_pct, 4),
                }
            )

        return self._cache_set(key, results)

    def get_historical_earnings_move(self, symbol) -> float:
        rows = self.get_earnings_dates(symbol, lookback_quarters=8)
        moves = [abs(_safe_float(row.get("stock_move_pct"), 0.0)) for row in rows if row.get("stock_move_pct") is not None]
        if not moves:
            return 0.0
        return sum(moves) / len(moves)

    def is_earnings_within(self, symbol, days=14) -> bool:
        try:
            date_str = self._next_earnings_date(symbol)
            if not date_str:
                return False
            earnings_date = datetime.fromisoformat(date_str).date()
            now = datetime.utcnow().date()
            delta_days = (earnings_date - now).days
            return 0 <= delta_days <= int(days)
        except Exception:
            return False
