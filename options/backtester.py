import math
import time
from datetime import datetime, timedelta, timezone

from brokers.webull_adapter import WebullAdapter

_CANDLE_CACHE = {}


def _log(message):
    print(f"[options_backtester] {message}")


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _normalize_symbol(symbol):
    return str(symbol or "").upper().strip()


def _to_utc_date(value, fallback_days):
    if value:
        dt = datetime.fromisoformat(str(value).strip())
    else:
        dt = datetime.now(timezone.utc) - timedelta(days=fallback_days)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.date()


def _annualized_volatility(bars, lookback=30):
    if len(bars) < 2:
        return 0.35
    closes = [_safe_float(row.get("close"), 0.0) for row in bars[-lookback:]]
    returns = []
    for idx in range(1, len(closes)):
        if closes[idx - 1] <= 0 or closes[idx] <= 0:
            continue
        returns.append(math.log(closes[idx] / closes[idx - 1]))
    if len(returns) < 2:
        return 0.35
    mean_ret = sum(returns) / len(returns)
    variance = sum((value - mean_ret) ** 2 for value in returns) / max(1, len(returns) - 1)
    return max(0.08, math.sqrt(variance) * math.sqrt(252.0))


def _normal_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _normal_cdf(x):
    try:
        from scipy.stats import norm  # type: ignore

        return float(norm.cdf(x))
    except Exception:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _max_drawdown_pct(curve):
    peak = 0.0
    max_dd = 0.0
    for row in curve:
        equity = _safe_float(row.get("equity"), 0.0)
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, (peak - equity) / peak)
    return max_dd * 100.0


def _profit_factor(pnls):
    wins = sum(value for value in pnls if value > 0)
    losses = sum(abs(value) for value in pnls if value < 0)
    if losses <= 0:
        return wins if wins > 0 else 0.0
    return wins / losses


def _fetch_bars_yfinance(symbol, start_date, end_date):
    try:
        import yfinance as yf

        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start_date, end=end_date)
        return [
            {
                "date": str(idx.date()),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": int(row["Volume"]),
            }
            for idx, row in df.iterrows()
        ]
    except Exception:
        return []


class OptionsBacktester:
    def __init__(self, config=None):
        config = dict(config or {})
        self.config = {
            "starting_capital": _safe_float(config.get("starting_capital"), 5000.0),
            "strategy": str(config.get("strategy") or "wheel").strip().lower(),
            "symbol": _normalize_symbol(config.get("symbol") or "AAPL"),
            "start_date": config.get("start_date"),
            "end_date": config.get("end_date"),
            "params": dict(config.get("params") or {}),
            "fee_per_contract": _safe_float(config.get("fee_per_contract"), 0.65),
        }
        self.cash = self.config["starting_capital"]
        self.positions = []
        self.shares_held = {}
        self.trade_log = []
        self.equity_curve = []
        self.assignment_log = []
        self.total_fees = 0.0
        self.total_premium_collected = 0.0
        self.total_assignment_pnl = 0.0
        self._last_result = {}
        self._webull_adapter = WebullAdapter()

    def fetch_underlying_bars(self, symbol, start_date, end_date) -> list:
        symbol = _normalize_symbol(symbol)
        cache_key = (symbol, str(start_date), str(end_date))
        cached = _CANDLE_CACHE.get(cache_key)
        if isinstance(cached, list) and cached:
            return [dict(row) for row in cached]

        bars = []
        try:
            _, _, data_client = self._webull_adapter._get_clients()  # pylint: disable=protected-access
            market_data = getattr(data_client, "market_data", None)
            get_history_bar = getattr(market_data, "get_history_bar", None) if market_data else None
            if callable(get_history_bar):
                response = get_history_bar(symbol, "US_STOCK", "D", count="500")
                raw = response.json() if hasattr(response, "json") else response
                rows = raw.get("data") if isinstance(raw, dict) else raw
                rows = rows if isinstance(rows, list) else []
                parsed = []
                start_cutoff = str(start_date)
                end_cutoff = str(end_date)
                for row in rows:
                    item = row if isinstance(row, dict) else {}
                    date_value = str(item.get("date") or item.get("time") or item.get("trade_date") or "")
                    if not date_value:
                        ts = _safe_int(item.get("timestamp") or item.get("ts"), 0)
                        if ts > 0:
                            date_value = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
                    if not date_value:
                        continue
                    if date_value < start_cutoff or date_value > end_cutoff:
                        continue
                    parsed.append(
                        {
                            "date": date_value,
                            "open": _safe_float(item.get("open"), 0.0),
                            "high": _safe_float(item.get("high"), 0.0),
                            "low": _safe_float(item.get("low"), 0.0),
                            "close": _safe_float(item.get("close"), 0.0),
                            "volume": _safe_int(item.get("volume"), 0),
                        }
                    )
                if parsed:
                    bars = sorted(parsed, key=lambda row: row["date"])
        except Exception as exc:
            _log(f"webull_history_fallback symbol={symbol} error={exc}")

        if not bars:
            bars = _fetch_bars_yfinance(symbol, start_date, end_date)

        _CANDLE_CACHE[cache_key] = [dict(row) for row in bars]
        return [dict(row) for row in bars]

    def compute_theoretical_option_price(self, spot, strike, dte_days, iv, option_type, risk_free_rate=0.05) -> float:
        spot = _safe_float(spot, 0.0)
        strike = _safe_float(strike, 0.0)
        iv = max(0.01, _safe_float(iv, 0.35))
        t = max(1.0 / 365.0, _safe_float(dte_days, 0.0) / 365.0)
        if spot <= 0 or strike <= 0:
            return 0.0
        sqrt_t = math.sqrt(t)
        d1 = (math.log(spot / strike) + (risk_free_rate + 0.5 * iv * iv) * t) / (iv * sqrt_t)
        d2 = d1 - iv * sqrt_t
        if str(option_type).lower().strip() == "put":
            price = strike * math.exp(-risk_free_rate * t) * _normal_cdf(-d2) - spot * _normal_cdf(-d1)
        else:
            price = spot * _normal_cdf(d1) - strike * math.exp(-risk_free_rate * t) * _normal_cdf(d2)
        return max(0.01, price)

    def compute_theoretical_greeks(self, spot, strike, dte_days, iv, option_type) -> dict:
        spot = _safe_float(spot, 0.0)
        strike = _safe_float(strike, 0.0)
        iv = max(0.01, _safe_float(iv, 0.35))
        t = max(1.0 / 365.0, _safe_float(dte_days, 0.0) / 365.0)
        if spot <= 0 or strike <= 0:
            return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}

        sqrt_t = math.sqrt(t)
        d1 = (math.log(spot / strike) + (0.05 + 0.5 * iv * iv) * t) / (iv * sqrt_t)
        d2 = d1 - iv * sqrt_t
        is_put = str(option_type).lower().strip() == "put"
        delta = _normal_cdf(d1) - 1.0 if is_put else _normal_cdf(d1)
        gamma = _normal_pdf(d1) / (spot * iv * sqrt_t)
        theta = (
            -(spot * _normal_pdf(d1) * iv) / (2.0 * sqrt_t)
            - (0.05 * strike * math.exp(-0.05 * t) * (_normal_cdf(-d2) if is_put else _normal_cdf(d2)))
        ) / 365.0
        vega = (spot * _normal_pdf(d1) * sqrt_t) / 100.0
        return {"delta": delta, "gamma": gamma, "theta": theta, "vega": vega}

    def estimate_iv_from_history(self, bars, lookback=30) -> float:
        return max(0.12, _annualized_volatility(bars, lookback=lookback) * 1.2)

    def _record_trade(self, **payload):
        self.trade_log.append(payload)

    def _record_equity(self, date_value, close_price, positions_value=0.0):
        shares_total = 0.0
        for symbol, shares in (self.shares_held or {}).items():
            if _normalize_symbol(symbol) == self.config["symbol"]:
                shares_total += _safe_float(shares, 0.0) * close_price
        equity = self.cash + shares_total + positions_value
        self.equity_curve.append(
            {
                "date": str(date_value),
                "equity": round(equity, 2),
                "cash": round(self.cash, 2),
                "positions_value": round(positions_value, 2),
                "shares_value": round(shares_total, 2),
            }
        )

    def _record_assignment(self, date_value, symbol, strike, shares, cost_basis):
        self.assignment_log.append(
            {
                "date": str(date_value),
                "symbol": symbol,
                "strike": round(strike, 2),
                "shares": int(shares),
                "cost_basis": round(cost_basis, 2),
            }
        )

    def _days_between(self, start_date, end_date):
        start_dt = datetime.fromisoformat(str(start_date))
        end_dt = datetime.fromisoformat(str(end_date))
        return max(1, (end_dt - start_dt).days)

    def _sell_option(self, date_value, symbol, option_type, strike, dte_days, iv, qty, reason):
        spot = self._current_close
        premium = self.compute_theoretical_option_price(spot, strike, dte_days, iv, option_type)
        credit = premium * qty * 100.0
        fee = qty * self.config["fee_per_contract"]
        self.cash += max(0.0, credit - fee)
        self.total_fees += fee
        self.total_premium_collected += credit
        position = {
            "symbol": symbol,
            "type": option_type,
            "strike": strike,
            "entry_price": premium,
            "qty": qty,
            "opened": str(date_value),
            "dte_days": int(dte_days),
            "start_dte": int(dte_days),
            "iv": iv,
            "entry_credit": credit,
            "reason": reason,
        }
        self.positions.append(position)
        self._record_trade(
            date=str(date_value),
            action="SELL_TO_OPEN",
            symbol=symbol,
            strike=round(strike, 2),
            type=option_type,
            premium=round(credit, 2),
            pnl=0.0,
            fee=round(fee, 2),
            reason=reason,
        )
        return position

    def _close_option(self, position, date_value, spot, reason):
        current_price = self.compute_theoretical_option_price(
            spot,
            position["strike"],
            max(0, position["dte_days"]),
            position["iv"],
            position["type"],
        )
        debit = current_price * position["qty"] * 100.0
        fee = position["qty"] * self.config["fee_per_contract"]
        pnl = position["entry_credit"] - debit - fee
        self.cash -= debit + fee
        self.total_fees += fee
        self._record_trade(
            date=str(date_value),
            action="BUY_TO_CLOSE",
            symbol=position["symbol"],
            strike=round(position["strike"], 2),
            type=position["type"],
            premium=round(-debit, 2),
            pnl=round(pnl, 2),
            fee=round(fee, 2),
            reason=reason,
        )
        return pnl

    def _simulate_generic_short_option(self, bars, option_type, params):
        symbol = self.config["symbol"]
        target_dte = _safe_int(params.get("target_dte"), 30)
        target_delta = abs(_safe_float(params.get("target_delta"), 0.25))
        profit_take = _safe_float(params.get("profit_take_pct"), 0.50)
        cooldown_days = _safe_int(params.get("cooldown_days"), 3)
        iv = self.estimate_iv_from_history(bars)
        last_exit_idx = -999
        results = []

        for idx, bar in enumerate(bars):
            self._current_close = _safe_float(bar.get("close"), 0.0)
            self._record_equity(bar["date"], self._current_close, 0.0)
            if idx - last_exit_idx < cooldown_days:
                continue

            if self.positions:
                position = self.positions[0]
                position["dte_days"] = max(0, _safe_int(position.get("dte_days"), 0) - 1)
                remaining_price = self.compute_theoretical_option_price(
                    self._current_close,
                    position["strike"],
                    position["dte_days"],
                    position["iv"],
                    position["type"],
                )
                current_value = remaining_price * position["qty"] * 100.0
                target_close_value = position["entry_credit"] * (1.0 - profit_take)
                expired = position["dte_days"] <= 0
                if current_value <= target_close_value or expired:
                    pnl = self._close_option(position, bar["date"], self._current_close, "profit_take" if current_value <= target_close_value else "expiry")
                    self.positions = []
                    last_exit_idx = idx
                    results.append(pnl)
                continue

            strike = self._current_close * (1.0 - target_delta * 0.35) if option_type == "put" else self._current_close * (1.0 + target_delta * 0.35)
            contracts = max(1, int(self.cash / max(1.0, self._current_close * 100.0)))
            if contracts <= 0:
                continue
            self._sell_option(bar["date"], symbol, option_type, strike, target_dte, iv, 1, params.get("reason", "short_premium"))
        return results

    def simulate_wheel(self, bars, params) -> dict:
        params = {
            "target_dte": 30,
            "target_delta": 0.25,
            "profit_take_pct": 0.50,
            "cooldown_days": 3,
            **dict(params or {}),
        }
        symbol = self.config["symbol"]
        iv = self.estimate_iv_from_history(bars)
        state = "csp"
        open_option = None
        pnl_samples = []

        for bar in bars:
            self._current_close = _safe_float(bar.get("close"), 0.0)
            option_mark = 0.0
            if open_option:
                current_opt = self.compute_theoretical_option_price(
                    self._current_close,
                    open_option["strike"],
                    max(0, open_option["dte_days"]),
                    open_option["iv"],
                    open_option["type"],
                )
                option_mark = current_opt * open_option["qty"] * 100.0
                open_option["dte_days"] = max(0, open_option["dte_days"] - 1)

            self._record_equity(bar["date"], self._current_close, option_mark)

            if open_option:
                current_opt = self.compute_theoretical_option_price(
                    self._current_close,
                    open_option["strike"],
                    max(0, open_option["dte_days"]),
                    open_option["iv"],
                    open_option["type"],
                )
                current_value = current_opt * open_option["qty"] * 100.0
                target_close = open_option["entry_credit"] * (1.0 - _safe_float(params.get("profit_take_pct"), 0.5))
                expired = open_option["dte_days"] <= 0
                if current_value <= target_close or expired:
                    if expired and open_option["type"] == "put" and self._current_close < open_option["strike"]:
                        shares = open_option["qty"] * 100
                        assignment_cost = open_option["strike"] * shares
                        self.cash -= assignment_cost
                        self.shares_held[symbol] = self.shares_held.get(symbol, 0) + shares
                        self.total_assignment_pnl -= assignment_cost
                        self._record_assignment(bar["date"], symbol, open_option["strike"], shares, open_option["strike"])
                        self._record_trade(
                            date=str(bar["date"]),
                            action="ASSIGNED",
                            symbol=symbol,
                            strike=round(open_option["strike"], 2),
                            type="put",
                            premium=0.0,
                            pnl=0.0,
                            fee=0.0,
                            reason="put_assignment",
                        )
                        state = "cc"
                    elif expired and open_option["type"] == "call" and self._current_close > open_option["strike"]:
                        shares = self.shares_held.get(symbol, 0)
                        if shares > 0:
                            proceeds = open_option["strike"] * shares
                            self.cash += proceeds
                            self.total_assignment_pnl += proceeds
                            self.shares_held[symbol] = 0
                        state = "csp"
                    else:
                        pnl = self._close_option(open_option, bar["date"], self._current_close, "profit_take" if current_value <= target_close else "expiry")
                        pnl_samples.append(pnl)
                    open_option = None
                continue

            if state == "csp":
                reserved = self._current_close * 100.0
                if self.cash >= reserved:
                    strike = self._current_close * (1.0 - abs(_safe_float(params.get("target_delta"), 0.25)) * 0.35)
                    open_option = self._sell_option(bar["date"], symbol, "put", strike, _safe_int(params.get("target_dte"), 30), iv, 1, "wheel_csp")
            else:
                shares = self.shares_held.get(symbol, 0)
                if shares >= 100:
                    strike = self._current_close * (1.0 + abs(_safe_float(params.get("target_delta"), 0.25)) * 0.35)
                    open_option = self._sell_option(bar["date"], symbol, "call", strike, _safe_int(params.get("target_dte"), 30), iv, max(1, shares // 100), "wheel_cc")

        return {
            "strategy": "wheel",
            "pnl_samples": pnl_samples,
            "total_premium_collected": round(self.total_premium_collected, 2),
        }

    def simulate_credit_spread(self, bars, params) -> dict:
        params = {"target_dte": 30, "width": 5.0, "profit_take_pct": 0.50, **dict(params or {})}
        symbol = self.config["symbol"]
        iv = self.estimate_iv_from_history(bars)
        sma200 = []
        closes = [_safe_float(row.get("close"), 0.0) for row in bars]
        for idx in range(len(closes)):
            if idx < 199:
                sma200.append(None)
            else:
                sma200.append(sum(closes[idx - 199: idx + 1]) / 200.0)

        open_spread = None
        spread_pnls = []
        for idx, bar in enumerate(bars):
            close = _safe_float(bar.get("close"), 0.0)
            self._current_close = close
            if open_spread:
                short_price = self.compute_theoretical_option_price(close, open_spread["short_strike"], open_spread["dte_days"], iv, open_spread["type"])
                long_price = self.compute_theoretical_option_price(close, open_spread["long_strike"], open_spread["dte_days"], iv, open_spread["type"])
                mark = max(0.0, (short_price - long_price) * 100.0)
                self._record_equity(bar["date"], close, mark)
                open_spread["dte_days"] = max(0, open_spread["dte_days"] - 1)
                loss_trigger = open_spread["credit"] * 1.5
                if mark <= open_spread["credit"] * (1.0 - _safe_float(params.get("profit_take_pct"), 0.5)) or mark >= loss_trigger or open_spread["dte_days"] <= 7:
                    fee = 2 * self.config["fee_per_contract"]
                    pnl = open_spread["credit"] - mark - fee
                    self.cash -= mark + fee
                    self.total_fees += fee
                    spread_pnls.append(pnl)
                    self._record_trade(
                        date=str(bar["date"]),
                        action="CLOSE_SPREAD",
                        symbol=symbol,
                        strike=f"{round(open_spread['short_strike'], 2)}/{round(open_spread['long_strike'], 2)}",
                        type=open_spread["type"],
                        premium=round(-mark, 2),
                        pnl=round(pnl, 2),
                        fee=round(fee, 2),
                        reason="credit_spread_exit",
                    )
                    open_spread = None
                continue

            self._record_equity(bar["date"], close, 0.0)
            trend_bull = sma200[idx] is not None and close >= _safe_float(sma200[idx], close)
            spread_type = "put" if trend_bull else "call"
            short_strike = close * (0.95 if trend_bull else 1.05)
            width = _safe_float(params.get("width"), 5.0)
            long_strike = short_strike - width if trend_bull else short_strike + width
            short_premium = self.compute_theoretical_option_price(close, short_strike, _safe_int(params.get("target_dte"), 30), iv, spread_type)
            long_premium = self.compute_theoretical_option_price(close, long_strike, _safe_int(params.get("target_dte"), 30), iv, spread_type)
            credit = max(0.05, (short_premium - long_premium) * 100.0)
            fee = 2 * self.config["fee_per_contract"]
            self.cash += max(0.0, credit - fee)
            self.total_fees += fee
            self.total_premium_collected += credit
            open_spread = {
                "type": spread_type,
                "short_strike": short_strike,
                "long_strike": long_strike,
                "dte_days": _safe_int(params.get("target_dte"), 30),
                "credit": credit,
            }
            self._record_trade(
                date=str(bar["date"]),
                action="OPEN_SPREAD",
                symbol=symbol,
                strike=f"{round(short_strike, 2)}/{round(long_strike, 2)}",
                type=spread_type,
                premium=round(credit, 2),
                pnl=0.0,
                fee=round(fee, 2),
                reason="credit_spread_entry",
            )
        return {"strategy": "credit_spread", "pnl_samples": spread_pnls}

    def simulate_iron_condor(self, bars, params) -> dict:
        params = {"target_dte": 35, "width": 3.0, "profit_take_pct": 0.50, **dict(params or {})}
        symbol = self.config["symbol"]
        iv = self.estimate_iv_from_history(bars)
        open_condor = None
        condor_pnls = []

        for bar in bars:
            close = _safe_float(bar.get("close"), 0.0)
            self._current_close = close
            if open_condor:
                short_put = self.compute_theoretical_option_price(close, open_condor["short_put"], open_condor["dte_days"], iv, "put")
                long_put = self.compute_theoretical_option_price(close, open_condor["long_put"], open_condor["dte_days"], iv, "put")
                short_call = self.compute_theoretical_option_price(close, open_condor["short_call"], open_condor["dte_days"], iv, "call")
                long_call = self.compute_theoretical_option_price(close, open_condor["long_call"], open_condor["dte_days"], iv, "call")
                mark = max(0.0, ((short_put - long_put) + (short_call - long_call)) * 100.0)
                self._record_equity(bar["date"], close, mark)
                open_condor["dte_days"] = max(0, open_condor["dte_days"] - 1)
                breached = close <= open_condor["short_put"] or close >= open_condor["short_call"]
                if mark <= open_condor["credit"] * (1.0 - _safe_float(params.get("profit_take_pct"), 0.5)) or breached or open_condor["dte_days"] <= 7:
                    fee = 4 * self.config["fee_per_contract"]
                    pnl = open_condor["credit"] - mark - fee
                    self.cash -= mark + fee
                    self.total_fees += fee
                    condor_pnls.append(pnl)
                    self._record_trade(
                        date=str(bar["date"]),
                        action="CLOSE_CONDOR",
                        symbol=symbol,
                        strike=f"{round(open_condor['short_put'], 2)}-{round(open_condor['short_call'], 2)}",
                        type="iron_condor",
                        premium=round(-mark, 2),
                        pnl=round(pnl, 2),
                        fee=round(fee, 2),
                        reason="iron_condor_exit",
                    )
                    open_condor = None
                continue

            self._record_equity(bar["date"], close, 0.0)
            width = _safe_float(params.get("width"), 3.0)
            short_put = close * 0.93
            long_put = short_put - width
            short_call = close * 1.07
            long_call = short_call + width
            legs = [
                self.compute_theoretical_option_price(close, short_put, _safe_int(params.get("target_dte"), 35), iv, "put"),
                self.compute_theoretical_option_price(close, long_put, _safe_int(params.get("target_dte"), 35), iv, "put"),
                self.compute_theoretical_option_price(close, short_call, _safe_int(params.get("target_dte"), 35), iv, "call"),
                self.compute_theoretical_option_price(close, long_call, _safe_int(params.get("target_dte"), 35), iv, "call"),
            ]
            credit = max(0.05, ((legs[0] - legs[1]) + (legs[2] - legs[3])) * 100.0)
            fee = 4 * self.config["fee_per_contract"]
            self.cash += max(0.0, credit - fee)
            self.total_fees += fee
            self.total_premium_collected += credit
            open_condor = {
                "short_put": short_put,
                "long_put": long_put,
                "short_call": short_call,
                "long_call": long_call,
                "dte_days": _safe_int(params.get("target_dte"), 35),
                "credit": credit,
            }
            self._record_trade(
                date=str(bar["date"]),
                action="OPEN_CONDOR",
                symbol=symbol,
                strike=f"{round(short_put, 2)}-{round(short_call, 2)}",
                type="iron_condor",
                premium=round(credit, 2),
                pnl=0.0,
                fee=round(fee, 2),
                reason="iron_condor_entry",
            )
        return {"strategy": "iron_condor", "pnl_samples": condor_pnls}

    def simulate_earnings_strangle(self, bars, params) -> dict:
        params = {"hold_days_before": 2, "hold_days_after": 1, "target_dte": 14, **dict(params or {})}
        symbol = self.config["symbol"]
        iv = self.estimate_iv_from_history(bars)
        earnings_dates = []
        if bars:
            start_year = datetime.fromisoformat(str(bars[0]["date"])).year
            end_year = datetime.fromisoformat(str(bars[-1]["date"])).year
            for year in range(start_year, end_year + 1):
                for month in (1, 4, 7, 10):
                    earnings_dates.append(datetime(year, month, 25, tzinfo=timezone.utc).date().isoformat())
        earnings_dates = sorted(set(earnings_dates))
        open_trade = None
        pnl_samples = []

        for idx, bar in enumerate(bars):
            close = _safe_float(bar.get("close"), 0.0)
            self._current_close = close
            trade_value = 0.0
            if open_trade:
                call_px = self.compute_theoretical_option_price(close, open_trade["call_strike"], open_trade["dte_days"], iv, "call")
                put_px = self.compute_theoretical_option_price(close, open_trade["put_strike"], open_trade["dte_days"], iv, "put")
                trade_value = (call_px + put_px) * 100.0
                open_trade["dte_days"] = max(0, open_trade["dte_days"] - 1)
            self._record_equity(bar["date"], close, trade_value)

            if open_trade and idx >= open_trade["exit_idx"]:
                debit = trade_value
                fee = 2 * self.config["fee_per_contract"]
                pnl = debit - open_trade["cost"] - fee
                self.cash += debit - fee
                self.total_fees += fee
                pnl_samples.append(pnl)
                self._record_trade(
                    date=str(bar["date"]),
                    action="SELL_STRANGLE",
                    symbol=symbol,
                    strike=f"{round(open_trade['put_strike'], 2)}-{round(open_trade['call_strike'], 2)}",
                    type="strangle",
                    premium=round(debit, 2),
                    pnl=round(pnl, 2),
                    fee=round(fee, 2),
                    reason="post_earnings_exit",
                )
                open_trade = None
                continue

            if open_trade:
                continue

            entry_idx = None
            exit_idx = None
            for earnings_date in earnings_dates:
                if earnings_date == str(bar["date"]):
                    entry_idx = max(0, idx - _safe_int(params.get("hold_days_before"), 2))
                    exit_idx = min(len(bars) - 1, idx + _safe_int(params.get("hold_days_after"), 1))
                    break
            if entry_idx is None or idx != entry_idx:
                continue

            call_strike = close * 1.03
            put_strike = close * 0.97
            cost = (
                self.compute_theoretical_option_price(close, call_strike, _safe_int(params.get("target_dte"), 14), iv, "call")
                + self.compute_theoretical_option_price(close, put_strike, _safe_int(params.get("target_dte"), 14), iv, "put")
            ) * 100.0
            fee = 2 * self.config["fee_per_contract"]
            total_cost = cost + fee
            if self.cash < total_cost:
                continue
            self.cash -= total_cost
            self.total_fees += fee
            open_trade = {
                "call_strike": call_strike,
                "put_strike": put_strike,
                "dte_days": _safe_int(params.get("target_dte"), 14),
                "cost": cost,
                "exit_idx": exit_idx,
            }
            self._record_trade(
                date=str(bar["date"]),
                action="BUY_STRANGLE",
                symbol=symbol,
                strike=f"{round(put_strike, 2)}-{round(call_strike, 2)}",
                type="strangle",
                premium=round(-cost, 2),
                pnl=0.0,
                fee=round(fee, 2),
                reason="pre_earnings_entry",
            )
        return {"strategy": "earnings_strangle", "pnl_samples": pnl_samples}

    def run(self) -> dict:
        self.cash = self.config["starting_capital"]
        self.positions = []
        self.shares_held = {}
        self.trade_log = []
        self.equity_curve = []
        self.assignment_log = []
        self.total_fees = 0.0
        self.total_premium_collected = 0.0
        self.total_assignment_pnl = 0.0

        start_date = _to_utc_date(self.config.get("start_date"), 180).isoformat()
        end_date = _to_utc_date(self.config.get("end_date"), 0).isoformat()
        bars = self.fetch_underlying_bars(self.config["symbol"], start_date, end_date)
        if not bars:
            result = {
                "equity_curve": [],
                "trade_log": [],
                "assignment_log": [],
                "summary": {
                    "starting_capital": self.config["starting_capital"],
                    "ending_capital": self.config["starting_capital"],
                    "total_return_pct": 0.0,
                    "total_premium_collected": 0.0,
                    "total_assignment_pnl": 0.0,
                    "win_rate": 0.0,
                    "profit_factor": 0.0,
                    "max_drawdown_pct": 0.0,
                    "avg_days_in_trade": 0.0,
                    "total_trades": 0,
                    "total_fees": 0.0,
                    "sharpe_ratio": 0.0,
                    "monthly_returns": [],
                },
            }
            self._last_result = result
            return result

        strategy = self.config["strategy"]
        params = dict(self.config.get("params") or {})
        if strategy == "credit_spread":
            meta = self.simulate_credit_spread(bars, params)
        elif strategy == "iron_condor":
            meta = self.simulate_iron_condor(bars, params)
        elif strategy == "earnings_strangle":
            meta = self.simulate_earnings_strangle(bars, params)
        else:
            meta = self.simulate_wheel(bars, params)

        ending_capital = _safe_float(self.equity_curve[-1]["equity"], self.cash) if self.equity_curve else self.cash
        total_return_pct = ((ending_capital - self.config["starting_capital"]) / self.config["starting_capital"]) * 100.0
        pnls = [_safe_float(row.get("pnl"), 0.0) for row in self.trade_log if _safe_float(row.get("pnl"), 0.0) != 0.0]
        wins = [value for value in pnls if value > 0]
        holds = []
        current_open = None
        for row in self.trade_log:
            action = str(row.get("action") or "")
            if action in {"SELL_TO_OPEN", "OPEN_SPREAD", "OPEN_CONDOR", "BUY_STRANGLE"}:
                current_open = row
            elif action in {"BUY_TO_CLOSE", "CLOSE_SPREAD", "CLOSE_CONDOR", "SELL_STRANGLE"} and current_open:
                holds.append(self._days_between(current_open["date"], row["date"]))
                current_open = None

        equity_values = [_safe_float(row.get("equity"), 0.0) for row in self.equity_curve]
        daily_returns = []
        for idx in range(1, len(equity_values)):
            prev = equity_values[idx - 1]
            curr = equity_values[idx]
            if prev > 0:
                daily_returns.append((curr - prev) / prev)
        avg_return = sum(daily_returns) / len(daily_returns) if daily_returns else 0.0
        vol_return = math.sqrt(sum((value - avg_return) ** 2 for value in daily_returns) / max(1, len(daily_returns) - 1)) if len(daily_returns) > 1 else 0.0
        sharpe = (avg_return / vol_return) * math.sqrt(252.0) if vol_return > 0 else 0.0

        monthly_groups = {}
        for row in self.equity_curve:
            key = str(row["date"])[:7]
            monthly_groups.setdefault(key, []).append(_safe_float(row.get("equity"), 0.0))
        monthly_returns = []
        for key, values in sorted(monthly_groups.items()):
            if len(values) >= 2 and values[0] > 0:
                monthly_returns.append({"month": key, "return_pct": round(((values[-1] - values[0]) / values[0]) * 100.0, 2)})

        summary = {
            "starting_capital": round(self.config["starting_capital"], 2),
            "ending_capital": round(ending_capital, 2),
            "total_return_pct": round(total_return_pct, 2),
            "total_premium_collected": round(self.total_premium_collected, 2),
            "total_assignment_pnl": round(self.total_assignment_pnl, 2),
            "win_rate": round((len(wins) / len(pnls)) * 100.0, 2) if pnls else 0.0,
            "profit_factor": round(_profit_factor(pnls), 3),
            "max_drawdown_pct": round(_max_drawdown_pct(self.equity_curve), 2),
            "avg_days_in_trade": round(sum(holds) / len(holds), 2) if holds else 0.0,
            "total_trades": len(self.trade_log),
            "total_fees": round(self.total_fees, 2),
            "sharpe_ratio": round(sharpe, 3),
            "monthly_returns": monthly_returns,
        }
        result = {
            "equity_curve": self.equity_curve,
            "trade_log": self.trade_log,
            "assignment_log": self.assignment_log,
            "summary": summary,
            "meta": meta,
        }
        self._last_result = result
        return result

    def get_summary(self) -> dict:
        if not self._last_result:
            self.run()
        return dict((self._last_result or {}).get("summary") or {})
