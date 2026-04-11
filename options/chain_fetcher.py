import math
import threading
import time
from datetime import datetime, timedelta, timezone

from brokers.ibkr_adapter import IBKRAdapter
from brokers.webull_adapter import WebullAdapter

_CHAIN_CACHE = {}
_CHAIN_CACHE_TTL = 300
_QUOTE_CACHE = {}
_QUOTE_CACHE_TTL = 60
_IV_HISTORY = {}
_IV_HISTORY_TTL = 366 * 24 * 60 * 60
_CACHE_LOCK = threading.RLock()


def _log(message):
    print(f"[option_chain_fetcher] {message}")


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


def _parse_expiration(expiration):
    try:
        return datetime.strptime(str(expiration or "").strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def _utcnow():
    return datetime.now(timezone.utc)


def _normal_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _normal_cdf(x):
    try:
        from scipy.stats import norm  # type: ignore

        return float(norm.cdf(x))
    except Exception:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _days_to_expiration(expiration):
    expiry = _parse_expiration(expiration)
    if expiry is None:
        return 0
    return max(0, (expiry - _utcnow().date()).days)


def _cache_get(store, key, ttl):
    with _CACHE_LOCK:
        row = store.get(key)
        if not isinstance(row, dict):
            return None
        if (time.time() - _safe_float(row.get("ts"), 0.0)) >= ttl:
            store.pop(key, None)
            return None
        return row.get("data")


def _cache_set(store, key, data):
    with _CACHE_LOCK:
        store[key] = {"ts": time.time(), "data": data}


def _record_iv(symbol, iv):
    iv = _safe_float(iv, 0.0)
    if iv <= 0:
        return
    now = time.time()
    with _CACHE_LOCK:
        rows = list(_IV_HISTORY.get(symbol, []))
        rows.append({"ts": now, "iv": iv})
        cutoff = now - _IV_HISTORY_TTL
        rows = [row for row in rows if _safe_float(row.get("ts"), 0.0) >= cutoff]
        _IV_HISTORY[symbol] = rows[-5000:]


def _sorted_unique_expirations(chains):
    values = []
    for expiration in chains.keys():
        exp_date = _parse_expiration(expiration)
        if exp_date is None:
            continue
        values.append((exp_date, expiration))
    values.sort(key=lambda row: row[0])
    return [expiration for _, expiration in values]


class OptionChainFetcher:
    def __init__(self, broker="webull"):
        self.broker_name = str(broker or "webull").strip().lower()
        self.chain_cache_ttl = _CHAIN_CACHE_TTL
        self.quote_cache_ttl = _QUOTE_CACHE_TTL
        self.adapter = self._build_adapter(self.broker_name)

    def _build_adapter(self, broker_name):
        if broker_name == "ibkr":
            return IBKRAdapter()
        if broker_name == "webull":
            return WebullAdapter()
        raise ValueError(f"unsupported_broker={broker_name}")

    def _normalize_contract(self, contract, expiration, side, underlying_price):
        row = contract if isinstance(contract, dict) else {}
        strike = _safe_float(row.get("strike"), 0.0)
        normalized = {
            "strike": strike,
            "bid": _safe_float(row.get("bid"), 0.0),
            "ask": _safe_float(row.get("ask"), 0.0),
            "last": _safe_float(row.get("last"), 0.0),
            "volume": _safe_int(row.get("volume"), 0),
            "open_interest": _safe_int(row.get("open_interest"), 0),
            "delta": row.get("delta"),
            "gamma": row.get("gamma"),
            "theta": row.get("theta"),
            "vega": row.get("vega"),
            "iv": row.get("iv"),
            "expiration": expiration,
            "option_type": side,
        }

        needs_greeks = any(normalized.get(key) in {None, ""} for key in ("delta", "gamma", "theta", "vega", "iv"))
        if needs_greeks:
            computed = self.compute_greeks(normalized, underlying_price)
            for key, value in computed.items():
                if normalized.get(key) in {None, ""}:
                    normalized[key] = value

        normalized["delta"] = _safe_float(normalized.get("delta"), 0.0)
        normalized["gamma"] = _safe_float(normalized.get("gamma"), 0.0)
        normalized["theta"] = _safe_float(normalized.get("theta"), 0.0)
        normalized["vega"] = _safe_float(normalized.get("vega"), 0.0)
        normalized["iv"] = _safe_float(normalized.get("iv"), 0.0)
        _record_iv(_normalize_symbol(row.get("symbol") or row.get("underlying") or ""), normalized["iv"])
        return normalized

    def _get_quote(self, symbol):
        symbol = _normalize_symbol(symbol)
        cache_key = (self.broker_name, symbol)
        cached = _cache_get(_QUOTE_CACHE, cache_key, self.quote_cache_ttl)
        if cached is not None:
            return cached

        quote = self.adapter.get_stock_quote(symbol)
        if isinstance(quote, dict) and quote.get("ok"):
            _cache_set(_QUOTE_CACHE, cache_key, quote)
            return quote
        return quote if isinstance(quote, dict) else {"ok": False, "error": "quote_unavailable", "symbol": symbol}

    def get_chain(self, symbol, expiration=None) -> dict:
        symbol = _normalize_symbol(symbol)
        exp_key = str(expiration or "").strip() or None
        cache_key = (self.broker_name, symbol, exp_key)
        cached = _cache_get(_CHAIN_CACHE, cache_key, self.chain_cache_ttl)
        if cached is not None:
            return cached

        try:
            quote = self._get_quote(symbol)
            underlying_price = _safe_float(quote.get("price"), 0.0)
            response = self.adapter.get_option_chain(symbol, expiration=exp_key)
            if not isinstance(response, dict) or not response.get("ok"):
                error = response.get("error") if isinstance(response, dict) else "chain_unavailable"
                return {"ok": False, "symbol": symbol, "error": str(error)}

            raw_chains = response.get("chains") or {}
            normalized_chains = {}
            expirations = _sorted_unique_expirations(raw_chains)
            if exp_key and exp_key not in expirations:
                expirations.append(exp_key)
                expirations = sorted(set(expirations))

            for exp in expirations:
                chain = raw_chains.get(exp) or {"calls": [], "puts": []}
                calls = [self._normalize_contract(row, exp, "call", underlying_price) for row in (chain.get("calls") or [])]
                puts = [self._normalize_contract(row, exp, "put", underlying_price) for row in (chain.get("puts") or [])]
                normalized_chains[exp] = {
                    "calls": sorted(calls, key=lambda row: _safe_float(row.get("strike"), 0.0)),
                    "puts": sorted(puts, key=lambda row: _safe_float(row.get("strike"), 0.0)),
                }

                all_ivs = [row.get("iv") for row in calls + puts if _safe_float(row.get("iv"), 0.0) > 0]
                if all_ivs:
                    _record_iv(symbol, sum(_safe_float(iv, 0.0) for iv in all_ivs) / len(all_ivs))

            result = {
                "ok": True,
                "symbol": symbol,
                "underlying_price": underlying_price,
                "expirations": expirations,
                "chains": normalized_chains,
            }
            _cache_set(_CHAIN_CACHE, cache_key, result)
            return result
        except Exception as exc:
            _log(f"get_chain failed symbol={symbol} error={exc}")
            return {"ok": False, "symbol": symbol, "error": str(exc)}

    def get_expirations(self, symbol) -> list:
        chain = self.get_chain(symbol)
        if not chain.get("ok"):
            return []

        results = []
        for expiration in chain.get("expirations") or []:
            days_out = _days_to_expiration(expiration)
            if 7 <= days_out <= 90:
                results.append(expiration)
        return results

    def get_atm_options(self, symbol, expiration) -> dict:
        chain = self.get_chain(symbol, expiration=expiration)
        if not chain.get("ok"):
            return {"ok": False, "error": chain.get("error"), "symbol": _normalize_symbol(symbol)}

        underlying_price = _safe_float(chain.get("underlying_price"), 0.0)
        calls = ((chain.get("chains") or {}).get(expiration) or {}).get("calls") or []
        puts = ((chain.get("chains") or {}).get(expiration) or {}).get("puts") or []

        def _closest(rows):
            if not rows:
                return None
            return min(rows, key=lambda row: abs(_safe_float(row.get("strike"), 0.0) - underlying_price))

        return {
            "ok": True,
            "call": _closest(calls),
            "put": _closest(puts),
            "underlying_price": underlying_price,
        }

    def get_otm_options(self, symbol, expiration, otm_pct=5.0) -> dict:
        chain = self.get_chain(symbol, expiration=expiration)
        if not chain.get("ok"):
            return {"ok": False, "error": chain.get("error"), "symbol": _normalize_symbol(symbol), "calls": [], "puts": []}

        underlying_price = _safe_float(chain.get("underlying_price"), 0.0)
        max_distance = abs(underlying_price) * (_safe_float(otm_pct, 5.0) / 100.0)
        chain_slice = (chain.get("chains") or {}).get(expiration) or {}
        calls = []
        puts = []
        for row in chain_slice.get("calls") or []:
            strike = _safe_float(row.get("strike"), 0.0)
            if strike > underlying_price and (strike - underlying_price) <= max_distance:
                calls.append(row)
        for row in chain_slice.get("puts") or []:
            strike = _safe_float(row.get("strike"), 0.0)
            if strike < underlying_price and (underlying_price - strike) <= max_distance:
                puts.append(row)
        return {"ok": True, "calls": calls, "puts": puts}

    def compute_greeks(self, option_data, underlying_price, risk_free_rate=0.05) -> dict:
        row = option_data if isinstance(option_data, dict) else {}
        option_type = str(row.get("option_type") or row.get("side") or "call").strip().lower()
        strike = _safe_float(row.get("strike"), 0.0)
        mid = _safe_float(row.get("last"), 0.0)
        if mid <= 0:
            bid = _safe_float(row.get("bid"), 0.0)
            ask = _safe_float(row.get("ask"), 0.0)
            if bid > 0 and ask > 0:
                mid = (bid + ask) / 2.0
            else:
                mid = max(bid, ask, 0.01)

        expiry = _parse_expiration(row.get("expiration"))
        if underlying_price <= 0 or strike <= 0 or expiry is None:
            return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "iv": 0.0}

        time_to_expiry = max((_parse_expiration(row.get("expiration")) - _utcnow().date()).days / 365.0, 1.0 / 365.0)
        sigma = _safe_float(row.get("iv"), 0.0)
        if sigma <= 0:
            sigma = self._solve_implied_vol(option_type, underlying_price, strike, risk_free_rate, time_to_expiry, mid)

        if sigma <= 0:
            sigma = 0.30

        sqrt_t = math.sqrt(time_to_expiry)
        d1 = (math.log(underlying_price / strike) + (risk_free_rate + 0.5 * sigma * sigma) * time_to_expiry) / (sigma * sqrt_t)
        d2 = d1 - sigma * sqrt_t

        if option_type == "put":
            delta = _normal_cdf(d1) - 1.0
        else:
            delta = _normal_cdf(d1)

        gamma = _normal_pdf(d1) / (underlying_price * sigma * sqrt_t)
        vega = underlying_price * _normal_pdf(d1) * sqrt_t / 100.0
        if option_type == "put":
            theta = (
                (-underlying_price * _normal_pdf(d1) * sigma / (2.0 * sqrt_t))
                + risk_free_rate * strike * math.exp(-risk_free_rate * time_to_expiry) * _normal_cdf(-d2)
            ) / 365.0
        else:
            theta = (
                (-underlying_price * _normal_pdf(d1) * sigma / (2.0 * sqrt_t))
                - risk_free_rate * strike * math.exp(-risk_free_rate * time_to_expiry) * _normal_cdf(d2)
            ) / 365.0

        return {
            "delta": round(delta, 6),
            "gamma": round(gamma, 6),
            "theta": round(theta, 6),
            "vega": round(vega, 6),
            "iv": round(sigma, 6),
        }

    def _black_scholes_price(self, option_type, spot, strike, rate, time_to_expiry, sigma):
        if spot <= 0 or strike <= 0 or time_to_expiry <= 0 or sigma <= 0:
            return 0.0
        sqrt_t = math.sqrt(time_to_expiry)
        d1 = (math.log(spot / strike) + (rate + 0.5 * sigma * sigma) * time_to_expiry) / (sigma * sqrt_t)
        d2 = d1 - sigma * sqrt_t
        if option_type == "put":
            return strike * math.exp(-rate * time_to_expiry) * _normal_cdf(-d2) - spot * _normal_cdf(-d1)
        return spot * _normal_cdf(d1) - strike * math.exp(-rate * time_to_expiry) * _normal_cdf(d2)

    def _solve_implied_vol(self, option_type, spot, strike, rate, time_to_expiry, premium):
        premium = max(_safe_float(premium, 0.0), 0.0)
        if premium <= 0:
            return 0.0

        low = 0.01
        high = 5.0
        for _ in range(40):
            mid = (low + high) / 2.0
            price = self._black_scholes_price(option_type, spot, strike, rate, time_to_expiry, mid)
            if price > premium:
                high = mid
            else:
                low = mid
        return (low + high) / 2.0

    def get_iv_rank(self, symbol, lookback_days=365) -> float:
        symbol = _normalize_symbol(symbol)
        chain = self.get_chain(symbol)
        if chain.get("ok"):
            all_ivs = []
            for expiry_data in (chain.get("chains") or {}).values():
                for side in ("calls", "puts"):
                    for row in expiry_data.get(side) or []:
                        iv = _safe_float(row.get("iv"), 0.0)
                        if iv > 0:
                            all_ivs.append(iv)
            if all_ivs:
                _record_iv(symbol, sum(all_ivs) / len(all_ivs))

        with _CACHE_LOCK:
            rows = list(_IV_HISTORY.get(symbol, []))

        if not rows:
            return 0.0

        cutoff = time.time() - (max(1, int(lookback_days or 365)) * 86400)
        values = [_safe_float(row.get("iv"), 0.0) for row in rows if _safe_float(row.get("ts"), 0.0) >= cutoff and _safe_float(row.get("iv"), 0.0) > 0]
        if not values:
            return 0.0

        current_iv = values[-1]
        low_iv = min(values)
        high_iv = max(values)
        if high_iv <= low_iv:
            return 0.0
        return round(((current_iv - low_iv) / (high_iv - low_iv)) * 100.0, 2)
