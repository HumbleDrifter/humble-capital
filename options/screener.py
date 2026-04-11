import math
import time

from options.chain_fetcher import OptionChainFetcher
from options.sentiment import SocialSentimentScanner

_SCAN_CACHE = {}
_SCAN_CACHE_TTL = 900

DEFAULT_WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AMD",
    "SPY", "QQQ", "IWM", "NFLX", "DIS", "BA", "JPM", "GS", "BAC",
    "XOM", "CVX", "PFE", "JNJ", "V", "MA", "COST", "WMT",
    "HD", "LOW", "CRM", "ORCL", "ADBE", "INTC", "MU", "QCOM",
    "COIN", "MARA", "RIOT", "SQ", "PYPL", "UBER", "ABNB",
    "PLTR", "SOFI", "NIO", "RIVN", "F", "GM", "T", "VZ", "KO", "PEP",
]

MEME_STOCKS = {
    "GME", "AMC", "BBAI", "NIO", "BYND", "OPEN", "MARA", "RIOT",
    "SOFI", "PLTR", "RIVN", "HOOD", "CLOV", "WISH", "SPCE", "WKHS",
    "BIOR", "MULN", "BBBY", "FFIE", "NKLA", "XELA", "ATER", "PROG",
    "ASST", "CXAI", "SNDL", "EXPR", "KOSS", "NAKD",
}

SQUEEZE_MENTION_THRESHOLD = 50    # min social mentions to qualify
SQUEEZE_IV_MIN = 40.0             # min IV rank for squeeze confirmation
SQUEEZE_SENTIMENT_MIN = 10.0      # min composite sentiment score
SQUEEZE_MAX_CAPITAL_PCT = 1.0     # allow up to 100% of available capital
SQUEEZE_CALL_DELTA_MIN = 0.25     # slightly OTM calls
SQUEEZE_CALL_DELTA_MAX = 0.55     # not too deep ITM
SQUEEZE_DTE_MIN = 7               # short dated for max leverage
SQUEEZE_DTE_MAX = 30              # but not weeklies

WHEEL_PARAMS = {
    "delta_min": 0.18,
    "delta_max": 0.35,
    "dte_min": 14,
    "dte_max": 45,
    "min_monthly_yield_pct": 1.0,
    "min_open_interest": 100,
    "min_earnings_buffer_days": 14,
}

CREDIT_SPREAD_PARAMS = {
    "delta_min": 0.20,
    "delta_max": 0.30,
    "dte_min": 14,
    "dte_max": 45,
    "min_credit_width_ratio": 0.30,
    "min_open_interest": 100,
    "min_width": 2.0,
    "max_width": 5.0,
}

EARNINGS_STRANGLE_PARAMS = {
    "lookahead_days": 7,
    "historical_move_min_pct": 8.0,
    "target_delta_min": 0.25,
    "target_delta_max": 0.35,
    "dte_min": 7,
    "dte_max": 21,
    "min_iv_rank": 35.0,
}


def _log(message):
    print(f"[options_screener] {message}")


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


def _mid_price(contract):
    contract = contract if isinstance(contract, dict) else {}
    bid = _safe_float(contract.get("bid"), 0.0)
    ask = _safe_float(contract.get("ask"), 0.0)
    last = _safe_float(contract.get("last"), 0.0)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return max(last, bid, ask, 0.0)


def _dte(expiration):
    try:
        from datetime import datetime, timezone

        exp = datetime.strptime(str(expiration or "").strip(), "%Y-%m-%d").date()
        now = datetime.now(timezone.utc).date()
        return max(0, (exp - now).days)
    except Exception:
        return 0


def _annualized_yield(premium_received, capital_at_risk, dte):
    premium_received = _safe_float(premium_received, 0.0)
    capital_at_risk = _safe_float(capital_at_risk, 0.0)
    dte = max(1, _safe_int(dte, 0))
    if premium_received <= 0 or capital_at_risk <= 0:
        return 0.0
    return (premium_received / capital_at_risk) * (365.0 / dte) * 100.0


def is_meme_squeeze(symbol, sentiment, iv_rank):
    """
    Returns True if this symbol meets meme squeeze criteria:
    - In the known meme stock universe
    - Social sentiment is bullish and trending
    - Mentions above threshold
    - IV rank elevated (unusual options activity)
    """
    symbol = _normalize_symbol(symbol)
    if symbol not in MEME_STOCKS:
        return False
    mentions = _safe_int((sentiment or {}).get("total_mentions"), 0)
    composite = _safe_float((sentiment or {}).get("composite_score"), 0.0)
    trending = _safe_int((sentiment or {}).get("trending_sources"), 0) > 0
    iv = _safe_float(iv_rank, 0.0)
    return (
        mentions >= SQUEEZE_MENTION_THRESHOLD
        and composite >= SQUEEZE_SENTIMENT_MIN
        and trending
        and iv >= SQUEEZE_IV_MIN
    )


def allocate_meme_squeeze_capital(conviction_score):
    """
    For confirmed meme squeeze plays, bypass normal strategy caps.
    conviction_score: 0.0-1.0 from sentiment/scoring engine.
    Returns allocation fraction (0.0-1.0) of available options buying power.
    """
    conviction_score = max(0.0, min(1.0, _safe_float(conviction_score, 0.5)))
    if conviction_score >= 0.85:
        return SQUEEZE_MAX_CAPITAL_PCT        # 100% — maximum aggression
    if conviction_score >= 0.70:
        return 0.75
    if conviction_score >= 0.55:
        return 0.60
    return 0.40                               # minimum for a confirmed squeeze


def _prob_from_delta(delta):
    delta = abs(_safe_float(delta, 0.0))
    return max(0.0, min(0.99, 1.0 - delta))


def _risk_reward_ratio(credit, width):
    credit = _safe_float(credit, 0.0)
    width = _safe_float(width, 0.0)
    risk = max(0.01, width - credit)
    return credit / risk


def allocate_options_capital(vix_level):
    vix_level = _safe_float(vix_level, 0.0)
    if vix_level >= 28:
        return {
            "covered_call": 0.25,
            "cash_secured_put": 0.35,
            "bull_put": 0.20,
            "bear_call": 0.05,
            "iron_condor": 0.15,
            "wheel": 0.35,
            "earnings_strangle": 0.05,
        }
    if vix_level >= 20:
        return {
            "covered_call": 0.20,
            "cash_secured_put": 0.25,
            "bull_put": 0.20,
            "bear_call": 0.10,
            "iron_condor": 0.15,
            "wheel": 0.30,
            "earnings_strangle": 0.10,
        }
    return {
        "covered_call": 0.15,
        "cash_secured_put": 0.20,
        "bull_put": 0.15,
        "bear_call": 0.10,
        "iron_condor": 0.10,
        "wheel": 0.20,
        "earnings_strangle": 0.30,
    }


class OptionsScreener:
    def __init__(self, watchlist=None, broker="webull"):
        self.watchlist = [_normalize_symbol(symbol) for symbol in (watchlist or DEFAULT_WATCHLIST) if _normalize_symbol(symbol)]
        self.broker = str(broker or "webull").strip().lower()
        self.chain_fetcher = OptionChainFetcher(broker=self.broker)

    def _cache_key(self, strategies, sentiment_filter=None):
        strategies = tuple(sorted(str(item).strip().lower() for item in (strategies or [])))
        sentiment_filter = str(sentiment_filter or "").strip().lower()
        return (self.broker, tuple(self.watchlist), strategies, sentiment_filter)

    def _cache_get(self, key):
        row = _SCAN_CACHE.get(key)
        if not isinstance(row, dict):
            return None
        if (time.time() - _safe_float(row.get("ts"), 0.0)) >= _SCAN_CACHE_TTL:
            _SCAN_CACHE.pop(key, None)
            return None
        return row.get("data")

    def _cache_set(self, key, data):
        _SCAN_CACHE[key] = {"ts": time.time(), "data": data}

    def _iter_tradeable_expirations(self, symbol):
        expirations = self.chain_fetcher.get_expirations(symbol)
        return [exp for exp in expirations if 14 <= _dte(exp) <= 45]

    def _get_positions_by_symbol(self):
        try:
            rows = self.chain_fetcher.adapter.get_positions()
        except Exception:
            rows = []
        grouped = {}
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            symbol = _normalize_symbol(row.get("symbol"))
            if not symbol:
                continue
            grouped.setdefault(symbol, []).append(row)
        return grouped

    def _days_to_next_earnings(self, symbol):
        try:
            quote = self.chain_fetcher._get_quote(symbol)
            earnings_date = (
                (quote or {}).get("next_earnings_date")
                or (quote or {}).get("earnings_date")
                or (quote or {}).get("earningsDate")
            )
            if not earnings_date:
                return None
            return _dte(earnings_date)
        except Exception:
            return None

    def _historical_earnings_move_pct(self, symbol):
        try:
            quote = self.chain_fetcher._get_quote(symbol)
            move = (
                (quote or {}).get("historical_earnings_move_pct")
                or (quote or {}).get("avg_earnings_move_pct")
                or (quote or {}).get("earnings_move_pct")
            )
            if move is not None:
                return _safe_float(move, 0.0)
        except Exception:
            pass
        return 0.0

    def scan_meme_calls(self, symbol) -> list:
        """
        Scan for long call opportunities on meme/squeeze candidates.
        Uses wider delta range and shorter DTE than income strategies.
        Returns opportunities tagged with is_meme_squeeze=True.
        """
        symbol = _normalize_symbol(symbol)
        opportunities = []
        try:
            quote = self.chain_fetcher.get_quote(symbol)
            current_price = _safe_float((quote or {}).get("price"), 0.0)
            if current_price <= 0:
                return []

            chain = self.chain_fetcher.get_chain(symbol)
            calls = [
                leg for exp in (chain or {}).get("expirations", [])
                for leg in exp.get("calls", [])
                if SQUEEZE_DTE_MIN <= _safe_int(leg.get("dte"), 0) <= SQUEEZE_DTE_MAX
            ]

            for call in calls:
                delta = abs(_safe_float(call.get("delta"), 0.0))
                if not (SQUEEZE_CALL_DELTA_MIN <= delta <= SQUEEZE_CALL_DELTA_MAX):
                    continue
                strike = _safe_float(call.get("strike"), 0.0)
                if strike <= 0:
                    continue
                ask = _safe_float(call.get("ask"), 0.0)
                bid = _safe_float(call.get("bid"), 0.0)
                last = _safe_float(call.get("last"), 0.0)
                mid = (bid + ask) / 2.0 if bid > 0 and ask > 0 else last
                if mid <= 0:
                    continue
                oi = _safe_int(call.get("open_interest"), 0)
                vol = _safe_int(call.get("volume"), 0)
                dte = _safe_int(call.get("dte"), 0)
                iv_raw = _safe_float(call.get("iv"), 0.0)

                # Cost per contract (100 shares)
                cost_per_contract = mid * 100.0
                # Potential gain: price needs to reach strike + premium
                breakeven = strike + mid
                upside_pct = ((breakeven / current_price) - 1.0) * 100.0

                opportunities.append({
                    "symbol": symbol,
                    "strategy": "meme_call",
                    "is_meme_squeeze": True,
                    "strike": round(strike, 2),
                    "expiration": str(call.get("expiration") or ""),
                    "dte": dte,
                    "delta": round(delta, 3),
                    "iv": round(iv_raw * 100.0, 1) if iv_raw < 5 else round(iv_raw, 1),
                    "bid": round(bid, 2),
                    "ask": round(ask, 2),
                    "mid": round(mid, 2),
                    "cost_per_contract": round(cost_per_contract, 2),
                    "breakeven": round(breakeven, 2),
                    "upside_needed_pct": round(upside_pct, 2),
                    "open_interest": oi,
                    "volume": vol,
                    "liquidity_score": float(oi * max(1, vol)),
                    "current_price": round(current_price, 2),
                    "prob_profit": round(delta, 3),
                    "risk_reward_ratio": 0.0,
                    "annualized_yield": 0.0,
                })
        except Exception as exc:
            _log(f"scan_meme_calls failed symbol={symbol} error={exc}")
        return opportunities

    def scan_covered_calls(self, symbol, shares_held=100) -> list:
        symbol = _normalize_symbol(symbol)
        quote = self.chain_fetcher._get_quote(symbol)
        current_price = _safe_float((quote or {}).get("price"), 0.0)
        if current_price <= 0 or _safe_int(shares_held, 0) < 100:
            return []

        opportunities = []
        for expiration in self._iter_tradeable_expirations(symbol):
            chain = self.chain_fetcher.get_chain(symbol, expiration=expiration)
            if not chain.get("ok"):
                continue
            calls = ((chain.get("chains") or {}).get(expiration) or {}).get("calls") or []
            dte = _dte(expiration)
            for call in calls:
                delta = abs(_safe_float(call.get("delta"), 0.0))
                oi = _safe_int(call.get("open_interest"), 0)
                strike = _safe_float(call.get("strike"), 0.0)
                premium = _mid_price(call)
                if not (0.20 <= delta <= 0.35):
                    continue
                if oi <= 100 or premium <= 0 or strike <= current_price:
                    continue
                monthly_yield_pct = (premium / current_price) * (30.0 / max(1, dte)) * 100.0
                if monthly_yield_pct <= 1.0:
                    continue
                annualized = _annualized_yield(premium * 100.0, current_price * 100.0, dte)
                opportunities.append({
                    "symbol": symbol,
                    "strategy": "covered_call",
                    "expiration": expiration,
                    "details": {
                        "strike": strike,
                        "premium": round(premium, 2),
                        "delta": round(delta, 3),
                        "open_interest": oi,
                        "shares_covered": _safe_int(shares_held, 100),
                    },
                    "annualized_yield": round(annualized, 2),
                    "prob_profit": round(_prob_from_delta(delta), 3),
                    "iv_rank": self.chain_fetcher.get_iv_rank(symbol),
                    "risk_reward_ratio": 3.0,
                    "liquidity_score": oi * max(1, _safe_int(call.get("volume"), 0)),
                    "risk_reward": "covered",
                })
        return sorted(opportunities, key=lambda row: row.get("annualized_yield", 0.0), reverse=True)

    def scan_cash_secured_puts(self, symbol, max_capital=None) -> list:
        symbol = _normalize_symbol(symbol)
        quote = self.chain_fetcher._get_quote(symbol)
        current_price = _safe_float((quote or {}).get("price"), 0.0)
        if current_price <= 0:
            return []

        max_capital = _safe_float(max_capital, 0.0)
        opportunities = []
        for expiration in self._iter_tradeable_expirations(symbol):
            chain = self.chain_fetcher.get_chain(symbol, expiration=expiration)
            if not chain.get("ok"):
                continue
            puts = ((chain.get("chains") or {}).get(expiration) or {}).get("puts") or []
            dte = _dte(expiration)
            for put in puts:
                delta = _safe_float(put.get("delta"), 0.0)
                abs_delta = abs(delta)
                oi = _safe_int(put.get("open_interest"), 0)
                strike = _safe_float(put.get("strike"), 0.0)
                premium = _mid_price(put)
                capital_at_risk = strike * 100.0
                if not (0.20 <= abs_delta <= 0.35):
                    continue
                if oi <= 100 or premium <= 0 or strike >= current_price * 0.95:
                    continue
                if max_capital > 0 and capital_at_risk > max_capital:
                    continue
                monthly_yield_pct = (premium / strike) * (30.0 / max(1, dte)) * 100.0
                if monthly_yield_pct <= 1.0:
                    continue
                annualized = _annualized_yield(premium * 100.0, capital_at_risk, dte)
                opportunities.append({
                    "symbol": symbol,
                    "strategy": "cash_secured_put",
                    "expiration": expiration,
                    "details": {
                        "strike": strike,
                        "premium": round(premium, 2),
                        "delta": round(delta, 3),
                        "open_interest": oi,
                        "secured_capital": round(capital_at_risk, 2),
                    },
                    "annualized_yield": round(annualized, 2),
                    "prob_profit": round(_prob_from_delta(delta), 3),
                    "iv_rank": self.chain_fetcher.get_iv_rank(symbol),
                    "risk_reward_ratio": 2.5,
                    "liquidity_score": oi * max(1, _safe_int(put.get("volume"), 0)),
                    "risk_reward": "secured",
                })
        return sorted(opportunities, key=lambda row: row.get("annualized_yield", 0.0), reverse=True)

    def scan_credit_spreads(self, symbol, spread_type="bull_put") -> list:
        symbol = _normalize_symbol(symbol)
        spread_type = str(spread_type or "bull_put").strip().lower()
        quote = self.chain_fetcher._get_quote(symbol)
        current_price = _safe_float((quote or {}).get("price"), 0.0)
        if current_price <= 0:
            return []

        opportunities = []
        for expiration in self._iter_tradeable_expirations(symbol):
            chain = self.chain_fetcher.get_chain(symbol, expiration=expiration)
            if not chain.get("ok"):
                continue
            side_key = "puts" if spread_type == "bull_put" else "calls"
            contracts = ((chain.get("chains") or {}).get(expiration) or {}).get(side_key) or []
            dte = _dte(expiration)
            for short_leg in contracts:
                short_delta = abs(_safe_float(short_leg.get("delta"), 0.0))
                if not (0.20 <= short_delta <= 0.30):
                    continue
                for long_leg in contracts:
                    short_strike = _safe_float(short_leg.get("strike"), 0.0)
                    long_strike = _safe_float(long_leg.get("strike"), 0.0)
                    width = abs(short_strike - long_strike)
                    if width < 2 or width > 5:
                        continue
                    if spread_type == "bull_put" and not (long_strike < short_strike):
                        continue
                    if spread_type == "bear_call" and not (long_strike > short_strike):
                        continue
                    credit = _mid_price(short_leg) - _mid_price(long_leg)
                    if credit <= 0 or credit < (width * 0.30):
                        continue
                    liquidity = min(_safe_int(short_leg.get("open_interest"), 0), _safe_int(long_leg.get("open_interest"), 0))
                    if liquidity <= 100:
                        continue
                    rr = _risk_reward_ratio(credit, width)
                    opportunities.append({
                        "symbol": symbol,
                        "strategy": spread_type,
                        "expiration": expiration,
                        "details": {
                            "short_strike": short_strike,
                            "long_strike": long_strike,
                            "credit": round(credit, 2),
                            "width": round(width, 2),
                            "delta": round(short_delta, 3),
                        },
                        "annualized_yield": round(_annualized_yield(credit * 100.0, (width - credit) * 100.0, dte), 2),
                        "prob_profit": round(_prob_from_delta(short_delta), 3),
                        "iv_rank": self.chain_fetcher.get_iv_rank(symbol),
                        "risk_reward_ratio": round(rr, 3),
                        "liquidity_score": liquidity * max(1, _safe_int(short_leg.get("volume"), 0)),
                        "risk_reward": f"{rr:.2f}:1",
                    })
        return sorted(opportunities, key=lambda row: row.get("risk_reward_ratio", 0.0), reverse=True)

    def scan_iron_condors(self, symbol) -> list:
        symbol = _normalize_symbol(symbol)
        iv_rank = self.chain_fetcher.get_iv_rank(symbol)
        if iv_rank <= 50:
            return []

        opportunities = []
        for expiration in self._iter_tradeable_expirations(symbol):
            dte = _dte(expiration)
            if not (30 <= dte <= 45):
                continue
            chain = self.chain_fetcher.get_chain(symbol, expiration=expiration)
            if not chain.get("ok"):
                continue
            expiry_chain = (chain.get("chains") or {}).get(expiration) or {}
            calls = expiry_chain.get("calls") or []
            puts = expiry_chain.get("puts") or []
            short_calls = [row for row in calls if 0.15 <= abs(_safe_float(row.get("delta"), 0.0)) <= 0.20]
            short_puts = [row for row in puts if 0.15 <= abs(_safe_float(row.get("delta"), 0.0)) <= 0.20]
            for short_call in short_calls:
                for short_put in short_puts:
                    call_candidates = [row for row in calls if 2 <= (_safe_float(row.get("strike"), 0.0) - _safe_float(short_call.get("strike"), 0.0)) <= 3]
                    put_candidates = [row for row in puts if 2 <= (_safe_float(short_put.get("strike"), 0.0) - _safe_float(row.get("strike"), 0.0)) <= 3]
                    if not call_candidates or not put_candidates:
                        continue
                    long_call = call_candidates[0]
                    long_put = put_candidates[-1]
                    call_width = _safe_float(long_call.get("strike"), 0.0) - _safe_float(short_call.get("strike"), 0.0)
                    put_width = _safe_float(short_put.get("strike"), 0.0) - _safe_float(long_put.get("strike"), 0.0)
                    total_width = min(call_width, put_width)
                    credit = (
                        _mid_price(short_call) - _mid_price(long_call)
                        + _mid_price(short_put) - _mid_price(long_put)
                    )
                    if total_width <= 0 or credit <= 0 or credit < (total_width * 0.33):
                        continue
                    pop = min(_prob_from_delta(_safe_float(short_call.get("delta"), 0.0)), _prob_from_delta(_safe_float(short_put.get("delta"), 0.0)))
                    liquidity = min(
                        _safe_int(short_call.get("open_interest"), 0),
                        _safe_int(short_put.get("open_interest"), 0),
                        _safe_int(long_call.get("open_interest"), 0),
                        _safe_int(long_put.get("open_interest"), 0),
                    )
                    if liquidity <= 100:
                        continue
                    rr = _risk_reward_ratio(credit, total_width)
                    opportunities.append({
                        "symbol": symbol,
                        "strategy": "iron_condor",
                        "expiration": expiration,
                        "details": {
                            "short_call": _safe_float(short_call.get("strike"), 0.0),
                            "long_call": _safe_float(long_call.get("strike"), 0.0),
                            "short_put": _safe_float(short_put.get("strike"), 0.0),
                            "long_put": _safe_float(long_put.get("strike"), 0.0),
                            "credit": round(credit, 2),
                            "width": round(total_width, 2),
                        },
                        "annualized_yield": round(_annualized_yield(credit * 100.0, (total_width - credit) * 100.0, dte), 2),
                        "prob_profit": round(pop, 3),
                        "iv_rank": round(iv_rank, 2),
                        "risk_reward_ratio": round(rr, 3),
                        "liquidity_score": liquidity,
                        "risk_reward": f"{rr:.2f}:1",
                    })
        return sorted(opportunities, key=lambda row: (row.get("annualized_yield", 0.0), row.get("prob_profit", 0.0)), reverse=True)

    def scan_wheel_opportunities(self, symbol) -> list:
        symbol = _normalize_symbol(symbol)
        params = dict(WHEEL_PARAMS)
        earnings_days = self._days_to_next_earnings(symbol)
        if earnings_days is not None and earnings_days < _safe_int(params.get("min_earnings_buffer_days"), 14):
            return []

        quote = self.chain_fetcher._get_quote(symbol)
        current_price = _safe_float((quote or {}).get("price"), 0.0)
        if current_price <= 0:
            return []

        positions = self._get_positions_by_symbol()
        held_rows = positions.get(symbol, [])
        shares_held = sum(_safe_float(row.get("qty"), 0.0) for row in held_rows if _normalize_asset_type(row.get("asset_type")) == "stock")
        phase = "covered_call" if shares_held >= 100 else "cash_secured_put"

        opportunities = []
        expirations = self.chain_fetcher.get_expirations(symbol)
        expirations = [exp for exp in expirations if _safe_int(params["dte_min"]) <= _dte(exp) <= _safe_int(params["dte_max"])]
        for expiration in expirations:
            chain = self.chain_fetcher.get_chain(symbol, expiration=expiration)
            if not chain.get("ok"):
                continue
            dte = _dte(expiration)
            side_key = "calls" if phase == "covered_call" else "puts"
            contracts = ((chain.get("chains") or {}).get(expiration) or {}).get(side_key) or []
            for contract in contracts:
                delta = abs(_safe_float(contract.get("delta"), 0.0))
                if not (_safe_float(params["delta_min"]) <= delta <= _safe_float(params["delta_max"])):
                    continue
                oi = _safe_int(contract.get("open_interest"), 0)
                if oi < _safe_int(params["min_open_interest"]):
                    continue
                strike = _safe_float(contract.get("strike"), 0.0)
                premium = _mid_price(contract)
                if premium <= 0:
                    continue
                if phase == "covered_call" and strike <= current_price:
                    continue
                if phase == "cash_secured_put" and strike >= current_price * 0.95:
                    continue
                monthly_yield_pct = (premium / max(strike, current_price, 0.01)) * (30.0 / max(1, dte)) * 100.0
                if monthly_yield_pct < _safe_float(params["min_monthly_yield_pct"], 1.0):
                    continue
                capital_at_risk = (current_price * 100.0) if phase == "covered_call" else (strike * 100.0)
                opp = {
                    "symbol": symbol,
                    "strategy": "wheel",
                    "expiration": expiration,
                    "details": {
                        "phase": phase,
                        "strike": strike,
                        "premium": round(premium, 2),
                        "delta": round(delta, 3),
                        "open_interest": oi,
                        "shares_held": round(shares_held, 2),
                        "secured_capital": round(capital_at_risk, 2),
                    },
                    "annualized_yield": round(_annualized_yield(premium * 100.0, capital_at_risk, dte), 2),
                    "prob_profit": round(_prob_from_delta(delta), 3),
                    "iv_rank": round(self.chain_fetcher.get_iv_rank(symbol), 2),
                    "risk_reward_ratio": 2.5 if phase == "cash_secured_put" else 3.0,
                    "liquidity_score": oi * max(1, _safe_int(contract.get("volume"), 0)),
                    "risk_reward": "wheel",
                }
                opp["score"] = self.score_opportunity_v2(opp)
                opportunities.append(opp)
        return sorted(opportunities, key=lambda row: row.get("score", 0.0), reverse=True)

    def _get_upcoming_earnings(self):
        upcoming = []
        now_ts = int(time.time())
        max_days = _safe_int(EARNINGS_STRANGLE_PARAMS.get("lookahead_days"), 7)
        for symbol in self.watchlist:
            days = self._days_to_next_earnings(symbol)
            if days is None or days < 0 or days > max_days:
                continue
            upcoming.append({"symbol": symbol, "days": days, "ts": now_ts + days * 86400})
        upcoming.sort(key=lambda row: row["days"])
        return upcoming

    def scan_earnings_strangles(self) -> list:
        params = dict(EARNINGS_STRANGLE_PARAMS)
        opportunities = []
        for item in self._get_upcoming_earnings():
            symbol = item["symbol"]
            historical_move = self._historical_earnings_move_pct(symbol)
            if historical_move < _safe_float(params["historical_move_min_pct"], 8.0):
                continue
            iv_rank = self.chain_fetcher.get_iv_rank(symbol)
            if iv_rank < _safe_float(params["min_iv_rank"], 35.0):
                continue
            for expiration in self.chain_fetcher.get_expirations(symbol):
                dte = _dte(expiration)
                if not (_safe_int(params["dte_min"]) <= dte <= _safe_int(params["dte_max"])):
                    continue
                chain = self.chain_fetcher.get_chain(symbol, expiration=expiration)
                if not chain.get("ok"):
                    continue
                expiry_chain = (chain.get("chains") or {}).get(expiration) or {}
                calls = expiry_chain.get("calls") or []
                puts = expiry_chain.get("puts") or []
                target_min = _safe_float(params["target_delta_min"], 0.25)
                target_max = _safe_float(params["target_delta_max"], 0.35)
                short_call = next((row for row in calls if target_min <= abs(_safe_float(row.get("delta"), 0.0)) <= target_max), None)
                short_put = next((row for row in puts if target_min <= abs(_safe_float(row.get("delta"), 0.0)) <= target_max), None)
                if not short_call or not short_put:
                    continue
                call_premium = _mid_price(short_call)
                put_premium = _mid_price(short_put)
                total_credit = call_premium + put_premium
                if total_credit <= 0:
                    continue
                breakeven_width = total_credit / max(_safe_float((chain.get("underlying_price") or 0.0), 1.0), 0.01) * 100.0
                iv_crush_edge = max(0.0, historical_move - breakeven_width)
                opp = {
                    "symbol": symbol,
                    "strategy": "earnings_strangle",
                    "expiration": expiration,
                    "details": {
                        "short_call_strike": _safe_float(short_call.get("strike"), 0.0),
                        "short_put_strike": _safe_float(short_put.get("strike"), 0.0),
                        "credit": round(total_credit, 2),
                        "historical_move_pct": round(historical_move, 2),
                        "earnings_days": item["days"],
                        "iv_crush_edge": round(iv_crush_edge, 2),
                    },
                    "annualized_yield": round(_annualized_yield(total_credit * 100.0, max(1.0, total_credit * 100.0) * 2.0, dte), 2),
                    "prob_profit": round(min(0.95, 0.50 + max(0.0, iv_crush_edge) / 100.0), 3),
                    "iv_rank": round(iv_rank, 2),
                    "risk_reward_ratio": round(max(0.1, total_credit / max(0.01, historical_move / 100.0)), 3),
                    "liquidity_score": min(
                        _safe_int(short_call.get("open_interest"), 0),
                        _safe_int(short_put.get("open_interest"), 0),
                    ) * max(1, _safe_int(short_call.get("volume"), 0) + _safe_int(short_put.get("volume"), 0)),
                    "risk_reward": "earnings",
                }
                opp["score"] = self.score_opportunity_v2(opp)
                opportunities.append(opp)
                break
        return sorted(opportunities, key=lambda row: row.get("score", 0.0), reverse=True)

    def score_opportunity_v2(self, opp) -> float:
        opp = opp if isinstance(opp, dict) else {}
        annualized = min(100.0, max(0.0, _safe_float(opp.get("annualized_yield"), 0.0)))
        pop = min(1.0, max(0.0, _safe_float(opp.get("prob_profit"), 0.0))) * 100.0
        iv_rank = min(100.0, max(0.0, _safe_float(opp.get("iv_rank"), 0.0)))
        liquidity_raw = max(1.0, _safe_float(opp.get("liquidity_score"), 0.0))
        liquidity = min(100.0, math.log10(liquidity_raw + 1.0) * 20.0)
        rr = min(100.0, max(0.0, _safe_float(opp.get("risk_reward_ratio"), 0.0) * 20.0))
        social_sentiment = max(-100.0, min(100.0, _safe_float(opp.get("social_sentiment"), 0.0)))
        sentiment_score = (social_sentiment + 100.0) / 2.0
        if bool(opp.get("social_trending")):
            sentiment_score = min(100.0, sentiment_score * 1.2)
        score = (
            annualized * 0.25
            + pop * 0.20
            + iv_rank * 0.20
            + liquidity * 0.10
            + rr * 0.10
            + sentiment_score * 0.15
        )
        return round(max(0.0, min(100.0, score)), 2)

    def scan_universe(self, strategies=None, sentiment_filter=None) -> dict:
        default_strategies = [
            "covered_calls",
            "cash_secured_puts",
            "credit_spreads",
            "iron_condors",
            "wheel",
            "earnings_strangles",
            "meme_calls",
        ]
        strategies = [str(item).strip().lower() for item in (strategies or default_strategies)]
        sentiment_filter = str(sentiment_filter or "").strip().lower()
        cache_key = self._cache_key(strategies, sentiment_filter=sentiment_filter)
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        opportunities = []
        scanned = 0
        sentiment_scanner = SocialSentimentScanner()
        sentiment_by_symbol = {}
        for symbol in self.watchlist:
            scanned += 1
            try:
                sentiment = sentiment_scanner.get_composite_sentiment(symbol)
                sentiment_by_symbol[symbol] = sentiment
                composite_score = _safe_float(sentiment.get("composite_score"), 0.0)
                if sentiment_filter == "bullish_only" and composite_score < 0:
                    continue
                if sentiment_filter == "bearish_only" and composite_score > 0:
                    continue

                iv_rank = self.chain_fetcher.get_iv_rank(symbol)
                symbol_opps = []
                if "covered_calls" in strategies and iv_rank >= 50:
                    symbol_opps.extend(self.scan_covered_calls(symbol))
                if "cash_secured_puts" in strategies and iv_rank >= 50:
                    symbol_opps.extend(self.scan_cash_secured_puts(symbol))
                if "credit_spreads" in strategies and iv_rank >= 50:
                    symbol_opps.extend(self.scan_credit_spreads(symbol, "bull_put"))
                    symbol_opps.extend(self.scan_credit_spreads(symbol, "bear_call"))
                if "iron_condors" in strategies and iv_rank >= 50:
                    symbol_opps.extend(self.scan_iron_condors(symbol))
                if "wheel" in strategies:
                    symbol_opps.extend(self.scan_wheel_opportunities(symbol))
                if "meme_calls" in strategies and is_meme_squeeze(symbol, sentiment, iv_rank):
                    meme_opps = self.scan_meme_calls(symbol)
                    squeeze_conviction = min(1.0, _safe_float(sentiment.get("composite_score"), 0.0) / 100.0 + 0.5)
                    for opp in meme_opps:
                        opp["squeeze_capital_pct"] = allocate_meme_squeeze_capital(squeeze_conviction)
                        opp["squeeze_conviction"] = round(squeeze_conviction, 3)
                    symbol_opps.extend(meme_opps)

                squeeze_active = is_meme_squeeze(symbol, sentiment, iv_rank)
                for opp in symbol_opps:
                    opp["iv_rank"] = round(_safe_float(opp.get("iv_rank"), iv_rank), 2)
                    opp["social_sentiment"] = round(composite_score, 1)
                    opp["social_mentions"] = _safe_int(sentiment.get("total_mentions"), 0)
                    opp["social_trending"] = _safe_int(sentiment.get("trending_sources"), 0) > 0
                    opp["social_label"] = str(sentiment.get("composite_label") or "Neutral")
                    if "is_meme_squeeze" not in opp:
                        opp["is_meme_squeeze"] = False
                    if squeeze_active and not opp.get("is_meme_squeeze"):
                        opp["is_meme_squeeze"] = False  # only meme_call strategy gets True
                    opp["score"] = self.score_opportunity_v2(opp)
                opportunities.extend(symbol_opps)
            except Exception as exc:
                _log(f"scan failed symbol={symbol} error={exc}")
            time.sleep(0.5)

        if "earnings_strangles" in strategies:
            try:
                earnings_opps = self.scan_earnings_strangles()
                for opp in earnings_opps:
                    symbol = _normalize_symbol(opp.get("symbol"))
                    sentiment = sentiment_by_symbol.get(symbol)
                    if sentiment is None:
                        sentiment = sentiment_scanner.get_composite_sentiment(symbol)
                        sentiment_by_symbol[symbol] = sentiment
                    composite_score = _safe_float(sentiment.get("composite_score"), 0.0)
                    if sentiment_filter == "bullish_only" and composite_score < 0:
                        continue
                    if sentiment_filter == "bearish_only" and composite_score > 0:
                        continue
                    opp["social_sentiment"] = round(composite_score, 1)
                    opp["social_mentions"] = _safe_int(sentiment.get("total_mentions"), 0)
                    opp["social_trending"] = _safe_int(sentiment.get("trending_sources"), 0) > 0
                    opp["social_label"] = str(sentiment.get("composite_label") or "Neutral")
                    opp["score"] = self.score_opportunity_v2(opp)
                    opportunities.append(opp)
            except Exception as exc:
                _log(f"earnings strangle scan failed error={exc}")

        opportunities.sort(key=lambda row: (row.get("score", 0.0), row.get("annualized_yield", 0.0)), reverse=True)
        avg_yield = sum(_safe_float(row.get("annualized_yield"), 0.0) for row in opportunities) / len(opportunities) if opportunities else 0.0
        result = {
            "scan_time": int(time.time()),
            "opportunities": opportunities,
            "summary": {
                "total_scanned": scanned,
                "opportunities_found": len(opportunities),
                "best_opportunity": opportunities[0] if opportunities else None,
                "avg_annualized_yield": round(avg_yield, 2),
            },
        }
        self._cache_set(cache_key, result)
        return result

    def get_recommendation(self, capital_available, risk_tolerance="moderate") -> list:
        capital_available = _safe_float(capital_available, 0.0)
        risk_tolerance = str(risk_tolerance or "moderate").strip().lower()
        scan = self.scan_universe()
        opportunities = list(scan.get("opportunities") or [])
        vix_level = self.chain_fetcher.get_iv_rank("SPY")
        allocations = allocate_options_capital(vix_level)
        allowed = {
            "conservative": {"covered_call", "cash_secured_put", "wheel"},
            "moderate": {"covered_call", "cash_secured_put", "bull_put", "bear_call", "wheel"},
            "aggressive": {"covered_call", "cash_secured_put", "bull_put", "bear_call", "iron_condor", "wheel", "earnings_strangle"},
        }.get(risk_tolerance, {"covered_call", "cash_secured_put", "bull_put", "bear_call", "wheel"})

        recommendations = []
        used_symbols = set()
        strategy_limits = {}
        for key, pct in allocations.items():
            strategy_limits[key] = max(1, int(round(pct * 5)))
        for opp in opportunities:
            strategy = str(opp.get("strategy") or "").strip().lower()
            if strategy not in allowed:
                continue
            if strategy_limits.get(strategy, 1) <= 0:
                continue
            details = opp.get("details") or {}
            required_capital = 0.0
            if strategy == "cash_secured_put":
                required_capital = _safe_float(details.get("secured_capital"), 0.0)
            elif strategy in {"bull_put", "bear_call", "iron_condor"}:
                width = _safe_float(details.get("width"), 0.0)
                credit = _safe_float(details.get("credit"), 0.0)
                required_capital = max(0.0, (width - credit) * 100.0)
            elif strategy == "wheel":
                required_capital = _safe_float(details.get("secured_capital"), 0.0)
            elif strategy == "earnings_strangle":
                required_capital = max(250.0, _safe_float(details.get("credit"), 0.0) * 100.0)
            else:
                required_capital = 0.0

            if required_capital > capital_available > 0:
                continue
            if opp.get("symbol") in used_symbols and strategy not in {"earnings_strangle"}:
                continue

            recommendations.append({
                "symbol": opp.get("symbol"),
                "strategy": opp.get("strategy"),
                "expiration": opp.get("expiration"),
                "score": opp.get("score"),
                "annualized_yield": opp.get("annualized_yield"),
                "prob_profit": opp.get("prob_profit"),
                "iv_rank": opp.get("iv_rank"),
                "required_capital": round(required_capital, 2),
                "order_details": details,
            })
            used_symbols.add(opp.get("symbol"))
            strategy_limits[strategy] = strategy_limits.get(strategy, 1) - 1
            if len(recommendations) >= 5:
                break

        return recommendations
