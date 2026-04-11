from backtester import adx, atr, bb_percent_b, bollinger_bands, ema, macd, rsi, sma


def _log(message):
    print(f"[scoring_engine] {message}")


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _normalize_product_id(product_id):
    return str(product_id or "").upper().strip()


def _clamp(value, low=0.0, high=100.0):
    return max(low, min(high, _safe_float(value, low)))


def _linear_score(value, low, high):
    if high <= low:
        return 50.0
    return _clamp(((_safe_float(value) - low) / (high - low)) * 100.0)


class ScoringEngine:
    def __init__(self):
        from options.sentiment import SocialSentimentScanner

        self.sentiment_scanner = SocialSentimentScanner()

    def _enrich_candles(self, candles):
        rows = [dict(row) for row in (candles or []) if isinstance(row, dict)]
        if not rows:
            return []

        closes = [_safe_float(row.get("close"), 0.0) for row in rows]
        highs = [_safe_float(row.get("high"), 0.0) for row in rows]
        lows = [_safe_float(row.get("low"), 0.0) for row in rows]
        volumes = [_safe_float(row.get("volume"), 0.0) for row in rows]

        ema_fast = ema(closes, 9)
        ema_mid = ema(closes, 21)
        ema_slow = ema(closes, 50)
        ema_trend = ema(closes, 200)
        rsi_vals = rsi(closes, 14)
        _, _, macd_hist = macd(closes, 12, 26, 9)
        adx_vals = adx(highs, lows, closes, 14)
        atr_vals = atr(highs, lows, closes, 14)
        bb_upper, bb_middle, bb_lower = bollinger_bands(closes, 20, 2.0)
        bb_pct = bb_percent_b(closes, bb_upper, bb_lower)
        vol_sma = sma(volumes, 20)

        for idx, row in enumerate(rows):
            row.setdefault("ema_fast", ema_fast[idx])
            row.setdefault("ema_mid", ema_mid[idx])
            row.setdefault("ema_slow", ema_slow[idx])
            row.setdefault("ema_trend", ema_trend[idx])
            row.setdefault("rsi", rsi_vals[idx])
            row.setdefault("macd_hist", macd_hist[idx])
            row.setdefault("adx", adx_vals[idx])
            row.setdefault("atr", atr_vals[idx])
            row.setdefault("bb_upper", bb_upper[idx])
            row.setdefault("bb_middle", bb_middle[idx])
            row.setdefault("bb_lower", bb_lower[idx])
            row.setdefault("bb_pctb", bb_pct[idx])
            row.setdefault("vol_sma", vol_sma[idx])
        return rows

    def _technical_components(self, candles):
        rows = self._enrich_candles(candles)
        if len(rows) < 3:
            return {"score": 0.0, "latest": {}, "previous": {}}

        latest = rows[-1]
        previous = rows[-2]
        close = _safe_float(latest.get("close"), 0.0)
        ema_fast = _safe_float(latest.get("ema_fast"), 0.0)
        ema_mid = _safe_float(latest.get("ema_mid"), 0.0)
        ema_slow = _safe_float(latest.get("ema_slow"), 0.0)
        ema_trend = _safe_float(latest.get("ema_trend"), 0.0)
        rsi_val = _safe_float(latest.get("rsi"), 50.0)
        macd_hist = _safe_float(latest.get("macd_hist"), 0.0)
        prev_macd = _safe_float(previous.get("macd_hist"), 0.0)
        adx_val = _safe_float(latest.get("adx"), 0.0)
        bb_pctb = _safe_float(latest.get("bb_pctb"), 0.5)

        ema_alignment = 100.0 if (ema_fast > ema_mid > ema_slow) else 35.0 if (ema_fast > ema_mid) else 10.0
        rsi_score = 100.0 - min(100.0, abs(rsi_val - 52.5) / 12.5 * 100.0)
        macd_score = 100.0 if macd_hist > 0 and macd_hist >= prev_macd else 55.0 if macd_hist > 0 else 15.0
        adx_score = _linear_score(adx_val, 15.0, 35.0)
        trend_score = 100.0 if close > ema_trend > 0 else 20.0
        bb_score = 100.0 - min(100.0, abs(bb_pctb - 0.25) / 0.25 * 100.0)

        score = (
            ema_alignment * 0.20
            + rsi_score * 0.20
            + macd_score * 0.15
            + adx_score * 0.15
            + trend_score * 0.15
            + bb_score * 0.15
        )
        return {"score": round(score, 2), "latest": latest, "previous": previous}

    def _momentum_score(self, product_id, candles, btc_candles=None):
        rows = self._enrich_candles(candles)
        if len(rows) < 8:
            return 0.0

        closes = [_safe_float(row.get("close"), 0.0) for row in rows]
        latest_close = closes[-1]
        bars_24h = 6 if len(rows) >= 7 else 1
        bars_7d = 42 if len(rows) >= 43 else max(1, len(rows) - 1)

        close_24h = closes[-1 - bars_24h] if len(rows) > bars_24h else closes[0]
        close_7d = closes[-1 - bars_7d] if len(rows) > bars_7d else closes[0]
        chg_24h = ((latest_close - close_24h) / close_24h * 100.0) if close_24h > 0 else 0.0
        chg_7d = ((latest_close - close_7d) / close_7d * 100.0) if close_7d > 0 else 0.0

        latest = rows[-1]
        volume = _safe_float(latest.get("volume"), 0.0)
        vol_sma = _safe_float(latest.get("vol_sma"), 0.0)
        volume_ratio = volume / vol_sma if vol_sma > 0 else 1.0

        rs_btc = 0.0
        if btc_candles:
            btc_rows = self._enrich_candles(btc_candles)
            if len(btc_rows) >= 8:
                btc_closes = [_safe_float(row.get("close"), 0.0) for row in btc_rows]
                btc_latest = btc_closes[-1]
                btc_7d = btc_closes[-1 - min(bars_7d, len(btc_rows) - 1)] if len(btc_rows) > 1 else btc_latest
                btc_return_7d = ((btc_latest - btc_7d) / btc_7d * 100.0) if btc_7d > 0 else 0.0
                rs_btc = chg_7d - btc_return_7d

        score = (
            _clamp(50.0 + (chg_24h * 4.0), 0.0, 100.0) * 0.25
            + _clamp(50.0 + (chg_7d * 2.5), 0.0, 100.0) * 0.35
            + _clamp(volume_ratio * 50.0, 0.0, 100.0) * 0.20
            + _clamp(50.0 + (rs_btc * 4.0), 0.0, 100.0) * 0.20
        )
        return round(score, 2)

    def _regime_score(self, regime):
        regime = str(regime or "neutral").lower().strip()
        mapping = {
            "bull": 100.0,
            "neutral": 70.0,
            "caution": 40.0,
            "risk_off": 10.0,
        }
        return mapping.get(regime, 50.0)

    def _sentiment_score(self, product_id):
        try:
            sentiment = self.sentiment_scanner.get_composite_sentiment(product_id)
        except Exception as exc:
            _log(f"sentiment lookup failed product_id={product_id} error={exc}")
            sentiment = {
                "composite_score": 0.0,
                "composite_label": "Neutral",
                "total_mentions": 0,
                "trending_sources": 0,
                "recommendation": "Sentiment unavailable.",
            }
        raw = _safe_float(sentiment.get("composite_score"), 0.0)
        normalized = _clamp((raw + 100.0) / 2.0, 0.0, 100.0)
        if _safe_float(sentiment.get("trending_sources"), 0.0) > 0:
            normalized = min(100.0, normalized * 1.1)
        return round(normalized, 2), sentiment

    def score_crypto_asset(self, product_id, candles, regime, portfolio_snapshot=None) -> dict:
        product_id = _normalize_product_id(product_id)
        technical = self._technical_components(candles)
        btc_candles = None
        if isinstance(portfolio_snapshot, dict):
            btc_candles = portfolio_snapshot.get("btc_candles")
        momentum_score = self._momentum_score(product_id, candles, btc_candles=btc_candles)
        sentiment_score, sentiment = self._sentiment_score(product_id)
        regime_score = self._regime_score(regime)
        composite = (
            technical["score"] * 0.40
            + sentiment_score * 0.25
            + momentum_score * 0.20
            + regime_score * 0.15
        )
        composite = round(_clamp(composite), 2)

        if composite >= 80:
            signal = "strong_buy"
        elif composite >= 65:
            signal = "buy"
        elif composite >= 45:
            signal = "hold"
        elif composite >= 30:
            signal = "sell"
        else:
            signal = "strong_sell"

        latest = technical.get("latest") or {}
        reasoning = (
            f"{product_id} scores {composite:.1f}/100 with technicals at {technical['score']:.1f}, "
            f"social sentiment at {sentiment_score:.1f}, momentum at {momentum_score:.1f}, "
            f"and regime fit at {regime_score:.1f}. "
            f"RSI is {_safe_float(latest.get('rsi'), 50.0):.1f}, ADX is {_safe_float(latest.get('adx'), 0.0):.1f}, "
            f"and sentiment is {sentiment.get('composite_label', 'Neutral')}."
        )
        return {
            "product_id": product_id,
            "composite_score": composite,
            "technical_score": round(technical["score"], 2),
            "sentiment_score": round(sentiment_score, 2),
            "momentum_score": round(momentum_score, 2),
            "regime_score": round(regime_score, 2),
            "signal": signal,
            "conviction": round(composite / 100.0, 3),
            "reasoning": reasoning,
            "sentiment": sentiment,
        }

    def score_for_entry(self, product_id, candles, regime) -> dict:
        score = self.score_crypto_asset(product_id, candles, regime)
        already_holding = False
        try:
            from portfolio import get_portfolio_snapshot

            snapshot = get_portfolio_snapshot()
            already_holding = _safe_float(((snapshot.get("positions") or {}).get(_normalize_product_id(product_id)) or {}).get("value_total_usd"), 0.0) > 1.0
        except Exception:
            already_holding = False

        regime_ok = str(regime or "neutral").lower().strip() in {"bull", "neutral"}
        enter = (
            score["composite_score"] >= 65.0
            and score["technical_score"] >= 50.0
            and score["sentiment_score"] >= 40.0
            and regime_ok
            and not already_holding
        )
        reason_bits = []
        if score["composite_score"] < 65.0:
            reason_bits.append("composite score below 65")
        if score["technical_score"] < 50.0:
            reason_bits.append("technical setup is too weak")
        if score["sentiment_score"] < 40.0:
            reason_bits.append("social sentiment is bearish")
        if not regime_ok:
            reason_bits.append(f"regime {regime} blocks new entries")
        if already_holding:
            reason_bits.append("already holding the asset")
        return {
            "enter": bool(enter),
            "score": score["composite_score"],
            "conviction": score["conviction"],
            "reason": "Entry approved." if enter else "; ".join(reason_bits) or "Entry blocked.",
        }

    def score_for_exit(self, product_id, candles, regime, entry_price, bars_held) -> dict:
        score = self.score_crypto_asset(product_id, candles, regime)
        rows = self._enrich_candles(candles)
        latest = rows[-1] if rows else {}
        current_price = _safe_float(latest.get("close"), 0.0)
        atr_value = _safe_float(latest.get("atr"), 0.0)
        sentiment_bearish = score["sentiment_score"] < 30.0 and score["technical_score"] < 40.0
        atr_stop_hit = current_price > 0 and entry_price > 0 and atr_value > 0 and current_price < (entry_price - (2.0 * atr_value))
        stale_position = _safe_float(bars_held, 0.0) > 72.0 and current_price <= _safe_float(entry_price, 0.0)
        regime_risk = str(regime or "").lower().strip() == "risk_off"

        exit_now = any(
            [
                score["composite_score"] < 30.0,
                sentiment_bearish,
                atr_stop_hit,
                stale_position,
                regime_risk,
            ]
        )
        if score["composite_score"] < 30.0:
            reason = "Composite opportunity score degraded below 30."
        elif sentiment_bearish:
            reason = "Sentiment flipped bearish while technicals weakened."
        elif atr_stop_hit:
            reason = "ATR stop hit."
        elif stale_position:
            reason = "Position is stale after 72 bars with no profit."
        elif regime_risk:
            reason = "Market regime turned risk_off."
        else:
            reason = "Hold."
        return {"exit": bool(exit_now), "score": score["composite_score"], "reason": reason}

    def rank_universe(self, all_candles, regime, portfolio_snapshot=None) -> list:
        ranked = []
        btc_candles = (all_candles or {}).get("BTC-USD")
        for product_id, candles in (all_candles or {}).items():
            if not isinstance(candles, list) or not candles:
                continue
            snapshot = dict(portfolio_snapshot or {})
            if btc_candles:
                snapshot["btc_candles"] = btc_candles
            try:
                ranked.append(self.score_crypto_asset(product_id, candles, regime, portfolio_snapshot=snapshot))
            except Exception as exc:
                _log(f"rank failed product_id={product_id} error={exc}")
        ranked.sort(key=lambda row: row.get("composite_score", 0.0), reverse=True)
        return ranked
