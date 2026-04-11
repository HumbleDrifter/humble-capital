import time
from datetime import datetime, timedelta, timezone

from backtester import adx, atr, bollinger_bands, ema, macd, rsi, sma
from scoring_engine import ScoringEngine
from stock_universe import StockUniverse


class StockScanner:
    def __init__(self, watchlist=None):
        if watchlist:
            self.watchlist = [str(s or "").upper().strip() for s in watchlist if str(s or "").strip()]
            self.tier1 = list(self.watchlist)
            self.tier2 = []
            self.universe = None
        else:
            self.universe = StockUniverse()
            tiers = self.universe.discover_universe()
            self.tier1 = list(tiers.get("tier1", []) or [])
            self.tier2 = list(tiers.get("tier2", []) or [])
            self.watchlist = list(self.tier1)
        self.scoring_engine = ScoringEngine()
        self._last_scan = {"scan_time": 0, "opportunities": [], "promotions": [], "summary": {}}

    def _log(self, message):
        print(f"[stock_scanner] {message}")

    def _safe_float(self, value, default=0.0):
        try:
            return float(value)
        except Exception:
            return float(default)

    def _normalize_symbol(self, symbol):
        return str(symbol or "").strip().upper()

    def fetch_stock_bars(self, symbol, days=200):
        symbol = self._normalize_symbol(symbol)
        try:
            import os
            from webull.core.client import ApiClient
            from webull.data.data_client import DataClient

            app_key = os.getenv("WEBULL_APP_KEY", "").strip()
            app_secret = os.getenv("WEBULL_APP_SECRET", "").strip()
            if app_key and app_secret:
                api_client = ApiClient(app_key, app_secret, "us")
                api_client.add_endpoint("us", "api.webull.com")
                data_client = DataClient(api_client)
                market_data = getattr(data_client, "market_data", None)
                if market_data and hasattr(market_data, "get_history_bar"):
                    response = market_data.get_history_bar(symbol, "US_STOCK", "D", count=str(max(30, int(days or 200))))
                    data = response.json() if hasattr(response, "json") else response
                    rows = data if isinstance(data, list) else data.get("items", data.get("data", []))
                    bars = []
                    for row in rows or []:
                        ts = int(self._safe_float(row.get("time") or row.get("timestamp") or 0, 0))
                        bars.append({
                            "ts": ts if ts > 0 else int(time.time()),
                            "date": str(row.get("date") or datetime.fromtimestamp(ts or time.time(), tz=timezone.utc).date()),
                            "open": self._safe_float(row.get("open"), 0.0),
                            "high": self._safe_float(row.get("high"), 0.0),
                            "low": self._safe_float(row.get("low"), 0.0),
                            "close": self._safe_float(row.get("close"), 0.0),
                            "volume": self._safe_float(row.get("volume"), 0.0),
                        })
                    if bars:
                        return sorted(bars, key=lambda row: row["ts"])
        except Exception as exc:
            self._log(f"webull bars failed symbol={symbol} error={exc}")

        try:
            import yfinance as yf

            end = datetime.now(timezone.utc)
            start = end - timedelta(days=max(30, int(days or 200)) + 5)
            hist = yf.Ticker(symbol).history(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), interval="1d")
            bars = []
            for idx, row in hist.iterrows():
                ts = int(idx.timestamp()) if hasattr(idx, "timestamp") else int(time.time())
                bars.append({
                    "ts": ts,
                    "date": str(getattr(idx, "date", lambda: idx)()),
                    "open": self._safe_float(row.get("Open"), 0.0),
                    "high": self._safe_float(row.get("High"), 0.0),
                    "low": self._safe_float(row.get("Low"), 0.0),
                    "close": self._safe_float(row.get("Close"), 0.0),
                    "volume": self._safe_float(row.get("Volume"), 0.0),
                })
            return bars
        except Exception as exc:
            self._log(f"yfinance bars failed symbol={symbol} error={exc}")
            return []

    def compute_indicators(self, bars):
        rows = [dict(row) for row in (bars or []) if isinstance(row, dict)]
        closes = [self._safe_float(row.get("close"), 0.0) for row in rows]
        highs = [self._safe_float(row.get("high"), 0.0) for row in rows]
        lows = [self._safe_float(row.get("low"), 0.0) for row in rows]
        volumes = [self._safe_float(row.get("volume"), 0.0) for row in rows]

        ema21 = ema(closes, 21)
        ema50 = ema(closes, 50)
        ema200 = ema(closes, 200)
        rsi14 = rsi(closes, 14)
        macd_line, macd_sig, macd_hist = macd(closes, 12, 26, 9)
        bb_upper, bb_middle, bb_lower = bollinger_bands(closes, 20, 2.0)
        adx_vals = adx(highs, lows, closes, 14)
        atr_vals = atr(highs, lows, closes, 14)
        vol_sma = sma(volumes, 20)

        for idx, row in enumerate(rows):
            row["ema_fast"] = ema21[idx]
            row["ema_mid"] = ema50[idx]
            row["ema_slow"] = ema200[idx]
            row["ema_trend"] = ema200[idx]
            row["rsi"] = rsi14[idx]
            row["macd"] = macd_line[idx]
            row["macd_signal"] = macd_sig[idx]
            row["macd_hist"] = macd_hist[idx]
            row["bb_upper"] = bb_upper[idx]
            row["bb_middle"] = bb_middle[idx]
            row["bb_lower"] = bb_lower[idx]
            row["adx"] = adx_vals[idx]
            row["atr"] = atr_vals[idx]
            row["vol_sma"] = vol_sma[idx]
        return rows

    def _score_stock(self, symbol, bars, regime="neutral", include_sentiment=True):
        if not bars:
            return None
        score = self.scoring_engine.score_crypto_asset(symbol, bars, regime, None)
        if not include_sentiment:
            technical = self._safe_float(score.get("technical_score"), 0.0)
            momentum = self._safe_float(score.get("momentum_score"), 0.0)
            regime_score = self._safe_float(score.get("regime_score"), 0.0)
            score["sentiment_score"] = 50.0
            score["composite_score"] = round((technical * 0.5) + (momentum * 0.3) + (regime_score * 0.2), 2)
            score["reasoning"] = f"{symbol} technical-only scan due to lower tier coverage."
        latest = bars[-1]
        previous = bars[-2] if len(bars) >= 2 else latest
        score.update({
            "symbol": symbol,
            "price": self._safe_float(latest.get("close"), 0.0),
            "change_24h": ((self._safe_float(latest.get("close"), 0.0) - self._safe_float(previous.get("close"), 0.0)) / max(0.01, self._safe_float(previous.get("close"), 0.0))) * 100.0,
            "volume": self._safe_float(latest.get("volume"), 0.0),
            "social_mentions": int((score.get("sentiment") or {}).get("total_mentions", 0) or 0),
            "social_label": (score.get("sentiment") or {}).get("composite_label", "Neutral"),
            "social_sentiment": self._safe_float((score.get("sentiment") or {}).get("composite_score"), 0.0),
            "social_trending": int((score.get("sentiment") or {}).get("trending_sources", 0) or 0) > 0,
        })
        return score

    def scan_universe(self, regime="neutral", include_tier2=False):
        scan_time = int(time.time())
        symbols = list(self.tier1)
        if include_tier2:
            symbols.extend(self.tier2)
        opportunities = []
        promotions = []
        summary = {"scanned": 0, "strong_buy": 0, "buy": 0, "hold": 0, "sell": 0}

        for symbol in symbols:
            tier = "tier1" if symbol in self.tier1 else "tier2"
            bars = self.compute_indicators(self.fetch_stock_bars(symbol, 240))
            if len(bars) < 60:
                continue
            summary["scanned"] += 1
            score = self._score_stock(symbol, bars, regime=regime, include_sentiment=(tier == "tier1"))
            if not score:
                continue
            signal = str(score.get("signal") or "hold")
            if signal in summary:
                summary[signal] += 1
            elif signal == "strong_buy":
                summary["strong_buy"] += 1

            latest = bars[-1]
            vol_ratio = self._safe_float(latest.get("volume"), 0.0) / max(1.0, self._safe_float(latest.get("vol_sma"), 1.0))
            bb_upper = self._safe_float(latest.get("bb_upper"), 0.0)
            close = self._safe_float(latest.get("close"), 0.0)
            if self.universe is not None:
                if tier == "tier2" and (vol_ratio >= 3.0 or score.get("social_mentions", 0) > 20):
                    self.universe.promote_ticker(symbol, "tier2", "tier1", "volume_or_social_spike")
                    promotions.append({"symbol": symbol, "from_tier": "tier2", "to_tier": "tier1", "reason": "volume_or_social_spike"})
                if tier not in {"tier1", "tier2"} and close > bb_upper > 0:
                    self.universe.promote_ticker(symbol, "tier3", "tier2", "breakout_detected")
                    promotions.append({"symbol": symbol, "from_tier": "tier3", "to_tier": "tier2", "reason": "breakout_detected"})

            opportunities.append(score)
            time.sleep(0.1)

        opportunities.sort(key=lambda row: row.get("composite_score", 0.0), reverse=True)
        result = {
            "scan_time": scan_time,
            "opportunities": opportunities,
            "promotions": promotions,
            "summary": summary,
        }
        self._last_scan = result
        self._log(f"scan complete scanned={summary['scanned']} strong_buy={summary['strong_buy']} buy={summary['buy']}")
        return result

    def get_top_opportunities(self, n=10):
        rows = list((self._last_scan or {}).get("opportunities", []) or [])
        rows.sort(key=lambda row: row.get("composite_score", 0.0), reverse=True)
        return rows[: max(1, int(n or 10))]
