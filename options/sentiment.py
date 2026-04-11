import json
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone

import requests

_SENTIMENT_CACHE = {}
_CACHE_TTL = 900
_CACHE_LOCK = threading.RLock()

CRYPTO_SUBREDDITS = [
    "cryptocurrency", "cryptomarkets", "bitcoin", "ethereum", "solana",
    "altcoin", "satoshistreetbets", "memecoin", "shitcoins",
]
STOCK_SUBREDDITS = ["wallstreetbets", "options", "stocks", "investing", "stockmarket"]
CRYPTO_TICKERS = {
    "BTC", "ETH", "SOL", "XRP", "DOGE", "SHIB", "PEPE", "BONK",
    "SUI", "AVAX", "ADA", "DOT", "LINK", "UNI", "AAVE", "RENDER",
    "WIF", "PENGU", "HYPE", "JASMY", "ICP", "MARA", "TAO", "ATOM",
}

BULLISH_WORDS = {
    "buy", "calls", "long", "moon", "rocket", "bullish", "breakout", "squeeze",
    "undervalued", "dip", "buying", "loaded", "accumulate", "yolo", "tendies",
    "diamond hands", "to the moon", "going up", "pump", "rally", "ATH",
    "gap up", "earnings beat", "upgrade", "strong buy", "oversold", "bounce",
    "support", "golden cross", "bull flag", "higher lows", "ripping"
}

BEARISH_WORDS = {
    "sell", "puts", "short", "crash", "dump", "bearish", "overvalued", "bubble",
    "selling", "exit", "avoid", "downgrade", "miss", "tank", "drill",
    "paper hands", "going down", "dead cat", "rug pull", "bankruptcy",
    "gap down", "earnings miss", "death cross", "bear flag", "lower highs",
    "resistance", "overbought", "top", "correction", "capitulation"
}

CRYPTO_BULLISH = {
    "hodl", "wagmi", "lfg", "ngmi bears", "pump it", "send it",
    "accumulate", "dca", "buy the dip", "diamond hands", "moon",
    "bullrun", "alt season", "100x", "gem", "based",
}

CRYPTO_BEARISH = {
    "rugpull", "scam", "dump", "rekt", "ngmi", "exit liquidity",
    "dead coin", "sell now", "bear market", "capitulation",
    "liquidated", "worthless", "ponzi",
}


def _log(message):
    print(f"[sentiment] {message}")


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _is_crypto(symbol):
    clean = str(symbol or "").replace("-USD", "").upper().strip()
    return clean in CRYPTO_TICKERS or str(symbol or "").upper().strip().endswith("-USD")


def _score_text(text, symbol=None):
    """
    Score a piece of text for bullish/bearish sentiment.
    Returns float from -1.0 (very bearish) to +1.0 (very bullish).
    """
    text_lower = str(text or "").lower()
    words = set(re.findall(r"\b\w+\b", text_lower))

    bigrams = set()
    word_list = re.findall(r"\b\w+\b", text_lower)
    for i in range(len(word_list) - 1):
        bigrams.add(f"{word_list[i]} {word_list[i + 1]}")

    all_tokens = words | bigrams

    bullish_words = set(BULLISH_WORDS)
    bearish_words = set(BEARISH_WORDS)
    if _is_crypto(symbol):
        bullish_words |= CRYPTO_BULLISH
        bearish_words |= CRYPTO_BEARISH

    bull_count = len(all_tokens & bullish_words)
    bear_count = len(all_tokens & bearish_words)

    total = bull_count + bear_count
    if total == 0:
        return 0.0

    return round((bull_count - bear_count) / total, 3)


class SocialSentimentScanner:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "HumbleCapital/1.0 (Options Research Bot)"
        })
        self.youtube_api_key = os.getenv("YOUTUBE_API_KEY", "").strip()

    def _cache_get(self, key):
        with _CACHE_LOCK:
            row = _SENTIMENT_CACHE.get(key)
            if not row:
                return None
            if time.time() - row["ts"] > _CACHE_TTL:
                _SENTIMENT_CACHE.pop(key, None)
                return None
            return row["data"]

    def _cache_set(self, key, data):
        with _CACHE_LOCK:
            _SENTIMENT_CACHE[key] = {"ts": time.time(), "data": data}

    def scan_reddit(self, symbol, limit=50) -> dict:
        """
        Scan Reddit for mentions of a stock ticker or crypto asset.
        Uses Reddit's public JSON API (no auth needed).
        """
        cache_key = f"reddit:{symbol}"
        cached = self._cache_get(cache_key)
        if cached:
            return cached

        subreddits = CRYPTO_SUBREDDITS if _is_crypto(symbol) else STOCK_SUBREDDITS
        all_posts = []
        search_terms = [str(symbol or "").upper().strip()]
        clean_symbol = str(symbol or "").replace("-USD", "").upper().strip()
        if clean_symbol and clean_symbol not in search_terms:
            search_terms.append(clean_symbol)

        for sub in subreddits:
            try:
                url = f"https://www.reddit.com/r/{sub}/search.json"
                params = {"q": clean_symbol or symbol, "sort": "new", "limit": limit, "t": "week", "restrict_sr": "on"}
                resp = self.session.get(url, params=params, timeout=10)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                posts = data.get("data", {}).get("children", [])
                for post in posts:
                    p = post.get("data", {})
                    title = p.get("title", "")
                    body = p.get("selftext", "")
                    full_text = f"{title} {body}"
                    matched = False
                    for term in search_terms:
                        if re.search(rf"\b{re.escape(term)}\b", full_text, re.IGNORECASE) or re.search(rf"\${re.escape(term)}\b", full_text, re.IGNORECASE):
                            matched = True
                            break
                    if not matched:
                        continue
                    sentiment = _score_text(full_text, symbol=symbol)
                    all_posts.append({
                        "title": title[:200],
                        "subreddit": sub,
                        "upvotes": int(p.get("ups", 0)),
                        "comments": int(p.get("num_comments", 0)),
                        "sentiment": sentiment,
                        "url": f"https://reddit.com{p.get('permalink', '')}",
                        "created": int(p.get("created_utc", 0)),
                    })
                time.sleep(0.5)
            except Exception as exc:
                _log(f"reddit scan failed sub={sub} symbol={symbol} error={exc}")

        mentions = len(all_posts)
        total_upvotes = sum(p["upvotes"] for p in all_posts)
        total_comments = sum(p["comments"] for p in all_posts)
        sentiments = [p["sentiment"] for p in all_posts]
        avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0.0
        bullish = len([s for s in sentiments if s > 0.1])
        bearish = len([s for s in sentiments if s < -0.1])
        neutral = mentions - bullish - bearish

        result = {
            "source": "reddit",
            "symbol": symbol,
            "mentions": mentions,
            "total_upvotes": total_upvotes,
            "total_comments": total_comments,
            "avg_sentiment": round(avg_sentiment, 3),
            "bullish_pct": round(bullish / max(1, mentions) * 100, 1),
            "bearish_pct": round(bearish / max(1, mentions) * 100, 1),
            "neutral_pct": round(neutral / max(1, mentions) * 100, 1),
            "top_posts": sorted(all_posts, key=lambda p: p["upvotes"], reverse=True)[:5],
            "trending": mentions >= 20,
        }
        self._cache_set(cache_key, result)
        return result

    def scan_stocktwits(self, symbol) -> dict:
        """
        Scan StockTwits for sentiment on a ticker.
        Public API, no auth needed.
        """
        cache_key = f"stocktwits:{symbol}"
        cached = self._cache_get(cache_key)
        if cached:
            return cached

        try:
            url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
            resp = self.session.get(url, timeout=10)
            if resp.status_code != 200:
                return {
                    "source": "stocktwits",
                    "symbol": symbol,
                    "mentions": 0,
                    "avg_sentiment": 0.0,
                    "bullish_pct": 0,
                    "bearish_pct": 0,
                    "error": f"http_{resp.status_code}",
                }

            data = resp.json()
            messages = data.get("messages", [])
            symbol_info = data.get("symbol", {})

            sentiments = []
            for msg in messages:
                s = msg.get("entities", {}).get("sentiment", {})
                if s:
                    basic = s.get("basic")
                    if basic == "Bullish":
                        sentiments.append(1.0)
                    elif basic == "Bearish":
                        sentiments.append(-1.0)
                    else:
                        sentiments.append(0.0)
                else:
                    sentiments.append(_score_text(msg.get("body", ""), symbol=symbol))

            mentions = len(messages)
            avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0.0
            bullish = len([s for s in sentiments if s > 0])
            bearish = len([s for s in sentiments if s < 0])

            result = {
                "source": "stocktwits",
                "symbol": symbol,
                "mentions": mentions,
                "avg_sentiment": round(avg_sentiment, 3),
                "bullish_pct": round(bullish / max(1, mentions) * 100, 1),
                "bearish_pct": round(bearish / max(1, mentions) * 100, 1),
                "watchers": int(symbol_info.get("watchlist_count", 0)),
                "trending": bool(symbol_info.get("is_following", False)) or mentions >= 25,
            }
            self._cache_set(cache_key, result)
            return result
        except Exception as exc:
            _log(f"stocktwits scan failed symbol={symbol} error={exc}")
            return {
                "source": "stocktwits",
                "symbol": symbol,
                "mentions": 0,
                "avg_sentiment": 0.0,
                "bullish_pct": 0,
                "bearish_pct": 0,
                "error": str(exc),
            }

    def scan_youtube(self, symbol, max_results=10) -> dict:
        """
        Search YouTube for recent videos about a stock ticker.
        Uses YouTube Data API v3 if YOUTUBE_API_KEY is set.
        """
        cache_key = f"youtube:{symbol}"
        cached = self._cache_get(cache_key)
        if cached:
            return cached

        if not self.youtube_api_key:
            return {
                "source": "youtube",
                "symbol": symbol,
                "videos": 0,
                "total_views": 0,
                "avg_sentiment": 0.0,
                "top_videos": [],
                "trending": False,
                "note": "Set YOUTUBE_API_KEY for YouTube scanning",
            }

        try:
            published_after = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
            url = "https://www.googleapis.com/youtube/v3/search"
            params = {
                "part": "snippet",
                "q": f"{symbol} {'crypto' if _is_crypto(symbol) else 'stock'} analysis",
                "type": "video",
                "order": "viewCount",
                "publishedAfter": published_after,
                "maxResults": max_results,
                "key": self.youtube_api_key,
            }
            resp = self.session.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return {"source": "youtube", "symbol": symbol, "videos": 0, "error": f"http_{resp.status_code}"}

            data = resp.json()
            items = data.get("items", [])

            videos = []
            for item in items:
                snippet = item.get("snippet", {})
                title = snippet.get("title", "")
                channel = snippet.get("channelTitle", "")
                video_id = item.get("id", {}).get("videoId", "")
                sentiment = _score_text(title + " " + snippet.get("description", ""), symbol=symbol)
                videos.append({
                    "title": title[:200],
                    "channel": channel,
                    "views": 0,
                    "published": snippet.get("publishedAt", ""),
                    "sentiment": sentiment,
                    "url": f"https://youtube.com/watch?v={video_id}",
                })

            avg_sentiment = sum(v["sentiment"] for v in videos) / len(videos) if videos else 0.0

            result = {
                "source": "youtube",
                "symbol": symbol,
                "videos": len(videos),
                "total_views": sum(v["views"] for v in videos),
                "avg_sentiment": round(avg_sentiment, 3),
                "top_videos": videos[:5],
                "trending": len(videos) >= 5,
            }
            self._cache_set(cache_key, result)
            return result
        except Exception as exc:
            _log(f"youtube scan failed symbol={symbol} error={exc}")
            return {"source": "youtube", "symbol": symbol, "videos": 0, "error": str(exc)}

    def scan_news(self, symbol) -> dict:
        """
        Scan financial news for a stock using Google News RSS.
        """
        cache_key = f"news:{symbol}"
        cached = self._cache_get(cache_key)
        if cached:
            return cached

        headlines = []

        try:
            query = f"{symbol}+{'crypto' if _is_crypto(symbol) else 'stock'}"
            url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                import xml.etree.ElementTree as ET

                root = ET.fromstring(resp.content)
                for item in root.findall(".//item")[:20]:
                    title = item.findtext("title", "")
                    source = item.findtext("source", "")
                    pub_date = item.findtext("pubDate", "")
                    link = item.findtext("link", "")

                    if symbol.upper() in title.upper() or len(title) < 200:
                        sentiment = _score_text(title, symbol=symbol)
                        headlines.append({
                            "title": title[:200],
                            "source": source,
                            "published": pub_date,
                            "sentiment": sentiment,
                            "url": link,
                        })
        except Exception as exc:
            _log(f"news scan failed symbol={symbol} error={exc}")

        avg_sentiment = sum(h["sentiment"] for h in headlines) / len(headlines) if headlines else 0.0

        result = {
            "source": "news",
            "symbol": symbol,
            "articles": len(headlines),
            "avg_sentiment": round(avg_sentiment, 3),
            "headlines": headlines[:10],
            "trending": len(headlines) >= 10,
        }
        self._cache_set(cache_key, result)
        return result

    def scan_crypto_twitter(self, symbol) -> dict:
        """
        Search Twitter/X for crypto cashtag mentions using public RSS-style fallbacks.
        Falls back to Google News crypto-filtered results if direct proxies are unavailable.
        """
        cache_key = f"crypto_twitter:{symbol}"
        cached = self._cache_get(cache_key)
        if cached:
            return cached

        symbol_clean = str(symbol or "").replace("-USD", "").upper().strip()
        headlines = []
        queries = [
            f"{symbol_clean}+crypto",
            f"%24{symbol_clean}+crypto",
            f"%23{symbol_clean}+crypto",
        ]
        for query in queries:
            try:
                url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
                resp = self.session.get(url, timeout=10)
                if resp.status_code != 200:
                    continue
                import xml.etree.ElementTree as ET

                root = ET.fromstring(resp.content)
                for item in root.findall(".//item")[:10]:
                    title = item.findtext("title", "")
                    link = item.findtext("link", "")
                    source = item.findtext("source", "")
                    if symbol_clean not in title.upper():
                        continue
                    headlines.append(
                        {
                            "title": title[:200],
                            "source": source or "X/Twitter",
                            "published": item.findtext("pubDate", ""),
                            "sentiment": _score_text(title, symbol=symbol),
                            "url": link,
                        }
                    )
                if headlines:
                    break
            except Exception as exc:
                _log(f"crypto twitter scan failed symbol={symbol} error={exc}")

        avg_sentiment = sum(row["sentiment"] for row in headlines) / len(headlines) if headlines else 0.0
        result = {
            "source": "crypto_twitter",
            "symbol": symbol,
            "mentions": len(headlines),
            "avg_sentiment": round(avg_sentiment, 3),
            "headlines": headlines[:10],
            "trending": len(headlines) >= 5,
        }
        self._cache_set(cache_key, result)
        return result

    def get_composite_sentiment(self, symbol) -> dict:
        """
        Run all scanners and produce a composite sentiment score.
        Automatically adapts to stock vs crypto symbols.
        """
        is_crypto = _is_crypto(symbol)
        reddit = self.scan_reddit(symbol)
        stocktwits = self.scan_stocktwits(str(symbol or "").replace("-USD", "").upper().strip())
        youtube = self.scan_youtube(symbol)
        news = self.scan_news(symbol)
        crypto_twitter = self.scan_crypto_twitter(symbol) if is_crypto else None

        if is_crypto:
            weights = {"reddit": 0.35, "stocktwits": 0.20, "news": 0.20, "youtube": 0.10, "crypto_twitter": 0.15}
            sources = {
                "reddit": reddit,
                "stocktwits": stocktwits,
                "news": news,
                "youtube": youtube,
                "crypto_twitter": crypto_twitter or {"mentions": 0, "avg_sentiment": 0.0, "trending": False},
            }
        else:
            weights = {"reddit": 0.30, "stocktwits": 0.25, "news": 0.30, "youtube": 0.15}
            sources = {"reddit": reddit, "stocktwits": stocktwits, "news": news, "youtube": youtube}

        weighted_sentiment = 0.0
        total_weight = 0.0
        for source_name, weight in weights.items():
            s = sources[source_name].get("avg_sentiment", 0.0)
            mentions = (
                sources[source_name].get("mentions", 0)
                or sources[source_name].get("articles", 0)
                or sources[source_name].get("videos", 0)
            )
            if mentions > 0:
                weighted_sentiment += s * weight
                total_weight += weight

        composite = (weighted_sentiment / total_weight * 100) if total_weight > 0 else 0.0
        composite = round(max(-100, min(100, composite)), 1)

        if composite >= 50:
            label = "Very Bullish"
        elif composite >= 20:
            label = "Bullish"
        elif composite >= -20:
            label = "Neutral"
        elif composite >= -50:
            label = "Bearish"
        else:
            label = "Very Bearish"

        total_mentions = sum([
            reddit.get("mentions", 0),
            stocktwits.get("mentions", 0),
            youtube.get("videos", 0),
            news.get("articles", 0),
            (crypto_twitter or {}).get("mentions", 0),
        ])

        trending = sum([
            bool(reddit.get("trending", False)),
            bool(stocktwits.get("trending", False)),
            bool(youtube.get("trending", False)),
            bool(news.get("trending", False)),
            bool((crypto_twitter or {}).get("trending", False)),
        ])

        signal_strength = min(100, (total_mentions ** 0.5) * abs(composite) / 10)

        if is_crypto and composite >= 30 and total_mentions >= 20:
            rec = f"Strong crypto bullish sentiment ({total_mentions} mentions). Momentum supports CSPs on related equities or selective premium-selling into strength."
        elif is_crypto and composite <= -30 and total_mentions >= 20:
            rec = f"Strong crypto bearish sentiment ({total_mentions} mentions). Reduce exposure, avoid bullish premium selling, and look for defensive spreads."
        elif composite >= 30 and total_mentions >= 20:
            rec = f"Strong bullish sentiment ({total_mentions} mentions). Consider selling cash-secured puts to collect premium while entering at a discount."
        elif composite >= 10:
            rec = "Mildly bullish sentiment. Covered calls or bull put spreads are appropriate."
        elif composite <= -30 and total_mentions >= 20:
            rec = f"Strong bearish sentiment ({total_mentions} mentions). Consider bear call spreads or avoid new bullish positions."
        elif composite <= -10:
            rec = "Mildly bearish sentiment. Reduce position size or use protective puts."
        else:
            rec = "Neutral sentiment. Iron condors or strangles may work well in this environment."

        return {
            "symbol": symbol,
            "asset_type": "crypto" if is_crypto else "stock",
            "composite_score": composite,
            "composite_label": label,
            "total_mentions": total_mentions,
            "trending_sources": trending,
            "sources": sources,
            "signal_strength": round(signal_strength, 1),
            "recommendation": rec,
        }

    def scan_watchlist(self, symbols, top_n=10) -> list:
        """
        Scan sentiment for a list of symbols and return the top N by signal strength.
        """
        results = []
        for symbol in symbols:
            try:
                result = self.get_composite_sentiment(symbol)
                results.append(result)
            except Exception as exc:
                _log(f"watchlist scan failed symbol={symbol} error={exc}")
            time.sleep(0.5)

        results.sort(key=lambda r: r.get("signal_strength", 0), reverse=True)
        return results[:top_n]
