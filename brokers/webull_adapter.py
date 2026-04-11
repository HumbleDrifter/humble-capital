import json
import os
import threading
import time
from datetime import datetime, timezone

from env_runtime import load_runtime_env

from brokers.base import BrokerAdapter, broker_result

load_runtime_env(override=True)

try:
    from webull import paper_webull as _paper_webull_class
except Exception:
    _paper_webull_class = None

try:
    from webull import webull as _webull_class
except Exception:
    _webull_class = None

_SHARED_CLIENT = None
_SHARED_SESSION_TS = 0.0
_SHARED_MODE = None
_SHARED_LOCK = threading.RLock()
_SESSION_TTL_SEC = 23 * 60 * 60


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


def _env_bool(name, default=False):
    value = str(os.getenv(name, str(default)) or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _utcnow_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_side(side):
    raw = str(side or "").strip().lower()
    if raw in {"buy", "b"}:
        return "BUY"
    if raw in {"sell", "s"}:
        return "SELL"
    return raw.upper()


def _normalize_asset_type(value):
    raw = str(value or "").strip().lower()
    if "option" in raw or raw in {"opt"}:
        return "option"
    return "stock"


def _log_webull_event(event, payload=None):
    envelope = {
        "ts": int(time.time()),
        "pid": os.getpid(),
        "thread": threading.current_thread().name,
        "component": "webull_adapter",
        "event": str(event or "").strip() or "unknown",
        "payload": payload if isinstance(payload, dict) else {},
    }
    print(json.dumps(envelope, sort_keys=True, ensure_ascii=False))


class WebullAdapter(BrokerAdapter):
    name = "webull"

    def __init__(self):
        load_runtime_env(override=True)
        self.email = str(os.getenv("WEBULL_EMAIL", "") or "").strip()
        self.password = str(os.getenv("WEBULL_PASSWORD", "") or "").strip()
        self.device_id = str(os.getenv("WEBULL_DEVICE_ID", "") or "").strip()
        self.trading_pin = str(os.getenv("WEBULL_TRADING_PIN", "") or "").strip()
        self.paper_trading = _env_bool("WEBULL_PAPER_TRADING", True)
        self.enabled = _env_bool("WEBULL_ENABLED", False)
        self._lock = threading.RLock()

    def _mode(self):
        return "paper" if self.paper_trading else "live"

    def _client_class(self):
        if self.paper_trading and _paper_webull_class is not None:
            return _paper_webull_class
        return _webull_class

    def _reset_shared_client(self):
        global _SHARED_CLIENT, _SHARED_SESSION_TS, _SHARED_MODE
        with _SHARED_LOCK:
            _SHARED_CLIENT = None
            _SHARED_SESSION_TS = 0.0
            _SHARED_MODE = None

    def _call_client_method(self, client, names, *args, **kwargs):
        for name in names:
            method = getattr(client, name, None)
            if callable(method):
                return method(*args, **kwargs)
        raise AttributeError(f"none of the Webull client methods exist: {names}")

    def _build_client(self):
        client_class = self._client_class()
        if client_class is None:
            raise RuntimeError("webull_sdk_unavailable")

        try:
            client = client_class()
        except TypeError:
            client = client_class

        if self.device_id:
            for attr in ("_did", "did", "device_id"):
                try:
                    setattr(client, attr, self.device_id)
                    break
                except Exception:
                    continue

        return client

    def _login(self, client):
        if not self.email or not self.password:
            raise RuntimeError("missing_webull_credentials")

        login_methods = ["login", "pwd_login", "email_login"]
        result = None
        last_error = None

        for method_name in login_methods:
            method = getattr(client, method_name, None)
            if not callable(method):
                continue
            try:
                try:
                    result = method(self.email, self.password, device_name=self.device_id or None)
                except TypeError:
                    try:
                        result = method(self.email, self.password)
                    except TypeError:
                        result = method(username=self.email, password=self.password)
                break
            except Exception as exc:
                last_error = exc
                continue

        if result is None and last_error is not None:
            raise last_error
        if result is None:
            raise RuntimeError("no_login_method_available")

        status_text = json.dumps(result, ensure_ascii=False) if isinstance(result, (dict, list)) else str(result)
        if "mfa" in status_text.lower() or "verification" in status_text.lower():
            raise RuntimeError("webull_mfa_required")

        if self.trading_pin:
            for method_name in ("get_trade_token", "trade_token", "unlock_trade"):
                method = getattr(client, method_name, None)
                if not callable(method):
                    continue
                try:
                    method(self.trading_pin)
                    break
                except Exception:
                    continue

        return result

    def _get_client(self, force_reconnect=False):
        global _SHARED_CLIENT, _SHARED_SESSION_TS, _SHARED_MODE

        if not self.enabled:
            raise RuntimeError("webull_disabled")

        mode = self._mode()
        with _SHARED_LOCK:
            is_fresh = (
                _SHARED_CLIENT is not None
                and not force_reconnect
                and _SHARED_MODE == mode
                and (time.time() - _SHARED_SESSION_TS) < _SESSION_TTL_SEC
            )
            if is_fresh:
                return _SHARED_CLIENT

            client = self._build_client()
            self._login(client)
            _SHARED_CLIENT = client
            _SHARED_SESSION_TS = time.time()
            _SHARED_MODE = mode
            _log_webull_event("session_ready", {"mode": mode, "paper_trading": self.paper_trading})
            return _SHARED_CLIENT

    def _with_reconnect(self, fn):
        try:
            client = self._get_client(force_reconnect=False)
            return fn(client)
        except Exception as exc:
            _log_webull_event("request_retry", {"mode": self._mode(), "error": str(exc)})
            try:
                client = self._get_client(force_reconnect=True)
                return fn(client)
            except Exception:
                raise exc

    def connect(self):
        try:
            client = self._get_client(force_reconnect=False)
            account_probe = self._call_client_method(
                client,
                ["get_account", "get_account_info", "get_portfolio"],
            )
            _log_webull_event("connect_ok", {"mode": self._mode()})
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                connected=True,
                last_ready_at=_utcnow_iso(),
                account_probe=account_probe,
            )
        except Exception as exc:
            _log_webull_event("connect_failed", {"mode": self._mode(), "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), connected=False, error=str(exc))

    def get_account_info(self) -> dict:
        try:
            def _fetch(client):
                account = self._call_client_method(client, ["get_account", "get_account_info", "get_portfolio"])
                positions = self.get_positions()
                account_dict = account if isinstance(account, dict) else {}
                balance = _safe_float(
                    account_dict.get("netLiquidation")
                    or account_dict.get("netLiquidationValue")
                    or account_dict.get("balance")
                    or account_dict.get("totalValue"),
                    0.0,
                )
                buying_power = _safe_float(
                    account_dict.get("buyingPower")
                    or account_dict.get("buying_power")
                    or account_dict.get("cashBalance")
                    or account_dict.get("cash"),
                    0.0,
                )
                return broker_result(
                    True,
                    broker=self.name,
                    mode=self._mode(),
                    balance=balance,
                    buying_power=buying_power,
                    positions=positions,
                )

            return self._with_reconnect(_fetch)
        except Exception as exc:
            _log_webull_event("get_account_info_failed", {"error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), balance=0.0, buying_power=0.0, positions=[])

    def get_positions(self) -> list:
        try:
            def _fetch(client):
                raw_positions = self._call_client_method(client, ["get_positions", "get_current_positions"])
                rows = raw_positions if isinstance(raw_positions, list) else []
                positions = []
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    qty = _safe_float(row.get("position") or row.get("quantity") or row.get("qty"), 0.0)
                    avg_cost = _safe_float(row.get("costPrice") or row.get("avgCost") or row.get("avg_cost"), 0.0)
                    market_value = _safe_float(row.get("marketValue") or row.get("market_value") or row.get("value"), 0.0)
                    unrealized_pnl = _safe_float(row.get("unrealizedProfitLoss") or row.get("unrealized_pnl") or row.get("unrealizedProfit"), 0.0)
                    positions.append(
                        {
                            "symbol": str(row.get("ticker") or row.get("symbol") or "").upper().strip(),
                            "qty": qty,
                            "avg_cost": avg_cost,
                            "market_value": market_value,
                            "unrealized_pnl": unrealized_pnl,
                            "asset_type": _normalize_asset_type(row.get("assetType") or row.get("secType") or row.get("asset_type")),
                        }
                    )
                return positions

            return self._with_reconnect(_fetch)
        except Exception as exc:
            _log_webull_event("get_positions_failed", {"error": str(exc)})
            return []

    def place_stock_order(self, symbol, side, qty, order_type="MKT", limit_price=None) -> dict:
        try:
            symbol = str(symbol or "").upper().strip()
            side = _normalize_side(side)
            qty = _safe_int(qty, 0)
            order_type = str(order_type or "MKT").upper().strip()
            if not symbol or side not in {"BUY", "SELL"} or qty <= 0:
                return broker_result(False, broker=self.name, mode=self._mode(), error="invalid_stock_order")

            def _submit(client):
                kwargs = {
                    "stock": symbol,
                    "action": side,
                    "quant": qty,
                    "orderType": "LMT" if order_type in {"LMT", "LIMIT"} else "MKT",
                    "enforce": "DAY",
                }
                if order_type in {"LMT", "LIMIT"} and limit_price is not None:
                    kwargs["price"] = _safe_float(limit_price, 0.0)

                resp = self._call_client_method(
                    client,
                    ["place_order", "place_stock_order", "order"],
                    **kwargs,
                )
                data = resp if isinstance(resp, dict) else {}
                order_id = str(data.get("orderId") or data.get("order_id") or data.get("id") or "").strip() or None
                filled_qty = _safe_float(data.get("filledQuantity") or data.get("filled_qty") or data.get("filled"), 0.0)
                fill_price = _safe_float(data.get("avgFilledPrice") or data.get("fill_price") or data.get("price"), 0.0)
                status = str(data.get("status") or data.get("state") or "submitted").strip().lower()
                _log_webull_event("stock_order_submitted", {"symbol": symbol, "side": side, "qty": qty, "order_type": order_type, "mode": self._mode()})
                return broker_result(
                    True,
                    broker=self.name,
                    mode=self._mode(),
                    order_id=order_id,
                    filled_qty=filled_qty,
                    fill_price=fill_price,
                    status=status,
                    raw=data,
                )

            return self._with_reconnect(_submit)
        except Exception as exc:
            _log_webull_event("place_stock_order_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc))

    def place_options_order(self, order) -> dict:
        try:
            order = order if isinstance(order, dict) else {}
            option_type = str(order.get("option_type") or "").lower().strip()
            side = _normalize_side(order.get("side"))
            qty = _safe_int(order.get("qty"), 0)
            if option_type not in {"call", "put"} or side not in {"BUY", "SELL"} or qty <= 0:
                return broker_result(False, broker=self.name, mode=self._mode(), error="invalid_options_order")

            def _submit(client):
                payload = {
                    "stock": str(order.get("underlying") or "").upper().strip(),
                    "action": side,
                    "orderType": "LMT" if str(order.get("order_type") or "MKT").upper().strip() in {"LMT", "LIMIT"} else "MKT",
                    "quant": qty,
                    "price": _safe_float(order.get("limit_price"), 0.0) if order.get("limit_price") is not None else None,
                    "lmtPrice": _safe_float(order.get("limit_price"), 0.0) if order.get("limit_price") is not None else None,
                    "optionType": option_type.upper(),
                    "strikePrice": _safe_float(order.get("strike"), 0.0),
                    "expireDate": str(order.get("expiration") or "").strip(),
                }
                resp = self._call_client_method(
                    client,
                    ["place_option_order", "place_options_order", "order_option"],
                    **{k: v for k, v in payload.items() if v not in {None, ""}},
                )
                data = resp if isinstance(resp, dict) else {}
                return broker_result(
                    True,
                    broker=self.name,
                    mode=self._mode(),
                    order_id=str(data.get("orderId") or data.get("order_id") or data.get("id") or "").strip() or None,
                    filled_qty=_safe_float(data.get("filledQuantity") or data.get("filled_qty"), 0.0),
                    fill_price=_safe_float(data.get("avgFilledPrice") or data.get("fill_price") or data.get("price"), 0.0),
                    status=str(data.get("status") or data.get("state") or "submitted").strip().lower(),
                    raw=data,
                )

            return self._with_reconnect(_submit)
        except Exception as exc:
            _log_webull_event("place_options_order_failed", {"error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc))

    def get_option_chain(self, symbol, expiration=None) -> dict:
        try:
            symbol = str(symbol or "").upper().strip()
            if not symbol:
                return broker_result(False, broker=self.name, mode=self._mode(), error="missing_symbol")

            def _fetch(client):
                raw = self._call_client_method(client, ["get_options", "get_option_chain", "get_options_by_expire_date"], stock=symbol)
                rows = raw if isinstance(raw, list) else raw.get("data", []) if isinstance(raw, dict) else []
                chains = {}
                expirations = set()
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    expiry = str(row.get("expireDate") or row.get("expirationDate") or row.get("expiration") or "").strip()
                    if expiration and expiry != str(expiration):
                        continue
                    if not expiry:
                        continue
                    expirations.add(expiry)
                    chain = chains.setdefault(expiry, {"calls": [], "puts": []})
                    option_row = {
                        "strike": _safe_float(row.get("strikePrice") or row.get("strike"), 0.0),
                        "bid": _safe_float(row.get("bidPrice") or row.get("bid"), 0.0),
                        "ask": _safe_float(row.get("askPrice") or row.get("ask"), 0.0),
                        "last": _safe_float(row.get("close") or row.get("lastPrice") or row.get("last"), 0.0),
                        "volume": _safe_int(row.get("volume"), 0),
                        "open_interest": _safe_int(row.get("openInterest") or row.get("open_interest"), 0),
                        "delta": _safe_float(row.get("delta"), 0.0),
                        "gamma": _safe_float(row.get("gamma"), 0.0),
                        "theta": _safe_float(row.get("theta"), 0.0),
                        "vega": _safe_float(row.get("vega"), 0.0),
                        "iv": _safe_float(row.get("impliedVolatility") or row.get("iv"), 0.0),
                    }
                    bucket = "calls" if str(row.get("direction") or row.get("optionType") or "").lower().startswith("c") else "puts"
                    chain[bucket].append(option_row)

                return broker_result(
                    True,
                    broker=self.name,
                    mode=self._mode(),
                    symbol=symbol,
                    expirations=sorted(expirations),
                    chains=chains,
                )

            return self._with_reconnect(_fetch)
        except Exception as exc:
            _log_webull_event("get_option_chain_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), symbol=symbol, expirations=[], chains={})

    def get_stock_quote(self, symbol) -> dict:
        try:
            symbol = str(symbol or "").upper().strip()
            if not symbol:
                return broker_result(False, broker=self.name, mode=self._mode(), error="missing_symbol")

            def _fetch(client):
                raw = self._call_client_method(client, ["get_quote", "get_stock", "get_ticker_quote"], stock=symbol)
                data = raw[0] if isinstance(raw, list) and raw else raw if isinstance(raw, dict) else {}
                return broker_result(
                    True,
                    broker=self.name,
                    mode=self._mode(),
                    symbol=symbol,
                    price=_safe_float(data.get("close") or data.get("lastPrice") or data.get("price"), 0.0),
                    bid=_safe_float(data.get("bidList", [{}])[0].get("price") if isinstance(data.get("bidList"), list) and data.get("bidList") else data.get("bid"), 0.0),
                    ask=_safe_float(data.get("askList", [{}])[0].get("price") if isinstance(data.get("askList"), list) and data.get("askList") else data.get("ask"), 0.0),
                    volume=_safe_int(data.get("volume"), 0),
                    raw=data,
                )

            return self._with_reconnect(_fetch)
        except Exception as exc:
            _log_webull_event("get_stock_quote_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), symbol=symbol)

    def get_watchlist(self) -> list:
        try:
            def _fetch(client):
                raw = self._call_client_method(client, ["get_watchlist", "get_watch_lists"])
                if isinstance(raw, dict):
                    candidates = raw.get("data") or raw.get("watchList") or raw.get("watchlists") or []
                else:
                    candidates = raw if isinstance(raw, list) else []

                symbols = []
                for row in candidates:
                    if isinstance(row, dict):
                        symbol = str(row.get("tickerSymbol") or row.get("symbol") or row.get("ticker") or "").upper().strip()
                        if symbol:
                            symbols.append(symbol)
                return sorted(set(symbols))

            return self._with_reconnect(_fetch)
        except Exception as exc:
            _log_webull_event("get_watchlist_failed", {"error": str(exc)})
            return []

    def cancel_order(self, order_id) -> dict:
        try:
            order_id = str(order_id or "").strip()
            if not order_id:
                return broker_result(False, broker=self.name, mode=self._mode(), error="missing_order_id")

            def _cancel(client):
                raw = self._call_client_method(client, ["cancel_order", "cancelOrder"], order_id)
                data = raw if isinstance(raw, dict) else {}
                return broker_result(True, broker=self.name, mode=self._mode(), order_id=order_id, status=str(data.get("status") or "cancel_requested"), raw=data)

            return self._with_reconnect(_cancel)
        except Exception as exc:
            _log_webull_event("cancel_order_failed", {"order_id": str(order_id or ""), "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), order_id=str(order_id or "").strip())

    def get_order_status(self, order_id) -> dict:
        try:
            order_id = str(order_id or "").strip()
            if not order_id:
                return broker_result(False, broker=self.name, mode=self._mode(), error="missing_order_id")

            def _fetch(client):
                raw = self._call_client_method(client, ["get_order", "get_order_detail", "get_history_orders"], order_id)
                data = raw if isinstance(raw, dict) else {}
                return broker_result(
                    True,
                    broker=self.name,
                    mode=self._mode(),
                    order_id=order_id,
                    status=str(data.get("status") or data.get("state") or "").strip().lower(),
                    filled_qty=_safe_float(data.get("filledQuantity") or data.get("filled_qty"), 0.0),
                    fill_price=_safe_float(data.get("avgFilledPrice") or data.get("fill_price") or data.get("price"), 0.0),
                    raw=data,
                )

            return self._with_reconnect(_fetch)
        except Exception as exc:
            _log_webull_event("get_order_status_failed", {"order_id": str(order_id or ""), "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), order_id=str(order_id or "").strip())
