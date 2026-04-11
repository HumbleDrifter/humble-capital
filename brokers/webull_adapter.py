import json
import os
import threading
import time
from datetime import datetime, timezone

from env_runtime import load_runtime_env

from brokers.base import BrokerAdapter, broker_result

load_runtime_env(override=True)

try:
    from webull.core.client import ApiClient
    from webull.trade.trade_client import TradeClient
    from webull.data.data_client import DataClient
    _WEBULL_SDK_ERROR = None
except Exception as exc:
    ApiClient = None
    TradeClient = None
    DataClient = None
    _WEBULL_SDK_ERROR = exc

_REQUEST_TIMEOUT_SEC = 10
_SESSION_TTL_SEC = 24 * 60 * 60
_SHARED_API_CLIENT = None
_SHARED_TRADE_CLIENT = None
_SHARED_DATA_CLIENT = None
_SHARED_SESSION_TS = 0.0
_SHARED_LOCK = threading.RLock()


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
    raw = str(os.getenv(name, str(default)) or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _utcnow_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_symbol(symbol):
    return str(symbol or "").upper().strip()


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


def _to_dict(value):
    return value.to_dict() if hasattr(value, "to_dict") else value


def _flatten_candidates(value):
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in ("data", "items", "rows", "list", "results", "records"):
            row = value.get(key)
            if isinstance(row, list):
                return row
    return []


def _first_dict(value):
    if isinstance(value, dict):
        for key in ("data", "item", "result"):
            row = value.get(key)
            if isinstance(row, dict):
                return row
        return value
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return value[0]
    return {}


def _resolve_path(obj, dotted_path):
    current = obj
    for part in str(dotted_path or "").split("."):
        current = getattr(current, part, None)
        if current is None:
            return None
    return current


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
        self.enabled = _env_bool("WEBULL_ENABLED", False)
        self.app_key = str(os.getenv("WEBULL_APP_KEY", "") or "").strip()
        self.app_secret = str(os.getenv("WEBULL_APP_SECRET", "") or "").strip()
        self.paper_trading = _env_bool("WEBULL_PAPER_TRADING", True)
        self.region = "us"
        self.endpoint = "api.webull.com"
        self._lock = threading.RLock()
        self._account_id = None

    def _mode(self):
        return "paper" if self.paper_trading else "live"

    def _ensure_ready(self):
        if not self.enabled:
            raise RuntimeError("webull_disabled")
        if _WEBULL_SDK_ERROR is not None or ApiClient is None or TradeClient is None or DataClient is None:
            raise RuntimeError(f"webull_openapi_sdk_unavailable:{_WEBULL_SDK_ERROR}")
        if not self.app_key or not self.app_secret:
            raise RuntimeError("missing_webull_openapi_credentials")

    def _reset_clients(self):
        global _SHARED_API_CLIENT, _SHARED_TRADE_CLIENT, _SHARED_DATA_CLIENT, _SHARED_SESSION_TS
        with _SHARED_LOCK:
            _SHARED_API_CLIENT = None
            _SHARED_TRADE_CLIENT = None
            _SHARED_DATA_CLIENT = None
            _SHARED_SESSION_TS = 0.0

    def _build_clients(self):
        api_client = ApiClient(self.app_key, self.app_secret, self.region)
        try:
            api_client.add_endpoint(self.region, self.endpoint)
        except Exception:
            pass
        try:
            setattr(api_client, "timeout", _REQUEST_TIMEOUT_SEC)
        except Exception:
            pass
        trade_client = TradeClient(api_client)
        data_client = DataClient(api_client)
        return api_client, trade_client, data_client

    def _get_clients(self, force_reset=False):
        global _SHARED_API_CLIENT, _SHARED_TRADE_CLIENT, _SHARED_DATA_CLIENT, _SHARED_SESSION_TS
        self._ensure_ready()

        with _SHARED_LOCK:
            is_fresh = (
                _SHARED_API_CLIENT is not None
                and _SHARED_TRADE_CLIENT is not None
                and _SHARED_DATA_CLIENT is not None
                and not force_reset
                and (time.time() - _SHARED_SESSION_TS) < _SESSION_TTL_SEC
            )
            if is_fresh:
                return _SHARED_API_CLIENT, _SHARED_TRADE_CLIENT, _SHARED_DATA_CLIENT

            api_client, trade_client, data_client = self._build_clients()
            _SHARED_API_CLIENT = api_client
            _SHARED_TRADE_CLIENT = trade_client
            _SHARED_DATA_CLIENT = data_client
            _SHARED_SESSION_TS = time.time()
            _log_webull_event(
                "session_ready",
                {
                    "mode": self._mode(),
                    "endpoint": self.endpoint,
                    "token_path": "conf/token.txt",
                },
            )
            return _SHARED_API_CLIENT, _SHARED_TRADE_CLIENT, _SHARED_DATA_CLIENT

    def _call_first(self, client, dotted_paths, call_variants=None):
        call_variants = call_variants or [({}, ())]
        last_error = None
        for dotted_path in dotted_paths:
            method = _resolve_path(client, dotted_path)
            if not callable(method):
                continue
            for kwargs, args in call_variants:
                try:
                    response = method(*args, **kwargs)
                    return _to_dict(response)
                except Exception as exc:
                    last_error = exc
                    continue
        if last_error is not None:
            raise last_error
        raise AttributeError(f"no_callable_sdk_method:{dotted_paths}")

    def _extract_accounts(self, payload):
        rows = _flatten_candidates(payload)
        accounts = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            account_id = str(row.get("accountId") or row.get("account_id") or row.get("id") or "").strip()
            if account_id:
                accounts.append(row)
        if not accounts:
            row = _first_dict(payload)
            account_id = str(row.get("accountId") or row.get("account_id") or row.get("id") or "").strip()
            if account_id:
                accounts.append(row)
        return accounts

    def _get_account_id(self):
        if self._account_id:
            return self._account_id

        _api_client, trade_client, _data_client = self._get_clients(force_reset=False)
        payload = self._call_first(
            trade_client,
            [
                "account_v2.get_account_list",
                "account.get_account_list",
                "account_v2.list_accounts",
            ],
            call_variants=[({}, ())],
        )
        accounts = self._extract_accounts(payload)
        if not accounts:
            raise RuntimeError("webull_account_list_empty_or_unapproved")
        row = accounts[0]
        self._account_id = str(row.get("accountId") or row.get("account_id") or row.get("id") or "").strip()
        return self._account_id

    def _extract_positions(self, payload):
        rows = _flatten_candidates(payload)
        if rows:
            return [row for row in rows if isinstance(row, dict)]
        row = _first_dict(payload)
        nested = row.get("positions") if isinstance(row, dict) else None
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]
        return []

    def connect(self):
        try:
            self._ensure_ready()
            account_id = self._get_account_id()
            status = "verified"
            _log_webull_event("connect_ok", {"mode": self._mode(), "account_id": account_id})
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                connected=True,
                account_id=account_id,
                approval_status=status,
                endpoint=self.endpoint,
                token_path="conf/token.txt",
                last_ready_at=_utcnow_iso(),
            )
        except Exception as exc:
            _log_webull_event("connect_failed", {"mode": self._mode(), "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), connected=False, error=str(exc))

    def get_account_info(self) -> dict:
        try:
            _api_client, trade_client, _data_client = self._get_clients(force_reset=False)
            account_id = self._get_account_id()
            payload = self._call_first(
                trade_client,
                [
                    "account_v2.get_account_detail",
                    "account_v2.get_account_info",
                    "account.get_account_detail",
                    "account.get_account_info",
                ],
                call_variants=[
                    ({"account_id": account_id}, ()),
                    ({"accountId": account_id}, ()),
                    ({}, (account_id,)),
                ],
            )
            row = _first_dict(payload)
            balance = _safe_float(
                row.get("netLiquidation")
                or row.get("netAssetValue")
                or row.get("balance")
                or row.get("totalAsset")
                or 0.0
            )
            buying_power = _safe_float(
                row.get("buyingPower")
                or row.get("buying_power")
                or row.get("cashBalance")
                or row.get("cash")
                or 0.0
            )
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                balance=balance,
                buying_power=buying_power,
                positions=self.get_positions(),
                raw=row,
            )
        except Exception as exc:
            _log_webull_event("get_account_info_failed", {"error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), balance=0.0, buying_power=0.0, positions=[])

    def get_positions(self) -> list:
        try:
            _api_client, trade_client, _quotes_client = self._get_clients(force_reset=False)
            account_id = self._get_account_id()
            payload = self._call_first(
                trade_client,
                [
                    "account_v2.get_positions",
                    "account.get_positions",
                    "position.get_positions",
                ],
                call_variants=[
                    ({"account_id": account_id}, ()),
                    ({"accountId": account_id}, ()),
                    ({}, (account_id,)),
                ],
            )
            rows = self._extract_positions(payload)
            positions = []
            for row in rows:
                symbol = _normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("underlying"))
                if not symbol:
                    continue
                positions.append(
                    {
                        "symbol": symbol,
                        "qty": _safe_float(row.get("qty") or row.get("quantity") or row.get("position"), 0.0),
                        "avg_cost": _safe_float(row.get("avgCost") or row.get("avg_cost") or row.get("costPrice"), 0.0),
                        "market_value": _safe_float(row.get("marketValue") or row.get("market_value") or row.get("value"), 0.0),
                        "unrealized_pnl": _safe_float(
                            row.get("unrealizedPnl") or row.get("unrealizedProfitLoss") or row.get("unrealized_pnl"),
                            0.0,
                        ),
                        "asset_type": _normalize_asset_type(row.get("assetType") or row.get("secType") or row.get("asset_type")),
                    }
                )
            return positions
        except Exception as exc:
            _log_webull_event("get_positions_failed", {"error": str(exc)})
            return []

    def get_stock_quote(self, symbol) -> dict:
        symbol = _normalize_symbol(symbol)
        if not symbol:
            return broker_result(False, broker=self.name, mode=self._mode(), error="missing_symbol", symbol=symbol)
        try:
            _api_client, _trade_client, data_client = self._get_clients(force_reset=False)
            payload = self._call_first(
                data_client,
                [
                    "stock.get_quote",
                    "stock.get_snapshot",
                    "quote.get_quote",
                    "get_stock_quote",
                ],
                call_variants=[
                    ({"symbol": symbol}, ()),
                    ({"symbols": [symbol]}, ()),
                    ({}, (symbol,)),
                ],
            )
            row = _first_dict(payload)
            bid = _safe_float(row.get("bid") or row.get("bidPrice") or row.get("bestBid"), 0.0)
            ask = _safe_float(row.get("ask") or row.get("askPrice") or row.get("bestAsk"), 0.0)
            price = _safe_float(row.get("close") or row.get("last") or row.get("lastPrice") or row.get("price"), 0.0)
            volume = _safe_int(row.get("volume"), 0)
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                symbol=symbol,
                price=price,
                bid=bid,
                ask=ask,
                volume=volume,
                raw=row,
            )
        except Exception as exc:
            _log_webull_event("get_stock_quote_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), symbol=symbol)

    def get_option_chain(self, symbol, expiration=None) -> dict:
        symbol = _normalize_symbol(symbol)
        if not symbol:
            return broker_result(False, broker=self.name, mode=self._mode(), error="missing_symbol", symbol=symbol, expirations=[], chains={})
        try:
            _api_client, _trade_client, data_client = self._get_clients(force_reset=False)
            payload = self._call_first(
                data_client,
                [
                    "option.get_option_chain",
                    "option.get_chain",
                    "options.get_option_chain",
                    "get_option_chain",
                ],
                call_variants=[
                    ({"symbol": symbol, "expiration": expiration}, ()),
                    ({"symbol": symbol}, ()),
                    ({}, (symbol, expiration) if expiration else (symbol,)),
                ],
            )
            rows = _flatten_candidates(payload)
            chains = {}
            expirations = set()
            for row in rows:
                if not isinstance(row, dict):
                    continue
                exp = str(row.get("expiration") or row.get("expireDate") or row.get("expirationDate") or "").strip()
                if expiration and exp and exp != str(expiration):
                    continue
                if not exp:
                    continue
                expirations.add(exp)
                bucket = "calls" if str(row.get("optionType") or row.get("direction") or "").lower().startswith("c") else "puts"
                chain = chains.setdefault(exp, {"calls": [], "puts": []})
                chain[bucket].append(
                    {
                        "strike": _safe_float(row.get("strike") or row.get("strikePrice"), 0.0),
                        "bid": _safe_float(row.get("bid") or row.get("bidPrice"), 0.0),
                        "ask": _safe_float(row.get("ask") or row.get("askPrice"), 0.0),
                        "last": _safe_float(row.get("last") or row.get("lastPrice") or row.get("close"), 0.0),
                        "volume": _safe_int(row.get("volume"), 0),
                        "open_interest": _safe_int(row.get("openInterest") or row.get("open_interest"), 0),
                        "delta": _safe_float(row.get("delta"), 0.0),
                        "gamma": _safe_float(row.get("gamma"), 0.0),
                        "theta": _safe_float(row.get("theta"), 0.0),
                        "vega": _safe_float(row.get("vega"), 0.0),
                        "iv": _safe_float(row.get("iv") or row.get("impliedVolatility"), 0.0),
                        "contract_id": str(row.get("contractId") or row.get("instrumentId") or row.get("id") or "").strip(),
                    }
                )
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                symbol=symbol,
                expirations=sorted(expirations),
                chains=chains,
            )
        except Exception as exc:
            _log_webull_event("get_option_chain_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), symbol=symbol, expirations=[], chains={})

    def place_stock_order(self, symbol, side, qty, order_type="MKT", limit_price=None) -> dict:
        symbol = _normalize_symbol(symbol)
        side = _normalize_side(side)
        qty = _safe_float(qty, 0.0)
        order_type = str(order_type or "MKT").upper().strip()
        if not symbol or side not in {"BUY", "SELL"} or qty <= 0:
            return broker_result(False, broker=self.name, mode=self._mode(), error="invalid_stock_order")
        try:
            _api_client, trade_client, _data_client = self._get_clients(force_reset=False)
            account_id = self._get_account_id()
            payload = self._call_first(
                trade_client,
                [
                    "order.place_order",
                    "stock_order.place_order",
                    "trade_order.place_order",
                ],
                call_variants=[
                    (
                        {
                            "account_id": account_id,
                            "symbol": symbol,
                            "side": side,
                            "qty": qty,
                            "order_type": order_type,
                            "limit_price": limit_price,
                        },
                        (),
                    ),
                    (
                        {},
                        (
                            account_id,
                            symbol,
                            side,
                            qty,
                            order_type,
                            limit_price,
                        ),
                    ),
                ],
            )
            row = _first_dict(payload)
            order_id = str(row.get("orderId") or row.get("order_id") or row.get("id") or "").strip()
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                order_id=order_id,
                filled_qty=_safe_float(row.get("filledQty") or row.get("filled_qty"), 0.0),
                fill_price=_safe_float(row.get("fillPrice") or row.get("avgFilledPrice"), 0.0),
                status=str(row.get("status") or row.get("orderStatus") or "submitted").strip().lower(),
                raw=row,
            )
        except Exception as exc:
            _log_webull_event("place_stock_order_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc))

    def place_options_order(self, order) -> dict:
        order = order if isinstance(order, dict) else {}
        symbol = _normalize_symbol(order.get("underlying") or order.get("symbol"))
        side = _normalize_side(order.get("side"))
        qty = _safe_int(order.get("qty"), 0)
        order_type = str(order.get("order_type") or "MKT").upper().strip()
        if not symbol or side not in {"BUY", "SELL"} or qty <= 0:
            return broker_result(False, broker=self.name, mode=self._mode(), error="invalid_options_order")
        try:
            _api_client, trade_client, _data_client = self._get_clients(force_reset=False)
            account_id = self._get_account_id()
            payload = self._call_first(
                trade_client,
                [
                    "order.place_order",
                    "option_order.place_order",
                    "trade_order.place_order",
                ],
                call_variants=[
                    (
                        {
                            "account_id": account_id,
                            "underlying": symbol,
                            "expiration": order.get("expiration"),
                            "strike": order.get("strike"),
                            "option_type": order.get("option_type"),
                            "side": side,
                            "qty": qty,
                            "order_type": order_type,
                            "limit_price": order.get("limit_price"),
                        },
                        (),
                    ),
                ],
            )
            row = _first_dict(payload)
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                order_id=str(row.get("orderId") or row.get("order_id") or row.get("id") or "").strip(),
                filled_qty=_safe_float(row.get("filledQty") or row.get("filled_qty"), 0.0),
                fill_price=_safe_float(row.get("fillPrice") or row.get("avgFilledPrice"), 0.0),
                status=str(row.get("status") or row.get("orderStatus") or "submitted").strip().lower(),
                raw=row,
            )
        except Exception as exc:
            _log_webull_event("place_options_order_failed", {"symbol": symbol, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc))

    def cancel_order(self, order_id) -> dict:
        order_id = str(order_id or "").strip()
        if not order_id:
            return broker_result(False, broker=self.name, mode=self._mode(), error="missing_order_id", order_id=order_id)
        try:
            _api_client, trade_client, _data_client = self._get_clients(force_reset=False)
            payload = self._call_first(
                trade_client,
                [
                    "order.cancel_order",
                    "order.cancel",
                ],
                call_variants=[
                    ({"order_id": order_id}, ()),
                    ({}, (order_id,)),
                ],
            )
            row = _first_dict(payload)
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                order_id=order_id,
                status=str(row.get("status") or row.get("orderStatus") or "cancel_requested").strip().lower(),
                raw=row,
            )
        except Exception as exc:
            _log_webull_event("cancel_order_failed", {"order_id": order_id, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), order_id=order_id)

    def get_order_status(self, order_id) -> dict:
        order_id = str(order_id or "").strip()
        if not order_id:
            return broker_result(False, broker=self.name, mode=self._mode(), error="missing_order_id", order_id=order_id)
        try:
            _api_client, trade_client, _quotes_client = self._get_clients(force_reset=False)
            payload = self._call_first(
                trade_client,
                [
                    "order.get_order_detail",
                    "order.get_order",
                    "order.get_order_status",
                ],
                call_variants=[
                    ({"order_id": order_id}, ()),
                    ({}, (order_id,)),
                ],
            )
            row = _first_dict(payload)
            return broker_result(
                True,
                broker=self.name,
                mode=self._mode(),
                order_id=order_id,
                status=str(row.get("status") or row.get("orderStatus") or "unknown").strip().lower(),
                filled_qty=_safe_float(row.get("filledQty") or row.get("filled_qty"), 0.0),
                fill_price=_safe_float(row.get("fillPrice") or row.get("avgFilledPrice"), 0.0),
                raw=row,
            )
        except Exception as exc:
            _log_webull_event("get_order_status_failed", {"order_id": order_id, "error": str(exc)})
            return broker_result(False, broker=self.name, mode=self._mode(), error=str(exc), order_id=order_id)

    def get_watchlist(self) -> list:
        try:
            _api_client, _trade_client, data_client = self._get_clients(force_reset=False)
            payload = self._call_first(
                data_client,
                [
                    "watchlist.get_watchlist",
                    "watchlist.list_watchlists",
                    "get_watchlist",
                ],
                call_variants=[({}, ())],
            )
            rows = _flatten_candidates(payload)
            symbols = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                symbol = _normalize_symbol(row.get("symbol") or row.get("ticker") or row.get("displaySymbol"))
                if symbol:
                    symbols.append(symbol)
            return sorted(set(symbols))
        except Exception as exc:
            _log_webull_event("get_watchlist_failed", {"error": str(exc)})
            return []
