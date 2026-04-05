import os
import threading
import traceback
from datetime import datetime, timezone

from env_runtime import load_runtime_env

from brokers.base import BrokerAdapter, broker_result

load_runtime_env(override=True)

_SHARED_IB = None
_SHARED_RUNTIME_KEY = None
_SHARED_IB_LOCK = threading.RLock()
_LAST_ERROR = None
_LAST_READY_AT = None


class ContractQualificationError(Exception):
    pass


class ConnectionRetryableError(Exception):
    pass


def _env_bool(name, default=False):
    value = str(os.getenv(name, str(default)) or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _env_int(name, default=0):
    try:
        return int(float(os.getenv(name, default) or default))
    except Exception:
        return int(default)


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _utcnow_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_ibkr_runtime_config():
    load_runtime_env(override=True)
    paper_mode = _env_bool("IBKR_PAPER_TRADING", True)
    return {
        "enabled": _env_bool("IBKR_ENABLED", False),
        "host": str(os.getenv("IBKR_HOST", "127.0.0.1") or "127.0.0.1").strip(),
        "paper_mode": paper_mode,
        "port": _env_int("IBKR_PORT_PAPER" if paper_mode else "IBKR_PORT_LIVE", 7497 if paper_mode else 7496),
        "client_id": _env_int("IBKR_CLIENT_ID", 91),
        "account": str(os.getenv("IBKR_ACCOUNT", "") or "").strip(),
        "allow_live_options": _env_bool("IBKR_ALLOW_LIVE_OPTIONS", False),
        "exchange": str(os.getenv("IBKR_OPTIONS_EXCHANGE", "SMART") or "SMART").strip() or "SMART",
        "currency": str(os.getenv("IBKR_OPTIONS_CURRENCY", "USD") or "USD").strip() or "USD",
    }


class IBKRAdapter(BrokerAdapter):
    name = "ibkr"

    def __init__(self, runtime_config=None):
        # Keep runtime configuration explicit here so call sites can inject paper/live settings safely.
        self.runtime_config = runtime_config or get_ibkr_runtime_config()

    def _mark_ready(self):
        global _LAST_READY_AT, _LAST_ERROR
        with _SHARED_IB_LOCK:
            _LAST_READY_AT = _utcnow_iso()
            _LAST_ERROR = None

    def _set_last_error(self, error):
        global _LAST_ERROR
        with _SHARED_IB_LOCK:
            _LAST_ERROR = str(error or "").strip() or None

    def _runtime_key(self):
        runtime = dict(self.runtime_config or {})
        return (
            str(runtime.get("host") or "").strip(),
            int(runtime.get("port") or 0),
            int(runtime.get("client_id") or 0),
            str(runtime.get("account") or "").strip(),
            bool(runtime.get("paper_mode")),
        )

    def _get_connection(self, IB):
        global _SHARED_IB, _SHARED_RUNTIME_KEY

        with _SHARED_IB_LOCK:
            runtime = dict(self.runtime_config or {})
            runtime_key = self._runtime_key()
            ib = _SHARED_IB

            # Keep the transport isolated here and reuse one compatible IB session when possible.
            if ib is not None:
                try:
                    if _SHARED_RUNTIME_KEY != runtime_key and ib.isConnected():
                        ib.disconnect()
                        ib = None
                        _SHARED_IB = None
                        _SHARED_RUNTIME_KEY = None
                    elif ib.isConnected():
                        self._mark_ready()
                        return ib
                except Exception:
                    ib = None
                    _SHARED_IB = None
                    _SHARED_RUNTIME_KEY = None

            ib = IB()
            try:
                ib.connect(
                    runtime.get("host"),
                    int(runtime.get("port") or 0),
                    clientId=int(runtime.get("client_id") or 0),
                    account=(runtime.get("account") or None),
                    readonly=False,
                    timeout=10,
                )
            except Exception as exc:
                self._set_last_error(exc)
                raise
            _SHARED_IB = ib
            _SHARED_RUNTIME_KEY = runtime_key
            self._mark_ready()
            return ib

    def _reset_shared_connection(self):
        global _SHARED_IB, _SHARED_RUNTIME_KEY
        with _SHARED_IB_LOCK:
            ib = _SHARED_IB
            _SHARED_IB = None
            _SHARED_RUNTIME_KEY = None
            if ib is None:
                return
            try:
                if ib.isConnected():
                    ib.disconnect()
            except Exception:
                pass

    def _connection_status(self):
        global _SHARED_IB, _SHARED_RUNTIME_KEY, _LAST_ERROR, _LAST_READY_AT
        with _SHARED_IB_LOCK:
            connected = False
            runtime_matches = False
            try:
                connected = bool(_SHARED_IB is not None and _SHARED_IB.isConnected())
                runtime_matches = bool(_SHARED_RUNTIME_KEY == self._runtime_key())
            except Exception:
                connected = False
                runtime_matches = False
            return {
                "connected": connected,
                "runtime_matches": runtime_matches,
                "usable": bool(connected and runtime_matches),
                "last_error": _LAST_ERROR,
                "last_ready_at": _LAST_READY_AT,
            }

    def health_status(self):
        runtime = dict(self.runtime_config or {})
        connection = self._connection_status()
        reason = ""
        options_enabled = _env_bool("OPTIONS_ENABLED", False)
        import_error = None
        try:
            import ib_insync  # noqa: F401
        except Exception as exc:
            import_error = str(exc)
        if not runtime.get("enabled"):
            reason = "ibkr_disabled"
        elif not options_enabled:
            reason = "options_disabled"
        elif import_error:
            reason = "ib_insync_unavailable"
        elif not connection.get("connected"):
            reason = "not_connected"
        return {
            "ok": bool(runtime.get("enabled")) and bool(options_enabled) and not bool(import_error) and bool(connection.get("connected")),
            "ibkr_enabled": bool(runtime.get("enabled")),
            "options_enabled": options_enabled,
            "paper_mode": bool(runtime.get("paper_mode")),
            "host": str(runtime.get("host") or "").strip(),
            "port": int(runtime.get("port") or 0),
            "connected": bool(connection.get("connected")),
            "account": str(runtime.get("account") or "").strip(),
            "reason": reason or connection.get("last_error") or None,
            "connection_reused": bool(connection.get("connected") and connection.get("runtime_matches")),
            "connection_reuse_capable": True,
            "last_error": import_error or connection.get("last_error"),
            "last_ready_at": connection.get("last_ready_at"),
        }

    def _build_single_contract(self, ib, Option, order, leg):
        contract = Option(
            symbol=str(order.get("underlying", "")).strip().upper(),
            lastTradeDateOrContractMonth=str(leg.get("expiry", "")).strip(),
            strike=_safe_float(leg.get("strike")),
            right=str(leg.get("right_code", leg.get("right", ""))).strip().upper(),
            exchange=str(leg.get("exchange") or self.runtime_config.get("exchange") or "SMART").strip(),
            currency=str(leg.get("currency") or self.runtime_config.get("currency") or "USD").strip(),
            multiplier="100",
        )
        qualified = ib.qualifyContracts(contract)
        qualified_contract = qualified[0] if qualified else contract
        if not getattr(qualified_contract, "conId", None):
            raise ContractQualificationError(
                f"failed to qualify option contract for {str(order.get('underlying', '')).strip().upper()} "
                f"{str(leg.get('expiry', '')).strip()} {_safe_float(leg.get('strike'))} "
                f"{str(leg.get('right_code', leg.get('right', ''))).strip().upper()}"
            )
        return qualified_contract

    def _build_combo_contract(self, ib, Bag, ComboLeg, Option, order):
        qualified = []
        combo_legs = []

        for leg in order.get("legs", []):
            contract = self._build_single_contract(ib, Option, order, leg)
            qualified.append((leg, contract))

        # Manual follow-up: if IBKR transport changes later, this combo builder is the only
        # place that should need library-specific BAG/ComboLeg updates.
        for leg, contract in qualified:
            combo_legs.append(
                ComboLeg(
                    conId=int(contract.conId),
                    ratio=int(leg.get("quantity", 1) or 1),
                    action=str(leg.get("side", "BUY")).strip().upper(),
                    exchange=str(leg.get("exchange") or self.runtime_config.get("exchange") or "SMART").strip(),
                )
            )

        bag = Bag()
        bag.symbol = str(order.get("underlying", "")).strip().upper()
        bag.secType = "BAG"
        bag.currency = str(self.runtime_config.get("currency") or "USD").strip()
        bag.exchange = str(self.runtime_config.get("exchange") or "SMART").strip()
        bag.comboLegs = combo_legs
        return bag

    def _submit_order_once(self, ib, LimitOrder, Bag, ComboLeg, Option, order):
        legs = list(order.get("legs") or [])
        quantity = int(max((int(leg.get("quantity", 1) or 1) for leg in legs), default=1))
        overall_action = str(order.get("ibkr_action") or "BUY").strip().upper() or "BUY"
        limit_price = _safe_float(order.get("limit_price"))
        if not legs:
            return broker_result(False, reason="missing_order_legs", broker=self.name)
        if len(legs) > 1 and limit_price <= 0:
            return broker_result(False, reason="invalid_combo_limit_price", broker=self.name)

        contract = (
            self._build_single_contract(ib, Option, order, legs[0])
            if len(legs) == 1
            else self._build_combo_contract(ib, Bag, ComboLeg, Option, order)
        )

        ib_order = LimitOrder(
            action=overall_action,
            totalQuantity=quantity,
            lmtPrice=limit_price,
            tif=str(order.get("tif", "DAY")).strip().upper() or "DAY",
            account=(self.runtime_config.get("account") or None),
        )

        try:
            trade = ib.placeOrder(contract, ib_order)
            ib.sleep(1.5)
        except Exception as exc:
            if "connect" in str(exc or "").lower() or "socket" in str(exc or "").lower() or "disconnect" in str(exc or "").lower():
                raise ConnectionRetryableError(str(exc))
            raise

        status = str(getattr(getattr(trade, "orderStatus", None), "status", "") or "Submitted").strip() or "Submitted"
        order_id = getattr(getattr(trade, "order", None), "orderId", None)
        contract_summary = {
            "secType": str(getattr(contract, "secType", "") or "").strip(),
            "symbol": str(getattr(contract, "symbol", "") or "").strip().upper(),
            "expiry": str(getattr(contract, "lastTradeDateOrContractMonth", "") or "").strip(),
            "strike": _safe_float(getattr(contract, "strike", 0.0)),
            "right_code": str(getattr(contract, "right", "") or "").strip().upper(),
            "exchange": str(getattr(contract, "exchange", "") or "").strip(),
            "currency": str(getattr(contract, "currency", "") or "").strip(),
        }
        legs_summary = [
            {
                "side": str((leg or {}).get("side", "") or "").strip().upper(),
                "quantity": int((leg or {}).get("quantity", 0) or 0),
                "expiry": str((leg or {}).get("expiry", "") or "").strip(),
                "strike": _safe_float((leg or {}).get("strike", 0.0)),
                "right_code": str((leg or {}).get("right_code", (leg or {}).get("right", "")) or "").strip().upper(),
            }
            for leg in legs
        ]
        self._mark_ready()
        return broker_result(
            True,
            broker=self.name,
            mode="paper" if self.runtime_config.get("paper_mode") else "live",
            order_id=str(order_id) if order_id is not None else None,
            status=status,
            order_type="LIMIT",
            limit_price=limit_price,
            tif=str(order.get("tif", "DAY")).strip().upper() or "DAY",
            contract_summary=contract_summary,
            legs_summary=legs_summary,
        )

    def sync_options_state(self):
        runtime = dict(self.runtime_config or {})
        if not runtime.get("enabled"):
            return broker_result(False, reason="ibkr_disabled", broker=self.name, orders=[], positions=[])

        try:
            from ib_insync import IB
        except Exception as exc:
            self._set_last_error(exc)
            return broker_result(False, reason="ib_insync_unavailable", broker=self.name, error=str(exc), orders=[], positions=[])

        try:
            ib = self._get_connection(IB)
            open_orders = list(ib.reqAllOpenOrders() or [])
            positions = list(ib.positions() or [])
            self._mark_ready()
        except Exception as exc:
            self._set_last_error(exc)
            return broker_result(False, reason="ibkr_sync_failed", broker=self.name, error=str(exc), orders=[], positions=[])

        order_rows = []
        for trade in open_orders:
            contract = getattr(trade, "contract", None)
            order_obj = getattr(trade, "order", None)
            order_status = getattr(trade, "orderStatus", None)
            sec_type = str(getattr(contract, "secType", "") or "").strip().upper()
            if sec_type not in {"OPT", "BAG"}:
                continue
            order_rows.append(
                {
                    "broker_order_id": str(getattr(order_obj, "orderId", "") or "").strip(),
                    "created_at": _utcnow_iso(),
                    "updated_at": _utcnow_iso(),
                    "underlying": str(getattr(contract, "symbol", "") or "").strip().upper(),
                    "strategy": "combo" if sec_type == "BAG" else "single_leg",
                    "broker": self.name,
                    "asset_class": "option",
                    "order_type": str(getattr(order_obj, "orderType", "LMT") or "LMT").strip().upper(),
                    "limit_price": _safe_float(getattr(order_obj, "lmtPrice", 0.0)),
                    "tif": str(getattr(order_obj, "tif", "DAY") or "DAY").strip().upper(),
                    "status": str(getattr(order_status, "status", "") or "").strip() or "Open",
                    "source": "ibkr_sync",
                    "contract_summary": {
                        "secType": sec_type,
                        "symbol": str(getattr(contract, "symbol", "") or "").strip().upper(),
                        "expiry": str(getattr(contract, "lastTradeDateOrContractMonth", "") or "").strip(),
                        "strike": _safe_float(getattr(contract, "strike", 0.0)),
                        "right_code": str(getattr(contract, "right", "") or "").strip().upper(),
                    },
                    "legs": [],
                }
            )

        position_rows = []
        for item in positions:
            contract = getattr(item, "contract", None)
            sec_type = str(getattr(contract, "secType", "") or "").strip().upper()
            if sec_type != "OPT":
                continue
            qty = _safe_float(getattr(item, "position", 0.0))
            position_rows.append(
                {
                    "contract_key": (
                        f"{str(getattr(contract, 'symbol', '') or '').strip().upper()}|"
                        f"{str(getattr(contract, 'lastTradeDateOrContractMonth', '') or '').strip()}|"
                        f"{_safe_float(getattr(contract, 'strike', 0.0))}|"
                        f"{str(getattr(contract, 'right', '') or '').strip().upper()}"
                    ),
                    "updated_at": _utcnow_iso(),
                    "broker": self.name,
                    "account": str(getattr(item, "account", "") or "").strip() or str(runtime.get("account") or "").strip(),
                    "underlying": str(getattr(contract, "symbol", "") or "").strip().upper(),
                    "expiry": str(getattr(contract, "lastTradeDateOrContractMonth", "") or "").strip(),
                    "strike": _safe_float(getattr(contract, "strike", 0.0)),
                    "right_code": str(getattr(contract, "right", "") or "").strip().upper(),
                    "quantity": abs(qty),
                    "side": "LONG" if qty >= 0 else "SHORT",
                    "avg_cost": _safe_float(getattr(item, "avgCost", 0.0)),
                    "market_price": 0.0,
                    "market_value": 0.0,
                    "status": "OPEN",
                }
            )

        return broker_result(
            True,
            broker=self.name,
            mode="paper" if runtime.get("paper_mode") else "live",
            orders=order_rows,
            positions=position_rows,
            connection_reused=bool(self._connection_status().get("usable")),
            reconnect_attempted=False,
            last_ready_at=self._connection_status().get("last_ready_at"),
            summary={
                "orders_count": len(order_rows),
                "positions_count": len(position_rows),
            },
        )

    def place_options_order(self, order):
        runtime = dict(self.runtime_config or {})
        if not runtime.get("enabled"):
            return broker_result(False, reason="ibkr_disabled", broker=self.name)

        if not runtime.get("paper_mode") and not runtime.get("allow_live_options"):
            return broker_result(False, reason="live_options_disabled", broker=self.name)

        if str(order.get("order_type", "")).strip().upper() not in {"LIMIT", "LMT"}:
            return broker_result(False, reason="limit_orders_only", broker=self.name)

        try:
            from ib_insync import Bag, ComboLeg, IB, LimitOrder, Option
        except Exception as exc:
            self._set_last_error(exc)
            return broker_result(False, reason="ib_insync_unavailable", broker=self.name, error=str(exc))

        connection_reused = False
        reconnect_attempted = False
        try:
            with _SHARED_IB_LOCK:
                connection_state = self._connection_status()
                connection_reused = bool(connection_state.get("connected") and connection_state.get("runtime_matches"))
                ib = self._get_connection(IB)
                result = self._submit_order_once(ib, LimitOrder, Bag, ComboLeg, Option, order)
                return {
                    **result,
                    "connection_reused": connection_reused,
                    "reconnect_attempted": reconnect_attempted,
                }
        except ContractQualificationError as exc:
            self._set_last_error(exc)
            return broker_result(
                False,
                reason="contract_qualification_failed",
                broker=self.name,
                error=str(exc),
                connection_reused=connection_reused,
                reconnect_attempted=reconnect_attempted,
            )
        except ConnectionRetryableError as exc:
            reconnect_attempted = True
            # Retry scope stays intentionally narrow: reset the shared session, reconnect once, and fail closed.
            self._reset_shared_connection()
            try:
                with _SHARED_IB_LOCK:
                    ib = self._get_connection(IB)
                    result = self._submit_order_once(ib, LimitOrder, Bag, ComboLeg, Option, order)
                    return {
                        **result,
                        "connection_reused": connection_reused,
                        "reconnect_attempted": reconnect_attempted,
                    }
            except ContractQualificationError as retry_exc:
                self._set_last_error(retry_exc)
                return broker_result(
                    False,
                    reason="contract_qualification_failed",
                    broker=self.name,
                    error=str(retry_exc),
                    connection_reused=connection_reused,
                    reconnect_attempted=reconnect_attempted,
                )
            except Exception as retry_exc:
                traceback.print_exc()
                self._set_last_error(retry_exc or exc)
                return broker_result(
                    False,
                    reason="ibkr_retry_failed",
                    broker=self.name,
                    error=str(retry_exc or exc),
                    connection_reused=connection_reused,
                    reconnect_attempted=reconnect_attempted,
                )
        except Exception as exc:
            traceback.print_exc()
            self._set_last_error(exc)
            return broker_result(
                False,
                reason="ibkr_order_failed",
                broker=self.name,
                error=str(exc),
                connection_reused=connection_reused,
                reconnect_attempted=reconnect_attempted,
            )
