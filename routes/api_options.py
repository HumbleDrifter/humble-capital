from datetime import datetime, timedelta, timezone

from flask import Blueprint, jsonify, request, session

from brokers.ibkr_adapter import IBKRAdapter, get_ibkr_runtime_config
from options.validator import get_options_risk_config
from routes.api import require_admin_auth
from services.config_proposal_service import (
    OPTIONS_ORDER_PROPOSAL_TYPE,
    generate_options_order_proposal,
    proposal_is_stale,
)
from storage import (
    get_config_proposal_by_id,
    list_open_options_positions,
    list_recent_options_executions,
    list_recent_options_orders,
    replace_options_positions_snapshot,
    save_options_order_record,
)
from workers.execution_queue import submit_job

api_options_bp = Blueprint("api_options", __name__)


def _proposal_actor():
    return (
        str(session.get("username") or "").strip()
        or str(session.get("email") or "").strip()
        or str(session.get("user_id") or "").strip()
        or None
    )


def _safe_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _choose_test_expiry(risk):
    min_dte = max(0, _safe_int(risk.get("min_dte", 1), 1))
    max_dte = max(min_dte, _safe_int(risk.get("max_dte", 45), 45))
    if min_dte == 0 and not bool(risk.get("allow_0dte")):
        min_dte = 1
    target_dte = min(max(min_dte + 3, 10), max_dte)
    return (datetime.now(timezone.utc).date() + timedelta(days=target_dte)).strftime("%Y%m%d")


def _build_test_options_payload(overrides=None):
    overrides = overrides if isinstance(overrides, dict) else {}
    risk = get_options_risk_config()
    underlyings = list(risk.get("allowed_underlyings") or [])
    underlying = str(overrides.get("underlying") or (underlyings[0] if underlyings else "SPY")).strip().upper() or "SPY"
    expiry = str(overrides.get("expiry") or "").strip() or _choose_test_expiry(risk)
    right = str(overrides.get("right") or "CALL").strip().upper() or "CALL"
    strike = float(overrides.get("strike", 0.0) or 0.0)
    if strike <= 0:
        strike = 100.0
    quantity = max(1, _safe_int(overrides.get("quantity", 1), 1))
    limit_price = float(overrides.get("limit_price", 1.0) or 1.0)

    return {
        "asset_class": "option",
        "broker": "ibkr",
        "action": "BUY",
        "underlying": underlying,
        "strategy": str(overrides.get("strategy") or ("long_put" if right == "PUT" else "long_call")).strip().lower(),
        "legs": [
            {
                "side": "BUY",
                "right": right,
                "strike": strike,
                "expiry": expiry,
                "quantity": quantity,
                "exchange": str(overrides.get("exchange") or "SMART").strip() or "SMART",
                "currency": str(overrides.get("currency") or "USD").strip() or "USD",
            }
        ],
        "order_type": str(overrides.get("order_type") or "LIMIT").strip().upper() or "LIMIT",
        "limit_price": limit_price,
        "tif": str(overrides.get("tif") or "DAY").strip().upper() or "DAY",
        "source": str(overrides.get("source") or "admin_test_submit").strip() or "admin_test_submit",
        "broker_mode": str(overrides.get("broker_mode") or "paper").strip().lower() or "paper",
    }


@api_options_bp.route("/api/options/proposals", methods=["POST"])
@require_admin_auth
def api_options_proposals_create():
    try:
        payload = request.get_json(silent=True) or {}
        result = generate_options_order_proposal(payload)
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_options_bp.route("/api/options/ibkr/health", methods=["GET"])
@require_admin_auth
def api_options_ibkr_health():
    try:
        adapter = IBKRAdapter(get_ibkr_runtime_config())
        return jsonify(adapter.health_status())
    except Exception as exc:
        runtime = get_ibkr_runtime_config()
        risk = get_options_risk_config()
        return jsonify(
            {
                "ok": False,
                "ibkr_enabled": bool(runtime.get("enabled")),
                "options_enabled": bool(risk.get("options_enabled")),
                "paper_mode": bool(runtime.get("paper_mode")),
                "host": str(runtime.get("host") or "").strip(),
                "port": int(runtime.get("port") or 0),
                "connected": False,
                "account": str(runtime.get("account") or "").strip(),
                "reason": str(exc),
                "connection_reused": False,
                "connection_reuse_capable": True,
                "last_error": str(exc),
                "last_ready_at": None,
            }
        ), 500


@api_options_bp.route("/api/options/executions", methods=["GET"])
@require_admin_auth
def api_options_executions():
    try:
        limit = max(1, min(50, _safe_int(request.args.get("limit"), 10)))
        items = list_recent_options_executions(limit=limit)
        return jsonify({"ok": True, "items": items, "count": len(items)})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "items": []}), 500


@api_options_bp.route("/api/options/orders", methods=["GET"])
@require_admin_auth
def api_options_orders():
    try:
        limit = max(1, min(50, _safe_int(request.args.get("limit"), 10)))
        items = list_recent_options_orders(limit=limit)
        return jsonify({"ok": True, "items": items, "count": len(items)})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "items": []}), 500


@api_options_bp.route("/api/options/positions", methods=["GET"])
@require_admin_auth
def api_options_positions():
    try:
        items = list_open_options_positions()
        return jsonify({"ok": True, "items": items, "count": len(items)})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "items": []}), 500


@api_options_bp.route("/api/options/sync", methods=["POST"])
@require_admin_auth
def api_options_sync():
    try:
        adapter = IBKRAdapter(get_ibkr_runtime_config())
        result = adapter.sync_options_state()
        if not result.get("ok"):
            return jsonify(result), 400

        orders = list(result.get("orders") or [])
        positions = list(result.get("positions") or [])
        for item in orders:
            save_options_order_record(item)
        positions_count = replace_options_positions_snapshot(positions)
        health = adapter.health_status()
        return jsonify(
            {
                "ok": True,
                "status": "synced",
                "orders_count": len(orders),
                "positions_count": positions_count,
                "health": health,
                "summary": result.get("summary") if isinstance(result.get("summary"), dict) else {},
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "status": "sync_failed"}), 500


@api_options_bp.route("/api/options/proposals/test_submit", methods=["POST"])
@require_admin_auth
def api_options_proposals_test_submit():
    try:
        overrides = request.get_json(silent=True) or {}
        result = generate_options_order_proposal(_build_test_options_payload(overrides))
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@api_options_bp.route("/api/options/proposals/<proposal_id>/execute", methods=["POST"])
@require_admin_auth
def api_options_proposals_execute(proposal_id):
    try:
        proposal_record = get_config_proposal_by_id(proposal_id)
        if not proposal_record:
            return jsonify({"ok": False, "reason": "not_found", "proposal_id": str(proposal_id or "").strip()}), 404

        if str(proposal_record.get("proposal_type") or "").strip() != OPTIONS_ORDER_PROPOSAL_TYPE:
            return jsonify({"ok": False, "reason": "wrong_proposal_type", "proposal_id": str(proposal_id or "").strip()}), 400

        if proposal_is_stale(proposal_record):
            return jsonify({"ok": False, "reason": "expired", "proposal_id": str(proposal_id or "").strip()}), 400

        current_status = str(proposal_record.get("status") or "").strip().lower()
        if current_status == "applied":
            return jsonify({"ok": False, "reason": "already_applied", "proposal_id": str(proposal_id or "").strip()}), 400
        if current_status != "approved":
            return jsonify({"ok": False, "reason": "cannot_execute_until_approved", "proposal_id": str(proposal_id or "").strip(), "status": current_status}), 400

        proposal = proposal_record.get("proposal") if isinstance(proposal_record.get("proposal"), dict) else {}
        order = proposal.get("order") if isinstance(proposal.get("order"), dict) else {}
        if not order:
            return jsonify({"ok": False, "reason": "missing_order_payload", "proposal_id": str(proposal_id or "").strip()}), 400

        submit_job(
            {
                **order,
                "asset_class": "option",
                "broker": "ibkr",
                "proposal_id": str(proposal_id or "").strip(),
                "approval_verified": True,
                "requested_by": _proposal_actor(),
            }
        )
        save_options_order_record(
            {
                "proposal_id": str(proposal_id or "").strip(),
                "underlying": str(order.get("underlying") or "").strip().upper(),
                "strategy": str(order.get("strategy") or "").strip().lower(),
                "broker": "ibkr",
                "asset_class": "option",
                "order_type": str(order.get("order_type") or "").strip().upper(),
                "limit_price": float(order.get("limit_price") or 0.0),
                "tif": str(order.get("tif") or "").strip().upper(),
                "status": "Queued",
                "source": str(order.get("source") or "proposal_execute").strip() or "proposal_execute",
                "contract_summary": {
                    "symbol": str(order.get("underlying") or "").strip().upper(),
                },
                "legs": list(order.get("legs") or []),
            }
        )

        return jsonify(
            {
                "ok": True,
                "status": "queued",
                "proposal_id": str(proposal_id or "").strip(),
                "message": "Approved options proposal queued for execution review.",
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "proposal_id": str(proposal_id or "").strip()}), 500
