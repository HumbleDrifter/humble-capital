import json
import sys
import time
from typing import Dict, Any

from dotenv import load_dotenv

from daily_bot_report import _send_telegram
from services.rebalance_proposal_service import (
    get_proposal_by_id,
    mark_proposal_executed,
    proposal_is_stale,
)

load_dotenv("/root/tradingbot/.env", override=True)


def _now_ms() -> str:
    return str(int(time.time() * 1000))


def _load_flask_app():
    from app import app
    return app


def _build_buy_payload(action: Dict[str, Any], proposal_id: str) -> Dict[str, Any]:
    return {
        "secret": "",
        "timestamp": _now_ms(),
        "order_id": f"{proposal_id}-{action['product_id']}-BUY-{int(time.time())}",
        "action": "BUY",
        "signal_type": "APPROVED_REBALANCE_BUY",
        "product_id": action["product_id"],
        "timeframe": "4H",
        "strategy": "approved_rebalance",
        "price": "0",
        "quote_size": str(float(action.get("quote_usd", 0) or 0)),
    }


def _build_trim_payload(action: Dict[str, Any], proposal_id: str) -> Dict[str, Any]:
    return {
        "secret": "",
        "timestamp": _now_ms(),
        "order_id": f"{proposal_id}-{action['product_id']}-EXIT-{int(time.time())}",
        "action": "EXIT",
        "signal_type": "EXIT",
        "product_id": action["product_id"],
        "timeframe": "4H",
        "strategy": "approved_rebalance",
        "price": "0",
        "quote_size": str(float(action.get("quote_usd", 0) or 0)),
    }


def _inject_live_secret(payload: Dict[str, Any]) -> Dict[str, Any]:
    import os
    from dotenv import load_dotenv

    load_dotenv("/root/tradingbot/.env", override=True)
    secret = (
        os.getenv("WEBHOOK_SHARED_SECRET")
        or os.getenv("INTERNAL_API_SECRET")
        or os.getenv("STATUS_SECRET")
        or ""
    )
    payload["secret"] = str(secret).strip()
    return payload


def _execute_action(client, action: Dict[str, Any], proposal_id: str) -> Dict[str, Any]:
    action_type = str(action.get("action", "") or "").lower().strip()

    if action_type == "buy":
        payload = _build_buy_payload(action, proposal_id)
    elif action_type == "trim":
        payload = _build_trim_payload(action, proposal_id)
    else:
        return {
            "ok": False,
            "action": action,
            "status_code": None,
            "body": f"unsupported action: {action_type}",
        }

    payload = _inject_live_secret(payload)
    resp = client.post("/webhook", json=payload)

    return {
        "ok": resp.status_code in (200, 201),
        "action": action,
        "status_code": resp.status_code,
        "body": resp.get_data(as_text=True),
    }


def _render_execution_summary(proposal_id: str, results: list[Dict[str, Any]]) -> str:
    lines = [
        "⚙️ Rebalance Execution Result",
        f"ID: {proposal_id}",
        "",
    ]

    ok_count = 0
    fail_count = 0
    blocked_buys = 0

    for r in results:
        a = r["action"]
        action_type = str(a.get("action", "")).upper()
        pid = str(a.get("product_id", "")).upper()
        usd = float(a.get("quote_usd", 0) or 0)
        body = str(r.get("body", "") or "")

        if r["ok"]:
            ok_count += 1
            lines.append(f"✅ {action_type} {pid} ${usd:,.2f} (HTTP {r['status_code']})")
        else:
            fail_count += 1
            if "buy_not_allowed" in body:
                blocked_buys += 1
            lines.append(f"❌ {action_type} {pid} ${usd:,.2f} (HTTP {r['status_code']})")

    lines.extend([
        "",
        f"Successful: {ok_count}",
        f"Failed: {fail_count}",
    ])

    if blocked_buys:
        lines.append(f"⚠️ Buys blocked by risk controls: {blocked_buys}")

    if ok_count and fail_count:
        lines.append("⚠️ Partial execution")

    return "\n".join(lines).strip()

def execute_proposal_by_id(proposal_id: str) -> str:
    proposal_row = get_proposal_by_id(proposal_id)

    if not proposal_row:
        msg = f"❌ Rebalance execution failed\nID: {proposal_id}\nReason: proposal not found"
        print(msg)
        _send_telegram(msg)
        return msg

    if proposal_row["status"] != "approved":
        msg = (
            f"❌ Rebalance execution failed\n"
            f"ID: {proposal_id}\n"
            f"Reason: proposal not approved\n"
            f"Status: {proposal_row['status']}"
        )
        print(msg)
        _send_telegram(msg)
        return msg

    if proposal_is_stale(proposal_row, max_age_minutes=20):
        msg = (
            f"⏳ Rebalance execution refused\n"
            f"ID: {proposal_id}\n"
            f"Reason: approved proposal is stale"
        )
        print(msg)
        _send_telegram(msg)
        return msg

    proposal = proposal_row.get("proposal") or {}
    actions = proposal.get("proposed_actions", []) or []

    if not actions:
        msg = f"ℹ️ Rebalance execution skipped\nID: {proposal_id}\nReason: no proposed actions"
        print(msg)
        _send_telegram(msg)
        return msg

    app = _load_flask_app()

    results = []
    with app.test_client() as client:
        for action in actions:
            results.append(_execute_action(client, action, proposal_id))
            time.sleep(1)

    success_any = any(r["ok"] for r in results)
    if success_any:
        mark_proposal_executed(proposal_id)

    summary = _render_execution_summary(proposal_id, results)
    print(summary)
    _send_telegram(summary)
    return summary



def main():
    if len(sys.argv) < 2:
        print("Usage: python3 execute_rebalance.py RB-YYYYMMDD-001")
        sys.exit(1)

    proposal_id = sys.argv[1].strip()
    execute_proposal_by_id(proposal_id)

