import traceback

from core.order_router import route_order
from reconcile import reconcile_positions
from rebalancer import execute_satellite_signal, execute_buy, execute_rebalance_plan
from portfolio import get_portfolio_snapshot, portfolio_summary
from notify import send_telegram
from storage import get_config_proposal_by_id, set_config_proposal_status


def ensure_fresh_state():
    try:
        reconcile_positions()
    except Exception as e:
        send_telegram(f"⚠ reconcile failed: {e}")


def process_trade_job(job):
    order_id = job.get("order_id")
    signal_type = str(job.get("signal_type") or "").upper().strip()
    product_id = str(job.get("product_id") or "").upper().strip()
    asset_class = str(job.get("asset_class") or "").lower().strip()
    broker = str(job.get("broker") or "").lower().strip()
    proposal_id = str(job.get("proposal_id") or "").strip()

    try:
        if asset_class in {"option", "options"} or broker == "ibkr":
            result = route_order(job)

            if result.get("ok") and proposal_id:
                existing = get_config_proposal_by_id(proposal_id)
                if existing and str(existing.get("status") or "").strip().lower() == "approved":
                    set_config_proposal_status(
                        proposal_id=proposal_id,
                        status="applied",
                        timestamp_field="applied_at",
                        actor_field="applied_by",
                        actor=str(job.get("requested_by") or "options_executor").strip() or "options_executor",
                        expected_current_status="approved",
                    )

            send_telegram(
                f"🧾 options execution\n"
                f"proposal={proposal_id or 'n/a'}\n"
                f"broker={broker or 'ibkr'}\n"
                f"underlying={str(job.get('underlying') or '').upper()}\n"
                f"strategy={str(job.get('strategy') or '').lower()}\n"
                f"ok={result.get('ok')}\n"
                f"reason={result.get('reason', result.get('status', 'submitted'))}"
            )
            return result

        ensure_fresh_state()

        if signal_type in {
            "SATELLITE_BUY",
            "SATELLITE_BUY_EARLY",
            "SATELLITE_BUY_HEAVY",
        }:
            result = execute_satellite_signal(product_id, signal_type=signal_type)

            send_telegram(
                f"📡 satellite signal\n"
                f"order={order_id}\n"
                f"product={product_id}\n"
                f"signal={signal_type}\n"
                f"ok={result.get('ok')}\n"
                f"reason={result.get('reason')}"
            )
            return result

        if signal_type == "CORE_BUY_WINDOW":
            snapshot = get_portfolio_snapshot()
            summary = portfolio_summary(snapshot)

            allowed = float(
                summary.get("assets", {})
                .get(product_id, {})
                .get("allowed_buy_usd", 0.0) or 0.0
            )

            result = execute_buy(product_id, allowed, signal_type=signal_type)

            send_telegram(
                f"🧱 core buy\n"
                f"order={order_id}\n"
                f"product={product_id}\n"
                f"amount=${allowed:.2f}\n"
                f"ok={result.get('ok')}\n"
                f"reason={result.get('reason')}"
            )
            return result

        if signal_type == "REBALANCE":
            result = execute_rebalance_plan()

            send_telegram(
                f"♻ rebalance run\n"
                f"order={order_id}\n"
                f"ok={result.get('ok')}\n"
                f"reason={result.get('reason')}"
            )
            return result

        return {"ok": False, "reason": f"unknown signal_type={signal_type}"}

    except Exception as e:
        traceback.print_exc()

        send_telegram(
            f"🔥 trade job failed\n"
            f"order={order_id}\n"
            f"signal={signal_type}\n"
            f"product={product_id}\n"
            f"error={e}"
        )
        return {"ok": False, "error": str(e)}
