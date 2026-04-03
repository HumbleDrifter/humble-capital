import sys
from dotenv import load_dotenv

from daily_bot_report import _send_telegram
from services.rebalance_proposal_service import approve_proposal, get_proposal_by_id

load_dotenv("/root/tradingbot/.env", override=True)


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 approve_rebalance.py RB-YYYYMMDD-001")
        sys.exit(1)

    proposal_id = sys.argv[1].strip()
    result = approve_proposal(proposal_id)

    if not result.get("ok"):
        msg = (
            f"❌ Rebalance approval failed\n"
            f"ID: {proposal_id}\n"
            f"Reason: {result.get('reason')}\n"
            f"Status: {result.get('current_status', 'n/a')}"
        )
        print(msg)
        _send_telegram(msg)
        sys.exit(1)

    proposal = get_proposal_by_id(proposal_id)
    msg = [
        "✅ Rebalance Approved",
        f"ID: {proposal_id}",
        f"Status: {result.get('status')}",
        f"Approved At: {result.get('approved_at')}",
    ]

    if proposal and proposal.get("proposal"):
        actions = proposal["proposal"].get("proposed_actions", []) or []
        if actions:
            msg.append("")
            msg.append("Pending Execution Actions")
            for a in actions:
                if a.get("action") == "buy":
                    msg.append(
                        f"• Buy {a.get('product_id')} for ${float(a.get('quote_usd', 0) or 0):,.2f}"
                    )
                elif a.get("action") == "trim":
                    msg.append(
                        f"• Trim {a.get('product_id')} by ${float(a.get('quote_usd', 0) or 0):,.2f}"
                    )

    final = "\n".join(msg)
    print(final)
    _send_telegram(final)


if __name__ == "__main__":
    main()
