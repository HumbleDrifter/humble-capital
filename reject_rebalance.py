import sys
from dotenv import load_dotenv

from daily_bot_report import _send_telegram
from services.rebalance_proposal_service import reject_proposal

load_dotenv("/root/tradingbot/.env", override=True)


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 reject_rebalance.py RB-YYYYMMDD-001")
        sys.exit(1)

    proposal_id = sys.argv[1].strip()
    result = reject_proposal(proposal_id)

    if not result.get("ok"):
        msg = (
            f"❌ Rebalance rejection failed\n"
            f"ID: {proposal_id}\n"
            f"Reason: {result.get('reason')}\n"
            f"Status: {result.get('current_status', 'n/a')}"
        )
        print(msg)
        _send_telegram(msg)
        sys.exit(1)

    msg = (
        f"🛑 Rebalance Rejected\n"
        f"ID: {proposal_id}\n"
        f"Status: {result.get('status')}\n"
        f"Rejected At: {result.get('rejected_at')}"
    )
    print(msg)
    _send_telegram(msg)


if __name__ == "__main__":
    main()
