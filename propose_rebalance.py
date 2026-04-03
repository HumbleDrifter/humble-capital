from dotenv import load_dotenv

from daily_bot_report import _send_telegram
from services.rebalance_proposal_service import (
    build_rebalance_proposal,
    render_rebalance_proposal_text,
    save_proposal,
)

load_dotenv("/root/tradingbot/.env", override=True)


def main():
    proposal = build_rebalance_proposal()
    preview_text = render_rebalance_proposal_text(proposal)
    proposal_id = save_proposal(proposal, preview_text)

    proposal["proposal_id"] = proposal_id
    final_text = render_rebalance_proposal_text(proposal)

    print(final_text)
    _send_telegram(final_text)


if __name__ == "__main__":
    main()
