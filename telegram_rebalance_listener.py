import json
import os
import re
import time
from pathlib import Path

from execute_rebalance import main as execute_rebalance_main
from execute_rebalance import execute_proposal_by_id
import requests
from env_runtime import load_runtime_env, preferred_env_path
from daily_bot_report import _send_telegram
from notify import render_config_proposal_status_text
from services.config_proposal_service import approve_config_proposal, reject_config_proposal
from services.rebalance_proposal_service import approve_proposal, reject_proposal, get_proposal_by_id
from storage import get_config_proposal_by_id

load_runtime_env(override=True)

STATE_PATH = preferred_env_path().resolve().parent / "telegram_listener_state.json"
POLL_TIMEOUT_SEC = 30
SLEEP_SEC = 2


def _telegram_token() -> str:
    load_runtime_env(override=True)
    return str(os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()


def _telegram_chat_id() -> str:
    load_runtime_env(override=True)
    return str(os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()


def _api_url(method: str) -> str:
    token = _telegram_token()
    return f"https://api.telegram.org/bot{token}/{method}"


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {"last_update_id": 0}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"last_update_id": 0}


def _save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _get_updates(offset: int) -> list:
    token = _telegram_token()
    chat_id = _telegram_chat_id()

    if not token or not chat_id:
        print("[telegram_listener] missing telegram config")
        return []

    try:
        r = requests.get(
            _api_url("getUpdates"),
            params={
                "offset": offset,
                "timeout": POLL_TIMEOUT_SEC,
            },
            timeout=POLL_TIMEOUT_SEC + 10,
        )
        r.raise_for_status()
        data = r.json()
        if not data.get("ok"):
            return []
        return data.get("result", []) or []
    except Exception as exc:
        print(f"[telegram_listener] getUpdates error: {exc}")
        return []


def _normalize_text(text: str) -> str:
    return str(text or "").strip()


def _extract_command(text: str):
    text = _normalize_text(text)
    m = re.match(r"^(APPROVE|REJECT|EXECUTE)\s+((?:RB|CFG)-\d{8}-\d{3})$", text, re.IGNORECASE)
    if not m:
        return None, None
    return m.group(1).upper(), m.group(2).upper()


def _chat_matches(update: dict) -> bool:
    expected_chat_id = _telegram_chat_id()
    try:
        msg = update.get("message") or {}
        chat = msg.get("chat") or {}
        actual = str(chat.get("id"))
        return actual == str(expected_chat_id)
    except Exception:
        return False


def _render_approval_result(proposal_id: str, result: dict) -> str:
    if not result.get("ok"):
        return (
            f"❌ Rebalance command failed\n"
            f"ID: {proposal_id}\n"
            f"Reason: {result.get('reason')}\n"
            f"Status: {result.get('current_status', 'n/a')}"
        )

    proposal = get_proposal_by_id(proposal_id)
    lines = []

    if result.get("status") == "approved":
        lines.extend([
            "✅ Rebalance Approved",
            f"ID: {proposal_id}",
            f"Approved At: {result.get('approved_at')}",
        ])
    elif result.get("status") == "rejected":
        lines.extend([
            "🛑 Rebalance Rejected",
            f"ID: {proposal_id}",
            f"Rejected At: {result.get('rejected_at')}",
        ])

    if proposal and proposal.get("proposal"):
        actions = proposal["proposal"].get("proposed_actions", []) or []
        if actions and result.get("status") == "approved":
            lines.append("")
            lines.append("Pending Execution Actions")
            for a in actions:
                if a.get("action") == "buy":
                    lines.append(
                        f"• Buy {a.get('product_id')} for ${float(a.get('quote_usd', 0) or 0):,.2f}"
                    )
                elif a.get("action") == "trim":
                    lines.append(
                        f"• Trim {a.get('product_id')} by ${float(a.get('quote_usd', 0) or 0):,.2f}"
                    )

    return "\n".join(lines).strip()


def _telegram_actor(update: dict) -> str:
    try:
        msg = update.get("message") or {}
        chat = msg.get("chat") or {}
        user = msg.get("from") or {}
        chat_id = str(chat.get("id") or "").strip()
        username = str(user.get("username") or "").strip()
        if username:
            return f"telegram:{chat_id}:{username}"
        return f"telegram:{chat_id}"
    except Exception:
        return "telegram:unknown"


def _handle_update(update: dict) -> None:
    if not _chat_matches(update):
        return

    msg = update.get("message") or {}
    text = _normalize_text(msg.get("text", ""))

    if not text:
        return

    command, proposal_id = _extract_command(text)
    if not command or not proposal_id:
        return

    if proposal_id.startswith("CFG-"):
        if command not in {"APPROVE", "REJECT"}:
            return

        actor = _telegram_actor(update)

        if command == "APPROVE":
            result = approve_config_proposal(proposal_id, actor=actor)
        else:
            result = reject_config_proposal(proposal_id, actor=actor)

        proposal = get_config_proposal_by_id(proposal_id)
        text_out = render_config_proposal_status_text(proposal_id, result, proposal)
        print(text_out)
        _send_telegram(text_out)
        return

    if command == "APPROVE":
        result = approve_proposal(proposal_id)
        text_out = _render_approval_result(proposal_id, result)
        print(text_out)
        _send_telegram(text_out)
        return

    if command == "REJECT":
        result = reject_proposal(proposal_id)
        text_out = _render_approval_result(proposal_id, result)
        print(text_out)
        _send_telegram(text_out)
        return

    if command == "EXECUTE":
        text_out = execute_proposal_by_id(proposal_id)
        print(text_out)
        return


def main():
    state = _load_state()
    last_update_id = int(state.get("last_update_id", 0) or 0)

    print("[telegram_listener] started")

    while True:
        updates = _get_updates(last_update_id + 1)

        for update in updates:
            try:
                update_id = int(update.get("update_id", 0) or 0)
                if update_id > last_update_id:
                    last_update_id = update_id

                _handle_update(update)
            except Exception as exc:
                print(f"[telegram_listener] update handling error: {exc}")

        state["last_update_id"] = last_update_id
        _save_state(state)
        time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    main()
