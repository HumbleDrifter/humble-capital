"""
Telegram bot polling loop — handles incoming APPROVE/REJECT commands
and routes them to the agent or config proposal system.
"""
import os
import time
import requests
import threading

_LAST_UPDATE_ID = 0
_POLL_LOCK = threading.Lock()


def _token():
    from env_runtime import load_runtime_env
    load_runtime_env(override=True)
    return str(os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()


def _chat():
    from env_runtime import load_runtime_env
    load_runtime_env(override=True)
    return str(os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()


def _send(text: str) -> bool:
    try:
        token = _token()
        chat = _chat()
        if not token or not chat:
            return False
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        return True
    except Exception:
        return False


def _get_updates(offset: int = 0) -> list:
    try:
        token = _token()
        if not token:
            return []
        resp = requests.get(
            f"https://api.telegram.org/bot{token}/getUpdates",
            params={"offset": offset, "timeout": 10, "limit": 10},
            timeout=15,
        )
        if resp.ok:
            return resp.json().get("result", [])
    except Exception:
        pass
    return []


def _handle_message(text: str) -> None:
    """Route incoming Telegram messages to appropriate handler."""
    text = text.strip()
    upper = text.upper()

    # Agent proposal approval
    if upper.startswith("APPROVE_") or upper.startswith("APPROVE "):
        proposal_id = text.split("_", 1)[-1].split(" ", 1)[-1].strip()
        try:
            from agent import approve_proposal
            result = approve_proposal(proposal_id)
            if result.get("ok"):
                _send(f"✅ Proposal <b>{proposal_id}</b> approved and executed!")
            else:
                _send(f"❌ Could not execute proposal {proposal_id}: {result.get('error')}")
        except Exception as e:
            _send(f"❌ Approval error: {e}")

    elif upper.startswith("REJECT_") or upper.startswith("REJECT "):
        proposal_id = text.split("_", 1)[-1].split(" ", 1)[-1].strip()
        try:
            from agent import reject_proposal
            reject_proposal(proposal_id)
            _send(f"🚫 Proposal <b>{proposal_id}</b> rejected")
        except Exception as e:
            _send(f"❌ Reject error: {e}")

    # Config proposal commands (existing system)
    elif upper.startswith("APPROVE CFG-") or upper.startswith("APPROVE CFG"):
        proposal_id = text.split(" ", 1)[-1].strip()
        try:
            from services.config_proposal_service import approve_config_proposal
            result = approve_config_proposal(proposal_id, actor="telegram")
            if result.get("ok"):
                _send(f"✅ Config proposal <b>{proposal_id}</b> approved!")
            else:
                _send(f"❌ Failed: {result.get('reason')}")
        except Exception as e:
            _send(f"❌ Error: {e}")

    elif upper.startswith("REJECT CFG-") or upper.startswith("REJECT CFG"):
        proposal_id = text.split(" ", 1)[-1].strip()
        try:
            from services.config_proposal_service import reject_config_proposal
            result = reject_config_proposal(proposal_id, actor="telegram")
            _send(f"🚫 Config proposal rejected")
        except Exception as e:
            _send(f"❌ Error: {e}")

    # Quick commands
    elif upper == "/STATUS" or upper == "STATUS":
        try:
            from portfolio import get_portfolio_snapshot
            snap = get_portfolio_snapshot()
            total = snap.get("total_value_usd", 0)
            pnl = snap.get("day_pnl_usd", 0)
            regime = snap.get("regime", "neutral")
            pnl_sign = "+" if pnl >= 0 else ""
            _send(
                f"📊 <b>Portfolio Status</b>\n"
                f"Total: ${total:,.2f}\n"
                f"Day P&L: {pnl_sign}${pnl:,.2f}\n"
                f"Regime: {regime}"
            )
        except Exception as e:
            _send(f"❌ Status error: {e}")

    elif upper == "/AGENT" or upper == "AGENT":
        try:
            from agent import _is_enabled
            if not _is_enabled():
                _send("⚠️ Agent is disabled. Enable in Settings → Agent.")
            else:
                _send("🤖 Running agent analysis cycle...")
                # Signal agent_runner via flag file instead of running inline
                import os
                with open("/tmp/apex_run_now.flag", "w") as f:
                    f.write("1")
        except Exception as e:
            _send(f"❌ Agent error: {e}")

    elif upper == "/PENDING" or upper == "PENDING":
        try:
            from agent import get_pending_proposals
            proposals = get_pending_proposals()
            if not proposals:
                _send("✅ No pending proposals")
            else:
                lines = [f"📋 <b>{len(proposals)} Pending Proposals:</b>"]
                for p in proposals:
                    lines.append(f"\n• <b>{p.get('title')}</b>")
                    lines.append(f"  ID: {p.get('id')}")
                    lines.append(f"  APPROVE_{p.get('id')} | REJECT_{p.get('id')}")
                _send("\n".join(lines))
        except Exception as e:
            _send(f"❌ Error: {e}")

    elif upper == "/HELP" or upper == "HELP":
        _send(
            "🤖 <b>Humble Capital Bot Commands</b>\n\n"
            "/status — Portfolio snapshot\n"
            "/agent — Run agent analysis now\n"
            "/pending — List pending proposals\n"
            "APPROVE_[id] — Approve a proposal\n"
            "REJECT_[id] — Reject a proposal\n"
            "APPROVE CFG-[id] — Approve config proposal\n"
            "REJECT CFG-[id] — Reject config proposal\n"
        )


def run_polling_loop() -> None:
    """Poll Telegram for incoming messages and route them."""
    global _LAST_UPDATE_ID
    print("[telegram_bot] polling loop started", flush=True)
    # Send startup message
    time.sleep(5)
    try:
        _send("🟢 <b>Humble Capital Bot online</b>\nSend /help for commands")
    except Exception:
        pass

    while True:
        try:
            updates = _get_updates(offset=_LAST_UPDATE_ID + 1)
            for update in updates:
                update_id = update.get("update_id", 0)
                if update_id > _LAST_UPDATE_ID:
                    _LAST_UPDATE_ID = update_id
                msg = update.get("message", {})
                text = str(msg.get("text") or "").strip()
                # Only process messages from our chat
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if text and chat_id == _chat():
                    print(f"[telegram_bot] received: {text}", flush=True)
                    _handle_message(text)
        except Exception as e:
            print(f"[telegram_bot] poll error: {e}", flush=True)
        time.sleep(3)
