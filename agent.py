"""
Humble Capital AI Agent — Claude Sonnet powered portfolio manager.
Analyzes portfolio, UW flow, and proposes actions via Telegram.
Supports auto-execute mode or Telegram approval flow.
"""
import os
import json
import time
import threading
from typing import Any

_AGENT_LOG: list = []
_AGENT_LOCK = threading.Lock()
_PENDING_PROPOSALS: dict = {}  # proposal_id -> proposal dict


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[agent] {msg}", flush=True)
    with _AGENT_LOCK:
        _AGENT_LOG.append({"ts": ts, "msg": msg})
        if len(_AGENT_LOG) > 200:
            _AGENT_LOG.pop(0)


def get_agent_log(limit: int = 50) -> list:
    with _AGENT_LOCK:
        return list(reversed(_AGENT_LOG[-int(limit):]))


def _anthropic_key():
    from env_runtime import load_runtime_env
    load_runtime_env(override=True)
    return str(os.getenv("ANTHROPIC_API_KEY", "") or "").strip()


def _agent_config() -> dict:
    try:
        with open("/root/tradingbot/asset_config.json") as f:
            cfg = json.load(f)
        return cfg.get("agent", {})
    except Exception:
        return {}


def _is_enabled() -> bool:
    return bool(_agent_config().get("enabled", False))


def _auto_execute() -> bool:
    return bool(_agent_config().get("auto_execute", False))


def _get_portfolio_context() -> dict:
    """Gather full portfolio state for agent context."""
    ctx = {}
    try:
        from portfolio import get_portfolio_snapshot
        snap = get_portfolio_snapshot()
        ctx["total_value"] = snap.get("total_value_usd", 0)
        ctx["day_pnl"] = snap.get("day_pnl_usd", 0)
        ctx["regime"] = snap.get("regime", "neutral")
        ctx["positions"] = {
            pid: {
                "value": p.get("value_total_usd", 0),
                "pnl_pct": p.get("unrealized_pnl_pct", 0),
                "class": p.get("class", ""),
            }
            for pid, p in (snap.get("positions") or {}).items()
        }
    except Exception as e:
        ctx["error"] = str(e)

    try:
        from brokers.webull_adapter import WebullAdapter
        info = WebullAdapter().get_account_info()
        opts = [p for p in (info.get("positions") or [])
                if str(p.get("asset_type", "")).lower() == "option"]
        ctx["options_positions"] = [
            {
                "symbol": p.get("symbol"),
                "option_type": p.get("option_type"),
                "strike": p.get("strike"),
                "expiration": p.get("expiration"),
                "qty": p.get("qty"),
                "pnl": p.get("unrealized_pnl"),
                "pnl_pct": p.get("unrealized_pnl_pct"),
                "market_value": p.get("market_value"),
            }
            for p in opts
        ]
        raw = info.get("raw", {}).get("account_currency_assets", [{}])[0]
        ctx["options_buying_power"] = float(raw.get("option_buying_power", 0))
        ctx["webull_balance"] = float(raw.get("net_liquidation_value", 0))
    except Exception as e:
        ctx["options_error"] = str(e)

    return ctx


def _get_uw_context() -> dict:
    """Gather Unusual Whales data for agent context."""
    from unusual_whales import (
        is_configured, get_flow_alerts, get_darkpool_recent,
        get_market_tide, get_meme_flow, get_oi_change
    )
    if not is_configured():
        return {"configured": False}

    ctx = {"configured": True}
    try:
        ctx["flow_alerts"] = get_flow_alerts(limit=30)
    except Exception:
        ctx["flow_alerts"] = []
    try:
        ctx["meme_flow"] = get_meme_flow()
    except Exception:
        ctx["meme_flow"] = []
    try:
        ctx["darkpool"] = get_darkpool_recent(limit=10)
    except Exception:
        ctx["darkpool"] = []
    try:
        ctx["market_tide"] = get_market_tide()
    except Exception:
        ctx["market_tide"] = {}
    try:
        ctx["oi_change"] = get_oi_change(limit=10)
    except Exception:
        ctx["oi_change"] = []
    return ctx


def _get_bot_config() -> dict:
    try:
        with open("/root/tradingbot/asset_config.json") as f:
            cfg = json.load(f)
        return {
            "options_trading": cfg.get("options_trading", {}),
            "auto_trading": cfg.get("auto_trading", {}),
            "webull_allocation": cfg.get("webull_allocation", {}),
        }
    except Exception:
        return {}


def run_agent_cycle() -> dict:
    """
    Main agent cycle — analyze portfolio + market data,
    generate proposals, send via Telegram.
    """
    api_key = _anthropic_key()
    if not api_key:
        _log("ANTHROPIC_API_KEY not set — skipping cycle")
        return {"ok": False, "reason": "no_api_key"}

    _log("Starting agent analysis cycle...")

    portfolio = _get_portfolio_context()
    uw = _get_uw_context()
    bot_cfg = _get_bot_config()

    # Build system prompt
    system_prompt = """You are Humble Capital's AI trading agent — an expert in aggressive options trading, 
momentum strategies, and options flow analysis. You manage a portfolio with:
- Coinbase crypto positions (BTC, ETH, SOL, meme coins)
- Webull options positions (cheap OTM calls/puts on meme stocks)
- Target: 75% options / 25% stocks on Webull
- Strategy: Aggressive OTM calls/puts, delta 0.10-0.35, under $200/contract
- Exit: Pure momentum-based (EMA/MACD signals), not fixed % targets

Your job is to analyze the current portfolio state, market conditions, and options flow data,
then propose specific actionable changes. Be aggressive and creative in finding opportunities.

For each proposal, provide:
1. A clear action (BUY/SELL/CONFIG_CHANGE)
2. Specific parameters (symbol, strike, expiry, qty OR config key/value)
3. Confidence level (HIGH/MEDIUM/LOW)
4. Reasoning based on data

Format your response as JSON with this structure:
{
  "market_assessment": "brief market read",
  "proposals": [
    {
      "id": "unique_id",
      "type": "BUY_OPTION|SELL_OPTION|CONFIG_CHANGE|HOLD",
      "title": "short title",
      "action": "specific action description",
      "symbol": "ticker if applicable",
      "option_type": "call|put if applicable",
      "strike": 0.0,
      "expiry": "YYYY-MM-DD",
      "qty": 0,
      "config_key": "dot.path if config change",
      "config_value": null,
      "confidence": "HIGH|MEDIUM|LOW",
      "reasoning": "detailed reasoning",
      "estimated_cost": 0.0,
      "risk": "brief risk assessment"
    }
  ],
  "summary": "one paragraph summary of what you recommend and why"
}"""

    user_message = f"""Analyze this portfolio and market data, then propose actions:

PORTFOLIO STATE:
{json.dumps(portfolio, indent=2, default=str)}

BOT CONFIGURATION:
{json.dumps(bot_cfg, indent=2)}

UNUSUAL WHALES DATA:
{json.dumps(uw, indent=2, default=str)}

Focus on:
1. Any unusual flow on meme stocks in our watchlist (MARA, RIOT, NIO, AMC, BBAI, GME, SOFI, PLTR)
2. Dark pool prints suggesting institutional positioning
3. Whether current options positions should be held or exited based on flow
4. New OTM call/put opportunities under $200/contract
5. Config adjustments to maximize the current market regime
6. Crypto positions that should be rotated

Be specific, aggressive, and data-driven. If UW data is not configured, work with portfolio data only."""

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=2000,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}]
        )
        raw = response.content[0].text
        # Parse JSON response
        try:
            # Extract JSON if wrapped in markdown
            if "```json" in raw:
                raw = raw.split("```json")[1].split("```")[0].strip()
            elif "```" in raw:
                raw = raw.split("```")[1].split("```")[0].strip()
            result = json.loads(raw)
        except Exception:
            result = {"summary": raw, "proposals": [], "market_assessment": "Parse error"}

        proposals = result.get("proposals", [])
        _log(f"Agent generated {len(proposals)} proposals")

        # Send to Telegram
        _send_proposals_to_telegram(result)

        # Auto-execute if enabled
        if _auto_execute():
            for proposal in proposals:
                if proposal.get("confidence") == "HIGH":
                    _execute_proposal(proposal)
        else:
            # Store pending for manual approval
            with _AGENT_LOCK:
                for p in proposals:
                    pid = p.get("id", f"agent_{int(time.time())}")
                    _PENDING_PROPOSALS[pid] = p

        return {"ok": True, "proposals": len(proposals), "summary": result.get("summary", "")}

    except Exception as e:
        _log(f"Agent cycle error: {e}")
        return {"ok": False, "error": str(e)}


def _send_proposals_to_telegram(result: dict) -> None:
    """Send agent proposals to Telegram with approve/reject options."""
    try:
        from notify import _send
        auto = _auto_execute()
        proposals = result.get("proposals", [])
        actionable = [p for p in proposals if p.get("type") != "HOLD"]

        lines = [
            f"🤖 Agent Analysis",
            f"",
            f"📊 {result.get('market_assessment', '')}",
            f"",
        ]

        if actionable:
            lines.append(f"📋 {len(actionable)} Proposals:")
            for p in actionable[:5]:
                conf_emoji = "🟢" if p.get("confidence") == "HIGH" else "🟡" if p.get("confidence") == "MEDIUM" else "🔴"
                lines.append(f"")
                lines.append(f"{conf_emoji} {p.get('title', p.get('type', ''))}")
                lines.append(f"  {p.get('action', '')}")
                if p.get("estimated_cost"):
                    lines.append(f"  Cost: ${p.get('estimated_cost', 0):.0f}")
                lines.append(f"  {p.get('reasoning', '')[:100]}")
                if not auto:
                    lines.append(f"  APPROVE_{p.get('id', '')} | REJECT_{p.get('id', '')}")
        else:
            lines.append("✅ No actionable proposals — current setup looks optimal")

        if auto:
            lines.append(f"")
            lines.append(f"⚡ Auto-execute ON — HIGH confidence proposals will execute automatically")

        lines.append(f"")
        lines.append(f"💬 {result.get('summary', '')[:200]}")

        _send("\n".join(lines))
    except Exception as e:
        _log(f"Telegram send error: {e}")


def _execute_proposal(proposal: dict) -> bool:
    """Execute a single agent proposal."""
    ptype = str(proposal.get("type", "")).upper()
    _log(f"Executing proposal: {proposal.get('title')} type={ptype}")

    try:
        if ptype == "CONFIG_CHANGE":
            key = proposal.get("config_key")
            value = proposal.get("config_value")
            if key and value is not None:
                with open("/root/tradingbot/asset_config.json") as f:
                    cfg = json.load(f)
                keys = key.split(".")
                node = cfg
                for k in keys[:-1]:
                    node = node.setdefault(k, {})
                node[keys[-1]] = value
                with open("/root/tradingbot/asset_config.json", "w") as f:
                    json.dump(cfg, f, indent=2)
                _log(f"Config updated: {key} = {value}")
                from notify import _send
                _send(f"✅ Agent applied config: {key} = {value}")
                return True

        elif ptype in ("BUY_OPTION", "SELL_OPTION"):
            from brokers.webull_adapter import WebullAdapter
            from options.screener import OptionsScreener
            symbol = str(proposal.get("symbol", "")).upper()
            opt_type = str(proposal.get("option_type", "call")).lower()
            strike = float(proposal.get("strike", 0))
            expiry = str(proposal.get("expiry", ""))
            qty = int(proposal.get("qty", 1))
            side = "BUY" if ptype == "BUY_OPTION" else "SELL"

            if not all([symbol, strike, expiry, qty]):
                _log(f"Missing order params: {proposal}")
                return False

            adapter = WebullAdapter()
            order = {
                "underlying": symbol,
                "option_type": opt_type,
                "strike": strike,
                "expiration": expiry,
                "qty": qty,
                "side": side,
                "order_type": "LMT",
            }
            result = adapter.place_options_order(order)
            if result.get("ok"):
                from notify import _send
                _send(f"✅ Agent executed: {side} {qty}x {symbol} {opt_type.upper()} ${strike} exp {expiry}")
                _log(f"Order placed: {result.get('order_id')}")
                return True
            else:
                _log(f"Order failed: {result.get('error')}")
                return False

    except Exception as e:
        _log(f"Execute error: {e}")
        return False

    return False


def approve_proposal(proposal_id: str) -> dict:
    """Approve and execute a pending proposal."""
    with _AGENT_LOCK:
        proposal = _PENDING_PROPOSALS.pop(proposal_id, None)
    if not proposal:
        return {"ok": False, "error": "proposal not found"}
    ok = _execute_proposal(proposal)
    return {"ok": ok, "proposal_id": proposal_id}


def reject_proposal(proposal_id: str) -> dict:
    """Reject a pending proposal."""
    with _AGENT_LOCK:
        _PENDING_PROPOSALS.pop(proposal_id, None)
    from notify import _send
    _send(f"🚫 Proposal {proposal_id} rejected")
    return {"ok": True, "proposal_id": proposal_id}


def get_pending_proposals() -> list:
    with _AGENT_LOCK:
        return list(_PENDING_PROPOSALS.values())
