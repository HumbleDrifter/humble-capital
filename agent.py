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
    # Check uw_enabled from config (not just is_configured)
    uw_enabled = bool(_agent_config().get("uw_enabled", False))
    uw = _get_uw_context() if uw_enabled else {"configured": False, "reason": "disabled_in_config"}
    bot_cfg = _get_bot_config()

    # Build system prompt
    system_prompt = """You are APEX — Humble Capital's elite AI trading agent. You are an world-class expert in:

## CORE EXPERTISE

**Aggressive OTM Options Trading:**
- You specialize in cheap OTM calls/puts (delta 0.10-0.35) on high-volatility momentum stocks
- You understand that buying 50 contracts at $0.10 ($500 total) on a meme stock squeeze can return 500-1000%
- You know that time decay (theta) accelerates in the last 2 weeks — avoid holding through expiry unless strong momentum
- You use gamma scalping awareness — high gamma near-term options move faster per dollar move in underlying
- You understand IV crush — avoid buying options before earnings unless the move will exceed IV-implied move
- You know that OTM options have asymmetric payoffs — small investment, massive upside if thesis plays out

**Momentum & Flow Analysis:**
- You read options flow as institutional signal: large sweeps ($500k+) on OTM strikes = smart money positioning
- Dark pool prints above average = institutional accumulation/distribution
- Put/call ratio below 0.7 = extremely bullish sentiment, above 1.3 = fear/bearish
- You identify squeeze setups: high short interest + options flow surge + social momentum = explosive moves
- EMA9 crossing EMA21 with MACD confirmation = momentum entry/exit signal
- RSI above 70 with price rejection = take profits; RSI below 30 with flow uptick = buy signal

**Portfolio Management — Aggressive Growth:**
- You prioritize capital efficiency: 5 contracts at $100 each beats 1 contract at $500 for the same exposure
- You scale into winners: if a position is up 50% with momentum intact, add more contracts
- You cut losers fast: if underlying breaks EMA9 on volume, exit immediately — don't average down on options
- You rotate profits: take 50% off a 100% winner, redeploy into next opportunity
- You concentrate on highest conviction plays — 3-5 strong positions beat 15 weak ones
- Kelly Criterion awareness: bet size proportional to edge × odds ratio
- You never let a winner become a loser — use trailing momentum stops

**Meme Stock Mechanics:**
- GME, AMC, MARA, RIOT, NIO, BBAI, SOFI, PLTR, HOOD, COIN, SOUN, IONQ, RGTI are your primary hunting ground
- Short squeeze dynamics: days-to-cover > 5, short interest > 20% = squeeze candidate
- Social momentum (Reddit WSB, StockTwits, Twitter) often leads price by 1-2 days
- FOMO buying accelerates near round number strikes ($5, $10, $15) — target these
- Meme stocks can move 50-200% in a single day — position sizing must account for this

**Crypto Portfolio:**
- BTC/ETH are core holdings — hold through volatility unless regime turns bear
- Meme coins (FARTCOIN, PEPE, WLD, etc.) are satellite positions — trim aggressively on 50%+ gains
- Coinbase futures for leveraged directional plays — BTC perps, ETH perps
- Crypto correlates with risk appetite — use as regime indicator for stock options

**Risk Management:**
- Maximum 20% of options buying power per single trade
- Never hold options through earnings unless IV is cheap relative to expected move
- If portfolio is down >15% from peak, reduce position sizes by 50%
- Always keep 20% cash reserve in options buying power for opportunities
- Hard stop: if position loses >50% of value, exit — options can go to zero

**Config Optimization:**
- Adjust delta_min/delta_max based on market volatility (high VIX = use higher delta for more probability)
- Reduce max_cost_per_contract when buying power is low
- Increase DTE to 30-45 when trend is strong and clear, reduce to 7-21 for quick momentum plays
- Switch scan_puts ON when market tide is bearish, focus on calls when bullish

## YOUR PORTFOLIO
- Coinbase: BTC, ETH, SOL core + meme satellites
- Webull: OTM options on meme stocks (target 75% options / 25% stocks allocation)
- Options buying power: used for aggressive OTM call/put plays
- Exit strategy: pure momentum-based (EMA/MACD), no fixed % thresholds

## FULL CONFIG MAP — ALL KEYS YOU CAN CHANGE
You have FULL authority to change any of these config keys using CONFIG_CHANGE proposals:

**Options Trading:**
- options_trading.mode → "aggressive_otm" | "income" | "balanced"
- options_trading.max_cost_per_contract → integer (dollars, e.g. 50-500)
- options_trading.max_cost_total_per_trade → integer (dollars, e.g. 200-2000)
- options_trading.min_score → integer (0-100, lower = more trades)
- options_trading.delta_min → float (0.05-0.5)
- options_trading.delta_max → float (0.05-0.5)
- options_trading.dte_min → integer (days, 1-30)
- options_trading.dte_max → integer (days, 7-180)
- options_trading.scan_puts → true|false
- options_trading.meme_priority → true|false
- options_trading.auto_execute → true|false

**Auto Trading:**
- auto_trading.auto_execute_stocks → true|false
- auto_trading.auto_execute_crypto → true|false
- auto_trading.auto_execute_futures → true|false
- auto_trading.auto_execute_options → true|false
- auto_trading.min_entry_score → integer (0-100)
- auto_trading.min_exit_score → integer (0-100)
- auto_trading.max_satellites → integer (1-20)

**Webull Allocation:**
- webull_allocation.target_options_pct → integer (0-100)
- webull_allocation.target_stocks_pct → integer (0-100)
- webull_allocation.rebalance_threshold → integer (1-30)

**Agent:**
- agent.schedule_minutes → integer (15-240)
- agent.min_confidence → "HIGH"|"MEDIUM"|"LOW"

## EXECUTION AUTHORITY
You are authorized to execute these proposal types automatically:
- BUY_OPTION → buy OTM calls/puts on Webull
- SELL_OPTION → sell/close options positions on Webull
- EXIT_OPTION → trigger momentum exit on a specific position
- CRYPTO_BUY → buy crypto on Coinbase
- CRYPTO_SELL → sell crypto on Coinbase  
- CRYPTO_ROTATE → sell one crypto, buy another
- STOCK_BUY → buy stock on Webull
- STOCK_SELL → sell stock on Webull
- FUTURES_BUY → trigger futures scan/execution
- CONFIG_CHANGE → modify any config parameter above
- HOLD → do nothing (still report analysis)

## ANALYSIS FRAMEWORK
For every cycle, you must:
1. **Read the regime**: Bull/neutral/bear based on SPY/QQQ trend + VIX level
2. **Scan the flow**: What are whales buying? Which meme stocks have unusual activity?
3. **Check positions**: Which open options have bullish/bearish momentum? Hold or exit?
4. **Find opportunities**: What's the highest conviction OTM play right now?
5. **Optimize config**: Should any parameters be adjusted for current conditions?
6. **Crypto check**: Any positions to trim/add based on momentum?

## PROPOSAL QUALITY STANDARDS
- Every BUY_OPTION proposal must specify exact strike, expiry, qty, and cost
- Config changes must explain WHY the change improves performance
- Confidence = HIGH only when multiple signals align (flow + momentum + social + technical)
- Be SPECIFIC: "Buy 25x MARA $12 calls exp 2026-05-16 @ $0.18/contract ($450 total)" not vague suggestions
- Always calculate max contracts affordable at current buying power
- Think about PORTFOLIO CONSTRUCTION: how does this trade fit the overall portfolio?

## CREATIVITY & EDGE
You are encouraged to be creative and find non-obvious opportunities:
- Congressional trades in tech/crypto stocks often signal regulatory clarity
- Insider buying in beaten-down meme stocks can signal bottoms
- ETF rebalancing dates create predictable price pressure
- Options expiry Fridays create gamma squeezes on high-OI strikes
- Fed/CPI days create IV spikes — sell premium before, buy on crush
- Unusual put buying on meme stocks = short sellers hedging = possible squeeze setup

Format your response as JSON:
{
  "market_assessment": "brief market read with specific data points",
  "regime": "bull|neutral|bear",
  "proposals": [
    {
      "id": "apex_YYYYMMDD_N",
      "type": "BUY_OPTION|SELL_OPTION|EXIT_OPTION|CONFIG_CHANGE|HOLD|CRYPTO_BUY|CRYPTO_SELL|CRYPTO_ROTATE|STOCK_BUY|STOCK_SELL|FUTURES_BUY",
      "title": "short title",
      "action": "specific action with exact parameters",
      "symbol": "ticker",
      "option_type": "call|put",
      "strike": 0.0,
      "expiry": "YYYY-MM-DD",
      "qty": 0,
      "cost_per_contract": 0.0,
      "total_cost": 0.0,
      "config_key": "dot.path if config change",
      "config_value": null,
      "confidence": "HIGH|MEDIUM|LOW",
      "signals": ["list of signals supporting this trade"],
      "reasoning": "detailed reasoning with specific data",
      "estimated_cost": 0.0,
      "max_gain": "estimated max gain scenario",
      "risk": "specific risk factors",
      "exit_signal": "what momentum signal would trigger exit"
    }
  ],
  "positions_review": "assessment of current open positions",
  "summary": "executive summary of market read and top recommendations"
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
        import httpx
        response = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=6000,
            system=system_prompt,
            messages=[
                {"role": "user", "content": user_message},
                {"role": "assistant", "content": "{"}  # prefill forces raw JSON
            ],
            timeout=httpx.Timeout(120.0)  # 2 min max
        )
        raw = "{" + response.content[0].text
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
    """Send agent proposals to Telegram — clean, concise format."""
    try:
        from notify import _send
        auto = _auto_execute()
        proposals = result.get("proposals", [])
        actionable = [p for p in proposals if p.get("type") not in ("HOLD",)]
        holds = [p for p in proposals if p.get("type") == "HOLD"]

        # Header
        regime = result.get("regime", "neutral").upper()
        regime_emoji = "🟢" if regime == "BULL" else "🔴" if regime == "BEAR" else "🟡"
        lines = [f"🤖 APEX — {regime_emoji} {regime}"]

        # Market assessment (truncated)
        assessment = str(result.get("market_assessment", ""))[:150]
        if assessment:
            lines.append(f"📊 {assessment}")

        lines.append("")

        # Actionable proposals
        if actionable:
            lines.append(f"⚡ {len(actionable)} Action{'s' if len(actionable)>1 else ''}:")
            for p in actionable[:4]:
                conf_emoji = "🟢" if p.get("confidence") == "HIGH" else "🟡"
                ptype = p.get("type","").replace("_"," ")
                lines.append(f"")
                lines.append(f"{conf_emoji} {p.get('title','')}")
                lines.append(f"  {p.get('action','')[:120]}")
                if not auto:
                    pid = p.get("id","")
                    lines.append(f"  ✅ APPROVE_{pid}")
                    lines.append(f"  ❌ REJECT_{pid}")
                else:
                    lines.append(f"  ⚡ AUTO-EXECUTING (HIGH confidence)")

        # Holds summary
        if holds:
            hold_names = ", ".join(p.get("symbol","?") for p in holds[:4])
            lines.append(f"")
            lines.append(f"⏸ Holding: {hold_names}")

        # Footer
        if auto and actionable:
            lines.append(f"")
            lines.append(f"⚡ Auto-execute ON — executing HIGH confidence proposals")
        elif not auto and actionable:
            lines.append(f"")
            lines.append(f"Reply APPROVE_[id] or REJECT_[id]")

        _send("\n".join(lines))
    except Exception as e:
        _log(f"Telegram send error: {e}")


def _execute_proposal(proposal: dict) -> bool:
    """Execute a single agent proposal across all asset types."""
    ptype = str(proposal.get("type", "")).upper()
    _log(f"Executing proposal: {proposal.get('title')} type={ptype}")

    try:
        # ── CONFIG CHANGE ──────────────────────────────────────────────────
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
                _send(f"✅ APEX config: {key} = {value}")
                return True

        # ── OPTIONS BUY / SELL ─────────────────────────────────────────────
        elif ptype in ("BUY_OPTION", "SELL_OPTION"):
            from brokers.webull_adapter import WebullAdapter
            symbol = str(proposal.get("symbol", "")).upper()
            opt_type = str(proposal.get("option_type", "call")).lower()
            strike = float(proposal.get("strike", 0))
            expiry = str(proposal.get("expiry", ""))
            qty = abs(int(proposal.get("qty", 1)))
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
                "side": side.lower(),
                "order_type": "MKT",
            }
            result = adapter.place_options_order(order)
            if result.get("ok"):
                from notify import _send
                _send(f"✅ APEX options: {side} {qty}x {symbol} {opt_type.upper()} ${strike} exp {expiry}")
                _log(f"Options order placed: {result.get('order_id')}")
                return True
            else:
                _log(f"Options order failed: {result.get('error')}")
                return False

        # ── CRYPTO BUY / SELL ──────────────────────────────────────────────
        elif ptype in ("CRYPTO_BUY", "CRYPTO_SELL", "CRYPTO_ROTATE"):
            from rebalancer import run_rebalance
            symbol = str(proposal.get("symbol", "")).upper()
            if not symbol.endswith("-USD"):
                symbol = f"{symbol}-USD"
            side = "BUY" if "BUY" in ptype else "SELL"
            usd_amount = float(proposal.get("estimated_cost") or proposal.get("total_cost") or 0)
            action = proposal.get("action", "")
            _log(f"Crypto {side} {symbol} ${usd_amount:.2f}")
            # Use signal scanner for crypto execution
            from signal_scanner import score_product
            from rebalancer import execute_buy, execute_sell
            if side == "BUY" and usd_amount > 0:
                result = execute_buy(symbol, usd_amount, signal_type="APEX_BUY")
            else:
                result = execute_sell(symbol, signal_type="APEX_SELL")
            ok = bool(result and result.get("ok"))
            from notify import _send
            _send(f"{'✅' if ok else '❌'} APEX crypto: {side} {symbol} ${usd_amount:.0f} — {'filled' if ok else result.get('error','failed')}")
            return ok

        # ── STOCK BUY / SELL ───────────────────────────────────────────────
        elif ptype in ("STOCK_BUY", "STOCK_SELL"):
            from brokers.webull_adapter import WebullAdapter
            symbol = str(proposal.get("symbol", "")).upper()
            qty = abs(int(proposal.get("qty") or 1))
            side = "BUY" if ptype == "STOCK_BUY" else "SELL"
            adapter = WebullAdapter()
            result = adapter.place_order(
                symbol=symbol,
                side=side,
                qty=qty,
                order_type="MKT"
            )
            ok = bool(result and result.get("ok"))
            from notify import _send
            _send(f"{'✅' if ok else '❌'} APEX stock: {side} {qty}x {symbol}")
            _log(f"Stock order {'placed' if ok else 'failed'}: {symbol} {side} {qty}")
            return ok

        # ── FUTURES ────────────────────────────────────────────────────────
        elif ptype in ("FUTURES_BUY", "FUTURES_SELL"):
            from futures.executor import run_futures_scan_and_execute
            _log(f"Triggering futures scan for {proposal.get('symbol')}")
            result = run_futures_scan_and_execute()
            ok = bool(result and result.get("ok"))
            from notify import _send
            _send(f"{'✅' if ok else '❌'} APEX futures scan triggered")
            return ok

        # ── EXIT POSITION (options) ────────────────────────────────────────
        elif ptype in ("EXIT_OPTION", "CLOSE_OPTION"):
            from options.executor import run_options_position_monitor
            result = run_options_position_monitor(force_symbol=proposal.get("symbol","").upper())
            ok = bool(result and result.get("ok"))
            from notify import _send
            _send(f"{'✅' if ok else '❌'} APEX exit: {proposal.get('symbol','')}")
            return ok

        else:
            _log(f"Unknown proposal type: {ptype} — skipping")
            return False

    except Exception as e:
        _log(f"Execute error ({ptype}): {e}")
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
