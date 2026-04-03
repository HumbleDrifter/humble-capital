import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

load_dotenv("/root/tradingbot/.env", override=True)

BASE_DIR = Path("/root/tradingbot")
DB_PATH = str(os.getenv("TRADINGBOT_DB_PATH", "/root/tradingbot/trading.db") or "").strip() or "/root/tradingbot/trading.db"
ASSET_CONFIG_PATH = BASE_DIR / "asset_config.json"
MEME_ROTATION_PATH = BASE_DIR / "meme_rotation.json"
PORTFOLIO_SNAPSHOT_PATH = BASE_DIR / "advisor_export" / "portfolio_snapshot.json"


def _load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, str(default)) or str(default)).strip())
    except Exception:
        return default


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_day_token() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_rebalance_proposals_table() -> None:
    conn = _db()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rebalance_proposals (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                proposal_json TEXT NOT NULL,
                summary_text TEXT NOT NULL,
                approved_at TEXT,
                rejected_at TEXT,
                executed_at TEXT
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _next_proposal_id() -> str:
    ensure_rebalance_proposals_table()
    prefix = f"RB-{_utc_day_token()}-"
    conn = _db()
    try:
        rows = conn.execute(
            """
            SELECT id
            FROM rebalance_proposals
            WHERE id LIKE ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (f"{prefix}%",),
        ).fetchall()

        if not rows:
            return f"{prefix}001"

        last_id = str(rows[0]["id"])
        try:
            n = int(last_id.split("-")[-1])
        except Exception:
            n = 0
        return f"{prefix}{n + 1:03d}"
    finally:
        conn.close()


def save_proposal(proposal: Dict[str, Any], summary_text: str) -> str:
    ensure_rebalance_proposals_table()
    proposal_id = _next_proposal_id()
    created_at = _utc_now_iso()

    payload = dict(proposal)
    payload["proposal_id"] = proposal_id
    payload["created_at"] = created_at

    conn = _db()
    try:
        conn.execute(
            """
            INSERT INTO rebalance_proposals (
                id, created_at, status, proposal_json, summary_text,
                approved_at, rejected_at, executed_at
            )
            VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL)
            """,
            (
                proposal_id,
                created_at,
                "pending",
                json.dumps(payload, ensure_ascii=False),
                summary_text,
            ),
        )
        conn.commit()
        return proposal_id
    finally:
        conn.close()


def get_latest_pending_proposal() -> Dict[str, Any] | None:
    ensure_rebalance_proposals_table()
    conn = _db()
    try:
        row = conn.execute(
            """
            SELECT id, created_at, status, proposal_json, summary_text
            FROM rebalance_proposals
            WHERE status = 'pending'
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

        if not row:
            return None

        try:
            payload = json.loads(row["proposal_json"])
        except Exception:
            payload = {}

        return {
            "id": row["id"],
            "created_at": row["created_at"],
            "status": row["status"],
            "proposal": payload,
            "summary_text": row["summary_text"],
        }
    finally:
        conn.close()


def _load_asset_config() -> Dict[str, Any]:
    cfg = _load_json(ASSET_CONFIG_PATH, {})
    return cfg if isinstance(cfg, dict) else {}


def _load_scanner() -> Dict[str, Any]:
    data = _load_json(MEME_ROTATION_PATH, {})
    return data if isinstance(data, dict) else {}


def _load_portfolio_snapshot() -> Dict[str, Any]:
    data = _load_json(PORTFOLIO_SNAPSHOT_PATH, {})
    return data if isinstance(data, dict) else {}


def _configured_core_assets() -> set[str]:
    cfg = _load_asset_config()
    core_assets = cfg.get("core_assets", []) or []
    return {str(x).upper().strip() for x in core_assets if str(x or "").strip()}


def _scanner_candidates() -> List[Dict[str, Any]]:
    scanner = _load_scanner()
    out = []
    for c in scanner.get("candidates", []) or []:
        if isinstance(c, dict):
            out.append(c)
    return out


def _portfolio_positions() -> Dict[str, Dict[str, Any]]:
    snap = _load_portfolio_snapshot()
    positions = snap.get("positions", {}) or {}
    return positions if isinstance(positions, dict) else {}


def _market_regime() -> str:
    snap = _load_portfolio_snapshot()
    cash_weight = float(snap.get("cash_weight", 0) or 0)
    if cash_weight >= 0.70:
        return "risk_off"
    if cash_weight >= 0.35:
        return "neutral"
    return "bull"


def _cash_usd() -> float:
    snap = _load_portfolio_snapshot()
    return float(snap.get("usd_cash", 0) or 0)


def _total_value_usd() -> float:
    snap = _load_portfolio_snapshot()
    return float(snap.get("total_value_usd", 0) or 0)


def _current_satellite_positions() -> List[Dict[str, Any]]:
    positions = _portfolio_positions()
    core_assets = _configured_core_assets()
    out = []

    for pid, pos in positions.items():
        if not isinstance(pos, dict):
            continue

        pid = str(pid).upper().strip()
        if pid in core_assets:
            continue

        asset_class = str(pos.get("class", "") or "").strip().lower()
        if asset_class == "dust":
            continue

        value_total = float(pos.get("value_total_usd", 0) or 0)
        weight_total = float(pos.get("weight_total", 0) or 0)

        if value_total <= 0:
            continue

        out.append({
            "product_id": pid,
            "value_total_usd": value_total,
            "weight_total": weight_total,
            "class": asset_class or "satellite",
        })

    out.sort(key=lambda x: x["value_total_usd"], reverse=True)
    return out


def _top_scanner_noncore(limit: int = 5) -> List[Dict[str, Any]]:
    core_assets = _configured_core_assets()
    out = []

    for c in _scanner_candidates():
        pid = str(c.get("product_id", "") or "").upper().strip()
        if not pid or pid in core_assets:
            continue

        out.append({
            "product_id": pid,
            "score": float(c.get("score", 0) or 0),
            "momentum_tag": str(c.get("momentum_tag", "unknown") or "unknown"),
            "volume_tag": str(c.get("volume_tag", "unknown") or "unknown"),
            "meme_boost_bonus": float(((c.get("score_breakdown") or {}).get("meme_boost_bonus", 0)) or 0),
            "pump_protected": bool(c.get("pump_protected", False)),
        })

    return out[:limit]


def _suggested_buy_budget_usd() -> float:
    regime = _market_regime()
    cash = _cash_usd()
    max_quote = _env_float("MAX_QUOTE_PER_TRADE_USD", 25.0)

    if regime == "risk_off":
        return min(max_quote, max(0.0, cash * 0.02))
    if regime == "neutral":
        return min(max_quote, max(0.0, cash * 0.03))
    return min(max_quote, max(0.0, cash * 0.05))


def build_rebalance_proposal() -> Dict[str, Any]:
    regime = _market_regime()
    cash = _cash_usd()
    total_value = _total_value_usd()
    scanner_mode = str((_load_scanner().get("meme_bias_mode", "unknown")) or "unknown")

    top_scanner = _top_scanner_noncore(limit=5)
    held_satellites = _current_satellite_positions()

    held_ids = {p["product_id"] for p in held_satellites}
    scanner_ids = {c["product_id"] for c in top_scanner}

    proposed_actions: List[Dict[str, Any]] = []
    rationale: List[str] = []

    buy_budget = _suggested_buy_budget_usd()

    for c in top_scanner[:3]:
        if c["pump_protected"]:
            continue
        if c["product_id"] in held_ids:
            continue

        proposed_actions.append({
            "action": "buy",
            "product_id": c["product_id"],
            "quote_usd": round(buy_budget, 2),
            "score": c["score"],
            "reason": f"{c['momentum_tag']}/{c['volume_tag']}",
        })

    for p in held_satellites[:3]:
        if p["product_id"] in scanner_ids:
            continue
        if p["value_total_usd"] < 15:
            continue

        trim_usd = min(_env_float("MAX_QUOTE_PER_TRADE_USD", 25.0), p["value_total_usd"] * 0.25)
        if trim_usd < 5:
            continue

        proposed_actions.append({
            "action": "trim",
            "product_id": p["product_id"],
            "quote_usd": round(trim_usd, 2),
            "reason": "held satellite not in current scanner leaders",
        })

    if top_scanner:
        rationale.append(
            "Scanner leaders: " + ", ".join([f"{c['product_id']} ({c['score']:.1f})" for c in top_scanner[:3]])
        )

    if regime == "risk_off":
        rationale.append("Market regime is risk_off; suggestions stay small and defensive.")
    elif regime == "neutral":
        rationale.append("Market regime is neutral; suggestions stay moderate.")
    else:
        rationale.append("Market regime is bull; scanner entries can be more aggressive.")

    rationale.append(f"Cash available: ${cash:,.2f}")
    rationale.append(f"Total portfolio value: ${total_value:,.2f}")

    return {
        "regime": regime,
        "scanner_mode": scanner_mode,
        "cash_usd": cash,
        "total_value_usd": total_value,
        "top_scanner": top_scanner,
        "held_satellites": held_satellites[:5],
        "proposed_actions": proposed_actions,
        "rationale": rationale,
    }


def render_rebalance_proposal_text(proposal: Dict[str, Any]) -> str:
    proposal_id = str(proposal.get("proposal_id", "PENDING") or "PENDING")

    lines = [
        "📬 Rebalance Suggestion",
        f"ID: {proposal_id}",
        "",
        f"Regime: {proposal.get('regime', 'unknown')}",
        f"Scanner Mode: {proposal.get('scanner_mode', 'unknown')}",
        f"Cash: ${float(proposal.get('cash_usd', 0) or 0):,.2f}",
        "",
    ]

    top_scanner = proposal.get("top_scanner", []) or []
    if top_scanner:
        lines.append("Top Scanner Names")
        for c in top_scanner[:5]:
            boost = float(c.get("meme_boost_bonus", 0) or 0)
            boost_txt = f", meme+{boost:.0f}" if boost > 0 else ""
            lines.append(
                f"• {c['product_id']}  |  {c['score']:.1f}  |  {c['momentum_tag']}/{c['volume_tag']}{boost_txt}"
            )
        lines.append("")

    actions = proposal.get("proposed_actions", []) or []
    if actions:
        lines.append("Suggested Actions")
        for a in actions:
            if a["action"] == "buy":
                lines.append(
                    f"• Buy {a['product_id']} for ${float(a['quote_usd']):,.2f} "
                    f"({a.get('reason', 'scanner strength')})"
                )
            elif a["action"] == "trim":
                lines.append(
                    f"• Trim {a['product_id']} by ${float(a['quote_usd']):,.2f} "
                    f"({a.get('reason', 'stale satellite')})"
                )
        lines.append("")
    else:
        lines.append("Suggested Actions")
        lines.append("• No action suggested right now.")
        lines.append("")

    rationale = proposal.get("rationale", []) or []
    if rationale:
        lines.append("Why")
        for r in rationale:
            lines.append(f"• {r}")

    lines.append("")
    lines.append(f"Reply later with: APPROVE {proposal_id}")
    lines.append(f"or: REJECT {proposal_id}")

    return "\n".join(lines).strip()

def get_proposal_by_id(proposal_id: str) -> Dict[str, Any] | None:
    ensure_rebalance_proposals_table()
    conn = _db()
    try:
        row = conn.execute(
            """
            SELECT id, created_at, status, proposal_json, summary_text,
                   approved_at, rejected_at, executed_at
            FROM rebalance_proposals
            WHERE id = ?
            LIMIT 1
            """,
            (str(proposal_id).strip(),),
        ).fetchone()

        if not row:
            return None

        try:
            payload = json.loads(row["proposal_json"])
        except Exception:
            payload = {}

        return {
            "id": row["id"],
            "created_at": row["created_at"],
            "status": row["status"],
            "proposal": payload,
            "summary_text": row["summary_text"],
            "approved_at": row["approved_at"],
            "rejected_at": row["rejected_at"],
            "executed_at": row["executed_at"],
        }
    finally:
        conn.close()


def approve_proposal(proposal_id: str) -> Dict[str, Any]:
    ensure_rebalance_proposals_table()
    proposal_id = str(proposal_id or "").strip()

    existing = get_proposal_by_id(proposal_id)
    if not existing:
        return {
            "ok": False,
            "reason": "not_found",
            "proposal_id": proposal_id,
        }

    if existing["status"] != "pending":
        return {
            "ok": False,
            "reason": "not_pending",
            "proposal_id": proposal_id,
            "current_status": existing["status"],
        }

    approved_at = _utc_now_iso()

    conn = _db()
    try:
        conn.execute(
            """
            UPDATE rebalance_proposals
            SET status = 'approved',
                approved_at = ?
            WHERE id = ?
              AND status = 'pending'
            """,
            (approved_at, proposal_id),
        )
        conn.commit()
    finally:
        conn.close()

    updated = get_proposal_by_id(proposal_id)
    return {
        "ok": True,
        "proposal_id": proposal_id,
        "status": updated["status"] if updated else "approved",
        "approved_at": approved_at,
    }


def reject_proposal(proposal_id: str) -> Dict[str, Any]:
    ensure_rebalance_proposals_table()
    proposal_id = str(proposal_id or "").strip()

    existing = get_proposal_by_id(proposal_id)
    if not existing:
        return {
            "ok": False,
            "reason": "not_found",
            "proposal_id": proposal_id,
        }

    if existing["status"] != "pending":
        return {
            "ok": False,
            "reason": "not_pending",
            "proposal_id": proposal_id,
            "current_status": existing["status"],
        }

    rejected_at = _utc_now_iso()

    conn = _db()
    try:
        conn.execute(
            """
            UPDATE rebalance_proposals
            SET status = 'rejected',
                rejected_at = ?
            WHERE id = ?
              AND status = 'pending'
            """,
            (rejected_at, proposal_id),
        )
        conn.commit()
    finally:
        conn.close()

    updated = get_proposal_by_id(proposal_id)
    return {
        "ok": True,
        "proposal_id": proposal_id,
        "status": updated["status"] if updated else "rejected",
        "rejected_at": rejected_at,
    }

def mark_proposal_executed(proposal_id: str) -> Dict[str, Any]:
    ensure_rebalance_proposals_table()
    proposal_id = str(proposal_id or "").strip()

    existing = get_proposal_by_id(proposal_id)
    if not existing:
        return {
            "ok": False,
            "reason": "not_found",
            "proposal_id": proposal_id,
        }

    if existing["status"] != "approved":
        return {
            "ok": False,
            "reason": "not_approved",
            "proposal_id": proposal_id,
            "current_status": existing["status"],
        }

    executed_at = _utc_now_iso()

    conn = _db()
    try:
        conn.execute(
            """
            UPDATE rebalance_proposals
            SET status = 'executed',
                executed_at = ?
            WHERE id = ?
              AND status = 'approved'
            """,
            (executed_at, proposal_id),
        )
        conn.commit()
    finally:
        conn.close()

    updated = get_proposal_by_id(proposal_id)
    return {
        "ok": True,
        "proposal_id": proposal_id,
        "status": updated["status"] if updated else "executed",
        "executed_at": executed_at,
    }


def proposal_is_stale(proposal: Dict[str, Any], max_age_minutes: int = 60) -> bool:
    from datetime import datetime, timezone

    created_at = str(proposal.get("created_at", "") or "").strip()
    if not created_at:
        return True

    try:
        created_dt = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return True

    age_sec = (datetime.now(timezone.utc) - created_dt).total_seconds()
    return age_sec > (max_age_minutes * 60)
