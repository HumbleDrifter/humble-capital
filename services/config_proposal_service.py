import hashlib
import json
import os
import time
from datetime import datetime, timedelta

from notify import notify_config_proposal
from portfolio import (
    build_adaptive_suggestions,
    build_auto_adaptive_recommendation,
    build_portfolio_history_analytics,
    build_portfolio_risk_score,
    get_portfolio_snapshot,
    portfolio_summary,
)
from storage import (
    expire_pending_config_proposals,
    find_pending_config_proposal_by_fingerprint,
    get_config_proposal_by_id,
    get_latest_pending_config_proposal,
    get_portfolio_history_since,
    list_pending_config_proposals,
    save_config_proposal,
    set_config_proposal_status,
    supersede_pending_config_proposals,
    utcnow_iso,
)


PROPOSAL_TYPE = "config_guardrail"
DEFAULT_ADVISORY_RANGE = "30d"
DEFAULT_PROPOSAL_TTL_MINUTES = int(float(os.getenv("CONFIG_PROPOSAL_TTL_MINUTES", "120") or "120"))
ALLOWED_CHANGE_KEYS = {
    "satellite_total_target",
    "satellite_total_max",
    "min_cash_reserve",
    "trade_min_value_usd",
    "max_active_satellites",
    "max_new_satellites_per_cycle",
}


def _safe_dict(value):
    return value if isinstance(value, dict) else {}


def _safe_list(value):
    return value if isinstance(value, list) else []


def _clean_text_list(values, limit=3):
    out = []
    for value in _safe_list(values):
        text = str(value or "").strip()
        if text and text not in out:
            out.append(text)
        if len(out) >= limit:
            break
    return out


def _range_to_start_ts(range_name):
    name = str(range_name or DEFAULT_ADVISORY_RANGE).strip().lower()
    now = int(time.time())
    if name == "7d":
        return now - (7 * 86400)
    if name == "30d":
        return now - (30 * 86400)
    if name == "90d":
        return now - (90 * 86400)
    return None


def _expires_at_iso(ttl_minutes):
    ttl = max(1, int(ttl_minutes or DEFAULT_PROPOSAL_TTL_MINUTES))
    expires_at = datetime.utcnow() + timedelta(minutes=ttl)
    return expires_at.replace(microsecond=0).isoformat() + "Z"


def _normalize_change_value(value, kind):
    if value in (None, ""):
        return None

    if kind == "int":
        return int(float(value))

    return float(value)


def _change_kind_for_item(item):
    format_type = str(_safe_dict(item).get("format", "float") or "float").strip().lower()
    return "int" if format_type == "integer" else "float"


def _changes_from_simulation(auto_adaptive):
    simulation = _safe_dict(_safe_dict(auto_adaptive).get("simulation"))
    changes = []

    for item in _safe_list(simulation.get("changed_controls")):
        item = _safe_dict(item)
        key = str(item.get("key", "") or "").strip()
        if key not in ALLOWED_CHANGE_KEYS:
            continue

        kind = _change_kind_for_item(item)
        current_value = _normalize_change_value(item.get("current_value"), kind)
        proposed_value = _normalize_change_value(item.get("projected_value"), kind)

        if current_value == proposed_value:
            continue

        changes.append(
            {
                "key": key,
                "label": str(item.get("label", key) or key).strip(),
                "current_value": current_value,
                "proposed_value": proposed_value,
                "kind": kind,
                "format": str(item.get("format", "text") or "text").strip(),
            }
        )

    changes.sort(key=lambda x: x["key"])
    return changes


def _build_advisory_context(range_name=DEFAULT_ADVISORY_RANGE):
    snapshot = _safe_dict(get_portfolio_snapshot())
    summary = _safe_dict(portfolio_summary(snapshot))
    history_rows = get_portfolio_history_since(start_ts=_range_to_start_ts(range_name))
    analytics = _safe_dict(build_portfolio_history_analytics(history_rows, source="portfolio_history"))
    risk_score = _safe_dict(
        build_portfolio_risk_score(
            snapshot=snapshot,
            summary=summary,
            history_analytics=analytics,
        )
    )
    adaptive_suggestions = _safe_dict(
        build_adaptive_suggestions(
            snapshot=snapshot,
            summary=summary,
            history_analytics=analytics,
            risk_score=risk_score,
        )
    )
    auto_adaptive = _safe_dict(
        build_auto_adaptive_recommendation(
            snapshot=snapshot,
            summary=summary,
            history_analytics=analytics,
            risk_score=risk_score,
        )
    )

    return {
        "range": str(range_name or DEFAULT_ADVISORY_RANGE).strip().lower() or DEFAULT_ADVISORY_RANGE,
        "snapshot": snapshot,
        "summary": summary,
        "analytics": analytics,
        "risk_score": risk_score,
        "adaptive_suggestions": adaptive_suggestions,
        "auto_adaptive": auto_adaptive,
    }


def build_config_proposal(range_name=DEFAULT_ADVISORY_RANGE):
    context = _build_advisory_context(range_name=range_name)
    risk_score = _safe_dict(context.get("risk_score"))
    adaptive_suggestions = _safe_dict(context.get("adaptive_suggestions"))
    auto_adaptive = _safe_dict(context.get("auto_adaptive"))
    simulation = _safe_dict(auto_adaptive.get("simulation"))
    changes = _changes_from_simulation(auto_adaptive)

    if not changes:
        return {
            "ok": True,
            "status": "noop",
            "reason": "no_allowed_changes",
            "context": context,
            "proposal": None,
        }

    proposal = {
        "proposal_type": PROPOSAL_TYPE,
        "source": {
            "advisory_range": context["range"],
            "risk_score": int(risk_score.get("score", 0) or 0),
            "risk_band": str(risk_score.get("band", "Moderate Risk") or "Moderate Risk"),
            "recommended_preset": str(auto_adaptive.get("recommended_preset", "balanced") or "balanced"),
            "recommended_label": str(auto_adaptive.get("label", "Balanced") or "Balanced"),
            "confidence": str(auto_adaptive.get("confidence", "low") or "low"),
        },
        "summary": str(auto_adaptive.get("summary", "") or "").strip(),
        "reasons": _clean_text_list(auto_adaptive.get("reasons"), limit=3),
        "notes": _clean_text_list(adaptive_suggestions.get("notes"), limit=3),
        "changes": changes,
        "simulation": {
            "current_score": int(simulation.get("current_score", 0) or 0),
            "projected_score": int(simulation.get("projected_score", 0) or 0),
            "score_delta": int(simulation.get("score_delta", 0) or 0),
            "current_band": str(simulation.get("current_band", "Moderate Risk") or "Moderate Risk"),
            "projected_band": str(simulation.get("projected_band", "Moderate Risk") or "Moderate Risk"),
            "summary": str(simulation.get("summary", "") or "").strip(),
        },
    }

    return {
        "ok": True,
        "status": "proposal_ready",
        "context": context,
        "proposal": proposal,
    }


def compute_config_proposal_fingerprint(proposal):
    proposal = _safe_dict(proposal)
    normalized = {
        "proposal_type": str(proposal.get("proposal_type", PROPOSAL_TYPE) or PROPOSAL_TYPE),
        "recommended_preset": str(_safe_dict(proposal.get("source")).get("recommended_preset", "balanced") or "balanced"),
        "projected_band": str(_safe_dict(proposal.get("simulation")).get("projected_band", "Moderate Risk") or "Moderate Risk"),
        "changes": [
            {
                "key": str(_safe_dict(item).get("key", "") or "").strip(),
                "proposed_value": _safe_dict(item).get("proposed_value"),
                "kind": str(_safe_dict(item).get("kind", "float") or "float"),
            }
            for item in sorted(_safe_list(proposal.get("changes")), key=lambda x: str(_safe_dict(x).get("key", "")))
            if str(_safe_dict(item).get("key", "") or "").strip()
        ],
    }
    raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def proposal_is_stale(proposal):
    proposal = _safe_dict(proposal)
    expires_at = str(proposal.get("expires_at", "") or "").strip()
    if not expires_at:
        return False

    try:
        return datetime.utcnow() >= datetime.strptime(expires_at, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return True


def expire_stale_proposals(proposal_type=PROPOSAL_TYPE):
    return expire_pending_config_proposals(utcnow_iso(), proposal_type=proposal_type)


def generate_config_proposal(range_name=DEFAULT_ADVISORY_RANGE, ttl_minutes=DEFAULT_PROPOSAL_TTL_MINUTES):
    expired_count = expire_stale_proposals()
    built = build_config_proposal(range_name=range_name)

    if built.get("status") == "noop":
        return {
            "ok": True,
            "status": "noop",
            "reason": built.get("reason", "no_allowed_changes"),
            "expired_count": expired_count,
            "proposal": None,
        }

    proposal = _safe_dict(built.get("proposal"))
    fingerprint = compute_config_proposal_fingerprint(proposal)
    existing = find_pending_config_proposal_by_fingerprint(fingerprint, proposal_type=PROPOSAL_TYPE)

    if existing and not proposal_is_stale(existing):
        return {
            "ok": True,
            "status": "deduped",
            "expired_count": expired_count,
            "proposal_id": existing.get("id"),
            "proposal": existing,
        }

    pending = list_pending_config_proposals(PROPOSAL_TYPE)
    pending = [item for item in pending if item]
    pending = [item for item in pending if str(item.get("fingerprint", "") or "").strip() != fingerprint]
    superseded_count = 0
    if pending:
        superseded_count = supersede_pending_config_proposals(PROPOSAL_TYPE)

    expires_at = _expires_at_iso(ttl_minutes)
    summary_text = str(proposal.get("summary", "") or "").strip() or "Config guardrail proposal ready for review."
    proposal_id = save_config_proposal(
        proposal=proposal,
        summary_text=summary_text,
        fingerprint=fingerprint,
        proposal_type=PROPOSAL_TYPE,
        expires_at=expires_at,
        status="pending",
    )
    saved = get_config_proposal_by_id(proposal_id)
    notification_sent = False
    try:
        if saved:
            notification_sent = bool(notify_config_proposal(saved))
    except Exception:
        notification_sent = False

    return {
        "ok": True,
        "status": "created",
        "expired_count": expired_count,
        "superseded_count": superseded_count,
        "proposal_id": proposal_id,
        "proposal": saved,
        "notification_sent": notification_sent,
    }


def get_latest_config_proposal():
    return get_latest_pending_config_proposal(PROPOSAL_TYPE)


def approve_config_proposal(proposal_id, actor=None):
    expire_stale_proposals()
    proposal_id = str(proposal_id or "").strip()
    actor = str(actor or "").strip() or None

    existing = get_config_proposal_by_id(proposal_id)
    if not existing:
        return {
            "ok": False,
            "reason": "not_found",
            "proposal_id": proposal_id,
            "current_status": None,
        }

    current_status = str(existing.get("status") or "").strip() or None

    if current_status == "superseded":
        return {
            "ok": False,
            "reason": "superseded",
            "proposal_id": proposal_id,
            "current_status": current_status,
        }

    if current_status != "pending":
        return {
            "ok": False,
            "reason": "not_pending",
            "proposal_id": proposal_id,
            "current_status": current_status,
        }

    if proposal_is_stale(existing):
        expire_stale_proposals()
        refreshed = get_config_proposal_by_id(proposal_id) or existing
        return {
            "ok": False,
            "reason": "expired",
            "proposal_id": proposal_id,
            "current_status": refreshed.get("status", "expired"),
        }

    result = set_config_proposal_status(
        proposal_id=proposal_id,
        status="approved",
        timestamp_field="approved_at",
        actor_field="approved_by",
        actor=actor,
        expected_current_status="pending",
    )

    if not result.get("ok"):
        updated = get_config_proposal_by_id(proposal_id)
        return {
            "ok": False,
            "reason": "status_update_failed",
            "proposal_id": proposal_id,
            "current_status": (updated or {}).get("status"),
        }

    updated = get_config_proposal_by_id(proposal_id)
    return {
        "ok": True,
        "proposal_id": proposal_id,
        "status": (updated or {}).get("status", "approved"),
        "approved_at": result.get("timestamp"),
        "approved_by": actor,
    }


def reject_config_proposal(proposal_id, actor=None):
    expire_stale_proposals()
    proposal_id = str(proposal_id or "").strip()
    actor = str(actor or "").strip() or None

    existing = get_config_proposal_by_id(proposal_id)
    if not existing:
        return {
            "ok": False,
            "reason": "not_found",
            "proposal_id": proposal_id,
            "current_status": None,
        }

    current_status = str(existing.get("status") or "").strip() or None

    if current_status == "superseded":
        return {
            "ok": False,
            "reason": "superseded",
            "proposal_id": proposal_id,
            "current_status": current_status,
        }

    if current_status != "pending":
        return {
            "ok": False,
            "reason": "not_pending",
            "proposal_id": proposal_id,
            "current_status": current_status,
        }

    if proposal_is_stale(existing):
        expire_stale_proposals()
        refreshed = get_config_proposal_by_id(proposal_id) or existing
        return {
            "ok": False,
            "reason": "expired",
            "proposal_id": proposal_id,
            "current_status": refreshed.get("status", "expired"),
        }

    result = set_config_proposal_status(
        proposal_id=proposal_id,
        status="rejected",
        timestamp_field="rejected_at",
        actor_field="rejected_by",
        actor=actor,
        expected_current_status="pending",
    )

    if not result.get("ok"):
        updated = get_config_proposal_by_id(proposal_id)
        return {
            "ok": False,
            "reason": "status_update_failed",
            "proposal_id": proposal_id,
            "current_status": (updated or {}).get("status"),
        }

    updated = get_config_proposal_by_id(proposal_id)
    return {
        "ok": True,
        "proposal_id": proposal_id,
        "status": (updated or {}).get("status", "rejected"),
        "rejected_at": result.get("timestamp"),
        "rejected_by": actor,
    }
