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
    load_asset_config,
    portfolio_summary,
    save_asset_config,
)
from shadow_rotation_report import build_shadow_rotation_report
from storage import (
    expire_pending_config_proposals,
    find_pending_config_proposal_by_fingerprint,
    get_config_proposal_by_id,
    get_latest_pending_config_proposal,
    get_portfolio_history_since,
    list_recent_config_proposals,
    list_pending_config_proposals,
    save_config_proposal,
    set_config_proposal_status,
    supersede_pending_config_proposals,
    utcnow_iso,
)


PROPOSAL_TYPE = "config_guardrail"
SHADOW_ALLOWLIST_PROPOSAL_TYPE = "satellite_enable_recommendation"
DEFAULT_ADVISORY_RANGE = "30d"
DEFAULT_PROPOSAL_TTL_MINUTES = int(float(os.getenv("CONFIG_PROPOSAL_TTL_MINUTES", "120") or "120"))
DEFAULT_PROPOSAL_GENERATION_MODE = "manual"
DEFAULT_PROPOSAL_APPLY_MODE = "manual"
DEFAULT_PROPOSAL_MIN_CONFIDENCE = "high"
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


def _normalize_confidence_value(value, allowed_values, fallback):
    normalized = str(value or "").strip().lower()
    return normalized if normalized in allowed_values else fallback


def _confidence_rank(value):
    mapping = {"low": 0, "medium": 1, "high": 2}
    return mapping.get(str(value or "").strip().lower(), -1)


def get_config_proposal_automation_settings(config=None):
    cfg = _safe_dict(config) if isinstance(config, dict) else _safe_dict(load_asset_config())

    generation_mode = _normalize_confidence_value(
        cfg.get("config_proposal_generation_mode", DEFAULT_PROPOSAL_GENERATION_MODE),
        {"manual", "auto"},
        DEFAULT_PROPOSAL_GENERATION_MODE,
    )

    apply_mode = _normalize_confidence_value(
        cfg.get("config_proposal_apply_mode", DEFAULT_PROPOSAL_APPLY_MODE),
        {"manual", "after_approval"},
        DEFAULT_PROPOSAL_APPLY_MODE,
    )

    min_confidence = _normalize_confidence_value(
        cfg.get("config_proposal_min_confidence", DEFAULT_PROPOSAL_MIN_CONFIDENCE),
        {"medium", "high"},
        DEFAULT_PROPOSAL_MIN_CONFIDENCE,
    )

    return {
        "generation_mode": generation_mode,
        "apply_mode": apply_mode,
        "min_confidence": min_confidence,
    }


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


def build_shadow_allowlist_proposal(window_hours=24):
    report = _safe_dict(build_shadow_rotation_report(window_hours=window_hours))
    candidates = _safe_list(report.get("shadow_eligible_candidates"))

    if not candidates:
        return {
            "ok": True,
            "status": "noop",
            "reason": "no_shadow_eligible_candidates",
            "proposal": None,
        }

    confidence = "high" if any(str(_safe_dict(item).get("confidence_band", "")).strip().lower() == "high" for item in candidates) else "medium"
    blocked_reasons = _safe_list(report.get("blocked_reason_breakdown"))
    top_blocker = str(_safe_dict(blocked_reasons[0]).get("reason", "") or "").strip() if blocked_reasons else ""

    proposal = {
        "proposal_type": SHADOW_ALLOWLIST_PROPOSAL_TYPE,
        "source": {
            "window_hours": int(report.get("window_hours", window_hours) or window_hours),
            "top_n": int(report.get("top_n", len(candidates)) or len(candidates)),
            "confidence": confidence,
            "report_generated_at": report.get("generated_at"),
        },
        "summary": "These satellite candidates meet review thresholds but are not yet live in the allowlist.",
        "reasons": _clean_text_list(report.get("quick_takeaways"), limit=3),
        "notes": _clean_text_list(
            [
                f"Top blocker: {top_blocker}" if top_blocker else "",
                f"Cycles analyzed: {int(report.get('cycles_analyzed', 0) or 0)}",
            ],
            limit=3,
        ),
        "candidates": [
            {
                "product_id": str(_safe_dict(item).get("product_id", "") or "").strip(),
                "net_score": float(_safe_dict(item).get("net_score", 0.0) or 0.0),
                "confidence_band": str(_safe_dict(item).get("confidence_band", "") or "").strip(),
                "liquidity_bucket": str(_safe_dict(item).get("liquidity_bucket", "") or "").strip(),
                "volatility_bucket": str(_safe_dict(item).get("volatility_bucket", "") or "").strip(),
                "shadow_eligible": bool(_safe_dict(item).get("shadow_eligible")),
                "shadow_eligibility_reason": str(_safe_dict(item).get("shadow_eligibility_reason", "") or "").strip(),
                "shadow_block_reason": str(_safe_dict(item).get("shadow_block_reason", "") or "").strip(),
            }
            for item in candidates
            if str(_safe_dict(item).get("product_id", "") or "").strip()
        ],
        "changes": [],
        "simulation": {},
    }

    return {
        "ok": True,
        "status": "proposal_ready",
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
        "candidates": [
            {
                "product_id": str(_safe_dict(item).get("product_id", "") or "").strip(),
                "net_score": float(_safe_dict(item).get("net_score", 0.0) or 0.0),
                "confidence_band": str(_safe_dict(item).get("confidence_band", "") or "").strip(),
                "liquidity_bucket": str(_safe_dict(item).get("liquidity_bucket", "") or "").strip(),
                "volatility_bucket": str(_safe_dict(item).get("volatility_bucket", "") or "").strip(),
                "shadow_block_reason": str(_safe_dict(item).get("shadow_block_reason", "") or "").strip(),
            }
            for item in sorted(_safe_list(proposal.get("candidates")), key=lambda x: str(_safe_dict(x).get("product_id", "")))
            if str(_safe_dict(item).get("product_id", "") or "").strip()
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


def _parse_iso_datetime(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def _find_recent_matching_proposal(fingerprint, proposal_type=PROPOSAL_TYPE, window_minutes=180):
    recent_items = list_recent_config_proposals(limit=5, proposal_type=proposal_type)
    cutoff = datetime.utcnow() - timedelta(minutes=max(1, int(window_minutes or 180)))

    for item in recent_items:
        item = _safe_dict(item)
        if str(item.get("fingerprint", "") or "").strip() != str(fingerprint or "").strip():
            continue
        created_at = _parse_iso_datetime(item.get("created_at"))
        if created_at and created_at >= cutoff:
            return item
    return None


def expire_stale_proposals(proposal_type=PROPOSAL_TYPE):
    return expire_pending_config_proposals(utcnow_iso(), proposal_type=proposal_type)


def generate_config_proposal(range_name=DEFAULT_ADVISORY_RANGE, ttl_minutes=DEFAULT_PROPOSAL_TTL_MINUTES, min_confidence=None):
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
    proposal_confidence = _normalize_confidence_value(
        _safe_dict(proposal.get("source")).get("confidence", "low"),
        {"low", "medium", "high"},
        "low",
    )
    required_confidence = _normalize_confidence_value(
        min_confidence,
        {"medium", "high"},
        "",
    )

    if required_confidence and _confidence_rank(proposal_confidence) < _confidence_rank(required_confidence):
        return {
            "ok": True,
            "status": "skipped_low_confidence",
            "reason": "confidence_below_threshold",
            "expired_count": expired_count,
            "confidence": proposal_confidence,
            "required_confidence": required_confidence,
            "proposal": None,
        }

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

    recent_match = _find_recent_matching_proposal(fingerprint, proposal_type=PROPOSAL_TYPE, window_minutes=ttl_minutes)
    if recent_match:
        return {
            "ok": True,
            "status": "deduped_recent",
            "expired_count": expired_count,
            "proposal_id": recent_match.get("id"),
            "proposal": recent_match,
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


def generate_shadow_allowlist_proposal(window_hours=24, ttl_minutes=DEFAULT_PROPOSAL_TTL_MINUTES):
    expired_count = expire_stale_proposals(proposal_type=SHADOW_ALLOWLIST_PROPOSAL_TYPE)
    built = build_shadow_allowlist_proposal(window_hours=window_hours)

    if built.get("status") == "noop":
        return {
            "ok": True,
            "status": "noop",
            "reason": built.get("reason", "no_shadow_eligible_candidates"),
            "expired_count": expired_count,
            "proposal": None,
        }

    proposal = _safe_dict(built.get("proposal"))
    fingerprint = compute_config_proposal_fingerprint(proposal)
    existing = find_pending_config_proposal_by_fingerprint(fingerprint, proposal_type=SHADOW_ALLOWLIST_PROPOSAL_TYPE)

    if existing and not proposal_is_stale(existing):
        return {
            "ok": True,
            "status": "deduped",
            "expired_count": expired_count,
            "proposal_id": existing.get("id"),
            "proposal": existing,
        }

    recent_match = _find_recent_matching_proposal(
        fingerprint,
        proposal_type=SHADOW_ALLOWLIST_PROPOSAL_TYPE,
        window_minutes=ttl_minutes,
    )
    if recent_match:
        return {
            "ok": True,
            "status": "deduped_recent",
            "expired_count": expired_count,
            "proposal_id": recent_match.get("id"),
            "proposal": recent_match,
        }

    pending = list_pending_config_proposals(SHADOW_ALLOWLIST_PROPOSAL_TYPE)
    pending = [item for item in pending if item]
    pending = [item for item in pending if str(item.get("fingerprint", "") or "").strip() != fingerprint]
    superseded_count = 0
    if pending:
        superseded_count = supersede_pending_config_proposals(SHADOW_ALLOWLIST_PROPOSAL_TYPE)

    expires_at = _expires_at_iso(ttl_minutes)
    summary_text = str(proposal.get("summary", "") or "").strip() or "Satellite enable recommendation ready for review."
    proposal_id = save_config_proposal(
        proposal=proposal,
        summary_text=summary_text,
        fingerprint=fingerprint,
        proposal_type=SHADOW_ALLOWLIST_PROPOSAL_TYPE,
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


def generate_review_proposals(range_name=DEFAULT_ADVISORY_RANGE, ttl_minutes=DEFAULT_PROPOSAL_TTL_MINUTES, min_confidence=None):
    config_result = generate_config_proposal(range_name=range_name, ttl_minutes=ttl_minutes, min_confidence=min_confidence)
    shadow_result = generate_shadow_allowlist_proposal(window_hours=24, ttl_minutes=ttl_minutes)
    results = [config_result, shadow_result]

    created = [item for item in results if str(item.get("status", "")).strip().lower() == "created"]
    deduped = [item for item in results if str(item.get("status", "")).strip().lower() in {"deduped", "deduped_recent"}]
    primary = (
        (shadow_result if str(shadow_result.get("status", "")).strip().lower() == "created" else None)
        or (config_result if str(config_result.get("status", "")).strip().lower() == "created" else None)
        or (shadow_result if str(shadow_result.get("status", "")).strip().lower() in {"deduped", "deduped_recent"} else None)
        or (config_result if str(config_result.get("status", "")).strip().lower() in {"deduped", "deduped_recent"} else None)
        or shadow_result
        or config_result
    )

    if created:
        aggregate_status = "created"
    elif deduped:
        aggregate_status = "deduped"
    elif all(str(item.get("status", "")).strip().lower() == "noop" for item in results):
        aggregate_status = "noop"
    else:
        aggregate_status = str(primary.get("status", "noop") or "noop")

    return {
        "ok": True,
        "status": aggregate_status,
        "proposal_id": primary.get("proposal_id"),
        "expired_count": sum(int(item.get("expired_count", 0) or 0) for item in results),
        "superseded_count": sum(int(item.get("superseded_count", 0) or 0) for item in results),
        "notification_sent": any(bool(item.get("notification_sent", False)) for item in results),
        "created_count": len(created),
        "deduped_count": len(deduped),
        "noop_count": sum(1 for item in results if str(item.get("status", "")).strip().lower() == "noop"),
        "config_guardrail": config_result,
        "satellite_enable_recommendation": shadow_result,
    }


def get_latest_config_proposal():
    return get_latest_pending_config_proposal(PROPOSAL_TYPE)


def _clear_config_apply_caches():
    try:
        from routes.api import _API_CACHE, _API_CACHE_LOCK

        with _API_CACHE_LOCK:
            _API_CACHE.pop("config", None)
            _API_CACHE.pop("portfolio", None)
            _API_CACHE.pop("rebalance_preview", None)
            _API_CACHE.pop("heatmap", None)
            _API_CACHE.pop("meme_heatmap", None)
            _API_CACHE.pop("meme_rotation", None)
            _API_CACHE.pop("portfolio_summary_v2", None)
            _API_CACHE.pop("portfolio_allocations", None)
    except Exception:
        return


def apply_config_proposal(proposal_id, applied_by=None):
    expire_stale_proposals()
    proposal_id = str(proposal_id or "").strip()
    applied_by = str(applied_by or "").strip() or None

    existing = get_config_proposal_by_id(proposal_id)
    if not existing:
        return {
            "ok": False,
            "reason": "not_found",
            "proposal_id": proposal_id,
            "current_status": None,
        }

    current_status = str(existing.get("status") or "").strip() or None
    if current_status != "approved":
        return {
            "ok": False,
            "reason": "not_approved",
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

    proposal = _safe_dict(existing.get("proposal"))
    changes = [item for item in _safe_list(proposal.get("changes")) if str(_safe_dict(item).get("key", "") or "").strip() in ALLOWED_CHANGE_KEYS]

    if not changes:
        return {
            "ok": False,
            "reason": "no_allowed_changes",
            "proposal_id": proposal_id,
            "current_status": current_status,
        }

    config = _safe_dict(load_asset_config())
    changed = False

    for item in changes:
        item = _safe_dict(item)
        key = str(item.get("key", "") or "").strip()
        kind = str(item.get("kind", "float") or "float").strip().lower()
        proposed_value = item.get("proposed_value")

        if key not in ALLOWED_CHANGE_KEYS:
            continue

        if kind == "int":
            normalized_value = int(float(proposed_value or 0))
        else:
            normalized_value = float(proposed_value or 0.0)

        current_value = config.get(key)
        if current_value != normalized_value:
            config[key] = normalized_value
            changed = True

    if changed:
        save_asset_config(config)
        _clear_config_apply_caches()

    result = set_config_proposal_status(
        proposal_id=proposal_id,
        status="applied",
        timestamp_field="applied_at",
        actor_field="applied_by",
        actor=applied_by,
        expected_current_status="approved",
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
        "status": (updated or {}).get("status", "applied"),
        "applied_at": result.get("timestamp"),
        "applied_by": applied_by,
        "config_changed": changed,
    }


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
    approval_result = {
        "ok": True,
        "proposal_id": proposal_id,
        "status": (updated or {}).get("status", "approved"),
        "approved_at": result.get("timestamp"),
        "approved_by": actor,
    }

    settings = get_config_proposal_automation_settings()
    if settings.get("apply_mode") != "after_approval":
        return approval_result

    apply_result = apply_config_proposal(proposal_id, applied_by=actor)
    if apply_result.get("ok"):
        return {
            **approval_result,
            **apply_result,
            "auto_apply_attempted": True,
            "auto_apply_ok": True,
        }

    return {
        **approval_result,
        "auto_apply_attempted": True,
        "auto_apply_ok": False,
        "auto_apply_reason": apply_result.get("reason"),
        "auto_apply_status": apply_result.get("current_status"),
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
