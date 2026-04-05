from __future__ import annotations

from datetime import datetime, timedelta


REPLACEMENT_SCORE_DELTA_THRESHOLD = 8.0
RECENT_PROPOSAL_COOLDOWN_MINUTES = 360
RECENT_REPLACEMENT_COOLDOWN_MINUTES = 720
MIN_STABILITY_HITS = 2
STABILITY_WINDOW_CYCLES = 6
ALMOST_READY_MIN_NET_SCORE = 55.0


def _safe_dict(value):
    return value if isinstance(value, dict) else {}


def _safe_list(value):
    return value if isinstance(value, list) else []


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _parse_iso_datetime(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def _recent_satellite_proposals(recent_proposals):
    items = []
    for proposal in _safe_list(recent_proposals):
        proposal = _safe_dict(proposal)
        nested = _safe_dict(proposal.get("proposal"))
        proposal_type = str(nested.get("proposal_type") or proposal.get("proposal_type") or "").strip().lower()
        if proposal_type != "satellite_enable_recommendation":
            continue
        items.append(
            {
                "id": str(proposal.get("id") or "").strip(),
                "status": str(proposal.get("status") or "").strip().lower(),
                "created_at": _parse_iso_datetime(proposal.get("created_at")),
                "approved_at": _parse_iso_datetime(proposal.get("approved_at")),
                "applied_at": _parse_iso_datetime(proposal.get("applied_at")),
                "candidates": [
                    _safe_dict(item)
                    for item in _safe_list(nested.get("candidates"))
                    if str(_safe_dict(item).get("product_id") or "").strip()
                ],
            }
        )
    return items


def _recent_candidate_maps(recent_proposals):
    now = datetime.utcnow()
    proposal_cutoff = now - timedelta(minutes=RECENT_PROPOSAL_COOLDOWN_MINUTES)
    replacement_cutoff = now - timedelta(minutes=RECENT_REPLACEMENT_COOLDOWN_MINUTES)
    recent_candidate_ids = set()
    recent_replacement_targets = set()

    for proposal in _recent_satellite_proposals(recent_proposals):
        event_time = proposal.get("applied_at") or proposal.get("approved_at") or proposal.get("created_at")
        if not event_time:
            continue

        for candidate in proposal.get("candidates", []):
            product_id = str(candidate.get("product_id") or "").strip()
            if not product_id:
                continue
            if event_time >= proposal_cutoff and proposal.get("status") in {"pending", "approved", "applied"}:
                recent_candidate_ids.add(product_id)

            replacement_target = str(candidate.get("replacement_target") or "").strip()
            if replacement_target and event_time >= replacement_cutoff and proposal.get("status") in {"pending", "approved", "applied"}:
                recent_replacement_targets.add(replacement_target)

    return {
        "recent_candidate_ids": recent_candidate_ids,
        "recent_replacement_targets": recent_replacement_targets,
    }


def _stability_snapshot(product_id, cycles, top_n):
    sorted_cycles = sorted(_safe_list(cycles), key=lambda item: int(_safe_dict(item).get("logged_at") or 0), reverse=True)
    observed_cycles = 0
    stability_hits = 0

    for cycle in sorted_cycles[:STABILITY_WINDOW_CYCLES]:
        ranking = _safe_list(_safe_dict(cycle).get("ranking_by_net_score"))
        if not ranking:
            continue
        observed_cycles += 1
        top_rows = ranking[: max(1, int(top_n or 1))]
        if any(str(_safe_dict(row).get("product_id") or "").strip() == product_id for row in top_rows):
            stability_hits += 1

    return {
        "stability_hits": stability_hits,
        "stability_window_cycles": observed_cycles,
        "stability_ok": observed_cycles < MIN_STABILITY_HITS or stability_hits >= MIN_STABILITY_HITS,
    }


def _active_rows(latest_rows, current_system_selections):
    current_selection_ids = {str(item or "").strip() for item in _safe_list(current_system_selections) if str(item or "").strip()}
    active_rows = []
    for row in _safe_list(latest_rows):
        row = _safe_dict(row)
        product_id = str(row.get("product_id") or "").strip()
        if not product_id:
            continue
        if bool(row.get("core")) or bool(row.get("blocked")):
            continue
        if product_id in current_selection_ids or bool(row.get("active_buy_universe")) or bool(row.get("held")) or bool(row.get("allowed")):
            active_rows.append(row)
    return active_rows


def _weakest_replaceable_active(latest_rows, current_system_selections):
    active_rows = _active_rows(latest_rows, current_system_selections)
    if not active_rows:
        return None
    return min(active_rows, key=lambda row: _safe_float(_safe_dict(row).get("net_score")))


def _decision_priority(value):
    normalized = str(value or "").strip().lower()
    if normalized == "recommend_replacement":
        return 4
    if normalized == "recommend_for_enable":
        return 3
    if normalized == "almost_ready":
        return 2
    if normalized == "blocked":
        return 1
    return 0


def _portfolio_context(row, active_count, max_active):
    row = _safe_dict(row)
    held = bool(row.get("held"))
    active_buy_universe = bool(row.get("active_buy_universe"))
    allowed = bool(row.get("allowed"))
    slots_remaining = None if max_active is None else max(0, max_active - active_count)

    held_context = "Already held" if held else "New candidate"
    if held:
        slot_pressure = "Held slot preserved"
    elif max_active is None:
        slot_pressure = "Flexible room"
    elif active_count >= max_active:
        slot_pressure = "Slots full"
    elif active_count >= max(max_active - 1, 0):
        slot_pressure = "Limited room"
    else:
        slot_pressure = "Room available"

    if max_active is None:
        portfolio_pressure = "normal"
    elif active_count >= max_active:
        portfolio_pressure = "high"
    elif active_count >= max(max_active - 1, 0):
        portfolio_pressure = "moderate"
    else:
        portfolio_pressure = "normal"

    note_parts = [
        "Already held in the portfolio." if held else "Adds new satellite exposure.",
    ]
    if allowed:
        note_parts.append("Already represented in the allowlist.")
    elif active_buy_universe:
        note_parts.append("Already represented in the live active universe.")
    else:
        note_parts.append("Not live in the active universe yet.")
    if slots_remaining is not None:
        note_parts.append(f"{slots_remaining} active slot{'s' if slots_remaining != 1 else ''} open.")

    return {
        "held_context": held_context,
        "slot_pressure": slot_pressure,
        "portfolio_pressure": portfolio_pressure,
        "portfolio_context_note": " ".join(note_parts),
        "active_satellite_count": active_count,
        "configured_max_active": max_active,
        "slots_remaining": slots_remaining,
    }


def build_satellite_decisions(latest_rows, *, cycles=None, current_system_selections=None, configured_max_active=None, recent_proposals=None):
    rows = [_safe_dict(row) for row in _safe_list(latest_rows) if str(_safe_dict(row).get("product_id") or "").strip()]
    cycles = _safe_list(cycles)
    current_system_selections = _safe_list(current_system_selections)
    recent_maps = _recent_candidate_maps(recent_proposals)
    recent_candidate_ids = recent_maps["recent_candidate_ids"]
    recent_replacement_targets = recent_maps["recent_replacement_targets"]

    active_rows = _active_rows(rows, current_system_selections)
    active_count = len(active_rows)
    max_active = _safe_int(configured_max_active, 0) or None
    weakest_active = _weakest_replaceable_active(rows, current_system_selections)

    decisions = []
    for row in rows:
        product_id = str(row.get("product_id") or "").strip()
        net_score = _safe_float(row.get("net_score"))
        held = bool(row.get("held"))
        allowed = bool(row.get("allowed"))
        blocked = bool(row.get("blocked"))
        shadow_eligible = bool(row.get("shadow_eligible"))
        active_buy_universe = bool(row.get("active_buy_universe"))
        confidence_band = str(row.get("confidence_band") or "").strip().lower() or "unknown"

        stability = _stability_snapshot(product_id, cycles, max_active or 1)
        blockers = []
        replacement_target = None
        replacement_score_delta = None
        decision = "ignore"
        decision_reason = "Not currently actionable for review."
        portfolio_context = _portfolio_context(row, active_count, max_active)

        if blocked:
            decision = "blocked"
            blockers.append("blocked_by_policy")
            decision_reason = "Blocked by policy and excluded from review action."
        elif allowed:
            decision = "ignore"
            decision_reason = "Already allowed and does not need a new review proposal."
        elif not shadow_eligible:
            decision = "almost_ready" if net_score >= ALMOST_READY_MIN_NET_SCORE else "ignore"
            primary_fail = str(row.get("primary_fail_reason") or row.get("shadow_block_reason") or "insufficient_signal").strip()
            if primary_fail:
                blockers.append(primary_fail)
            decision_reason = str(row.get("fail_explanation") or row.get("shadow_eligibility_reason") or "Review thresholds are not fully met.").strip()
        else:
            if not stability["stability_ok"] and not held:
                blockers.append("low_persistence")

            if product_id in recent_candidate_ids and not held:
                blockers.append("recent_proposal_suppression")

            room_available = held or max_active is None or active_count < max_active
            if room_available:
                if blockers:
                    decision = "almost_ready"
                    decision_reason = "Meets core review thresholds, but anti-churn checks still require more patience."
                else:
                    decision = "recommend_for_enable"
                    decision_reason = (
                        "Already held and merits allowlist review."
                        if held else
                        "Meets review thresholds with available satellite room."
                    )
            else:
                weakest_score = _safe_float(_safe_dict(weakest_active).get("net_score")) if weakest_active else None
                replacement_candidate_id = str(_safe_dict(weakest_active).get("product_id") or "").strip() if weakest_active else ""
                replacement_score_delta = round(net_score - weakest_score, 2) if weakest_score is not None else None

                if replacement_candidate_id and replacement_candidate_id in recent_replacement_targets:
                    blockers.append("replacement_cooldown")
                if replacement_score_delta is None or replacement_score_delta < REPLACEMENT_SCORE_DELTA_THRESHOLD:
                    blockers.append("insufficient_replacement_gap")

                if blockers:
                    decision = "almost_ready"
                    decision_reason = "Review quality is strong, but a clean replacement case is not ready yet."
                else:
                    decision = "recommend_replacement"
                    replacement_target = replacement_candidate_id or None
                    decision_reason = "Meaningfully out-ranks the weakest replaceable active satellite."

        if decision == "ignore" and active_buy_universe:
            decision_reason = "Already represented in the live active universe."

        blockers = sorted({str(item or "").strip() for item in blockers if str(item or "").strip()})
        decision_confidence = (
            "high" if decision in {"recommend_for_enable", "recommend_replacement"} and confidence_band == "high"
            else "medium" if decision in {"recommend_for_enable", "recommend_replacement", "almost_ready"}
            else "low"
        )

        decisions.append(
            {
                **row,
                "decision": decision,
                "decision_reason": decision_reason,
                "decision_blockers": blockers,
                "decision_confidence": decision_confidence,
                "replacement_target": replacement_target,
                "replacement_score_delta": replacement_score_delta,
                "stability_hits": stability["stability_hits"],
                "stability_window_cycles": stability["stability_window_cycles"],
                **portfolio_context,
            }
        )

    counts = {
        "recommend_for_enable": 0,
        "recommend_replacement": 0,
        "almost_ready": 0,
        "blocked": 0,
        "ignore": 0,
    }
    for item in decisions:
        counts[item["decision"]] = counts.get(item["decision"], 0) + 1

    decisions.sort(
        key=lambda row: (
            _decision_priority(row.get("decision")),
            _safe_float(row.get("net_score")),
        ),
        reverse=True,
    )

    return {
        "items": decisions,
        "summary": counts,
    }
