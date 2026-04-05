let ASSETS = [];
let FILTERED_ASSETS = [];
let ALLOWED_SATELLITES = [];
let BLOCKED_SATELLITES = [];
let CORE_ASSETS = [];
let HOLDINGS_BY_PRODUCT = {};
let TOTAL_ASSET_VALUE_USD = 0;
let ACTIVE_PRESET = "";
let URL_PRESET_APPLIED = false;
let CURRENT_CONFIG = {};
let LATEST_PROPOSAL = null;
let LATEST_AUTOMATION_MESSAGE = "";
let RECENT_PROPOSALS = [];
let LAST_CONFIGURATION_REFRESH_AT = 0;
const PERCENT_FIELD_IDS = [
  "satellite_total_target",
  "satellite_total_max",
  "min_cash_reserve"
];
const PROPOSAL_GENERATION_MODES = ["manual", "auto"];
const PROPOSAL_APPLY_MODES = ["manual", "after_approval"];
const PROPOSAL_MIN_CONFIDENCE_VALUES = ["medium", "high"];

const CONFIG_PRESETS = {
  conservative: {
    label: "Conservative",
    values: {
      satellite_total_target: 0.20,
      satellite_total_max: 0.30,
      min_cash_reserve: 0.20,
      trade_min_value_usd: 50,
      max_active_satellites: 4,
      max_new_satellites_per_cycle: 1
    }
  },
  balanced: {
    label: "Balanced",
    values: {
      satellite_total_target: 0.35,
      satellite_total_max: 0.45,
      min_cash_reserve: 0.10,
      trade_min_value_usd: 25,
      max_active_satellites: 6,
      max_new_satellites_per_cycle: 2
    }
  },
  aggressive: {
    label: "Aggressive",
    values: {
      satellite_total_target: 0.50,
      satellite_total_max: 0.60,
      min_cash_reserve: 0.05,
      trade_min_value_usd: 15,
      max_active_satellites: 10,
      max_new_satellites_per_cycle: 4
    }
  }
};

const URL_PARAMS = new URLSearchParams(window.location.search);
const API_SECRET =
  (window.CONFIGURATION_CONFIG && window.CONFIGURATION_CONFIG.apiSecret) ||
  URL_PARAMS.get("secret") ||
  "";

function authUrl(path) {
  if (!API_SECRET) return path;
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}secret=${encodeURIComponent(API_SECRET)}`;
}

async function fetchJson(path, options = {}, timeoutMs = 30000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(authUrl(path), {
      credentials: "same-origin",
      ...options,
      signal: controller.signal
    });

    const text = await res.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      throw new Error(`Invalid JSON response from ${path}`);
    }

    if (!res.ok || data.ok === false) {
      throw new Error(data.error || `HTTP ${res.status}`);
    }

    return data;
  } catch (err) {
    if (err.name === "AbortError") {
      throw new Error(`Request timed out for ${path}`);
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

function escapeHtml(v) {
  return String(v || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatUsd(v) {
  const n = Number(v || 0);
  return `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatPct(v) {
  const n = Number(v || 0) * 100;
  return `${n.toFixed(2)}%`;
}

function formatSignedPct(v) {
  const n = Number(v || 0) * 100;
  const prefix = n > 0 ? "+" : "";
  return `${prefix}${n.toFixed(2)}%`;
}

function hasNumericValue(value) {
  return value !== null && value !== undefined && Number.isFinite(Number(value));
}

function setStatus(message, isError = false) {
  const el = document.getElementById("configStatus");
  if (!el) return;
  el.textContent = message;
  el.className = isError ? "status-console error" : "status-console";
}

function setPresetStatus(message) {
  const el = document.getElementById("presetStatus");
  if (!el) return;
  el.textContent = message;
}

function isPercentFieldId(id) {
  return PERCENT_FIELD_IDS.includes(String(id || "").trim());
}

function normalizeProposalAutomationMode(value, allowedValues, fallback) {
  const normalized = String(value || "").trim().toLowerCase();
  return allowedValues.includes(normalized) ? normalized : fallback;
}

function titleCase(value) {
  return String(value || "")
    .split(/[\s_-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function snapshotCurrentConfigForm() {
  const generationMode = normalizeProposalAutomationMode(
    document.getElementById("config_proposal_generation_mode")?.value,
    PROPOSAL_GENERATION_MODES,
    "manual"
  );
  const applyMode = normalizeProposalAutomationMode(
    document.getElementById("config_proposal_apply_mode")?.value,
    PROPOSAL_APPLY_MODES,
    "manual"
  );
  const minConfidence = normalizeProposalAutomationMode(
    document.getElementById("config_proposal_min_confidence")?.value,
    PROPOSAL_MIN_CONFIDENCE_VALUES,
    "high"
  );

  return {
    satellite_total_target: normalizePercentInput("satellite_total_target"),
    satellite_total_max: normalizePercentInput("satellite_total_max"),
    min_cash_reserve: normalizePercentInput("min_cash_reserve"),
    trade_min_value_usd: document.getElementById("trade_min_value_usd")?.value || "",
    max_active_satellites: document.getElementById("max_active_satellites")?.value || "",
    max_new_satellites_per_cycle: document.getElementById("max_new_satellites_per_cycle")?.value || "",
    config_proposal_generation_mode: generationMode,
    config_proposal_apply_mode: applyMode,
    config_proposal_min_confidence: minConfidence
  };
}

function summarizeConfigChanges(previousConfig, nextConfig) {
  const formatValue = (value, type) => {
    if (type === "percent") return hasNumericValue(value) ? formatPct(value) : "—";
    if (type === "usd") return hasNumericValue(value) ? formatUsd(value) : "—";
    if (type === "integer") return hasNumericValue(value) ? String(Math.round(Number(value))) : "—";
    if (type === "mode") {
      const normalized = String(value || "").trim().toLowerCase();
      if (!normalized) return "—";
      if (normalized === "manual") return "Manual";
      if (normalized === "auto") return "Auto";
      if (normalized === "after_approval") return "After Approval";
      if (normalized === "medium") return "Medium";
      if (normalized === "high") return "High";
      return titleCase(normalized);
    }
    return value === null || value === undefined || value === "" ? "—" : String(value);
  };

  const defs = [
    ["satellite_total_target", "Satellite target", "percent"],
    ["satellite_total_max", "Satellite max", "percent"],
    ["min_cash_reserve", "Reserve", "percent"],
    ["trade_min_value_usd", "Trade floor", "usd"],
    ["max_active_satellites", "Active satellites", "integer"],
    ["max_new_satellites_per_cycle", "New satellites per cycle", "integer"],
    ["config_proposal_generation_mode", "Recommendation drafting", "mode"],
    ["config_proposal_apply_mode", "After approval", "mode"],
    ["config_proposal_min_confidence", "Minimum confidence", "mode"]
  ];

  const changes = [];
  for (const [key, label, type] of defs) {
    const prevRaw = previousConfig?.[key];
    const nextRaw = nextConfig?.[key];
    const prevValue = hasNumericValue(prevRaw) ? Number(prevRaw) : prevRaw;
    const nextValue = hasNumericValue(nextRaw) ? Number(nextRaw) : nextRaw;
    if (String(prevValue ?? "") === String(nextValue ?? "")) continue;
    changes.push(`${label}: ${formatValue(prevValue, type)} → ${formatValue(nextValue, type)}`);
  }

  if (!changes.length) {
    return "No meaningful changes were saved.";
  }

  const visibleChanges = changes.slice(0, 4).join(" • ");
  if (changes.length > 4) {
    return `${visibleChanges} • +${changes.length - 4} more`;
  }
  return visibleChanges;
}

function overviewStateTone(state) {
  const normalized = String(state || "").toLowerCase();
  if (normalized === "healthy") return "positive";
  if (normalized === "constrained") return "warn";
  if (normalized === "restricted") return "negative";
  return "";
}

function clampPercentValue(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.min(100, Math.max(0, n));
}

function formatPercentDisplayValue(value) {
  const clamped = clampPercentValue(value);
  return clamped == null ? "" : clamped.toFixed(2);
}

function setPercentFieldValue(id, percentValue) {
  const el = document.getElementById(id);
  if (!el) return;

  const n = Number(percentValue);
  el.value = Number.isFinite(n) ? formatPercentDisplayValue(n) : "";
}

function setPercentFieldDecimalValue(id, decimalValue) {
  const el = document.getElementById(id);
  if (!el) return;

  const n = Number(decimalValue);
  el.value = Number.isFinite(n) ? formatPercentDisplayValue(n * 100) : "";
}

function normalizePercentFieldInput(id) {
  const el = document.getElementById(id);
  if (!el) return;

  const raw = String(el.value || "").trim();
  if (!raw) return;
  el.value = formatPercentDisplayValue(raw);
}

function bindPercentFieldBehavior() {
  PERCENT_FIELD_IDS.forEach((id) => {
    const el = document.getElementById(id);
    if (!el || el.dataset.percentBound === "true") return;

    el.dataset.percentBound = "true";
    el.addEventListener("blur", () => {
      normalizePercentFieldInput(id);
    });
  });
}

function safeObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function cleanTextList(value, limit = 3) {
  const out = [];
  for (const item of Array.isArray(value) ? value : []) {
    const text = String(item || "").trim();
    if (text && !out.includes(text)) out.push(text);
    if (out.length >= limit) break;
  }
  return out;
}

function normalizeActionPayload(action, fallbackLabel = "Open Config") {
  const source = safeObject(action);
  const label = String(source.label || fallbackLabel).trim() || fallbackLabel;
  const target = String(source.target || "").trim();
  const section = String(source.section || "").trim();
  return { label, target, section };
}

function normalizeAdaptiveSuggestionsPayload(value) {
  const source = safeObject(value);
  const priorityRaw = String(source.priority || "moderate").trim().toLowerCase();
  const priority = ["low", "moderate", "high"].includes(priorityRaw) ? priorityRaw : "moderate";
  const suggestions = [];

  for (const item of Array.isArray(source.suggestions) ? source.suggestions : []) {
    const raw = safeObject(item);
    const title = String(raw.title || "").trim();
    const detail = String(raw.detail || "").trim();
    if (!title && !detail) continue;
    const normalized = {
      title: title || "Suggestion",
      detail: detail || "Review the current portfolio posture in configuration before making manual changes."
    };
    const action = normalizeActionPayload(raw.action, "Adjust In Config");
    if (action.target) normalized.action = action;
    suggestions.push(normalized);
    if (suggestions.length >= 3) break;
  }

  return {
    summary: String(source.summary || "").trim(),
    priority,
    suggestions,
    notes: cleanTextList(source.notes, 2)
  };
}

function normalizeAutoAdaptivePayload(value) {
  const source = safeObject(value);
  const confidenceRaw = String(source.confidence || "low").trim().toLowerCase();
  const confidence = ["low", "medium", "high"].includes(confidenceRaw) ? confidenceRaw : "low";
  const action = normalizeActionPayload(source.action, "Stage Recommended Preset");
  const simulationRaw = safeObject(source.simulation);
  const changedControls = [];

  for (const item of Array.isArray(simulationRaw.changed_controls) ? simulationRaw.changed_controls : []) {
    const raw = safeObject(item);
    const label = String(raw.label || "").trim();
    if (!label) continue;
    changedControls.push({
      label,
      current_value: raw.current_value,
      projected_value: raw.projected_value,
      format: String(raw.format || "text").trim() || "text"
    });
    if (changedControls.length >= 4) break;
  }

  return {
    label: String(source.label || "Balanced").trim() || "Balanced",
    confidence,
    summary: String(source.summary || "").trim(),
    reasons: cleanTextList(source.reasons, 3),
    action,
    simulation: {
      current_score: hasNumericValue(simulationRaw.current_score) ? Number(simulationRaw.current_score) : null,
      projected_score: hasNumericValue(simulationRaw.projected_score) ? Number(simulationRaw.projected_score) : null,
      score_delta: hasNumericValue(simulationRaw.score_delta) ? Number(simulationRaw.score_delta) : null,
      projected_band: String(simulationRaw.projected_band || "Projected band pending").trim() || "Projected band pending",
      summary: String(simulationRaw.summary || "").trim(),
      changed_controls: changedControls,
      notes: cleanTextList(simulationRaw.notes, 2)
    }
  };
}

function normalizeConfigProposalRecord(value) {
  const source = safeObject(value);
  const proposal = safeObject(source.proposal);
  const proposalSource = safeObject(proposal.source);
  const simulation = safeObject(proposal.simulation);
  const changes = [];
  const candidates = [];

  for (const item of Array.isArray(proposal.changes) ? proposal.changes : []) {
    const raw = safeObject(item);
    const key = String(raw.key || "").trim();
    const label = String(raw.label || key || "Control").trim();
    if (!label) continue;
    changes.push({
      key,
      label,
      current_value: raw.current_value,
      proposed_value: raw.proposed_value,
      kind: String(raw.kind || "float").trim() || "float",
      format: String(raw.format || "text").trim() || "text"
    });
  }

  for (const item of Array.isArray(proposal.candidates) ? proposal.candidates : []) {
    const raw = safeObject(item);
    const productId = String(raw.product_id || "").trim();
    if (!productId) continue;
    candidates.push({
      product_id: productId,
      net_score: hasNumericValue(raw.net_score) ? Number(raw.net_score) : null,
      confidence_band: String(raw.confidence_band || "").trim(),
      liquidity_bucket: String(raw.liquidity_bucket || "").trim(),
      volatility_bucket: String(raw.volatility_bucket || "").trim(),
      shadow_eligible: Boolean(raw.shadow_eligible),
      shadow_eligibility_reason: String(raw.shadow_eligibility_reason || "").trim(),
      shadow_block_reason: String(raw.shadow_block_reason || "").trim()
    });
  }

  return {
    id: String(source.id || "").trim(),
    status: String(source.status || "pending").trim().toLowerCase() || "pending",
    summary_text: String(source.summary_text || proposal.summary || "").trim(),
    created_at: String(source.created_at || "").trim(),
    expires_at: String(source.expires_at || "").trim(),
    approved_at: String(source.approved_at || "").trim(),
    approved_by: String(source.approved_by || "").trim(),
    rejected_at: String(source.rejected_at || "").trim(),
    rejected_by: String(source.rejected_by || "").trim(),
    applied_at: String(source.applied_at || "").trim(),
    applied_by: String(source.applied_by || "").trim(),
    expired_at: String(source.expired_at || "").trim(),
    superseded_at: String(source.superseded_at || "").trim(),
    proposal: {
      proposal_type: String(proposal.proposal_type || "config_guardrail").trim() || "config_guardrail",
      source: {
        advisory_range: String(proposalSource.advisory_range || "").trim(),
        risk_score: hasNumericValue(proposalSource.risk_score) ? Number(proposalSource.risk_score) : null,
        risk_band: String(proposalSource.risk_band || "").trim(),
        recommended_preset: String(proposalSource.recommended_preset || "").trim(),
        recommended_label: String(proposalSource.recommended_label || "").trim(),
        confidence: String(proposalSource.confidence || "").trim().toLowerCase()
      },
      summary: String(proposal.summary || "").trim(),
      reasons: cleanTextList(proposal.reasons, 3),
      candidates,
      changes,
      simulation: {
        current_score: hasNumericValue(simulation.current_score) ? Number(simulation.current_score) : null,
        projected_score: hasNumericValue(simulation.projected_score) ? Number(simulation.projected_score) : null,
        score_delta: hasNumericValue(simulation.score_delta) ? Number(simulation.score_delta) : null,
        current_band: String(simulation.current_band || "").trim(),
        projected_band: String(simulation.projected_band || "").trim(),
        summary: String(simulation.summary || "").trim()
      }
    }
  };
}

function formatProposalDate(value) {
  const text = String(value || "").trim();
  if (!text) return "—";
  const dt = new Date(text);
  if (Number.isNaN(dt.getTime())) return "—";
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const month = months[dt.getUTCMonth()] || "—";
  const day = dt.getUTCDate();
  const year = dt.getUTCFullYear();
  const hour = String(dt.getUTCHours()).padStart(2, "0");
  const minute = String(dt.getUTCMinutes()).padStart(2, "0");
  return `${month} ${day}, ${year} ${hour}:${minute} UTC`;
}

function formatProposalRisk(score, band) {
  const scoreText = hasNumericValue(score) ? String(Math.round(Number(score))) : "";
  const bandText = String(band || "").trim();
  if (!scoreText && !bandText) return "—";
  if (!scoreText) return bandText || "—";
  if (!bandText) return scoreText;
  return `${scoreText} · ${bandText}`;
}

function formatProposalScoreDelta(value) {
  if (!hasNumericValue(value)) return "—";
  const n = Math.round(Number(value));
  return n > 0 ? `+${n}` : String(n);
}

function proposalStatusTone(status) {
  const normalized = String(status || "").toLowerCase();
  if (["pending", "approved", "applied", "rejected", "expired", "superseded"].includes(normalized)) {
    return normalized;
  }
  return "";
}

function proposalStatusLabel(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (!normalized) return "—";
  if (normalized === "pending") return "Pending Review";
  if (normalized === "approved") return "Approved";
  if (normalized === "applied") return "Applied";
  if (normalized === "rejected") return "Rejected";
  if (normalized === "expired") return "Expired";
  if (normalized === "superseded") return "Superseded";
  return titleCase(normalized);
}

function proposalStatusNote(status) {
  const normalized = String(status || "").toLowerCase();
  if (normalized === "approved") return "This proposal is approved and ready to apply.";
  if (normalized === "applied") return "This proposal has been applied to live configuration.";
  if (normalized === "rejected") return "This proposal was rejected and did not change live configuration.";
  if (normalized === "expired") return "This proposal expired before it was applied.";
  if (normalized === "superseded") return "This proposal was replaced by a newer recommendation.";
  return "This proposal is awaiting operator review.";
}

function proposalTypeLabel(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "satellite_enable_recommendation") return "Satellite Enable Recommendation";
  if (normalized === "config_guardrail") return "Config Guardrail";
  return titleCase(normalized || "unknown");
}

function setProposalAutomationMessageFromStatus(result) {
  const status = String(result?.status || "").trim().toLowerCase();
  if (status === "created" || status === "drafted") {
    const delivery = result?.notification_sent === false ? " Telegram delivery needs review." : "";
    const createdCount = Number(result?.created_count || 0);
    setProposalAutomationResult(`${createdCount || 1} review proposal${createdCount === 1 || !createdCount ? "" : "s"} created.${result.proposal_id ? ` Latest ${result.proposal_id}.` : ""}${delivery}`.trim());
    return;
  }
  if (status === "deduped" || status === "deduped_recent" || status === "already_matches_current_state") {
    setProposalAutomationResult(`Existing pending proposal ${result.proposal_id || ""} already matches the current advisory state.`.trim());
    return;
  }
  if (status === "confidence_below_threshold" || status === "skipped_low_confidence") {
    const required = String(result?.required_confidence || result?.min_confidence || "").trim();
    const actual = String(result?.confidence || "").trim();
    setProposalAutomationResult(`Auto-draft skipped because confidence ${actual || "current"} is below ${required || "the required"} threshold.`);
    return;
  }
  if (status === "manual_mode") {
    setProposalAutomationResult("Auto-draft is disabled because Draft Recommendations is set to Manual.");
    return;
  }
  if (status === "noop") {
    setProposalAutomationResult("No proposal was generated because no new guardrail or satellite review changes qualified.");
    return;
  }
  setProposalAutomationResult(`Proposal generation returned status: ${status || "unknown"}.`);
}

function setProposalActionResult(message = "", isError = false, sticky = false) {
  const el = document.getElementById("configProposalActionResult");
  if (!el) return;
  const text = String(message || "").trim();
  el.hidden = !text;
  el.textContent = text;
  el.className = isError ? "config-proposal-action-result error" : "config-proposal-action-result";
  if (sticky && text) {
    el.dataset.userMessage = "true";
  } else {
    delete el.dataset.userMessage;
  }
}

function relevantProposalEvent(proposal) {
  const item = normalizeConfigProposalRecord(proposal);
  if (item.applied_at) return { label: "Applied", value: item.applied_at };
  if (item.approved_at) return { label: "Approved", value: item.approved_at };
  if (item.rejected_at) return { label: "Rejected", value: item.rejected_at };
  if (item.expired_at) return { label: "Expired", value: item.expired_at };
  if (item.superseded_at) return { label: "Superseded", value: item.superseded_at };
  return { label: "Created", value: item.created_at };
}

function formatPercent(value) {
  if (!hasNumericValue(value)) return "—";
  return `${(Number(value) * 100).toFixed(2)}%`;
}

function formatUSD(value) {
  if (!hasNumericValue(value)) return "—";
  return `$${Number(value).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatInteger(value) {
  if (!hasNumericValue(value)) return "—";
  return Math.round(Number(value)).toLocaleString();
}

function renderProposalLatest(data) {
  const emptyEl = document.getElementById("configProposalEmptyState");
  const contentEl = document.getElementById("configProposalLatestContent");
  const proposal = data ? normalizeConfigProposalRecord(data) : null;
  LATEST_PROPOSAL = proposal && proposal.id ? proposal : null;

  if (!proposal || !proposal.id) {
    if (emptyEl) emptyEl.hidden = false;
    if (contentEl) contentEl.hidden = true;
    renderAutomationOverview();
    return;
  }

  if (emptyEl) emptyEl.hidden = true;
  if (contentEl) contentEl.hidden = false;

  const statusTone = proposalStatusTone(proposal.status);
  const isSatelliteEnableProposal = proposal.proposal.proposal_type === "satellite_enable_recommendation";
  const isPending = proposal.status === "pending";
  const isApproved = proposal.status === "approved";
  const statusEl = document.getElementById("configProposalStatus");
  if (statusEl) {
    statusEl.textContent = proposalStatusLabel(proposal.status || "—");
    statusEl.className = `config-proposal-status${statusTone ? ` ${statusTone}` : ""}`;
  }

  const changesHost = document.getElementById("configProposalChanges");
  if (changesHost) {
    const formatChangeValue = (value, format) => {
      if (format === "percent") return formatPercent(value);
      if (format === "usd") return formatUSD(value);
      if (format === "integer") return formatInteger(value);
      return value === null || value === undefined || value === "" ? "—" : String(value);
    };

    changesHost.innerHTML = isSatelliteEnableProposal
      ? (
        proposal.proposal.candidates.length
          ? proposal.proposal.candidates.map((item) => `
            <div class="config-proposal-change-row">
              <span class="config-proposal-change-label">${escapeHtml(item.product_id || "Candidate")}</span>
              <span class="config-proposal-change-values">
                ${escapeHtml(`Score ${hasNumericValue(item.net_score) ? Number(item.net_score).toFixed(1) : "—"} • ${item.confidence_band || "unknown"} confidence • ${item.liquidity_bucket || "unknown"} liquidity • ${item.volatility_bucket || "unknown"} volatility`)}
              </span>
            </div>
          `).join("")
          : `<div class="config-proposal-change-row">
              <span class="config-proposal-change-label">Recommended Candidates</span>
              <span class="config-proposal-change-values">—</span>
            </div>`
      )
      : proposal.proposal.changes.length
      ? proposal.proposal.changes.map((item) => `
        <div class="config-proposal-change-row">
          <span class="config-proposal-change-label">${escapeHtml(item.label || "Control")}</span>
          <span class="config-proposal-change-values">
            ${escapeHtml(formatChangeValue(item.current_value, item.format))} → ${escapeHtml(formatChangeValue(item.proposed_value, item.format))}
          </span>
        </div>
      `).join("")
      : `<div class="config-proposal-change-row">
          <span class="config-proposal-change-label">Changed Controls</span>
          <span class="config-proposal-change-values">—</span>
        </div>`;
  }

  const valueMap = {
    configProposalId: proposal.id || "—",
    configProposalSummary: proposal.summary_text || proposal.proposal.summary || "—",
    configProposalType: proposalTypeLabel(proposal.proposal.proposal_type),
    configProposalConfidence: proposal.proposal.source.confidence || "—",
    configProposalCurrentRisk: isSatelliteEnableProposal ? `${proposal.proposal.candidates.length || 0} review-ready` : formatProposalRisk(proposal.proposal.source.risk_score, proposal.proposal.source.risk_band),
    configProposalProjectedRisk: isSatelliteEnableProposal ? "Approval only" : formatProposalRisk(proposal.proposal.simulation.projected_score, proposal.proposal.simulation.projected_band),
    configProposalScoreDelta: formatProposalScoreDelta(proposal.proposal.simulation.score_delta),
    configProposalCreatedAt: formatProposalDate(proposal.created_at),
    configProposalExpiresAt: formatProposalDate(proposal.expires_at),
    configProposalApprovedBy: proposal.approved_by || "—",
    configProposalAppliedBy: proposal.applied_by || "—",
    configProposalNote: isSatelliteEnableProposal
      ? "This recommendation is approval-only and will not change the live allowlist until an explicit operator action is taken."
      : proposalStatusNote(proposal.status)
  };

  Object.entries(valueMap).forEach(([id, value]) => {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
  });

  const actionsEl = document.getElementById("configProposalActions");
  const approveBtn = document.getElementById("configProposalApproveBtn");
  const applyBtn = document.getElementById("configProposalApplyBtn");
  const rejectBtn = document.getElementById("configProposalRejectBtn");
  if (actionsEl) actionsEl.hidden = !(isPending || isApproved);
  if (approveBtn) {
    approveBtn.disabled = !isPending;
    approveBtn.hidden = !isPending;
    approveBtn.dataset.proposalId = proposal.id || "";
  }
  if (applyBtn) {
    applyBtn.disabled = !isApproved;
    applyBtn.hidden = !isApproved;
    applyBtn.dataset.proposalId = proposal.id || "";
  }
  if (rejectBtn) {
    rejectBtn.disabled = !isPending;
    rejectBtn.hidden = !isPending;
    rejectBtn.dataset.proposalId = proposal.id || "";
  }
  const actionResultEl = document.getElementById("configProposalActionResult");
  if (!(isPending || isApproved) && !(actionResultEl && actionResultEl.dataset.userMessage)) {
    setProposalActionResult("");
  }

  renderAutomationOverview();
}

function renderProposalHistory(items) {
  const host = document.getElementById("configProposalHistory");
  if (!host) return;

  const proposals = Array.isArray(items) ? items.map(normalizeConfigProposalRecord).filter((item) => item.id).slice(0, 5) : [];
  RECENT_PROPOSALS = proposals;
  if (!proposals.length) {
    host.innerHTML = `<div class="config-proposal-history-empty">No recent proposal history yet.</div>`;
    renderAutomationOverview();
    return;
  }

  host.innerHTML = proposals.map((proposal) => {
    const event = relevantProposalEvent(proposal);
    const tone = proposalStatusTone(proposal.status);
    return `
      <div class="config-proposal-history-row">
        <div class="config-proposal-history-main">
          <div class="config-proposal-history-top">
            <span class="config-proposal-id">${escapeHtml(proposal.id)}</span>
            <span class="config-proposal-status ${escapeHtml(tone)}">${escapeHtml(proposalStatusLabel(proposal.status))}</span>
            <span class="config-proposal-history-type">${escapeHtml(proposalTypeLabel(proposal.proposal.proposal_type))}</span>
          </div>
          <div class="config-proposal-history-summary">${escapeHtml(proposal.summary_text || "—")}</div>
        </div>
        <div class="config-proposal-history-meta">
          <div><span class="config-proposal-label">Created</span><span class="config-proposal-value">${escapeHtml(formatProposalDate(proposal.created_at))}</span></div>
          <div><span class="config-proposal-label">${escapeHtml(event.label)}</span><span class="config-proposal-value">${escapeHtml(formatProposalDate(event.value))}</span></div>
        </div>
      </div>
    `;
  }).join("");
  renderAutomationOverview();
}

function priorityTone(priority) {
  if (priority === "low") return "positive";
  if (priority === "high") return "negative";
  return "warn";
}

function confidenceTone(confidence) {
  if (confidence === "high") return "positive";
  if (confidence === "medium") return "warn";
  return "negative";
}

function riskBandTone(band) {
  const normalized = String(band || "").toLowerCase();
  if (normalized === "low risk") return "positive";
  if (normalized === "elevated risk") return "warn";
  if (normalized === "high risk") return "negative";
  return "";
}

function fmtControlValue(value, format = "text") {
  if (value === null || value === undefined || value === "") return "—";
  if (format === "percent") return formatPercent(value);
  if (format === "usd") return formatUSD(value);
  if (format === "integer") return formatInteger(value);
  return String(value);
}

function configFocusUrl(action) {
  const params = new URLSearchParams();
  if (action?.target) params.set("focus", action.target);
  if (action?.section) params.set("section", action.section);
  if (action?.label) params.set("source_action", action.label);
  if (API_SECRET) params.set("secret", API_SECRET);
  const query = params.toString();
  return `/configuration${query ? `?${query}` : ""}`;
}

function configPresetUrl(action, label) {
  const params = new URLSearchParams();
  if (action?.target) params.set("preset", action.target);
  params.set("preset_source", String(label || "Auto-Adaptive Mode"));
  params.set("section", "core-controls");
  params.set("focus", "satellite_total_target");
  if (API_SECRET) params.set("secret", API_SECRET);
  const query = params.toString();
  return `/configuration${query ? `?${query}` : ""}`;
}

function renderConfigurationAdvisory(data) {
  const adaptiveSuggestions = normalizeAdaptiveSuggestionsPayload(data?.adaptive_suggestions);
  const autoAdaptive = normalizeAutoAdaptivePayload(data?.auto_adaptive);

  const adaptiveCard = document.getElementById("configAdaptiveSuggestionsCard");
  const adaptivePriorityEl = document.getElementById("configAdaptiveSuggestionsPriority");
  const adaptiveSummaryEl = document.getElementById("configAdaptiveSuggestionsSummary");
  const adaptiveListEl = document.getElementById("configAdaptiveSuggestionsList");
  const adaptiveNotesEl = document.getElementById("configAdaptiveSuggestionsNotes");

  if (adaptiveCard) {
    adaptiveCard.className = `adaptive-suggestions-card${priorityTone(adaptiveSuggestions.priority) ? ` ${priorityTone(adaptiveSuggestions.priority)}` : ""}`;
  }
  if (adaptivePriorityEl) {
    adaptivePriorityEl.textContent = `${adaptiveSuggestions.priority} priority`;
    adaptivePriorityEl.className = `adaptive-suggestions-priority${priorityTone(adaptiveSuggestions.priority) ? ` ${priorityTone(adaptiveSuggestions.priority)}` : ""}`;
  }
  if (adaptiveSummaryEl) {
    adaptiveSummaryEl.textContent = adaptiveSuggestions.summary || "Advisory guidance is being assembled from the current portfolio and analytics inputs.";
  }
  if (adaptiveListEl) {
    const fallbackItems = [
      {
        title: "Stay measured",
        detail: "The advisory layer is read-only and highlights only the most defensible next considerations."
      }
    ];
    const items = adaptiveSuggestions.suggestions.length ? adaptiveSuggestions.suggestions : fallbackItems;
    adaptiveListEl.innerHTML = items.map((item) => `
      <div class="adaptive-suggestion-item">
        <div class="adaptive-suggestion-title">${escapeHtml(item.title || "Suggestion")}</div>
        <div class="adaptive-suggestion-detail">${escapeHtml(item.detail || "")}</div>
        ${item.action && item.action.target ? `
          <div class="adaptive-suggestion-actions">
            <a class="btn btn-secondary adaptive-suggestion-link" href="${escapeHtml(configFocusUrl(item.action))}">
              ${escapeHtml(item.action.label || "Adjust In Config")}
            </a>
          </div>
        ` : ""}
      </div>
    `).join("");
  }
  if (adaptiveNotesEl) {
    adaptiveNotesEl.innerHTML = adaptiveSuggestions.notes.map((note) => `
      <span class="adaptive-suggestion-note">${escapeHtml(note)}</span>
    `).join("");
  }

  const autoAdaptiveCard = document.getElementById("configAutoAdaptiveCard");
  const autoAdaptivePresetEl = document.getElementById("configAutoAdaptivePreset");
  const autoAdaptiveConfidenceEl = document.getElementById("configAutoAdaptiveConfidence");
  const autoAdaptiveSummaryEl = document.getElementById("configAutoAdaptiveSummary");
  const autoAdaptiveReasonsEl = document.getElementById("configAutoAdaptiveReasons");
  const autoAdaptiveSimulationEl = document.getElementById("configAutoAdaptiveSimulation");
  const autoAdaptiveSimulationBandEl = document.getElementById("configAutoAdaptiveSimulationBand");
  const autoAdaptiveProjectionLineEl = document.getElementById("configAutoAdaptiveProjectionLine");
  const autoAdaptiveSimulationSummaryEl = document.getElementById("configAutoAdaptiveSimulationSummary");
  const autoAdaptiveChangedControlsEl = document.getElementById("configAutoAdaptiveChangedControls");
  const autoAdaptiveSimulationNotesEl = document.getElementById("configAutoAdaptiveSimulationNotes");
  const autoAdaptiveActionsEl = document.getElementById("configAutoAdaptiveActions");

  if (autoAdaptiveCard) {
    autoAdaptiveCard.className = `auto-adaptive-card${confidenceTone(autoAdaptive.confidence) ? ` ${confidenceTone(autoAdaptive.confidence)}` : ""}`;
  }
  if (autoAdaptivePresetEl) {
    autoAdaptivePresetEl.textContent = `${autoAdaptive.label} preset recommended`;
  }
  if (autoAdaptiveConfidenceEl) {
    autoAdaptiveConfidenceEl.textContent = `${autoAdaptive.confidence} confidence`;
    autoAdaptiveConfidenceEl.className = `auto-adaptive-confidence${confidenceTone(autoAdaptive.confidence) ? ` ${confidenceTone(autoAdaptive.confidence)}` : ""}`;
  }
  if (autoAdaptiveSummaryEl) {
    autoAdaptiveSummaryEl.textContent = autoAdaptive.summary || "Recommendation-only intelligence is evaluating the current portfolio posture.";
  }
  if (autoAdaptiveReasonsEl) {
    const reasons = autoAdaptive.reasons.length ? autoAdaptive.reasons : ["Recommendation confidence is conservative until more portfolio context is available."];
    autoAdaptiveReasonsEl.innerHTML = reasons.map((reason) => `
      <div class="auto-adaptive-reason">${escapeHtml(reason)}</div>
    `).join("");
  }
  if (autoAdaptiveSimulationEl) {
    autoAdaptiveSimulationEl.className = `auto-adaptive-simulation${riskBandTone(autoAdaptive.simulation.projected_band) ? ` ${riskBandTone(autoAdaptive.simulation.projected_band)}` : ""}`;
  }
  if (autoAdaptiveSimulationBandEl) {
    autoAdaptiveSimulationBandEl.textContent = autoAdaptive.simulation.projected_band;
    autoAdaptiveSimulationBandEl.className = `auto-adaptive-simulation-band${riskBandTone(autoAdaptive.simulation.projected_band) ? ` ${riskBandTone(autoAdaptive.simulation.projected_band)}` : ""}`;
  }
  if (autoAdaptiveProjectionLineEl) {
    const currentText = autoAdaptive.simulation.current_score == null ? "--" : Math.round(autoAdaptive.simulation.current_score);
    const projectedText = autoAdaptive.simulation.projected_score == null ? "--" : Math.round(autoAdaptive.simulation.projected_score);
    const delta = autoAdaptive.simulation.score_delta;
    const deltaText =
      delta == null ? "no score change projected" :
      delta === 0 ? "no score change projected" :
      `${delta > 0 ? "+" : ""}${Math.round(delta)} points projected`;
    autoAdaptiveProjectionLineEl.textContent = `Current score ${currentText} → projected score ${projectedText} • ${deltaText}`;
  }
  if (autoAdaptiveSimulationSummaryEl) {
    autoAdaptiveSimulationSummaryEl.textContent =
      autoAdaptive.simulation.summary || "This is a projected guardrail simulation only. Nothing is applied automatically.";
  }
  if (autoAdaptiveChangedControlsEl) {
    autoAdaptiveChangedControlsEl.innerHTML = autoAdaptive.simulation.changed_controls.map((item) => `
      <div class="auto-adaptive-control-chip">
        <span class="auto-adaptive-control-label">${escapeHtml(item.label || "Control")}</span>
        <span class="auto-adaptive-control-values">
          ${escapeHtml(fmtControlValue(item.current_value, item.format))} → ${escapeHtml(fmtControlValue(item.projected_value, item.format))}
        </span>
      </div>
    `).join("");
  }
  if (autoAdaptiveSimulationNotesEl) {
    autoAdaptiveSimulationNotesEl.innerHTML = autoAdaptive.simulation.notes.map((note) => `
      <span class="auto-adaptive-note">${escapeHtml(note)}</span>
    `).join("");
  }
  if (autoAdaptiveActionsEl) {
    autoAdaptiveActionsEl.innerHTML = autoAdaptive.action && autoAdaptive.action.target ? `
      <a class="btn btn-secondary auto-adaptive-link" href="${escapeHtml(configPresetUrl(autoAdaptive.action, "Auto-Adaptive Mode"))}">
        ${escapeHtml(autoAdaptive.action.label || "Stage Recommended Preset")}
      </a>
    ` : "";
  }
}

function clearConfigFocusState() {
  document.querySelectorAll(".config-focus-active").forEach((el) => {
    el.classList.remove("config-focus-active");
  });
}

function openDetailsAncestors(node) {
  let current = node;
  while (current) {
    if (current.tagName === "DETAILS") {
      current.open = true;
    }
    current = current.parentElement;
  }
}

function safeCssValue(value) {
  if (window.CSS && typeof window.CSS.escape === "function") {
    return window.CSS.escape(value);
  }
  return String(value).replace(/["\\]/g, "\\$&");
}

function applyConfigurationFocus() {
  const focusTarget = String(URL_PARAMS.get("focus") || "").trim();
  const sectionTarget = String(URL_PARAMS.get("section") || "").trim();
  const sourceAction = String(URL_PARAMS.get("source_action") || "").trim();

  if (!focusTarget && !sectionTarget) return;

  const sectionEl = sectionTarget
    ? document.querySelector(`[data-config-section="${safeCssValue(sectionTarget)}"]`)
    : null;

  if (sectionEl && sectionEl.tagName === "DETAILS") {
    sectionEl.open = true;
  }

  const inputEl = focusTarget ? document.getElementById(focusTarget) : null;
  const targetEl =
    (focusTarget && document.querySelector(`[data-focus-target="${safeCssValue(focusTarget)}"]`)) ||
    inputEl ||
    sectionEl;

  if (!targetEl) return;

  openDetailsAncestors(sectionEl);
  openDetailsAncestors(targetEl);

  clearConfigFocusState();
  targetEl.classList.add("config-focus-active");

  if (sectionEl && sectionEl !== targetEl) {
    sectionEl.classList.add("config-focus-active");
  }

  window.setTimeout(() => {
    targetEl.scrollIntoView({ behavior: "smooth", block: "center" });
    if (inputEl && typeof inputEl.focus === "function") {
      inputEl.focus({ preventScroll: true });
      if (typeof inputEl.select === "function") {
        inputEl.select();
      }
    }
    if (sourceAction) {
      setStatus(`${sourceAction} opened the relevant configuration control. Review manually before saving.`);
    }
  }, 120);

  window.setTimeout(() => {
    clearConfigFocusState();
  }, 2600);
}

function updateConfigurationSummary() {
  const modeEl = document.getElementById("assetModeSummary");
  if (!modeEl) return;

  const enabledCount = ALLOWED_SATELLITES.length;
  const blockedCount = BLOCKED_SATELLITES.length;
  const coreCount = CORE_ASSETS.length;
  modeEl.textContent = `enabled ${enabledCount} • blocked ${blockedCount} • core ${coreCount}`;
}

function setInputValue(id, value, percent = false) {
  if (percent) {
    setPercentFieldDecimalValue(id, value);
    return;
  }

  if (isPercentFieldId(id)) {
    setPercentFieldValue(id, value);
    return;
  }

  const el = document.getElementById(id);
  if (!el) return;
  el.value = value ?? "";
}

function setPresetActiveState(name) {
  ACTIVE_PRESET = String(name || "");
  document.querySelectorAll(".config-preset-btn").forEach((button) => {
    const isActive = button.dataset.preset === ACTIVE_PRESET;
    button.classList.toggle("active", isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
}

function renderRiskConfig(cfg) {
  setInputValue("satellite_total_max", cfg.satellite_total_max, true);
  setInputValue("satellite_total_target", cfg.satellite_total_target, true);
  setInputValue("min_cash_reserve", cfg.min_cash_reserve, true);
  setInputValue("trade_min_value_usd", cfg.trade_min_value_usd, false);
  setInputValue("max_active_satellites", cfg.max_active_satellites, false);
  setInputValue("max_new_satellites_per_cycle", cfg.max_new_satellites_per_cycle, false);
  setPresetActiveState("");
  setPresetStatus("No preset staged. Manual edits remain available.");
}

function renderProposalAutomationSettings(cfg) {
  const generationEl = document.getElementById("config_proposal_generation_mode");
  const applyEl = document.getElementById("config_proposal_apply_mode");
  const minConfidenceEl = document.getElementById("config_proposal_min_confidence");

  const generationMode = normalizeProposalAutomationMode(
    cfg?.config_proposal_generation_mode,
    PROPOSAL_GENERATION_MODES,
    "manual"
  );
  const applyMode = normalizeProposalAutomationMode(
    cfg?.config_proposal_apply_mode,
    PROPOSAL_APPLY_MODES,
    "manual"
  );
  const minConfidence = normalizeProposalAutomationMode(
    cfg?.config_proposal_min_confidence,
    PROPOSAL_MIN_CONFIDENCE_VALUES,
    "high"
  );

  if (generationEl) generationEl.value = generationMode;
  if (applyEl) applyEl.value = applyMode;
  if (minConfidenceEl) minConfidenceEl.value = minConfidence;
}

function renderAutomationOverview() {
  const cfg = CURRENT_CONFIG || {};
  const generationMode = normalizeProposalAutomationMode(
    cfg?.config_proposal_generation_mode,
    PROPOSAL_GENERATION_MODES,
    document.getElementById("config_proposal_generation_mode")?.value || "manual"
  );
  const applyMode = normalizeProposalAutomationMode(
    cfg?.config_proposal_apply_mode,
    PROPOSAL_APPLY_MODES,
    document.getElementById("config_proposal_apply_mode")?.value || "manual"
  );
  const minConfidence = normalizeProposalAutomationMode(
    cfg?.config_proposal_min_confidence,
    PROPOSAL_MIN_CONFIDENCE_VALUES,
    document.getElementById("config_proposal_min_confidence")?.value || "high"
  );

  const tradingMode =
    ASSETS.length
      ? `${ASSETS.length} markets • ${ALLOWED_SATELLITES.length} enabled • ${BLOCKED_SATELLITES.length} blocked • ${CORE_ASSETS.length} core`
      : "Reviewing tradable market coverage";
  const automationStatus =
    generationMode === "auto"
      ? `Auto proposals live • ${titleCase(minConfidence)} confidence minimum`
      : "Manual proposals only";
  const approvalPosture =
    applyMode === "after_approval"
      ? "Approval required • approved changes apply immediately"
      : "Approval required • approved changes wait for confirmation";

  const target = cfg?.satellite_total_target;
  const max = cfg?.satellite_total_max;
  const reserve = cfg?.min_cash_reserve;
  const tradeFloor = cfg?.trade_min_value_usd;
  const guardrails = [
    hasNumericValue(target) ? `Satellite Target ${formatPct(target)}` : "",
    hasNumericValue(max) ? `Satellite Max ${formatPct(max)}` : "",
    hasNumericValue(reserve) ? `Cash Reserve ${formatPct(reserve)}` : "",
    hasNumericValue(tradeFloor) ? `Minimum Trade ${formatUsd(tradeFloor)}` : ""
  ].filter(Boolean).join(" • ") || "Reviewing current operating limits";

  const pendingCount = RECENT_PROPOSALS.filter((proposal) => proposal.status === "pending").length;
  let automationState = "constrained";
  if (generationMode === "auto" && minConfidence === "medium") {
    automationState = "healthy";
  } else if (generationMode === "manual") {
    automationState = "constrained";
  }

  let approvalState = applyMode === "manual" ? "healthy" : "constrained";

  let guardrailState = "healthy";
  if ((hasNumericValue(reserve) && Number(reserve) >= 0.15) || (hasNumericValue(max) && Number(max) <= 0.35)) {
    guardrailState = "constrained";
  }
  if ((hasNumericValue(reserve) && Number(reserve) >= 0.25) || (hasNumericValue(max) && Number(max) <= 0.20)) {
    guardrailState = "restricted";
  }

  let systemConfidence = "Moderate confidence";
  let systemConfidenceState = "warn";
  if (guardrailState === "restricted") {
    systemConfidence = "Defensive posture";
    systemConfidenceState = "negative";
  } else if (pendingCount > 0) {
    systemConfidence = "Pending review";
    systemConfidenceState = "warn";
  } else if (generationMode === "auto" && applyMode === "manual" && minConfidence === "high") {
    systemConfidence = "High confidence";
    systemConfidenceState = "positive";
  } else if (generationMode === "manual") {
    systemConfidence = "Operator review mode";
    systemConfidenceState = "warn";
  }

  let latestAction = "No recent automation activity yet.";
  if (LATEST_PROPOSAL && LATEST_PROPOSAL.id) {
    const event = relevantProposalEvent(LATEST_PROPOSAL);
    latestAction = `${titleCase(LATEST_PROPOSAL.status || "pending")} • ${formatProposalDate(event.value)}`;
  } else if (LATEST_AUTOMATION_MESSAGE) {
    latestAction = LATEST_AUTOMATION_MESSAGE;
  }

  let nextAction = "Generate a recommendation when you want a fresh automation review.";
  if (LATEST_PROPOSAL && LATEST_PROPOSAL.status === "pending") {
    nextAction = "A recommendation is waiting for review. Open the proposal details before any settings change.";
  } else if (LATEST_PROPOSAL && LATEST_PROPOSAL.status === "approved") {
    nextAction = applyMode === "after_approval"
      ? "The next approved recommendation should apply automatically after approval."
      : "Apply the approved recommendation when you are ready for the change to go live.";
  } else if (generationMode === "auto") {
    nextAction = "The system will draft a recommendation automatically when the advisory rules qualify a safe change.";
  }

  const valueMap = {
    configOverviewAutomationStatus: automationStatus,
    configOverviewTradingMode: tradingMode,
    configOverviewApprovalPosture: approvalPosture,
    configOverviewGuardrails: guardrails,
    configOverviewLastAction: latestAction,
    configOverviewNextAction: nextAction
  };

  Object.entries(valueMap).forEach(([id, value]) => {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
  });

  const toneMap = {
    configOverviewAutomationStatus: automationState,
    configOverviewApprovalPosture: approvalState,
    configOverviewGuardrails: guardrailState
  };

  Object.entries(toneMap).forEach(([id, state]) => {
    const el = document.getElementById(id);
    if (!el) return;
    const tone = overviewStateTone(state);
    el.className = `configuration-shell-value${tone ? ` ${tone}` : ""}`;
  });

  const badgeEl = document.getElementById("configOverviewPendingBadge");
  if (badgeEl) {
    if (pendingCount > 0) {
      badgeEl.hidden = false;
      badgeEl.textContent = `${pendingCount} pending recommendation${pendingCount === 1 ? "" : "s"} to review`;
      badgeEl.className = `configuration-shell-badge ${pendingCount > 1 ? "warn" : "accent"}`;
    } else {
      badgeEl.hidden = false;
      badgeEl.textContent = "No pending recommendations";
      badgeEl.className = "configuration-shell-badge positive";
    }
  }

  const confidenceEl = document.getElementById("configOverviewSystemConfidence");
  if (confidenceEl) {
    const tone = overviewStateTone(systemConfidenceState);
    confidenceEl.textContent = systemConfidence;
    confidenceEl.className = `configuration-shell-micro${tone ? ` ${tone}` : ""}`;
  }
}

function getAssetMode(productId) {
  if (CORE_ASSETS.includes(productId)) {
    return { key: "core", badge: '<span class="badge accent2">core</span>' };
  }
  if (BLOCKED_SATELLITES.includes(productId)) {
    return { key: "disable", badge: '<span class="badge bad">disable</span>' };
  }
  if (ALLOWED_SATELLITES.includes(productId)) {
    return { key: "enable", badge: '<span class="badge good">enable</span>' };
  }
  return { key: "auto", badge: '<span class="badge accent">auto</span>' };
}

function buildActionButtons(productId, modeKey) {
  if (modeKey === "core") {
    return `<span class="muted">Managed as core</span>`;
  }

  const safe = escapeHtml(productId);

  return `
    <div class="asset-mode-actions">
      <button class="btn ${modeKey === "enable" ? "btn-primary" : "btn-secondary"} asset-mode-btn" onclick="setAssetMode('${safe}','enable')">Enable</button>
      <button class="btn ${modeKey === "auto" ? "btn-primary" : "btn-secondary"} asset-mode-btn" onclick="setAssetMode('${safe}','auto')">Auto</button>
      <button class="btn ${modeKey === "disable" ? "btn-primary" : "btn-secondary"} asset-mode-btn" onclick="setAssetMode('${safe}','disable')">Disable</button>
    </div>
  `;
}

function updateAssetMeta() {
  const countEl = document.getElementById("assetUniverseCount");
  if (countEl) countEl.textContent = `${ASSETS.length} loaded`;

  const metaEl = document.getElementById("assetSearchMeta");
  if (metaEl) metaEl.textContent = `Showing ${FILTERED_ASSETS.length} of ${ASSETS.length} tradable USD assets`;
  renderAutomationOverview();
}

function drawAssetRows() {
  const tbody = document.getElementById("assetRows");
  if (!tbody) return;

  if (!FILTERED_ASSETS.length) {
    tbody.innerHTML = `<tr><td colspan="5" class="muted">No assets found.</td></tr>`;
    updateAssetMeta();
    return;
  }

  tbody.innerHTML = FILTERED_ASSETS.map((row) => {
    const productId = row.product_id;
    const mode = getAssetMode(productId);
    const heldValue = Number(HOLDINGS_BY_PRODUCT[productId] || 0);
    const weight = TOTAL_ASSET_VALUE_USD > 0 ? heldValue / TOTAL_ASSET_VALUE_USD : 0;

    return `
      <tr>
        <td><strong>${escapeHtml(productId)}</strong></td>
        <td>${mode.badge}</td>
        <td class="right">${formatUsd(heldValue)}</td>
        <td class="right">${formatPct(weight)}</td>
        <td>${buildActionButtons(productId, mode.key)}</td>
      </tr>
    `;
  }).join("");

  updateAssetMeta();
}

function applyAssetFilter() {
  const q = (document.getElementById("assetSearch")?.value || "").trim().toUpperCase();
  FILTERED_ASSETS = !q ? ASSETS.slice() : ASSETS.filter((row) => String(row.product_id || "").toUpperCase().includes(q));
  drawAssetRows();
}

async function loadPortfolioSnapshot() {
  try {
    const data = await fetchJson("/api/portfolio", {}, 30000);
    const snapshot = data.snapshot || {};
    const positionsObj = snapshot.positions || {};

    HOLDINGS_BY_PRODUCT = {};
    TOTAL_ASSET_VALUE_USD = 0;

    for (const [productIdRaw, pos] of Object.entries(positionsObj)) {
      const productId = String(productIdRaw || pos.product_id || "").toUpperCase();
      const value = Number(
        pos.value_total_usd ??
        pos.value_usd ??
        pos.usd_value ??
        pos.value ??
        pos.usd ??
        0
      );
      HOLDINGS_BY_PRODUCT[productId] = value;
      TOTAL_ASSET_VALUE_USD += value;
    }
  } catch (err) {
    console.warn("Portfolio snapshot failed:", err.message);
    HOLDINGS_BY_PRODUCT = {};
    TOTAL_ASSET_VALUE_USD = 0;
  }
}

async function loadAdvisoryState() {
  try {
    const data = await fetchJson("/api/portfolio/history?range=30d", {}, 30000);
    renderConfigurationAdvisory(data || {});
  } catch (err) {
    console.warn("Advisory load failed:", err.message);
    renderConfigurationAdvisory({});
  }
}

async function loadProposalState() {
  try {
    const [latest, recent] = await Promise.all([
      fetchJson("/api/config_proposals/latest", {}, 20000),
      fetchJson("/api/config_proposals/recent?limit=5", {}, 20000)
    ]);
    renderProposalLatest(latest?.proposal || null);
    renderProposalHistory(recent?.items || []);
  } catch (err) {
    console.warn("Proposal visibility load failed:", err.message);
    renderProposalLatest(null);
    renderProposalHistory([]);
  }
}

async function loadConfigState() {
  const cfgData = await fetchJson("/api/config", {}, 20000);
  const cfg = cfgData.config || {};
  CURRENT_CONFIG = cfg;
  ALLOWED_SATELLITES = Array.isArray(cfg.satellite_allowed) ? cfg.satellite_allowed.slice() : [];
  BLOCKED_SATELLITES = Array.isArray(cfg.satellite_blocked) ? cfg.satellite_blocked.slice() : [];
  CORE_ASSETS = Object.keys(cfg.core_assets || {});
  renderRiskConfig(cfg);
  renderProposalAutomationSettings(cfg);
  updateConfigurationSummary();
  renderAutomationOverview();
}

async function loadTradableAssets() {
  const data = await fetchJson("/api/valid_product_ids?quote=USD&tradable_only=true", {}, 45000);
  ASSETS = (data.products || []).map((p) => ({ product_id: p, quote_currency_id: "USD" }));
  FILTERED_ASSETS = ASSETS.slice();
}

async function loadConfiguration() {
  const tbody = document.getElementById("assetRows");
  if (tbody) tbody.innerHTML = `<tr><td colspan="5" class="muted">Loading asset universe...</td></tr>`;
  const metaEl = document.getElementById("assetSearchMeta");
  if (metaEl) metaEl.textContent = "Loading asset universe...";
  setStatus("Loading configuration...");

  try {
    await Promise.all([loadTradableAssets(), loadConfigState(), loadPortfolioSnapshot()]);
    await loadAdvisoryState();
    await loadProposalState();
    drawAssetRows();
    applyPresetFromUrlIfPresent();
    setStatus(`Loaded ${ASSETS.length} tradable USD assets. Portfolio guardrails and advanced sections are ready.`);
    LAST_CONFIGURATION_REFRESH_AT = Date.now();
  } catch (err) {
    console.error(err);
    if (tbody) tbody.innerHTML = `<tr><td colspan="5" class="bad">Asset load failed: ${escapeHtml(err.message)}</td></tr>`;
    setStatus(`Configuration load failed: ${err.message}`, true);
  }
}

function normalizePercentInput(id) {
  const el = document.getElementById(id);
  const raw = String(el?.value || "").trim();
  if (!raw) return "";

  const n = clampPercentValue(raw);
  return Number.isFinite(n) ? (n / 100) : "";
}

function applyConfigPreset(name, options = {}) {
  const preset = CONFIG_PRESETS[String(name || "").toLowerCase()];
  if (!preset) return;

  const values = preset.values || {};
  setInputValue("satellite_total_target", values.satellite_total_target, true);
  setInputValue("satellite_total_max", values.satellite_total_max, true);
  setInputValue("min_cash_reserve", values.min_cash_reserve, true);
  setInputValue("trade_min_value_usd", values.trade_min_value_usd, false);
  setInputValue("max_active_satellites", values.max_active_satellites, false);
  setInputValue("max_new_satellites_per_cycle", values.max_new_satellites_per_cycle, false);

  setPresetActiveState(name);
  const sourceLabel = String(options.sourceLabel || "").trim();
  if (sourceLabel === "Safe Mode") {
    setPresetStatus("Safe Mode reduces exposure and raises protective limits in the form. Save Configuration to make it live.");
  } else if (sourceLabel) {
    setPresetStatus(`${sourceLabel} recommended the ${preset.label} preset. It has been staged in the form but not saved.`);
  } else {
    setPresetStatus(`${preset.label} preset staged. Changes are not saved until you click Save Configuration.`);
  }
  if (!options.silentStatus) {
    const statusLead = sourceLabel === "Safe Mode"
      ? "Safe Mode loaded lower-risk values into the current fields."
      : sourceLabel
        ? `${sourceLabel} staged the ${preset.label} preset.`
        : `${preset.label} preset applied to current fields.`;
    setStatus(`${statusLead} Review and save when ready.`);
  }
}

function applyPresetFromUrlIfPresent() {
  if (URL_PRESET_APPLIED) return;

  const preset = String(URL_PARAMS.get("preset") || "").trim().toLowerCase();
  if (!preset || !CONFIG_PRESETS[preset]) return;

  URL_PRESET_APPLIED = true;
  applyConfigPreset(preset, {
    sourceLabel: String(URL_PARAMS.get("preset_source") || "Auto-Adaptive Mode").trim(),
    silentStatus: false
  });
}

async function postAdminAssetAction(payload, successMessage) {
  try {
    await fetchJson("/api/admin/asset", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...payload, ...(API_SECRET ? { secret: API_SECRET } : {}) })
    }, 20000);
    setStatus(successMessage);
    await loadConfiguration();
  } catch (err) {
    console.error(err);
    setStatus(`${successMessage} failed: ${err.message}`, true);
  }
}

async function setAssetMode(productId, mode) {
  if (mode === "enable") return postAdminAssetAction({ action: "enable_satellite", product_id: productId }, `${productId} enabled as satellite`);
  if (mode === "disable") return postAdminAssetAction({ action: "block", product_id: productId }, `${productId} disabled from trading`);
  if (mode === "auto") {
    try {
      await fetchJson("/api/admin/asset", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "disable_satellite", product_id: productId, ...(API_SECRET ? { secret: API_SECRET } : {}) })
      }, 20000);
      await fetchJson("/api/admin/asset", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "unblock", product_id: productId, ...(API_SECRET ? { secret: API_SECRET } : {}) })
      }, 20000);
      setStatus(`${productId} returned to auto mode`);
      await loadConfiguration();
    } catch (err) {
      console.error(err);
      setStatus(`Set auto failed for ${productId}: ${err.message}`, true);
    }
  }
}

async function saveRiskControls() {
  try {
    const nextConfig = snapshotCurrentConfigForm();
    const changeSummary = summarizeConfigChanges(CURRENT_CONFIG, nextConfig);

    await fetchJson("/api/admin/asset", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action: "set_risk",
        satellite_total_max: nextConfig.satellite_total_max,
        satellite_total_target: nextConfig.satellite_total_target,
        min_cash_reserve: nextConfig.min_cash_reserve,
        trade_min_value_usd: nextConfig.trade_min_value_usd,
        max_active_satellites: nextConfig.max_active_satellites,
        max_new_satellites_per_cycle: nextConfig.max_new_satellites_per_cycle,
        config_proposal_generation_mode: nextConfig.config_proposal_generation_mode,
        config_proposal_apply_mode: nextConfig.config_proposal_apply_mode,
        config_proposal_min_confidence: nextConfig.config_proposal_min_confidence,
        ...(API_SECRET ? { secret: API_SECRET } : {})
      })
    }, 20000);
    await loadConfiguration();
    LATEST_AUTOMATION_MESSAGE = `Saved changes • ${changeSummary}`;
    renderAutomationOverview();
    setStatus(`Saved changes • ${changeSummary}`);
  } catch (err) {
    console.error(err);
    setStatus(`Risk control save failed: ${err.message}`, true);
  }
}

function setProposalAutomationResult(message, isError = false) {
  const el = document.getElementById("configProposalAutomationResult");
  if (!el) return;
  el.textContent = message;
  el.className = isError ? "config-proposal-automation-result error" : "config-proposal-automation-result";
  LATEST_AUTOMATION_MESSAGE = message;
  renderAutomationOverview();
}

async function updateLatestProposalStatus(action) {
  const proposalId = String(LATEST_PROPOSAL?.id || "").trim();
  if (!proposalId) {
    setProposalActionResult(action === "apply" ? "No approved proposal is available to apply." : "No pending proposal is available to review.", true, true);
    return;
  }

  const approveBtn = document.getElementById("configProposalApproveBtn");
  const applyBtn = document.getElementById("configProposalApplyBtn");
  const rejectBtn = document.getElementById("configProposalRejectBtn");
  if (approveBtn) approveBtn.disabled = true;
  if (applyBtn) applyBtn.disabled = true;
  if (rejectBtn) rejectBtn.disabled = true;

  const actionLabel =
    action === "approve"
      ? "Approving"
      : action === "apply"
        ? "Applying"
        : "Rejecting";
  setProposalActionResult(`${actionLabel} proposal...`, false, true);

  try {
    const result = await fetchJson(`/api/config_proposals/${encodeURIComponent(proposalId)}/${encodeURIComponent(action)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...(API_SECRET ? { secret: API_SECRET } : {}) })
    }, 30000);

    if (action === "approve") {
      if (result?.auto_apply_attempted && result?.auto_apply_ok) {
        setProposalActionResult("Proposal approved and applied based on the current After Approval setting.", false, true);
      } else if (result?.auto_apply_attempted && result?.auto_apply_ok === false) {
        setProposalActionResult("Proposal was approved, but the automatic apply step needs review.", true, true);
      } else {
        setProposalActionResult("Proposal approved. Apply remains a separate operator step.", false, true);
      }
    } else if (action === "apply") {
      const status = String(result?.status || "").trim().toLowerCase();
      const reason = String(result?.reason || "").trim().toLowerCase();
      if (status === "applied") {
        setProposalActionResult("Proposal applied.", false, true);
      } else if (reason === "already_applied") {
        setProposalActionResult("Proposal was already applied.", false, true);
      } else if (reason === "cannot_apply_until_approved") {
        setProposalActionResult("Proposal must be approved before it can be applied.", true, true);
      } else if (reason === "review_only_proposal") {
        setProposalActionResult("This proposal is approval-only and does not support direct apply.", true, true);
      } else {
        setProposalActionResult(`Apply returned status: ${status || reason || "unknown"}.`, status !== "applied", true);
      }
    } else {
      setProposalActionResult("Proposal rejected.", false, true);
    }

    await loadProposalState();
  } catch (err) {
    console.error(err);
    setProposalActionResult(`${actionLabel} failed: ${err.message}`, true, true);
    await loadProposalState();
  }
}

async function approveLatestProposal() {
  await updateLatestProposalStatus("approve");
}

async function applyLatestProposal() {
  await updateLatestProposalStatus("apply");
}

async function rejectLatestProposal() {
  await updateLatestProposalStatus("reject");
}

async function evaluateAutoDraftNow() {
  setProposalAutomationResult("Evaluating recommendations...");

  try {
    const result = await fetchJson("/api/config_proposals/auto_draft", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...(API_SECRET ? { secret: API_SECRET } : {}) })
    }, 30000);

    setProposalAutomationMessageFromStatus(result);
    await loadProposalState();
  } catch (err) {
    console.error(err);
    setProposalAutomationResult(`Recommendation evaluation failed: ${err.message}`, true);
  }
}

async function generateProposalNow() {
  setProposalAutomationResult("Generating proposal...");

  try {
    const result = await fetchJson("/api/config_proposals/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...(API_SECRET ? { secret: API_SECRET } : {}) })
    }, 30000);

    setProposalAutomationMessageFromStatus(result);
    await loadProposalState();
  } catch (err) {
    console.error(err);
    setProposalAutomationResult(`Proposal generation failed: ${err.message}`, true);
  }
}

window.loadConfiguration = loadConfiguration;
window.refreshAssets = loadConfiguration;
window.renderAssetRows = applyAssetFilter;
window.setAssetMode = setAssetMode;
window.saveRiskControls = saveRiskControls;
window.applyConfigPreset = applyConfigPreset;
window.evaluateAutoDraftNow = evaluateAutoDraftNow;
window.generateProposalNow = generateProposalNow;
window.approveLatestProposal = approveLatestProposal;
window.applyLatestProposal = applyLatestProposal;
window.rejectLatestProposal = rejectLatestProposal;

window.addEventListener("DOMContentLoaded", () => {
  const search = document.getElementById("assetSearch");
  if (search) search.addEventListener("input", applyAssetFilter);
  bindPercentFieldBehavior();
  loadConfiguration();
  applyConfigurationFocus();
});

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState !== "visible") return;
  if (Date.now() - LAST_CONFIGURATION_REFRESH_AT < 15000) return;
  loadConfiguration();
});
