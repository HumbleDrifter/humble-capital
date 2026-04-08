let ASSETS = [];
let FILTERED_ASSETS = [];
let ALLOWED_SATELLITES = [];
let BLOCKED_SATELLITES = [];
let CORE_ASSETS = [];
let ASSET_STATE_BY_PRODUCT = {};
let HOLDINGS_BY_PRODUCT = {};
let TOTAL_ASSET_VALUE_USD = 0;
let ACTIVE_PRESET = "";
let URL_PRESET_APPLIED = false;
let CURRENT_CONFIG = {};
let LATEST_PROPOSAL = null;
let LATEST_AUTOMATION_MESSAGE = "";
let RECENT_PROPOSALS = [];
let LAST_CONFIGURATION_REFRESH_AT = 0;
let LATEST_OPTIONS_HEALTH = null;
let OPTIONS_TELEMETRY_WARNING_SHOWN = false;
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

async function requestJson(path, options = {}, timeoutMs = 30000) {
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

    return {
      ...data,
      _httpOk: res.ok,
      _httpStatus: res.status
    };
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
  const order = safeObject(proposal.order);
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
      order: {
        asset_class: String(order.asset_class || "").trim(),
        broker: String(order.broker || "").trim(),
        action: String(order.action || "").trim(),
        underlying: String(order.underlying || "").trim(),
        strategy: String(order.strategy || "").trim(),
        order_type: String(order.order_type || "").trim(),
        limit_price: hasNumericValue(order.limit_price) ? Number(order.limit_price) : null,
        tif: String(order.tif || "").trim(),
        source: String(order.source || "").trim(),
        proposal_id: String(order.proposal_id || "").trim(),
        legs: Array.isArray(order.legs) ? order.legs.map((item) => {
          const raw = safeObject(item);
          return {
            side: String(raw.side || "").trim(),
            right: String(raw.right || raw.right_code || "").trim(),
            expiry: String(raw.expiry || "").trim(),
            strike: hasNumericValue(raw.strike) ? Number(raw.strike) : null,
            quantity: hasNumericValue(raw.quantity) ? Number(raw.quantity) : null
          };
        }) : []
      },
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
  if (normalized === "options_order_recommendation") return "Options Order Recommendation";
  if (normalized === "config_guardrail") return "Config Guardrail";
  return titleCase(normalized || "unknown");
}

function isProposalApplyCapable(proposal) {
  const proposalRecord = safeObject(proposal);
  const nestedProposal = safeObject(proposalRecord.proposal);
  const proposalType = String(nestedProposal.proposal_type || proposalRecord.proposal_type || "").trim().toLowerCase();
  return proposalType === "config_guardrail";
}

function isOptionsProposalType(proposal) {
  const proposalRecord = safeObject(proposal);
  const nestedProposal = safeObject(proposalRecord.proposal);
  const proposalType = String(nestedProposal.proposal_type || proposalRecord.proposal_type || "").trim().toLowerCase();
  return proposalType === "options_order_recommendation";
}

function isOptionsProposalExecuteCapable(proposal) {
  return isOptionsProposalType(proposal) && String(safeObject(proposal).status || "").trim().toLowerCase() === "approved";
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
    const executeOptionsBtn = document.getElementById("configProposalExecuteOptionsBtn");
    if (executeOptionsBtn) {
      executeOptionsBtn.disabled = true;
      executeOptionsBtn.hidden = true;
      executeOptionsBtn.dataset.proposalId = "";
    }
    if (emptyEl) emptyEl.hidden = false;
    if (contentEl) contentEl.hidden = true;
    renderAutomationOverview();
    return;
  }

  if (emptyEl) emptyEl.hidden = true;
  if (contentEl) contentEl.hidden = false;

  const statusTone = proposalStatusTone(proposal.status);
  const isSatelliteEnableProposal = proposal.proposal.proposal_type === "satellite_enable_recommendation";
  const isOptionsOrderProposal = proposal.proposal.proposal_type === "options_order_recommendation";
  const isPending = proposal.status === "pending";
  const isApproved = proposal.status === "approved";
  const isApplyCapable = isProposalApplyCapable(proposal);
  const isOptionsExecuteCapable = isOptionsProposalExecuteCapable(proposal);
  const statusEl = document.getElementById("configProposalStatus");
  if (statusEl) {
    statusEl.textContent = proposalStatusLabel(proposal.status || "—");
    statusEl.className = `config-proposal-status${statusTone ? ` ${statusTone}` : ""}`;
  }

  const changesHost = document.getElementById("configProposalChanges");
  const changesTitleEl = document.getElementById("configProposalChangesTitle");
  if (changesTitleEl) {
    changesTitleEl.textContent = isSatelliteEnableProposal
      ? "Recommended Candidates"
      : isOptionsOrderProposal
        ? "Proposed Order"
        : "Changed Controls";
  }
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
      : isOptionsOrderProposal
      ? (
        proposal.proposal.order.underlying
          ? [
              `<div class="config-proposal-change-row">
                <span class="config-proposal-change-label">${escapeHtml(proposal.proposal.order.underlying || "Underlying")}</span>
                <span class="config-proposal-change-values">${escapeHtml(
                  `${proposal.proposal.order.broker || "ibkr"} • ${proposal.proposal.order.strategy || "options"} • ${proposal.proposal.order.order_type || "LIMIT"} ${hasNumericValue(proposal.proposal.order.limit_price) ? formatUSD(proposal.proposal.order.limit_price) : "—"} • ${proposal.proposal.order.tif || "DAY"}`
                )}</span>
              </div>`,
              ...(proposal.proposal.order.legs || []).map((leg, idx) => `
                <div class="config-proposal-change-row">
                  <span class="config-proposal-change-label">${escapeHtml(`Leg ${idx + 1}`)}</span>
                  <span class="config-proposal-change-values">${escapeHtml(
                    `${leg.side || "—"} ${hasNumericValue(leg.quantity) ? formatInteger(leg.quantity) : "—"} ${leg.expiry || "—"} ${hasNumericValue(leg.strike) ? Number(leg.strike).toFixed(2) : "—"} ${leg.right || "—"}`
                  )}</span>
                </div>
              `)
            ].join("")
          : `<div class="config-proposal-change-row">
              <span class="config-proposal-change-label">Proposed Order</span>
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
    configProposalCurrentRisk: isSatelliteEnableProposal
      ? `${proposal.proposal.candidates.length || 0} review-ready`
      : isOptionsOrderProposal
        ? `${proposal.proposal.order.broker || "ibkr"} • ${proposal.proposal.order.strategy || "options"}`
        : formatProposalRisk(proposal.proposal.source.risk_score, proposal.proposal.source.risk_band),
    configProposalProjectedRisk: isSatelliteEnableProposal
      ? "Approval only"
      : isOptionsOrderProposal
        ? `${proposal.proposal.order.order_type || "LIMIT"} ${hasNumericValue(proposal.proposal.order.limit_price) ? formatUSD(proposal.proposal.order.limit_price) : "—"}`
        : formatProposalRisk(proposal.proposal.simulation.projected_score, proposal.proposal.simulation.projected_band),
    configProposalScoreDelta: isOptionsOrderProposal
      ? `${(proposal.proposal.order.legs || []).length || 0} legs`
      : formatProposalScoreDelta(proposal.proposal.simulation.score_delta),
    configProposalCreatedAt: formatProposalDate(proposal.created_at),
    configProposalExpiresAt: formatProposalDate(proposal.expires_at),
    configProposalApprovedBy: proposal.approved_by || "—",
    configProposalAppliedBy: proposal.applied_by || "—",
    configProposalNote: isSatelliteEnableProposal
      ? "This recommendation is approval-only and will not change the live allowlist until an explicit operator action is taken."
      : isOptionsOrderProposal
        ? "This options recommendation is approval-first. Execution must be queued explicitly after review, and options remain limit-only and fail-closed on validation."
      : proposalStatusNote(proposal.status)
  };

  Object.entries(valueMap).forEach(([id, value]) => {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
  });

  const actionsEl = document.getElementById("configProposalActions");
  const approveBtn = document.getElementById("configProposalApproveBtn");
  const applyBtn = document.getElementById("configProposalApplyBtn");
  const executeOptionsBtn = document.getElementById("configProposalExecuteOptionsBtn");
  const rejectBtn = document.getElementById("configProposalRejectBtn");
  if (actionsEl) actionsEl.hidden = !(isPending || (isApproved && (isApplyCapable || isOptionsExecuteCapable)));
  if (approveBtn) {
    approveBtn.disabled = !isPending;
    approveBtn.hidden = !isPending;
    approveBtn.dataset.proposalId = proposal.id || "";
  }
  if (applyBtn) {
    applyBtn.disabled = !(isApproved && isApplyCapable);
    applyBtn.hidden = !(isApproved && isApplyCapable);
    applyBtn.dataset.proposalId = proposal.id || "";
  }
  if (executeOptionsBtn) {
    executeOptionsBtn.disabled = !(isApproved && isOptionsExecuteCapable);
    executeOptionsBtn.hidden = !(isApproved && isOptionsExecuteCapable);
    executeOptionsBtn.dataset.proposalId = proposal.id || "";
  }
  if (rejectBtn) {
    rejectBtn.disabled = !isPending;
    rejectBtn.hidden = !isPending;
    rejectBtn.dataset.proposalId = proposal.id || "";
  }
  const actionResultEl = document.getElementById("configProposalActionResult");
  if (!(isPending || (isApproved && (isApplyCapable || isOptionsExecuteCapable))) && !(actionResultEl && actionResultEl.dataset.userMessage)) {
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
  const rowState = String(ASSET_STATE_BY_PRODUCT[String(productId || "").toUpperCase()]?.effective_state || "").trim().toLowerCase();
  if (rowState === "core") {
    return { key: "core", badge: '<span class="badge accent2">core</span>' };
  }
  if (rowState === "disable") {
    return { key: "disable", badge: '<span class="badge bad">disable</span>' };
  }
  if (rowState === "enable") {
    return { key: "enable", badge: '<span class="badge good">enable</span>' };
  }
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

function assetControlId(productId, suffix) {
  const safe = String(productId || "").toUpperCase().replace(/[^A-Z0-9]+/g, "-");
  return `asset-${safe}-${suffix}`;
}

function readCoreField(productId, suffix, fallbackValue) {
  const el = document.getElementById(assetControlId(productId, suffix));
  const raw = String(el?.value ?? "").trim();
  if (!raw) return fallbackValue;
  const numeric = Number(raw);
  return Number.isFinite(numeric) ? numeric : fallbackValue;
}

function buildCoreControls(row) {
  const productId = row.product_id;
  const targetWeight = Number(row.target_weight || 0.05);
  const rebalanceBand = Number(row.rebalance_band || 0.02);
  return `
    <div class="asset-core-controls">
      <label class="tiny" for="${assetControlId(productId, "target-weight")}">Target</label>
      <input id="${assetControlId(productId, "target-weight")}" class="config-input asset-core-input" type="number" min="0.01" max="1" step="0.01" value="${targetWeight.toFixed(2)}">
      <label class="tiny" for="${assetControlId(productId, "rebalance-band")}">Band</label>
      <input id="${assetControlId(productId, "rebalance-band")}" class="config-input asset-core-input" type="number" min="0.01" max="1" step="0.01" value="${rebalanceBand.toFixed(2)}">
      <button class="btn btn-secondary asset-mode-btn" onclick="saveCoreSettings('${escapeHtml(productId)}')">Save Core</button>
    </div>
  `;
}

function buildActionButtons(row, modeKey) {
  const productId = row.product_id;
  const canAssignCore = Boolean(row.can_assign_core);
  const canEnable = Boolean(row.can_enable);
  const invalidReason = String(row.invalid_reason || "").trim();
  const invalidMessage = invalidReason ? `<div class="tiny muted">Unavailable: ${escapeHtml(invalidReason)}</div>` : "";

  if (modeKey === "core" && !canAssignCore) {
    return `<div class="muted">Managed as core</div>${invalidMessage}`;
  }

  const safe = escapeHtml(productId);

  return `
    <div class="asset-mode-actions">
      <button class="btn ${modeKey === "core" ? "btn-primary" : "btn-secondary"} asset-mode-btn" onclick="setAssetMode('${safe}','core')" ${canAssignCore ? "" : "disabled"}>Core</button>
      <button class="btn ${modeKey === "enable" ? "btn-primary" : "btn-secondary"} asset-mode-btn" onclick="setAssetMode('${safe}','enable')" ${canEnable ? "" : "disabled"}>Enable</button>
      <button class="btn ${modeKey === "auto" ? "btn-primary" : "btn-secondary"} asset-mode-btn" onclick="setAssetMode('${safe}','auto')">Auto</button>
      <button class="btn ${modeKey === "disable" ? "btn-primary" : "btn-secondary"} asset-mode-btn" onclick="setAssetMode('${safe}','disable')">Disable</button>
    </div>
    ${modeKey === "core" ? buildCoreControls(row) : ""}
    ${modeKey !== "core" ? invalidMessage : ""}
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
    const invalidMeta = !row.is_valid_product ? `<div class="tiny muted">Invalid for manual enable/core</div>` : "";

    return `
      <tr>
        <td><strong>${escapeHtml(productId)}</strong>${invalidMeta}</td>
        <td>${mode.badge}</td>
        <td class="right">${formatUsd(heldValue)}</td>
        <td class="right">${formatPct(weight)}</td>
        <td>${buildActionButtons(row, mode.key)}</td>
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

async function loadOptionsHealth() {
  try {
    const data = await requestJson("/api/options/ibkr/health", {}, 20000);
    renderOptionsHealth(data);
  } catch (err) {
    console.warn("Options health load failed:", err.message);
    renderOptionsHealth({
      ok: false,
      paper_mode: false,
      connected: false,
      host: "",
      port: "",
      account: "",
      reason: err.message
    });
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
  const data = await fetchJson("/api/assets/config", {}, 45000);
  ASSETS = (data.items || []).map((item) => ({
    ...item,
    product_id: String(item.product_id || "").toUpperCase(),
    quote_currency_id: "USD"
  }));
  FILTERED_ASSETS = ASSETS.slice();
  ASSET_STATE_BY_PRODUCT = Object.fromEntries(
    ASSETS.map((item) => [String(item.product_id || "").toUpperCase(), item])
  );
  CORE_ASSETS = ASSETS.filter((item) => item.state === "core").map((item) => item.product_id);
  ALLOWED_SATELLITES = ASSETS.filter((item) => item.state === "enable").map((item) => item.product_id);
  BLOCKED_SATELLITES = ASSETS.filter((item) => item.state === "disable").map((item) => item.product_id);
}

async function loadConfiguration() {
  const tbody = document.getElementById("assetRows");
  if (tbody) tbody.innerHTML = `<tr><td colspan="5" class="muted">Loading asset universe...</td></tr>`;
  const metaEl = document.getElementById("assetSearchMeta");
  if (metaEl) metaEl.textContent = "Loading asset universe...";
  setStatus("Loading configuration...");

  try {
    await Promise.all([loadTradableAssets(), loadConfigState(), loadPortfolioSnapshot()]);
    await Promise.all([loadAdvisoryState(), loadProposalState(), loadOptionsHealth(), loadOptionsTelemetry()]);
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
  try {
    const currentRow = ASSET_STATE_BY_PRODUCT[String(productId || "").toUpperCase()] || {};
    const payload = {
      product_id: productId,
      state: mode,
      ...(API_SECRET ? { secret: API_SECRET } : {})
    };
    if (mode === "core") {
      payload.target_weight = readCoreField(productId, "target-weight", Number(currentRow.target_weight || 0.05));
      payload.rebalance_band = readCoreField(productId, "rebalance-band", Number(currentRow.rebalance_band || 0.02));
    }
    const result = await fetchJson("/api/assets/config/update", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    }, 20000);
    const stateLabel =
      mode === "core"
        ? "promoted to core"
        : mode === "enable"
          ? "enabled"
          : mode === "disable"
            ? "disabled"
            : "returned to auto";
    setStatus(`${productId} ${stateLabel}`);
    if (result?.item?.product_id) {
      ASSET_STATE_BY_PRODUCT[String(result.item.product_id || "").toUpperCase()] = result.item;
    }
    await loadConfiguration();
  } catch (err) {
    console.error(err);
    setStatus(`Set ${mode} failed for ${productId}: ${err.message}`, true);
  }
}

async function saveCoreSettings(productId) {
  return setAssetMode(productId, "core");
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
  const proposalApplyCapable = isProposalApplyCapable(LATEST_PROPOSAL);
  const proposalIsOptionsType = isOptionsProposalType(LATEST_PROPOSAL);

  const approveBtn = document.getElementById("configProposalApproveBtn");
  const applyBtn = document.getElementById("configProposalApplyBtn");
  const executeOptionsBtn = document.getElementById("configProposalExecuteOptionsBtn");
  const rejectBtn = document.getElementById("configProposalRejectBtn");
  if (approveBtn) approveBtn.disabled = true;
  if (applyBtn) applyBtn.disabled = true;
  if (executeOptionsBtn) executeOptionsBtn.disabled = true;
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
      } else if (proposalIsOptionsType) {
        setProposalActionResult("Options proposal approved. Execution remains a separate operator step through the queue.", false, true);
      } else if (!proposalApplyCapable) {
        setProposalActionResult("Proposal approved. This proposal is review-only and does not have a separate apply step.", false, true);
      } else {
        await loadProposalState();
        const shouldApplyNow = window.confirm("Proposal approved. Do you want to apply it now?");
        if (shouldApplyNow) {
          await applyLatestProposal();
          return;
        }
        setProposalActionResult("Proposal approved. You can apply it later from this panel.", false, true);
        return;
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

async function executeLatestOptionsProposal() {
  const proposalId = String(LATEST_PROPOSAL?.id || "").trim();
  if (!proposalId || !isOptionsProposalExecuteCapable(LATEST_PROPOSAL)) {
    setProposalActionResult("No approved options proposal is available to execute.", true, true);
    return;
  }

  const executeOptionsBtn = document.getElementById("configProposalExecuteOptionsBtn");
  if (executeOptionsBtn) executeOptionsBtn.disabled = true;
  setProposalActionResult("Queueing approved options proposal...", false, true);

  try {
    const result = await requestJson(`/api/options/proposals/${encodeURIComponent(proposalId)}/execute`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...(API_SECRET ? { secret: API_SECRET } : {}) })
    }, 30000);

    if (result.ok) {
      setProposalActionResult(
        `Options proposal ${proposalId} queued for execution review.${result.message ? ` ${result.message}` : ""}`.trim(),
        false,
        true
      );
    } else {
      setProposalActionResult(
        `Options execution failed: ${String(result.reason || result.error || `HTTP ${result._httpStatus || "unknown"}`).trim()}.`,
        true,
        true
      );
    }
    await loadProposalState();
    await loadOptionsTelemetry();
  } catch (err) {
    console.error(err);
    setProposalActionResult(`Options execution failed: ${err.message}`, true, true);
    await loadProposalState();
    await loadOptionsTelemetry();
  }
}

async function rejectLatestProposal() {
  await updateLatestProposalStatus("reject");
}

async function createTestOptionsProposal() {
  setOptionsHealthResult("Creating test options proposal...");

  try {
    const result = await requestJson("/api/options/proposals/test_submit", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...(API_SECRET ? { secret: API_SECRET } : {}) })
    }, 30000);

    if (result.ok && (result.status === "created" || result.status === "deduped" || result.status === "deduped_recent")) {
      const message = result.status === "created"
        ? `Test options proposal ${result.proposal_id || ""} created with status ${result.status}.`
        : `Matching test options proposal ${result.proposal_id || ""} already exists with status ${result.status}.`;
      setOptionsHealthResult(message.trim(), false);
      await Promise.all([loadProposalState(), loadOptionsHealth()]);
      return;
    }

    setOptionsHealthResult(
      `Test options proposal failed: ${String(
        result.reason || result.error || result.status || `HTTP ${result._httpStatus || "unknown"}`
      ).trim()}.`,
      true
    );
  } catch (err) {
    console.error(err);
    setOptionsHealthResult(`Test options proposal failed: ${err.message}`, true);
  }
}

async function syncOptionsState() {
  setOptionsHealthResult("Syncing options state from IBKR paper session...");
  try {
    const result = await requestJson("/api/options/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...(API_SECRET ? { secret: API_SECRET } : {}) })
    }, 30000);

    if (result.ok) {
      if (result.health) {
        renderOptionsHealth(result.health);
      }
      setOptionsHealthResult(
        `Options sync complete. Orders ${Number(result.orders_count || 0)} • Positions ${Number(result.positions_count || 0)}.`,
        false
      );
      await Promise.all([loadOptionsTelemetry(), loadOptionsHealth()]);
      return;
    }

    setOptionsHealthResult(
      `Options sync failed: ${String(
        result.reason || result.error || result.status || `HTTP ${result._httpStatus || "unknown"}`
      ).trim()}.`,
      true
    );
    await loadOptionsHealth();
  } catch (err) {
    console.error(err);
    setOptionsHealthResult(`Options sync failed: ${err.message}`, true);
    await loadOptionsHealth();
  }
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

function setOptionsHealthResult(message = "", isError = false) {
  const el = document.getElementById("optionsHealthResult");
  if (!el) return;
  el.textContent = String(message || "").trim() || "Options paper-trading health has not been checked yet.";
  el.className = isError ? "config-proposal-automation-result error" : "config-proposal-automation-result";
}

function renderOptionsHealth(data) {
  const payload = safeObject(data);
  LATEST_OPTIONS_HEALTH = payload;

  const valueMap = {
    optionsHealthStatus: payload.ok ? "Ready" : "Needs Attention",
    optionsHealthPaperMode: payload.paper_mode ? "On" : "Off",
    optionsHealthConnected: payload.connected ? "Connected" : "Disconnected",
    optionsHealthEndpoint: payload.host && payload.port ? `${payload.host}:${payload.port}` : "—",
    optionsHealthAccount: String(payload.account || "").trim() || "—",
    optionsHealthReason: String(payload.reason || payload.error || "").trim() || "—"
  };

  Object.entries(valueMap).forEach(([id, value]) => {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
  });

  if (payload.ok) {
    const readiness = payload.last_ready_at ? ` Last ready ${formatProposalDate(payload.last_ready_at)}.` : "";
    setOptionsHealthResult(`IBKR options paper-trading health is ready for operator testing.${readiness}`.trim());
  } else {
    const reason = String(payload.reason || payload.error || payload.last_error || "not_ready").trim();
    setOptionsHealthResult(`IBKR options paper-trading is not ready: ${reason}.`, true);
  }
}

function renderOptionsExecutionHistory(items) {
  const host = document.getElementById("optionsExecutionsHistory");
  if (!host) return;
  const rows = Array.isArray(items) ? items.slice(0, 8) : [];
  if (!rows.length) {
    host.innerHTML = `<div class="config-proposal-history-empty">No options execution activity yet.</div>`;
    return;
  }
  host.innerHTML = rows.map((item) => `
    <div class="config-proposal-history-row">
      <div class="config-proposal-history-main">
        <div class="config-proposal-history-top">
          <span class="config-proposal-id">${escapeHtml(item.underlying || "—")}</span>
          <span class="config-proposal-status ${item.ok ? "positive" : "negative"}">${escapeHtml(item.ok ? (item.status || "ok") : (item.reason || "failed"))}</span>
        </div>
        <div class="config-proposal-history-summary">${escapeHtml(`${item.strategy || "option"} • ${item.order_type || "LIMIT"} ${hasNumericValue(item.limit_price) ? formatUSD(item.limit_price) : "—"} • ${item.order_id || "no order id"}${item.error ? ` • ${item.error}` : ""}`)}</div>
      </div>
      <div class="config-proposal-history-meta">
        <div><span class="config-proposal-label">Proposal</span><span class="config-proposal-value">${escapeHtml(item.proposal_id || "—")}</span></div>
        <div><span class="config-proposal-label">Created</span><span class="config-proposal-value">${escapeHtml(formatProposalDate(item.created_at || item.ts))}</span></div>
      </div>
    </div>
  `).join("");
}

function renderOptionsOrdersHistory(items) {
  const host = document.getElementById("optionsOrdersHistory");
  if (!host) return;
  const rows = Array.isArray(items) ? items.slice(0, 8) : [];
  if (!rows.length) {
    host.innerHTML = `<div class="config-proposal-history-empty">No options order records yet.</div>`;
    return;
  }
  host.innerHTML = rows.map((item) => `
    <div class="config-proposal-history-row">
      <div class="config-proposal-history-main">
        <div class="config-proposal-history-top">
          <span class="config-proposal-id">${escapeHtml(item.underlying || "—")}</span>
          <span class="config-proposal-status">${escapeHtml(item.status || "—")}</span>
        </div>
        <div class="config-proposal-history-summary">${escapeHtml(`${item.strategy || "option"} • ${item.order_type || "LIMIT"} ${hasNumericValue(item.limit_price) ? formatUSD(item.limit_price) : "—"} • ${item.broker_order_id || item.record_key || "no broker order id"}`)}</div>
      </div>
      <div class="config-proposal-history-meta">
        <div><span class="config-proposal-label">Proposal</span><span class="config-proposal-value">${escapeHtml(item.proposal_id || "—")}</span></div>
        <div><span class="config-proposal-label">Updated</span><span class="config-proposal-value">${escapeHtml(formatProposalDate(item.updated_at || item.created_at))}</span></div>
      </div>
    </div>
  `).join("");
}

function renderOptionsPositionsHistory(items) {
  const host = document.getElementById("optionsPositionsHistory");
  if (!host) return;
  const rows = Array.isArray(items) ? items.slice(0, 10) : [];
  if (!rows.length) {
    host.innerHTML = `<div class="config-proposal-history-empty">No open options positions snapshot yet.</div>`;
    return;
  }
  host.innerHTML = rows.map((item) => `
    <div class="config-proposal-history-row">
      <div class="config-proposal-history-main">
        <div class="config-proposal-history-top">
          <span class="config-proposal-id">${escapeHtml(item.underlying || "—")}</span>
          <span class="config-proposal-status">${escapeHtml(item.side || "—")}</span>
        </div>
        <div class="config-proposal-history-summary">${escapeHtml(`${item.expiry || "—"} ${hasNumericValue(item.strike) ? Number(item.strike).toFixed(2) : "—"} ${item.right_code || "—"} • qty ${hasNumericValue(item.quantity) ? Number(item.quantity).toFixed(2) : "—"} • avg ${hasNumericValue(item.avg_cost) ? formatUSD(item.avg_cost) : "—"}`)}</div>
      </div>
      <div class="config-proposal-history-meta">
        <div><span class="config-proposal-label">Status</span><span class="config-proposal-value">${escapeHtml(item.status || "—")}</span></div>
        <div><span class="config-proposal-label">Updated</span><span class="config-proposal-value">${escapeHtml(formatProposalDate(item.updated_at))}</span></div>
      </div>
    </div>
  `).join("");
}

function warnOptionsTelemetryUnavailableOnce() {
  if (OPTIONS_TELEMETRY_WARNING_SHOWN) return;
  OPTIONS_TELEMETRY_WARNING_SHOWN = true;
  console.warn("options telemetry unavailable");
}

async function requestOptionalTelemetryJson(path, options = {}, timeoutMs = 30000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(authUrl(path), {
      credentials: "same-origin",
      ...options,
      signal: controller.signal
    });

    if (!res.ok) {
      return null;
    }

    const text = await res.text();
    if (!text) return {};

    try {
      return JSON.parse(text);
    } catch {
      warnOptionsTelemetryUnavailableOnce();
      return null;
    }
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function loadOptionsTelemetry() {
  try {
    const [executions, orders, positions] = await Promise.all([
      requestOptionalTelemetryJson("/api/options/executions?limit=8", {}, 20000),
      requestOptionalTelemetryJson("/api/options/orders?limit=8", {}, 20000),
      requestOptionalTelemetryJson("/api/options/positions", {}, 20000)
    ]);
    const telemetryUnavailable =
      executions === null ||
      orders === null ||
      positions === null ||
      executions?.ok === false ||
      orders?.ok === false ||
      positions?.ok === false;

    if (telemetryUnavailable) {
      warnOptionsTelemetryUnavailableOnce();
    }

    renderOptionsExecutionHistory(executions && executions?.ok !== false ? executions?.items || [] : []);
    renderOptionsOrdersHistory(orders && orders?.ok !== false ? orders?.items || [] : []);
    renderOptionsPositionsHistory(positions && positions?.ok !== false ? positions?.items || [] : []);
  } catch {
    warnOptionsTelemetryUnavailableOnce();
    renderOptionsExecutionHistory([]);
    renderOptionsOrdersHistory([]);
    renderOptionsPositionsHistory([]);
  }
}

async function fetchJson(path, options = {}, timeoutMs = 30000) {
  const data = await requestJson(path, options, timeoutMs);
  if (!data._httpOk || data.ok === false) {
    throw new Error(data.error || data.reason || `HTTP ${data._httpStatus}`);
  }
  return data;
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
window.saveCoreSettings = saveCoreSettings;
window.saveRiskControls = saveRiskControls;
window.applyConfigPreset = applyConfigPreset;
window.evaluateAutoDraftNow = evaluateAutoDraftNow;
window.generateProposalNow = generateProposalNow;
window.loadOptionsHealth = loadOptionsHealth;
window.loadOptionsTelemetry = loadOptionsTelemetry;
window.createTestOptionsProposal = createTestOptionsProposal;
window.syncOptionsState = syncOptionsState;
window.approveLatestProposal = approveLatestProposal;
window.applyLatestProposal = applyLatestProposal;
window.executeLatestOptionsProposal = executeLatestOptionsProposal;
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
