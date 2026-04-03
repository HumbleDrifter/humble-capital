let ASSETS = [];
let FILTERED_ASSETS = [];
let ALLOWED_SATELLITES = [];
let BLOCKED_SATELLITES = [];
let CORE_ASSETS = [];
let HOLDINGS_BY_PRODUCT = {};
let TOTAL_ASSET_VALUE_USD = 0;
let ACTIVE_PRESET = "";
let URL_PRESET_APPLIED = false;
const PERCENT_FIELD_IDS = [
  "satellite_total_target",
  "satellite_total_max",
  "min_cash_reserve"
];

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
  if (format === "percent") return formatPct(Number(value || 0));
  if (format === "usd") return formatUsd(Number(value || 0));
  if (format === "integer") return Number(value || 0).toLocaleString();
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

async function loadConfigState() {
  const cfgData = await fetchJson("/api/config", {}, 20000);
  const cfg = cfgData.config || {};
  ALLOWED_SATELLITES = Array.isArray(cfg.satellite_allowed) ? cfg.satellite_allowed.slice() : [];
  BLOCKED_SATELLITES = Array.isArray(cfg.satellite_blocked) ? cfg.satellite_blocked.slice() : [];
  CORE_ASSETS = Object.keys(cfg.core_assets || {});
  renderRiskConfig(cfg);
  updateConfigurationSummary();
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
    drawAssetRows();
    applyPresetFromUrlIfPresent();
    setStatus(`Loaded ${ASSETS.length} tradable USD assets. Portfolio guardrails and advanced sections are ready.`);
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
  if (sourceLabel) {
    setPresetStatus(`${sourceLabel} recommended the ${preset.label} preset. It has been staged in the form but not saved.`);
  } else {
    setPresetStatus(`${preset.label} preset staged. Changes are not saved until you click Save Configuration.`);
  }
  if (!options.silentStatus) {
    const statusLead = sourceLabel ? `${sourceLabel} staged the ${preset.label} preset.` : `${preset.label} preset applied to current fields.`;
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
    await fetchJson("/api/admin/asset", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action: "set_risk",
        satellite_total_max: normalizePercentInput("satellite_total_max"),
        satellite_total_target: normalizePercentInput("satellite_total_target"),
        min_cash_reserve: normalizePercentInput("min_cash_reserve"),
        trade_min_value_usd: document.getElementById("trade_min_value_usd")?.value || "",
        max_active_satellites: document.getElementById("max_active_satellites")?.value || "",
        max_new_satellites_per_cycle: document.getElementById("max_new_satellites_per_cycle")?.value || "",
        ...(API_SECRET ? { secret: API_SECRET } : {})
      })
    }, 20000);
    setStatus("Risk controls saved.");
    await loadConfiguration();
  } catch (err) {
    console.error(err);
    setStatus(`Risk control save failed: ${err.message}`, true);
  }
}

window.loadConfiguration = loadConfiguration;
window.refreshAssets = loadConfiguration;
window.renderAssetRows = applyAssetFilter;
window.setAssetMode = setAssetMode;
window.saveRiskControls = saveRiskControls;
window.applyConfigPreset = applyConfigPreset;

window.addEventListener("DOMContentLoaded", () => {
  const search = document.getElementById("assetSearch");
  if (search) search.addEventListener("input", applyAssetFilter);
  bindPercentFieldBehavior();
  loadConfiguration();
  applyConfigurationFocus();
});
