let ASSETS = [];
let FILTERED_ASSETS = [];
let ALLOWED_SATELLITES = [];
let BLOCKED_SATELLITES = [];
let CORE_ASSETS = [];
let HOLDINGS_BY_PRODUCT = {};
let TOTAL_ASSET_VALUE_USD = 0;
let ACTIVE_PRESET = "";

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
  const el = document.getElementById(id);
  if (!el) return;
  const n = Number(value);
  el.value = Number.isFinite(n) && percent ? (n * 100).toFixed(2) : (value ?? "");
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
    drawAssetRows();
    setStatus(`Loaded ${ASSETS.length} tradable USD assets. Core controls and advanced sections are ready.`);
  } catch (err) {
    console.error(err);
    if (tbody) tbody.innerHTML = `<tr><td colspan="5" class="bad">Asset load failed: ${escapeHtml(err.message)}</td></tr>`;
    setStatus(`Configuration load failed: ${err.message}`, true);
  }
}

function normalizePercentInput(id) {
  const el = document.getElementById(id);
  const n = Number(el?.value || 0);
  return Number.isFinite(n) ? (n / 100) : "";
}

function applyConfigPreset(name) {
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
  setPresetStatus(`${preset.label} preset staged. Changes are not saved until you click Save Configuration.`);
  setStatus(`${preset.label} preset applied to current fields. Review and save when ready.`);
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
  loadConfiguration();
  applyConfigurationFocus();
});
