const API_SECRET = (window.SETTINGS_CONFIG && window.SETTINGS_CONFIG.apiSecret) || "";

function authUrl(path) {
  if (!API_SECRET) return path;
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}secret=${encodeURIComponent(API_SECRET)}`;
}

async function fetchJson(path, opts = {}) {
  const res = await fetch(authUrl(path), opts);
  const data = await res.json();

  if (!res.ok || data.ok === false) {
    throw new Error(data.error || `HTTP ${res.status}`);
  }

  return data;
}

function setStatus(message, isError = false) {
  const el = document.getElementById("settingsStatus");
  if (!el) return;
  el.textContent = message;
  el.className = isError ? "status-console error" : "status-console";
}

function setValue(id, value, fallback = "") {
  const el = document.getElementById(id);
  if (el) el.value = value ?? fallback;
}

function renderConfig(data) {
  const cfg = data.config || {};

  setValue("satMode", cfg.satellite_mode || "rotation");
  setValue("satTotalMax", cfg.satellite_total_max, "");
  setValue("satTotalTarget", cfg.satellite_total_target, "");
  setValue("minCashReserve", cfg.min_cash_reserve, "");
  setValue("tradeMinValueUsd", cfg.trade_min_value_usd, "");
  setValue("maxQuotePerTradeUsd", cfg.max_quote_per_trade_usd, "");
  setValue("maxActiveSatellites", cfg.max_active_satellites, "");
  setValue("rotationCooldownMinutes", cfg.rotation_cooldown_minutes, "");
  setValue("minMemeScore", cfg.min_meme_score, "");
}

async function refreshSettings() {
  try {
    const data = await fetchJson("/api/config");
    renderConfig(data);
    setStatus("Settings loaded.");
  } catch (err) {
    console.error(err);
    setStatus(`Error loading settings: ${err.message}`, true);
  }
}

async function refreshCaches() {
  try {
    await fetchJson("/api/admin/cache/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ secret: API_SECRET })
    });

    setStatus("Caches refreshed.");
  } catch (err) {
    console.error(err);
    setStatus(`Cache refresh failed: ${err.message}`, true);
  }
}

async function setMode() {
  try {
    const payload = {
      action: "set_mode",
      satellite_mode: document.getElementById("satMode").value,
      secret: API_SECRET
    };

    await fetchJson("/api/admin/asset", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    setStatus("Satellite mode updated.");
    await refreshSettings();
  } catch (err) {
    console.error(err);
    setStatus(`Mode update failed: ${err.message}`, true);
  }
}

async function setRisk() {
  try {
    const payload = {
      action: "set_risk",
      satellite_total_max: document.getElementById("satTotalMax").value,
      satellite_total_target: document.getElementById("satTotalTarget").value,
      min_cash_reserve: document.getElementById("minCashReserve").value,
      trade_min_value_usd: document.getElementById("tradeMinValueUsd").value,
      max_quote_per_trade_usd: document.getElementById("maxQuotePerTradeUsd").value,
      max_active_satellites: document.getElementById("maxActiveSatellites").value,
      rotation_cooldown_minutes: document.getElementById("rotationCooldownMinutes").value,
      min_meme_score: document.getElementById("minMemeScore").value,
      secret: API_SECRET
    };

    await fetchJson("/api/admin/asset", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    setStatus("Risk settings updated.");
    await refreshSettings();
  } catch (err) {
    console.error(err);
    setStatus(`Risk update failed: ${err.message}`, true);
  }
}

window.refreshSettings = refreshSettings;
window.refreshCaches = refreshCaches;
window.setMode = setMode;
window.setRisk = setRisk;

refreshSettings();
