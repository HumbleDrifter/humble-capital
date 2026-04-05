const API_SECRET = (window.SYSTEM_STATUS_CONFIG && window.SYSTEM_STATUS_CONFIG.apiSecret) || "";

function authUrl(path) {
  if (!API_SECRET) return path;
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}secret=${encodeURIComponent(API_SECRET)}`;
}

async function fetchJson(path, options = {}) {
  const res = await fetch(authUrl(path), options);
  const raw = await res.text();
  let data;

  try {
    data = raw ? JSON.parse(raw) : {};
  } catch (_err) {
    throw new Error(`Invalid JSON from ${path}`);
  }

  if (!res.ok || data.ok === false) {
    throw new Error(data.error || `HTTP ${res.status}`);
  }

  return data;
}

function renderText(id, text, isError = false) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text;
  el.className = isError ? "status-console error" : "status-console";
}

function fmtTs(ts) {
  const n = Number(ts || 0);
  if (!n) return "—";
  return new Date(n * 1000).toLocaleString();
}

function fmtUsd(v) {
  return Number(v || 0).toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2
  });
}

function normalizeSymbolList(values) {
  return Array.from(
    new Set(
      (Array.isArray(values) ? values : [])
        .map((value) => String(value || "").trim().toUpperCase())
        .filter(Boolean)
    )
  ).sort();
}

function diffList(left, right) {
  const rightSet = new Set(normalizeSymbolList(right));
  return normalizeSymbolList(left).filter((item) => !rightSet.has(item));
}

function fmtList(values, emptyLabel = "none") {
  const items = normalizeSymbolList(values);
  return items.length ? items.join(", ") : emptyLabel;
}

function renderUniverseText(id, text, isError = false) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text;
  el.className = isError ? "status-console error" : "status-console";
}

function localUniverseConfig(config) {
  const cfg = config || {};
  return {
    coreAssets: normalizeSymbolList(Object.keys(cfg.core_assets || {})),
    satelliteAllowed: normalizeSymbolList(cfg.satellite_allowed || []),
    satelliteBlocked: normalizeSymbolList(cfg.satellite_blocked || [])
  };
}

function buildTradableUniverseDrift(serverData, config) {
  const server = serverData || {};
  const local = localUniverseConfig(config);

  const serverCore = normalizeSymbolList(server.core_assets || []);
  const serverAllowed = normalizeSymbolList(server.satellite_allowed || []);
  const serverBlocked = normalizeSymbolList(server.satellite_blocked || []);
  const activeUniverse = normalizeSymbolList(server.active_satellite_buy_universe || []);
  const currentSelections = normalizeSymbolList(server.current_system_selections || []);

  return {
    localCoreMissingOnServer: diffList(local.coreAssets, serverCore),
    serverCoreMissingLocally: diffList(serverCore, local.coreAssets),
    localAllowedNotLive: diffList(local.satelliteAllowed, activeUniverse),
    serverLiveMissingLocalAllowed: diffList(activeUniverse, local.satelliteAllowed),
    localBlockedMissingOnServer: diffList(local.satelliteBlocked, serverBlocked),
    serverAllowedMissingLocally: diffList(serverAllowed, local.satelliteAllowed),
    serverBlocked,
    currentSelectionsOutsideLocalAllowed: diffList(currentSelections, local.satelliteAllowed)
  };
}

function renderTradableUniverse(serverData, config) {
  const universe = serverData || {};
  const summary = universe.summary || {};
  const drift = buildTradableUniverseDrift(universe, config);

  renderUniverseText(
    "tradableUniverseStatus",
    `Server universe loaded. Generated ${fmtTs(universe.generated_at)}${universe.snapshot_timestamp ? ` • Snapshot ${fmtTs(universe.snapshot_timestamp)}` : ""}.`
  );

  renderUniverseText(
    "tradableUniverseSummary",
    [
      `Generated: ${fmtTs(universe.generated_at)}`,
      `Snapshot: ${fmtTs(universe.snapshot_timestamp)}`,
      `Satellite mode: ${universe.satellite_mode || "—"}`,
      `Core assets: ${summary.core_asset_count ?? normalizeSymbolList(universe.core_assets || []).length}`,
      `Satellite allowed: ${summary.satellite_allowed_count ?? normalizeSymbolList(universe.satellite_allowed || []).length}`,
      `Satellite blocked: ${summary.satellite_blocked_count ?? normalizeSymbolList(universe.satellite_blocked || []).length}`,
      `Active buy universe: ${summary.active_satellite_buy_universe_count ?? normalizeSymbolList(universe.active_satellite_buy_universe || []).length}`,
      `Current system selections: ${summary.current_system_selection_count ?? normalizeSymbolList(universe.current_system_selections || []).length}`
    ].join("\n")
  );

  renderUniverseText(
    "tradableUniverseDrift",
    [
      `Local core missing on server: ${fmtList(drift.localCoreMissingOnServer)}`,
      `Server core missing locally: ${fmtList(drift.serverCoreMissingLocally)}`,
      `Local allowed not live on server: ${fmtList(drift.localAllowedNotLive)}`,
      `Server live missing local allowed: ${fmtList(drift.serverLiveMissingLocalAllowed)}`,
      `Local blocked missing on server: ${fmtList(drift.localBlockedMissingOnServer)}`,
      `Server allowed missing locally: ${fmtList(drift.serverAllowedMissingLocally)}`,
      `Server blocked symbols: ${fmtList(drift.serverBlocked)}`,
      `Current selections outside local allowed: ${fmtList(drift.currentSelectionsOutsideLocalAllowed)}`
    ].join("\n"),
    drift.serverBlocked.length > 0
  );

  const listsEl = document.getElementById("tradableUniverseLists");
  if (listsEl) {
    listsEl.textContent = [
      `Core assets`,
      `${fmtList(universe.core_assets)}`,
      ``,
      `Satellite allowed`,
      `${fmtList(universe.satellite_allowed)}`,
      ``,
      `Satellite blocked`,
      `${fmtList(universe.satellite_blocked)}`,
      ``,
      `Active satellite buy universe`,
      `${fmtList(universe.active_satellite_buy_universe)}`,
      ``,
      `Current system selections`,
      `${fmtList(universe.current_system_selections)}`
    ].join("\n");
  }
}

function renderTradableUniverseError(message) {
  renderUniverseText("tradableUniverseStatus", `Server universe load failed: ${message}`, true);
  renderUniverseText("tradableUniverseSummary", "Unavailable.", true);
  renderUniverseText("tradableUniverseDrift", "Unable to compare local config against server state.", true);
  const listsEl = document.getElementById("tradableUniverseLists");
  if (listsEl) {
    listsEl.textContent = message;
  }
}

function renderStatus(data) {
  const badges = document.getElementById("statusBadges");
  const health = document.getElementById("serviceHealth");
  const cache = document.getElementById("cacheHealth");
  const diag = document.getElementById("diagnostics");
  const meta = document.getElementById("statusMeta");

  const service = data.service || "unknown";
  const status = data.status || "unknown";
  const cacheMeta = data._cache || {};
  const pc = data.portfolio_cache || {};

  if (badges) {
    badges.innerHTML = [
      `<span class="badge good">${status}</span>`,
      `<span class="badge accent">${service}</span>`,
      `<span class="badge">${cacheMeta.source || "unknown"}</span>`
    ].join("");
  }

  if (meta) {
    meta.textContent = `Operational state and cache controls • Updated ${fmtTs(data.time)}`;
  }

  if (health) {
    health.textContent = [
      `Service: ${service}`,
      `Status: ${status}`,
      `Portfolio cache: ${pc.ok === false ? "error" : "ok"}`,
      `Portfolio source: ${pc.source || "unknown"}`,
      `Total value: ${fmtUsd(pc.total_value_usd || 0)}`,
      `USD cash: ${fmtUsd(pc.usd_cash || 0)}`,
      `Snapshot timestamp: ${fmtTs(pc.timestamp)}`
    ].join("\n");
  }

  if (cache) {
    cache.textContent = [
      `Cache source: ${cacheMeta.source || "unknown"}`,
      `Cached at: ${fmtTs(cacheMeta.cached_at)}`,
      `TTL sec: ${cacheMeta.ttl_sec ?? "—"}`,
      `Stale sec: ${cacheMeta.stale_sec ?? "—"}`
    ].join("\n");
  }

  if (diag) {
    diag.textContent = JSON.stringify(data, null, 2);
  }
}

async function refreshCaches() {
  try {
    await fetchJson("/api/admin/cache/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ secret: API_SECRET })
    });
    await refreshSystemStatus();
  } catch (err) {
    console.error(err);
    renderText("cacheHealth", `Cache refresh failed: ${err.message}`, true);
  }
}

async function refreshTradableUniverse() {
  try {
    renderUniverseText("tradableUniverseStatus", "Fetching server tradable universe...");
    const [serverData, configData] = await Promise.all([
      fetchJson("/api/system/tradable_universe"),
      fetchJson("/api/config")
    ]);
    renderTradableUniverse(serverData, configData.config || {});
  } catch (err) {
    console.error(err);
    renderTradableUniverseError(err.message);
  }
}

async function refreshSystemStatus() {
  try {
    const data = await fetchJson("/api/status");
    renderStatus(data);
  } catch (err) {
    console.error(err);
    renderText("serviceHealth", `System status load failed: ${err.message}`, true);
    renderText("cacheHealth", "Unavailable.", true);
    renderText("diagnostics", err.stack || String(err), true);
  }

  await refreshTradableUniverse();
}

window.refreshSystemStatus = refreshSystemStatus;
window.refreshCaches = refreshCaches;
window.refreshTradableUniverse = refreshTradableUniverse;

refreshSystemStatus();
