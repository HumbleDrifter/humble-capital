const API_SECRET = (window.SYSTEM_STATUS_CONFIG && window.SYSTEM_STATUS_CONFIG.apiSecret) || "";
let CURRENT_TRADINGVIEW_MANIFEST = null;

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

function renderManifestText(id, text, isError = false) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text;
  el.className = isError ? "status-console error" : "status-console";
}

function setManifestActionStatus(message, isError = false) {
  renderManifestText("tradingViewManifestActionStatus", message, isError);
}

function manifestGroupValues(groupName) {
  const manifest = CURRENT_TRADINGVIEW_MANIFEST || {};
  const groups = manifest.strategy_groups || {};
  return normalizeSymbolList(groups[groupName] || []);
}

function manifestRawListsText() {
  const manifest = CURRENT_TRADINGVIEW_MANIFEST || {};
  const groups = manifest.strategy_groups || {};
  return [
    "core_buy",
    fmtList(groups.core_buy),
    "",
    "core_exit",
    fmtList(groups.core_exit),
    "",
    "satellite_buy",
    fmtList(groups.satellite_buy),
    "",
    "satellite_exit",
    fmtList(groups.satellite_exit),
    "",
    "sniper_buy",
    fmtList(groups.sniper_buy)
  ].join("\n");
}

async function copyTextToClipboard(text) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }

  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "absolute";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.select();
  document.execCommand("copy");
  document.body.removeChild(textarea);
}

async function copyManifestGroup(groupName) {
  const values = manifestGroupValues(groupName);
  if (!CURRENT_TRADINGVIEW_MANIFEST) {
    setManifestActionStatus("Fetch the TradingView manifest before copying strategy groups.", true);
    return;
  }
  if (!values.length) {
    setManifestActionStatus(`No symbols are available for ${groupName}.`, true);
    return;
  }

  try {
    await copyTextToClipboard(values.join(", "));
    setManifestActionStatus(`Copied ${groupName} (${values.length} symbol${values.length === 1 ? "" : "s"}).`);
  } catch (err) {
    console.error(err);
    setManifestActionStatus(`Copy failed for ${groupName}: ${err.message}`, true);
  }
}

async function copyManifestRawLists() {
  if (!CURRENT_TRADINGVIEW_MANIFEST) {
    setManifestActionStatus("Fetch the TradingView manifest before copying raw lists.", true);
    return;
  }

  try {
    await copyTextToClipboard(manifestRawListsText());
    setManifestActionStatus("Copied raw TradingView manifest lists.");
  } catch (err) {
    console.error(err);
    setManifestActionStatus(`Copy failed for raw lists: ${err.message}`, true);
  }
}

function exportManifestJson() {
  if (!CURRENT_TRADINGVIEW_MANIFEST) {
    setManifestActionStatus("Fetch the TradingView manifest before exporting JSON.", true);
    return;
  }

  try {
    const manifest = CURRENT_TRADINGVIEW_MANIFEST || {};
    const stamp = Number(manifest.generated_at || 0);
    const safeStamp = stamp ? new Date(stamp * 1000).toISOString().replace(/[:.]/g, "-") : "latest";
    const blob = new Blob([JSON.stringify(manifest, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `tradingview_manifest_${safeStamp}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    setManifestActionStatus("Exported TradingView manifest JSON.");
  } catch (err) {
    console.error(err);
    setManifestActionStatus(`Export failed: ${err.message}`, true);
  }
}

function renderTradingViewManifest(data) {
  const manifest = data || {};
  const summary = manifest.summary || {};
  const notes = manifest.notes || {};
  const groups = manifest.strategy_groups || {};
  const groupsHost = document.getElementById("tradingViewManifestGroups");
  const listsEl = document.getElementById("tradingViewManifestLists");
  CURRENT_TRADINGVIEW_MANIFEST = manifest;

  renderManifestText(
    "tradingViewManifestStatus",
    `TradingView manifest loaded. Generated ${fmtTs(manifest.generated_at)}${manifest.snapshot_timestamp ? ` • Snapshot ${fmtTs(manifest.snapshot_timestamp)}` : ""}.`
  );

  renderManifestText(
    "tradingViewManifestSummary",
    [
      `Generated: ${fmtTs(manifest.generated_at)}`,
      `Snapshot: ${fmtTs(manifest.snapshot_timestamp)}`,
      `Source: ${manifest.source || "—"} v${manifest.version ?? "—"}`,
      `Satellite mode: ${manifest.satellite_mode || "—"}`,
      `Core buy: ${summary.core_buy_count ?? normalizeSymbolList(groups.core_buy || []).length}`,
      `Core exit: ${summary.core_exit_count ?? normalizeSymbolList(groups.core_exit || []).length}`,
      `Satellite buy: ${summary.satellite_buy_count ?? normalizeSymbolList(groups.satellite_buy || []).length}`,
      `Satellite exit: ${summary.satellite_exit_count ?? normalizeSymbolList(groups.satellite_exit || []).length}`,
      `Sniper buy: ${summary.sniper_buy_count ?? normalizeSymbolList(groups.sniper_buy || []).length}`
    ].join("\n")
  );

  renderManifestText(
    "tradingViewManifestNotes",
    [
      notes.purpose || "Server-authoritative TradingView maintenance manifest.",
      notes.satellite_buy_definition || "",
      notes.satellite_exit_definition || "",
      notes.sniper_buy_definition || ""
    ].filter(Boolean).join("\n")
  );

  if (groupsHost) {
    const groupEntries = [
      ["Core Buy", groups.core_buy],
      ["Core Exit", groups.core_exit],
      ["Satellite Buy", groups.satellite_buy],
      ["Satellite Exit", groups.satellite_exit],
      ["Sniper Buy", groups.sniper_buy]
    ];

    groupsHost.innerHTML = groupEntries.map(([label, values]) => `
      <div class="system-manifest-card">
        <div class="system-universe-label">${label}</div>
        <div class="status-console">${fmtList(values)}</div>
      </div>
    `).join("");
  }

  if (listsEl) {
    listsEl.textContent = manifestRawListsText();
  }

  setManifestActionStatus("Manifest tools are ready. You can copy strategy groups or export JSON.");
}

function renderTradingViewManifestError(message) {
  CURRENT_TRADINGVIEW_MANIFEST = null;
  renderManifestText("tradingViewManifestStatus", `TradingView manifest load failed: ${message}`, true);
  renderManifestText("tradingViewManifestSummary", "Unavailable.", true);
  renderManifestText("tradingViewManifestNotes", "Unable to load server manifest notes.", true);
  setManifestActionStatus("Manifest tools are unavailable until a valid manifest is loaded.", true);
  const groupsHost = document.getElementById("tradingViewManifestGroups");
  if (groupsHost) {
    groupsHost.innerHTML = `<div class="status-console error">${message}</div>`;
  }
  const listsEl = document.getElementById("tradingViewManifestLists");
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

async function refreshTradingViewManifest() {
  try {
    renderManifestText("tradingViewManifestStatus", "Fetching TradingView manifest...");
    const data = await fetchJson("/api/system/tradingview_manifest");
    renderTradingViewManifest(data);
  } catch (err) {
    console.error(err);
    renderTradingViewManifestError(err.message);
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
  await refreshTradingViewManifest();
}

window.refreshSystemStatus = refreshSystemStatus;
window.refreshCaches = refreshCaches;
window.refreshTradableUniverse = refreshTradableUniverse;
window.refreshTradingViewManifest = refreshTradingViewManifest;
window.copyManifestGroup = copyManifestGroup;
window.copyManifestRawLists = copyManifestRawLists;
window.exportManifestJson = exportManifestJson;

refreshSystemStatus();
