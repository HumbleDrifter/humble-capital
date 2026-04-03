const API_SECRET = (window.SYSTEM_STATUS_CONFIG && window.SYSTEM_STATUS_CONFIG.apiSecret) || "";

function authUrl(path) {
  if (!API_SECRET) return path;
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}secret=${encodeURIComponent(API_SECRET)}`;
}

async function fetchJson(path, options = {}) {
  const res = await fetch(authUrl(path), options);
  const data = await res.json();

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
}

window.refreshSystemStatus = refreshSystemStatus;
window.refreshCaches = refreshCaches;

refreshSystemStatus();
