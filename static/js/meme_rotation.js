const API_SECRET = (window.MEME_ROTATION_CONFIG && window.MEME_ROTATION_CONFIG.apiSecret) || "";

function authUrl(path) {
  if (!API_SECRET) return path;
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}secret=${encodeURIComponent(API_SECRET)}`;
}

async function fetchJson(path, options = {}, timeoutMs = 12000) {
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

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function fmtUsd(v) {
  return Number(v || 0).toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2
  });
}

function fmtPct(v, alreadyPercent = true) {
  const raw = Number(v || 0);
  const value = alreadyPercent ? raw : raw * 100;
  return `${value.toFixed(2)}%`;
}

function candidateTone(score) {
  const s = Number(score || 0);
  if (s >= 90) return "rgba(22, 163, 74, 0.30)";
  if (s >= 75) return "rgba(34, 197, 94, 0.24)";
  if (s >= 50) return "rgba(74, 222, 128, 0.18)";
  return "rgba(21, 128, 61, 0.12)";
}

function sortCandidates(rows, sortKey) {
  const items = [...rows];

  if (sortKey === "weight") {
    items.sort((a, b) => Number(b.portfolio_weight || b.weight || 0) - Number(a.portfolio_weight || a.weight || 0));
  } else if (sortKey === "held_value") {
    items.sort((a, b) => Number(b.held_value_usd || 0) - Number(a.held_value_usd || 0));
  } else if (sortKey === "change_24h") {
    items.sort((a, b) => Number(b.change_24h || b.move_24h || 0) - Number(a.change_24h || a.move_24h || 0));
  } else {
    items.sort((a, b) => Number(b.score || 0) - Number(a.score || 0));
  }

  return items;
}

function renderHeatmap(data) {
  const meta = document.getElementById("heatmapMeta");
  const status = document.getElementById("heatmapStatus");
  const grid = document.getElementById("heatmapGrid");
  const sortKey = document.getElementById("heatmapSort")?.value || "score";

  if (!grid) return;

  const rows = Array.isArray(data.candidates) ? data.candidates : [];
  const sorted = sortCandidates(rows, sortKey);

  if (meta) {
    const regime = data.market_regime || "unknown";
    meta.textContent = `${sorted.length} candidate(s) loaded • regime ${regime}`;
  }

  if (status) {
    const cache = data._cache?.source || "unknown";
    const universeCount = Array.isArray(data.active_satellite_buy_universe)
      ? data.active_satellite_buy_universe.length
      : 0;

    status.innerHTML = `
      <span class="badge accent">${escapeHtml(cache)}</span>
      <span class="badge good">universe ${universeCount}</span>
    `;
  }

  grid.innerHTML = sorted.length ? sorted.map((row) => {
    const productId = row.product_id || row.symbol || "—";
    const score = Number(row.score || 0);
    const source = row.source || row.strategy || "hunter";
    const heldValue = Number(row.held_value_usd || 0);
    const weight = Number(row.portfolio_weight || row.weight || 0);
    const move1h = Number(row.change_1h || row.move_1h || 0);
    const move24h = Number(row.change_24h || row.move_24h || 0);
    const pnl = Number(row.unrealized_pnl_pct || 0);
    const statusText = row.status || (heldValue > 0 ? "Held" : "Watching");
    const flags = [
      row.allowed ? '<span class="pill">allowed</span>' : '',
      row.blocked ? '<span class="pill">blocked</span>' : '',
      row.core ? '<span class="pill">core</span>' : '',
      row.active_buy_universe ? '<span class="pill">active buy</span>' : ''
    ].filter(Boolean).join('');

    return `
      <div class="tile" style="background:${candidateTone(score)}; border:1px solid rgba(34,197,94,0.18);">
        <div style="display:flex; justify-content:space-between; align-items:flex-start; gap:10px; margin-bottom:10px;">
          <div>
            <div style="font-size:18px; font-weight:800;">${escapeHtml(productId)}</div>
            <div class="tiny">${escapeHtml(source)}</div>
          </div>
          <span class="badge good">Score ${score.toFixed(1)}</span>
        </div>

        <div class="pill-wrap" style="margin-bottom:10px;">
          <span class="pill">${escapeHtml(statusText)}</span>
          ${flags}
        </div>

        <div class="statusline" style="display:grid; grid-template-columns:1fr 1fr; gap:8px;">
          <div><span class="tiny">Weight</span><br><strong>${fmtPct(weight, false)}</strong></div>
          <div><span class="tiny">Held Value</span><br><strong>${fmtUsd(heldValue)}</strong></div>
          <div><span class="tiny">1H</span><br><strong>${fmtPct(move1h)}</strong></div>
          <div><span class="tiny">24H</span><br><strong>${fmtPct(move24h)}</strong></div>
          <div><span class="tiny">Unrealized</span><br><strong>${fmtPct(pnl)}</strong></div>
        </div>
      </div>
    `;
  }).join("") : `<div class="muted">No Meme Hunter candidates found.</div>`;
}

async function refreshMemeRotation() {
  try {
    const data = await fetchJson("/api/meme_rotation");
    renderHeatmap(data);
  } catch (err) {
    console.error(err);
    const grid = document.getElementById("heatmapGrid");
    if (grid) {
      grid.innerHTML = `<div class="status-console error">Meme Hunter load failed: ${escapeHtml(err.message)}</div>`;
    }
  }
}

window.refreshMemeRotation = refreshMemeRotation;
refreshMemeRotation();
