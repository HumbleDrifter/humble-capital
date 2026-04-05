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

function fmtNumber(v) {
  return Number(v || 0).toLocaleString(undefined, {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1
  });
}

function numericOrNull(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function resolve24hMove(row) {
  const candidates = [
    row.price_change_24h,
    row.price_change_24h_pct,
    row.change_24h,
    row.move_24h
  ];

  for (const value of candidates) {
    const numeric = numericOrNull(value);
    if (numeric != null) return numeric;
  }

  return null;
}

function resolveUniverseCount(data) {
  const explicitCount = numericOrNull(data.candidate_count);
  if (explicitCount != null) return explicitCount;

  if (Array.isArray(data.active_satellite_buy_universe)) {
    return data.active_satellite_buy_universe.length;
  }

  if (Array.isArray(data.candidates)) {
    return data.candidates.length;
  }

  return null;
}

function sortCandidates(rows, sortKey) {
  const items = [...rows];

  if (sortKey === "weight") {
    items.sort((a, b) => Number(b.portfolio_weight || b.weight || 0) - Number(a.portfolio_weight || a.weight || 0));
  } else if (sortKey === "held_value") {
    items.sort((a, b) => Number(b.held_value_usd || 0) - Number(a.held_value_usd || 0));
  } else if (sortKey === "change_24h") {
    items.sort((a, b) => (resolve24hMove(b) ?? Number.NEGATIVE_INFINITY) - (resolve24hMove(a) ?? Number.NEGATIVE_INFINITY));
  } else {
    items.sort((a, b) => Number(b.score || 0) - Number(a.score || 0));
  }

  return items;
}

function opportunityTone(score) {
  const s = Number(score || 0);
  if (s >= 90) return "high";
  if (s >= 75) return "strong";
  if (s >= 50) return "building";
  return "early";
}

function scoreLabel(score) {
  const s = Number(score || 0);
  if (s >= 85) return "High";
  if (s >= 65) return "Medium";
  return "Developing";
}

function normalizeRegime(value) {
  const normalized = String(value || "unknown")
    .replaceAll("_", " ")
    .trim()
    .toLowerCase();

  if (!normalized || normalized === "unknown") return "Unknown";
  if (normalized === "bull") return "Bullish";
  if (normalized === "bear") return "Bearish";
  if (normalized === "neutral") return "Neutral";
  if (normalized === "risk off") return "Risk Off";

  return normalized
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function regimeTone(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "bull") return "bull";
  if (normalized === "bear") return "bear";
  if (normalized === "neutral") return "neutral";
  if (normalized === "risk_off") return "risk-off";
  return "unknown";
}

function assetTypeForCandidate(row) {
  const cls = String(row.class || "").trim().toLowerCase();
  const productId = String(row.product_id || "").trim().toUpperCase();

  if (cls.includes("option")) return "Options";
  if (cls.includes("future")) return "Futures";
  if (cls.includes("stock") || cls.includes("equity")) return "Stock";
  if (cls.includes("crypto")) return "Crypto";
  if (productId.includes("-")) return "Crypto";
  return "Asset";
}

function signalLabel(row) {
  return String(row.source || row.strategy || "Scanner").trim() || "Scanner";
}

function statusText(row) {
  if (row.blocked || row.enabled === false) return "Paused";
  if (row.core) return "Core (Portfolio)";
  if (row.held) return "Live";
  if (row.allowed) return "Allowed";
  if (row.active_buy_universe) return "Ready";
  return "Watching";
}

function groupForCandidate(row) {
  if (row.blocked || row.enabled === false) return "paused";
  if (row.held || row.allowed || row.active_buy_universe || row.core) return "active";
  return "watching";
}

function groupTitle(groupKey) {
  if (groupKey === "active") return "Active";
  if (groupKey === "paused") return "Paused";
  return "Watching";
}

function groupDescription(groupKey) {
  if (groupKey === "active") return "Live, allowed, or ready-to-enter opportunities worth active operator attention.";
  if (groupKey === "paused") return "Opportunities intentionally blocked or otherwise taken out of active rotation.";
  return "Candidates being monitored without an active portfolio or allowlist commitment.";
}

function emptyStateText(groupKey) {
  if (groupKey === "active") return "No currently active opportunities are standing out right now.";
  if (groupKey === "paused") return "No opportunities are currently paused or blocked.";
  return "No candidates are being monitored right now.";
}

function actionButtons(row) {
  const productId = String(row.product_id || "");
  const safeId = escapeHtml(productId);
  const mode = row.blocked ? "disable" : row.allowed ? "enable" : "auto";

  return `
    <div class="opportunity-actions">
      <button class="btn ${mode === "enable" ? "btn-primary" : "btn-secondary"} opportunity-action-btn" type="button" onclick="setOpportunityMode('${safeId}','enable')">Enable</button>
      <button class="btn ${mode === "auto" ? "btn-primary" : "btn-secondary"} opportunity-action-btn" type="button" onclick="setOpportunityMode('${safeId}','auto')">Auto</button>
      <button class="btn ${mode === "disable" ? "btn-primary" : "btn-secondary"} opportunity-action-btn" type="button" onclick="setOpportunityMode('${safeId}','disable')">Disable</button>
    </div>
  `;
}

function flagPills(row) {
  const flags = [];
  if (row.allowed) flags.push('<span class="pill">allowed</span>');
  if (row.blocked) flags.push('<span class="pill">blocked</span>');
  if (row.core) flags.push('<span class="pill">core</span>');
  if (row.active_buy_universe) flags.push('<span class="pill">ready</span>');
  return flags.join("");
}

function renderSummary(data, groups) {
  const host = document.getElementById("opportunitySummary");
  if (!host) return;
  const regimeText = normalizeRegime(data.market_regime || "unknown");
  const regimeClass = regimeTone(data.market_regime || "unknown");

  host.innerHTML = `
    <div class="opportunity-summary-card">
      <div class="opportunity-summary-label">Active</div>
      <div class="opportunity-summary-value">${groups.active.length}</div>
    </div>
    <div class="opportunity-summary-card">
      <div class="opportunity-summary-label">Watching</div>
      <div class="opportunity-summary-value">${groups.watching.length}</div>
    </div>
    <div class="opportunity-summary-card">
      <div class="opportunity-summary-label">Paused</div>
      <div class="opportunity-summary-value">${groups.paused.length}</div>
    </div>
    <div class="opportunity-summary-card">
      <div class="opportunity-summary-label">Regime</div>
      <div class="opportunity-summary-value">
        <span class="opportunity-summary-regime ${escapeHtml(regimeClass)}">${escapeHtml(regimeText)}</span>
      </div>
    </div>
  `;
}

function renderScoreLegend(data) {
  const host = document.getElementById("opportunityScoreLegend");
  if (!host) return;

  const rows = Array.isArray(data.candidates) ? data.candidates : [];
  const topScore = rows.reduce((best, row) => Math.max(best, Number(row.score || 0)), 0);

  host.innerHTML = `
    <div class="opportunity-score-legend-head">
      <div>
        <div class="opportunity-summary-label">Top Score Right Now</div>
        <div class="opportunity-summary-value">${fmtNumber(topScore)}</div>
      </div>
      <div class="opportunity-score-legend-bands">
        <span class="opportunity-score-legend-band high">85+ = High conviction</span>
        <span class="opportunity-score-legend-band strong">65-84 = Building setup</span>
        <span class="opportunity-score-legend-band early">Below 65 = Early / lower confidence</span>
      </div>
    </div>
    <p class="opportunity-score-legend-note">Higher scores indicate stronger opportunity quality based on current scanner inputs.</p>
  `;
}

function renderScannerStatus(systemData) {
  const summaryEl = document.getElementById("scannerStatusSummary");
  const toggleBtn = document.getElementById("scannerToggleBtn");
  const known = typeof systemData?.admin_state?.meme_rotation_enabled === "boolean";
  const enabled = Boolean(systemData?.admin_state?.meme_rotation_enabled);

  if (summaryEl) {
    summaryEl.textContent = !known
      ? "Scanner state unavailable. Toggle still uses the live scanner control."
      : enabled
        ? "Scanner is live and evaluating opportunities."
        : "Scanner is paused. Existing records remain visible for review.";
  }

  if (toggleBtn) {
    toggleBtn.textContent = !known ? "Toggle Scanner" : enabled ? "Pause Scanner" : "Resume Scanner";
    toggleBtn.className = `btn ${enabled ? "btn-secondary" : "btn-primary"}`;
    toggleBtn.dataset.enabled = enabled ? "true" : "false";
  }
}

function renderStatus(data, systemData) {
  const status = document.getElementById("heatmapStatus");
  if (!status) return;

  const cache = data._cache?.source || "unknown";
  const universeCount = resolveUniverseCount(data);
  const scannerEnabled = Boolean(systemData?.admin_state?.meme_rotation_enabled);

  status.innerHTML = `
    <span class="badge accent">${escapeHtml(cache)}</span>
    <span class="badge good">universe ${universeCount == null ? "—" : escapeHtml(String(universeCount))}</span>
    <span class="badge ${scannerEnabled ? "good" : "warn"}">${scannerEnabled ? "scanner live" : "scanner paused"}</span>
  `;
}

function opportunityCard(row) {
  const productId = row.product_id || row.symbol || "—";
  const tone = opportunityTone(row.score);
  const move24h = resolve24hMove(row);
  return `
    <article class="opportunity-card ${tone}">
      <div class="opportunity-card-head">
        <div>
          <div class="opportunity-symbol">${escapeHtml(productId)}</div>
          <div class="opportunity-subline">
            <span class="badge">${escapeHtml(assetTypeForCandidate(row))}</span>
            <span class="tiny">${escapeHtml(signalLabel(row))}</span>
          </div>
        </div>
        <div class="opportunity-score-wrap">
          <div class="opportunity-score">${fmtNumber(row.score)}</div>
          <div class="tiny">${escapeHtml(scoreLabel(row.score))} confidence</div>
        </div>
      </div>

      <div class="opportunity-pill-row">
        <span class="pill">${escapeHtml(statusText(row))}</span>
        ${flagPills(row)}
      </div>

        <div class="opportunity-metrics">
        <div class="opportunity-metric">
          <span class="opportunity-metric-label">Target Allocation</span>
          <strong>${fmtPct(row.portfolio_weight || 0, false)}</strong>
        </div>
        <div class="opportunity-metric">
          <span class="opportunity-metric-label">Held Value</span>
          <strong>${fmtUsd(row.held_value_usd || 0)}</strong>
        </div>
        <div class="opportunity-metric">
          <span class="opportunity-metric-label">24H Move</span>
          <strong>${move24h == null ? "N/A" : fmtPct(move24h)}</strong>
        </div>
        <div class="opportunity-metric">
          <span class="opportunity-metric-label">Unrealized</span>
          <strong>${fmtPct(row.unrealized_pnl_pct || 0)}</strong>
        </div>
      </div>

      <div class="divider"></div>

      ${actionButtons(row)}
    </article>
  `;
}

function renderGroups(data) {
  const meta = document.getElementById("heatmapMeta");
  const grid = document.getElementById("heatmapGrid");
  const sortKey = document.getElementById("heatmapSort")?.value || "score";

  if (!grid) return;

  const rows = Array.isArray(data.candidates) ? data.candidates : [];
  const sorted = sortCandidates(rows, sortKey);
  const groups = { active: [], watching: [], paused: [] };

  for (const row of sorted) {
    groups[groupForCandidate(row)].push(row);
  }

  if (meta) {
    meta.textContent = `${sorted.length} opportunity candidate(s) loaded • regime ${normalizeRegime(data.market_regime || "unknown")}`;
  }

  renderSummary(data, groups);

  grid.className = "";
  grid.innerHTML = `
    <div class="opps-board">
      ${["active", "watching", "paused"].map((groupKey) => {
        const items = groups[groupKey];
        return `
          <section class="opps-section opps-section-${groupKey}">
            <div class="section-header compact-header">
              <div>
                <h3>${groupTitle(groupKey)}</h3>
                <p class="section-subtitle">${groupDescription(groupKey)}</p>
              </div>
              <span class="badge">${items.length}</span>
            </div>
            <div class="opps-cards">
              ${items.length
                ? items.map((row) => opportunityCard(row)).join("")
                : `<div class="opportunity-empty muted">${escapeHtml(emptyStateText(groupKey))}</div>`
              }
            </div>
          </section>
        `;
      }).join("")}
    </div>
  `;
}

async function postJson(path, body) {
  return fetchJson(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...body, ...(API_SECRET ? { secret: API_SECRET } : {}) })
  }, 15000);
}

async function setOpportunityMode(productId, mode) {
  try {
    if (mode === "enable") {
      await postJson("/api/admin/satellite/block", { product_id: productId, action: "remove" });
      await postJson("/api/admin/satellite/allow", { product_id: productId, action: "add" });
    } else if (mode === "disable") {
      await postJson("/api/admin/satellite/allow", { product_id: productId, action: "remove" });
      await postJson("/api/admin/satellite/block", { product_id: productId, action: "add" });
    } else {
      await postJson("/api/admin/satellite/allow", { product_id: productId, action: "remove" });
      await postJson("/api/admin/satellite/block", { product_id: productId, action: "remove" });
    }

    await refreshMemeRotation();
  } catch (err) {
    console.error(err);
    const status = document.getElementById("heatmapStatus");
    if (status) {
      status.innerHTML = `<span class="badge bad">${escapeHtml(err.message)}</span>`;
    }
  }
}

async function toggleOpportunityScanner() {
  const button = document.getElementById("scannerToggleBtn");
  const enabled = String(button?.dataset.enabled || "false") === "true";

  try {
    await postJson("/api/admin/meme_rotation", { enabled: !enabled });
    await refreshMemeRotation();
  } catch (err) {
    console.error(err);
    const summaryEl = document.getElementById("scannerStatusSummary");
    if (summaryEl) {
      summaryEl.textContent = `Scanner update failed: ${err.message}`;
    }
  }
}

async function refreshMemeRotation() {
  try {
    const [data, systemData] = await Promise.all([
      fetchJson("/api/meme_rotation"),
      fetchJson("/api/system_snapshot").catch(() => ({}))
    ]);
    renderStatus(data, systemData || {});
    renderScannerStatus(systemData || {});
    renderScoreLegend(data);
    renderGroups(data);
  } catch (err) {
    console.error(err);
    const grid = document.getElementById("heatmapGrid");
    if (grid) {
      grid.innerHTML = `<div class="status-console error">Opportunities load failed: ${escapeHtml(err.message)}</div>`;
    }
  }
}

window.refreshMemeRotation = refreshMemeRotation;
window.setOpportunityMode = setOpportunityMode;
window.toggleOpportunityScanner = toggleOpportunityScanner;

refreshMemeRotation();
