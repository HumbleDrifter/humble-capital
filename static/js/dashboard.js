const API_SECRET = (window.DASHBOARD_CONFIG && window.DASHBOARD_CONFIG.apiSecret) || "";
let AUTO_REFRESH_MS = 120000;
let refreshTimer = null;
let selectedHistoryRange = "30d";

function authUrl(path) {
  if (!API_SECRET) return path;
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}secret=${encodeURIComponent(API_SECRET)}`;
}

async function fetchJson(path, options = {}, timeoutMs = 15000) {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);

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
    window.clearTimeout(timer);
  }
}

async function fetchJsonSafe(path, options = {}, timeoutMs = 15000) {
  try {
    const data = await fetchJson(path, options, timeoutMs);
    return { ok: true, data, error: null, path };
  } catch (err) {
    console.error(`Dashboard fetch failed for ${path}:`, err);
    return {
      ok: false,
      data: null,
      error: String(err && err.message ? err.message : err),
      path
    };
  }
}

function fmtUsd(v) {
  return Number(v || 0).toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2
  });
}

function fmtPct(v, alreadyPercent = false) {
  const raw = Number(v || 0);
  const value = alreadyPercent ? raw : raw * 100;
  return `${value.toFixed(2)}%`;
}

function fmtQty(v) {
  return Number(v || 0).toLocaleString(undefined, {
    maximumFractionDigits: 8
  });
}

function fmtSignedUsd(v) {
  const value = Number(v || 0);
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${fmtUsd(value)}`;
}

function formatUnixTime(ts) {
  const n = Number(ts || 0);
  if (!n) return "—";
  return new Date(n * 1000).toLocaleString();
}

function formatShortDate(ts) {
  const n = Number(ts || 0);
  if (!n) return "";
  return new Date(n * 1000).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric"
  });
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function cacheBadge(cache) {
  if (!cache) return '<span class="badge">no-cache-meta</span>';

  const src = String(cache.source || "unknown").toLowerCase();

  if (src === "live") return '<span class="badge good">live</span>';
  if (src === "fresh-cache") return '<span class="badge accent">fresh-cache</span>';
  if (src === "stale-cache") return '<span class="badge warn">stale-cache</span>';
  if (src === "fallback") return '<span class="badge warn">fallback</span>';

  return `<span class="badge">${escapeHtml(src)}</span>`;
}

function setUiRefreshNow() {
  const el = document.getElementById("uiRefresh");
  if (el) el.textContent = new Date().toLocaleTimeString();
}

function setSecretMode() {
  const el = document.getElementById("secretMode");
  if (!el) return;
  el.textContent = API_SECRET ? "secret+session" : "session";
}

function setStatusBadges(items) {
  const el = document.getElementById("statusBadges");
  if (!el) return;
  el.innerHTML = items.join("");
}

function displayClass(cls) {
  if (cls === "core") return "core";
  if (cls === "satellite_active") return "satellite";
  if (cls === "satellite_blocked") return "blocked";
  if (cls === "dust") return "remainder";
  if (cls === "nontradable") return "nontradable";
  return cls || "—";
}

function buildSvgLine(points, key, labelFormatter = fmtUsd) {
  const width = 900;
  const height = 220;
  const pad = 24;

  const values = points.map((p) => Number(p[key] || 0));
  if (!values.length) {
    return `<div class="muted">No chart points available.</div>`;
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = Math.max(1e-9, max - min);

  const coords = points.map((p, i) => {
    const x = pad + (i * (width - pad * 2)) / Math.max(1, points.length - 1);
    const y = height - pad - ((Number(p[key] || 0) - min) / spread) * (height - pad * 2);
    return [x, y];
  });

  const d = coords.map((c, i) => `${i === 0 ? "M" : "L"} ${c[0]} ${c[1]}`).join(" ");

  return `
    <svg viewBox="0 0 ${width} ${height}" style="width:100%; height:220px; display:block;">
      <rect x="0" y="0" width="${width}" height="${height}" rx="18" ry="18" fill="rgba(255,255,255,0.02)"></rect>
      <line x1="${pad}" y1="${height - pad}" x2="${width - pad}" y2="${height - pad}" stroke="rgba(147,160,184,0.25)" />
      <line x1="${pad}" y1="${pad}" x2="${pad}" y2="${height - pad}" stroke="rgba(147,160,184,0.25)" />
      <path d="${d}" fill="none" stroke="rgba(52,211,153,1)" stroke-width="3" stroke-linecap="round" />
      <text x="${pad}" y="${pad - 6}" fill="rgba(147,160,184,0.95)" font-size="12">Min: ${labelFormatter(min)}</text>
      <text x="${width - 180}" y="${pad - 6}" fill="rgba(147,160,184,0.95)" font-size="12">Max: ${labelFormatter(max)}</text>
    </svg>
  `;
}

function rangeLabel(rangeName) {
  if (rangeName === "7d") return "7D";
  if (rangeName === "90d") return "90D";
  return "30D";
}

function buildTrendSvg(points, key) {
  const width = 960;
  const height = 280;
  const padTop = 24;
  const padRight = 20;
  const padBottom = 38;
  const padLeft = 18;
  const values = points.map((p) => Number(p[key] || 0));

  if (!values.length) {
    return `<div class="trend-chart-empty">No portfolio history available.</div>`;
  }

  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = Math.max(1e-9, max - min);

  const coords = points.map((p, i) => {
    const x = padLeft + (i * (width - padLeft - padRight)) / Math.max(1, points.length - 1);
    const y = padTop + (max - Number(p[key] || 0)) * ((height - padTop - padBottom) / spread);
    return [x, y];
  });

  const linePath = coords.map((c, i) => `${i === 0 ? "M" : "L"} ${c[0]} ${c[1]}`).join(" ");
  const areaPath = `${linePath} L ${coords[coords.length - 1][0]} ${height - padBottom} L ${coords[0][0]} ${height - padBottom} Z`;

  const firstTs = points[0]?.ts || 0;
  const lastTs = points[points.length - 1]?.ts || 0;

  return `
    <svg viewBox="0 0 ${width} ${height}" aria-label="Portfolio value trend chart">
      <defs>
        <linearGradient id="trendFill" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stop-color="rgba(52,211,153,0.42)"></stop>
          <stop offset="100%" stop-color="rgba(52,211,153,0.02)"></stop>
        </linearGradient>
      </defs>
      <rect x="0" y="0" width="${width}" height="${height}" rx="18" ry="18" fill="rgba(255,255,255,0.02)"></rect>
      <line x1="${padLeft}" y1="${height - padBottom}" x2="${width - padRight}" y2="${height - padBottom}" stroke="rgba(147,160,184,0.18)"></line>
      <line x1="${padLeft}" y1="${padTop}" x2="${padLeft}" y2="${height - padBottom}" stroke="rgba(147,160,184,0.12)"></line>
      <path d="${areaPath}" fill="url(#trendFill)"></path>
      <path d="${linePath}" fill="none" stroke="rgba(52,211,153,1)" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round"></path>
      <circle cx="${coords[coords.length - 1][0]}" cy="${coords[coords.length - 1][1]}" r="5" fill="rgba(52,211,153,1)"></circle>
      <text x="${padLeft}" y="${padTop - 6}" fill="rgba(147,160,184,0.95)" font-size="12">Low ${fmtUsd(min)}</text>
      <text x="${width - 142}" y="${padTop - 6}" fill="rgba(147,160,184,0.95)" font-size="12">High ${fmtUsd(max)}</text>
      <text x="${padLeft}" y="${height - 12}" fill="rgba(147,160,184,0.8)" font-size="12">${escapeHtml(formatShortDate(firstTs))}</text>
      <text x="${width - 92}" y="${height - 12}" fill="rgba(147,160,184,0.8)" font-size="12">${escapeHtml(formatShortDate(lastTs))}</text>
    </svg>
  `;
}

function setTrendRangeButtons(activeRange) {
  document.querySelectorAll(".trend-range-btn").forEach((button) => {
    const isActive = button.dataset.range === activeRange;
    button.classList.toggle("active", isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
}

function normalizeAssetRows(portfolioData) {
  const snapshot = portfolioData?.snapshot || {};
  const summary = portfolioData?.summary || {};

  if (summary.assets && typeof summary.assets === "object") {
    return Object.values(summary.assets).map((row) => ({
      product_id: row.product_id || "—",
      class: row.class || "",
      value_total_usd: Number(row.value_total_usd || 0),
      weight_total: Number(row.weight_total || 0),
      price_usd: Number(row.price_usd || 0),
      base_qty_total: Number(row.base_qty_total || 0)
    }));
  }

  if (snapshot.positions && typeof snapshot.positions === "object") {
    return Object.entries(snapshot.positions).map(([productId, row]) => ({
      product_id: productId,
      class: row.class || "",
      value_total_usd: Number(row.value_total_usd || 0),
      weight_total: Number(row.weight_total || 0),
      price_usd: Number(row.price_usd || 0),
      base_qty_total: Number(row.base_qty_total || 0)
    }));
  }

  return [];
}

function sumField(rows, field) {
  return rows.reduce((sum, row) => sum + Number(row[field] || 0), 0);
}

function renderAccountValueHistory(data) {
  const host = document.getElementById("accountValueChartHost");
  const meta = document.getElementById("accountValueMeta");
  if (!host) return;

  const points = Array.isArray(data?.points) ? data.points : [];
  const rangeText = rangeLabel(data?.range || selectedHistoryRange);
  const seriesType = data?.series_type === "realized_pnl" ? "realized_pnl" : data?.series_type === "portfolio_value" ? "portfolio_value" : "empty";
  const valueKey = seriesType === "realized_pnl" ? "realized_pnl" : "equity_usd";
  const valueFormatter = seriesType === "realized_pnl" ? fmtSignedUsd : fmtUsd;
  const currentLabel = seriesType === "realized_pnl" ? "Current Realized PnL" : "Current Value";
  const deltaLabel = seriesType === "realized_pnl" ? "Range PnL Change" : "Range Change";
  const sourceLabel = seriesType === "realized_pnl" ? "Realized PnL" : "Portfolio Value";

  if (!points.length || seriesType === "empty") {
    if (meta) meta.textContent = `${rangeText} view • no portfolio history available yet`;
    host.innerHTML = `<div class="trend-chart-shell"><div class="trend-chart-empty">No portfolio history data found for this range.</div></div>`;
    return;
  }

  const firstValue = Number(points[0]?.[valueKey] || 0);
  const lastValue = Number(points[points.length - 1]?.[valueKey] || 0);
  const deltaValue = lastValue - firstValue;

  if (meta) {
    meta.textContent = `${rangeText} view • ${points.length} point(s) • ${sourceLabel}`;
  }

  host.innerHTML = `
    <div class="trend-chart-shell">
      <div class="trend-chart-summary">
        <div class="trend-stat">
          <div class="trend-stat-label">${currentLabel}</div>
          <div class="trend-stat-value">${valueFormatter(lastValue)}</div>
        </div>
        <div class="trend-stat">
          <div class="trend-stat-label">${deltaLabel}</div>
          <div class="trend-stat-value ${deltaValue >= 0 ? "positive" : "negative"}">${fmtSignedUsd(deltaValue)}</div>
        </div>
        <div class="trend-stat">
          <div class="trend-stat-label">Range</div>
          <div class="trend-stat-value">${rangeText}</div>
        </div>
      </div>
      ${buildTrendSvg(points, valueKey)}
    </div>
  `;
}

function renderPortfolio(data) {
  const snapshot = data?.snapshot || {};
  const summary = data?.summary || {};
  const rows = normalizeAssetRows(data).sort(
    (a, b) => Number(b.value_total_usd || 0) - Number(a.value_total_usd || 0)
  );

  const rowsValue = sumField(rows, "value_total_usd");

  const totalValue =
    Number(summary.total_value_usd || 0) ||
    Number(snapshot.total_value_usd || 0) ||
    (rowsValue + Number(summary.usd_cash || snapshot.usd_cash || 0));

  const usdCash = Number(summary.usd_cash || snapshot.usd_cash || 0);

  const coreWeight =
    Number(summary.core_weight || 0) ||
    Number(snapshot.core_weight || 0);

  const satWeight =
    Number(summary.satellite_weight || 0) ||
    Number(snapshot.satellite_weight || 0);

  const cashWeight =
    Number(summary.cash_weight || 0) ||
    Number(snapshot.cash_weight || 0) ||
    (totalValue > 0 ? usdCash / totalValue : 0);

  const kpiTotal = document.getElementById("kpiTotal");
  const kpiCash = document.getElementById("kpiCash");
  const kpiCore = document.getElementById("kpiCore");
  const kpiSat = document.getElementById("kpiSat");

  if (kpiTotal) kpiTotal.textContent = fmtUsd(totalValue);
  if (kpiCash) kpiCash.textContent = fmtUsd(usdCash);
  if (kpiCore) kpiCore.textContent = fmtPct(coreWeight);
  if (kpiSat) kpiSat.textContent = fmtPct(satWeight);

  const allocationText = document.getElementById("allocationText");
  if (allocationText) {
    allocationText.textContent =
      `Cash ${fmtPct(cashWeight)} | Core ${fmtPct(coreWeight)} | Satellite ${fmtPct(satWeight)}`;
  }

  const segCash = document.getElementById("segCash");
  const segCore = document.getElementById("segCore");
  const segSat = document.getElementById("segSat");

  if (segCash) {
    segCash.style.width = `${Math.max(0, cashWeight * 100)}%`;
    segCash.textContent = cashWeight > 0.08 ? "Cash" : "";
  }
  if (segCore) {
    segCore.style.width = `${Math.max(0, coreWeight * 100)}%`;
    segCore.textContent = coreWeight > 0.08 ? "Core" : "";
  }
  if (segSat) {
    segSat.style.width = `${Math.max(0, satWeight * 100)}%`;
    segSat.textContent = satWeight > 0.08 ? "Satellite" : "";
  }

  const portfolioMeta = document.getElementById("portfolioMeta");
  if (portfolioMeta) {
    portfolioMeta.innerHTML =
      `${cacheBadge(data?._cache)} <span class="tiny">snapshot: ${formatUnixTime(summary.timestamp || snapshot.timestamp)}</span>`;
  }

  const cashBreakdown = summary.cash_breakdown || snapshot.cash_breakdown || {};
  const cashUsd = document.getElementById("cashUsd");
  const cashUsdc = document.getElementById("cashUsdc");
  const cashUsdt = document.getElementById("cashUsdt");
  const cashTotal = document.getElementById("cashTotal");

  if (cashUsd) cashUsd.textContent = fmtUsd(cashBreakdown.USD || 0);
  if (cashUsdc) cashUsdc.textContent = fmtUsd(cashBreakdown.USDC || 0);
  if (cashUsdt) cashUsdt.textContent = fmtUsd(cashBreakdown.USDT || 0);
  if (cashTotal) cashTotal.textContent = fmtUsd(usdCash);

  const tbody = document.getElementById("portfolioTable");
  if (!tbody) return;

  tbody.innerHTML = rows.length
    ? rows.map((row) => {
        const cls = displayClass(row.class || "");
        const productId = row.product_id || "—";
        const valueUsd = Number(row.value_total_usd || 0);
        const weight = Number(row.weight_total || 0);
        const priceUsd = Number(row.price_usd || 0);
        const qty = Number(row.base_qty_total || 0);

        return `
          <tr>
            <td>${escapeHtml(productId)}</td>
            <td class="right">${fmtQty(qty)}</td>
            <td class="right">${fmtUsd(priceUsd)}</td>
            <td class="right">${fmtUsd(valueUsd)}</td>
            <td class="right">${fmtPct(weight)}</td>
            <td>${escapeHtml(cls)}</td>
          </tr>
        `;
      }).join("")
    : `<tr><td colspan="6" class="muted">No portfolio holdings found.</td></tr>`;
}

function renderActiveBuyUniverse(source) {
  const host = document.getElementById("activeBuyUniverse");
  if (!host) return;

  const snapshot = source?.portfolio || source?.snapshot || source || {};
  const symbols = Array.isArray(snapshot.active_satellite_buy_universe)
    ? snapshot.active_satellite_buy_universe
    : [];

  host.innerHTML = symbols.length
    ? symbols.map((sym) => `<span class="pill">${escapeHtml(sym)}</span>`).join("")
    : `<div class="muted">No active satellite buy universe entries available.</div>`;
}

function buyAmount(x) {
  return Number(
    x.quote_size_usd ??
    x.buy_usd ??
    x.target_buy_usd ??
    x.trade_value_usd ??
    x.amount_usd ??
    0
  );
}

function trimAmount(x) {
  return Number(
    x.trim_usd ??
    x.quote_size_usd ??
    x.sell_usd ??
    x.trade_value_usd ??
    x.amount_usd ??
    0
  );
}

function harvestAmount(x) {
  return Number(
    x.harvest_usd ??
    x.quote_size_usd ??
    x.sell_usd ??
    x.trade_value_usd ??
    x.amount_usd ??
    0
  );
}

function renderRebalance(data) {
  const el = document.getElementById("rebalanceSummary");
  const meta = document.getElementById("rebalanceMeta");
  if (!el) return;

  const summary = data?.summary || {};
  const plan = data?.plan || {};
  const harvest = data?.harvest || {};

  const buys = Array.isArray(plan.buys) ? plan.buys : [];
  const trims = Array.isArray(plan.trims) ? plan.trims : [];
  const harvests = Array.isArray(harvest.harvests) ? harvest.harvests : [];

  if (meta) {
    meta.innerHTML = `${cacheBadge(data?._cache)} <span class="tiny">regime: ${escapeHtml(summary.market_regime || "unknown")}</span>`;
  }

  const topBuys = buys.slice(0, 3).map((x) => `
    <div class="signal-row">
      <span>${escapeHtml(x.product_id || "—")}</span>
      <strong>${fmtUsd(buyAmount(x))}</strong>
    </div>
  `).join("") || `<div class="signal-empty">No buy candidates</div>`;

  const topTrims = trims.slice(0, 3).map((x) => `
    <div class="signal-row">
      <span>${escapeHtml(x.product_id || "—")}</span>
      <strong>${fmtUsd(trimAmount(x))}</strong>
    </div>
  `).join("") || `<div class="signal-empty">No trim candidates</div>`;

  const topHarvests = harvests.slice(0, 3).map((x) => `
    <div class="signal-row">
      <span>${escapeHtml(x.product_id || "—")}</span>
      <strong>${fmtUsd(harvestAmount(x))}</strong>
    </div>
  `).join("") || `<div class="signal-empty">No harvest candidates</div>`;

  el.innerHTML = `
    <div class="signal-chip-row">
      <span class="badge good">buys ${buys.length}</span>
      <span class="badge warn">trims ${trims.length}</span>
      <span class="badge accent">harvests ${harvests.length}</span>
      <span class="badge">${escapeHtml(summary.market_regime || "unknown")}</span>
    </div>

    <div class="rebalance-grid">
      <div class="signal-card">
        <div class="signal-title">Next Buys</div>
        ${topBuys}
      </div>
      <div class="signal-card">
        <div class="signal-title">Next Trims</div>
        ${topTrims}
      </div>
    </div>

    <div class="signal-card harvest-card">
      <div class="signal-title">Harvest Routes</div>
      ${topHarvests}
    </div>
  `;
}

function renderTradePreview(snapshotData) {
  const body = document.getElementById("recentTradesPreview");
  if (!body) return;

  const rows = Array.isArray(snapshotData?.recent_trades) ? snapshotData.recent_trades : [];

  body.innerHTML = rows.length
    ? rows.slice(0, 8).map((row) => `
      <tr>
        <td>${formatUnixTime(row.created_at)}</td>
        <td>${escapeHtml(row.product_id || "—")}</td>
        <td>${escapeHtml(row.side || "—")}</td>
        <td class="right">${fmtUsd(row.notional_usd || 0)}</td>
        <td>${escapeHtml(row.status || "—")}</td>
      </tr>
    `).join("")
    : `<tr><td colspan="5" class="muted">No recent trades found.</td></tr>`;
}

function renderDashboardStatus(systemData, portfolioData, rebalanceData, failures = []) {
  const badges = [];

  if (portfolioData && portfolioData._cache) {
    badges.push(cacheBadge(portfolioData._cache));
  }

  const regime =
    systemData?.portfolio_summary?.market_regime ||
    rebalanceData?.summary?.market_regime ||
    "unknown";

  badges.push(`<span class="badge">${escapeHtml(regime)}</span>`);

  if (systemData?.trade_stats?.trade_count != null) {
    badges.push(`<span class="badge accent">trades ${Number(systemData.trade_stats.trade_count || 0)}</span>`);
  }

  if (failures.length) {
    badges.push(`<span class="badge warn">partial data mode</span>`);
  }

  setStatusBadges(badges);
}

function renderDashboardErrorState(failures) {
  const failureText = failures.map((f) => `${f.path}: ${f.error}`).join(" | ");
  setStatusBadges([
    `<span class="badge bad">dashboard load failed</span>`,
    `<span class="badge warn">${escapeHtml(failureText)}</span>`
  ]);
}

async function refreshAll(showBadgeMessage = false) {
  const [systemRes, portfolioRes, rebalanceRes, historyRes] = await Promise.all([
    fetchJsonSafe("/api/system_snapshot?recent_count=8"),
    fetchJsonSafe("/api/portfolio"),
    fetchJsonSafe("/api/rebalance/preview"),
    fetchJsonSafe(`/api/portfolio/history?range=${encodeURIComponent(selectedHistoryRange)}`)
  ]);

  const failures = [systemRes, portfolioRes, rebalanceRes, historyRes].filter((x) => !x.ok);

  const systemData = systemRes.data || {};
  const portfolioData = portfolioRes.data || {};
  const rebalanceData = rebalanceRes.data || {};
  const historyData = historyRes.data || {};

  if (portfolioRes.ok) {
    renderPortfolio(portfolioData);
  }

  if (systemRes.ok || portfolioRes.ok) {
    renderActiveBuyUniverse(systemData?.portfolio || portfolioData?.snapshot || {});
  }

  if (rebalanceRes.ok) {
    renderRebalance(rebalanceData);
  }

  if (historyRes.ok) {
    renderAccountValueHistory(historyData);
  } else {
    renderAccountValueHistory({ points: [], series_type: "empty" });
  }

  if (systemRes.ok) {
    renderTradePreview(systemData);
  }

  renderDashboardStatus(systemData, portfolioData, rebalanceData, failures);
  setUiRefreshNow();

  if (!systemRes.ok && !portfolioRes.ok && !rebalanceRes.ok && !historyRes.ok) {
    renderDashboardErrorState(failures);
    return;
  }

  if (showBadgeMessage) {
    const items = [
      '<span class="badge good">dashboard refreshed</span>'
    ];

    if (portfolioData && portfolioData._cache) {
      items.push(cacheBadge(portfolioData._cache));
    }

    if (failures.length) {
      items.push(`<span class="badge warn">partial data mode</span>`);
    }

    setStatusBadges(items);
  }
}

function startAutoRefresh() {
  stopAutoRefresh();
  refreshTimer = window.setInterval(() => {
    refreshAll(false);
  }, AUTO_REFRESH_MS);
}

function stopAutoRefresh() {
  if (refreshTimer) {
    window.clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

window.refreshAll = refreshAll;

document.querySelectorAll(".trend-range-btn").forEach((button) => {
  button.addEventListener("click", () => {
    const nextRange = String(button.dataset.range || "").trim();
    if (!nextRange || nextRange === selectedHistoryRange) return;
    selectedHistoryRange = nextRange;
    setTrendRangeButtons(selectedHistoryRange);
    refreshAll(false);
  });
});

document.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    stopAutoRefresh();
  } else {
    refreshAll(false);
    startAutoRefresh();
  }
});

setSecretMode();
setTrendRangeButtons(selectedHistoryRange);
refreshAll(false);
startAutoRefresh();
