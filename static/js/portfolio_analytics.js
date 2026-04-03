const API_SECRET = (window.PORTFOLIO_ANALYTICS_CONFIG && window.PORTFOLIO_ANALYTICS_CONFIG.apiSecret) || "";

function authUrl(path) {
  if (!API_SECRET) return path;
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}secret=${encodeURIComponent(API_SECRET)}`;
}

async function fetchJson(path) {
  const res = await fetch(authUrl(path));
  const data = await res.json();

  if (!res.ok || data.ok === false) {
    throw new Error(data.error || `HTTP ${res.status}`);
  }

  return data;
}

function fmtUsd(v) {
  return Number(v || 0).toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2
  });
}

function fmtPct(v) {
  return `${(Number(v || 0) * 100).toFixed(2)}%`;
}

function fmtQty(v) {
  return Number(v || 0).toLocaleString(undefined, {
    maximumFractionDigits: 8
  });
}

function formatUnixTime(ts) {
  const n = Number(ts || 0);
  if (!n) return "—";
  return new Date(n * 1000).toLocaleString();
}

function cacheBadge(cache) {
  if (!cache) return '<span class="badge">no-cache-meta</span>';
  const src = cache.source || "unknown";
  if (src === "live") return '<span class="badge good">live</span>';
  if (src === "fresh-cache") return '<span class="badge accent">fresh-cache</span>';
  if (src === "stale-cache") return '<span class="badge warn">stale-cache</span>';
  return `<span class="badge">${src}</span>`;
}

function setConsole(message, isError = false) {
  const el = document.getElementById("analyticsConsole");
  if (!el) return;
  el.innerHTML = isError ? `<span class="bad">${message}</span>` : message.replace(/\n/g, "<br>");
}

function renderSummary(data) {
  const s = data.summary || {};

  document.getElementById("paTotal").textContent = fmtUsd(s.total_value_usd);
  document.getElementById("paCash").textContent = fmtUsd(s.usd_cash);
  document.getElementById("paInvested").textContent = fmtUsd(s.invested_usd);
  document.getElementById("paRealized").textContent = fmtUsd(s.realized_pnl_total);

  document.getElementById("paAssetCount").textContent = String(s.asset_count || 0);
  document.getElementById("paTradeCount").textContent = String(s.trade_count || 0);
  document.getElementById("paPnlPoints").textContent = String(s.realized_pnl_points || 0);
  document.getElementById("paRegime").textContent = s.market_regime || "—";
  document.getElementById("paTopAsset").textContent = s.top_asset || "—";

  const cashW = Number(s.cash_weight || 0);
  const coreW = Number(s.core_weight || 0);
  const satW = Number(s.satellite_weight || 0);

  document.getElementById("paAllocationText").textContent =
    `Cash ${fmtPct(cashW)} | Core ${fmtPct(coreW)} | Satellite ${fmtPct(satW)}`;

  const segCash = document.getElementById("paSegCash");
  const segCore = document.getElementById("paSegCore");
  const segSat = document.getElementById("paSegSat");

  segCash.style.width = `${cashW * 100}%`;
  segCore.style.width = `${coreW * 100}%`;
  segSat.style.width = `${satW * 100}%`;

  segCash.textContent = cashW > 0.08 ? "Cash" : "";
  segCore.textContent = coreW > 0.08 ? "Core" : "";
  segSat.textContent = satW > 0.08 ? "Satellite" : "";

  const meta = document.getElementById("analyticsMeta");
  if (meta) {
    meta.innerHTML = `${cacheBadge(data._cache)} <span class="tiny">Updated: ${formatUnixTime(s.timestamp)}</span>`;
  }

  const mode = s.data_mode || "live";
  const warning = s.warning || data.warning || "";

  const consoleLines = [
    `Portfolio total: ${fmtUsd(s.total_value_usd)}`,
    `Cash: ${fmtUsd(s.usd_cash)}`,
    `Invested: ${fmtUsd(s.invested_usd)}`,
    `Realized PnL: ${fmtUsd(s.realized_pnl_total)}`,
    `Asset count: ${s.asset_count || 0}`,
    `Trades logged: ${s.trade_count || 0}`,
    `PnL points: ${s.realized_pnl_points || 0}`,
    `Top asset: ${s.top_asset || "—"}`,
    `Mode: ${mode}`
  ];

  if (warning) {
    consoleLines.push(`Warning: ${warning}`);
  }

  setConsole(consoleLines.join("\n"), mode === "degraded");
}

function renderAllocations(data) {
  const rows = data.allocations || [];
  const tbody = document.getElementById("allocationsTable");
  const meta = document.getElementById("allocationsMeta");

  if (meta) {
    meta.textContent = rows.length
      ? `${rows.length} live holdings`
      : "No live holdings available";
  }

  if (!tbody) return;

  function displayClass(cls) {
    if (cls === "core") return "core";
    if (cls === "satellite_active") return "satellite";
    if (cls === "dust") return "remainder";
    return cls || "—";
  }

  tbody.innerHTML = rows.length
    ? rows.map(r => `
      <tr>
        <td>${r.product_id}</td>
        <td>${displayClass(r.class)}</td>
        <td class="right">${fmtQty(r.base_qty_total)}</td>
        <td class="right">${fmtUsd(r.price_usd)}</td>
        <td class="right">${fmtUsd(r.value_total_usd)}</td>
        <td class="right">${fmtPct(r.weight_total)}</td>
      </tr>
    `).join("")
    : `<tr><td colspan="6" class="muted">No holdings found.</td></tr>`;
}

function buildSvgLine(points, key, labelFormatter = fmtUsd) {
  const width = 900;
  const height = 280;
  const pad = 24;

  const values = points.map(p => Number(p[key] || 0));
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
    <svg viewBox="0 0 ${width} ${height}" style="width:100%; height:280px; display:block;">
      <rect x="0" y="0" width="${width}" height="${height}" rx="18" ry="18" fill="rgba(255,255,255,0.02)"></rect>
      <line x1="${pad}" y1="${height-pad}" x2="${width-pad}" y2="${height-pad}" stroke="rgba(147,160,184,0.25)" />
      <line x1="${pad}" y1="${pad}" x2="${pad}" y2="${height-pad}" stroke="rgba(147,160,184,0.25)" />
      <path d="${d}" fill="none" stroke="rgba(79,140,255,1)" stroke-width="3" stroke-linecap="round" />
      <text x="${pad}" y="${pad-6}" fill="rgba(147,160,184,0.95)" font-size="12">Min: ${labelFormatter(min)}</text>
      <text x="${width-180}" y="${pad-6}" fill="rgba(147,160,184,0.95)" font-size="12">Max: ${labelFormatter(max)}</text>
    </svg>
  `;
}

function renderHistory(data) {
  const host = document.getElementById("historyChartHost");
  const meta = document.getElementById("historyChartMeta");
  const title = document.getElementById("historyChartTitle");

  if (!host) return;

  const points = data.points || [];
  const type = data.series_type || "realized_pnl";

  if (type === "equity_fallback") {
    title.textContent = "Equity Snapshot";
    if (meta) meta.textContent = "No pnl_history rows yet. Showing a current-equity fallback point.";
    host.innerHTML = `
      <div class="kpi-card">
        <div class="kpi-label">Current Portfolio Value</div>
        <div class="kpi-value">${fmtUsd((points[0] || {}).equity_usd || 0)}</div>
      </div>
    `;
    return;
  }

  if (type === "empty") {
    title.textContent = "History Unavailable";
    if (meta) meta.textContent = "No portfolio history series is available yet.";
    host.innerHTML = `<div class="muted">No history data found for the selected range.</div>`;
    return;
  }

  title.textContent = "Realized PnL History";

  if (!points.length) {
    if (meta) meta.textContent = "No realized PnL history found.";
    host.innerHTML = `<div class="muted">No pnl_history rows are available yet.</div>`;
    return;
  }

  if (meta) {
    meta.textContent = `${points.length} realized PnL point(s) in selected range`;
  }

  host.innerHTML = buildSvgLine(points, "realized_pnl", fmtUsd);
}

async function refreshAnalytics() {
  try {
    const range = document.getElementById("historyRange")?.value || "30d";

    const [summary, allocations, history] = await Promise.all([
      fetchJson("/api/portfolio/summary"),
      fetchJson("/api/portfolio/allocations"),
      fetchJson(`/api/portfolio/history?range=${encodeURIComponent(range)}`)
    ]);

    renderSummary(summary);
    renderAllocations(allocations);
    renderHistory(history);
  } catch (err) {
    console.error(err);
    setConsole(`Portfolio analytics load failed: ${err.message}`, true);
  }
}

window.refreshAnalytics = refreshAnalytics;
refreshAnalytics();
