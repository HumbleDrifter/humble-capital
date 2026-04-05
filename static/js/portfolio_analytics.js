const API_SECRET = (window.PORTFOLIO_ANALYTICS_CONFIG && window.PORTFOLIO_ANALYTICS_CONFIG.apiSecret) || "";
let lastAnalyticsRefreshAt = 0;

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

function normalizeRegimeLabel(value) {
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
  const realizedPnl = Number(s.realized_pnl_total || 0);
  const realizedEl = document.getElementById("paRealized");

  document.getElementById("paTotal").textContent = fmtUsd(s.total_value_usd);
  document.getElementById("paCash").textContent = fmtUsd(s.usd_cash);
  document.getElementById("paInvested").textContent = fmtUsd(s.invested_usd);
  if (realizedEl) {
    realizedEl.textContent = fmtUsd(realizedPnl);
    realizedEl.className = `kpi-value${realizedPnl > 0 ? " positive" : realizedPnl < 0 ? " negative" : ""}`;
  }

  document.getElementById("paAssetCount").textContent = String(s.asset_count || 0);
  document.getElementById("paTradeCount").textContent = String(s.trade_count || 0);
  document.getElementById("paPnlPoints").textContent = String(s.realized_pnl_points || 0);
  document.getElementById("paRegime").textContent = normalizeRegimeLabel(s.market_regime || "unknown");
  document.getElementById("paTopAsset").textContent = s.top_asset || "—";

  const cashW = Number(s.cash_weight || 0);
  const coreW = Number(s.core_weight || 0);
  const satW = Number(s.satellite_weight || 0);

  document.getElementById("paAllocationText").textContent =
    `Cash ${fmtPct(cashW)} | Core ${fmtPct(coreW)} | Satellite Assets ${fmtPct(satW)}`;

  const segCash = document.getElementById("paSegCash");
  const segCore = document.getElementById("paSegCore");
  const segSat = document.getElementById("paSegSat");

  segCash.style.width = `${cashW * 100}%`;
  segCore.style.width = `${coreW * 100}%`;
  segSat.style.width = `${satW * 100}%`;

  segCash.textContent = cashW > 0.08 ? "Cash" : "";
  segCore.textContent = coreW > 0.08 ? "Core" : "";
  segSat.textContent = satW > 0.12 ? "Satellite Assets" : "";

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
    if (cls === "satellite_active") return "satellite assets";
    if (cls === "dust") return "remainder";
    return cls || "—";
  }

  tbody.innerHTML = rows.length
    ? rows.map((r) => `
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

function buildTrendStat(label, value, tone = "") {
  return `
    <div class="trend-stat">
      <div class="trend-stat-label">${label}</div>
      <div class="trend-stat-value${tone ? ` ${tone}` : ""}">${value}</div>
    </div>
  `;
}

function buildTrendSvg(points, key, labelFormatter = fmtUsd) {
  const width = 960;
  const height = 280;
  const padLeft = 18;
  const padRight = 18;
  const padTop = 26;
  const padBottom = 28;
  const innerWidth = width - padLeft - padRight;
  const innerHeight = height - padTop - padBottom;

  const values = points.map((p) => Number(p[key] || 0));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = Math.max(1e-9, max - min);
  const areaBaseY = padTop + innerHeight;
  const lastValue = values[values.length - 1] || 0;
  const startValue = values[0] || 0;
  const trendPositive = lastValue - startValue >= 0;
  const stroke = trendPositive ? "#34d399" : "#fb7185";
  const areaAccent = trendPositive ? "52, 211, 153" : "251, 113, 133";

  const coords = points.map((p, i) => {
    const x = padLeft + (i * innerWidth) / Math.max(1, points.length - 1);
    const y = areaBaseY - ((Number(p[key] || 0) - min) / spread) * innerHeight;
    return [x, y];
  });

  const linePath = coords.map((c, i) => `${i === 0 ? "M" : "L"} ${c[0]} ${c[1]}`).join(" ");
  const areaPath = `${linePath} L ${coords[coords.length - 1][0]} ${areaBaseY} L ${coords[0][0]} ${areaBaseY} Z`;
  const yMarkers = [
    { label: `Max ${labelFormatter(max)}`, y: padTop + 10 },
    { label: `Min ${labelFormatter(min)}`, y: areaBaseY - 6 }
  ];

  return `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Realized PnL history chart">
      <defs>
        <linearGradient id="analyticsTrendArea" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="rgba(${areaAccent}, 0.34)" />
          <stop offset="100%" stop-color="rgba(${areaAccent}, 0.02)" />
        </linearGradient>
      </defs>
      <rect x="0" y="0" width="${width}" height="${height}" rx="22" ry="22" fill="rgba(255,255,255,0.02)"></rect>
      ${yMarkers.map((marker) => `
        <line
          x1="${padLeft}"
          y1="${marker.y}"
          x2="${width - padRight}"
          y2="${marker.y}"
          stroke="rgba(154, 169, 192, 0.12)"
          stroke-dasharray="4 6"
        ></line>
      `).join("")}
      <path d="${areaPath}" fill="url(#analyticsTrendArea)"></path>
      <path
        d="${linePath}"
        fill="none"
        stroke="${stroke}"
        stroke-width="3.25"
        stroke-linecap="round"
        stroke-linejoin="round"
      ></path>
      ${coords.length ? `
        <circle
          cx="${coords[coords.length - 1][0]}"
          cy="${coords[coords.length - 1][1]}"
          r="4.5"
          fill="${stroke}"
          stroke="rgba(236, 243, 255, 0.92)"
          stroke-width="2"
        ></circle>
      ` : ""}
      <text x="${padLeft}" y="${padTop - 8}" fill="rgba(154, 169, 192, 0.92)" font-size="12">Max ${labelFormatter(max)}</text>
      <text x="${padLeft}" y="${areaBaseY + 18}" fill="rgba(154, 169, 192, 0.92)" font-size="12">Min ${labelFormatter(min)}</text>
    </svg>
  `;
}

function renderHistory(data) {
  const host = document.getElementById("historyChartHost");
  const meta = document.getElementById("historyChartMeta");
  const title = document.getElementById("historyChartTitle");

  if (!host) return;

  const points = Array.isArray(data.points) ? data.points : [];
  const realizedPoints = points.filter((point) => Number.isFinite(Number(point?.realized_pnl)));

  if ((data.series_type || "realized_pnl") === "empty") {
    title.textContent = "History Unavailable";
    if (meta) meta.textContent = "No portfolio history series is available yet.";
    host.innerHTML = `<div class="trend-chart-empty">No history data found for the selected range.</div>`;
    return;
  }

  if (!realizedPoints.length) {
    title.textContent = "Realized PnL History";
    if (meta) meta.textContent = "No realized PnL history found.";
    host.innerHTML = `<div class="trend-chart-empty">No history data found for the selected range.</div>`;
    return;
  }

  const values = realizedPoints.map((point) => Number(point.realized_pnl || 0));
  const startValue = values[0] || 0;
  const currentValue = values[values.length - 1] || 0;
  const rangeDelta = currentValue - startValue;
  const lowValue = Math.min(...values);
  const tone = rangeDelta >= 0 ? "positive" : "negative";

  title.textContent = "Realized PnL History";
  if (meta) meta.textContent = `${realizedPoints.length} realized PnL point(s) in selected range`;

  host.innerHTML = `
    <div class="trend-chart-shell">
      <div class="trend-chart-summary">
        ${buildTrendStat("Current PnL", fmtUsd(currentValue), tone)}
        ${buildTrendStat("Range Change", `${rangeDelta >= 0 ? "+" : "-"}${fmtUsd(Math.abs(rangeDelta))}`, tone)}
        ${buildTrendStat("Low Watermark", fmtUsd(lowValue))}
      </div>
      ${buildTrendSvg(realizedPoints, "realized_pnl", fmtUsd)}
      <div class="trend-chart-note">Persisted realized PnL entries are plotted across the selected range.</div>
    </div>
  `;
}

async function refreshAnalytics() {
  try {
    lastAnalyticsRefreshAt = Date.now();
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

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState !== "visible") return;
  if (Date.now() - lastAnalyticsRefreshAt < 15000) return;
  refreshAnalytics();
});
