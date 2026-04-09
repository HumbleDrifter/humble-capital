const API_SECRET = (window.PORTFOLIO_ANALYTICS_CONFIG && window.PORTFOLIO_ANALYTICS_CONFIG.apiSecret) || "";
let currentRange = "30d";
let lastAnalyticsRefreshAt = 0;

function authUrl(path) {
  if (!API_SECRET) return path;
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}secret=${encodeURIComponent(API_SECRET)}`;
}

async function fetchJson(path) {
  const res = await fetch(authUrl(path), { credentials: "same-origin" });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.ok === false) {
    throw new Error(data.error || `HTTP ${res.status}`);
  }
  return data;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function fmtUsd(value) {
  return Number(value || 0).toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2
  });
}

function fmtPct(value, alreadyPercent = false) {
  const numeric = Number(value || 0);
  const pct = alreadyPercent ? numeric : numeric * 100;
  return `${pct.toFixed(2)}%`;
}

function fmtSignedPct(value, alreadyPercent = false) {
  const numeric = Number(value || 0);
  const pct = alreadyPercent ? numeric : numeric * 100;
  return `${pct >= 0 ? "+" : "-"}${Math.abs(pct).toFixed(2)}%`;
}

function fmtQty(value) {
  return Number(value || 0).toLocaleString(undefined, {
    maximumFractionDigits: 6
  });
}

function formatUnixTime(ts) {
  const n = Number(ts || 0);
  if (!n) return "—";
  return new Date(n * 1000).toLocaleString();
}

function normalizeRegimeLabel(value) {
  const normalized = String(value || "")
    .replaceAll("_", " ")
    .trim()
    .toLowerCase();

  if (!normalized) return "Unknown";
  if (normalized === "bull") return "Risk On";
  if (normalized === "neutral") return "Neutral";
  if (normalized === "caution") return "Caution";
  if (normalized === "risk off") return "Risk Off";

  return normalized
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function updateRegimeBadge(regime) {
  const badge = document.getElementById("paRegimeBadge");
  const label = document.getElementById("paRegimeLabel");
  if (!badge || !label) return;

  const normalized = String(regime || "neutral").toLowerCase();
  badge.classList.remove("is-bull", "is-neutral", "is-caution", "is-risk_off");
  badge.classList.add(`is-${normalized}`);
  label.textContent = normalizeRegimeLabel(normalized);
}

function setRangeButtons(activeRange) {
  document.querySelectorAll(".hc-range-btn").forEach((button) => {
    const isActive = button.dataset.range === activeRange;
    button.classList.toggle("active", isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
}

function seededNoise(seed) {
  let x = 0;
  for (let i = 0; i < seed.length; i += 1) {
    x = (x * 31 + seed.charCodeAt(i)) % 2147483647;
  }
  return function next() {
    x = (x * 48271) % 2147483647;
    return x / 2147483647;
  };
}

function buildSparkline(seed, positive) {
  const rand = seededNoise(seed);
  const width = 64;
  const height = 28;
  const points = [];
  let value = positive ? 16 : 12;

  for (let i = 0; i < 20; i += 1) {
    const drift = positive ? 0.45 : -0.45;
    value += drift + ((rand() - 0.5) * 3.2);
    value = Math.max(3, Math.min(25, value));
    const x = (i / 19) * width;
    const y = height - value;
    points.push(`${x.toFixed(2)},${y.toFixed(2)}`);
  }

  const stroke = positive ? "#2dd4bf" : "#fb7185";
  return `
    <svg viewBox="0 0 64 28" class="hc-pos-spark" aria-hidden="true">
      <polyline points="${points.join(" ")}" fill="none" stroke="${stroke}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"></polyline>
    </svg>
  `;
}

function rangeLabel(range) {
  if (range === "7d") return "7 days ago";
  if (range === "30d") return "30 days ago";
  if (range === "90d") return "90 days ago";
  if (range === "1y") return "1 year ago";
  return "Start";
}

function resolveHistorySeries(history) {
  const points = Array.isArray(history?.points) ? history.points.filter((row) => row && typeof row === "object") : [];
  const seriesType = String(history?.series_type || "portfolio_value");
  const valueKey = seriesType === "realized_pnl" ? "realized_pnl" : "equity_usd";
  const filteredPoints = points.filter((row) => Number.isFinite(Number(row?.[valueKey])));
  return { points: filteredPoints, seriesType, valueKey };
}

function buildHistoryChart(container, points, valueKey, range) {
  if (!container) return;
  if (!points.length) {
    container.innerHTML = `<div class="hc-chart-empty">No history data available for this range.</div>`;
    return;
  }

  const width = 800;
  const height = 280;
  const leftPad = 78;
  const rightPad = 22;
  const topPad = 18;
  const bottomPad = 36;
  const values = points.map((row) => Number(row[valueKey] || 0));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = Math.max(1, max - min);
  const chartHeight = height - topPad - bottomPad;
  const chartWidth = width - leftPad - rightPad;

  const coords = points.map((row, index) => {
    const x = leftPad + ((chartWidth * index) / Math.max(points.length - 1, 1));
    const y = topPad + ((max - Number(row[valueKey] || 0)) / spread) * chartHeight;
    return { x, y };
  });

  const linePath = coords.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(" ");
  const areaPath = `${linePath} L ${coords[coords.length - 1].x.toFixed(2)} ${height - bottomPad} L ${coords[0].x.toFixed(2)} ${height - bottomPad} Z`;
  const last = coords[coords.length - 1];

  const gridLines = [];
  for (let i = 0; i < 5; i += 1) {
    const ratio = i / 4;
    const value = max - (spread * ratio);
    const y = topPad + (chartHeight * ratio);
    gridLines.push(`
      <line x1="${leftPad}" y1="${y.toFixed(2)}" x2="${width - rightPad}" y2="${y.toFixed(2)}" stroke="rgba(255,255,255,0.06)" stroke-width="1"></line>
      <text x="10" y="${(y + 4).toFixed(2)}" fill="rgba(255,255,255,0.4)" font-size="12" font-family="DM Sans, system-ui, sans-serif">${escapeHtml(fmtUsd(value))}</text>
    `);
  }

  container.innerHTML = `
    <svg viewBox="0 0 800 280" width="100%" role="img" aria-label="Portfolio analytics history chart">
      <defs>
        <linearGradient id="paChartStroke" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stop-color="#2dd4bf"></stop>
          <stop offset="100%" stop-color="#60a5fa"></stop>
        </linearGradient>
        <linearGradient id="paChartArea" x1="0%" y1="0%" x2="0%" y2="100%">
          <stop offset="0%" stop-color="rgba(45,212,191,0.25)"></stop>
          <stop offset="100%" stop-color="rgba(45,212,191,0)"></stop>
        </linearGradient>
        <filter id="paGlow" x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur stdDeviation="4" result="blur"></feGaussianBlur>
          <feMerge>
            <feMergeNode in="blur"></feMergeNode>
            <feMergeNode in="SourceGraphic"></feMergeNode>
          </feMerge>
        </filter>
      </defs>
      ${gridLines.join("")}
      <path d="${areaPath}" fill="url(#paChartArea)"></path>
      <path d="${linePath}" fill="none" stroke="url(#paChartStroke)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"></path>
      <circle cx="${last.x.toFixed(2)}" cy="${last.y.toFixed(2)}" r="8" fill="rgba(45,212,191,0.25)" filter="url(#paGlow)"></circle>
      <circle cx="${last.x.toFixed(2)}" cy="${last.y.toFixed(2)}" r="4" fill="#2dd4bf"></circle>
      <text x="${leftPad}" y="${height - 12}" fill="rgba(255,255,255,0.4)" font-size="12" font-family="DM Sans, system-ui, sans-serif">${escapeHtml(rangeLabel(range))}</text>
      <text x="${width - 54}" y="${height - 12}" fill="rgba(255,255,255,0.4)" font-size="12" font-family="DM Sans, system-ui, sans-serif">Now</text>
    </svg>
  `;
}

function buildChartStat(label, value, tone = "") {
  return `
    <div class="pa-chart-stat ${tone}">
      <div class="pa-chart-stat-label">${escapeHtml(label)}</div>
      <div class="pa-chart-stat-value">${escapeHtml(value)}</div>
    </div>
  `;
}

function renderOverview(snapshot, summary) {
  const totalValue = Number(snapshot?.total_value_usd || summary?.total_value_usd || 0);
  const cash = Number(snapshot?.usd_cash || summary?.usd_cash || 0);
  const invested = Number(summary?.invested_usd || Math.max(0, totalValue - cash));
  const realized = Number(summary?.realized_pnl_total || 0);
  const cashWeight = Number(summary?.cash_weight || (totalValue > 0 ? cash / totalValue : 0));
  const coreWeight = Number(summary?.core_weight || 0);
  const satelliteWeight = Number(summary?.satellite_weight || 0);
  const regime = String(summary?.market_regime || snapshot?.market_regime || "neutral");
  const timestamp = summary?.timestamp || snapshot?.timestamp;

  document.getElementById("paTotal").textContent = fmtUsd(totalValue);
  document.getElementById("paCash").textContent = fmtUsd(cash);
  document.getElementById("paInvested").textContent = fmtUsd(invested);

  const realizedEl = document.getElementById("paRealized");
  realizedEl.textContent = fmtUsd(realized);
  realizedEl.classList.remove("pa-positive", "pa-negative");
  if (realized > 0) realizedEl.classList.add("pa-positive");
  if (realized < 0) realizedEl.classList.add("pa-negative");

  document.getElementById("paTotalDetail").textContent = timestamp ? `Updated ${formatUnixTime(timestamp)}` : "Live portfolio value";
  document.getElementById("paCashDetail").textContent = `${fmtPct(cashWeight)} of portfolio`;
  document.getElementById("paInvestedDetail").textContent = `${fmtPct(coreWeight + satelliteWeight)} deployed`;
  document.getElementById("paRealizedDetail").textContent = `${Number(summary?.trade_count || 0)} logged trade(s)`;

  document.getElementById("paAllocCore").style.width = `${Math.max(0, coreWeight * 100)}%`;
  document.getElementById("paAllocSat").style.width = `${Math.max(0, satelliteWeight * 100)}%`;
  document.getElementById("paAllocCash").style.width = `${Math.max(0, cashWeight * 100)}%`;
  document.getElementById("paAllocCorePct").textContent = Math.round(coreWeight * 100);
  document.getElementById("paAllocSatPct").textContent = Math.round(satelliteWeight * 100);
  document.getElementById("paAllocCashPct").textContent = Math.round(cashWeight * 100);
  document.getElementById("paAllocationText").textContent = `Core ${fmtPct(coreWeight)} · Satellite ${fmtPct(satelliteWeight)} · Cash ${fmtPct(cashWeight)}`;

  document.getElementById("paAssetCount").textContent = String(summary?.asset_count || 0);
  document.getElementById("paTradeCount").textContent = String(summary?.trade_count || 0);
  document.getElementById("paPnlPoints").textContent = String(summary?.realized_pnl_points || 0);
  document.getElementById("paRegime").textContent = normalizeRegimeLabel(regime);
  document.getElementById("paTopAsset").textContent = summary?.top_asset || "—";

  const meta = document.getElementById("analyticsMeta");
  if (meta) {
    meta.textContent = `${summary?.asset_count || 0} assets tracked • ${summary?.trade_count || 0} trades logged • ${timestamp ? `updated ${formatUnixTime(timestamp)}` : "live snapshot"}`;
  }

  updateRegimeBadge(regime);
}

function renderHistory(history) {
  const { points, seriesType, valueKey } = resolveHistorySeries(history);
  const host = document.getElementById("historyChartHost");
  const title = document.getElementById("historyChartTitle");
  const meta = document.getElementById("historyChartMeta");
  const stats = document.getElementById("paChartStats");

  if (!host || !title || !meta || !stats) return;

  if (!points.length) {
    title.textContent = "Portfolio History";
    meta.textContent = "No history data available for the selected range.";
    host.innerHTML = `<div class="hc-chart-empty">No history data available for this range.</div>`;
    stats.innerHTML = "";
    return;
  }

  const values = points.map((row) => Number(row[valueKey] || 0));
  const first = values[0] || 0;
  const last = values[values.length - 1] || 0;
  const low = Math.min(...values);
  const delta = last - first;
  const pct = first !== 0 ? delta / Math.abs(first) : 0;
  const tone = delta >= 0 ? "positive" : "negative";

  title.textContent = seriesType === "realized_pnl" ? "Realized PnL History" : "Equity Curve";
  meta.textContent = `${points.length} point(s) • ${seriesType === "realized_pnl" ? "closed-trade series" : "portfolio value series"}`;
  buildHistoryChart(host, points, valueKey, currentRange);
  stats.innerHTML = [
    buildChartStat("Current", fmtUsd(last), tone),
    buildChartStat("Range Change", delta >= 0 ? `+${fmtUsd(delta)}` : `-${fmtUsd(Math.abs(delta))}`, tone),
    buildChartStat("Range %", fmtSignedPct(pct), tone),
    buildChartStat("Low", fmtUsd(low))
  ].join("");
}

function symbolName(productId) {
  const symbol = String(productId || "").split("-")[0];
  const known = {
    BTC: "Bitcoin",
    ETH: "Ethereum",
    SOL: "Solana",
    XRP: "XRP",
    DOGE: "Dogecoin",
    SHIB: "Shiba Inu",
    BONK: "Bonk",
    PEPE: "Pepe"
  };
  return known[symbol] || symbol;
}

function renderHoldings(snapshot, summary, allocationsPayload) {
  const host = document.getElementById("paHoldingsGrid");
  const meta = document.getElementById("allocationsMeta");
  if (!host) return;

  const positions = snapshot?.positions && typeof snapshot.positions === "object" ? snapshot.positions : {};
  const summaryAssets = summary?.assets && typeof summary.assets === "object" ? summary.assets : {};
  const allocations = Array.isArray(allocationsPayload?.allocations) ? allocationsPayload.allocations : [];
  const allocationMap = Object.fromEntries(
    allocations.map((row) => [String(row.product_id || "").toUpperCase(), row || {}])
  );
  const coreAssets = new Set(Object.keys(snapshot?.config?.core_assets || {}).map((key) => String(key || "").toUpperCase()));

  const productIds = Array.from(
    new Set([
      ...Object.keys(positions || {}),
      ...Object.keys(summaryAssets || {}),
      ...Object.keys(allocationMap || {})
    ])
  );

  const rows = productIds
    .map((productId) => {
      const pos = positions[productId] || {};
      const asset = summaryAssets[productId] || {};
      const allocation = allocationMap[productId] || {};
      const value = Number(
        pos.value_total_usd ??
        asset.value_total_usd ??
        allocation.value_total_usd ??
        0
      );
      if (value <= 0) return null;

      const symbol = String(productId || "").split("-")[0];
      const isCore = coreAssets.has(String(productId || "").toUpperCase()) || allocation.class === "core" || asset.class === "core";
      const changePct = Number(
        asset.unrealized_pnl_pct ??
        pos.unrealized_pnl_pct ??
        allocation.unrealized_pnl_pct ??
        asset.change_24h ??
        0
      );
      const qty = Number(
        pos.base_balance ??
        pos.base_qty_total ??
        asset.base_qty_total ??
        allocation.base_qty_total ??
        0
      );
      const price = Number(
        asset.price_usd ??
        allocation.price_usd ??
        pos.price_usd ??
        0
      );

      return {
        productId,
        symbol,
        isCore,
        value,
        changePct,
        qty,
        price
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.value - a.value);

  if (meta) {
    meta.textContent = rows.length ? `${rows.length} live holding(s)` : "No live holdings available";
  }

  if (!rows.length) {
    host.innerHTML = `<div class="hc-empty-card">No live holdings available.</div>`;
    return;
  }

  host.innerHTML = rows.map((row) => {
    const positive = row.changePct >= 0;
    return `
      <article class="hc-pos-card pa-pos-card pa-reveal">
        <div class="hc-pos-icon ${row.isCore ? "core" : "satellite"}">${escapeHtml(row.symbol.slice(0, 2))}</div>
        <div class="hc-pos-info">
          <div class="hc-pos-symbol">${escapeHtml(row.symbol)}</div>
          <div class="hc-pos-name">${escapeHtml(symbolName(row.productId))} · ${row.isCore ? "Core" : "Satellite"}</div>
          <div class="pa-pos-meta">${escapeHtml(fmtQty(row.qty))} units · ${escapeHtml(fmtUsd(row.price))}</div>
        </div>
        ${buildSparkline(row.productId, positive)}
        <div class="hc-pos-right">
          <div class="hc-pos-value">${escapeHtml(fmtUsd(row.value))}</div>
          <div class="hc-pos-change ${positive ? "positive" : "negative"}">${escapeHtml(fmtSignedPct(row.changePct, true))}</div>
        </div>
      </article>
    `;
  }).join("");

  observeRevealTargets();
}

let revealObserver = null;

function observeRevealTargets() {
  if (!revealObserver) {
    revealObserver = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        entry.target.classList.add("pa-visible");
      });
    }, {
      threshold: 0.12,
      rootMargin: "0px 0px -8% 0px"
    });
  }

  document.querySelectorAll(".pa-observe, .pa-pos-card").forEach((node) => {
    if (node.dataset.paObserved === "true") return;
    node.dataset.paObserved = "true";
    revealObserver.observe(node);
  });
}

async function refreshAnalytics() {
  try {
    lastAnalyticsRefreshAt = Date.now();

    const [portfolioResp, summaryResp, allocationsResp, historyResp] = await Promise.all([
      fetchJson("/api/portfolio"),
      fetchJson("/api/portfolio/summary"),
      fetchJson("/api/portfolio/allocations"),
      fetchJson(`/api/portfolio/history?range=${encodeURIComponent(currentRange)}`)
    ]);

    const snapshot = portfolioResp.snapshot || {};
    const portfolioSummary = portfolioResp.summary || {};
    const statsSummary = summaryResp.summary || {};
    const mergedSummary = {
      ...portfolioSummary,
      ...statsSummary,
      assets: portfolioSummary.assets || statsSummary.assets || {}
    };

    renderOverview(snapshot, mergedSummary);
    renderHistory(historyResp);
    renderHoldings(snapshot, mergedSummary, allocationsResp);
    observeRevealTargets();
  } catch (err) {
    console.error(err);
    const meta = document.getElementById("analyticsMeta");
    const chartMeta = document.getElementById("historyChartMeta");
    const holdings = document.getElementById("paHoldingsGrid");
    if (meta) meta.textContent = `Portfolio analytics load failed: ${err.message}`;
    if (chartMeta) chartMeta.textContent = "History unavailable.";
    if (holdings) holdings.innerHTML = `<div class="hc-empty-card">Portfolio analytics load failed.</div>`;
  }
}

function bindRangeButtons() {
  document.querySelectorAll(".hc-range-btn").forEach((button) => {
    button.addEventListener("click", () => {
      const nextRange = button.dataset.range || "30d";
      if (nextRange === currentRange) return;
      currentRange = nextRange;
      setRangeButtons(currentRange);
      refreshAnalytics();
    });
  });
}

window.refreshAnalytics = refreshAnalytics;
bindRangeButtons();
setRangeButtons(currentRange);
observeRevealTargets();
refreshAnalytics();

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState !== "visible") return;
  if (Date.now() - lastAnalyticsRefreshAt < 15000) return;
  refreshAnalytics();
});
