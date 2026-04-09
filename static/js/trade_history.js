const API_SECRET = (window.TRADE_HISTORY_CONFIG && window.TRADE_HISTORY_CONFIG.apiSecret) || "";
let tradePage = 1;
let currentTradeRows = [];

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

function fmtUsd(v) {
  return Number(v || 0).toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2
  });
}

function fmtNum(v) {
  return Number(v || 0).toLocaleString(undefined, {
    maximumFractionDigits: 8
  });
}

function formatUnixTime(ts) {
  const n = Number(ts || 0);
  if (!n) return "—";
  return new Date(n * 1000).toLocaleString();
}

function relativeTime(ts) {
  const value = Number(ts || 0);
  if (!value) return "just now";
  const diffSec = Math.max(0, Math.floor(Date.now() / 1000) - value);
  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
  return `${Math.floor(diffSec / 86400)}d ago`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function tradeSideMeta(side) {
  const normalized = String(side || "").toUpperCase();
  if (normalized === "BUY") {
    return { label: "BUY", badge: "buy" };
  }
  if (normalized === "EXIT") {
    return { label: "EXIT", badge: "exit" };
  }
  if (normalized === "TRIM" || normalized === "SELL") {
    return { label: normalized || "SELL", badge: "sell" };
  }
  return { label: normalized || "—", badge: "neutral" };
}

function buildTradeQuery() {
  const productId = document.getElementById("tradeProductId")?.value || "";
  const side = document.getElementById("tradeSide")?.value || "";
  const pageSize = document.getElementById("tradePageSize")?.value || "50";

  const params = new URLSearchParams();
  params.set("page", String(tradePage));
  params.set("page_size", String(pageSize));

  if (productId) params.set("product_id", productId);
  if (side) params.set("side", side);

  return params.toString();
}

function renderProductOptions(rows) {
  const select = document.getElementById("tradeProductId");
  if (!select) return;

  const previousValue = select.value;
  const products = Array.from(
    new Set(
      (Array.isArray(rows) ? rows : [])
        .map((row) => String(row?.product_id || "").trim().toUpperCase())
        .filter(Boolean)
    )
  ).sort();

  select.innerHTML = [
    `<option value="">All Products</option>`,
    ...products.map((productId) => `<option value="${escapeHtml(productId)}">${escapeHtml(productId)}</option>`)
  ].join("");

  if (products.includes(previousValue)) {
    select.value = previousValue;
  }
}

function renderTradeSummary(data) {
  const rows = Array.isArray(data?.trades) ? data.trades : [];
  const total = Number(data?.total || rows.length || 0);
  const buyCount = rows.filter((row) => String(row?.side || "").toUpperCase() === "BUY").length;
  const sellCount = rows.filter((row) => ["SELL", "TRIM", "EXIT"].includes(String(row?.side || "").toUpperCase())).length;
  const timestamps = rows
    .map((row) => Number(row?.created_at || 0))
    .filter((value) => Number.isFinite(value) && value > 0)
    .sort((a, b) => a - b);

  const oldest = timestamps.length ? formatUnixTime(timestamps[0]) : "—";
  const newest = timestamps.length ? formatUnixTime(timestamps[timestamps.length - 1]) : "—";
  const rangeText = timestamps.length ? `${oldest} → ${newest}` : "No visible trades";

  document.getElementById("actTotalTrades").textContent = String(total);
  document.getElementById("actBuyCount").textContent = String(buyCount);
  document.getElementById("actSellCount").textContent = String(sellCount);
  document.getElementById("actDateRange").textContent = rangeText;
  document.getElementById("actTotalTradesDetail").textContent = `${rows.length} trade(s) on this page`;
  document.getElementById("actBuyDetail").textContent = `${buyCount} visible buy fill(s)`;
  document.getElementById("actSellDetail").textContent = `${sellCount} visible sell / trim / exit trade(s)`;
  document.getElementById("actDateRangeDetail").textContent = timestamps.length ? "Current page window" : "No timestamps available";
  document.getElementById("tradeLastTs").textContent = newest;
}

function renderTradeFeed(data) {
  const rows = Array.isArray(data?.trades) ? data.trades : [];
  const host = document.getElementById("tradeFeed");
  const meta = document.getElementById("tradeHistoryMeta");
  const tableMeta = document.getElementById("tradeTableMeta");
  const page = Number(data?.page || 1);
  const pageSize = Number(data?.page_size || 50);
  const total = Number(data?.total || 0);
  const totalPages = Math.max(1, Math.ceil(total / Math.max(1, pageSize)));
  const prevBtn = document.getElementById("tradePrevBtn");
  const nextBtn = document.getElementById("tradeNextBtn");

  document.getElementById("tradePageCurrent").textContent = String(page);
  document.getElementById("tradeTotalRows").textContent = String(total);
  document.getElementById("tradePageIndicator").textContent = String(page);

  if (prevBtn) prevBtn.disabled = page <= 1;
  if (nextBtn) nextBtn.disabled = page >= totalPages;

  if (meta) {
    meta.textContent = rows.length
      ? `Showing ${rows.length} execution(s) • page ${page} of ${totalPages}`
      : "No executions found for the current filter.";
  }

  if (tableMeta) {
    tableMeta.textContent = rows.length
      ? `Most recent ${rows.length} trade(s) in this filtered feed`
      : "No trade activity matches the selected filters.";
  }

  if (!host) return;

  if (!rows.length) {
    host.innerHTML = `<div class="hc-empty-card">No trade activity matches the selected filters.</div>`;
    return;
  }

  host.innerHTML = rows.map((trade, index) => {
    const side = tradeSideMeta(trade?.side);
    const productId = String(trade?.product_id || "—");
    const amount = Number(trade?.price || 0) * Number(trade?.base_size || 0);
    const status = String(trade?.status || "—").toUpperCase();

    return `
      <article class="act-trade-card hc-pos-card" style="animation-delay:${(index * 0.04).toFixed(2)}s">
        <div class="act-trade-left">
          <div class="hc-trade-badge ${side.badge}">${escapeHtml(side.label)}</div>
          <div class="act-trade-info">
            <div class="act-trade-symbol">${escapeHtml(productId)}</div>
            <div class="act-trade-meta">
              <span>${escapeHtml(status)}</span>
              <span>${escapeHtml(relativeTime(trade?.created_at))}</span>
            </div>
          </div>
        </div>
        <div class="act-trade-metrics">
          <div class="act-trade-metric">
            <span class="act-metric-label">Amount</span>
            <strong>${escapeHtml(fmtUsd(amount))}</strong>
          </div>
          <div class="act-trade-metric">
            <span class="act-metric-label">Price</span>
            <strong>${escapeHtml(fmtUsd(trade?.price || 0))}</strong>
          </div>
          <div class="act-trade-metric">
            <span class="act-metric-label">Base Size</span>
            <strong>${escapeHtml(fmtNum(trade?.base_size || 0))}</strong>
          </div>
          <div class="act-trade-metric">
            <span class="act-metric-label">Time</span>
            <strong>${escapeHtml(formatUnixTime(trade?.created_at))}</strong>
          </div>
        </div>
      </article>
    `;
  }).join("");

  observeTradeCards();
}

let cardObserver = null;

function observeTradeCards() {
  if (!cardObserver) {
    cardObserver = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        entry.target.classList.add("act-visible");
      });
    }, {
      threshold: 0.1,
      rootMargin: "0px 0px -8% 0px"
    });
  }

  document.querySelectorAll(".act-trade-card").forEach((card) => {
    if (card.dataset.observed === "true") return;
    card.dataset.observed = "true";
    cardObserver.observe(card);
  });
}

async function refreshTrades() {
  try {
    const qs = buildTradeQuery();
    const trades = await fetchJson(`/api/trades?${qs}`);
    currentTradeRows = Array.isArray(trades?.trades) ? trades.trades : [];
    renderProductOptions(currentTradeRows);
    renderTradeSummary(trades);
    renderTradeFeed(trades);
  } catch (err) {
    console.error(err);
    const host = document.getElementById("tradeFeed");
    const meta = document.getElementById("tradeHistoryMeta");
    if (meta) {
      meta.textContent = `Trade history load failed: ${err.message}`;
    }
    if (host) {
      host.innerHTML = `<div class="hc-empty-card">Trade history load failed: ${escapeHtml(err.message)}</div>`;
    }
  }
}

function applyTradeFilters() {
  tradePage = 1;
  refreshTrades();
}

function nextTradePage() {
  tradePage += 1;
  refreshTrades();
}

function prevTradePage() {
  tradePage = Math.max(1, tradePage - 1);
  refreshTrades();
}

window.refreshTrades = refreshTrades;
window.applyTradeFilters = applyTradeFilters;
window.nextTradePage = nextTradePage;
window.prevTradePage = prevTradePage;

refreshTrades();
