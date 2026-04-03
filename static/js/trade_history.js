const API_SECRET = (window.TRADE_HISTORY_CONFIG && window.TRADE_HISTORY_CONFIG.apiSecret) || "";
let tradePage = 1;

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

function sideBadge(side) {
  const s = String(side || "").toUpperCase();
  if (s === "BUY") return '<span class="badge good">BUY</span>';
  if (s === "SELL") return '<span class="badge bad">SELL</span>';
  return `<span class="badge">${s || "—"}</span>`;
}

function buildTradeQuery() {
  const productId = document.getElementById("tradeProductId")?.value?.trim()?.toUpperCase() || "";
  const side = document.getElementById("tradeSide")?.value || "";
  const status = document.getElementById("tradeStatus")?.value || "";
  const pageSize = document.getElementById("tradePageSize")?.value || "50";

  const params = new URLSearchParams();
  params.set("page", String(tradePage));
  params.set("page_size", String(pageSize));

  if (productId) params.set("product_id", productId);
  if (side) params.set("side", side);
  if (status) params.set("status", status);

  return params.toString();
}

function renderTradeStats(data) {
  const s = data.stats || {};

  document.getElementById("thTradeCount").textContent = String(s.trade_count || 0);
  document.getElementById("thBuyNotional").textContent = fmtUsd(s.buy_notional_usd);
  document.getElementById("thSellNotional").textContent = fmtUsd(s.sell_notional_usd);
  document.getElementById("thGrossNotional").textContent = fmtUsd(s.gross_notional_usd);
  document.getElementById("tradeLastTs").textContent = formatUnixTime(s.last_trade_ts);
}

function renderTrades(data) {
  const rows = data.trades || [];
  const tbody = document.getElementById("tradeHistoryTable");
  const meta = document.getElementById("tradeHistoryMeta");
  const tableMeta = document.getElementById("tradeTableMeta");

  document.getElementById("tradePageCurrent").textContent = String(data.page || 1);
  document.getElementById("tradeTotalRows").textContent = String(data.total || 0);

  if (meta) {
    meta.textContent = rows.length
      ? `Showing ${rows.length} row(s)`
      : "No orders found for current filter";
  }

  if (tableMeta) {
    const totalPages = Math.max(1, Math.ceil(Number(data.total || 0) / Number(data.page_size || 50)));
    tableMeta.textContent = `Showing page ${data.page || 1} of ${totalPages}`;
  }

  if (!tbody) return;

  tbody.innerHTML = rows.length
    ? rows.map(r => `
      <tr>
        <td>${formatUnixTime(r.created_at)}</td>
        <td>${r.product_id || "—"}</td>
        <td>${sideBadge(r.side)}</td>
        <td class="right">${fmtNum(r.base_size)}</td>
        <td class="right">${fmtUsd(r.price)}</td>
        <td class="right">${fmtUsd(r.notional_usd)}</td>
        <td>${r.status || "—"}</td>
        <td class="mono">${r.order_id || "—"}</td>
      </tr>
    `).join("")
    : `<tr><td colspan="8" class="muted">No trade history rows found.</td></tr>`;
}

async function refreshTrades() {
  try {
    const qs = buildTradeQuery();

    const [stats, trades] = await Promise.all([
      fetchJson(`/api/trades/stats?${qs}`),
      fetchJson(`/api/trades?${qs}`)
    ]);

    renderTradeStats(stats);
    renderTrades(trades);
  } catch (err) {
    console.error(err);
    const tbody = document.getElementById("tradeHistoryTable");
    if (tbody) {
      tbody.innerHTML = `<tr><td colspan="8" class="bad">Trade history load failed: ${err.message}</td></tr>`;
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
