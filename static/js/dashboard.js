(function () {
  const SECRET = window.DASHBOARD_CONFIG?.apiSecret || "";
  let currentRange = "30d";
  let refreshTimer = null;
  let lastPortfolioValue = 0;

  function authUrl(path) {
    if (!SECRET) return path;
    const sep = path.includes("?") ? "&" : "?";
    return `${path}${sep}secret=${encodeURIComponent(SECRET)}`;
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

  function fmtSignedUsd(value) {
    const numeric = Number(value || 0);
    return `${numeric >= 0 ? "+" : "-"}${fmtUsd(Math.abs(numeric))}`;
  }

  function fmtSignedPct(value, alreadyPercent = false) {
    const numeric = Number(value || 0);
    const pct = alreadyPercent ? numeric : numeric * 100;
    return `${pct >= 0 ? "+" : "-"}${Math.abs(pct).toFixed(2)}%`;
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

  async function fetchJson(path) {
    const response = await fetch(authUrl(path), { credentials: "same-origin" });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) {
      throw new Error(data.error || `HTTP ${response.status}`);
    }
    return data;
  }

  function animateValue(element, target, prefix = "", suffix = "", decimals = 2, duration = 900) {
    if (!element) return;
    const start = Number(element.dataset.currentValue || 0);
    const end = Number(target || 0);
    const startedAt = performance.now();

    function frame(now) {
      const progress = Math.min(1, (now - startedAt) / duration);
      const eased = 1 - Math.pow(1 - progress, 3);
      const value = start + ((end - start) * eased);
      element.textContent = `${prefix}${value.toLocaleString(undefined, {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
      })}${suffix}`;
      if (progress < 1) {
        requestAnimationFrame(frame);
      } else {
        element.dataset.currentValue = String(end);
      }
    }

    requestAnimationFrame(frame);
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
    return "Start";
  }

  function buildEquityChart(container, dataPoints, range) {
    if (!container) return;
    const points = Array.isArray(dataPoints) ? dataPoints.filter((row) => row && Number.isFinite(Number(row.equity_usd ?? row.total_value_usd))) : [];
    if (!points.length) {
      container.innerHTML = `<div class="hc-chart-empty">No portfolio history available for this range.</div>`;
      return;
    }

    const width = 800;
    const height = 280;
    const leftPad = 78;
    const rightPad = 22;
    const topPad = 18;
    const bottomPad = 36;
    const values = points.map((row) => Number(row.equity_usd ?? row.total_value_usd ?? 0));
    const min = Math.min(...values);
    const max = Math.max(...values);
    const spread = Math.max(1, max - min);
    const chartHeight = height - topPad - bottomPad;
    const chartWidth = width - leftPad - rightPad;

    const coords = points.map((row, index) => {
      const x = leftPad + ((chartWidth * index) / Math.max(points.length - 1, 1));
      const y = topPad + ((max - Number(row.equity_usd ?? row.total_value_usd ?? 0)) / spread) * chartHeight;
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
      <svg viewBox="0 0 800 280" width="100%" role="img" aria-label="Portfolio equity curve">
        <defs>
          <linearGradient id="hcChartStroke" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stop-color="#2dd4bf"></stop>
            <stop offset="100%" stop-color="#60a5fa"></stop>
          </linearGradient>
          <linearGradient id="hcChartArea" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stop-color="rgba(45,212,191,0.25)"></stop>
            <stop offset="100%" stop-color="rgba(45,212,191,0)"></stop>
          </linearGradient>
          <filter id="hcGlow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="4" result="blur"></feGaussianBlur>
            <feMerge>
              <feMergeNode in="blur"></feMergeNode>
              <feMergeNode in="SourceGraphic"></feMergeNode>
            </feMerge>
          </filter>
        </defs>
        ${gridLines.join("")}
        <path d="${areaPath}" fill="url(#hcChartArea)"></path>
        <path d="${linePath}" fill="none" stroke="url(#hcChartStroke)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"></path>
        <circle cx="${last.x.toFixed(2)}" cy="${last.y.toFixed(2)}" r="8" fill="rgba(45,212,191,0.25)" filter="url(#hcGlow)"></circle>
        <circle cx="${last.x.toFixed(2)}" cy="${last.y.toFixed(2)}" r="4" fill="#2dd4bf"></circle>
        <text x="${leftPad}" y="${height - 12}" fill="rgba(255,255,255,0.4)" font-size="12" font-family="DM Sans, system-ui, sans-serif">${escapeHtml(rangeLabel(range))}</text>
        <text x="${width - 54}" y="${height - 12}" fill="rgba(255,255,255,0.4)" font-size="12" font-family="DM Sans, system-ui, sans-serif">Now</text>
      </svg>
    `;
  }

  function setRangeButtons(activeRange) {
    document.querySelectorAll(".hc-range-btn").forEach((button) => {
      const isActive = button.dataset.range === activeRange;
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-pressed", isActive ? "true" : "false");
    });
  }

  function updateRegimeBadge(regime) {
    const badge = document.getElementById("regimeBadge");
    const label = document.getElementById("regimeLabel");
    if (!badge || !label) return;

    const normalized = String(regime || "neutral").toLowerCase();
    badge.classList.remove("is-bull", "is-neutral", "is-caution", "is-risk_off");
    badge.classList.add(`is-${normalized}`);

    const labels = {
      bull: "Risk On",
      neutral: "Neutral",
      caution: "Caution",
      risk_off: "Risk Off"
    };
    label.textContent = labels[normalized] || "Unknown";
  }

  function setHeroPnl(pnlUsd, pnlPct) {
    const host = document.getElementById("heroDailyPnl");
    if (!host) return;
    const valueEl = host.querySelector(".hc-hero-pnl-value");
    const pctEl = host.querySelector(".hc-hero-pnl-pct");
    host.classList.remove("positive", "negative");
    host.classList.add(pnlUsd >= 0 ? "positive" : "negative");
    if (valueEl) valueEl.textContent = fmtSignedUsd(pnlUsd);
    if (pctEl) pctEl.textContent = `(${fmtSignedPct(pnlPct)})`;
  }

  function inferRiskScore(snapshot) {
    const cashPct = Number(snapshot?.usd_cash || 0) / Math.max(Number(snapshot?.total_value_usd || 1), 1);
    const satPct = Number(snapshot?.satellite_value_usd || 0) / Math.max(Number(snapshot?.total_value_usd || 1), 1);
    const regime = String(snapshot?.market_regime || "neutral").toLowerCase();
    let score = Math.round((satPct * 100) + ((1 - cashPct) * 35));
    if (regime === "bull") score += 5;
    if (regime === "caution") score += 10;
    if (regime === "risk_off") score += 18;
    score = Math.max(0, Math.min(100, score));

    let label = "Balanced posture";
    if (score < 40) label = "Risk contained";
    else if (score < 70) label = "Measured exposure";
    else label = "Elevated exposure";

    return { score, label };
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

  function renderHoldings(snapshot) {
    const host = document.getElementById("hcHoldingsGrid");
    if (!host) return;

    const positions = snapshot?.positions && typeof snapshot.positions === "object" ? snapshot.positions : {};
    const coreAssets = new Set(Object.keys(snapshot?.config?.core_assets || {}));
    const rows = Object.entries(positions)
      .map(([productId, row]) => ({
        productId,
        row: row || {}
      }))
      .filter(({ row }) => Number(row.value_total_usd || 0) > 1.0)
      .sort((a, b) => Number(b.row.value_total_usd || 0) - Number(a.row.value_total_usd || 0));

    if (!rows.length) {
      host.innerHTML = `<div class="hc-empty-card">No active holdings available.</div>`;
      return;
    }

    host.innerHTML = rows.map(({ productId, row }) => {
      const symbol = String(productId || "").split("-")[0];
      const isCore = coreAssets.has(productId);
      const changePct = Number(row.unrealized_pnl_pct || row.change_24h || 0);
      const positive = changePct >= 0;
      return `
        <div class="hc-pos-card">
          <div class="hc-pos-icon ${isCore ? "core" : "satellite"}">${escapeHtml(symbol.slice(0, 2))}</div>
          <div class="hc-pos-info">
            <div class="hc-pos-symbol">${escapeHtml(symbol)}</div>
            <div class="hc-pos-name">${escapeHtml(symbolName(productId))}</div>
          </div>
          ${buildSparkline(productId, positive)}
          <div class="hc-pos-right">
            <div class="hc-pos-value">${escapeHtml(fmtUsd(row.value_total_usd || 0))}</div>
            <div class="hc-pos-change ${positive ? "positive" : "negative"}">${escapeHtml(fmtSignedPct(changePct, true))}</div>
          </div>
        </div>
      `;
    }).join("");
  }

  function renderActivity(trades) {
    const host = document.getElementById("hcRecentActivity");
    if (!host) return;

    const rows = Array.isArray(trades) ? trades.filter((row) => row && typeof row === "object") : [];
    if (!rows.length) {
      host.innerHTML = `<div class="hc-empty-card">No recent trades available.</div>`;
      return;
    }

    host.innerHTML = rows.slice(0, 8).map((trade) => {
      const side = String(trade.side || "BUY").toUpperCase();
      const badgeClass = side === "BUY" ? "buy" : side === "EXIT" ? "exit" : "trim";
      const signal = String(trade.signal_type || trade.side || "").toLowerCase().replaceAll("_", " ") || "execution";
      const amount = Number(trade.price || 0) * Number(trade.base_size || 0);
      return `
        <div class="hc-trade-card">
          <div class="hc-trade-badge ${badgeClass}">${escapeHtml(side)}</div>
          <div class="hc-trade-info">
            <div class="hc-trade-symbol">${escapeHtml(trade.product_id || "Unknown")}</div>
            <div class="hc-trade-signal">${escapeHtml(signal)}</div>
          </div>
          <div class="hc-trade-right">
            <div class="hc-trade-amount">${escapeHtml(fmtUsd(amount))}</div>
            <div class="hc-trade-time">${escapeHtml(relativeTime(trade.created_at))}</div>
          </div>
        </div>
      `;
    }).join("");
  }

  async function loadDashboard() {
    try {
      const resp = await fetchJson("/api/portfolio");
      const snapshot = resp.snapshot || {};
      const summary = resp.summary || {};
      const totalValue = Number(snapshot.total_value_usd || 0);
      const cash = Number(snapshot.usd_cash || 0);
      const coreValue = Number(snapshot.core_value_usd || 0);
      const satelliteValue = Number(snapshot.satellite_value_usd || 0);
      const regime = String(summary.market_regime || "neutral");
      const totalPnl = Number(
        summary.total_pnl_usd ??
        summary.realized_pnl_usd ??
        0
      );

      const hero = document.getElementById("heroPortfolioValue");
      if (hero) {
        animateValue(hero, totalValue, "$", "", 2, lastPortfolioValue ? 600 : 1100);
      }
      lastPortfolioValue = totalValue;

      const totalPnlEl = document.getElementById("heroTotalPnl");
      if (totalPnlEl) totalPnlEl.textContent = fmtSignedUsd(totalPnl);

      const meta = document.getElementById("heroMeta");
      if (meta) {
        meta.innerHTML = `All-time: <span id="heroTotalPnl">${escapeHtml(fmtSignedUsd(totalPnl))}</span>`;
      }

      const cashPct = totalValue > 0 ? cash / totalValue : 0;
      const corePct = totalValue > 0 ? coreValue / totalValue : 0;
      const satPct = totalValue > 0 ? satelliteValue / totalValue : 0;

      const risk = inferRiskScore(snapshot);
      const riskScoreEl = document.getElementById("ctxRiskScore");
      const riskLabelEl = document.getElementById("ctxRiskLabel");
      if (riskScoreEl) {
        riskScoreEl.classList.remove("is-low", "is-mid", "is-high");
        riskScoreEl.classList.add(risk.score < 40 ? "is-low" : risk.score < 70 ? "is-mid" : "is-high");
        riskScoreEl.innerHTML = `${risk.score}<span class="hc-context-unit">/100</span>`;
      }
      if (riskLabelEl) riskLabelEl.textContent = risk.label;

      const positions = snapshot.positions && typeof snapshot.positions === "object" ? Object.entries(snapshot.positions) : [];
      const coreAssets = new Set(Object.keys(snapshot.config?.core_assets || {}));
      const coreCount = positions.filter(([productId, row]) => coreAssets.has(productId) && Number(row?.value_total_usd || 0) > 0).length;
      const satelliteCount = positions.filter(([productId, row]) => !coreAssets.has(productId) && Number(row?.value_total_usd || 0) > 0).length;

      document.getElementById("ctxCash").textContent = fmtUsd(cash);
      document.getElementById("ctxCashPct").textContent = `${fmtPct(cashPct)} of portfolio`;
      document.getElementById("allocCore").style.width = `${Math.max(0, corePct * 100)}%`;
      document.getElementById("allocSat").style.width = `${Math.max(0, satPct * 100)}%`;
      document.getElementById("allocCash").style.width = `${Math.max(0, cashPct * 100)}%`;
      document.getElementById("allocCorePct").textContent = Math.round(corePct * 100);
      document.getElementById("allocSatPct").textContent = Math.round(satPct * 100);
      document.getElementById("allocCashPct").textContent = Math.round(cashPct * 100);
      document.getElementById("ctxPositionCount").textContent = String(coreCount + satelliteCount);
      document.getElementById("ctxPositionBreakdown").textContent = `${coreCount} core · ${satelliteCount} satellite`;

      updateRegimeBadge(regime);
      renderHoldings(snapshot);
    } catch (error) {
      console.warn("Dashboard snapshot load failed:", error);
      const host = document.getElementById("hcHoldingsGrid");
      if (host) host.innerHTML = `<div class="hc-empty-card">Dashboard snapshot unavailable.</div>`;
    }
  }

  async function loadChart(range) {
    try {
      const resp = await fetchJson(`/api/portfolio/history?range=${encodeURIComponent(range)}`);
      const points = Array.isArray(resp.points) ? resp.points : [];
      buildEquityChart(document.getElementById("hcChartHost"), points, range);

      if (points.length >= 2) {
        const first = Number(points[0].equity_usd ?? points[0].total_value_usd ?? 0);
        const last = Number(points[points.length - 1].equity_usd ?? points[points.length - 1].total_value_usd ?? 0);
        const prev = Number(points[Math.max(0, points.length - 2)].equity_usd ?? points[Math.max(0, points.length - 2)].total_value_usd ?? first);
        const dailyPnl = last - prev;
        const dailyPct = prev > 0 ? dailyPnl / prev : 0;
        setHeroPnl(dailyPnl, dailyPct);
      }
    } catch (error) {
      console.warn("Dashboard chart load failed:", error);
      const host = document.getElementById("hcChartHost");
      if (host) {
        host.innerHTML = `<div class="hc-chart-empty">Equity curve unavailable right now.</div>`;
      }
    }
  }

  async function loadActivity() {
    try {
      const data = await fetchJson("/api/trades");
      renderActivity(data.trades || []);
    } catch (error) {
      console.warn("Dashboard activity load failed:", error);
      const host = document.getElementById("hcRecentActivity");
      if (host) host.innerHTML = `<div class="hc-empty-card">Recent activity unavailable.</div>`;
    }
  }

  function bindRangeButtons() {
    document.querySelectorAll(".hc-range-btn").forEach((button) => {
      button.addEventListener("click", () => {
        const nextRange = button.dataset.range || "30d";
        if (nextRange === currentRange) return;
        currentRange = nextRange;
        setRangeButtons(currentRange);
        loadChart(currentRange);
      });
    });
  }

  function startAutoRefresh() {
    if (refreshTimer) window.clearInterval(refreshTimer);
    refreshTimer = window.setInterval(() => {
      loadDashboard();
      loadChart(currentRange);
      loadActivity();
    }, 30000);
  }

  bindRangeButtons();
  setRangeButtons(currentRange);
  loadDashboard();
  loadChart(currentRange);
  loadActivity();
  startAutoRefresh();
})();
