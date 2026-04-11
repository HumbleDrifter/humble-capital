(function () {
  const SECRET = window.PERFORMANCE_CONFIG?.apiSecret || "";
  let currentRange = "30d";
  let lastRefreshAt = 0;
  let revealObserver = null;

  function authUrl(path) {
    if (!SECRET) return path;
    const sep = path.includes("?") ? "&" : "?";
    return `${path}${sep}secret=${encodeURIComponent(SECRET)}`;
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

  function fmtSignedUsd(value) {
    const numeric = Number(value || 0);
    return `${numeric >= 0 ? "+" : "-"}${fmtUsd(Math.abs(numeric))}`;
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

  function formatRangeLabel(range) {
    if (range === "7d") return "Last 7 days";
    if (range === "30d") return "Last 30 days";
    if (range === "90d") return "Last 90 days";
    return "Full history";
  }

  function rangeToDays(range) {
    if (range === "7d") return 7;
    if (range === "30d") return 30;
    if (range === "90d") return 90;
    return null;
  }

  function formatDuration(seconds) {
    const total = Math.max(0, Number(seconds || 0));
    const days = Math.floor(total / 86400);
    const hours = Math.floor((total % 86400) / 3600);
    const minutes = Math.floor((total % 3600) / 60);

    if (days > 0) return `${days}d ${hours}h`;
    if (hours > 0) return `${hours}h ${minutes}m`;
    return `${minutes}m`;
  }

  function formatUnixTime(ts) {
    const n = Number(ts || 0);
    if (!n) return "—";
    return new Date(n * 1000).toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
      year: "numeric"
    });
  }

  function observeRevealTargets() {
    if (!revealObserver) {
      revealObserver = new IntersectionObserver((entries) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting) return;
          entry.target.classList.add("perf-visible");
        });
      }, {
        threshold: 0.12,
        rootMargin: "0px 0px -8% 0px"
      });
    }

    document.querySelectorAll(".perf-observe, .perf-card-reveal").forEach((node) => {
      if (node.dataset.perfObserved === "true") return;
      node.dataset.perfObserved = "true";
      revealObserver.observe(node);
    });
  }

  function setRangeButtons(activeRange) {
    document.querySelectorAll(".hc-range-btn").forEach((button) => {
      const isActive = button.dataset.range === activeRange;
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-pressed", isActive ? "true" : "false");
    });
  }

  function buildPath(coords) {
    return coords
      .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`)
      .join(" ");
  }

  function buildEquityChart(container, equityPayload) {
    if (!container) return;
    const points = Array.isArray(equityPayload?.equity_points) ? equityPayload.equity_points.filter((row) => row && typeof row === "object") : [];
    if (!points.length) {
      container.innerHTML = `<div class="hc-chart-empty">No equity analytics available for this range.</div>`;
      return;
    }

    const width = 800;
    const height = 280;
    const leftPad = 78;
    const rightPad = 22;
    const topPad = 18;
    const bottomPad = 36;
    const chartHeight = height - topPad - bottomPad;
    const chartWidth = width - leftPad - rightPad;

    const values = points.flatMap((row) => [Number(row.value || 0), Number(row.hwm || 0)]);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const spread = Math.max(1, max - min);

    const equityCoords = points.map((row, index) => {
      const x = leftPad + ((chartWidth * index) / Math.max(points.length - 1, 1));
      const y = topPad + ((max - Number(row.value || 0)) / spread) * chartHeight;
      return { x, y };
    });
    const hwmCoords = points.map((row, index) => {
      const x = leftPad + ((chartWidth * index) / Math.max(points.length - 1, 1));
      const y = topPad + ((max - Number(row.hwm || 0)) / spread) * chartHeight;
      return { x, y };
    });

    const equityPath = buildPath(equityCoords);
    const hwmPath = buildPath(hwmCoords);
    const drawdownPath = `${hwmPath} L ${equityCoords.slice().reverse().map((point) => `${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(" L ")} Z`;
    const lastEquity = equityCoords[equityCoords.length - 1];

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
      <svg viewBox="0 0 800 280" width="100%" role="img" aria-label="Equity and drawdown chart">
        <defs>
          <linearGradient id="perfEquityStroke" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stop-color="#2dd4bf"></stop>
            <stop offset="100%" stop-color="#60a5fa"></stop>
          </linearGradient>
          <linearGradient id="perfDrawdownFill" x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stop-color="rgba(251,113,133,0.18)"></stop>
            <stop offset="100%" stop-color="rgba(251,113,133,0.02)"></stop>
          </linearGradient>
          <filter id="perfEquityGlow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="4" result="blur"></feGaussianBlur>
            <feMerge>
              <feMergeNode in="blur"></feMergeNode>
              <feMergeNode in="SourceGraphic"></feMergeNode>
            </feMerge>
          </filter>
        </defs>
        ${gridLines.join("")}
        <path d="${drawdownPath}" fill="url(#perfDrawdownFill)"></path>
        <path d="${hwmPath}" fill="none" stroke="rgba(255,255,255,0.35)" stroke-width="1.5" stroke-dasharray="6 6"></path>
        <path d="${equityPath}" fill="none" stroke="url(#perfEquityStroke)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"></path>
        <circle cx="${lastEquity.x.toFixed(2)}" cy="${lastEquity.y.toFixed(2)}" r="8" fill="rgba(45,212,191,0.25)" filter="url(#perfEquityGlow)"></circle>
        <circle cx="${lastEquity.x.toFixed(2)}" cy="${lastEquity.y.toFixed(2)}" r="4" fill="#2dd4bf"></circle>
        <text x="${leftPad}" y="${height - 12}" fill="rgba(255,255,255,0.4)" font-size="12" font-family="DM Sans, system-ui, sans-serif">${escapeHtml(formatRangeLabel(currentRange))}</text>
        <text x="${width - 54}" y="${height - 12}" fill="rgba(255,255,255,0.4)" font-size="12" font-family="DM Sans, system-ui, sans-serif">Now</text>
      </svg>
    `;
  }

  function buildDailyPnlChart(container, rows) {
    if (!container) return;
    const points = Array.isArray(rows) ? rows.filter((row) => row && typeof row === "object") : [];
    if (!points.length) {
      container.innerHTML = `<div class="hc-chart-empty">No closed-trade P&amp;L for this range.</div>`;
      return;
    }

    const width = 800;
    const height = 260;
    const leftPad = 62;
    const rightPad = 18;
    const topPad = 18;
    const bottomPad = 42;
    const chartHeight = height - topPad - bottomPad;
    const chartWidth = width - leftPad - rightPad;
    const values = points.map((row) => Number(row.pnl_usd || 0));
    const absMax = Math.max(1, ...values.map((value) => Math.abs(value)));
    const zeroY = topPad + (chartHeight / 2);
    const barWidth = Math.max(8, Math.min(28, chartWidth / Math.max(points.length, 1) - 6));

    const bars = points.map((row, index) => {
      const x = leftPad + (index * (chartWidth / Math.max(points.length, 1))) + 4;
      const pnl = Number(row.pnl_usd || 0);
      const heightScale = (Math.abs(pnl) / absMax) * ((chartHeight / 2) - 8);
      const y = pnl >= 0 ? zeroY - heightScale : zeroY;
      const fill = pnl >= 0 ? "#2dd4bf" : "#fb7185";
      const label = String(row.date || "");
      return `
        <rect x="${x.toFixed(2)}" y="${y.toFixed(2)}" width="${barWidth.toFixed(2)}" height="${heightScale.toFixed(2)}" rx="6" fill="${fill}"></rect>
        ${index % Math.max(1, Math.ceil(points.length / 6)) === 0 ? `<text x="${(x + (barWidth / 2)).toFixed(2)}" y="${height - 14}" text-anchor="middle" fill="rgba(255,255,255,0.38)" font-size="11" font-family="DM Sans, system-ui, sans-serif">${escapeHtml(label.slice(5))}</text>` : ""}
      `;
    }).join("");

    container.innerHTML = `
      <svg viewBox="0 0 800 260" width="100%" role="img" aria-label="Daily P&L bar chart">
        <line x1="${leftPad}" y1="${zeroY.toFixed(2)}" x2="${width - rightPad}" y2="${zeroY.toFixed(2)}" stroke="rgba(255,255,255,0.10)" stroke-width="1"></line>
        <text x="10" y="${(topPad + 10).toFixed(2)}" fill="rgba(255,255,255,0.4)" font-size="12" font-family="DM Sans, system-ui, sans-serif">${escapeHtml(fmtUsd(absMax))}</text>
        <text x="10" y="${(zeroY + 4).toFixed(2)}" fill="rgba(255,255,255,0.4)" font-size="12" font-family="DM Sans, system-ui, sans-serif">$0</text>
        <text x="10" y="${(height - bottomPad + 10).toFixed(2)}" fill="rgba(255,255,255,0.4)" font-size="12" font-family="DM Sans, system-ui, sans-serif">-${escapeHtml(fmtUsd(absMax).replace("$", ""))}</text>
        ${bars}
      </svg>
    `;
  }

  function renderSummary(summary, equity) {
    const totalTrades = Number(summary?.total_trades || 0);
    const winningTrades = Number(summary?.winning_trades || 0);
    const losingTrades = Number(summary?.losing_trades || 0);
    const winRate = Number(summary?.win_rate || 0);
    const totalPnl = Number(summary?.total_pnl_usd || 0);
    const profitFactor = Number(summary?.profit_factor || 0);
    const avgHold = Number(summary?.avg_hold_duration_sec || 0);

    const winRateEl = document.getElementById("perfWinRate");
    winRateEl.textContent = fmtPct(winRate);
    winRateEl.classList.remove("perf-positive", "perf-negative");
    winRateEl.classList.add(winRate >= 0.5 ? "perf-positive" : "perf-negative");
    document.getElementById("perfWinRateDetail").textContent = `${winningTrades} winners · ${losingTrades} losers`;

    const totalPnlEl = document.getElementById("perfTotalPnl");
    totalPnlEl.textContent = fmtSignedUsd(totalPnl);
    totalPnlEl.classList.remove("perf-positive", "perf-negative");
    totalPnlEl.classList.add(totalPnl >= 0 ? "perf-positive" : "perf-negative");
    document.getElementById("perfTotalPnlDetail").textContent = `${totalTrades} closed trade(s)`;

    document.getElementById("perfProfitFactor").textContent = profitFactor ? profitFactor.toFixed(2) : "0.00";
    document.getElementById("perfProfitFactorDetail").textContent = summary?.largest_win_usd || summary?.largest_loss_usd ? `Largest win ${fmtUsd(summary?.largest_win_usd || 0)}` : "Gross wins / gross losses";

    document.getElementById("perfHoldDuration").textContent = formatDuration(avgHold);
    document.getElementById("perfHoldDurationDetail").textContent = summary?.current_streak?.count ? `${summary.current_streak.type} streak · ${summary.current_streak.count}` : "Average time in trade";

    document.getElementById("perfCurrentDrawdown").textContent = fmtPct(equity?.current_drawdown_pct || 0, true);
    document.getElementById("perfCurrentDrawdown").classList.remove("perf-positive", "perf-negative");
    document.getElementById("perfCurrentDrawdown").classList.add(Number(equity?.current_drawdown_pct || 0) > 0 ? "perf-negative" : "perf-positive");
    document.getElementById("perfCurrentDrawdownDetail").textContent = `${fmtUsd(equity?.current_drawdown_usd || 0)} below HWM`;

    document.getElementById("perfMaxDrawdown").textContent = fmtPct(equity?.max_drawdown_pct || 0, true);
    document.getElementById("perfMaxDrawdown").classList.remove("perf-positive", "perf-negative");
    document.getElementById("perfMaxDrawdown").classList.add(Number(equity?.max_drawdown_pct || 0) > 0 ? "perf-negative" : "perf-positive");
    document.getElementById("perfMaxDrawdownDetail").textContent = `${fmtUsd(equity?.max_drawdown_usd || 0)} peak-to-trough`;

    document.getElementById("perfDaysSinceHwm").textContent = String(Number(equity?.days_since_hwm || 0));
    document.getElementById("perfDaysSinceHwmDetail").textContent = "Time since best equity print";

    document.getElementById("perfCurrentVsHwm").textContent = fmtUsd(equity?.current_value_usd || 0);
    document.getElementById("perfCurrentVsHwmDetail").textContent = `HWM ${fmtUsd(equity?.high_water_mark_usd || 0)}`;

    document.getElementById("perfMeta").textContent = `${formatRangeLabel(currentRange)} • ${totalTrades} trades • best ${summary?.best_product || "—"} • worst ${summary?.worst_product || "—"}`;
    document.getElementById("perfEquityMeta").textContent = `Current ${fmtUsd(equity?.current_value_usd || 0)} • HWM ${fmtUsd(equity?.high_water_mark_usd || 0)} • generated ${formatUnixTime(summary?.generated_at || equity?.generated_at || 0)}`;
  }

  function renderProductBreakdown(rows) {
    const host = document.getElementById("perfProductGrid");
    const meta = document.getElementById("perfProductMeta");
    if (!host) return;

    const items = Array.isArray(rows) ? rows.filter((row) => row && typeof row === "object") : [];
    meta.textContent = items.length ? `${items.length} product(s) with closed-trade history` : "No product performance data yet";

    if (!items.length) {
      host.innerHTML = `<div class="hc-empty-card">No product performance data yet.</div>`;
      return;
    }

    host.innerHTML = items.map((row) => {
      const productId = String(row.product_id || "");
      const symbol = productId.split("-")[0] || productId;
      const totalPnl = Number(row.total_pnl_usd || 0);
      const avgPnl = Number(row.avg_pnl_usd || 0);
      const winRate = Number(row.win_rate || 0);
      const positive = totalPnl >= 0;

      return `
        <article class="hc-pos-card perf-product-card perf-card-reveal">
          <div class="hc-pos-icon core">${escapeHtml(symbol.slice(0, 2))}</div>
          <div class="hc-pos-info">
            <div class="hc-pos-symbol">${escapeHtml(symbol)}</div>
            <div class="hc-pos-name">${escapeHtml(symbolName(productId))}</div>
            <div class="perf-product-meta">${Number(row.trade_count || 0)} trades · ${fmtPct(winRate)}</div>
          </div>
          <div class="perf-product-right">
            <div class="hc-pos-value ${positive ? "perf-positive" : "perf-negative"}">${escapeHtml(fmtSignedUsd(totalPnl))}</div>
            <div class="perf-product-sub">${escapeHtml(`Avg ${fmtSignedUsd(avgPnl)}`)}</div>
          </div>
        </article>
      `;
    }).join("");

    observeRevealTargets();
  }

  function renderRecentTrips(rows) {
    const host = document.getElementById("perfTripFeed");
    const meta = document.getElementById("perfTripsMeta");
    if (!host) return;

    const items = Array.isArray(rows) ? rows.filter((row) => row && typeof row === "object") : [];
    meta.textContent = items.length ? `${items.length} most recent completed round trip(s)` : "No recent round trips yet";

    if (!items.length) {
      host.innerHTML = `<div class="hc-empty-card">No recent round trips yet.</div>`;
      return;
    }

    host.innerHTML = items.map((row) => {
      const pnl = Number(row.pnl_usd || 0);
      const positive = pnl >= 0;
      const productId = String(row.product_id || "");
      return `
        <article class="hc-trade-card perf-trip-card perf-card-reveal">
          <div class="hc-trade-badge ${positive ? "buy" : "exit"}">${positive ? "WIN" : "LOSS"}</div>
          <div class="hc-trade-info">
            <div class="hc-trade-symbol">${escapeHtml(productId)}</div>
            <div class="hc-trade-signal">${escapeHtml(fmtUsd(row.entry_price || 0))} → ${escapeHtml(fmtUsd(row.exit_price || 0))} · ${escapeHtml(formatDuration(row.hold_duration_sec || 0))}</div>
          </div>
          <div class="hc-trade-right">
            <div class="hc-trade-amount ${positive ? "perf-positive" : "perf-negative"}">${escapeHtml(fmtSignedUsd(pnl))}</div>
            <div class="hc-trade-time">${escapeHtml(formatUnixTime(row.closed_at || 0))}</div>
          </div>
        </article>
      `;
    }).join("");

    observeRevealTargets();
  }

  function buildOptionsStrategyPie(container, strategyBreakdown) {
    if (!container) return;
    const rows = Object.values(strategyBreakdown || {}).filter((row) => row && typeof row === "object");
    if (!rows.length) {
      container.innerHTML = `<div class="hc-chart-empty">No options strategy data yet.</div>`;
      return;
    }

    const total = rows.reduce((sum, row) => sum + Math.max(0, Math.abs(Number(row.pnl || 0))), 0) || 1;
    const colors = ["#60a5fa", "#38bdf8", "#fbbf24", "#fb7185", "#c084fc", "#94a3b8"];
    const radius = 72;
    const cx = 110;
    const cy = 110;
    let startAngle = -Math.PI / 2;
    const slices = rows.map((row, index) => {
      const value = Math.max(0.0001, Math.abs(Number(row.pnl || 0)));
      const angle = (value / total) * Math.PI * 2;
      const endAngle = startAngle + angle;
      const largeArc = angle > Math.PI ? 1 : 0;
      const x1 = cx + Math.cos(startAngle) * radius;
      const y1 = cy + Math.sin(startAngle) * radius;
      const x2 = cx + Math.cos(endAngle) * radius;
      const y2 = cy + Math.sin(endAngle) * radius;
      const path = `M ${cx} ${cy} L ${x1.toFixed(2)} ${y1.toFixed(2)} A ${radius} ${radius} 0 ${largeArc} 1 ${x2.toFixed(2)} ${y2.toFixed(2)} Z`;
      const color = colors[index % colors.length];
      startAngle = endAngle;
      return { path, color, row };
    });

    container.innerHTML = `
      <div style="display:grid;grid-template-columns:220px 1fr;gap:18px;align-items:center;">
        <svg viewBox="0 0 220 220" width="100%" role="img" aria-label="Options strategy breakdown pie chart">
          ${slices.map((slice) => `<path d="${slice.path}" fill="${slice.color}" opacity="0.95"></path>`).join("")}
          <circle cx="${cx}" cy="${cy}" r="34" fill="#0b1220"></circle>
          <text x="${cx}" y="${cy - 2}" text-anchor="middle" fill="#ecf3ff" font-size="18" font-family="DM Sans, system-ui, sans-serif">${rows.length}</text>
          <text x="${cx}" y="${cy + 18}" text-anchor="middle" fill="rgba(255,255,255,0.45)" font-size="12" font-family="DM Sans, system-ui, sans-serif">strategies</text>
        </svg>
        <div style="display:flex;flex-direction:column;gap:10px;">
          ${slices.map((slice) => `
            <div style="display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px 12px;border-radius:12px;background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);">
              <div style="display:flex;align-items:center;gap:10px;min-width:0;">
                <span style="width:10px;height:10px;border-radius:999px;background:${slice.color};display:inline-block;"></span>
                <span>${escapeHtml(String(slice.row.strategy || "unknown").replaceAll("_", " "))}</span>
              </div>
              <strong class="${Number(slice.row.pnl || 0) >= 0 ? "perf-positive" : "perf-negative"}">${escapeHtml(fmtSignedUsd(slice.row.pnl || 0))}</strong>
            </div>
          `).join("")}
        </div>
      </div>
    `;
  }

  function renderOptionsStrategyTable(container, strategyBreakdown) {
    if (!container) return;
    const rows = Object.values(strategyBreakdown || {}).filter((row) => row && typeof row === "object");
    if (!rows.length) {
      container.innerHTML = `<div class="hc-chart-empty">No options trade history to compare yet.</div>`;
      return;
    }

    container.innerHTML = `
      <div style="display:flex;flex-direction:column;gap:10px;">
        ${rows.map((row) => `
          <div style="display:grid;grid-template-columns:minmax(0,1.4fr) repeat(3,minmax(0,1fr));gap:10px;align-items:center;padding:12px 14px;border-radius:14px;background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);">
            <div>
              <strong>${escapeHtml(String(row.strategy || "unknown").replaceAll("_", " "))}</strong>
              <div style="color:rgba(255,255,255,0.5);font-size:12px;">${Number(row.trades || 0)} trade(s)</div>
            </div>
            <div>${escapeHtml(fmtPct(row.win_rate || 0))}</div>
            <div>${escapeHtml(fmtSignedUsd(row.pnl || 0))}</div>
            <div>${escapeHtml(fmtUsd(row.premium_collected || 0))}</div>
          </div>
        `).join("")}
      </div>
    `;
  }

  function renderOptionsPerformance(summary, risk) {
    const monthlyHost = document.getElementById("perfOptionsMonthlyChartHost");
    const pieHost = document.getElementById("perfOptionsStrategyPieHost");
    const tableHost = document.getElementById("perfOptionsStrategyTableHost");

    const monthlyRows = Array.isArray(summary?.monthly_returns) ? summary.monthly_returns : [];
    const monthlyAsDaily = monthlyRows.map((row) => ({ date: String(row.month || ""), pnl_usd: Number(row.pnl || 0) }));
    buildDailyPnlChart(monthlyHost, monthlyAsDaily);
    buildOptionsStrategyPie(pieHost, summary?.strategy_breakdown || {});
    renderOptionsStrategyTable(tableHost, summary?.strategy_breakdown || {});

    const totalPnl = Number(summary?.total_pnl || 0);
    const totalPnlEl = document.getElementById("perfOptionsTotalPnl");
    if (totalPnlEl) {
      totalPnlEl.textContent = fmtSignedUsd(totalPnl);
      totalPnlEl.classList.remove("perf-positive", "perf-negative");
      totalPnlEl.classList.add(totalPnl >= 0 ? "perf-positive" : "perf-negative");
    }
    const premiumEl = document.getElementById("perfOptionsPremiumCollected");
    if (premiumEl) premiumEl.textContent = `Premium collected ${fmtUsd(summary?.total_premium_collected || 0)} · Fees ${fmtUsd(summary?.total_fees || 0)}`;

    const winRateEl = document.getElementById("perfOptionsWinRate");
    if (winRateEl) {
      winRateEl.textContent = fmtPct(summary?.win_rate || 0);
      winRateEl.classList.remove("perf-positive", "perf-negative");
      winRateEl.classList.add(Number(summary?.win_rate || 0) >= 0.5 ? "perf-positive" : "perf-negative");
    }
    const tradeCountEl = document.getElementById("perfOptionsTradeCount");
    if (tradeCountEl) tradeCountEl.textContent = `${Number(summary?.total_trades || 0)} trades · PF ${(Number(summary?.profit_factor || 0)).toFixed(2)}`;

    const riskValueEl = document.getElementById("perfOptionsRiskValue");
    if (riskValueEl) riskValueEl.textContent = fmtUsd(risk?.total_capital_at_risk || 0);
    const riskDetailEl = document.getElementById("perfOptionsRiskDetail");
    if (riskDetailEl) {
      const expiring = Array.isArray(risk?.expiring_soon) ? risk.expiring_soon.length : 0;
      const earnings = Array.isArray(risk?.earnings_exposure) ? risk.earnings_exposure.length : 0;
      riskDetailEl.textContent = `${expiring} expiring soon · ${earnings} near earnings · Δ ${Number(risk?.portfolio_delta || 0).toFixed(2)}`;
    }

    const optionsMeta = document.getElementById("perfOptionsMeta");
    if (optionsMeta) optionsMeta.textContent = `${monthlyRows.length} month bucket(s) · ${Object.keys(summary?.strategy_breakdown || {}).length} strategy buckets`;
    const monthlyMeta = document.getElementById("perfOptionsMonthlyMeta");
    if (monthlyMeta) monthlyMeta.textContent = `${monthlyRows.length} month(s) of realized options P&L`;
    const pieMeta = document.getElementById("perfOptionsPieMeta");
    if (pieMeta) pieMeta.textContent = `${Object.keys(summary?.strategy_breakdown || {}).length} tracked strategy group(s)`;
    const tableMeta = document.getElementById("perfOptionsTableMeta");
    if (tableMeta) tableMeta.textContent = `${Object.keys(summary?.strategy_breakdown || {}).length} strategy row(s)`;
  }

  async function refreshPerformance() {
    try {
      lastRefreshAt = Date.now();
      const days = rangeToDays(currentRange);
      const query = days ? `?days=${encodeURIComponent(days)}` : "";
      const [data, optionsSummary, optionsRisk] = await Promise.all([
        fetchJson(`/api/performance${query}`),
        fetchJson(`/api/options/performance?period=${encodeURIComponent(days ? currentRange : "all")}`).catch(() => ({ monthly_returns: [], strategy_breakdown: {}, total_pnl: 0, total_premium_collected: 0, total_fees: 0, total_trades: 0, win_rate: 0, profit_factor: 0 })),
        fetchJson(`/api/options/performance/risk`).catch(() => ({ risk: {} }))
      ]);

      renderSummary(data.summary || {}, data.equity || {});
      buildEquityChart(document.getElementById("perfEquityChartHost"), data.equity || {});
      buildDailyPnlChart(document.getElementById("perfDailyChartHost"), data.daily_pnl || []);
      document.getElementById("perfDailyMeta").textContent = `${formatRangeLabel(currentRange)} • ${Array.isArray(data.daily_pnl) ? data.daily_pnl.length : 0} daily bucket(s)`;
      renderProductBreakdown(data.product_breakdown || []);
      renderRecentTrips(data.recent_round_trips || []);
      renderOptionsPerformance(optionsSummary || {}, optionsRisk?.risk || {});
      observeRevealTargets();
    } catch (err) {
      console.error(err);
      const meta = document.getElementById("perfMeta");
      const equityHost = document.getElementById("perfEquityChartHost");
      const dailyHost = document.getElementById("perfDailyChartHost");
      const productGrid = document.getElementById("perfProductGrid");
      const tripFeed = document.getElementById("perfTripFeed");
      const optionsMonthly = document.getElementById("perfOptionsMonthlyChartHost");
      const optionsPie = document.getElementById("perfOptionsStrategyPieHost");
      const optionsTable = document.getElementById("perfOptionsStrategyTableHost");
      if (meta) meta.textContent = `Performance analytics load failed: ${err.message}`;
      if (equityHost) equityHost.innerHTML = `<div class="hc-chart-empty">Equity analytics unavailable.</div>`;
      if (dailyHost) dailyHost.innerHTML = `<div class="hc-chart-empty">Daily P&amp;L unavailable.</div>`;
      if (productGrid) productGrid.innerHTML = `<div class="hc-empty-card">Product performance unavailable.</div>`;
      if (tripFeed) tripFeed.innerHTML = `<div class="hc-empty-card">Recent round trips unavailable.</div>`;
      if (optionsMonthly) optionsMonthly.innerHTML = `<div class="hc-chart-empty">Options monthly P&amp;L unavailable.</div>`;
      if (optionsPie) optionsPie.innerHTML = `<div class="hc-chart-empty">Options strategy breakdown unavailable.</div>`;
      if (optionsTable) optionsTable.innerHTML = `<div class="hc-chart-empty">Options win-rate table unavailable.</div>`;
    }
  }

  function bindRangeButtons() {
    document.querySelectorAll(".hc-range-btn").forEach((button) => {
      button.addEventListener("click", () => {
        const nextRange = button.dataset.range || "30d";
        if (nextRange === currentRange) return;
        currentRange = nextRange;
        setRangeButtons(currentRange);
        refreshPerformance();
      });
    });
  }

  bindRangeButtons();
  setRangeButtons(currentRange);
  observeRevealTargets();
  refreshPerformance();

  setInterval(() => {
    refreshPerformance();
  }, 60000);

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState !== "visible") return;
    if (Date.now() - lastRefreshAt < 15000) return;
    refreshPerformance();
  });
})();
