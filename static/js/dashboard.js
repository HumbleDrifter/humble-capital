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

  let _chartGeneration = 0;

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

    const tooltipHost = container.parentElement || container;
    tooltipHost.style.position = "relative";
    _chartGeneration += 1;
    const myGeneration = _chartGeneration;
    const tooltip = document.getElementById("equityTooltip");
      if (tooltip) { tooltip.style.display = "none"; }
      const svg = container.querySelector("svg");
      if (!svg || !tooltip) return;

    const ensureCrosshair = (id, tagName) => {
      let node = svg.querySelector(`#${id}`);
      if (!node) {
        node = document.createElementNS("http://www.w3.org/2000/svg", tagName);
        node.setAttribute("id", id);
        svg.appendChild(node);
      }
      return node;
    };

    const crosshair = ensureCrosshair("hcEquityCrosshair", "line");
    crosshair.setAttribute("stroke", "rgba(56,189,248,0.4)");
    crosshair.setAttribute("stroke-width", "1");
    crosshair.setAttribute("stroke-dasharray", "4,4");
    crosshair.style.display = "none";

    const dot = ensureCrosshair("hcEquityDot", "circle");
    dot.setAttribute("r", "4");
    dot.setAttribute("fill", "#38bdf8");
    dot.setAttribute("stroke", "#ecf3ff");
    dot.setAttribute("stroke-width", "1.5");
    dot.style.display = "none";

    const formatPointDate = (row) => {
      const raw = row?.ts ?? row?.timestamp ?? row?.date;
      if (raw == null) return "Unknown";
      if (typeof raw === "number" || /^\d+$/.test(String(raw))) {
        const ts = Number(raw);
        const millis = ts > 1e12 ? ts : ts * 1000;
        const date = new Date(millis);
        return Number.isNaN(date.getTime()) ? String(raw) : date.toLocaleString();
      }
      const parsed = new Date(raw);
      return Number.isNaN(parsed.getTime()) ? String(raw) : parsed.toLocaleString();
    };

    const showTooltip = (event) => {
      if (!svg.isConnected || myGeneration !== _chartGeneration) return;
        const rect = svg.getBoundingClientRect();
      const localX = event.clientX - rect.left;
      const clampedX = Math.max(leftPad, Math.min(width - rightPad, (localX / rect.width) * width));
      const ratio = (clampedX - leftPad) / Math.max(chartWidth, 1);
      const index = Math.max(0, Math.min(points.length - 1, Math.round(ratio * Math.max(points.length - 1, 0))));
      const point = points[index];
      const coord = coords[index];
      const value = Number(point?.equity_usd ?? point?.total_value_usd ?? 0);

      crosshair.setAttribute("x1", coord.x.toFixed(2));
      crosshair.setAttribute("x2", coord.x.toFixed(2));
      crosshair.setAttribute("y1", topPad.toFixed(2));
      crosshair.setAttribute("y2", (height - bottomPad).toFixed(2));
      crosshair.style.display = "block";

      dot.setAttribute("cx", coord.x.toFixed(2));
      dot.setAttribute("cy", coord.y.toFixed(2));
      dot.style.display = "block";

      tooltip.innerHTML = `
        <div style="font-weight:600; margin-bottom:2px;">${escapeHtml(formatPointDate(point))}</div>
        <div>${escapeHtml(fmtUsd(value))}</div>
      `;
      tooltip.style.display = "block";
      const tooltipX = Math.min(tooltipHost.clientWidth - 160, Math.max(8, (coord.x / width) * container.clientWidth + 12));
      const tooltipY = Math.max(8, (coord.y / height) * container.clientHeight - 12);
      tooltip.style.left = `${tooltipX}px`;
      tooltip.style.top = `${tooltipY}px`;
    };

    const hideTooltip = () => {
      tooltip.style.display = "none";
      crosshair.style.display = "none";
      dot.style.display = "none";
    };

    svg.onmousemove = showTooltip;
    svg.onmouseleave = hideTooltip;
    svg.onmouseenter = showTooltip;
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

  function buildHoldingCard(title, subtitle, value, pnlDisplay, pnlClass, href, badgeClass = "satellite") {
    return `
      <a class="holding-card-link" href="${escapeHtml(href)}">
        <div class="hc-pos-card">
          <div class="hc-pos-icon ${badgeClass}">${escapeHtml(String(title || "").slice(0, 2).toUpperCase())}</div>
          <div class="hc-pos-info">
            <div class="hc-pos-symbol">${escapeHtml(title || "Position")}</div>
            <div class="hc-pos-name">${escapeHtml(subtitle || "")}</div>
          </div>
          <div class="hc-pos-right">
            <div class="hc-pos-value">${escapeHtml(fmtUsd(value || 0))}</div>
            <div class="hc-pos-change ${pnlClass || ""}">${escapeHtml(pnlDisplay || "—")}</div>
          </div>
        </div>
      </a>
    `;
  }

  function renderHoldingsSection(title, totalValue, broker, cardsHtml) {
    return `
      <div class="holdings-section">
        <h3 class="holdings-group-title">
          <span>${escapeHtml(title)}</span>
          <span class="holdings-group-value">${escapeHtml(fmtUsd(totalValue || 0))}</span>
          <span class="holdings-group-badge">${escapeHtml(broker)}</span>
        </h3>
        <div class="hc-pos-grid">${cardsHtml || `<div class="hc-empty-card">No ${escapeHtml(title.toLowerCase())} positions.</div>`}</div>
      </div>
    `;
  }

  function updateBrokerBreakdown(snapshot) {
    const totalValue = Number(snapshot?.total_value_usd || 0);
    const coinbaseValue = Number(snapshot?.brokers?.coinbase?.value || snapshot?.coinbase_value_usd || 0);
    const webullValue = Number(snapshot?.brokers?.webull?.value || snapshot?.webull_value_usd || 0);
    const futuresBalance = Number(snapshot?.futures?.balance?.futures_balance || 0);
    const spotCoinbaseValue = Math.max(0, Number(snapshot?.coinbase_value_usd || coinbaseValue) - futuresBalance);
    const coinbasePct = totalValue > 0 ? (coinbaseValue / totalValue) * 100 : 0;
    const webullPct = totalValue > 0 ? (webullValue / totalValue) * 100 : 0;

    const coinbaseBar = document.getElementById("hcBrokerCoinbaseBar");
    const webullBar = document.getElementById("hcBrokerWebullBar");
    const coinbaseLabel = document.getElementById("hcBrokerCoinbaseValue");
    const webullLabel = document.getElementById("hcBrokerWebullValue");

    if (coinbaseBar) coinbaseBar.style.width = `${Math.max(0, coinbasePct)}%`;
    if (webullBar) webullBar.style.width = `${Math.max(0, webullPct)}%`;
    if (coinbaseLabel) {
      coinbaseLabel.textContent = futuresBalance > 0
        ? `${fmtUsd(coinbaseValue)} (Spot ${fmtUsd(spotCoinbaseValue)} + Futures ${fmtUsd(futuresBalance)})`
        : `${fmtUsd(coinbaseValue)} (${coinbasePct.toFixed(1)}%)`;
    }
    if (webullLabel) webullLabel.textContent = `${fmtUsd(webullValue)} (${webullPct.toFixed(1)}%)`;
  }

  function renderHoldings(snapshot) {
    const host = document.getElementById("hcHoldingsGrid");
    if (!host) return;

    const positions = snapshot?.positions && typeof snapshot.positions === "object" ? snapshot.positions : {};
    const dustMinValueUsd = Number(snapshot?.config?.dust_min_value_usd || 2.0);
    const cryptoRows = Object.entries(positions)
      .map(([productId, row]) => ({
        productId,
        row: row || {}
      }))
      .filter(({ row }) => Number(row.value_total_usd || 0) >= dustMinValueUsd)
      .sort((a, b) => Number(b.row.value_total_usd || 0) - Number(a.row.value_total_usd || 0));

    const webullStocks = Array.isArray(snapshot?.brokers?.webull?.stocks)
      ? snapshot.brokers.webull.stocks.filter((row) => Number(row?.market_value || 0) >= dustMinValueUsd)
      : [];
    const webullOptions = Array.isArray(snapshot?.brokers?.webull?.options)
      ? snapshot.brokers.webull.options.filter((row) => Number(row?.market_value || 0) >= dustMinValueUsd)
      : [];
    const futuresPositions = Array.isArray(snapshot?.futures?.positions)
      ? snapshot.futures.positions.filter((row) => Math.abs(Number(row?.unrealized_pnl || 0)) >= 0 || Number(row?.size || 0) > 0)
      : [];

    if (!cryptoRows.length && !webullStocks.length && !webullOptions.length && !futuresPositions.length) {
      host.innerHTML = `<div class="hc-empty-card">No active holdings available.</div>`;
      return;
    }

    const cryptoCards = cryptoRows.map(({ productId, row }) => {
      const symbol = String(productId || "").split("-")[0];
      const qty = Number(row.base_qty_total || 0);
      const entryPrice = Number(row.avg_entry_price || row.cost_basis || 0);
      const currentPrice = Number(row.price_usd || row.current_price || 0);
      let unrealizedPnl = Number(row.unrealized_pnl ?? row.unrealized_profit_loss ?? 0);
      let changePct = Number(row.unrealized_pnl_pct || row.change_24h || 0);
      if (entryPrice <= 0) {
        unrealizedPnl = null;
        changePct = null;
      } else {
        if (unrealizedPnl === 0 && currentPrice > 0 && qty > 0) {
          unrealizedPnl = (currentPrice - entryPrice) * qty;
        }
        if (changePct === 0 && currentPrice > 0) {
          changePct = ((currentPrice - entryPrice) / entryPrice) * 100;
        }
      }
      const positive = unrealizedPnl === null ? true : unrealizedPnl >= 0;
      const pnlDisplay = unrealizedPnl !== null
        ? `${fmtSignedUsd(unrealizedPnl)} (${fmtSignedPct(changePct, true)})`
        : "P&L unavailable";
      const pnlClass = unrealizedPnl === null ? "" : (positive ? "positive" : "negative");
      return buildHoldingCard(
        symbol,
        `${symbolName(productId)} · ${qty.toFixed(4)}`,
        Number(row.value_total_usd || 0),
        pnlDisplay,
        pnlClass,
        `/trading?symbol=${encodeURIComponent(productId)}#charts`,
        "core"
      );
    }).join("");

    const stockCards = webullStocks.map((row) => {
      const pnl = Number(row.unrealized_pnl || 0);
      const pnlPct = Number(row.unrealized_pnl_pct || 0);
      const pnlDisplay = `${fmtSignedUsd(pnl)} (${fmtSignedPct(pnlPct, true)})`;
      return buildHoldingCard(
        row.symbol || "Stock",
        `${Number(row.qty || 0).toFixed(2)} shares · Last ${fmtUsd(row.last_price || 0)}`,
        Number(row.market_value || 0),
        pnlDisplay,
        pnl >= 0 ? "positive" : "negative",
        `/trading?symbol=${encodeURIComponent(row.symbol || "")}#charts`,
        "satellite"
      );
    }).join("");

    const optionCards = webullOptions.map((row) => {
      const pnl = Number(row.unrealized_pnl || 0);
      const pnlPct = Number(row.unrealized_pnl_pct || 0);
      const strike = Number(row.strike || 0);
      const exp = String(row.expiration || "");
      const expShort = /^\d{4}-\d{2}-\d{2}$/.test(exp) ? exp.slice(5).replace("-", "/") : exp;
      const type = String(row.option_type || "").toUpperCase() || "OPTION";
      return buildHoldingCard(
        row.symbol || "Option",
        `${strike > 0 ? fmtUsd(strike) : "Strike n/a"} ${type} ${expShort} · ${Number(row.qty || 0)} contracts`,
        Number(row.market_value || 0),
        `${fmtSignedUsd(pnl)} (${fmtSignedPct(pnlPct, true)})`,
        pnl >= 0 ? "positive" : "negative",
        `/options?symbol=${encodeURIComponent(row.symbol || "")}#charts`,
        "satellite"
      );
    }).join("");

    const futuresCards = futuresPositions.map((row) => {
      const pnl = Number(row.unrealized_pnl || 0);
      const side = String(row.side || "").toLowerCase() === "short" ? "SHORT" : "LONG";
      const leverage = Number(row.leverage || 0);
      const displayName = row.display_name || row.product_id || "Futures";
      return buildHoldingCard(
        displayName,
        `${side} · ${fmtNum(row.size || 0, 4)} @ ${fmtUsd(row.entry_price || 0)}${leverage > 0 ? ` · ${fmtNum(leverage, 1)}x` : ""}`,
        Number(row.margin_used || row.unrealized_pnl || 0),
        `${fmtSignedUsd(pnl)}${leverage > 0 ? ` · ${fmtNum(leverage, 1)}x` : ""}`,
        side === "LONG" ? "positive" : "negative",
        `/trading?symbol=${encodeURIComponent(row.product_id || "")}#charts`,
        "satellite"
      );
    }).join("");

    const cryptoTotal = cryptoRows.reduce((sum, { row }) => sum + Number(row.value_total_usd || 0), 0);
    const stockTotal = webullStocks.reduce((sum, row) => sum + Number(row.market_value || 0), 0);
    const optionsTotal = webullOptions.reduce((sum, row) => sum + Number(row.market_value || 0), 0);
    const futuresTotal = Number(snapshot?.futures?.balance?.futures_balance || 0);

    const _newHoldingsHtml = [
      renderHoldingsSection("Crypto", cryptoTotal, "Coinbase", cryptoCards),
      renderHoldingsSection("Futures", futuresTotal, "Coinbase", futuresCards),
      renderHoldingsSection("Stocks", stockTotal, "Webull", stockCards),
      renderHoldingsSection("Options", optionsTotal, "Webull", optionCards)
    ].join("");
    if (host.innerHTML !== _newHoldingsHtml) {
      host.style.opacity = "1";
      host.innerHTML = _newHoldingsHtml;
    }
  }

  function renderActivity(trades) {
    const host = document.getElementById("hcRecentActivity");
    if (!host) return;

    const rows = Array.isArray(trades) ? trades.filter((row) => row && typeof row === "object") : [];
    if (!rows.length) {
      host.innerHTML = `<div class="hc-empty-card">No recent trades available.</div>`;
      return;
    }

    const _newActivityHtml = rows.slice(0, 12).map((trade) => {
      const side = String(trade.side || "BUY").toUpperCase();
      const badgeClass = side === "BUY" ? "buy" : side === "SELL" || side === "EXIT" ? "exit" : "trim";
      const signal = String(trade.signal_type || "").toLowerCase().replaceAll("_", " ") || String(side).toLowerCase();
      // Try multiple amount fields
      const amount = Number(trade.filled_value || trade.total_value ||
        (Number(trade.avg_fill_price || trade.price || 0) * Number(trade.filled_base || trade.base_size || 1)) || 0);
      const price = Number(trade.avg_fill_price || trade.price || 0);
      const sym = String(trade.product_id || trade.symbol || "Unknown");
      const priceText = price > 0 ? ` @ ${price < 1 ? price.toFixed(4) : price.toFixed(2)}` : "";
      const pnl = Number(trade.pnl || trade.realized_pnl || 0);
      const pnlText = pnl !== 0 ? `<span style="color:${pnl > 0 ? "#22c55e" : "#ef4444"};font-size:10px;font-weight:600;">${pnl > 0 ? "+" : ""}${fmtUsd(pnl)}</span>` : "";
      return `
        <div class="hc-trade-card">
          <div class="hc-trade-badge ${badgeClass}">${escapeHtml(side)}</div>
          <div class="hc-trade-info">
            <div class="hc-trade-symbol">${escapeHtml(sym)}${pnlText}</div>
            <div class="hc-trade-signal">${escapeHtml(signal)}${priceText}</div>
          </div>
          <div class="hc-trade-right">
            <div class="hc-trade-amount">${amount > 0 ? escapeHtml(fmtUsd(amount)) : "—"}</div>
            <div class="hc-trade-time">${escapeHtml(relativeTime(trade.created_at))}</div>
          </div>
        </div>
      `;
    }).join("");
    if (host.innerHTML !== _newActivityHtml) host.innerHTML = _newActivityHtml;
  }

  function renderDashboardSnapshot(snapshot, summary) {
    // Store day PnL for equity chart to use — always update
    const dayPnl = Number(snapshot.day_pnl_usd ?? 0);
    window._lastSnapshotDayPnl = dayPnl;
    const _heroTotalValue = Number(snapshot.total_value_usd || 0);
    const dailyPct = _heroTotalValue > 0 && (_heroTotalValue - dayPnl) > 0 ? dayPnl / (_heroTotalValue - dayPnl) : 0;
    setHeroPnl(dayPnl, dailyPct);
      const futuresBalanceRaw = Number(snapshot?.futures?.balance?.futures_balance || 0);
      const coinbaseValueRaw = Number(snapshot.coinbase_value_usd || 0);
      const futuresAlreadyInCoinbase = coinbaseValueRaw > 0 && futuresBalanceRaw > 0 &&
        Math.abs(coinbaseValueRaw - futuresBalanceRaw) > futuresBalanceRaw * 0.5;
      const totalValue = Number(snapshot.total_value_usd || 0) +
        (futuresAlreadyInCoinbase ? 0 : futuresBalanceRaw);
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
        const futuresBalance = Number(snapshot?.futures?.balance?.futures_balance || 0);
        const coinbaseValue = Number(snapshot.coinbase_value_usd || Math.max(0, totalValue - Number(snapshot.webull_value_usd || 0)));
        const spotCoinbaseValue = Math.max(0, coinbaseValue - futuresBalance);
        const webullValue = Number(snapshot.webull_value_usd || 0);
        if (webullValue > 0 || futuresBalance > 0) {
          meta.innerHTML = `Portfolio: <span>${escapeHtml(fmtUsd(totalValue))}</span> (Coinbase ${escapeHtml(fmtUsd(coinbaseValue))}${futuresBalance > 0 ? ` = Spot ${escapeHtml(fmtUsd(spotCoinbaseValue))} + Futures ${escapeHtml(fmtUsd(futuresBalance))}` : ""}${webullValue > 0 ? ` + Webull ${escapeHtml(fmtUsd(webullValue))}` : ""}) · All-time: <span id="heroTotalPnl">${escapeHtml(fmtSignedUsd(totalPnl))}</span>`;
        } else {
          meta.innerHTML = `All-time: <span id="heroTotalPnl">${escapeHtml(fmtSignedUsd(totalPnl))}</span>`;
        }
        }
        updateBrokerBreakdown(snapshot);

        const futuresBalance = Number(snapshot?.futures?.balance?.futures_balance || 0);
        const futuresBuyingPower = Number(snapshot?.futures?.balance?.buying_power || 0);
        const futuresMarginUsed = Number(snapshot?.futures?.balance?.initial_margin || 0);
        const cashPct = totalValue > 0 ? cash / totalValue : 0;
        const corePct = totalValue > 0 ? coreValue / totalValue : 0;
        const satPct = totalValue > 0 ? satelliteValue / totalValue : 0;
        const futuresPct = totalValue > 0 ? futuresBalance / totalValue : 0;

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
        const dustMin = Number(snapshot.config?.dust_min_value_usd || 2.0);
        const meaningfulCrypto = positions.filter(([, row]) => Number(row?.value_total_usd || 0) >= dustMin);
        const webullStocks = Array.isArray(snapshot?.brokers?.webull?.stocks) ? snapshot.brokers.webull.stocks.filter((row) => Number(row?.market_value || 0) >= dustMin) : [];
        const webullOptions = Array.isArray(snapshot?.brokers?.webull?.options) ? snapshot.brokers.webull.options.filter((row) => Number(row?.market_value || 0) >= dustMin) : [];
        const futuresPositions = Array.isArray(snapshot?.futures?.positions) ? snapshot.futures.positions.filter((row) => Number(row?.size || 0) > 0) : [];
        const coreCount = meaningfulCrypto.filter(([productId]) => coreAssets.has(productId)).length;
        const satelliteCount = meaningfulCrypto.filter(([productId]) => !coreAssets.has(productId)).length + webullStocks.length + webullOptions.length + futuresPositions.length;

        document.getElementById("ctxCash").textContent = fmtUsd(cash);
      document.getElementById("ctxCashPct").textContent = `${fmtPct(cashPct)} of portfolio`;
      document.getElementById("allocCore").style.width = `${Math.max(0, corePct * 100)}%`;
      document.getElementById("allocSat").style.width = `${Math.max(0, satPct * 100)}%`;
      document.getElementById("allocFutures").style.width = `${Math.max(0, futuresPct * 100)}%`;
      document.getElementById("allocCash").style.width = `${Math.max(0, cashPct * 100)}%`;
      document.getElementById("allocCorePct").textContent = Math.round(corePct * 100);
      document.getElementById("allocSatPct").textContent = Math.round(satPct * 100);
      document.getElementById("allocFuturesPct").textContent = Math.round(futuresPct * 100);
      document.getElementById("allocCashPct").textContent = Math.round(cashPct * 100);
      const futuresDetail = document.getElementById("allocFuturesDetail");
      if (futuresDetail) {
        if (futuresBalance > 0) {
          futuresDetail.style.display = "block";
          const futuresUtilization = futuresBalance > 0 ? Math.min(100, (futuresMarginUsed / futuresBalance) * 100) : 0;
          document.getElementById("allocFuturesBalance").textContent = fmtUsd(futuresBalance);
          document.getElementById("allocFuturesBuyingPower").textContent = fmtUsd(futuresBuyingPower);
          document.getElementById("allocFuturesMarginUsed").textContent = fmtUsd(futuresMarginUsed);
          const utilizationEl = document.getElementById("allocFuturesUtilization");
          if (utilizationEl) {
            utilizationEl.textContent = `${futuresUtilization.toFixed(1)}%`;
            utilizationEl.style.color = futuresUtilization >= 75 ? "#ef4444" : futuresUtilization >= 50 ? "#f59e0b" : "#22c55e";
          }
        } else {
          futuresDetail.style.display = "none";
        }
      }
      document.getElementById("ctxPositionCount").textContent = String(coreCount + satelliteCount);
      document.getElementById("ctxPositionBreakdown").textContent = `${coreCount} core · ${satelliteCount} satellite`;

      updateRegimeBadge(regime);
      renderHoldings(snapshot);
  }

  async function loadDashboard() {
    try {
      // --- PHASE 1: fast data — render portfolio immediately ---
      const resp = await fetchJson("/api/portfolio");
      const snapshot = resp.snapshot || {};
      const summary = resp.summary || {};
      snapshot.futures = { positions: [], balance: {} };
      // Clear any stale Webull data so holdings show loading state
      if (snapshot.brokers) {
        snapshot.brokers.webull = { stocks: [], options: [], balance: {} };
      }

      renderDashboardSnapshot(snapshot, summary);

      // --- PHASE 2: slow data — Webull + futures in parallel ---
      const [futuresPositionsResp, futuresBalanceResp, webullResp] = await Promise.all([
        fetchJson("/api/futures/positions").catch(() => ({ positions: [] })),
        fetchJson("/api/futures/balance").catch(() => ({ balance: {} })),
        fetchJson("/api/webull/positions").catch(() => ({ stocks: [], options: [], balance: {} }))
      ]);
      snapshot.futures = {
        positions: Array.isArray(futuresPositionsResp?.positions) ? futuresPositionsResp.positions : [],
        balance: futuresBalanceResp?.balance || {}
      };
      if (!snapshot.brokers) snapshot.brokers = {};
      snapshot.brokers.webull = {
        stocks: Array.isArray(webullResp?.stocks) ? webullResp.stocks : [],
        options: Array.isArray(webullResp?.options) ? webullResp.options : [],
        balance: webullResp?.balance || {}
      };
      // Keep webull_value_usd from Phase 1 snapshot (portfolio.py net liquidation)
      // Only update if webull balance API returns a reliable value
      const webullBalanceApi = Number(webullResp?.balance?.balance || 0);
      if (webullBalanceApi > 0) snapshot.webull_value_usd = webullBalanceApi;
      // Recalculate total to ensure webull is included
      const _coinbaseVal = Number(snapshot.coinbase_value_usd || 0);
      const _webullVal = Number(snapshot.webull_value_usd || 0);
      if (_coinbaseVal > 0 && _webullVal > 0) {
        snapshot.total_value_usd = _coinbaseVal + _webullVal;
      }
      const webullDayPnl = [...(snapshot.brokers.webull.stocks || []), ...(snapshot.brokers.webull.options || [])]
        .reduce((sum, row) => sum + Number(row.day_pnl || row.day_pnl_usd || 0), 0);
      const coinbaseDayPnl = Number(snapshot.day_pnl_usd || 0) - Number(snapshot.webull_day_pnl_usd || 0);
      snapshot.day_pnl_usd = webullDayPnl + coinbaseDayPnl;
      snapshot.webull_day_pnl_usd = webullDayPnl;

      renderDashboardSnapshot(snapshot, summary);

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
        const last = Number(points[points.length - 1].equity_usd ?? points[points.length - 1].total_value_usd ?? 0);
        // Prefer day_pnl_usd from snapshot if available (more accurate)
        const snapshotDayPnl = window._lastSnapshotDayPnl;
        if (snapshotDayPnl != null && snapshotDayPnl !== 0) {
          const dailyPct = last > 0 ? snapshotDayPnl / (last - snapshotDayPnl) : 0;
          setHeroPnl(snapshotDayPnl, dailyPct);
        } else {
          const prev = Number(points[Math.max(0, points.length - 2)].equity_usd ?? points[Math.max(0, points.length - 2)].total_value_usd ?? last);
          const dailyPnl = last - prev;
          const dailyPct = prev > 0 ? dailyPnl / prev : 0;
          setHeroPnl(dailyPnl, dailyPct);
        }
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
