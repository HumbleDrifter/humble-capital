const API_SECRET = (window.DASHBOARD_CONFIG && window.DASHBOARD_CONFIG.apiSecret) || "";
let AUTO_REFRESH_MS = 120000;
let refreshTimer = null;
let selectedHistoryRange = "30d";

function authUrl(path) {
  if (!API_SECRET) return path;
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}secret=${encodeURIComponent(API_SECRET)}`;
}

function configFocusUrl(action) {
  const target = String(action?.target || "").trim();
  const section = String(action?.section || "").trim();
  const label = String(action?.label || "").trim();
  const params = new URLSearchParams();
  if (target) params.set("focus", target);
  if (section) params.set("section", section);
  if (label) params.set("source_action", label);
  if (API_SECRET) params.set("secret", API_SECRET);
  const query = params.toString();
  return `/configuration${query ? `?${query}` : ""}`;
}

function configPresetUrl(action, label) {
  const preset = String(action?.target || "").trim();
  const source = String(label || "Auto-Adaptive Mode").trim();
  const params = new URLSearchParams();
  if (preset) params.set("preset", preset);
  params.set("preset_source", source);
  params.set("section", "core-controls");
  params.set("focus", "satellite_total_target");
  if (API_SECRET) params.set("secret", API_SECRET);
  const query = params.toString();
  return `/configuration${query ? `?${query}` : ""}`;
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

function fmtSignedPct(v, alreadyPercent = false) {
  const raw = Number(v || 0);
  const value = alreadyPercent ? raw : raw * 100;
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${value.toFixed(2)}%`;
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
  if (rangeName === "all") return "All";
  return "30D";
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

function formatPresetLabel(value) {
  const normalized = String(value || "")
    .replaceAll("_", " ")
    .trim()
    .toLowerCase();

  if (!normalized) return "";
  if (normalized === "balanced") return "Balanced";
  if (normalized === "aggressive") return "Aggressive";
  if (normalized === "conservative") return "Conservative";

  return normalized
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function signedToneClass(value) {
  if (value == null) return "";
  return Number(value) >= 0 ? "positive" : "negative";
}

function deriveRangePnlValue(historyData) {
  const analytics = safeObject(historyData?.analytics);
  if (hasNumericValue(analytics?.pnl_usd)) return Number(analytics.pnl_usd);

  const points = Array.isArray(historyData?.points) ? historyData.points : [];
  if (points.length < 2) return null;

  const key = historyData?.series_type === "realized_pnl" ? "realized_pnl" : "equity_usd";
  return Number(points[points.length - 1]?.[key] || 0) - Number(points[0]?.[key] || 0);
}

function deriveDailyPnlValue(historyData, fallbackHistoryData) {
  const primary = Array.isArray(historyData?.points) ? historyData.points : [];
  const fallback = Array.isArray(fallbackHistoryData?.points) ? fallbackHistoryData.points : [];
  const points = primary.length >= 2 ? primary : fallback;
  if (points.length < 2) return null;

  const source = primary.length >= 2 ? historyData : fallbackHistoryData;
  const key = source?.series_type === "realized_pnl" ? "realized_pnl" : "equity_usd";
  return Number(points[points.length - 1]?.[key] || 0) - Number(points[points.length - 2]?.[key] || 0);
}

function renderSummaryStrip(portfolioData, historyData, history30Data, historyAllData, rebalanceData, systemData) {
  const snapshot = safeObject(portfolioData?.snapshot);
  const summary = safeObject(portfolioData?.summary);
  const totalValue =
    Number(summary.total_value_usd || 0) ||
    Number(snapshot.total_value_usd || 0) ||
    0;
  const usdCash = Number(summary.usd_cash || snapshot.usd_cash || 0);
  const dailyPnl = deriveDailyPnlValue(historyData, history30Data);
  const totalPnl = deriveRangePnlValue(historyAllData);
  const regime =
    summary.market_regime ||
    rebalanceData?.summary?.market_regime ||
    systemData?.portfolio_summary?.market_regime ||
    "unknown";

  [
    ["summaryPortfolioValue", fmtUsd(totalValue), ""],
    ["summaryDailyPnl", dailyPnl == null ? "Limited" : fmtSignedUsd(dailyPnl), signedToneClass(dailyPnl)],
    ["summaryTotalPnl", totalPnl == null ? "Limited" : fmtSignedUsd(totalPnl), signedToneClass(totalPnl)],
    ["summaryCashAvailable", fmtUsd(usdCash), ""],
    ["summaryMarketRegime", normalizeRegimeLabel(regime), regimeTone(regime)]
  ].forEach(([id, text, tone]) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = text;
    el.className = `dashboard-command-value${tone ? ` ${tone}` : ""}`;
  });
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

function buildTrendStat(label, value, tone = "") {
  const toneClass = tone ? ` ${tone}` : "";
  return `
    <div class="trend-stat">
      <div class="trend-stat-label">${label}</div>
      <div class="trend-stat-value${toneClass}">${value}</div>
    </div>
  `;
}

function hasNumericValue(value) {
  return value !== null && value !== undefined && Number.isFinite(Number(value));
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function regimeTone(regime) {
  const normalized = String(regime || "").toLowerCase();
  if (normalized === "bull") return "positive";
  if (normalized === "bear") return "negative";
  if (normalized === "risk_off") return "negative";
  return "";
}

function drawdownTone(value) {
  return value == null ? "" : value > 0 ? "negative" : "positive";
}

function riskBandTone(band) {
  const normalized = String(band || "").toLowerCase();
  if (normalized === "low risk") return "positive";
  if (normalized === "moderate risk") return "";
  if (normalized === "elevated risk") return "warn";
  if (normalized === "high risk") return "negative";
  return "";
}

function riskScoreClass(score) {
  if (!hasNumericValue(score)) return "";
  const value = Number(score);
  if (value <= 24) return "risk-low";
  if (value <= 49) return "risk-moderate";
  if (value <= 74) return "risk-elevated";
  return "risk-high";
}

function priorityTone(priority) {
  const normalized = String(priority || "").toLowerCase();
  if (normalized === "low") return "positive";
  if (normalized === "moderate") return "warn";
  if (normalized === "high") return "negative";
  return "";
}

function confidenceTone(confidence) {
  const normalized = String(confidence || "").toLowerCase();
  if (normalized === "high") return "positive";
  if (normalized === "medium") return "warn";
  if (normalized === "low") return "negative";
  return "";
}

function fmtControlValue(value, format = "text") {
  if (value === null || value === undefined || value === "") return "—";
  if (format === "percent") return fmtPct(Number(value || 0));
  if (format === "usd") return fmtUsd(Number(value || 0));
  if (format === "integer") return Number(value || 0).toLocaleString();
  return String(value);
}

function safeObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function cleanTextList(value, limit = 3) {
  const out = [];
  const items = Array.isArray(value) ? value : [];
  for (const item of items) {
    const text = String(item || "").trim();
    if (text && !out.includes(text)) out.push(text);
    if (out.length >= limit) break;
  }
  return out;
}

function compactSentence(value, fallback = "") {
  const text = String(value || "").trim();
  if (!text) return fallback;
  const normalized = text.replace(/\s+/g, " ").trim();
  const match = normalized.match(/^[^.!?]+[.!?]?/);
  return (match ? match[0] : normalized).trim();
}

function normalizeActionPayload(action, fallbackLabel = "Open Config") {
  const source = safeObject(action);
  const label = String(source.label || fallbackLabel).trim() || fallbackLabel;
  const target = String(source.target || "").trim();
  const section = String(source.section || "").trim();
  return {
    label,
    target,
    section
  };
}

function normalizeRiskScorePayload(value) {
  const source = safeObject(value);
  const notes = cleanTextList(source.notes, 3);
  const singularNote = normalizeRiskScoreNote(source.note || source.summary || "");
  if (singularNote && !notes.includes(singularNote)) notes.unshift(singularNote);

  return {
    score: hasNumericValue(source.score) ? Math.max(0, Math.min(100, Number(source.score))) : null,
    band: String(source.band || "Moderate Risk").trim() || "Moderate Risk",
    notes: notes.slice(0, 3),
    inputs: safeObject(source.inputs)
  };
}

function normalizeAdaptiveSuggestionsPayload(value) {
  const source = safeObject(value);
  const priorityRaw = String(source.priority || "moderate").trim().toLowerCase();
  const priority = ["low", "moderate", "high"].includes(priorityRaw) ? priorityRaw : "moderate";
  const suggestions = [];

  for (const item of Array.isArray(source.suggestions) ? source.suggestions : []) {
    const raw = safeObject(item);
    const title = String(raw.title || "").trim();
    const detail = String(raw.detail || "").trim();
    if (!title && !detail) continue;
    const normalized = {
      title: title || "Suggestion",
      detail: detail || "Review the current portfolio posture in configuration before making manual changes."
    };
    const action = normalizeActionPayload(raw.action, "Adjust In Config");
    if (action.target) normalized.action = action;
    suggestions.push(normalized);
    if (suggestions.length >= 3) break;
  }

  return {
    summary: String(source.summary || "").trim(),
    priority,
    suggestions,
    notes: cleanTextList(source.notes, 2)
  };
}

function normalizeAutoAdaptivePayload(value) {
  const source = safeObject(value);
  const confidenceRaw = String(source.confidence || "low").trim().toLowerCase();
  const confidence = ["low", "medium", "high"].includes(confidenceRaw) ? confidenceRaw : "low";
  const action = normalizeActionPayload(source.action, "Stage Recommended Preset");
  const simulationRaw = safeObject(source.simulation);
  const changedControls = [];

  for (const item of Array.isArray(simulationRaw.changed_controls) ? simulationRaw.changed_controls : []) {
    const raw = safeObject(item);
    const label = String(raw.label || "").trim();
    if (!label) continue;
    changedControls.push({
      key: String(raw.key || "").trim(),
      label,
      current_value: raw.current_value,
      projected_value: raw.projected_value,
      format: String(raw.format || "text").trim() || "text",
      affects_score: Boolean(raw.affects_score)
    });
    if (changedControls.length >= 4) break;
  }

  return {
    label: String(source.label || "Balanced").trim() || "Balanced",
    recommended_preset: String(source.recommended_preset || "").trim(),
    recommended_label: String(source.recommended_label || "").trim(),
    confidence,
    summary: String(source.summary || "").trim(),
    reasons: cleanTextList(source.reasons, 3),
    action,
    simulation: {
      preset: String(simulationRaw.preset || "").trim(),
      label: String(simulationRaw.label || "").trim(),
      current_score: hasNumericValue(simulationRaw.current_score) ? Number(simulationRaw.current_score) : null,
      projected_score: hasNumericValue(simulationRaw.projected_score) ? Number(simulationRaw.projected_score) : null,
      score_delta: hasNumericValue(simulationRaw.score_delta) ? Number(simulationRaw.score_delta) : null,
      projected_band: String(simulationRaw.projected_band || "Projected band pending").trim() || "Projected band pending",
      summary: String(simulationRaw.summary || "").trim(),
      changed_controls: changedControls,
      notes: cleanTextList(simulationRaw.notes, 2)
    }
  };
}

function deriveCurrentPresetLabel(portfolioData, rebalanceData, systemData, autoAdaptive = null) {
  const candidates = [
    portfolioData?.summary?.current_preset_label,
    portfolioData?.summary?.current_preset,
    portfolioData?.summary?.config_preset_label,
    portfolioData?.summary?.config_preset,
    portfolioData?.summary?.preset_label,
    portfolioData?.summary?.preset,
    portfolioData?.snapshot?.current_preset_label,
    portfolioData?.snapshot?.current_preset,
    portfolioData?.snapshot?.config?.preset_label,
    portfolioData?.snapshot?.config?.preset,
    portfolioData?.snapshot?.preset_label,
    portfolioData?.snapshot?.preset,
    rebalanceData?.summary?.current_preset_label,
    rebalanceData?.summary?.current_preset,
    rebalanceData?.summary?.config_preset_label,
    rebalanceData?.summary?.config_preset,
    rebalanceData?.summary?.preset_label,
    rebalanceData?.summary?.preset,
    systemData?.portfolio_summary?.current_preset_label,
    systemData?.portfolio_summary?.current_preset,
    systemData?.portfolio_summary?.config_preset_label,
    systemData?.portfolio_summary?.config_preset,
    systemData?.portfolio_summary?.preset_label,
    systemData?.portfolio_summary?.preset
  ];

  for (const candidate of candidates) {
    const text = formatPresetLabel(candidate);
    if (text) return text;
  }

  const fallbackCandidates = [
    autoAdaptive?.recommended_label,
    autoAdaptive?.label,
    autoAdaptive?.recommended_preset,
    autoAdaptive?.action?.target,
    autoAdaptive?.simulation?.label,
    autoAdaptive?.simulation?.preset
  ];

  for (const candidate of fallbackCandidates) {
    const text = formatPresetLabel(candidate);
    if (text) return text;
  }

  return "Reviewing";
}

function normalizeRiskScoreNote(note) {
  const text = String(note || "").trim();
  if (!text) return "";

  const normalized = text.replace(/\s+/g, " ").trim();
  const lower = normalized.toLowerCase();
  if (lower.includes("live portfolio snapshot") && lower.includes("waiting")) {
    return "Risk score will update as live portfolio context refreshes.";
  }

  return normalized;
}

function buildNeedsAttentionItems(analytics, systemData, adaptiveSuggestions, autoAdaptive) {
  const items = [];
  const analyticsNote = String(analytics?.note || "").trim();
  const adaptiveSummary = String(adaptiveSuggestions?.summary || "").trim();
  const autoAdaptiveSummary = String(autoAdaptive?.summary || "").trim();
  const riskBand = String(systemData?.portfolio_summary?.risk_band || "").trim();
  const cache = safeObject(systemData?.status?.portfolio_cache);
  const snapshotAgeSec = hasNumericValue(cache.snapshot_age_sec)
    ? Number(cache.snapshot_age_sec)
    : (hasNumericValue(cache.age_sec) ? Number(cache.age_sec) : null);
  const lastError = String(cache.last_error || "").trim();
  const degradedText = `${analyticsNote} ${adaptiveSummary} ${autoAdaptiveSummary}`.toLowerCase();
  const addItem = (text, tone = "warn") => {
    const normalized = String(text || "").trim();
    if (!normalized) return;
    if (items.some((item) => item.text === normalized)) return;
    items.push({ text: normalized, tone });
  };

  if (analytics?.limited_history) {
    addItem("Portfolio history is still limited.", "muted");
  }

  if (
    degradedText.includes("fallback") ||
    degradedText.includes("degraded") ||
    degradedText.includes("waiting") ||
    degradedText.includes("unavailable")
  ) {
    addItem("Advisory is using fallback data.", "warn");
  }

  if (snapshotAgeSec != null && snapshotAgeSec > 300) {
    addItem("Snapshot freshness is degraded.", snapshotAgeSec > 900 ? "critical" : "warn");
  }

  if (lastError) {
    addItem("Portfolio cache reported an error.", "critical");
  }

  if (!items.length && String(riskBand || "").toLowerCase() === "high risk") {
    addItem("Risk posture is elevated and worth review.", "warn");
  }

  return items.slice(0, 3);
}

function renderNeedsAttention(items) {
  const panel = document.getElementById("needsAttentionPanel");
  const host = document.getElementById("needsAttentionStrip");
  if (!panel || !host) return;

  if (!Array.isArray(items) || !items.length) {
    panel.hidden = true;
    host.innerHTML = "";
    return;
  }

  panel.hidden = false;
  host.innerHTML = items
    .map((item) => {
      const tone = String(item?.tone || "warn").trim().toLowerCase();
      const text = String(item?.text || "").trim();
      const toneClass =
        tone === "critical" ? " critical" :
        tone === "muted" ? " muted" :
        " warn";
      return `<span class="needs-attention-item${toneClass}">${escapeHtml(text)}</span>`;
    })
    .join("");
}

function renderPerformanceSummary(portfolioData, historyData, history7Data, history30Data, rebalanceData, systemData) {
  const card = document.getElementById("performanceSummaryCard");
  if (!card) return;

  const snapshot = safeObject(portfolioData?.snapshot);
  const summary = safeObject(portfolioData?.summary);
  const analyticsSelected = safeObject(historyData?.analytics);
  const analytics7 = safeObject(history7Data?.analytics);
  const analytics30 = safeObject(history30Data?.analytics);
  const advisoryHistory = history30Data || historyData || {};
  const riskScore = normalizeRiskScorePayload(advisoryHistory?.risk_score);
  const adaptiveSuggestions = normalizeAdaptiveSuggestionsPayload(advisoryHistory?.adaptive_suggestions);
  const autoAdaptive = normalizeAutoAdaptivePayload(advisoryHistory?.auto_adaptive);

  const analyticsLimited = Boolean(analytics30?.limited_history || analyticsSelected?.limited_history);
  const scoreValue = riskScore.score;
  const scoreBand = riskScore.band || "Evaluating";
  const scoreNotes = riskScore.notes;
  const suggestionSummary = adaptiveSuggestions.summary;
  const suggestionItems = adaptiveSuggestions.suggestions.slice(0, 2);
  const recommendedPreset =
    formatPresetLabel(
      autoAdaptive.recommended_label ||
      autoAdaptive.label ||
      autoAdaptive.recommended_preset ||
      autoAdaptive.action?.target ||
      autoAdaptive.simulation?.label ||
      autoAdaptive.simulation?.preset
    ) || "Balanced";
  const adaptiveConfidence = autoAdaptive.confidence || "low";
  const adaptiveSummary = autoAdaptive.summary;
  const adaptiveReasons = autoAdaptive.reasons.slice(0, 2);
  const adaptiveAction = autoAdaptive.action;
  const attentionItems = buildNeedsAttentionItems(analytics30, systemData, adaptiveSuggestions, autoAdaptive);
  const currentPreset = deriveCurrentPresetLabel(portfolioData, rebalanceData, systemData, autoAdaptive);
  const hasHoldingsContext =
    Boolean(summary.assets && Object.keys(summary.assets).length) ||
    Boolean(snapshot.positions && Object.keys(snapshot.positions).length);
  const hasPortfolioMetrics = [
    summary.total_value_usd,
    snapshot.total_value_usd,
    summary.usd_cash,
    snapshot.usd_cash,
    summary.cash_weight,
    snapshot.cash_weight,
    summary.core_weight,
    snapshot.core_weight,
    summary.satellite_weight,
    snapshot.satellite_weight
  ].some(hasNumericValue);
  const hasRegimeContext = Boolean(
    summary.market_regime ||
    snapshot.market_regime ||
    rebalanceData?.summary?.market_regime ||
    systemData?.portfolio_summary?.market_regime
  );
  const hasLivePortfolioContext =
    hasHoldingsContext ||
    hasPortfolioMetrics ||
    hasRegimeContext ||
    Boolean(summary.timestamp || snapshot.timestamp);

  const riskStrip = document.getElementById("performanceRiskStrip");
  const riskScoreEl = document.getElementById("performanceRiskScore");
  const riskBandEl = document.getElementById("performanceRiskBand");
  const riskNotesEl = document.getElementById("performanceRiskNotes");
  const currentPresetEl = document.getElementById("dashboardCurrentPreset");

  if (riskStrip) {
    const tone = riskBandTone(scoreBand);
    riskStrip.className = `performance-risk-strip${tone ? ` ${tone}` : ""}`;
  }
  if (riskScoreEl) {
    riskScoreEl.textContent = scoreValue == null ? "--" : String(Math.round(scoreValue));
    riskScoreEl.className = `performance-risk-score${riskScoreClass(scoreValue) ? ` ${riskScoreClass(scoreValue)}` : ""}`;
  }
  if (riskBandEl) {
    riskBandEl.textContent = scoreBand;
    riskBandEl.className = `performance-risk-band${riskBandTone(scoreBand) ? ` ${riskBandTone(scoreBand)}` : ""}`;
  }
  if (riskNotesEl) {
    const scaleNote = "Risk score blends allocation, reserve, drawdown, and regime inputs. Scale: 0-24 low, 25-49 moderate, 50-74 elevated, 75-100 high.";
    riskNotesEl.innerHTML = `<span class="performance-risk-note">${escapeHtml(scaleNote)}</span>`;
  }
  if (currentPresetEl) {
    currentPresetEl.textContent = currentPreset;
  }

  const dashboardGuidanceCard = document.getElementById("dashboardGuidanceCard");
  const dashboardGuidanceTitleEl = document.getElementById("dashboardGuidanceTitle");
  const dashboardGuidanceConfidenceEl = document.getElementById("dashboardGuidanceConfidence");
  const dashboardGuidanceSummaryEl = document.getElementById("dashboardGuidanceSummary");
  const dashboardGuidanceActionsEl = document.getElementById("dashboardGuidanceActions");
  const guidanceAction =
    adaptiveAction && adaptiveAction.target
      ? {
          label: "Open Configuration",
          target: adaptiveAction.target,
          section: adaptiveAction.section || "core-controls"
        }
      : (suggestionItems.find((item) => item.action && item.action.target)?.action || {
          label: "Open Configuration",
          target: "satellite_total_target",
          section: "core-controls"
        });
  const compactGuidanceSummary = compactSentence(
    adaptiveSummary ||
    suggestionSummary ||
    adaptiveReasons[0] ||
    "Open Configuration for detailed adaptive guidance and manual guardrail review.",
    "Open Configuration for detailed adaptive guidance and manual guardrail review."
  );

  if (dashboardGuidanceCard) {
    const tone = confidenceTone(adaptiveConfidence);
    dashboardGuidanceCard.className = `dashboard-guidance-card${tone ? ` ${tone}` : ""}`;
  }
  if (dashboardGuidanceTitleEl) {
    dashboardGuidanceTitleEl.textContent = `${recommendedPreset} preset recommended`;
  }
  if (dashboardGuidanceConfidenceEl) {
    dashboardGuidanceConfidenceEl.textContent = `${adaptiveConfidence} confidence`;
    dashboardGuidanceConfidenceEl.className = `dashboard-guidance-badge${confidenceTone(adaptiveConfidence) ? ` ${confidenceTone(adaptiveConfidence)}` : ""}`;
  }
  if (dashboardGuidanceSummaryEl) {
    dashboardGuidanceSummaryEl.textContent = compactGuidanceSummary;
  }
  if (dashboardGuidanceActionsEl) {
    const href =
      adaptiveAction && adaptiveAction.target
        ? configPresetUrl(adaptiveAction, "Auto-Adaptive Mode")
        : configFocusUrl(guidanceAction);
    dashboardGuidanceActionsEl.innerHTML = `
      <a class="btn btn-secondary dashboard-guidance-link" href="${escapeHtml(href)}">
        Open Configuration
      </a>
    `;
  }

  renderNeedsAttention(attentionItems);
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
  const analytics = data?.analytics || {};
  const rangeText = rangeLabel(data?.range || selectedHistoryRange);
  const seriesType = data?.series_type === "realized_pnl" ? "realized_pnl" : data?.series_type === "portfolio_value" ? "portfolio_value" : "empty";
  const valueKey = seriesType === "realized_pnl" ? "realized_pnl" : "equity_usd";
  const valueFormatter = seriesType === "realized_pnl" ? fmtSignedUsd : fmtUsd;
  const currentLabel = seriesType === "realized_pnl" ? "Current Realized PnL" : "Current Value";
  const deltaLabel = seriesType === "realized_pnl" ? "Range PnL Change" : "Range Change";
  const sourceLabel = seriesType === "realized_pnl" ? "Realized PnL" : "Portfolio Value";
  const analyticsLimited = Boolean(analytics?.limited_history);
  const analyticsNote = String(analytics?.note || "").trim();

  if (!points.length || seriesType === "empty") {
    if (meta) meta.textContent = `${rangeText} view • no portfolio history available yet`;
    host.innerHTML = `<div class="trend-chart-shell"><div class="trend-chart-empty">No portfolio history data found for this range.</div></div>`;
    return;
  }

  const firstValue = Number(points[0]?.[valueKey] || 0);
  const lastValue = Number(points[points.length - 1]?.[valueKey] || 0);
  const deltaValue = hasNumericValue(analytics?.pnl_usd) ? Number(analytics.pnl_usd) : (lastValue - firstValue);
  const pnlPctValue = hasNumericValue(analytics?.pnl_pct) ? Number(analytics.pnl_pct) : null;
  const currentDrawdownPct = hasNumericValue(analytics?.current_drawdown_pct) ? Number(analytics.current_drawdown_pct) : null;
  const maxDrawdownPct = hasNumericValue(analytics?.max_drawdown_pct) ? Number(analytics.max_drawdown_pct) : null;
  const drawdownTone = (value) => (value == null ? "" : value > 0 ? "negative" : "positive");

  if (meta) {
    const metaSuffix = analyticsLimited && analyticsNote ? ` • ${analyticsNote}` : "";
    meta.textContent = `${rangeText} view • ${points.length} point(s) • ${sourceLabel}${metaSuffix}`;
  }

  host.innerHTML = `
    <div class="trend-chart-shell">
      <div class="trend-chart-summary">
        ${buildTrendStat(currentLabel, valueFormatter(lastValue))}
        ${buildTrendStat(deltaLabel, fmtSignedUsd(deltaValue), deltaValue >= 0 ? "positive" : "negative")}
        ${buildTrendStat("Range", rangeText)}
        ${buildTrendStat("PnL %", pnlPctValue == null ? "Limited" : fmtSignedPct(pnlPctValue), pnlPctValue == null ? "" : (pnlPctValue >= 0 ? "positive" : "negative"))}
        ${buildTrendStat("Current Drawdown", currentDrawdownPct == null ? "Limited" : fmtPct(currentDrawdownPct), drawdownTone(currentDrawdownPct))}
        ${buildTrendStat("Max Drawdown", maxDrawdownPct == null ? "Limited" : fmtPct(maxDrawdownPct), drawdownTone(maxDrawdownPct))}
      </div>
      ${buildTrendSvg(points, valueKey)}
      ${analyticsNote ? `<div class="trend-chart-note">${escapeHtml(analyticsNote)}</div>` : ""}
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
  setText("allocationCashValue", fmtPct(cashWeight));
  setText("allocationCoreValue", fmtPct(coreWeight));
  setText("allocationSatValue", fmtPct(satWeight));

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

  const allocationBreakdown = document.getElementById("allocationBreakdown");
  if (allocationBreakdown) {
    const topRows = rows.filter((row) => Number(row.weight_total || 0) > 0).slice(0, 6);
    allocationBreakdown.innerHTML = topRows.length
      ? topRows.map((row) => {
          const weightPct = Math.max(0, Number(row.weight_total || 0) * 100);
          return `
            <div class="dashboard-allocation-row">
              <div class="dashboard-allocation-row-head">
                <span class="dashboard-allocation-row-symbol">${escapeHtml(row.product_id || "—")}</span>
                <span class="dashboard-allocation-row-weight">${fmtPct(row.weight_total)}</span>
              </div>
              <div class="dashboard-allocation-row-bar">
                <span style="width:${Math.min(100, weightPct)}%"></span>
              </div>
              <div class="dashboard-allocation-row-meta">
                <span>${escapeHtml(displayClass(row.class || ""))}</span>
                <span>${fmtUsd(row.value_total_usd || 0)}</span>
              </div>
            </div>
          `;
        }).join("")
      : `<div class="muted">No allocation rows available from the current portfolio snapshot.</div>`;
  }

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

function opportunityPreviewTone(score) {
  const numeric = Number(score || 0);
  if (numeric >= 85) return "high";
  if (numeric >= 65) return "strong";
  if (numeric >= 45) return "building";
  return "early";
}

function opportunityPreviewStatus(row) {
  if (row.blocked || row.enabled === false) return "Paused";
  if (row.held || row.core) return "Live";
  if (row.allowed || row.active_buy_universe) return "Ready";
  return "Watching";
}

function renderOpportunitiesPreview(opportunityData, systemData) {
  const host = document.getElementById("dashboardOpportunitiesPreview");
  const meta = document.getElementById("dashboardOpportunitiesMeta");
  if (!host) return;

  const candidates = Array.isArray(opportunityData?.candidates) ? opportunityData.candidates.slice() : [];
  const activeCount = candidates.filter((row) => row.held || row.allowed || row.active_buy_universe || row.core).length;
  const watchingCount = candidates.filter((row) => !(row.blocked || row.enabled === false) && !(row.held || row.allowed || row.active_buy_universe || row.core)).length;
  const previewRows = candidates
    .filter((row) => !(row.blocked || row.enabled === false))
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
    .slice(0, 6);

  const scannerKnown = typeof systemData?.admin_state?.meme_rotation_enabled === "boolean";
  const scannerText = !scannerKnown
    ? "scanner state unavailable"
    : systemData.admin_state.meme_rotation_enabled
      ? "scanner live"
      : "scanner paused";

  if (meta) {
    meta.textContent = `${activeCount} active • ${watchingCount} watching • ${scannerText}`;
  }

  host.innerHTML = previewRows.length
    ? previewRows.map((row) => `
      <article class="dashboard-opportunity-card ${opportunityPreviewTone(row.score)}">
        <div class="dashboard-opportunity-card-head">
          <div>
            <div class="dashboard-opportunity-symbol">${escapeHtml(row.product_id || row.symbol || "—")}</div>
            <div class="dashboard-opportunity-subline">
              <span class="badge">${escapeHtml(opportunityPreviewStatus(row))}</span>
              <span class="tiny">${escapeHtml(row.source || row.strategy || "Scanner")}</span>
            </div>
          </div>
          <div class="dashboard-opportunity-score">${Number(row.score || 0).toFixed(1)}</div>
        </div>
        <div class="dashboard-opportunity-metrics">
          <div class="dashboard-opportunity-metric">
            <span>24H Move</span>
            <strong>${fmtSignedPct(row.change_24h || 0, true)}</strong>
          </div>
          <div class="dashboard-opportunity-metric">
            <span>Weight</span>
            <strong>${fmtPct(row.portfolio_weight || 0)}</strong>
          </div>
          <div class="dashboard-opportunity-metric">
            <span>Held Value</span>
            <strong>${fmtUsd(row.held_value_usd || 0)}</strong>
          </div>
        </div>
      </article>
    `).join("")
    : `<div class="dashboard-opportunities-empty muted">No current opportunity candidates are available for preview.</div>`;
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
    meta.innerHTML = `${cacheBadge(data?._cache)} <span class="tiny">regime: ${escapeHtml(normalizeRegimeLabel(summary.market_regime || "unknown"))}</span>`;
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
      <span class="badge">${escapeHtml(normalizeRegimeLabel(summary.market_regime || "unknown"))}</span>
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

function renderExecutionActivity(systemData) {
  const host = document.getElementById("executionActivity");
  if (!host) return;

  const rows = (Array.isArray(systemData?.recent_trades) ? systemData.recent_trades.slice() : [])
    .filter((row) => row && (row.product_id || row.side || row.status || row.created_at))
    .sort((a, b) => Number(b?.created_at || 0) - Number(a?.created_at || 0))
    .slice(0, 4);

  if (!rows.length) {
    host.innerHTML = `<div class="execution-activity-fallback">No recent execution activity available.</div>`;
    return;
  }

  host.innerHTML = rows.map((row) => `
    <article class="execution-activity-entry">
      <div class="execution-activity-entry-head">
        <div class="execution-activity-entry-title">${escapeHtml(row.product_id || "—")}</div>
        <span class="badge ${String(row.side || "").toLowerCase() === "buy" ? "good" : "warn"}">${escapeHtml(String(row.side || "—").toUpperCase())}</span>
      </div>
      <div class="execution-activity-entry-meta">
        <span>${escapeHtml(row.status || "status pending")}</span>
        <span>${escapeHtml(formatUnixTime(row.created_at))}</span>
      </div>
      <div class="execution-activity-entry-meta">
        <span>Price ${fmtUsd(row.price || 0)}</span>
        <span>Size ${fmtQty(row.base_size || 0)}</span>
      </div>
    </article>
  `).join("");
}

async function refreshAll(showBadgeMessage = false) {
  const selectedHistoryPath = `/api/portfolio/history?range=${encodeURIComponent(selectedHistoryRange)}`;
  const history7Path = "/api/portfolio/history?range=7d";
  const history30Path = "/api/portfolio/history?range=30d";
  const historyAllPath = "/api/portfolio/history?range=all";
  const historyPaths = Array.from(new Set([selectedHistoryPath, history7Path, history30Path, historyAllPath]));

  const [systemRes, portfolioRes, rebalanceRes, opportunitiesRes, ...historyResults] = await Promise.all([
    fetchJsonSafe("/api/system_snapshot?recent_count=8"),
    fetchJsonSafe("/api/portfolio"),
    fetchJsonSafe("/api/rebalance/preview"),
    fetchJsonSafe("/api/meme_rotation"),
    ...historyPaths.map((path) => fetchJsonSafe(path))
  ]);

  const historyByPath = Object.fromEntries(historyResults.map((result) => [result.path, result]));
  const historyRes = historyByPath[selectedHistoryPath] || { ok: false, data: null };
  const history7Res = historyByPath[history7Path] || historyRes;
  const history30Res = historyByPath[history30Path] || historyRes;
  const historyAllRes = historyByPath[historyAllPath] || historyRes;

  const failures = [systemRes, portfolioRes, rebalanceRes, opportunitiesRes, ...historyResults].filter((x) => !x.ok);

  const systemData = systemRes.data || {};
  const portfolioData = portfolioRes.data || {};
  const rebalanceData = rebalanceRes.data || {};
  const opportunitiesData = opportunitiesRes.data || {};
  const historyData = historyRes.data || {};
  const history7Data = history7Res.data || {};
  const history30Data = history30Res.data || {};
  const historyAllData = historyAllRes.data || {};

  if (portfolioRes.ok) {
    renderPortfolio(portfolioData);
  }

  renderSummaryStrip(portfolioData, historyData, history30Data, historyAllData, rebalanceData, systemData);

  renderOpportunitiesPreview(opportunitiesData, systemData);

  if (rebalanceRes.ok) {
    renderRebalance(rebalanceData);
  }

  if (historyRes.ok) {
    renderAccountValueHistory(historyData);
  } else {
    renderAccountValueHistory({ points: [], series_type: "empty" });
  }

  if (portfolioRes.ok || historyRes.ok || rebalanceRes.ok || systemRes.ok) {
    renderPerformanceSummary(portfolioData, historyData, history7Data, history30Data, rebalanceData, systemData);
  }

  if (systemRes.ok) {
    renderExecutionActivity(systemData);
  } else {
    renderExecutionActivity({});
  }

  if (!systemRes.ok && !portfolioRes.ok && !rebalanceRes.ok && !historyRes.ok) return;
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

setTrendRangeButtons(selectedHistoryRange);
refreshAll(false);
startAutoRefresh();
