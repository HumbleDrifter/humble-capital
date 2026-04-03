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
  return {
    score: hasNumericValue(source.score) ? Math.max(0, Math.min(100, Number(source.score))) : null,
    band: String(source.band || "Moderate Risk").trim() || "Moderate Risk",
    notes: cleanTextList(source.notes, 3),
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
    confidence,
    summary: String(source.summary || "").trim(),
    reasons: cleanTextList(source.reasons, 3),
    action,
    simulation: {
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

function renderPerformanceSummary(portfolioData, historyData, rebalanceData, systemData) {
  const card = document.getElementById("performanceSummaryCard");
  const grid = document.getElementById("performanceSummaryGrid");
  if (!card || !grid) return;

  const snapshot = safeObject(portfolioData?.snapshot);
  const summary = safeObject(portfolioData?.summary);
  const analytics = safeObject(historyData?.analytics);
  const riskScore = normalizeRiskScorePayload(historyData?.risk_score);
  const adaptiveSuggestions = normalizeAdaptiveSuggestionsPayload(historyData?.adaptive_suggestions);
  const autoAdaptive = normalizeAutoAdaptivePayload(historyData?.auto_adaptive);

  const totalValue =
    Number(summary.total_value_usd || 0) ||
    Number(snapshot.total_value_usd || 0) ||
    0;

  const regime =
    summary.market_regime ||
    rebalanceData?.summary?.market_regime ||
    systemData?.portfolio_summary?.market_regime ||
    "unknown";

  const pnlPctValue = hasNumericValue(analytics?.pnl_pct) ? Number(analytics.pnl_pct) : null;
  const currentDrawdownPct = hasNumericValue(analytics?.current_drawdown_pct) ? Number(analytics.current_drawdown_pct) : null;
  const maxDrawdownPct = hasNumericValue(analytics?.max_drawdown_pct) ? Number(analytics.max_drawdown_pct) : null;
  const analyticsLimited = Boolean(analytics?.limited_history);
  const analyticsNote = String(analytics?.note || "").trim();
  const scoreValue = riskScore.score;
  const scoreBand = riskScore.band || "Evaluating";
  const scoreNotes = riskScore.notes;
  const suggestionSummary = adaptiveSuggestions.summary;
  const suggestionPriority = adaptiveSuggestions.priority;
  const suggestionItems = adaptiveSuggestions.suggestions.slice(0, 2);
  const suggestionNotes = adaptiveSuggestions.notes;
  const recommendedPreset = autoAdaptive.label || "Balanced";
  const adaptiveConfidence = autoAdaptive.confidence || "low";
  const adaptiveSummary = autoAdaptive.summary;
  const adaptiveReasons = autoAdaptive.reasons.slice(0, 2);
  const adaptiveAction = autoAdaptive.action;
  const adaptiveSimulation = safeObject(autoAdaptive.simulation);
  const simulatedCurrentScore = adaptiveSimulation.current_score;
  const simulatedProjectedScore = adaptiveSimulation.projected_score;
  const simulatedScoreDelta = adaptiveSimulation.score_delta;
  const simulatedProjectedBand = adaptiveSimulation.projected_band || "Projected band pending";
  const simulatedSummary = adaptiveSimulation.summary;
  const simulatedChangedControls = Array.isArray(adaptiveSimulation.changed_controls)
    ? adaptiveSimulation.changed_controls.slice(0, 3)
    : [];
  const simulatedNotes = Array.isArray(adaptiveSimulation.notes)
    ? adaptiveSimulation.notes.slice(0, 2)
    : [];

  const title = analyticsLimited
    ? "Performance view is live, with fuller metrics unlocking as history builds."
    : "Persisted equity history is powering the current performance and drawdown view.";

  setText("performanceSummaryNote", analyticsNote || "Using persisted portfolio history and the current portfolio snapshot.");
  setText("performanceSummaryRegime", String(regime || "unknown").replaceAll("_", " "));

  const regimeBadge = document.getElementById("performanceSummaryRegime");
  if (regimeBadge) {
    regimeBadge.className = `performance-summary-badge${regimeTone(regime) ? ` ${regimeTone(regime)}` : ""}`;
  }

  const riskStrip = document.getElementById("performanceRiskStrip");
  const riskScoreEl = document.getElementById("performanceRiskScore");
  const riskBandEl = document.getElementById("performanceRiskBand");
  const riskNotesEl = document.getElementById("performanceRiskNotes");

  if (riskStrip) {
    const tone = riskBandTone(scoreBand);
    riskStrip.className = `performance-risk-strip${tone ? ` ${tone}` : ""}`;
  }
  if (riskScoreEl) {
    riskScoreEl.textContent = scoreValue == null ? "--" : String(Math.round(scoreValue));
  }
  if (riskBandEl) {
    riskBandEl.textContent = scoreBand;
    riskBandEl.className = `performance-risk-band${riskBandTone(scoreBand) ? ` ${riskBandTone(scoreBand)}` : ""}`;
  }
  if (riskNotesEl) {
    const fallbackNote = analyticsLimited
      ? "Drawdown inputs are limited while persisted equity history is building."
      : "Risk score is blending allocation, reserve, drawdown, and regime inputs.";
    const notes = scoreNotes.length ? scoreNotes : [fallbackNote];
    riskNotesEl.innerHTML = notes
      .map((note) => `<span class="performance-risk-note">${escapeHtml(note)}</span>`)
      .join("");
  }

  const adaptiveCard = document.getElementById("adaptiveSuggestionsCard");
  const adaptivePriorityEl = document.getElementById("adaptiveSuggestionsPriority");
  const adaptiveSummaryEl = document.getElementById("adaptiveSuggestionsSummary");
  const adaptiveListEl = document.getElementById("adaptiveSuggestionsList");
  const adaptiveNotesEl = document.getElementById("adaptiveSuggestionsNotes");

  if (adaptiveCard) {
    const tone = priorityTone(suggestionPriority);
    adaptiveCard.className = `adaptive-suggestions-card${tone ? ` ${tone}` : ""}`;
  }
  if (adaptivePriorityEl) {
    const label = suggestionPriority ? `${suggestionPriority} priority` : "reviewing";
    adaptivePriorityEl.textContent = label;
    adaptivePriorityEl.className = `adaptive-suggestions-priority${priorityTone(suggestionPriority) ? ` ${priorityTone(suggestionPriority)}` : ""}`;
  }
  if (adaptiveSummaryEl) {
    adaptiveSummaryEl.textContent = suggestionSummary || "Advisory guidance is being assembled from the current portfolio and analytics inputs.";
  }
  if (adaptiveListEl) {
    const fallbackItems = [
      {
        title: "Stay measured",
        detail: analyticsLimited
          ? "Persisted history is still building, so advisory guidance is intentionally conservative."
          : "The advisory layer is read-only and highlights only the most defensible next considerations."
      }
    ];
    const items = suggestionItems.length ? suggestionItems : fallbackItems;
    adaptiveListEl.innerHTML = items.map((item) => `
      <div class="adaptive-suggestion-item">
        <div class="adaptive-suggestion-title">${escapeHtml(item.title || "Suggestion")}</div>
        <div class="adaptive-suggestion-detail">${escapeHtml(item.detail || "")}</div>
        ${item.action && item.action.target ? `
          <div class="adaptive-suggestion-actions">
            <a class="btn btn-secondary adaptive-suggestion-link" href="${escapeHtml(configFocusUrl(item.action))}">
              ${escapeHtml(item.action.label || "Adjust In Config")}
            </a>
          </div>
        ` : ""}
      </div>
    `).join("");
  }
  if (adaptiveNotesEl) {
    adaptiveNotesEl.innerHTML = suggestionNotes.map((note) => `
      <span class="adaptive-suggestion-note">${escapeHtml(note)}</span>
    `).join("");
  }

  const autoAdaptiveCard = document.getElementById("autoAdaptiveCard");
  const autoAdaptivePresetEl = document.getElementById("autoAdaptivePreset");
  const autoAdaptiveConfidenceEl = document.getElementById("autoAdaptiveConfidence");
  const autoAdaptiveSummaryEl = document.getElementById("autoAdaptiveSummary");
  const autoAdaptiveReasonsEl = document.getElementById("autoAdaptiveReasons");
  const autoAdaptiveSimulationEl = document.getElementById("autoAdaptiveSimulation");
  const autoAdaptiveSimulationBandEl = document.getElementById("autoAdaptiveSimulationBand");
  const autoAdaptiveProjectionLineEl = document.getElementById("autoAdaptiveProjectionLine");
  const autoAdaptiveSimulationSummaryEl = document.getElementById("autoAdaptiveSimulationSummary");
  const autoAdaptiveChangedControlsEl = document.getElementById("autoAdaptiveChangedControls");
  const autoAdaptiveSimulationNotesEl = document.getElementById("autoAdaptiveSimulationNotes");
  const autoAdaptiveActionsEl = document.getElementById("autoAdaptiveActions");

  if (autoAdaptiveCard) {
    const tone = confidenceTone(adaptiveConfidence);
    autoAdaptiveCard.className = `auto-adaptive-card${tone ? ` ${tone}` : ""}`;
  }
  if (autoAdaptivePresetEl) {
    autoAdaptivePresetEl.textContent = `${recommendedPreset} preset recommended`;
  }
  if (autoAdaptiveConfidenceEl) {
    autoAdaptiveConfidenceEl.textContent = `${adaptiveConfidence} confidence`;
    autoAdaptiveConfidenceEl.className = `auto-adaptive-confidence${confidenceTone(adaptiveConfidence) ? ` ${confidenceTone(adaptiveConfidence)}` : ""}`;
  }
  if (autoAdaptiveSummaryEl) {
    autoAdaptiveSummaryEl.textContent = adaptiveSummary || "Recommendation-only intelligence is evaluating the current portfolio posture.";
  }
  if (autoAdaptiveReasonsEl) {
    const reasons = adaptiveReasons.length ? adaptiveReasons : ["Recommendation confidence is conservative until more portfolio context is available."];
    autoAdaptiveReasonsEl.innerHTML = reasons.map((reason) => `
      <div class="auto-adaptive-reason">${escapeHtml(reason)}</div>
    `).join("");
  }
  if (autoAdaptiveSimulationEl) {
    const tone = riskBandTone(simulatedProjectedBand);
    autoAdaptiveSimulationEl.className = `auto-adaptive-simulation${tone ? ` ${tone}` : ""}`;
  }
  if (autoAdaptiveSimulationBandEl) {
    autoAdaptiveSimulationBandEl.textContent = simulatedProjectedBand;
    autoAdaptiveSimulationBandEl.className = `auto-adaptive-simulation-band${riskBandTone(simulatedProjectedBand) ? ` ${riskBandTone(simulatedProjectedBand)}` : ""}`;
  }
  if (autoAdaptiveProjectionLineEl) {
    const deltaText =
      simulatedScoreDelta == null ? "no score change projected" :
      simulatedScoreDelta === 0 ? "no score change projected" :
      `${simulatedScoreDelta > 0 ? "+" : ""}${Math.round(simulatedScoreDelta)} points projected`;
    const currentText = simulatedCurrentScore == null ? "--" : Math.round(simulatedCurrentScore);
    const projectedText = simulatedProjectedScore == null ? "--" : Math.round(simulatedProjectedScore);
    autoAdaptiveProjectionLineEl.textContent = `Current score ${currentText} → projected score ${projectedText} • ${deltaText}`;
  }
  if (autoAdaptiveSimulationSummaryEl) {
    autoAdaptiveSimulationSummaryEl.textContent =
      simulatedSummary || "This is a projected guardrail simulation only. Nothing is applied automatically.";
  }
  if (autoAdaptiveChangedControlsEl) {
    const controls = simulatedChangedControls.length ? simulatedChangedControls : [];
    autoAdaptiveChangedControlsEl.innerHTML = controls.map((item) => `
      <div class="auto-adaptive-control-chip">
        <span class="auto-adaptive-control-label">${escapeHtml(item.label || "Control")}</span>
        <span class="auto-adaptive-control-values">
          ${escapeHtml(fmtControlValue(item.current_value, item.format))} → ${escapeHtml(fmtControlValue(item.projected_value, item.format))}
        </span>
      </div>
    `).join("");
  }
  if (autoAdaptiveSimulationNotesEl) {
    autoAdaptiveSimulationNotesEl.innerHTML = simulatedNotes.map((note) => `
      <span class="auto-adaptive-note">${escapeHtml(note)}</span>
    `).join("");
  }
  if (autoAdaptiveActionsEl) {
    autoAdaptiveActionsEl.innerHTML = adaptiveAction && adaptiveAction.target ? `
      <a class="btn btn-secondary auto-adaptive-link" href="${escapeHtml(configPresetUrl(adaptiveAction, "Auto-Adaptive Mode"))}">
        ${escapeHtml(adaptiveAction.label || "Stage Recommended Preset")}
      </a>
    ` : "";
  }

  const titleEl = card.querySelector(".performance-summary-title");
  if (titleEl) titleEl.textContent = title;

  grid.innerHTML = `
    <div class="performance-summary-metric primary">
      <div class="performance-summary-label">Portfolio Value</div>
      <div class="performance-summary-value">${fmtUsd(totalValue)}</div>
    </div>
    <div class="performance-summary-metric">
      <div class="performance-summary-label">Range PnL %</div>
      <div class="performance-summary-value ${pnlPctValue == null ? "" : (pnlPctValue >= 0 ? "positive" : "negative")}">${pnlPctValue == null ? "Limited" : fmtSignedPct(pnlPctValue)}</div>
    </div>
    <div class="performance-summary-metric">
      <div class="performance-summary-label">Current Drawdown</div>
      <div class="performance-summary-value ${drawdownTone(currentDrawdownPct)}">${currentDrawdownPct == null ? "Limited" : fmtPct(currentDrawdownPct)}</div>
    </div>
    <div class="performance-summary-metric">
      <div class="performance-summary-label">Max Drawdown</div>
      <div class="performance-summary-value ${drawdownTone(maxDrawdownPct)}">${maxDrawdownPct == null ? "Limited" : fmtPct(maxDrawdownPct)}</div>
    </div>
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

  if (portfolioRes.ok || historyRes.ok || rebalanceRes.ok || systemRes.ok) {
    renderPerformanceSummary(portfolioData, historyData, rebalanceData, systemData);
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
