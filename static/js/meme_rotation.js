const API_SECRET = (window.MEME_ROTATION_CONFIG && window.MEME_ROTATION_CONFIG.apiSecret) || "";
let CURRENT_PENDING_SATELLITE_PROPOSAL = null;
const HEATMAP_THEMES = {
  ocean:   { high: "#60a5fa", mid: "#38bdf8", low: "#1e3a5f", text_high: "#60a5fa", text_mid: "#94a3b8", text_low: "#fb7185" },
  thermal: { high: "#22c55e", mid: "#fbbf24", low: "#fb7185", text_high: "#22c55e", text_mid: "#fbbf24", text_low: "#fb7185" },
  mono:    { high: "#e2e8f0", mid: "#94a3b8", low: "#475569", text_high: "#e2e8f0", text_mid: "#94a3b8", text_low: "#64748b" },
  neon:    { high: "#c084fc", mid: "#f472b6", low: "#6b21a8", text_high: "#c084fc", text_mid: "#f472b6", text_low: "#fb7185" },
  sunset:  { high: "#f59e0b", mid: "#f97316", low: "#dc2626", text_high: "#fbbf24", text_mid: "#f97316", text_low: "#fb7185" },
};

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

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function fmtUsd(v) {
  return Number(v || 0).toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2
  });
}

function fmtPct(v, alreadyPercent = true) {
  const raw = Number(v || 0);
  const value = alreadyPercent ? raw : raw * 100;
  return `${value.toFixed(2)}%`;
}

function fmtNumber(v) {
  return Number(v || 0).toLocaleString(undefined, {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1
  });
}

function fmtPctValue(value, digits = 1) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return "—";
  return `${numeric.toFixed(digits)}%`;
}

function formatUnixTime(ts) {
  const numeric = Number(ts || 0);
  if (!numeric) return "—";
  return new Date(numeric * 1000).toLocaleString();
}

function titleCase(value) {
  return String(value || "")
    .split(/[\s_-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function safeObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function numericOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function getHeatmapTheme() {
  return localStorage.getItem("hc_heatmap_theme") || "ocean";
}

function hexToRgb(hex) {
  const normalized = String(hex || "").trim().replace("#", "");
  if (normalized.length !== 6) return { r: 96, g: 165, b: 250 };
  const int = Number.parseInt(normalized, 16);
  if (!Number.isFinite(int)) return { r: 96, g: 165, b: 250 };
  return {
    r: (int >> 16) & 255,
    g: (int >> 8) & 255,
    b: int & 255
  };
}

function rgbaFromHex(hex, alpha) {
  const { r, g, b } = hexToRgb(hex);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function scoreColor(score) {
  const theme = HEATMAP_THEMES[getHeatmapTheme()] || HEATMAP_THEMES.ocean;
  if (score >= 70) return theme.high;
  if (score >= 40) return theme.mid;
  return theme.low;
}

function scoreTextColor(score) {
  const theme = HEATMAP_THEMES[getHeatmapTheme()] || HEATMAP_THEMES.ocean;
  if (score >= 70) return theme.text_high;
  if (score >= 40) return theme.text_mid;
  return theme.text_low;
}

function opportunityCardThemeStyle(score) {
  const border = scoreColor(score);
  const rgb = hexToRgb(border);
  const deepBg = score >= 70
    ? "rgba(14, 29, 45, 0.99)"
    : score >= 40
      ? "rgba(18, 24, 34, 0.99)"
      : "rgba(34, 18, 25, 0.99)";
  const baseBg = score >= 70
    ? "rgba(9, 17, 25, 0.99)"
    : "rgba(10, 16, 25, 0.99)";
  const shadow = score >= 70 ? rgbaFromHex(border, 0.16) : score >= 40 ? rgbaFromHex(border, 0.08) : rgbaFromHex(border, 0.12);
  return [
    `border-color:${border}`,
    `background:radial-gradient(circle at top left, rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, ${score >= 70 ? 0.30 : score >= 40 ? 0.12 : 0.12}), transparent 42%), linear-gradient(145deg, ${deepBg}, ${baseBg})`,
    `box-shadow:inset 0 1px 0 rgba(255,255,255,0.04), 0 14px 30px ${shadow}, 0 0 0 1px rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, 0.05)`
  ].join("; ");
}

function resolve24hMove(row) {
  const candidates = [
    row.change_24h,
    row.price_change_24h,
    row.price_change_24h_pct,
    row.move_24h
  ];

  for (const value of candidates) {
    const numeric = numericOrNull(value);
    if (numeric != null) return numeric;
  }

  return null;
}

function formatPercentOrNA(value) {
  const numeric = numericOrNull(value);
  if (numeric == null) return "N/A";
  return `${numeric >= 0 ? "+" : ""}${numeric.toFixed(2)}%`;
}

function resolveUniverseCount(data) {
  const explicitCount = numericOrNull(data.candidate_count);
  if (explicitCount != null) return explicitCount;

  if (Array.isArray(data.candidates)) {
    return data.candidates.length;
  }

  return null;
}

function resolveOpportunityScore(row) {
  const canonical = numericOrNull(row?.display_score);
  if (canonical != null) return canonical;
  const candidates = [
    row?.net_score,
    row?.gross_score,
    row?.score
  ];

  for (const value of candidates) {
    const numeric = numericOrNull(value);
    if (numeric != null) return numeric;
  }

  return 0;
}

function decisionLabel(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "recommend_for_enable") return "Enable Ready";
  if (normalized === "recommend_replacement") return "Replacement Ready";
  if (normalized === "almost_ready") return "Almost Ready";
  if (normalized === "blocked") return "Blocked";
  if (normalized === "ignore") return "Monitor";
  return "";
}

function decisionBadgeTone(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "recommend_for_enable" || normalized === "recommend_replacement") return "good";
  if (normalized === "almost_ready") return "warn";
  if (normalized === "blocked") return "bad";
  return "accent";
}

function decisionSummaryText(summary) {
  const payload = safeObject(summary);
  const parts = [];
  const replacements = Number(payload.recommend_replacement || 0);
  const enableReady = Number(payload.recommend_for_enable || 0);
  const almostReady = Number(payload.almost_ready || 0);

  if (replacements > 0) parts.push(`${replacements} replacement${replacements === 1 ? "" : "s"}`);
  if (enableReady > 0) parts.push(`${enableReady} enable-ready`);
  if (almostReady > 0) parts.push(`${almostReady} almost ready`);

  return parts.length ? parts.join(" • ") : "";
}

function resolvePendingSatelliteProposal(items) {
  const proposals = Array.isArray(items) ? items : [];
  for (const item of proposals) {
    const proposal = safeObject(item);
    const nestedProposal = safeObject(proposal.proposal);
    const proposalType = String(nestedProposal.proposal_type || proposal.proposal_type || "").trim().toLowerCase();
    const status = String(proposal.status || "").trim().toLowerCase();
    const id = String(proposal.id || "").trim();
    if (proposalType === "satellite_enable_recommendation" && status === "pending" && id) {
      return {
        id,
        status,
        proposal_type: proposalType,
        summary_text: String(proposal.summary_text || nestedProposal.summary || "").trim()
      };
    }
  }
  return null;
}

function buildShadowPortfolioContext(row, opportunitiesData, systemData, configData, reviewReadyRows) {
  const config = safeObject(configData);
  const portfolioSummary = safeObject(systemData?.portfolio_summary);
  const held = Boolean(row?.held);
  const activeUniverseCount = Array.isArray(opportunitiesData?.active_satellite_buy_universe)
    ? opportunitiesData.active_satellite_buy_universe.length
    : 0;
  const maxActive = numericOrNull(config.max_active_satellites);
  const maxNewPerCycle = numericOrNull(config.max_new_satellites_per_cycle);
  const satelliteTarget = numericOrNull(config.satellite_total_target);
  const satelliteMax = numericOrNull(config.satellite_total_max);
  const satelliteWeight = numericOrNull(
    portfolioSummary.satellite_weight
    ?? portfolioSummary.satellite_alloc
    ?? portfolioSummary.satellite_weight_total
  );
  const newReviewReadyCount = Array.isArray(reviewReadyRows)
    ? reviewReadyRows.filter((item) => !item?.held).length
    : 0;

  let heldContext = row?.heldContext || row?.held_context || (held ? "Already held" : "New candidate");
  let slotPressure = row?.slotPressure || row?.slot_pressure || (held ? "Held slot preserved" : "Room available");
  if (!held && maxActive != null) {
    if (activeUniverseCount >= maxActive) {
      slotPressure = "Slots full";
    } else if (activeUniverseCount >= Math.max(maxActive - 1, 0)) {
      slotPressure = "Limited room";
    }
  }

  let portfolioPressure = row?.portfolioPressure || row?.portfolio_pressure || "Normal";
  if (satelliteWeight != null && satelliteMax != null && satelliteWeight >= satelliteMax) {
    portfolioPressure = "High";
  } else if (satelliteWeight != null && satelliteTarget != null && satelliteWeight >= satelliteTarget) {
    portfolioPressure = "Moderate";
  }

  const existingNote = String(row?.portfolioContextNote || row?.portfolio_context_note || "").trim();
  const noteParts = [];
  if (existingNote) {
    noteParts.push(existingNote);
  } else {
    noteParts.push(held ? "Already held in the portfolio." : "Adds new satellite exposure.");
    if (row?.active_buy_universe === false) {
      noteParts.push("Not live in the active universe yet.");
    }
    if (!held && maxNewPerCycle != null) {
      if (newReviewReadyCount > maxNewPerCycle) {
        noteParts.push(`Cycle entry pressure is elevated (${newReviewReadyCount}/${Math.round(maxNewPerCycle)} review-ready names).`);
      } else {
        noteParts.push("Cycle entry room is available.");
      }
    }
  }

  return {
    heldContext,
    slotPressure,
    portfolioPressure,
    portfolioContextNote: noteParts.join(" ")
  };
}

function renderDecisionLines(row, options = {}) {
  const {
    showQualifies = false,
    showPrimaryMiss = false,
    showPortfolio = true
  } = options;
  const lines = [];
  const decisionText = String(row?.decision_reason || "").trim();
  const decision = String(row?.decision || "").trim();
  const decisionConfidence = String(row?.decision_confidence || "").trim();
  const blockers = Array.isArray(row?.decision_blockers) ? row.decision_blockers.filter(Boolean) : [];
  const replacementTarget = String(row?.replacement_target || "").trim();
  const replacementScoreDelta = numericOrNull(row?.replacement_score_delta);
  const qualifiesText = String(row?.shadow_eligibility_reason || "").trim();
  const primaryMiss = String(row?.primary_fail_reason || row?.shadow_block_reason || "").trim();
  const failText = String(row?.fail_explanation || row?.shadow_eligibility_reason || "").trim();
  const notLiveText = String(row?.shadow_block_reason || "").trim();
  const portfolioText = String(row?.portfolioContextNote || row?.portfolio_context_note || "").trim();

  if (decisionText) {
    lines.push(`<div class="shadow-eligible-reason"><strong>Decision:</strong> ${escapeHtml(decisionText)}</div>`);
  } else if (decision) {
    lines.push(`<div class="shadow-eligible-reason"><strong>Decision:</strong> ${escapeHtml(decisionLabel(decision) || titleCase(decision))}</div>`);
  }

  if (decisionConfidence) {
    lines.push(`<div class="shadow-eligible-reason"><strong>Decision confidence:</strong> ${escapeHtml(titleCase(decisionConfidence))}</div>`);
  }

  if (showQualifies && qualifiesText) {
    lines.push(`<div class="shadow-eligible-reason"><strong>Qualifies:</strong> ${escapeHtml(qualifiesText)}</div>`);
  }

  if (showPrimaryMiss && primaryMiss) {
    lines.push(`<div class="shadow-eligible-reason"><strong>Primary miss:</strong> ${escapeHtml(titleCase(primaryMiss))}</div>`);
  }

  if (showPrimaryMiss && failText) {
    lines.push(`<div class="shadow-eligible-reason"><strong>Why it missed:</strong> ${escapeHtml(failText)}</div>`);
  }

  if (!showPrimaryMiss && notLiveText) {
    lines.push(`<div class="shadow-eligible-reason"><strong>Not live yet:</strong> ${escapeHtml(titleCase(notLiveText))}</div>`);
  }

  if (blockers.length) {
    lines.push(`<div class="shadow-eligible-reason"><strong>${showPrimaryMiss ? "Blockers" : "Guardrails"}:</strong> ${escapeHtml(blockers.map((item) => titleCase(item)).join(" • "))}</div>`);
  }

  if (replacementTarget) {
    lines.push(
      `<div class="shadow-eligible-reason"><strong>Replacement target:</strong> ${escapeHtml(replacementTarget)}${replacementScoreDelta != null ? ` (${escapeHtml(`+${replacementScoreDelta.toFixed(1)} intelligence score`)})` : ""}</div>`
    );
  }

  if (showPortfolio && portfolioText) {
    lines.push(`<div class="shadow-eligible-reason"><strong>Portfolio:</strong> ${escapeHtml(portfolioText)}</div>`);
  }

  return lines.join("");
}

function sortCandidates(rows, sortKey) {
  const items = [...rows];

  if (sortKey === "weight") {
    items.sort((a, b) => Number(b.portfolio_weight || b.weight || 0) - Number(a.portfolio_weight || a.weight || 0));
  } else if (sortKey === "held_value") {
    items.sort((a, b) => Number(b.held_value_usd || 0) - Number(a.held_value_usd || 0));
  } else if (sortKey === "change_24h") {
    items.sort((a, b) => (resolve24hMove(b) ?? Number.NEGATIVE_INFINITY) - (resolve24hMove(a) ?? Number.NEGATIVE_INFINITY));
  } else {
    items.sort((a, b) => (
      resolveOpportunityScore(b) - resolveOpportunityScore(a)
      || Number(b.gross_score || 0) - Number(a.gross_score || 0)
      || Number(b.score || 0) - Number(a.score || 0)
    ));
  }

  return items;
}

function opportunityTone(score) {
  const s = Number(score || 0);
  if (s >= 70) return "high";
  if (s >= 40) return "building";
  return "early";
}

function scoreLabel(score) {
  const s = Number(score || 0);
  if (s >= 70) return "High";
  if (s >= 40) return "Medium";
  return "Low";
}

function normalizeRegime(value) {
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

function regimeTone(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "bull") return "bull";
  if (normalized === "bear") return "bear";
  if (normalized === "neutral") return "neutral";
  if (normalized === "risk_off") return "risk-off";
  return "unknown";
}

function assetTypeForCandidate(row) {
  const cls = String(row.class || "").trim().toLowerCase();
  const productId = String(row.product_id || "").trim().toUpperCase();

  if (cls.includes("option")) return "Options";
  if (cls.includes("future")) return "Futures";
  if (cls.includes("stock") || cls.includes("equity")) return "Stock";
  if (cls.includes("crypto")) return "Crypto";
  if (productId.includes("-")) return "Crypto";
  return "Asset";
}

function signalLabel(row) {
  return String(row.source || row.strategy || "Scanner").trim() || "Scanner";
}

function statusText(row) {
  const canonical = String(row?.display_status || "").trim();
  if (canonical) return canonical;
  if (row.blocked || row.enabled === false) return "Paused";
  if (row.core) return "Core (Portfolio)";
  if (row.held) return "Live";
  if (row.allowed) return "Allowed";
  if (row.active_buy_universe) return "Ready";
  return "Watching";
}

function normalizeStateBadges(row) {
  const badges = [statusText(row)];
  if (row.core) badges.push("Core");
  if (row.active_buy_universe && !row.held && !row.allowed && !row.core) badges.push("Ready");

  const seen = new Set();
  return badges.filter((label) => {
    const normalized = String(label || "").trim().toLowerCase();
    if (!normalized || seen.has(normalized)) return false;
    seen.add(normalized);
    return true;
  });
}

function groupForCandidate(row) {
  const canonical = String(row?.display_group || "").trim().toLowerCase();
  if (canonical) return canonical;
  if (row.blocked || row.enabled === false) return "paused";
  if (row.held || row.allowed || row.active_buy_universe || row.core) return "active";
  return "watching";
}

function groupTitle(groupKey) {
  if (groupKey === "active") return "Active";
  if (groupKey === "paused") return "Paused";
  return "Watching";
}

function groupDescription(groupKey) {
  if (groupKey === "active") return "Live, allowed, or ready-to-enter opportunities worth active operator attention.";
  if (groupKey === "paused") return "Opportunities intentionally blocked or otherwise taken out of active rotation.";
  return "Candidates being monitored without an active portfolio or allowlist commitment.";
}

function emptyStateText(groupKey) {
  if (groupKey === "active") return "No currently active opportunities are standing out right now.";
  if (groupKey === "paused") return "No opportunities are currently paused or blocked.";
  return "No candidates are being monitored right now.";
}

  function actionButtons(row) {
    const productId = String(row.product_id || "");
    const safeId = escapeHtml(productId);
    const mode = row.blocked ? "disable" : row.allowed ? "enable" : "auto";

    return `
      <div class="opportunity-actions opp-actions">
        <button class="btn ${mode === "enable" ? "btn-primary" : "btn-secondary"} opportunity-action-btn opp-action-btn" type="button" onclick="setOpportunityMode('${safeId}','enable')">Enable</button>
        <button class="btn ${mode === "auto" ? "btn-primary" : "btn-secondary"} opportunity-action-btn opp-action-btn" type="button" onclick="setOpportunityMode('${safeId}','auto')">Auto</button>
        <button class="btn ${mode === "disable" ? "btn-primary" : "btn-secondary"} opportunity-action-btn opp-action-btn" type="button" onclick="setOpportunityMode('${safeId}','disable')">Disable</button>
      </div>
    `;
  }

function flagPills(row) {
  return normalizeStateBadges(row)
    .map((label) => `<span class="pill">${escapeHtml(label)}</span>`)
    .join("");
}

function renderSummary(data, groups) {
  const host = document.getElementById("opportunitySummary");
  if (!host) return;
  const regimeText = normalizeRegime(data.market_regime || "unknown");
    const regimeClass = regimeTone(data.market_regime || "unknown");

    host.innerHTML = `
      <div class="hc-context-card opp-summary-card opportunity-summary-card">
        <div class="hc-context-label opportunity-summary-label">Active</div>
        <div class="hc-context-value opportunity-summary-value">${groups.active.length}</div>
      </div>
      <div class="hc-context-card opp-summary-card opportunity-summary-card">
        <div class="hc-context-label opportunity-summary-label">Watching</div>
        <div class="hc-context-value opportunity-summary-value">${groups.watching.length}</div>
      </div>
      <div class="hc-context-card opp-summary-card opportunity-summary-card">
        <div class="hc-context-label opportunity-summary-label">Paused</div>
        <div class="hc-context-value opportunity-summary-value">${groups.paused.length}</div>
      </div>
      <div class="hc-context-card opp-summary-card opportunity-summary-card">
        <div class="hc-context-label opportunity-summary-label">Regime</div>
        <div class="hc-context-value opportunity-summary-value">
          <span class="opportunity-summary-regime opp-regime-pill ${escapeHtml(regimeClass)}">${escapeHtml(regimeText)}</span>
        </div>
      </div>
    `;
  }

function renderScoreLegend(data) {
  const host = document.getElementById("opportunityScoreLegend");
  if (!host) return;

  const rows = Array.isArray(data.candidates) ? data.candidates : [];
    const topScore = rows.reduce((best, row) => Math.max(best, resolveOpportunityScore(row)), 0);

    host.innerHTML = `
      <div class="opportunity-score-legend-head opp-score-legend-head">
        <div>
          <div class="opportunity-score-legend-title-row opp-score-legend-title-row">
            <div class="opportunity-summary-label hc-context-label">Top Intelligence Score</div>
            <div class="opportunity-control-help">
              <button
                class="opportunity-control-help-button"
              type="button"
              aria-label="Score range information"
            >i</button>
            <div class="opportunity-control-tooltip" role="tooltip">
              85+ = High conviction<br>
              65-84 = Building setup<br>
              Below 65 = Early / lower confidence
              </div>
            </div>
          </div>
          <div class="opportunity-summary-value hc-context-value">${fmtNumber(topScore)}</div>
        </div>
      </div>
      <p class="opportunity-score-legend-note opp-score-legend-note">Higher intelligence scores indicate stronger opportunity quality based on the current shadow ranking inputs.</p>
    `;
  }

function renderScannerStatus(systemData) {
  const summaryEl = document.getElementById("scannerStatusSummary");
  const toggleBtn = document.getElementById("scannerToggleBtn");
  const known = typeof systemData?.admin_state?.meme_rotation_enabled === "boolean";
  const enabled = Boolean(systemData?.admin_state?.meme_rotation_enabled);

  if (summaryEl) {
    summaryEl.textContent = !known
      ? "Scanner state unavailable. Toggle still uses the live scanner control."
      : enabled
        ? "Scanner is live and evaluating opportunities."
        : "Scanner is paused. Existing records remain visible for review.";
  }

  if (toggleBtn) {
    toggleBtn.textContent = !known ? "Toggle Scanner" : enabled ? "Pause Scanner" : "Resume Scanner";
    toggleBtn.className = `btn ${enabled ? "btn-secondary" : "btn-primary"}`;
    toggleBtn.dataset.enabled = enabled ? "true" : "false";
  }
}

function renderStatus(data, systemData) {
  const status = document.getElementById("heatmapStatus");
  if (!status) return;

  const cache = data._cache?.source || "unknown";
  const universeCount = resolveUniverseCount(data);
  const scannerEnabled = Boolean(systemData?.admin_state?.meme_rotation_enabled);

  status.innerHTML = `
    <span class="badge accent">${escapeHtml(cache)}</span>
    <span class="badge good">universe ${universeCount == null ? "—" : escapeHtml(String(universeCount))}</span>
    <span class="badge ${scannerEnabled ? "good" : "warn"}">${scannerEnabled ? "scanner live" : "scanner paused"}</span>
  `;
}

function renderShadowRotationReport(data, opportunityData = {}) {
  const host = document.getElementById("dashboardShadowRotation");
  if (!host) return;

  const cycles = Number(data?.cycles_analyzed || 0);
  if (!cycles) {
    host.innerHTML = `<div class="dashboard-shadow-fallback">No recent 24h shadow-rotation cycles are available yet.</div>`;
    return;
  }

  const topPicks = Array.isArray(data?.top_shadow_picks) ? data.top_shadow_picks.slice(0, 3) : [];
  const blockers = Array.isArray(data?.blocked_high_ranked_shadow_candidates)
    ? data.blocked_high_ranked_shadow_candidates.slice(0, 3)
    : [];
  const blockedReasons = Array.isArray(data?.blocked_reason_breakdown) ? data.blocked_reason_breakdown : [];
  const takeaways = Array.isArray(data?.quick_takeaways) ? data.quick_takeaways : [];
    const decisionSummary = decisionSummaryText(opportunityData?.satellite_decision_summary);
    const topBlocker = blockedReasons.length ? blockedReasons[0].reason : "none";
    const lastUpdatedTs = Number(data?.last_updated_ts || data?.generated_at || 0);

    host.innerHTML = `
      <div class="dashboard-shadow-head opp-shadow-head">
        <div>
          <div class="dashboard-shadow-title opp-shadow-title">Satellite Intelligence Monitor</div>
          <div class="dashboard-shadow-subtitle opp-shadow-subtitle">24h intelligence view of satellite selection, constraints, and missed opportunities.</div>
        </div>
        <div class="dashboard-shadow-updated opp-shadow-updated">Updated ${escapeHtml(formatUnixTime(lastUpdatedTs))}</div>
      </div>

      <div class="dashboard-shadow-summary-grid opp-shadow-summary-grid">
        <div class="dashboard-shadow-stat opp-shadow-stat">
          <div class="dashboard-shadow-stat-label">Cycles</div>
          <div class="dashboard-shadow-stat-value">${cycles}</div>
        </div>
        <div class="dashboard-shadow-stat opp-shadow-stat">
          <div class="dashboard-shadow-stat-label">Live Empty</div>
          <div class="dashboard-shadow-stat-value">${fmtPctValue(data?.empty_live_selection_rate_pct)}</div>
        </div>
        <div class="dashboard-shadow-stat opp-shadow-stat">
          <div class="dashboard-shadow-stat-label">Disagreement</div>
          <div class="dashboard-shadow-stat-value">${fmtPctValue(data?.shadow_live_disagreement_rate_pct)}</div>
        </div>
        <div class="dashboard-shadow-stat opp-shadow-stat">
          <div class="dashboard-shadow-stat-label">Avg Overlap</div>
          <div class="dashboard-shadow-stat-value">${Number(data?.average_overlap_count || 0).toFixed(2)}</div>
        </div>
    </div>

    <div class="dashboard-shadow-chip-row">
      <span class="dashboard-shadow-chip">Top blocker <strong>${escapeHtml(topBlocker)}</strong></span>
      ${decisionSummary ? `<span class="dashboard-shadow-chip">Decision engine <strong>${escapeHtml(decisionSummary)}</strong></span>` : ""}
      ${topPicks.map((row) => `
        <span class="dashboard-shadow-chip">${escapeHtml(row.product_id || "—")} <strong>${Number(row.count || 0)}</strong></span>
      `).join("")}
    </div>

    <div class="dashboard-shadow-list">
      <div class="dashboard-shadow-row">
        <span class="dashboard-shadow-row-label">Top Shadow Picks</span>
        <span class="dashboard-shadow-row-value">${topPicks.length ? topPicks.map((row) => `${row.product_id} (${row.count})`).join(" • ") : "No recurring picks yet"}</span>
      </div>
      <div class="dashboard-shadow-row">
        <span class="dashboard-shadow-row-label">Blocked High-Rank Names</span>
        <span class="dashboard-shadow-row-value">${blockers.length ? blockers.map((row) => `${row.product_id} (${row.count})`).join(" • ") : "No repeated blockers yet"}</span>
      </div>
    </div>

    <div class="dashboard-shadow-takeaway">${escapeHtml(takeaways[0] || "Shadow rotation monitoring is active.")}</div>
  `;
}

function renderShadowEligibleCandidates(data) {
  const host = document.getElementById("shadowEligibleCandidates");
  if (!host) return;

  const rows = Array.isArray(data?.shadow_eligible_candidates)
    ? data.shadow_eligible_candidates.slice(0, Math.max(5, Number(data?.top_n || 0)))
    : [];

  if (!rows.length) {
    host.innerHTML = `<div class="dashboard-shadow-fallback">No review-ready shadow candidates are waiting for enable right now.</div>`;
    return;
  }

    host.innerHTML = rows.map((row) => `
      <div class="shadow-eligible-row opp-eligible-card">
        <div class="shadow-eligible-main opp-eligible-main">
          <div class="shadow-eligible-head opp-eligible-head">
            <div class="shadow-eligible-symbol opp-eligible-symbol">${escapeHtml(row.product_id || "—")}</div>
            <span class="badge ${row.shadow_eligible && row.active_buy_universe === false ? "warn" : "good"}">
              ${escapeHtml(row.shadow_eligible && row.active_buy_universe === false ? "Not Live Yet" : "Review Ready")}
            </span>
            ${row.decision ? `<span class="badge ${escapeHtml(decisionBadgeTone(row.decision))}">${escapeHtml(decisionLabel(row.decision) || titleCase(row.decision))}</span>` : ""}
          </div>
          <div class="shadow-eligible-meta opp-eligible-meta">
            <span class="badge accent">score ${Number(row.net_score || 0).toFixed(1)}</span>
            <span class="pill">${escapeHtml(titleCase(row.confidence_band || "unknown"))}</span>
            <span class="pill">${escapeHtml(titleCase(row.liquidity_bucket || "unknown"))} liquidity</span>
          <span class="pill">${escapeHtml(titleCase(row.volatility_bucket || "unknown"))} volatility</span>
          <span class="pill">${escapeHtml(row.heldContext || "New candidate")}</span>
          <span class="pill">${escapeHtml(row.slotPressure || "Room available")}</span>
          <span class="pill">${escapeHtml(`Portfolio pressure: ${String(row.portfolioPressure || "Normal").toLowerCase()}`)}</span>
        </div>
        </div>
        <div class="shadow-eligible-reasons opp-eligible-reasons">
          ${renderDecisionLines(row, { showQualifies: true, showPortfolio: true })}
        </div>
      </div>
  `).join("");
}

function setShadowEligibleProposalResult(message, isError = false, sticky = false) {
  const el = document.getElementById("shadowEligibleProposalResult");
  if (!el) return;
  el.textContent = message;
  el.className = isError ? "shadow-eligible-result error" : "shadow-eligible-result";
  if (sticky) {
    el.dataset.userMessage = "true";
  } else {
    delete el.dataset.userMessage;
  }
}

function renderShadowNearMissCandidates(data) {
  const host = document.getElementById("shadowNearMissCandidates");
  if (!host) return;

  const rows = Array.isArray(data?.shadow_near_miss_candidates)
    ? data.shadow_near_miss_candidates.slice(0, Math.max(5, Number(data?.top_n || 0)))
    : [];

  if (!rows.length) {
    host.innerHTML = `<div class="dashboard-shadow-fallback">No strong near-miss shadow candidates are waiting for review right now.</div>`;
    return;
  }

    host.innerHTML = rows.map((row) => `
      <div class="shadow-eligible-row opp-eligible-card">
        <div class="shadow-eligible-main opp-eligible-main">
          <div class="shadow-eligible-head opp-eligible-head">
            <div class="shadow-eligible-symbol opp-eligible-symbol">${escapeHtml(row.product_id || "—")}</div>
            <span class="badge warn">Almost Ready</span>
            ${row.decision ? `<span class="badge ${escapeHtml(decisionBadgeTone(row.decision))}">${escapeHtml(decisionLabel(row.decision) || titleCase(row.decision))}</span>` : ""}
          </div>
          <div class="shadow-eligible-meta opp-eligible-meta">
            <span class="badge accent">score ${Number(row.net_score || 0).toFixed(1)}</span>
            <span class="pill">${escapeHtml(titleCase(row.confidence_band || "unknown"))}</span>
            <span class="pill">${escapeHtml(titleCase(row.liquidity_bucket || "unknown"))} liquidity</span>
          <span class="pill">${escapeHtml(titleCase(row.volatility_bucket || "unknown"))} volatility</span>
          <span class="pill">${escapeHtml(row.heldContext || "New candidate")}</span>
          <span class="pill">${escapeHtml(row.slotPressure || "Room available")}</span>
          <span class="pill">${escapeHtml(`Portfolio pressure: ${String(row.portfolioPressure || "Normal").toLowerCase()}`)}</span>
        </div>
        </div>
        <div class="shadow-eligible-reasons opp-eligible-reasons">
          ${renderDecisionLines(row, { showPrimaryMiss: true, showPortfolio: true })}
        </div>
      </div>
  `).join("");
}

function renderShadowProposalActionState(shadowData, recentProposalItems) {
  const button = document.getElementById("shadowEligibleProposalBtn");
  const resultEl = document.getElementById("shadowEligibleProposalResult");
  if (!button || !resultEl) return;

  const rows = Array.isArray(shadowData?.shadow_eligible_candidates)
    ? shadowData.shadow_eligible_candidates
    : [];
  const pendingProposal = resolvePendingSatelliteProposal(recentProposalItems);
  CURRENT_PENDING_SATELLITE_PROPOSAL = pendingProposal;

  button.textContent = pendingProposal ? "Approve Proposal" : "Generate Review Proposal";
  button.disabled = !pendingProposal && !rows.length;

  if (resultEl.dataset.userMessage) return;

  if (pendingProposal) {
    setShadowEligibleProposalResult(
      `Pending satellite-enable proposal ${pendingProposal.id} is ready for approval.`,
      false,
      false
    );
  } else if (!rows.length) {
    setShadowEligibleProposalResult("No review-ready candidates are available for proposal generation right now.", false, false);
  } else {
    setShadowEligibleProposalResult("Review-ready candidates can be bundled into an approval proposal.", false, false);
  }
}

  function opportunityCard(row) {
  const productId = row.product_id || row.symbol || "—";
  const score = resolveOpportunityScore(row);
  const tone = opportunityTone(score);
    const cardStyle = opportunityCardThemeStyle(score);
    const scoreStyle = `color:${scoreTextColor(score)}`;
    const move24h = resolve24hMove(row);
    const decision = String(row?.decision || "").trim();
    const decisionConfidence = String(row?.decision_confidence || "").trim();
    return `
      <article class="opportunity-card opp-card hc-pos-card ${tone}" style="${cardStyle}">
        <div class="opportunity-card-head opp-card-head">
          <div>
            <div class="opportunity-symbol opp-symbol">${escapeHtml(productId)}</div>
            <div class="opportunity-subline opp-subline">
              <span class="badge">${escapeHtml(assetTypeForCandidate(row))}</span>
              <span class="tiny">${escapeHtml(signalLabel(row))}</span>
            </div>
          </div>
          <div class="opportunity-score-wrap opp-score-wrap">
            <div class="opportunity-score-kicker opp-score-kicker">Intelligence Score</div>
            <div class="opportunity-score opp-score" style="${scoreStyle}">${fmtNumber(score)}</div>
            <div class="tiny">${escapeHtml(scoreLabel(score))} confidence</div>
          </div>
        </div>

        <div class="opportunity-pill-row opp-pill-row">
          ${flagPills(row)}
          ${decision ? `<span class="badge ${escapeHtml(decisionBadgeTone(decision))}">${escapeHtml(decisionLabel(decision) || titleCase(decision))}</span>` : ""}
          ${decisionConfidence ? `<span class="pill">${escapeHtml(titleCase(decisionConfidence))} decision confidence</span>` : ""}
        </div>

        ${decision || row?.decision_reason || row?.replacement_target || (Array.isArray(row?.decision_blockers) && row.decision_blockers.length)
          ? `<div class="opportunity-subline opp-subline">${renderDecisionLines(row, { showPortfolio: false })}</div>`
          : ""}

          <div class="opportunity-metrics opp-metrics">
          <div class="opportunity-metric opp-metric">
            <span class="opportunity-metric-label opp-metric-label">Target Allocation</span>
            <strong>${fmtPct(row.portfolio_weight || 0, false)}</strong>
          </div>
          <div class="opportunity-metric opp-metric">
            <span class="opportunity-metric-label opp-metric-label">Held Value</span>
            <strong>${fmtUsd(row.held_value_usd || 0)}</strong>
          </div>
          <div class="opportunity-metric opp-metric">
            <span class="opportunity-metric-label opp-metric-label">24H Move</span>
            <strong>${formatPercentOrNA(move24h)}</strong>
          </div>
          <div class="opportunity-metric opp-metric">
            <span class="opportunity-metric-label opp-metric-label">Unrealized</span>
            <strong>${fmtPct(row.unrealized_pnl_pct || 0)}</strong>
          </div>
        </div>

      <div class="divider"></div>

      ${actionButtons(row)}
    </article>
  `;
}

function renderGroups(data) {
  const meta = document.getElementById("heatmapMeta");
  const grid = document.getElementById("heatmapGrid");
  const sortKey = document.getElementById("heatmapSort")?.value || "score";

  if (!grid) return;

  const rows = Array.isArray(data.candidates) ? data.candidates : [];
  const sorted = sortCandidates(rows, sortKey);
  const groups = { active: [], watching: [], paused: [] };

  for (const row of sorted) {
    groups[groupForCandidate(row)].push(row);
  }

  if (meta) {
    meta.textContent = `${sorted.length} opportunity candidate(s) loaded • regime ${normalizeRegime(data.market_regime || "unknown")}`;
  }

  renderSummary(data, groups);

    grid.className = "";
    grid.innerHTML = `
      <div class="opps-board opp-board">
        ${["active", "watching", "paused"].map((groupKey) => {
          const items = groups[groupKey];
          return `
            <section class="opps-section opp-column opps-section-${groupKey}">
              <div class="section-header compact-header opp-column-head">
                <div>
                  <h3>${groupTitle(groupKey)}</h3>
                  <p class="section-subtitle">${groupDescription(groupKey)}</p>
                </div>
                <span class="badge">${items.length}</span>
              </div>
              <div class="opps-cards opp-column-cards">
                ${items.length
                  ? items.map((row) => opportunityCard(row)).join("")
                  : `<div class="opportunity-empty opp-empty muted">${escapeHtml(emptyStateText(groupKey))}</div>`
                }
              </div>
            </section>
        `;
      }).join("")}
    </div>
  `;
}

async function postJson(path, body) {
  return fetchJson(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...body, ...(API_SECRET ? { secret: API_SECRET } : {}) })
  }, 15000);
}

async function setOpportunityMode(productId, mode) {
  try {
    if (mode === "enable") {
      await postJson("/api/admin/satellite/block", { product_id: productId, action: "remove" });
      await postJson("/api/admin/satellite/allow", { product_id: productId, action: "add" });
    } else if (mode === "disable") {
      await postJson("/api/admin/satellite/allow", { product_id: productId, action: "remove" });
      await postJson("/api/admin/satellite/block", { product_id: productId, action: "add" });
    } else {
      await postJson("/api/admin/satellite/allow", { product_id: productId, action: "remove" });
      await postJson("/api/admin/satellite/block", { product_id: productId, action: "remove" });
    }

    await refreshMemeRotation();
  } catch (err) {
    console.error(err);
    const status = document.getElementById("heatmapStatus");
    if (status) {
      status.innerHTML = `<span class="badge bad">${escapeHtml(err.message)}</span>`;
    }
  }
}

async function toggleOpportunityScanner() {
  const button = document.getElementById("scannerToggleBtn");
  const enabled = String(button?.dataset.enabled || "false") === "true";

  try {
    await postJson("/api/admin/meme_rotation", { enabled: !enabled });
    await refreshMemeRotation();
  } catch (err) {
    console.error(err);
    const summaryEl = document.getElementById("scannerStatusSummary");
    if (summaryEl) {
      summaryEl.textContent = `Scanner update failed: ${err.message}`;
    }
  }
}

async function generateReviewProposal() {
  const button = document.getElementById("shadowEligibleProposalBtn");
  if (button?.disabled) {
    setShadowEligibleProposalResult("No review-ready candidates are available for proposal generation right now.");
    return;
  }

  if (button) button.disabled = true;
  setShadowEligibleProposalResult("Generating review proposal...", false, true);

  try {
    const result = await fetchJson("/api/config_proposals/generate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...(API_SECRET ? { secret: API_SECRET } : {}) })
    }, 30000);

    const status = String(result?.status || "").trim().toLowerCase();
    if (status === "created") {
      const createdCount = Number(result?.created_count || 0);
      const delivery = result?.notification_sent === false ? " Telegram delivery needs review." : "";
      setShadowEligibleProposalResult(`${createdCount || 1} review proposal${createdCount === 1 || !createdCount ? "" : "s"} created.${result.proposal_id ? ` Latest ${result.proposal_id}.` : ""}${delivery}`.trim(), false, true);
    } else if (status === "deduped" || status === "deduped_recent") {
      setShadowEligibleProposalResult(`Existing pending proposal ${result.proposal_id || ""} already matches the current advisory state.`.trim(), false, true);
    } else if (status === "noop") {
      setShadowEligibleProposalResult("No proposal was generated because no review-ready candidates qualified.", false, true);
    } else {
      setShadowEligibleProposalResult(`Proposal generation returned status: ${status || "unknown"}.`, false, true);
    }
  } catch (err) {
    console.error(err);
    setShadowEligibleProposalResult(`Proposal generation failed: ${err.message}`, true, true);
  } finally {
    await refreshMemeRotation();
  }
}

async function approveReviewProposal() {
  const proposalId = String(CURRENT_PENDING_SATELLITE_PROPOSAL?.id || "").trim();
  if (!proposalId) {
    setShadowEligibleProposalResult("No pending satellite-enable proposal is available for approval right now.", true, true);
    return;
  }

  const button = document.getElementById("shadowEligibleProposalBtn");
  if (button) button.disabled = true;
  setShadowEligibleProposalResult(`Approving proposal ${proposalId}...`, false, true);

  try {
    const result = await fetchJson(`/api/config_proposals/${encodeURIComponent(proposalId)}/approve`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...(API_SECRET ? { secret: API_SECRET } : {}) })
    }, 30000);

    if (result?.auto_apply_attempted && result?.auto_apply_ok) {
      setShadowEligibleProposalResult("Proposal approved and applied based on the current After Approval setting.", false, true);
    } else if (result?.auto_apply_attempted && result?.auto_apply_ok === false) {
      setShadowEligibleProposalResult("Proposal was approved, but the automatic apply step needs review.", true, true);
    } else {
      setShadowEligibleProposalResult("Proposal approved. Apply remains a separate operator step.", false, true);
    }
  } catch (err) {
    console.error(err);
    setShadowEligibleProposalResult(`Proposal approval failed: ${err.message}`, true, true);
  } finally {
    await refreshMemeRotation();
  }
}

async function handleReviewProposalAction() {
  if (CURRENT_PENDING_SATELLITE_PROPOSAL?.id) {
    await approveReviewProposal();
    return;
  }
  await generateReviewProposal();
}

function relativeTimeFromTs(ts) {
  const numeric = Number(ts || 0);
  if (!numeric) return "Unknown";
  const diffSec = Math.max(0, Math.floor(Date.now() / 1000) - numeric);
  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)} min ago`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h ago`;
  return `${Math.floor(diffSec / 86400)}d ago`;
}

function buildChartsHref(productId) {
  const base = API_SECRET
    ? `/charts?secret=${encodeURIComponent(API_SECRET)}`
    : "/charts";
  const sep = base.includes("?") ? "&" : "?";
  return `${base}${sep}product_id=${encodeURIComponent(productId)}`;
}

function toneClass(value) {
  const numeric = Number(value || 0);
  if (numeric > 0) return "positive";
  if (numeric < 0) return "negative";
  return "";
}

function renderStatus(data, systemData, shadowData = {}, signals = []) {
  const status = document.getElementById("heatmapStatus");
  if (!status) return;

  const scannerEnabled = Boolean(systemData?.admin_state?.meme_rotation_enabled);
  const regime = normalizeRegime(data.market_regime || systemData?.portfolio_summary?.market_regime || "unknown");
  const regimeClass = regimeTone(data.market_regime || systemData?.portfolio_summary?.market_regime || "unknown");
  const lastSweepTs = Number(
    shadowData?.last_updated_ts ||
    shadowData?.generated_at ||
    signals?.[0]?.ts ||
    data?.updated_at ||
    0
  );

  status.innerHTML = `
    <div class="opp-status-left">
      <span class="badge ${scannerEnabled ? "good" : "warn"}">${scannerEnabled ? "Scanner Enabled" : "Scanner Paused"}</span>
      <span class="opportunity-summary-regime opp-regime-pill ${escapeHtml(regimeClass)}">${escapeHtml(regime)}</span>
      <span class="pill">Last sweep: ${escapeHtml(relativeTimeFromTs(lastSweepTs))}</span>
    </div>
    <div class="opp-status-right">
      <span class="tiny">${escapeHtml(`${resolveUniverseCount(data) ?? 0} assets in view`)}</span>
    </div>
  `;
}

function renderLiveSignals(signals) {
  const host = document.getElementById("dashboardShadowRotation");
  if (!host) return;

  const rows = Array.isArray(signals) ? signals.slice(0, 10) : [];
  if (!rows.length) {
    host.innerHTML = `<div class="dashboard-shadow-fallback">Scanner is running. Signals will appear here as they fire.</div>`;
    return;
  }

  host.innerHTML = rows.map((row) => {
    const signal = String(row.signal || "").trim().toLowerCase();
    const badgeTone = signal === "buy" ? "good" : signal === "trim" ? "warn" : "bad";
    const conviction = Number(row.conviction || 0);
    return `
      <article class="hc-trade-card chart-signal-card">
        <div class="hc-trade-main">
          <span class="hc-trade-badge ${badgeTone}">${escapeHtml(signal.toUpperCase() || "SIGNAL")}</span>
          <div class="hc-trade-copy">
            <div class="hc-trade-title">${escapeHtml(row.product_id || "—")} at ${escapeHtml(fmtUsd(row.price || 0))}</div>
            <div class="hc-trade-subtitle">${escapeHtml(formatUnixTime(row.ts))} · conviction ${(conviction * 100).toFixed(0)}%</div>
          </div>
        </div>
      </article>
    `;
  }).join("");
}

function renderPositions(data) {
  const host = document.getElementById("opportunitySummary");
  if (!host) return;

  const rows = Array.isArray(data.candidates) ? data.candidates : [];
  const held = rows
    .filter((row) => Number(row.held_value_usd || row.value_total_usd || 0) > 1.0)
    .sort((a, b) => Number(b.held_value_usd || b.value_total_usd || 0) - Number(a.held_value_usd || a.value_total_usd || 0));

  if (!held.length) {
    host.innerHTML = `<div class="hc-empty-card">No held satellite positions above $1 right now.</div>`;
    return;
  }

  host.innerHTML = held.map((row) => {
    const productId = String(row.product_id || row.symbol || "—");
    const symbol = productId.split("-")[0];
    const heldValue = Number(row.held_value_usd || row.value_total_usd || 0);
    const unrealizedPct = Number(row.unrealized_pnl_pct || 0);
    const move24h = resolve24hMove(row);
    return `
      <article class="hc-pos-card opp-card" style="${opportunityCardThemeStyle(resolveOpportunityScore(row))}">
        <div class="hc-pos-icon satellite">${escapeHtml(symbol.slice(0, 2))}</div>
        <div class="hc-pos-info">
          <div class="hc-pos-symbol">${escapeHtml(symbol)}</div>
          <div class="hc-pos-name">${escapeHtml(productId)}</div>
          <div class="opportunity-subline opp-subline">Held value ${escapeHtml(fmtUsd(heldValue))}</div>
        </div>
        <div class="hc-pos-right">
          <div class="hc-pos-value ${toneClass(unrealizedPct)}">${unrealizedPct >= 0 ? "+" : ""}${fmtPct(unrealizedPct)}</div>
          <div class="hc-pos-change ${toneClass(move24h || 0)}">${formatPercentOrNA(move24h)}</div>
          <button class="btn btn-secondary opportunity-action-btn opp-action-btn" type="button" title="Uses the existing disable control path." onclick="setOpportunityMode('${escapeHtml(productId)}','disable')">Close</button>
        </div>
      </article>
    `;
  }).join("");
}

function renderTopOpportunities(data) {
  const meta = document.getElementById("heatmapMeta");
  const host = document.getElementById("heatmapGrid");
  const sortKey = document.getElementById("heatmapSort")?.value || "score";
  if (!host) return;

  const rows = Array.isArray(data.candidates) ? data.candidates : [];
  const opportunities = sortCandidates(
    rows.filter((row) => {
      const heldValue = Number(row.held_value_usd || row.value_total_usd || 0);
      return heldValue <= 1.0 && !row.blocked && row.enabled !== false && !row.core;
    }),
    sortKey
  ).slice(0, 12);

  if (meta) {
    meta.textContent = `${opportunities.length} top opportunities loaded • regime ${normalizeRegime(data.market_regime || "unknown")}`;
  }

  if (!opportunities.length) {
    host.innerHTML = `<div class="hc-empty-card">No unheld opportunities are standing out right now.</div>`;
    return;
  }

  host.innerHTML = `<div class="opps-board opp-board">${opportunities.map((row) => {
    const productId = String(row.product_id || row.symbol || "—");
    const score = Math.max(0, Math.min(100, resolveOpportunityScore(row)));
    const move24h = resolve24hMove(row);
    return `
      <article class="opportunity-card opp-card hc-pos-card ${opportunityTone(score)}" style="${opportunityCardThemeStyle(score)}">
        <div class="opportunity-card-head opp-card-head">
          <div>
            <div class="opportunity-symbol opp-symbol">${escapeHtml(productId)}</div>
            <div class="opportunity-subline opp-subline">${escapeHtml(assetTypeForCandidate(row))}</div>
          </div>
          <div class="opportunity-score-wrap opp-score-wrap">
            <div class="opportunity-score-kicker opp-score-kicker">Score</div>
            <div class="opportunity-score opp-score" style="color:${scoreTextColor(score)}">${fmtNumber(score)}</div>
          </div>
        </div>
        <div class="opp-progress-shell">
          <div class="opp-progress-bar">
            <span class="opp-progress-fill" style="width:${score}%; background:${scoreColor(score)};"></span>
          </div>
        </div>
        <div class="opportunity-metrics opp-metrics">
          <div class="opportunity-metric opp-metric">
            <span class="opportunity-metric-label opp-metric-label">24H Move</span>
            <strong class="${toneClass(move24h || 0)}">${formatPercentOrNA(move24h)}</strong>
          </div>
          <div class="opportunity-metric opp-metric">
            <span class="opportunity-metric-label opp-metric-label">Target Weight</span>
            <strong>${fmtPct(row.portfolio_weight || 0, false)}</strong>
          </div>
        </div>
        <div class="opportunity-actions opp-actions">
          <a class="btn btn-secondary opportunity-action-btn opp-action-btn" href="${buildChartsHref(productId)}">View Chart</a>
          <button class="btn btn-primary opportunity-action-btn opp-action-btn" type="button" onclick="setOpportunityMode('${escapeHtml(productId)}','enable')">Enable</button>
        </div>
      </article>
    `;
  }).join("")}</div>`;
}

function renderBlockedAssets(data) {
  const host = document.getElementById("shadowNearMissCandidates");
  if (!host) return;

  const rows = (Array.isArray(data.candidates) ? data.candidates : [])
    .filter((row) => row.blocked || row.enabled === false)
    .sort((a, b) => resolveOpportunityScore(b) - resolveOpportunityScore(a));

  if (!rows.length) {
    host.innerHTML = `<div class="dashboard-shadow-fallback">No blocked assets right now.</div>`;
    return;
  }

  host.innerHTML = rows.map((row) => `
    <div class="shadow-eligible-row opp-eligible-card">
      <div class="shadow-eligible-main opp-eligible-main">
        <div class="shadow-eligible-head opp-eligible-head">
          <div class="shadow-eligible-symbol opp-eligible-symbol">${escapeHtml(row.product_id || "—")}</div>
          <span class="badge bad">Blocked</span>
          <span class="badge accent">score ${Number(resolveOpportunityScore(row)).toFixed(1)}</span>
        </div>
        <div class="shadow-eligible-reasons opp-eligible-reasons">
          <div class="shadow-eligible-reason"><strong>24H move:</strong> ${escapeHtml(formatPercentOrNA(resolve24hMove(row)))}</div>
          <div class="shadow-eligible-reason"><strong>Status:</strong> ${escapeHtml(statusText(row))}</div>
        </div>
      </div>
      <div class="opp-actions">
        <button class="btn btn-secondary" type="button" onclick="setOpportunityMode('${escapeHtml(row.product_id || "")}','auto')">Unblock</button>
      </div>
    </div>
  `).join("");
}

async function runScannerNow() {
  const button = document.getElementById("oppRunScanBtn");
  if (button) button.disabled = true;
  try {
    const result = await postJson("/api/scanner/run", {});
    const core = Array.isArray(result?.core_signals) ? result.core_signals.length : 0;
    const sat = Array.isArray(result?.satellite_signals) ? result.satellite_signals.length : 0;
    const summaryEl = document.getElementById("scannerStatusSummary");
    if (summaryEl) {
      summaryEl.textContent = `Manual scan complete. Core signals ${core}, satellite signals ${sat}.`;
    }
    await refreshMemeRotation();
  } catch (err) {
    console.error(err);
    const summaryEl = document.getElementById("scannerStatusSummary");
    if (summaryEl) {
      summaryEl.textContent = `Manual scan failed: ${err.message}`;
    }
  } finally {
    if (button) button.disabled = false;
  }
}

async function refreshMemeRotation() {
  try {
    const [data, systemData, shadowRotationData, configData, proposalData, signalLog] = await Promise.all([
      fetchJson("/api/meme_rotation"),
      fetchJson("/api/system_snapshot").catch(() => ({})),
      fetchJson("/api/shadow_rotation_report").catch(() => ({})),
      fetchJson("/api/config").catch(() => ({ config: {} })),
      fetchJson("/api/config_proposals/recent?limit=10").catch(() => ({ items: [] })),
      fetchJson("/api/signals/log?limit=10").catch(() => ({ signals: [] }))
    ]);
    const config = safeObject(configData?.config);
    const recentProposals = Array.isArray(proposalData?.items) ? proposalData.items : [];
    const recentSignals = Array.isArray(signalLog?.signals) ? signalLog.signals : [];
    const eligibleRows = Array.isArray(shadowRotationData?.shadow_eligible_candidates)
      ? shadowRotationData.shadow_eligible_candidates
      : [];
    const enrichRows = (rows) => rows.map((row) => ({
      ...row,
      ...buildShadowPortfolioContext(row, data, systemData || {}, config, eligibleRows)
    }));
    const enrichedShadowData = {
      ...safeObject(shadowRotationData),
      shadow_eligible_candidates: enrichRows(eligibleRows),
      shadow_near_miss_candidates: enrichRows(Array.isArray(shadowRotationData?.shadow_near_miss_candidates) ? shadowRotationData.shadow_near_miss_candidates : [])
    };

    renderStatus(data, systemData || {}, enrichedShadowData || {}, recentSignals);
    renderScannerStatus(systemData || {});
    renderLiveSignals(recentSignals);
    renderShadowProposalActionState(enrichedShadowData || {}, recentProposals);
    renderShadowEligibleCandidates(enrichedShadowData || {});
    renderPositions(data);
    renderTopOpportunities(data);
    renderBlockedAssets(data);
  } catch (err) {
    console.error(err);
    const grid = document.getElementById("heatmapGrid");
    if (grid) {
      grid.innerHTML = `<div class="status-console error">Opportunities load failed: ${escapeHtml(err.message)}</div>`;
    }
  }
}

function applyHeatmapTheme() {
  const select = document.getElementById("heatmapColorTheme");
  const theme = select ? select.value : "ocean";
  localStorage.setItem("hc_heatmap_theme", theme);
  refreshMemeRotation();
}

window.handleReviewProposalAction = handleReviewProposalAction;
window.generateReviewProposal = generateReviewProposal;
window.refreshMemeRotation = refreshMemeRotation;
window.setOpportunityMode = setOpportunityMode;
window.toggleOpportunityScanner = toggleOpportunityScanner;
window.applyHeatmapTheme = applyHeatmapTheme;
window.runScannerNow = runScannerNow;

const savedTheme = getHeatmapTheme();
const themeSelect = document.getElementById("heatmapColorTheme");
if (themeSelect) themeSelect.value = savedTheme;
const runScanButton = document.getElementById("oppRunScanBtn");
if (runScanButton) runScanButton.addEventListener("click", runScannerNow);
refreshMemeRotation();
