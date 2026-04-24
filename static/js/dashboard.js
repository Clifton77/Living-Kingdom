/* ═══════════════════════════════════════════════════════════════
   WeatherBot Dashboard — Client-side JS
   SSE live updates · Controls · Theme toggle
═══════════════════════════════════════════════════════════════ */

'use strict';

// ── State ────────────────────────────────────────────────────
let pendingCloseMarketId = null;
let tradeHistoryOffset   = 0;
let _signals = (typeof INITIAL_STATE !== 'undefined' && INITIAL_STATE.signals) ? INITIAL_STATE.signals : {};
const HISTORY_PAGE_SIZE  = 20;
const STATION_CITIES = {
  KJFK: 'New York',      KORD: 'Chicago',       KMIA: 'Miami',         KDFW: 'Dallas',
  KLAX: 'Los Angeles',   KATL: 'Atlanta',        KDEN: 'Denver',        KHOU: 'Houston',
  KAUS: 'Austin',        KPHL: 'Philadelphia',   KBOS: 'Boston',        KDCA: 'Washington DC',
  KLAS: 'Las Vegas',     KMSP: 'Minneapolis',    KMSY: 'New Orleans',   KOKC: 'Oklahoma City',
  KPHX: 'Phoenix',       KSAT: 'San Antonio',    KSEA: 'Seattle',       KSFO: 'San Francisco',
};
const STATION_TZ = {
  KJFK: 'America/New_York',    KMIA: 'America/New_York',    KATL: 'America/New_York',
  KPHL: 'America/New_York',    KBOS: 'America/New_York',    KDCA: 'America/New_York',
  KORD: 'America/Chicago',     KDFW: 'America/Chicago',     KHOU: 'America/Chicago',
  KAUS: 'America/Chicago',     KMSP: 'America/Chicago',     KMSY: 'America/Chicago',
  KOKC: 'America/Chicago',     KSAT: 'America/Chicago',
  KDEN: 'America/Denver',      KPHX: 'America/Phoenix',
  KLAX: 'America/Los_Angeles', KLAS: 'America/Los_Angeles',
  KSEA: 'America/Los_Angeles', KSFO: 'America/Los_Angeles',
};

// ── Time / locale helpers ────────────────────────────────────
/** Convert a Zulu hour string like "18Z" or "0630Z" to station local time. */
function localizeZuluInText(text, station) {
  if (!text || !station) return text;
  const tz = STATION_TZ[station];
  if (!tz) return text;
  return text.replace(/\b(\d{2})(\d{2})?Z\b/g, (match, hh, mm) => {
    const h = parseInt(hh, 10);
    const m = mm ? parseInt(mm, 10) : 0;
    const now = new Date();
    const utc = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), h, m));
    const opts = { timeZone: tz, hour: 'numeric', timeZoneName: 'short' };
    if (m) opts.minute = '2-digit';
    const local = utc.toLocaleTimeString('en-US', opts);
    return `${match}\u202F(${local})`;
  });
}

/** Set tier footer span with both UTC string and user's local time. */
function setTierTime(id, utcStr) {
  const el = document.getElementById(id);
  if (!el) return;
  const m = String(utcStr).match(/(\d{1,2}):(\d{2})/);
  if (!m) { el.textContent = utcStr; return; }
  const now = new Date();
  const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(),
    parseInt(m[1], 10), parseInt(m[2], 10)));
  const local = d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZoneName: 'short' });
  el.textContent = `${utcStr} / ${local}`;
}

/** Run on page load: convert all TAF Z-times and tier spans to local. */
function applyLocalTimes() {
  // TAF summaries
  document.querySelectorAll('[data-taf-station]').forEach(el => {
    const station = el.getAttribute('data-taf-station');
    el.textContent = localizeZuluInText(el.textContent, station);
  });
  // Footer tier spans
  document.querySelectorAll('[data-utc]').forEach(el => {
    const utc = el.getAttribute('data-utc');
    const id  = el.id;
    if (id) setTierTime(id, utc);
  });
}

// ── Theme ────────────────────────────────────────────────────
(function initTheme() {
  const saved = localStorage.getItem('wb-theme') || 'dark';
  document.documentElement.setAttribute('data-bs-theme', saved);
  const btn = document.getElementById('btn-theme');
  if (btn) btn.textContent = saved === 'dark' ? '☀' : '☾';
})();

function toggleTheme() {
  const html    = document.documentElement;
  const current = html.getAttribute('data-bs-theme');
  const next    = current === 'dark' ? 'light' : 'dark';
  html.setAttribute('data-bs-theme', next);
  localStorage.setItem('wb-theme', next);
  document.getElementById('btn-theme').textContent = next === 'dark' ? '☀' : '☾';
}

// ── SSE connection ───────────────────────────────────────────
function initSSE() {
  const src = new EventSource('/stream');
  const indicator = document.getElementById('sse-status');

  src.addEventListener('open', () => {
    if (indicator) { indicator.textContent = '⬤ Live'; indicator.className = 'text-success'; }
  });

  src.addEventListener('full_state', e => {
    const state = JSON.parse(e.data);
    applyFullState(state);
  });

  src.addEventListener('state_update', e => {
    const s = JSON.parse(e.data);
    updateSummaryStrip(s);
    updateTimestamp();
  });

  src.addEventListener('position_opened', e => {
    const d = JSON.parse(e.data);
    // Full state will arrive shortly via state_update; refresh carousel
    fetch('/api/state').then(r => r.json()).then(state => {
      if (state.signals) _signals = state.signals;
      rebuildCarousel(state.positions);
      updatePositionCount(Object.keys(state.positions).length);
    });
  });

  src.addEventListener('position_price_update', e => {
    const d       = JSON.parse(e.data);
    const safeMid = d.market_id.replace(/-/g, '_');
    const sign    = d.unrealized_pnl >= 0 ? '+' : '';

    const bidEl = document.getElementById(`pos-bid-${safeMid}`);
    if (bidEl) bidEl.textContent = `$${d.current_bid.toFixed(2)}`;

    const badgeEl = document.getElementById(`pos-badge-${safeMid}`);
    if (badgeEl) {
      badgeEl.textContent = `$${sign}${d.unrealized_pnl.toFixed(2)}`;
      badgeEl.className   = `badge ${d.unrealized_pnl >= 0 ? 'bg-success' : 'bg-danger'} fs-6`;
    }

    const pctEl = document.getElementById(`pos-pct-${safeMid}`);
    if (pctEl) {
      pctEl.textContent = `${d.pnl_pct >= 0 ? '+' : ''}${d.pnl_pct.toFixed(1)}%`;
      pctEl.className   = `${d.pnl_pct >= 0 ? 'text-success' : 'text-danger'} fw-bold`;
    }
  });

  src.addEventListener('position_closed', e => {
    const d = JSON.parse(e.data);
    removeCarouselCard(d.market_id);
    appendTradeHistoryRow(d);
    showToast(`Position closed: ${d.market_id}`,
      `P/L: $${(d.realized_pnl >= 0 ? '+' : '') + d.realized_pnl.toFixed(4)}`,
      d.realized_pnl >= 0 ? 'success' : 'danger');
  });

  // If page loaded with no cards (bot was initializing), reload once signals arrive
  const _noCardsOnLoad = document.querySelectorAll('#signal-cards [data-station]').length === 0;
  let _reloadScheduled = false;

  src.addEventListener('taf_update', e => {
    const d = JSON.parse(e.data);
    const s = d.station;
    // Update sky badge in weather widget (TAF sky_cover takes precedence over METAR)
    const skyEl = document.getElementById(`wx-sky-${s}`);
    if (skyEl && d.sky_cover) skyEl.textContent = d.sky_cover;
    // Update weather icon class to reflect TAF condition
    const iconEl = document.getElementById(`wx-icon-${s}`);
    if (iconEl && d.condition) {
      iconEl.className = `wb-wx-icon wb-wx-${d.condition.replace(/ /g, '_')} mb-1`;
    }
    // AMD badge on decision badge parent
    const card = document.querySelector(`[data-station="${s}"]`);
    if (card && d.has_amd) {
      const badge = card.querySelector('.wb-decision-badge');
      if (badge && !badge.nextElementSibling?.classList.contains('wb-amd-badge')) {
        const amd = document.createElement('span');
        amd.className = 'badge bg-warning text-dark ms-1 wb-amd-badge';
        amd.textContent = 'AMD';
        badge.insertAdjacentElement('afterend', amd);
      }
    }
  });

  src.addEventListener('kalshi_top_update', e => {
    const d = JSON.parse(e.data);
    const s = d.station;
    // Kalshi prob = yes_ask
    const kalshiEl = document.getElementById(`top-kalshi-prob-${s}`);
    if (kalshiEl && d.yes_ask != null) kalshiEl.textContent = `${Math.round(d.yes_ask * 100)}%`;
    // Edge = model_prob - yes_ask
    const edgeEl = document.getElementById(`top-edge-${s}`);
    if (edgeEl && d.fresh_edge != null) {
      edgeEl.textContent = `${d.fresh_edge >= 0 ? '+' : ''}${d.fresh_edge.toFixed(2)}`;
      edgeEl.className   = d.fresh_edge > 0 ? 'text-success fw-bold' : 'text-danger';
    }
  });

  src.addEventListener('metar_update', e => {
    const d = JSON.parse(e.data);
    const s = d.station;

    // Weather widget
    const tempEl = document.getElementById(`wx-temp-${s}`);
    if (tempEl && d.temp_f != null) {
      const t   = Math.round(d.temp_f);
      const cls = t < 45 ? 'cold' : (t > 95 ? 'hot' : (t > 80 ? 'warm' : 'mild'));
      tempEl.textContent = `${t}°F`;
      tempEl.className   = `wb-wx-temp wb-temp-${cls}`;
    }
    const windEl = document.getElementById(`wx-wind-${s}`);
    if (windEl && d.wind_kt != null) windEl.textContent = `${Math.round(d.wind_kt)}kt`;
    const dewEl = document.getElementById(`wx-dew-${s}`);
    if (dewEl && d.dewpoint_f != null) dewEl.textContent = `DP ${Math.round(d.dewpoint_f)}°F`;
    const skyEl = document.getElementById(`wx-sky-${s}`);
    if (skyEl && d.sky_cover) skyEl.textContent = d.sky_cover;

    // Obs tracking row
    const obsTempEl = document.getElementById(`obs-temp-${s}`);
    if (obsTempEl && d.temp_f != null) obsTempEl.textContent = `${Math.round(d.temp_f)}°F`;
    const sig = _signals[s] || {};
    if (sig.forecast_adjusted != null && d.temp_f != null) {
      const divF   = d.temp_f - sig.forecast_adjusted;
      const divCls = divF > 4 ? 'hot' : (divF < -4 ? 'cold' : 'ok');
      const arrow  = divCls === 'hot' ? ' ▲' : (divCls === 'cold' ? ' ▼' : ' ≈');
      const obsDivEl = document.getElementById(`obs-div-${s}`);
      if (obsDivEl) {
        obsDivEl.textContent = `${divF >= 0 ? '+' : ''}${Math.round(divF)}°F${arrow}`;
        obsDivEl.className   = `wb-divergence-flag wb-divergence-${divCls}`;
      }
    }
  });

  src.addEventListener('signal_update', e => {
    const d = JSON.parse(e.data);

    if (_noCardsOnLoad && !_reloadScheduled) {
      _reloadScheduled = true;
      setTimeout(() => location.reload(), 4000);
      return;
    }

    // Update _signals cache with fresh data
    _signals[d.station] = Object.assign(_signals[d.station] || {}, d);

    // Decision badge + card border
    flashSignalCard(d.station, d.decision);
    const badgeEl = document.querySelector(`[data-station="${d.station}"] .wb-decision-badge`);
    if (badgeEl) {
      badgeEl.textContent = d.decision;
      badgeEl.className   = `wb-decision-badge badge wb-badge-${d.decision.toLowerCase()}`;
    }

    // Forecast temp
    const fcstEl = document.getElementById(`fcst-temp-${d.station}`);
    if (fcstEl && d.forecast_adjusted != null) {
      const t   = Math.round(d.forecast_adjusted);
      const cls = t < 45 ? 'cold' : (t > 95 ? 'hot' : (t > 80 ? 'warm' : 'mild'));
      fcstEl.textContent = `${t}°F`;
      fcstEl.className   = `fw-bold fs-5 wb-temp-${cls}`;
    }
    const stdEl = document.getElementById(`fcst-std-${d.station}`);
    if (stdEl && d.bias_std != null) stdEl.textContent = `±${d.bias_std.toFixed(1)}°F`;

    const divEl = document.getElementById(`fcst-div-${d.station}`);
    if (divEl && d.model_divergence_f != null) {
      const sign   = d.model_divergence_f >= 0 ? '+' : '';
      const bgCls  = d.model_divergence_f > 2 ? 'bg-warning text-dark' : (d.model_divergence_f < -2 ? 'bg-info text-dark' : 'bg-secondary');
      divEl.textContent = `NWS ${sign}${Math.round(d.model_divergence_f)}°F vs MOS`;
      divEl.className   = `badge ${bgCls} wb-mos-badge`;
    }

    // Top bucket row
    const lowerTail = d.live_lower_tail !== undefined ? d.live_lower_tail : 68;
    const upperTail = d.live_upper_tail !== undefined ? d.live_upper_tail : 77;
    const bucketEl  = document.getElementById(`top-bucket-${d.station}`);
    if (bucketEl && d.top_bucket != null) bucketEl.textContent = fmtBucket(d.top_bucket, lowerTail, upperTail);

    const modelProbEl = document.getElementById(`top-model-prob-${d.station}`);
    if (modelProbEl && d.top_model_prob != null) modelProbEl.textContent = `${Math.round(d.top_model_prob * 100)}%`;

    const kalshiProbEl = document.getElementById(`top-kalshi-prob-${d.station}`);
    if (kalshiProbEl && d.top_kalshi_prob != null) kalshiProbEl.textContent = `${Math.round(d.top_kalshi_prob * 100)}%`;

    const edgeEl = document.getElementById(`top-edge-${d.station}`);
    if (edgeEl && d.top_edge != null) {
      edgeEl.textContent = `${d.top_edge >= 0 ? '+' : ''}${d.top_edge.toFixed(2)}`;
      edgeEl.className   = d.top_edge > 0 ? 'text-success fw-bold' : 'text-danger';
    }

    const clusterEl = document.getElementById(`fcst-cluster-${d.station}`);
    if (clusterEl && d.cluster_id != null) {
      clusterEl.textContent = `Cluster ${d.cluster_id} · ${d.season} · n=${d.n_obs}`;
    }

    const signalsUpdated = document.getElementById('signals-updated');
    if (signalsUpdated) signalsUpdated.textContent = `Updated ${nowStr()}`;
  });

  src.addEventListener('tier_heartbeat', e => {
    const tiers = JSON.parse(e.data);
    ['tier1','tier2','tier3','settlement'].forEach(t => {
      if (tiers[t]) setTierTime(`${t}-last`, tiers[t]);
    });
  });

  src.addEventListener('bias_rebuild_start', e => {
    const d = JSON.parse(e.data);
    showToast('Bias Rebuild', d.message, 'info');
  });

  src.addEventListener('bias_rebuild_done', e => {
    const d = JSON.parse(e.data);
    const btn = document.getElementById('btn-rebuild-bias');
    if (btn) {
      btn.disabled = false;
      btn.textContent = '⟳ Bias';
      if (d.bias_updated) {
        btn.title = `Rebuild bias table from latest obs/forecast data. Last updated: ${d.bias_updated}`;
      }
    }
    showToast('Bias Rebuild', d.message, d.ok ? 'success' : 'danger');
  });

  src.addEventListener('alert', e => {
    const a = JSON.parse(e.data);
    showToast(a.title, a.message, alertBootstrapClass(a.level));
    prependAlert(a);
  });

  src.addEventListener('heartbeat', () => {
    updateTimestamp();
  });

  src.onerror = () => {
    if (indicator) { indicator.textContent = '⬤ Reconnecting…'; indicator.className = 'text-warning'; }
    // EventSource auto-reconnects — no manual action needed
  };
}

// ── Apply full state snapshot ────────────────────────────────
function applyFullState(state) {
  if (state.signals) _signals = state.signals;
  updateSummaryStrip(state.summary);
  if (state.positions) rebuildCarousel(state.positions);
  if (state.closed_trades) populateTradeHistory(state.closed_trades);
  if (state.tier_status) {
    ['tier1','tier2','tier3','settlement'].forEach(t => {
      if (state.tier_status[t]) setTierTime(`${t}-last`, state.tier_status[t]);
    });
  }
  updateTimestamp();
}

// ── Summary strip ────────────────────────────────────────────
function updateSummaryStrip(s) {
  setText('stat-bankroll',   `$${s.bankroll.toFixed(2)}`);
  setText('stat-available',  `$${s.available_capital.toFixed(2)}`);
  setPnl('stat-daily-pnl',   s.daily_pnl);
  setPnl('stat-total-pnl',   s.realized_pnl);
  setText('stat-win-rate',   `${s.win_rate}%`);
  setText('stat-open',       s.open_positions);
  updatePositionCount(s.open_positions);

  const haltBadge = document.getElementById('halt-badge');
  if (haltBadge) haltBadge.classList.toggle('d-none', !s.is_halted);

  const resetBtn = document.getElementById('btn-reset-pnl');
  if (resetBtn) resetBtn.style.display = s.is_halted ? '' : 'none';

  const ksLabel = document.getElementById('ks-label');
  if (ksLabel) ksLabel.textContent = s.kill_switch ? 'Full Stop' : 'Active';
}

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function setPnl(id, val) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = `$${val >= 0 ? '+' : ''}${val.toFixed(2)}`;
  el.className = val >= 0 ? 'wb-stat-value text-success' : 'wb-stat-value text-danger';
}

function updatePositionCount(count) {
  setText('position-count', count);
  const placeholder = document.getElementById('no-positions-placeholder');
  if (placeholder) placeholder.classList.toggle('d-none', count > 0);
}

// ── Carousel ─────────────────────────────────────────────────
function rebuildCarousel(positions) {
  const inner = document.getElementById('carousel-inner');
  if (!inner) return;

  const placeholder = document.getElementById('no-positions-placeholder');
  const prevBtn = document.getElementById('carousel-prev');
  const nextBtn = document.getElementById('carousel-next');

  const entries = Object.entries(positions);
  if (entries.length === 0) {
    inner.innerHTML = '';
    placeholder?.classList.remove('d-none');
    if (prevBtn) prevBtn.style.display = 'none';
    if (nextBtn) nextBtn.style.display = 'none';
    return;
  }

  placeholder?.classList.add('d-none');
  const showNav = entries.length > 1;
  if (prevBtn) prevBtn.style.display = showNav ? '' : 'none';
  if (nextBtn) nextBtn.style.display = showNav ? '' : 'none';

  inner.innerHTML = entries.map(([mid, pos], i) =>
    `<div class="carousel-item${i === 0 ? ' active' : ''}" data-market-id="${mid}">
       ${buildPositionCardInner(mid, pos)}
     </div>`
  ).join('');
}

function buildPositionCardInner(mid, pos) {
  const safeMid   = mid.replace(/-/g, '_');
  const pnlClass  = pos.unrealized_pnl >= 0 ? 'bg-success' : 'bg-danger';
  const pctClass  = pos.pnl_pct >= 0 ? 'text-success' : 'text-danger';
  const sign      = pos.unrealized_pnl >= 0 ? '+' : '';
  const sig       = _signals[pos.station] || {};
  const lowerTail = sig.live_lower_tail !== undefined ? sig.live_lower_tail : 68;
  const upperTail = sig.live_upper_tail !== undefined ? sig.live_upper_tail : 77;
  return `
<div class="wb-position-card card mx-auto">
  <div class="card-body">
    <div class="d-flex justify-content-between align-items-start mb-2">
      <div>
        <span class="fw-bold fs-5">${escHtml(pos.station)}</span>
        <span class="fw-semibold text-muted ms-1">${escHtml(STATION_CITIES[pos.station] || pos.station)}</span>
        <span class="badge bg-primary ms-2">HIGH</span>
        <span class="badge bg-secondary ms-1">${fmtBucket(pos.bucket_lower, lowerTail, upperTail)}</span>
      </div>
      <span class="badge ${pnlClass} fs-6" id="pos-badge-${safeMid}">
        $${sign}${pos.unrealized_pnl.toFixed(2)}
      </span>
    </div>
    <div class="wb-pnl-row mb-3">
      <div class="d-flex justify-content-between align-items-center">
        <div>
          <span class="text-muted small">Entry</span>
          <span class="ms-1 fw-semibold">$${pos.entry_price.toFixed(2)}</span>
          <span class="mx-2 text-muted">→</span>
          <span class="text-muted small">Current</span>
          <span class="ms-1 fw-semibold" id="pos-bid-${safeMid}">$${pos.current_bid.toFixed(2)}</span>
        </div>
        <span class="${pctClass} fw-bold" id="pos-pct-${safeMid}">${pos.pnl_pct >= 0 ? '+' : ''}${pos.pnl_pct.toFixed(1)}%</span>
      </div>
    </div>
    <div class="row g-1 text-muted small mb-3">
      <div class="col-6">Contracts: <span class="text-body">${pos.contracts}</span></div>
      <div class="col-6">Stake: <span class="text-body">$${pos.entry_usd.toFixed(2)}</span></div>
      <div class="col-12">Entered: <span class="text-body">${escHtml(pos.entry_time_display || pos.entry_time)}</span></div>
      <div class="col-12">Market: <span class="text-body">${escHtml(pos.event_date_display || pos.event_date)}</span></div>
    </div>
    <div class="d-flex gap-2">
      <button class="btn btn-sm btn-outline-danger flex-grow-1"
              onclick="confirmClose('${escHtml(mid)}', '${escHtml(pos.station)}', ${pos.unrealized_pnl})">
        Close Position
      </button>
    </div>
  </div>
</div>`;
}

function removeCarouselCard(marketId) {
  const item = document.querySelector(`.carousel-item[data-market-id="${marketId}"]`);
  if (!item) return;
  const wasActive = item.classList.contains('active');
  item.remove();
  if (wasActive) {
    document.querySelector('#carousel-inner .carousel-item')?.classList.add('active');
  }
  const remaining = document.querySelectorAll('#carousel-inner .carousel-item').length;
  updatePositionCount(remaining);
  const showNav = remaining > 1;
  const prevBtn = document.getElementById('carousel-prev');
  const nextBtn = document.getElementById('carousel-next');
  if (prevBtn) prevBtn.style.display = showNav ? '' : 'none';
  if (nextBtn) nextBtn.style.display = showNav ? '' : 'none';
}

function flashSignalCard(station, decision) {
  const card = document.querySelector(`[data-station="${station}"].wb-signal-card`);
  if (!card) return;
  // Update border class
  card.className = card.className.replace(/wb-border-\S+/, `wb-border-${decision.toLowerCase()}`);
  // Brief flash
  card.style.transition = 'opacity 0.15s';
  card.style.opacity = '0.5';
  setTimeout(() => { card.style.opacity = '1'; }, 150);
}

// ── Trade history ────────────────────────────────────────────
function appendTradeHistoryRow(d) {
  const tbody = document.getElementById('trade-history-body');
  if (!tbody) return;

  // Remove "no trades" placeholder if present
  const placeholder = tbody.querySelector('td[colspan]');
  if (placeholder) placeholder.closest('tr').remove();

  const pnl = d.realized_pnl || 0;
  const pnlClass = pnl >= 0 ? 'text-success' : 'text-danger';
  const sign = pnl >= 0 ? '+' : '';

  const bucket = d.bucket_lower != null ? `${d.bucket_lower}°F` : '—';
  const entry  = d.entry_price  != null ? `$${(d.entry_price * 100).toFixed(0)}¢` : '—';
  const exit   = d.exit_price   != null ? `$${(d.exit_price  * 100).toFixed(0)}¢` : '—';

  const row = document.createElement('tr');
  row.innerHTML = `
    <td>${nowStr()}</td>
    <td>${d.station || '—'}</td>
    <td>${bucket}</td>
    <td>${entry}</td>
    <td>${exit}</td>
    <td class="${pnlClass} fw-bold">$${sign}${pnl.toFixed(2)}</td>
    <td class="d-none d-md-table-cell text-muted small">${d.reason || '—'}</td>`;
  tbody.prepend(row);
}

function populateTradeHistory(trades) {
  const tbody = document.getElementById('trade-history-body');
  if (!tbody || !trades || trades.length === 0) return;
  tbody.innerHTML = '';
  trades.forEach(d => {
    const pnl = d.realized_pnl || 0;
    const pnlClass = pnl >= 0 ? 'text-success' : 'text-danger';
    const sign = pnl >= 0 ? '+' : '';
    const bucket = d.bucket_lower != null ? `${d.bucket_lower}°F` : '—';
    const entry  = d.entry_price  != null ? `${(d.entry_price  * 100).toFixed(0)}¢` : '—';
    const exit_p = d.exit_price   != null ? `${(d.exit_price   * 100).toFixed(0)}¢` : '—';
    const row = document.createElement('tr');
    row.innerHTML = `
      <td>${escHtml(d.ts || '—')}</td>
      <td>${escHtml(d.station || '—')}</td>
      <td>${bucket}</td>
      <td>${entry}</td>
      <td>${exit_p}</td>
      <td class="${pnlClass} fw-bold">$${sign}${pnl.toFixed(2)}</td>
      <td class="d-none d-md-table-cell text-muted small">${escHtml(d.reason || '—')}</td>`;
    tbody.appendChild(row);
  });
}

function loadMoreTrades() {
  showToast('Load More', 'Historical trade log coming in a future update.', 'secondary');
}

// ── Alerts ───────────────────────────────────────────────────
function prependAlert(a) {
  const container = document.getElementById('alerts-container');
  if (!container) return;

  const placeholder = document.getElementById('no-alerts-placeholder');
  if (placeholder) placeholder.remove();

  const div = document.createElement('div');
  div.className = `wb-alert-row d-flex gap-2 align-items-start py-2 border-bottom wb-alert-${a.level.toLowerCase()}`;
  div.innerHTML = `
    <span class="wb-alert-level badge wb-badge-${a.level.toLowerCase()} mt-1">${a.level}</span>
    <div class="flex-grow-1">
      <div class="fw-semibold small">${escHtml(a.title)}</div>
      <div class="text-muted small">${escHtml(a.message)}</div>
    </div>
    <span class="text-muted small text-nowrap">${a.timestamp}</span>`;
  container.prepend(div);
}

function clearAlerts() {
  const container = document.getElementById('alerts-container');
  if (container) {
    container.innerHTML = '<div class="text-muted text-center py-3" id="no-alerts-placeholder">No alerts.</div>';
  }
}

function alertBootstrapClass(level) {
  const map = { INFO: 'info', WARNING: 'warning', ERROR: 'danger', CRITICAL: 'danger' };
  return map[level] || 'secondary';
}

// ── Toast notifications ──────────────────────────────────────
function showToast(title, message, type = 'secondary') {
  const container = document.getElementById('toast-container');
  if (!container) return;

  const id   = `toast-${Date.now()}`;
  const html = `
<div id="${id}" class="toast align-items-center text-bg-${type} border-0" role="alert" aria-live="assertive">
  <div class="d-flex">
    <div class="toast-body">
      <strong>${escHtml(title)}</strong><br>
      <span class="small">${escHtml(message)}</span>
    </div>
    <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
  </div>
</div>`;
  container.insertAdjacentHTML('beforeend', html);
  const el    = document.getElementById(id);
  const toast = new bootstrap.Toast(el, { delay: 6000 });
  toast.show();
  el.addEventListener('hidden.bs.toast', () => el.remove());
}

// ── Controls ─────────────────────────────────────────────────
async function setKillSwitch(activate) {
  const endpoint = activate ? '/api/kill-switch/activate' : '/api/kill-switch/deactivate';
  try {
    const res  = await fetch(endpoint, { method: 'POST' });
    const data = await res.json();
    const label = activate ? 'Full Stop' : 'Active';
    showToast('Kill Switch', `Bot is now: ${label}`, activate ? 'danger' : 'success');
  } catch (err) {
    showToast('Error', err.message, 'danger');
  }
}

async function resetDailyPnl() {
  if (!confirm('Reset today\'s P&L and resume trading?')) return;
  try {
    const res  = await fetch('/api/reset-daily-pnl', { method: 'POST' });
    const data = await res.json();
    showToast('Day Reset', 'Daily P&L cleared — bot is now active', 'success');
  } catch (err) {
    showToast('Error', err.message, 'danger');
  }
}

async function runSignalPass() {
  const btn = document.getElementById('btn-signal');
  if (btn) { btn.disabled = true; btn.textContent = '↻ Running…'; }
  try {
    const res  = await fetch('/api/signal-pass', { method: 'POST' });
    const data = await res.json();
    showToast('Signal Pass', data.message || 'Running…', 'primary');
  } catch (err) {
    showToast('Error', err.message, 'danger');
  } finally {
    setTimeout(() => {
      if (btn) { btn.disabled = false; btn.textContent = '↻ Run Signal'; }
    }, 8000);
  }
}

async function rebuildBiasTable() {
  const btn = document.getElementById('btn-rebuild-bias');
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1" role="status"></span>Building…';
  }
  try {
    const res  = await fetch('/api/rebuild-bias', { method: 'POST' });
    const data = await res.json();
    showToast('Bias Rebuild', data.message, 'info');
  } catch (err) {
    showToast('Error', err.message, 'danger');
    if (btn) { btn.disabled = false; btn.textContent = '⟳ Bias'; }
  }
}

// ── Close position ───────────────────────────────────────────
function confirmClose(marketId, station, pnl) {
  pendingCloseMarketId = marketId;
  const stationEl = document.getElementById('close-pos-station');
  const pnlEl     = document.getElementById('close-pos-pnl');
  if (stationEl) stationEl.textContent = `${station} — ${marketId}`;
  if (pnlEl) {
    pnlEl.textContent = `$${pnl >= 0 ? '+' : ''}${pnl.toFixed(4)}`;
    pnlEl.className = pnl >= 0 ? 'text-success' : 'text-danger';
  }
  const modal = bootstrap.Modal.getOrCreateInstance(document.getElementById('modal-close-pos'));
  modal.show();
}

async function executeClose() {
  if (!pendingCloseMarketId) return;
  const mid = pendingCloseMarketId;
  pendingCloseMarketId = null;
  bootstrap.Modal.getInstance(document.getElementById('modal-close-pos'))?.hide();

  try {
    const res  = await fetch(`/api/close-position/${encodeURIComponent(mid)}`, { method: 'POST' });
    const data = await res.json();
    if (data.ok) {
      showToast('Position Closed', `P/L: $${data.realized_pnl >= 0 ? '+' : ''}${data.realized_pnl.toFixed(4)}`,
        data.realized_pnl >= 0 ? 'success' : 'secondary');
    } else {
      showToast('Close Failed', data.error || 'Unknown error', 'danger');
    }
  } catch (err) {
    showToast('Error', err.message, 'danger');
  }
}

async function executeCloseAll() {
  bootstrap.Modal.getInstance(document.getElementById('modal-close-all'))?.hide();
  try {
    const res  = await fetch('/api/close-all', { method: 'POST' });
    const data = await res.json();
    const wins = data.results?.filter(r => r.ok).length || 0;
    showToast('Close All', `${wins} position(s) closed.`, 'warning');
  } catch (err) {
    showToast('Error', err.message, 'danger');
  }
}

// ── Settings ─────────────────────────────────────────────────
async function saveSettings() {
  const inputs  = document.querySelectorAll('.wb-setting-input');
  const payload = {};
  inputs.forEach(inp => { payload[inp.name] = parseFloat(inp.value); });

  try {
    const res  = await fetch('/api/settings', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload),
    });
    const data = await res.json();
    const fb   = document.getElementById('settings-feedback');
    if (data.ok) {
      if (fb) fb.innerHTML = '<div class="text-success small">✓ Settings saved and applied.</div>';
      showToast('Settings', 'Saved and applied live.', 'success');
    } else {
      const errs = (data.errors || []).join('; ');
      if (fb) fb.innerHTML = `<div class="text-danger small">Errors: ${escHtml(errs)}</div>`;
      showToast('Settings Error', errs, 'danger');
    }
  } catch (err) {
    showToast('Error', err.message, 'danger');
  }
}

// ── Utilities ────────────────────────────────────────────────
function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/** Format a bucket lower bound into a human-readable range string. */
function fmtBucket(lower, lowerTail, upperTail) {
  lowerTail = lowerTail !== undefined ? lowerTail : 68;
  upperTail = upperTail !== undefined ? upperTail : 77;
  if (lower === lowerTail) return '≤' + lowerTail + '°F';
  if (lower === upperTail) return '≥' + upperTail + '°F';
  return lower + '–' + (lower + 1) + '°F';
}

function nowStr() {
  return new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function updateTimestamp() {
  const el = document.getElementById('last-updated');
  if (el) el.textContent = `Updated: ${nowStr()}`;
}

// ── Periodic page reload ──────────────────────────────────────
function startAutoRefresh(intervalMs = 5 * 60 * 1000) {
  setInterval(() => location.reload(), intervalMs);
}

// ── Init ─────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  applyLocalTimes();
  initSSE();
  updateTimestamp();
  startAutoRefresh();
});
