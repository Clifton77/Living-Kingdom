"""
Multi-tier APScheduler for the weather trading bot.

Tier 1 — sleep-based, ~5 min after each completion:
    Main trading engine. Fetches METARs + IEM 1-min running max for all
    stations.  Executes exits first (stop-loss, overshoot, undershoot,
    early profit), then entries (fresh Kalshi price vs cached Tier 3
    distribution — first entries, adjacent-bucket expansions, repositions).

    Scheduling note: standard ASOS posts at ~:53-:58 past the hour.
    A fixed IntervalTrigger aligned to :00/:05/... can read obs up to
    12 min stale.  Instead, Tier 1 self-reschedules via DateTrigger
    (TIER1_INTERVAL_SECONDS after its own completion).  The loop drifts
    naturally toward ASOS post times, keeping obs freshness ≤ a few min.

Tier 2 — every 10 minutes (clock-aligned IntervalTrigger):
    TAF amendment monitor.  Detects AMD flags; if found, regenerates the
    signal for that station immediately.  If the new signal flips to
    SKIP/HARD_SKIP or edge on the open bucket inverts, auto-closes the
    position without waiting for the next Tier 1 cycle.

Tier 3 — every 6 hours (aligned to GFS cycles: 00Z, 06Z, 12Z, 18Z + 30min):
    Full signal recompute only.  Classifies 500mb pattern, fetches live
    forecasts, computes bias-adjusted distributions.  Updates
    _latest_signals for Tier 1 to act on.  No orders placed.

Kill switch halts all tiers immediately. Bot can resume from dashboard.
"""

from __future__ import annotations

import dataclasses
import signal
import sys
import threading
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from utils.logging_config import setup_logging
from utils.asos_live import running_max_with_confluence
from utils.sheets import get_sheets_logger
from utils.events import push_event, push_alert

def _push_signal_update(sig) -> None:
    """Push a signal_update SSE event so the dashboard card refreshes immediately."""
    from zoneinfo import ZoneInfo
    _tz  = ZoneInfo(STATION_TIMEZONES[sig.station])
    _now = datetime.now(timezone.utc).astimezone(_tz)
    _local_time_now = _now.strftime("%I:%M %p %Z")
    _p4_action = ""
    _p4s_for_push = _p4_latest_signals.get(sig.station, [])
    _best_p4_for_push = next((s for s in _p4s_for_push if s.action != "PASS"), None)
    if _best_p4_for_push:
        _p4_action = _best_p4_for_push.action

    push_event("signal_update", {
        "station":            sig.station,
        "event_date":         str(sig.event_date),
        "decision":           sig.decision,
        "local_time":         _local_time_now,
        "top_edge":           sig.top_edge,
        "top_bucket":         sig.top_bucket,
        "top_model_prob":     sig.top_model_prob,
        "top_kalshi_prob":    sig.top_kalshi_prob,
        "top_yes_ask":        sig.top_yes_ask,
        "kelly_stake_usd":    sig.kelly_stake_usd,
        "kelly_contracts":    sig.kelly_contracts,
        "forecast_adjusted":  sig.forecast_adjusted,
        "bias_std":           sig.bias_std,
        "model_source":       sig.model_source,
        "skip_reason":        sig.skip_reason,
        "model_divergence_f": sig.model_divergence_f,
        "live_lower_tail":    sig.live_lower_tail,
        "live_upper_tail":    sig.live_upper_tail,
        "cluster_id":         sig.cluster_id,
        "season":             sig.season,
        "n_obs":              sig.n_obs,
        "p4_action":          _p4_action,
        "buckets": [
            {
                "bucket_lower": b.bucket_lower,
                "bucket_label": b.bucket_label,
                "model_prob":   b.model_prob,
                "kalshi_prob":  b.kalshi_prob,
                "edge":         b.edge,
                "yes_ask":      b.yes_ask,
                "yes_bid":      b.yes_bid,
            }
            for b in sig.buckets
        ],
        "metar": {
            "temp_f":      sig.metar.temp_f,
            "dewpoint_f":  sig.metar.dewpoint_f,
            "wind_kt":     sig.metar.wind_kt,
            "sky_cover":   sig.metar.sky_cover,
        } if sig.metar else None,
    })
from utils.dryrun_journal import log_entry as journal_entry, log_snapshot as journal_snapshot, log_exit as journal_exit
from utils.alerting import (
    alert_order_failure,
    alert_reconciliation_mismatch,
    alert_daily_loss_limit,
    alert_settlement_detected,
)
from scripts.signal_engine import run_signal_pass, TradeSignal, BucketAnalysis, check_forecast_availability
from scripts.taf_interpreter import interpret_taf, get_metar
from kalshi_client import KalshiClient, build_market_id
from risk import RiskManager
from utils.peak_hours import get_peak_hour
from config import (
    STATIONS,
    STATION_TIMEZONES,
    KALSHI_BUCKET_LOWER_TAIL,
    KALSHI_BUCKET_UPPER_TAIL,
    KALSHI_BUCKET_STARTS,
    EXPANSION_EDGE_MIN,
    EXPANSION_CURRENT_EDGE_MAX,
    MAX_STATION_POSITIONS,
    SIGNIFICANT_REPOSITION_EDGE_MIN,
    MAJOR_REPOSITION_EDGE_MIN,
    MIN_MARKET_VOLUME,
    MAX_BID_ASK_SPREAD,
    LIQUIDITY_RETRY_INTERVAL_MIN,
    LIQUIDITY_MAX_RETRIES,
    TIER1_INTERVAL_SECONDS,
    TIER1_NEARHR_INTERVAL_SECONDS,
    TIER1_NEARHR_START_MINUTE,
    TIER1_NEARHR_END_MINUTE,
    TIER2_INTERVAL_SECONDS,
    STALE_SIGNAL_HOURS,
    PRICE_SCAN_INTERVAL_MIN,
    SETTLEMENT_SWEEP_UTC_HOUR,
    TIER3_RETRY_INTERVAL_MIN,
    TIER3_MAX_RETRIES,
    DAILY_LOSS_LIMIT_PCT,
    MAX_STAKE_PCT,
    USE_DEMO,
    STARTING_BANKROLL,
    SNAPSHOT_INTERVAL_MIN,
    ENTRY_CUTOFF_PRE_PEAK_HOURS,
    SAME_DAY_ENTRY_OPEN_UTC_HOUR,
    SAME_DAY_ENTRY_OPEN_UTC_MINUTE,
    MIN_EDGE,
    MIN_YES_ASK,
    MAX_YES_ASK,
    MIN_MODEL_PROB_FOR_ENTRY,
    MAX_DAILY_ENTRIES_PER_STATION,
    settlement_station,
)
from phase4_signal_generator import (
    Phase4Forecaster, SignalGenerator, build_phase4_signals,
    _STATION_KELLY_MULT, is_12z_ready,
)
_p4_fetcher  = Phase4Forecaster()
_p4_gen      = SignalGenerator()
_p4_forecasts: dict = {}
_p4_latest_signals: dict[str, list] = {}   # station → list[Phase4Signal], refreshed each price cycle

logger = setup_logging("scheduler")

# Shared state — written by scheduler, read by dashboard
_latest_signals:   dict[str, TradeSignal] = {}
_latest_signals_lock = threading.Lock()

# Running-max cache: populated by exit pass, consumed by entry pass.
# Prevents entering a bucket the temperature has already surpassed.
_running_max_cache: dict[str, tuple[float, datetime]] = {}  # station → (max_f, fetched_at)

# Near-hour obs watcher: tracks fresh hourly METAR arrivals (~:50 past each hour).
# Tier 1 accelerates to 60-second polling from :47 to :05 to catch new obs ASAP.
_last_metar_obs_time:     dict[str, str] = {}    # settle_stn → last DDHHMMz string ("291853Z")
_current_hour_obs_seen:   set[str]       = set() # Kalshi station labels that got fresh obs this window
_near_hour_window_active: bool           = False  # True while inside a near-hour window

_risk_manager: Optional[RiskManager] = None
_kalshi:       Optional[KalshiClient] = None
_scheduler:    Optional[BackgroundScheduler] = None

# Tracks how many Tier 3 retries have fired when NWS data was unavailable
_tier3_retry_counts: dict[str, int] = {}   # key = event_date ISO string

# Tracks liquidity retry attempts per market — reset when entry succeeds or gives up
_liquidity_retry_counts: dict[str, int] = {}   # key = market_id

# Last-run timestamps per tier — read by dashboard
_tier_last_run: dict[str, str] = {
    "tier1":      "never",
    "tier2":      "never",
    "tier3":      "never",
    "settlement": "never",
}

# Per-station entry lock — prevents Tier 1 from double-entering while a
# concurrent fill confirmation is in flight.
_entry_lock         = threading.Lock()
_entry_in_progress: set[str] = set()

# Tracks when each open position last had a snapshot logged to Google Sheets.
# Key = market_id, value = UTC datetime of last snapshot write.
_last_snapshot_time: dict[str, datetime] = {}

# In-memory trade history for the dashboard (both opens and closes, current session only).
_trade_history: list = []

# Daily entry cap: (station, event_date) → count of new position opens.
# Prevents re-entering the same station+market more than MAX_DAILY_ENTRIES_PER_STATION times.
_daily_entry_counts: dict[tuple[str, date], int] = {}
_daily_entry_counts_lock = threading.Lock()

# ── Overnight sleep window ────────────────────────────────────────────────────
# After settlement the bot has nothing to do until 12z models are available.
# Suppress all tier passes from 03:00 UTC (markets settled) to 15:00 UTC
# (15 min before GFS 12z gate at 15:30 UTC), but only when there are no
# open positions — we never sleep with money on the table.
_SLEEP_WINDOW_START = dtime(3, 0)
_SLEEP_WINDOW_END   = dtime(15, 0)


def _in_sleep_window(now_utc: datetime) -> bool:
    """Return True when we're in the overnight dead zone with no open positions."""
    t = now_utc.time().replace(tzinfo=None)
    if not (_SLEEP_WINDOW_START <= t < _SLEEP_WINDOW_END):
        return False
    rm = get_risk_manager()
    return len(rm.state.positions) == 0


def get_trade_history() -> list:
    return list(_trade_history)


def append_trade_history(record: dict) -> None:
    _trade_history.append(record)


def _make_local_ts(station: str) -> str:
    tz = ZoneInfo(STATION_TIMEZONES.get(station, "UTC"))
    now_local = datetime.now(tz)
    h = now_local.hour % 12 or 12
    ampm = "AM" if now_local.hour < 12 else "PM"
    return f"{now_local.strftime('%b')} {now_local.day} {h}:{now_local.strftime('%M')} {ampm} {now_local.strftime('%Z')}"


def _refresh_all_kalshi_prices(kalshi) -> None:
    """
    Refresh Kalshi bucket prices for every active signal without re-running the
    forecast model.  Updates yes_ask/yes_bid/edge in-place, re-ranks the top
    bucket, and pushes a signal_update event so the dashboard reflects fresh
    market prices.  Called at the start of every Tier 1 and Tier 2 pass.
    Tier 3 skips this — its full run_signal_pass already fetches live prices.
    """
    with _latest_signals_lock:
        signals_snapshot = dict(_latest_signals)

    for station, sig in signals_snapshot.items():
        _is_hard_skip = sig.decision == "HARD_SKIP"
        # For non-HARD_SKIP stations we need buckets to update; HARD_SKIP stations
        # still need a snapshot fetch so Phase 4 can evaluate BUY_NO opportunities.
        if not sig.buckets and not _is_hard_skip:
            continue
        try:
            snapshots = kalshi.get_all_snapshots(station, sig.event_date)
            if not snapshots:
                continue

            market_total = sum(s.implied_prob for s in snapshots.values() if s is not None)
            if market_total <= 0:
                continue

            # Phase 4 evaluation always runs — HARD_SKIP weather can validate BUY_NO.
            try:
                _p4_season = getattr(sig, "season", "Summer") or "Summer"
                _p4_sigs = build_phase4_signals(
                    station, snapshots, sig, _p4_gen, _p4_forecasts, _p4_season
                )
                _p4_latest_signals[station] = _p4_sigs
                for _p4s in _p4_sigs:
                    logger.info("[Phase4] %s", _p4s)
                if not _p4_sigs:
                    logger.info("[Phase4] %s: no actionable buckets this cycle", station)
            except Exception as _p4_exc:
                logger.info("[Phase4] %s annotation error: %s", station, _p4_exc)

            # HARD_SKIP: push signal refresh but skip bucket distribution update.
            if _is_hard_skip:
                _push_signal_update(sig)
                continue

            if not sig.buckets:
                continue

            updated: list = []
            for b in sig.buckets:
                snap = snapshots.get(b.bucket_lower)
                if snap is None:
                    continue
                kalshi_p = snap.implied_prob / market_total
                updated.append(BucketAnalysis(
                    bucket_lower=b.bucket_lower,
                    bucket_label=b.bucket_label,
                    model_prob=b.model_prob,
                    kalshi_prob=round(kalshi_p, 4),
                    edge=round(b.model_prob - kalshi_p, 4),
                    yes_ask=snap.yes_ask,
                    yes_bid=snap.yes_bid,
                ))

            if not updated:
                continue

            top = sorted(updated, key=lambda b: b.yes_ask, reverse=True)[0]

            with _latest_signals_lock:
                live_sig = _latest_signals.get(station)
                if live_sig is sig:
                    live_sig.buckets         = updated
                    live_sig.top_bucket      = top.bucket_lower
                    live_sig.top_edge        = top.edge
                    live_sig.top_model_prob  = top.model_prob
                    live_sig.top_kalshi_prob = top.kalshi_prob
                    live_sig.top_yes_ask     = top.yes_ask

            _push_signal_update(sig)
            logger.info(
                "[PriceRefresh] %s: pushed %d buckets | top=%d ask=%.2f edge=%+.3f",
                station, len(updated), top.bucket_lower, top.yes_ask, top.edge,
            )

            # Push carousel price updates for any open positions on this station.
            # The exit loop in Tier 1 does the same but can be skipped by METAR
            # failures or exceptions. Running here (Tier 1 + Tier 2) keeps carousel
            # prices in sync with the station cards without extra API calls.
            _rm = get_risk_manager()
            for _mid, _pos in _rm.state.positions.items():
                if _pos.station != station:
                    continue
                try:
                    _pos_event_date = date.fromisoformat(_pos.event_date)
                except ValueError:
                    continue
                if _pos_event_date != sig.event_date:
                    continue
                _pos_snap = snapshots.get(_pos.bucket_lower)
                if _pos_snap is None:
                    continue
                _is_no_pos = getattr(_pos, "entry_side", "yes") == "no"
                _p_bid = _pos_snap.no_bid if _is_no_pos else _pos_snap.yes_bid
                _p_ask = _pos_snap.no_ask if _is_no_pos else _pos_snap.yes_ask
                if _p_bid is None or _p_ask is None:
                    continue
                _pnl  = round((_p_bid - (_pos.entry_price or 0.0)) * _pos.contracts, 4)
                _ppct = round(_pnl / _pos.entry_usd * 100, 2) if _pos.entry_usd else 0.0
                push_event("position_price_update", {
                    "market_id":      _mid,
                    "station":        station,
                    "bucket_lower":   _pos.bucket_lower,
                    "entry_side":     getattr(_pos, "entry_side", "yes"),
                    "current_bid":    _p_bid,
                    "current_ask":    _p_ask,
                    "yes_ask":        _pos_snap.yes_ask,
                    "unrealized_pnl": _pnl,
                    "pnl_pct":        _ppct,
                })

        except Exception as exc:
            logger.error("[PriceRefresh] %s: %s", station, exc, exc_info=True)


# ---------------------------------------------------------------------------
# Shared state accessors (for dashboard)
# ---------------------------------------------------------------------------

def get_latest_signals() -> dict[str, TradeSignal]:
    with _latest_signals_lock:
        return dict(_latest_signals)

def get_risk_manager() -> RiskManager:
    global _risk_manager
    if _risk_manager is None:
        _risk_manager = RiskManager()
    return _risk_manager

def get_kalshi() -> KalshiClient:
    global _kalshi
    if _kalshi is None:
        _kalshi = KalshiClient(demo=USE_DEMO)
    return _kalshi


# ---------------------------------------------------------------------------
# Tier 2 — TAF amendment monitor + auto-close on signal flip (every 10 min)
# ---------------------------------------------------------------------------

def tier2_taf_monitor():
    """
    Check each station's TAF for amendments every 10 minutes.
    On AMD detection: immediately regenerate the signal.  If the new
    signal flips to SKIP/HARD_SKIP, or the edge on an open bucket inverts
    (model_prob < Kalshi ask), auto-close that position without waiting
    for the next Tier 1 cycle.
    """
    _tier_last_run["tier2"] = datetime.now(timezone.utc).strftime("%H:%M UTC")
    push_event("tier_heartbeat", _tier_last_run)
    logger.info("[Tier2] TAF amendment scan")
    rm     = get_risk_manager()
    kalshi = get_kalshi()

    if rm.is_halted:
        logger.info("[Tier2] Bot halted — skipping")
        return

    now_utc = datetime.now(timezone.utc)
    if _in_sleep_window(now_utc):
        logger.debug("[Tier2] Overnight sleep window — skipping")
        return

    # Refresh Kalshi bucket prices before evaluating TAF conditions
    _refresh_all_kalshi_prices(kalshi)

    for station in STATIONS:
        try:
            # For stations with open positions, fetch TAF with peak-window context
            open_positions = {
                mid: pos for mid, pos in rm.state.positions.items()
                if pos.station == station
            }

            if open_positions:
                # Use the event date from the first open position
                sample_pos = next(iter(open_positions.values()))
                event_date = date.fromisoformat(sample_pos.event_date)
                peak_hour_local = get_peak_hour(station, event_date)
                taf = interpret_taf(station, event_date=event_date, peak_hour_local=peak_hour_local)
            else:
                taf = interpret_taf(station)

            # Push TAF condition to dashboard for every station every Tier 2 cycle
            push_event("taf_update", {
                "station":   station,
                "sky_cover": taf.sky_cover,
                "condition": taf.condition,
                "has_amd":   taf.has_amd,
                "summary":   taf.summary or "",
            })

            if not taf.has_amd:
                continue

            logger.warning("[Tier2] AMD at %s — %s", station, taf.summary)
            _single_station_signal_pass(station)

            with _latest_signals_lock:
                sig = _latest_signals.get(station)

            if not open_positions:
                continue

            for market_id, pos in open_positions.items():
                # Skip if already closed by a concurrent Tier 1 run
                if market_id not in rm.state.positions:
                    continue

                if sig is None:
                    continue

                # Check peak-window TAF condition for in-trade protection
                # peak_window_utc is (start, end) or (None, None) when unavailable
                has_peak_window = bool(taf.peak_window_utc and taf.peak_window_utc[0])
                peak_window_dangerous = (
                    has_peak_window
                    and taf.condition in ("hard_skip", "precip", "convective")
                )

                # Close if: peak-window weather dangerous, signal prohibits trade,
                # or model edge inverted on held bucket
                skip_signal = sig.decision in ("SKIP", "HARD_SKIP") or peak_window_dangerous
                edge_inverted = (
                    sig.top_bucket == pos.bucket_lower and sig.top_edge < 0.0
                )

                if peak_window_dangerous:
                    logger.warning(
                        "[Tier2] %s AMD: dangerous peak-window condition '%s' detected — auto-close",
                        station, taf.condition,
                    )

                if not (skip_signal or edge_inverted):
                    logger.info(
                        "[Tier2] %s AMD processed — signal still %s (edge=%+.3f), no auto-close",
                        station, sig.decision, sig.top_edge,
                    )
                    # If AMD strengthened or confirmed a TRADE signal, attempt
                    # entry immediately rather than waiting for the next Tier 1 cycle.
                    if sig.decision == "TRADE":
                        with _entry_lock:
                            if station not in _entry_in_progress:
                                _entry_in_progress.add(station)
                                try:
                                    _tier1_entry_pass(
                                        station, date.fromisoformat(pos.event_date),
                                        datetime.now(timezone.utc), rm, kalshi,
                                    )
                                except Exception as e_exc:
                                    logger.error("[Tier2] Entry pass after AMD failed for %s: %s", station, e_exc)
                                finally:
                                    _entry_in_progress.discard(station)
                    continue

                snap = kalshi.get_market_snapshot(
                    station, date.fromisoformat(pos.event_date), pos.bucket_lower
                )
                if snap is None:
                    logger.warning("[Tier2] Cannot fetch snapshot for %s — skipping auto-close", market_id)
                    continue

                if peak_window_dangerous:
                    reason = (
                        f"TAF AMD auto-close: peak-window condition='{taf.condition}' "
                        f"detected near forecast high (edge={sig.top_edge:+.3f})"
                    )
                elif skip_signal:
                    reason = (
                        f"TAF AMD auto-close: signal→{sig.decision} "
                        f"(edge={sig.top_edge:+.3f})"
                    )
                else:
                    reason = (
                        f"TAF AMD auto-close: edge inverted on bucket {pos.bucket_lower}°F "
                        f"(edge={sig.top_edge:+.3f})"
                    )
                logger.warning("[Tier2] Auto-closing %s — %s", market_id, reason)
                _t2_bid = snap.no_bid if getattr(pos, "entry_side", "yes") == "no" else snap.yes_bid
                _execute_exit(market_id, pos, _t2_bid, reason, kalshi, rm)

                # Block Tier 1 re-entry until the next Tier 3 run recomputes the signal.
                # Write a HARD_SKIP into _latest_signals so Tier 1 sees it immediately.
                with _latest_signals_lock:
                    current = _latest_signals.get(station)
                    if current is not None:
                        updated = dataclasses.replace(
                            current,
                            decision="HARD_SKIP",
                            weather_gate="hard_skip",
                            skip_reason="TAF amendment — weather gate forces skip; re-entry blocked until next Tier 3 run.",
                        )
                        _latest_signals[station] = updated
                        logger.info(
                            "[Tier2] %s signal overwritten to HARD_SKIP — "
                            "Tier 1 re-entry blocked until next Tier 3 run",
                            station,
                        )
                        _push_signal_update(updated)

        except Exception as exc:
            logger.error("[Tier2] Error scanning %s: %s", station, exc)

    push_event("state_update", get_risk_manager().summary())


def _single_station_signal_pass(station: str):
    """Re-run signal generation for one station (called on AMD detection or stale signal)."""
    try:
        from scripts.pattern_classifier import classify_pattern
        from scripts.signal_engine import generate_signal, _load_bias_table

        now_utc    = datetime.now(timezone.utc)
        event_date = _get_entry_event_date(station, now_utc) or date.today()
        bias_df    = _load_bias_table()
        pattern    = classify_pattern(event_date)
        kalshi     = get_kalshi()
        rm         = get_risk_manager()

        _p4_fc = _p4_forecasts.get(station)
        sig = generate_signal(
            station, event_date, kalshi, bias_df, pattern, rm.state.bankroll,
            p4_forecast_f=_p4_fc.blended_f if _p4_fc is not None else None,
            p4_model=_p4_fc.preferred_model if _p4_fc is not None else "PHASE4",
            p4_station_kelly_mult=_STATION_KELLY_MULT.get(station, 1.0),
            p4_gfs_raw=_p4_fc.gfs_f if _p4_fc is not None else None,
            p4_ecmwf_raw=_p4_fc.ecmwf_f if _p4_fc is not None else None,
        )

        with _latest_signals_lock:
            _latest_signals[station] = sig

        _push_signal_update(sig)
        push_event("state_update", get_risk_manager().summary())
        logger.info("[SignalRefresh] %s: %s | edge=%+.3f", station, sig.decision, sig.top_edge)

    except Exception as exc:
        logger.error("[SignalRefresh] Failed for %s: %s", station, exc)


# ---------------------------------------------------------------------------
# Entry date routing — same-day markets only
# ---------------------------------------------------------------------------

def _get_entry_event_date(station: str, now_utc: datetime) -> date | None:
    """
    Return today's date if the station is in its entry window, else None.

    Entry window: after 12:30 UTC (12Z Tier 3 model run) up to ENTRY_CUTOFF_PRE_PEAK_HOURS
    before the station's local peak hour.  Only same-day markets are traded.
    """
    tz          = ZoneInfo(STATION_TIMEZONES[station])
    now_local   = now_utc.astimezone(tz)
    today_local = now_local.date()

    # Gate 1: 12Z model run must have fired (12:30 UTC = 7:30 AM CT)
    open_utc = now_utc.replace(
        hour=SAME_DAY_ENTRY_OPEN_UTC_HOUR,
        minute=SAME_DAY_ENTRY_OPEN_UTC_MINUTE,
        second=0, microsecond=0,
    )
    if now_utc < open_utc:
        return None   # too early — wait for 12Z Tier 3 run

    # Gate 2: must be before peak cutoff
    peak_hour    = get_peak_hour(station, today_local)
    cutoff_local = now_local.replace(
        hour=peak_hour, minute=0, second=0, microsecond=0
    ) - timedelta(hours=ENTRY_CUTOFF_PRE_PEAK_HOURS)

    if now_local < cutoff_local:
        return today_local   # same-day entry window open

    return None   # past cutoff — done for today


# ---------------------------------------------------------------------------
# Tier 1 — METAR + exits + entries (sleep-based, ~5 min after completion)
# ---------------------------------------------------------------------------

def _is_near_top_of_hour(now_utc: datetime) -> bool:
    """True from :47 to :05 — the window when hourly ASOS obs are expected."""
    m = now_utc.minute
    return m >= TIER1_NEARHR_START_MINUTE or m < TIER1_NEARHR_END_MINUTE


def _reschedule_tier1() -> None:
    """
    Schedule the next Tier 1 run.

    Near the top of each hour (:47–:05), switches to a 60-second interval
    until all stations have received a fresh hourly METAR obs, then reverts
    to the normal 5-minute interval.  Called in a finally block so the
    interval is measured from completion, not a fixed clock.
    """
    global _scheduler
    if _scheduler is None or not _scheduler.running:
        return
    now   = datetime.now(timezone.utc)
    near  = _is_near_top_of_hour(now)
    fresh = len(_current_hour_obs_seen)
    total = len(STATIONS)
    if near and fresh < total:
        interval = TIER1_NEARHR_INTERVAL_SECONDS
        logger.info("[ObsWatcher] Near-hour fast poll (%d/%d fresh) — next in %ds", fresh, total, interval)
    else:
        interval = TIER1_INTERVAL_SECONDS
    run_at = now + timedelta(seconds=interval)
    _scheduler.add_job(
        tier1_metar_entries_exits,
        trigger=DateTrigger(run_date=run_at, timezone="UTC"),
        id="tier1_metar",
        name="METAR + Entries + Exits",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    logger.debug("[Tier1] Next run scheduled for %s UTC (%ds)", run_at.strftime("%H:%M"), interval)


def _handle_closed_market(market_id: str, pos, kalshi, rm, now_utc: datetime) -> None:
    """
    Called when get_market_snapshot() returns None or is_open=False for an open position.

    Tries Kalshi's settlements API immediately.  If Kalshi has confirmed the result,
    closes the position with the official settlement value.  If not yet available,
    marks the position pending_settlement=True so the dashboard can show
    "Settled (pending reconciliation)" until the morning LCD sweep finalises it.
    """
    pos_date = date.fromisoformat(pos.event_date)
    try:
        settlements = kalshi.get_settled_markets(pos_date)
    except Exception as exc:
        logger.warning("[Tier1] Settlement lookup failed for %s: %s", market_id, exc)
        settlements = {}

    if market_id in settlements:
        settlement_value = settlements[market_id]
        close_reason = f"Intraday settlement — Kalshi confirmed at ${settlement_value:.2f}"
        sheets = get_sheets_logger()
        realized = rm.close_position(market_id, exit_price=settlement_value, reason=close_reason)
        sheets.log_trade_closed(pos.station, market_id, settlement_value, realized, close_reason)
        push_event("position_closed", {"market_id": market_id, "realized_pnl": round(realized, 4)})
        push_event("state_update", rm.summary())
        logger.info("[Tier1] %s settled intraday | value=%.2f | P/L $%+.4f",
                    market_id, settlement_value, realized)
        return

    # Kalshi hasn't settled yet — market closed intraday (e.g. temp moved past bucket).
    # Mark pending so the dashboard can show the estimated payout.
    if not pos.pending_settlement:
        pos.pending_settlement = True
        rm._save_state()
        logger.info("[Tier1] %s market closed, not yet settled — marked pending", market_id)

    push_event("position_price_update", {
        "market_id":          market_id,
        "station":            pos.station,
        "bucket_lower":       pos.bucket_lower,
        "entry_side":         getattr(pos, "entry_side", "yes"),
        "current_bid":        pos.current_bid,
        "current_ask":        pos.current_ask,
        "unrealized_pnl":     round(pos.unrealized_pnl, 4),
        "pnl_pct":            round(pos.pnl_pct, 2),
        "pending_settlement": True,
    })


def tier1_metar_entries_exits():
    """
    ~5 min sleep-based cycle (post-ASOS-aligned):
      Pass 1 — Exits:  for each open position, evaluate stop-loss / overshoot /
                        undershoot / early profit using fresh METAR + IEM running max.
      Pass 2 — Entries: for each station with a TRADE signal and fresh Kalshi
                        prices, execute first entries, adjacent-bucket expansions,
                        or repositions as warranted.

    Self-reschedules via _reschedule_tier1().
    """
    try:
        _tier_last_run["tier1"] = datetime.now(timezone.utc).strftime("%H:%M UTC")
        push_event("tier_heartbeat", _tier_last_run)
        logger.info("[Tier1] METAR + entries + exits cycle")
        rm     = get_risk_manager()
        kalshi = get_kalshi()

        if rm.is_halted:
            logger.info("[Tier1] Bot halted — skipping")
            return

        now_utc = datetime.now(timezone.utc)

        if _in_sleep_window(now_utc):
            logger.debug("[Tier1] Overnight sleep window — skipping")
            return

        # ── Near-hour obs window tracking ────────────────────────────────────
        global _near_hour_window_active, _current_hour_obs_seen
        near = _is_near_top_of_hour(now_utc)
        if near and not _near_hour_window_active:
            _near_hour_window_active = True
            _current_hour_obs_seen.clear()
            logger.info("[ObsWatcher] Entering near-hour window at %s UTC — fast obs polling begins",
                        now_utc.strftime("%H:%M"))
        elif not near and _near_hour_window_active:
            _near_hour_window_active = False
            _current_hour_obs_seen.clear()
            logger.info("[ObsWatcher] Exiting near-hour window — reverting to %ds poll", TIER1_INTERVAL_SECONDS)

        # ── Price refresh — update all bucket prices before exit/entry logic ─
        _refresh_all_kalshi_prices(kalshi)

        # ── Pass 1: exits ────────────────────────────────────────────────────
        for station in STATIONS:
            try:
                settle   = settlement_station(station)
                metar    = get_metar(settle)
                obs_temp = metar.temp_f

                metar_ok = obs_temp is not None and obs_temp > -50.0
                if not metar_ok:
                    logger.warning("[Tier1] %s METAR failed (temp=%s, settle=%s) — intraday guards disabled, price updates continue",
                                   station, obs_temp, settle)
                    obs_temp = None

                # Push METAR to dashboard only when valid
                if metar_ok:
                    push_event("metar_update", {
                        "station":    station,
                        "temp_f":     obs_temp,
                        "wind_kt":    metar.wind_kt,
                        "dewpoint_f": metar.dewpoint_f,
                        "sky_cover":  metar.sky_cover or "—",
                        "obs_time":   metar.obs_time,
                    })
                    # Keep sig.metar current
                    with _latest_signals_lock:
                        live_sig = _latest_signals.get(station)
                        if live_sig is not None:
                            live_sig.metar = metar

                    # ── Near-hour new-obs detection ──────────────────────────
                    if near and metar.obs_time:
                        _prev_obs = _last_metar_obs_time.get(settle)
                        try:
                            _obs_hour        = int(metar.obs_time[2:4])
                            _is_current_hour = (_obs_hour == now_utc.hour)
                        except (IndexError, ValueError):
                            _is_current_hour = False
                        if _is_current_hour and metar.obs_time != _prev_obs:
                            _last_metar_obs_time[settle] = metar.obs_time
                            _current_hour_obs_seen.add(station)
                            logger.info("[ObsWatcher] NEW hourly obs %s: %s (was %s) — temp %.1f°F",
                                        station, metar.obs_time, _prev_obs or "—", obs_temp)

                station_positions = {
                    mid: pos for mid, pos in rm.state.positions.items()
                    if pos.station == station
                }
                if not station_positions:
                    continue

                local_now  = now_utc.astimezone(ZoneInfo(STATION_TIMEZONES[station]))
                local_hour = local_now.hour

                try:
                    rm_data     = running_max_with_confluence(settle, now_utc.date())
                    running_max = rm_data["running_max_f"]
                    _running_max_cache[station] = (running_max, now_utc)
                    if not rm_data["in_confluence"]:
                        logger.warning("[Tier1] %s temp confluence issue (settle=%s): %s", station, settle, rm_data["note"])
                except Exception as _rm_exc:
                    logger.warning("[Tier1] %s running_max fetch failed: %s — using cached", station, _rm_exc)
                    _cached_rm = _running_max_cache.get(station)
                    running_max = _cached_rm[0] if _cached_rm else None

                # obs_temp requires live METAR; running_max uses IEM 1-min data independently
                if not metar_ok:
                    obs_temp = None

                for market_id, pos in station_positions.items():
                    snap = kalshi.get_market_snapshot(
                        station, date.fromisoformat(pos.event_date), pos.bucket_lower
                    )
                    if snap is None or not snap.is_open:
                        _handle_closed_market(market_id, pos, kalshi, rm, now_utc)
                        continue

                    with _latest_signals_lock:
                        sig = _latest_signals.get(station)

                    if sig is not None:
                        age_hours = (
                            datetime.now(timezone.utc) - sig.signal_generated_at
                        ).total_seconds() / 3600
                        if age_hours > STALE_SIGNAL_HOURS:
                            logger.info(
                                "[Tier1] Signal for %s is %.1fh old — refreshing",
                                station, age_hours,
                            )
                            _single_station_signal_pass(station)
                            with _latest_signals_lock:
                                sig = _latest_signals.get(station)

                    current_edge      = sig.top_edge if sig and sig.top_bucket == pos.bucket_lower else 0.0
                    pos_event_date    = date.fromisoformat(pos.event_date)
                    peak_heating_hour = get_peak_hour(station, pos_event_date)

                    # Intraday guards (undershoot/overshoot) require today's running max
                    # and local hour.  Parse market date from the Kalshi ticker
                    # (e.g. "26APR26") as the authoritative source — pos.event_date
                    # can be stale if a signal refresh changed event_date mid-cycle.
                    try:
                        _ticker_date_str = market_id.split("-")[1]
                        _market_date = datetime.strptime(_ticker_date_str, "%y%b%d").date()
                    except (IndexError, ValueError):
                        _market_date = pos_event_date
                    pos_is_today = (_market_date == now_utc.date())
                    _is_no = getattr(pos, "entry_side", "yes") == "no"
                    _cur_bid = snap.no_bid if _is_no else snap.yes_bid
                    _cur_ask = snap.no_ask if _is_no else snap.yes_ask

                    exit_decision = rm.update_position(
                        market_id=market_id,
                        current_bid=_cur_bid,
                        current_ask=_cur_ask,
                        current_edge=current_edge,
                        current_obs_temp=obs_temp        if pos_is_today else None,
                        running_max=running_max          if pos_is_today else None,
                        local_hour=local_hour            if pos_is_today else None,
                        peak_heating_hour=peak_heating_hour if pos_is_today else None,
                    )

                    log_level = (
                        logger.warning if exit_decision.urgency in ("immediate", "warning")
                        else logger.info
                    )
                    log_level(
                        "[Tier1] %s %s bid=%.2f P/L=$%+.4f (%.1f%%) | [%s] %s",
                        market_id, pos.entry_side.upper(), _cur_bid,
                        pos.unrealized_pnl, pos.pnl_pct,
                        exit_decision.urgency.upper(),
                        exit_decision.reason,
                    )

                    # Push live price update to dashboard on every cycle
                    push_event("position_price_update", {
                        "market_id":     market_id,
                        "station":       station,
                        "bucket_lower":  pos.bucket_lower,
                        "entry_side":    getattr(pos, "entry_side", "yes"),
                        "current_bid":   _cur_bid,
                        "current_ask":   _cur_ask,
                        "yes_ask":       snap.yes_ask,   # always YES side for station card display
                        "unrealized_pnl": round(pos.unrealized_pnl, 4),
                        "pnl_pct":       round(pos.pnl_pct, 2),
                    })

                    if exit_decision.should_exit:
                        _execute_exit(market_id, pos, _cur_bid, exit_decision.reason, kalshi, rm)
                        _last_snapshot_time.pop(market_id, None)
                        # After undershoot (peak passed), the day is over — block re-entry.
                        if exit_decision.exit_type == "undershoot":
                            with _latest_signals_lock:
                                current_sig = _latest_signals.get(station)
                                if current_sig is not None:
                                    updated_sig = dataclasses.replace(
                                        current_sig,
                                        decision="HARD_SKIP",
                                        weather_gate="hard_skip",
                                        skip_reason="Peak hour passed — undershoot exit fired, re-entry blocked for today.",
                                    )
                                    _latest_signals[station] = updated_sig
                                    _push_signal_update(updated_sig)
                            logger.info(
                                "[Tier1] %s → HARD_SKIP after undershoot exit — "
                                "re-entry blocked until next Tier 3 run",
                                station,
                            )
                    else:
                        if exit_decision.urgency == "warning":
                            logger.warning(
                                "[Tier1] UNDERSHOOT WARNING on %s — manual close available on dashboard",
                                market_id,
                            )
                        # Type 3 NO obs-trajectory signal (log-only until Type 1+2 validated)
                        if (getattr(pos, "entry_side", "yes") == "yes"
                                and local_hour is not None and local_hour >= 11
                                and running_max is not None and peak_heating_hour is not None):
                            _expected_remaining = (peak_heating_hour - local_hour) * 1.5
                            _rm_gap = pos.bucket_lower - running_max
                            _t3_snap = kalshi.get_market_snapshot(
                                station, sig.event_date, pos.bucket_lower
                            ) if sig else None
                            if (_rm_gap > _expected_remaining
                                    and _t3_snap and _t3_snap.yes_ask >= 0.20):
                                logger.info(
                                    "[Tier1] %s TYPE3_NO signal: rm=%.1f bucket=%d "
                                    "gap=%.1f>expected=%.1f yes_ask=%.2f — "
                                    "log only, pending Type 1+2 validation",
                                    station, running_max, pos.bucket_lower,
                                    _rm_gap, _expected_remaining, _t3_snap.yes_ask,
                                )

                        # Log intraday snapshot if SNAPSHOT_INTERVAL_MIN has elapsed
                        last_snap = _last_snapshot_time.get(market_id)
                        if last_snap is None or (now_utc - last_snap).total_seconds() >= SNAPSHOT_INTERVAL_MIN * 60:
                            hours_held = (
                                (now_utc - datetime.fromisoformat(pos.entry_time)).total_seconds() / 3600
                                if pos.entry_time
                                else 0.0
                            )
                            get_sheets_logger().log_position_snapshot(
                                station=station,
                                market_id=market_id,
                                bucket_lower=pos.bucket_lower,
                                local_hour=local_hour,
                                obs_temp=obs_temp,
                                running_max=running_max,
                                yes_bid=snap.yes_bid,
                                yes_ask=snap.yes_ask,
                                edge=current_edge,
                                unrealized_pnl=pos.unrealized_pnl,
                                pnl_pct=pos.pnl_pct,
                                hours_since_entry=hours_held,
                            )
                            journal_snapshot(
                                station=station,
                                market_id=market_id,
                                bucket_lower=pos.bucket_lower,
                                yes_bid=snap.yes_bid,
                                yes_ask=snap.yes_ask,
                                model_prob=sig.top_model_prob if sig and sig.top_bucket == pos.bucket_lower else 0.0,
                                edge=current_edge,
                                running_max=running_max,
                                local_hour=local_hour,
                            )
                            _last_snapshot_time[market_id] = now_utc

            except Exception as exc:
                logger.error("[Tier1] Exit pass error at %s: %s", station, exc, exc_info=True)

        if near and len(_current_hour_obs_seen) == len(STATIONS):
            logger.info("[ObsWatcher] All %d stations have fresh hourly obs — next cycle reverts to %ds",
                        len(STATIONS), TIER1_INTERVAL_SECONDS)

        # ── Pass 2: entries ──────────────────────────────────────────────────
        entered = 0
        for station in STATIONS:
            try:
                target_date = _get_entry_event_date(station, now_utc)
                if target_date is None:
                    logger.debug("[Tier1] %s — in gap window, no entry target", station)
                    continue

                # Entry lock — prevents double-entry if a previous cycle's fill
                # confirmation is still in flight.
                with _entry_lock:
                    if station in _entry_in_progress:
                        continue
                    _entry_in_progress.add(station)

                try:
                    _tier1_entry_pass(
                        station, target_date, now_utc, rm, kalshi
                    )
                finally:
                    with _entry_lock:
                        _entry_in_progress.discard(station)

            except Exception as exc:
                logger.error("[Tier1] Entry pass error at %s: %s", station, exc)
                with _entry_lock:
                    _entry_in_progress.discard(station)

        push_event("state_update", rm.summary())
        logger.info("[Tier1] Cycle complete")

    finally:
        _reschedule_tier1()


def _tier1_entry_pass(station: str, event_date, now_utc, rm, kalshi):
    """
    Entry logic for one station in the Tier 1 cycle.
    Handles first entries, adjacent-bucket expansions, and repositions
    using the latest signal from _latest_signals (set by Tier 3).
    """
    with _latest_signals_lock:
        sig = _latest_signals.get(station)

    if sig is None:
        return

    # Guard: signal must be for today's event date.
    # Mismatches happen when _latest_signals holds a stale signal from a prior day.
    if sig.event_date != event_date:
        logger.debug(
            "[Tier1] %s signal date %s ≠ target %s — skipping entry",
            station, sig.event_date, event_date,
        )
        return

    # No trades on weather prohibits or stale model data.
    # Exception: HARD_SKIP stations can still trade BUY_NO — bad weather validates NO.
    if sig.decision == "HARD_SKIP":
        _no_sigs_hs = [s for s in _p4_latest_signals.get(station, []) if s.action == "BUY_NO"]
        if not _no_sigs_hs:
            return
        # Fall through — Phase 4 BUY_NO is valid despite weather HARD_SKIP
    age_hours = (now_utc - sig.signal_generated_at).total_seconds() / 3600
    if age_hours > STALE_SIGNAL_HOURS:
        logger.debug("[Tier1] %s signal %.1fh old — skipping entry", station, age_hours)
        return

    # SKIP and CONSTRAINED are never actionable
    if sig.decision in ("SKIP", "CONSTRAINED"):
        return

    # Block new entries on same-day markets once the station's local peak hour
    # has passed. Tier 3 regenerates TRADE signals without knowing the peak has
    # passed, which causes an immediate undershoot → re-entry loop.
    # Compare against the station's LOCAL date (not UTC) so western stations
    # behave correctly when their local date lags UTC.
    station_now   = now_utc.astimezone(ZoneInfo(STATION_TIMEZONES[station]))
    station_date  = station_now.date()
    station_hour  = station_now.hour
    if event_date == station_date:
        peak_hr = get_peak_hour(station, event_date)
        if station_hour >= peak_hr:
            logger.info(
                "[Tier1] %s same-day entry blocked — local hour %02dh ≥ peak %02dh",
                station, station_hour, peak_hr,
            )
            return

    existing = rm.station_positions(station)

    # ── Existing position routing (TRADE signals only) ────────────────────
    if existing:
        existing_pos = existing[0]
        dist = _bucket_distance(existing_pos.bucket_lower, sig.top_bucket)

        if dist == 0:
            return  # already in this bucket

        snap_check = kalshi.get_market_snapshot(station, sig.event_date, sig.top_bucket)
        if snap_check is None or not snap_check.is_open:
            return

        market_id = snap_check.market_id  # use API ticker (avoids B68 vs T68 mismatch)
        fresh_edge_check = sig.top_model_prob - snap_check.yes_ask
        push_event("kalshi_top_update", {
            "station":        station,
            "top_bucket":     sig.top_bucket,
            "yes_ask":        snap_check.yes_ask,
            "yes_bid":        snap_check.yes_bid,
            "fresh_edge":     round(fresh_edge_check, 4),
            "top_model_prob": sig.top_model_prob,
        })

        if dist == 1:
            expansion_decision = _evaluate_expansion(
                existing_pos=existing_pos,
                new_sig=sig,
                rm=rm,
                event_date=sig.event_date,
            )
            if expansion_decision["eligible"]:
                _execute_expansion(existing_pos, sig, market_id, sig.event_date,
                                   expansion_decision, kalshi, rm)
            else:
                logger.info("[Tier1] %s expansion ineligible: %s",
                            station, expansion_decision["reason"])
        else:
            required_edge = (
                SIGNIFICANT_REPOSITION_EDGE_MIN if dist == 2
                else MAJOR_REPOSITION_EDGE_MIN
            )
            reposition_ok, repo_reason = _evaluate_reposition(
                existing_pos=existing_pos,
                new_sig=sig,
                required_edge=required_edge,
                rm=rm,
                event_date=sig.event_date,
            )
            if reposition_ok:
                _execute_reposition(existing_pos, sig, market_id, required_edge,
                                    dist, sig.event_date, kalshi, rm)
            else:
                logger.info("[Tier1] %s reposition blocked (dist=%d): %s",
                            station, dist, repo_reason)
        return

    # ── New position — skip if at station limit ───────────────────────────
    if len(existing) >= MAX_STATION_POSITIONS:
        return

    # ── 12z model gate: hold all entries until 12z GFS is confirmed on Open-Meteo ──
    # Universal gate: 15:30 UTC for all stations (GFS 12z ready).
    # ECMWF 12z (18:30 UTC) would lock out Eastern/Central stations entirely
    # — their entry windows close before ECMWF disseminates.
    # The 19:25 UTC ECMWF refresh upgrades data silently; it does not gate entries.
    # Also verifies _p4_forecasts was fetched after 15:30 (not stale pre-12z data).
    _p4_fc_gate = _p4_forecasts.get(station)
    _model_pref_gate = _p4_fc_gate.preferred_model if _p4_fc_gate else "ECMWF"
    if not is_12z_ready(_model_pref_gate, now_utc):
        logger.debug(
            "[Tier1] %s holding — 12z %s not yet available (%.0f:%02.0f UTC)",
            station, _model_pref_gate,
            now_utc.hour, now_utc.minute,
        )
        return
    if _p4_fc_gate is None or not is_12z_ready(_model_pref_gate, _p4_fc_gate.fetched_at):
        logger.info(
            "[Tier1] %s holding — Phase4 data pre-dates 12z %s cutoff (fetched %s UTC)",
            station, _model_pref_gate,
            _p4_fc_gate.fetched_at.strftime("%H:%M") if _p4_fc_gate else "never",
        )
        return

    # ── Phase 4 gate: sole trade-validity check for new entries ──────────
    # BUY_YES: model's top bucket must be priced 40–70¢ (study: +0.034/+0.081 edge).
    # BUY_NO:  price 5–30¢  (study: 93.75% win rate; +5.7¢ avg edge per contract).
    #
    # Selection logic: start from the model's top bucket, then check if Phase 4
    # validates it (price in 40–70¢ zone). This avoids the previous approach of
    # picking an arbitrary BUY_YES bucket and post-hoc checking model alignment.
    _p4_entry = None
    _model_top_for_entry = None
    if sig.buckets:
        _model_top_for_entry = max(sig.buckets, key=lambda b: b.model_prob)
        _p4_entry = next(
            (s for s in _p4_latest_signals.get(station, [])
             if s.action == "BUY_YES" and s.bucket_lower == _model_top_for_entry.bucket_lower),
            None,
        )

    if _p4_entry is None:
        # No Phase 4 BUY_YES on model's top bucket — check for BUY_NO
        _no_signals = [s for s in _p4_latest_signals.get(station, []) if s.action == "BUY_NO"]
        _p4_no = max(_no_signals, key=lambda s: s.yes_ask, default=None)
        if _p4_no is None:
            logger.info("[Tier1] %s Phase4 PASS — no actionable bucket", station)
            return
        entry_bucket = _p4_no.bucket_lower
        entry_side   = "no"
        logger.info(
            "[Tier1] %s Phase4 BUY_NO B%d yes_ask=%.2f (no_cost=%.2f)",
            station, entry_bucket, _p4_no.yes_ask, 1 - _p4_no.yes_ask,
        )
    else:
        entry_bucket = _p4_entry.bucket_lower
        entry_side   = "yes"

    if entry_side == "yes":
        # Gate 1: model Gaussian must assign ≥30% probability to the entry bucket
        _entry_model_prob = _model_top_for_entry.model_prob if _model_top_for_entry else 0.0
        if _entry_model_prob < MIN_MODEL_PROB_FOR_ENTRY:
            logger.info(
                "[Tier1] %s Phase4 BUY_YES B%d skipped — model_prob %.3f < %.2f floor",
                station, entry_bucket, _entry_model_prob, MIN_MODEL_PROB_FOR_ENTRY,
            )
            return

        logger.info(
            "[Tier1] %s Phase4 %s B%d ask=%.2f conf=%.3f model_prob=%.3f",
            station, _p4_entry.action, entry_bucket, _p4_entry.yes_ask,
            _p4_entry.confidence, _entry_model_prob,
        )

    snap = kalshi.get_market_snapshot(station, sig.event_date, entry_bucket)
    if snap is None or not snap.is_open:
        return

    # Use the correct bid/ask side for this entry type
    _e_ask = snap.no_ask if entry_side == "no" else snap.yes_ask
    _e_bid = snap.no_bid if entry_side == "no" else snap.yes_bid

    push_event("kalshi_top_update", {
        "station":        station,
        "top_bucket":     entry_bucket,
        "yes_ask":        snap.yes_ask,
        "yes_bid":        snap.yes_bid,
        "fresh_edge":     round(sig.top_model_prob - snap.yes_ask, 4),
        "top_model_prob": sig.top_model_prob,
    })

    # Liquidity guard
    spread    = _e_ask - _e_bid
    spread_ok = spread <= MAX_BID_ASK_SPREAD
    volume_ok = snap.volume >= MIN_MARKET_VOLUME

    if not (spread_ok and volume_ok):
        issues = []
        if not spread_ok:
            issues.append(f"spread={spread:.2f}")
        if not volume_ok:
            issues.append(f"vol={snap.volume}")
        logger.info("[Tier1] %s illiquid (%s) — scheduling liquidity retry",
                    station, ", ".join(issues))
        _schedule_liquidity_retry(
            station, sig.event_date.isoformat(), entry_bucket, LIQUIDITY_RETRY_INTERVAL_MIN
        )
        return

    # Price bounds validated by Phase 4 (40–70¢ BUY_YES zone).
    # MIN_YES_ASK / MAX_YES_ASK guards removed — they would block the
    # 60–70¢ strong-buy zone that has the highest study edge (+0.081).

    # Daily entry cap: limit re-entries per station per market date
    with _daily_entry_counts_lock:
        _entry_key = (station, event_date)
        if _daily_entry_counts.get(_entry_key, 0) >= MAX_DAILY_ENTRIES_PER_STATION:
            logger.info(
                "[Tier1] %s daily entry cap reached (%d/%d for %s) — skip",
                station,
                _daily_entry_counts[_entry_key],
                MAX_DAILY_ENTRIES_PER_STATION,
                event_date,
            )
            return

    # Running-max guard: observed temp has already surpassed the bucket ceiling
    _rm_cached = _running_max_cache.get(station)
    if _rm_cached is not None and (now_utc - _rm_cached[1]).total_seconds() < 600:
        entry_running_max = _rm_cached[0]
    else:
        try:
            _rm_fresh = running_max_with_confluence(station, sig.event_date)
            entry_running_max = _rm_fresh["running_max_f"]
            _running_max_cache[station] = (entry_running_max, now_utc)
        except Exception:
            entry_running_max = None

    if entry_running_max is not None:
        if entry_bucket == KALSHI_BUCKET_LOWER_TAIL:
            bucket_ceiling = KALSHI_BUCKET_LOWER_TAIL + 1
        elif entry_bucket == KALSHI_BUCKET_UPPER_TAIL:
            bucket_ceiling = None
        else:
            bucket_ceiling = entry_bucket + 2

        if bucket_ceiling is not None and entry_running_max >= bucket_ceiling:
            logger.info(
                "[Tier1] %s running_max %.1f°F ≥ bucket ceiling %.0f°F (bucket %d) — skip",
                station, entry_running_max, bucket_ceiling, entry_bucket,
            )
            return

    fresh_edge = sig.top_model_prob - snap.yes_ask
    push_event("kalshi_top_update", {
        "station":        station,
        "top_bucket":     entry_bucket,
        "yes_ask":        snap.yes_ask,
        "yes_bid":        snap.yes_bid,
        "fresh_edge":     round(fresh_edge, 4),
        "top_model_prob": sig.top_model_prob,
    })
    # Size primary entry. NO positions only get half-Kelly (higher win rate but
    # per-contract cost is 70–95¢, so 2% bankroll = 2 contracts max).
    import math as _math
    splits = 2 if (entry_side == "yes" and _dual_entry_eligible(sig)) else 1
    primary_budget = round(rm.state.bankroll * MAX_STAKE_PCT / splits, 2)

    ok, reason = rm.can_open_position(primary_budget, station=station)
    if not ok:
        logger.info("[Tier1] %s risk gate: %s", station, reason)
        return

    max_price = _e_ask  # no_ask for NO positions, yes_ask for YES positions
    live_contracts = int(_math.floor(primary_budget / max_price)) if max_price > 0 else 0
    if live_contracts < 1:
        logger.info(
            "[Tier1] %s primary budget $%.2f yields 0 contracts at live ask $%.2f — skip",
            station, primary_budget, max_price,
        )
        return
    live_stake = round(live_contracts * max_price, 4)

    market_id = snap.market_id  # use API ticker (avoids B68 vs T68 mismatch)
    result = kalshi.place_order_with_fill_check(
        market_id=market_id,
        contracts=live_contracts,
        limit_price=max_price,
        side=entry_side,
    )

    if result.success:
        rm.open_position(
            station=station,
            market_id=market_id,
            bucket_lower=entry_bucket,
            contracts=live_contracts,
            entry_price=max_price,
            event_date=sig.event_date,
            entry_side=entry_side,
        )
        get_sheets_logger().log_trade_opened(
            station=station,
            event_date=sig.event_date,
            market_id=market_id,
            bucket_lower=entry_bucket,
            entry_price=max_price,
            contracts=live_contracts,
            stake_usd=live_stake,
            sig=sig,
        )
        journal_entry(
            station=station,
            market_id=market_id,
            bucket_lower=entry_bucket,
            entry_price=max_price,
            contracts=live_contracts,
            stake_usd=live_stake,
            model_prob=sig.top_model_prob,
            edge=fresh_edge,
            forecast_adjusted=sig.forecast_adjusted,
            sig=sig,
            entry_side=entry_side,
        )
        open_record = {
            "ts":           _make_local_ts(station),
            "type":         "OPEN",
            "market_id":    market_id,
            "station":      station,
            "bucket_lower": entry_bucket,
            "entry_price":  max_price,
            "contracts":    live_contracts,
            "stake_usd":    live_stake,
            "event_date":   str(sig.event_date),
            "entry_reason": "tier1",
            "entry_side":   entry_side,
        }
        _trade_history.append(open_record)
        push_event("position_opened", open_record)
        push_event("state_update", rm.summary())
        with _daily_entry_counts_lock:
            _entry_key = (station, event_date)
            _daily_entry_counts[_entry_key] = _daily_entry_counts.get(_entry_key, 0) + 1
        logger.info(
            "[Tier1] Entry: %s | bucket %d | %d contracts @ $%.2f | "
            "edge %+.3f | stake $%.2f",
            station, entry_bucket, live_contracts, max_price,
            fresh_edge, live_stake,
        )
        _attempt_dual_entry(station, sig, now_utc, rm, kalshi)
    else:
        logger.error("[Tier1] Order failed for %s bucket %d: %s",
                     station, entry_bucket, result.error)
        alert_order_failure(station, market_id, result.error or "unknown")


def _dual_entry_eligible(sig) -> bool:
    """True when the signal's top two buckets meet all dual-entry conditions."""
    if not sig.buckets or len(sig.buckets) < 2:
        return False
    ranked = sorted(sig.buckets, key=lambda b: b.yes_ask, reverse=True)
    first, second = ranked[0], ranked[1]
    if first.bucket_lower in (KALSHI_BUCKET_LOWER_TAIL, KALSHI_BUCKET_UPPER_TAIL):
        return False
    if second.bucket_lower in (KALSHI_BUCKET_LOWER_TAIL, KALSHI_BUCKET_UPPER_TAIL):
        return False
    gap = first.yes_ask - second.yes_ask
    return (
        first.yes_ask <= 0.35
        and gap <= 0.10
        and abs(first.bucket_lower - second.bucket_lower) == 2
    )


def _attempt_dual_entry(station: str, sig, now_utc: datetime, rm, kalshi):
    """
    Enter the second-ranked Kalshi bucket when _dual_entry_eligible() is True.
    Primary + secondary together equal 2% of bankroll (1% each).
    """
    import math as _math

    if not _dual_entry_eligible(sig):
        return
    if len(rm.station_positions(station)) >= MAX_STATION_POSITIONS:
        return

    ranked = sorted(sig.buckets, key=lambda b: b.yes_ask, reverse=True)
    first, second = ranked[0], ranked[1]
    gap = first.yes_ask - second.yes_ask

    snap2 = kalshi.get_market_snapshot(station, sig.event_date, second.bucket_lower)
    if snap2 is None or not snap2.is_open or snap2.yes_ask < MIN_YES_ASK:
        return

    spread2 = snap2.yes_ask - snap2.yes_bid
    if spread2 > MAX_BID_ASK_SPREAD or snap2.volume < MIN_MARKET_VOLUME:
        return

    # Running-max guard: skip if intraday observed high has already cleared the bucket ceiling
    _rm_cached = _running_max_cache.get(station)
    if _rm_cached and (now_utc - _rm_cached[1]).total_seconds() < 600:
        entry_rm = _rm_cached[0]
        if second.bucket_lower not in (KALSHI_BUCKET_LOWER_TAIL, KALSHI_BUCKET_UPPER_TAIL):
            if entry_rm >= second.bucket_lower + 2:
                logger.info(
                    "[Tier1] %s dual-entry: bucket %d running_max %.1f°F ≥ ceiling — skip",
                    station, second.bucket_lower, entry_rm,
                )
                return

    stake_budget = round(rm.state.bankroll * MAX_STAKE_PCT / 2, 2)
    ok, risk_reason = rm.can_open_position(stake_budget, station=station)
    if not ok:
        logger.info("[Tier1] %s dual-entry: risk gate: %s", station, risk_reason)
        return

    fresh_edge2  = second.model_prob - snap2.yes_ask
    max_price2   = snap2.yes_ask
    live_contracts2 = int(_math.floor(stake_budget / max_price2)) if max_price2 > 0 else 0
    if live_contracts2 < 1:
        return
    live_stake2 = round(live_contracts2 * max_price2, 4)

    market_id2 = snap2.market_id
    result2 = kalshi.place_order_with_fill_check(
        market_id=market_id2,
        contracts=live_contracts2,
        limit_price=max_price2,
        side="yes",
    )

    if result2.success:
        rm.open_position(
            station=station, market_id=market_id2,
            bucket_lower=second.bucket_lower, contracts=live_contracts2,
            entry_price=max_price2, event_date=sig.event_date,
        )
        get_sheets_logger().log_trade_opened(
            station=station, event_date=sig.event_date, market_id=market_id2,
            bucket_lower=second.bucket_lower, entry_price=max_price2,
            contracts=live_contracts2, stake_usd=live_stake2, sig=sig,
        )
        journal_entry(
            station=station, market_id=market_id2, bucket_lower=second.bucket_lower,
            entry_price=max_price2, contracts=live_contracts2, stake_usd=live_stake2,
            model_prob=second.model_prob, edge=fresh_edge2,
            forecast_adjusted=sig.forecast_adjusted, sig=sig,
        )
        open_record2 = {
            "ts":           _make_local_ts(station),
            "type":         "OPEN",
            "market_id":    market_id2,
            "station":      station,
            "bucket_lower": second.bucket_lower,
            "entry_price":  max_price2,
            "contracts":    live_contracts2,
            "stake_usd":    live_stake2,
            "event_date":   str(sig.event_date),
            "entry_reason": "dual_entry",
        }
        _trade_history.append(open_record2)
        push_event("position_opened", open_record2)
        push_event("state_update", rm.summary())
        logger.info(
            "[Tier1] Dual entry: %s | bucket %d | %d contracts @ $%.2f | "
            "gap=%.2f second_ask=%.2f | stake $%.2f",
            station, second.bucket_lower, live_contracts2, max_price2,
            gap, second.yes_ask, live_stake2,
        )
    else:
        logger.info("[Tier1] Dual entry order failed for %s bucket %d: %s",
                    station, second.bucket_lower, result2.error)


def _execute_exit(market_id, pos, bid_price, reason, kalshi, rm):
    """Place sell order and record close."""
    logger.info("[Exit] Executing: %s | side=%s | reason: %s", market_id, pos.entry_side, reason)

    result = kalshi.close_position(market_id, pos.contracts, bid_price, entry_side=pos.entry_side)
    if result.success:
        realized = rm.close_position(market_id, bid_price, reason)
        get_sheets_logger().log_trade_closed(pos.station, market_id, bid_price, realized, reason)
        journal_exit(
            station=pos.station,
            market_id=market_id,
            bucket_lower=pos.bucket_lower,
            exit_price=bid_price,
            entry_price=pos.entry_price,
            contracts=pos.contracts,
            reason=reason,
        )
        mode = "DEMO" if USE_DEMO else "LIVE"
        get_sheets_logger().update_dashboard(rm.summary(), mode=mode)
        closed_record = {
            "ts":           _make_local_ts(pos.station),
            "type":         "CLOSE",
            "market_id":    market_id,
            "station":      pos.station,
            "bucket_lower": pos.bucket_lower,
            "contracts":    pos.contracts,
            "entry_price":  pos.entry_price,
            "exit_price":   bid_price,
            "realized_pnl": realized,
            "reason":       reason,
        }
        _trade_history.append(closed_record)
        push_event("position_closed", closed_record)
        push_event("state_update", rm.summary())
        logger.info("[Exit] Complete: %s | realized P/L $%+.4f", market_id, realized)
    else:
        logger.error("[Exit] Order failed for %s: %s", market_id, result.error)
        push_alert(f"Exit failed — {market_id}", result.error or "unknown", "ERROR")




# ---------------------------------------------------------------------------
# Phase 4 forecast refresh — runs after 12z GFS (15:25 UTC) and 12z ECMWF
# (19:25 UTC) to ensure _p4_forecasts holds post-12z data before the Tier 1
# gate opens.  Does not recompute the full signal pass.
# ---------------------------------------------------------------------------

def _phase4_refresh():
    """Re-fetch GFS + ECMWF forecasts from Open-Meteo and update _p4_forecasts."""
    global _p4_forecasts
    now_utc = datetime.now(timezone.utc)
    try:
        fresh = _p4_fetcher.fetch_all(date.today())
        _p4_forecasts.update(fresh)
        logger.info(
            "[Phase4] 12z refresh complete at %s UTC — %d stations updated",
            now_utc.strftime("%H:%M"), len(fresh),
        )
    except Exception as exc:
        logger.warning("[Phase4] 12z refresh failed: %s", exc)


# ---------------------------------------------------------------------------
# Tier 3 — Full signal recompute only (every 6 hours, no order execution)
# ---------------------------------------------------------------------------

def tier3_full_signal_pass(event_date: date | None = None):
    """
    Full signal recompute for all stations.  Updates _latest_signals so
    Tier 1 can act on fresh distributions at the next 5-min cycle.
    No orders are placed here — all trade execution is Tier 1's job.

    Only same-day markets are targeted.  event_date is always None from cron
    calls; each station's target date is determined by _get_entry_event_date().

    Before running:
      1. Probe forecast availability (NWS/Open-Meteo). If unavailable,
         schedule a retry job and return — don't run on stale data.
      2. Check daily loss limit alert threshold.
    """
    logger.info("[Tier3] Full signal recompute starting")
    rm = get_risk_manager()

    # Purge daily entry counts older than 2 days to avoid memory growth
    with _daily_entry_counts_lock:
        cutoff = datetime.now(timezone.utc).date() - timedelta(days=2)
        stale  = [k for k in _daily_entry_counts if k[1] < cutoff]
        for k in stale:
            del _daily_entry_counts[k]

    # Sync bankroll from live Kalshi balance — picks up deposits/withdrawals
    # without requiring a bot restart.
    try:
        kalshi = get_kalshi()
        live_balance = kalshi.get_balance()
        if live_balance > 0 and abs(live_balance - rm.state.bankroll) > 0.01:
            logger.info(
                "[Tier3] Bankroll synced from Kalshi: $%.2f → $%.2f",
                rm.state.bankroll, live_balance,
            )
            rm.state.bankroll = live_balance
            rm._save_state()
    except Exception as exc:
        logger.warning("[Tier3] Bankroll sync failed: %s", exc)

    if rm.is_halted:
        logger.info("[Tier3] Bot halted — skipping signal recompute")
        if not rm.state.kill_switch_active:
            limit = rm.state.bankroll * DAILY_LOSS_LIMIT_PCT
            alert_daily_loss_limit(rm.state.daily_pnl, limit)
        return

    now_utc = datetime.now(timezone.utc)
    if _in_sleep_window(now_utc):
        logger.info("[Tier3] Overnight sleep window — skipping signal recompute")
        return

    from scripts.pattern_classifier import classify_pattern
    from scripts.signal_engine import generate_signal, _load_bias_table
    import time as _time

    now_utc    = datetime.now(timezone.utc)
    probe_date = date.today()

    # ── Forecast availability probe ───────────────────────────────────────
    avail = check_forecast_availability(probe_date)
    if not avail.get("available"):
        retry_count = _tier3_retry_counts.get(probe_date.isoformat(), 0)
        if retry_count < TIER3_MAX_RETRIES:
            logger.warning(
                "[Tier3] Forecast unavailable (%s) — scheduling retry %d/%d in %d min",
                avail.get("details", "unknown"), retry_count + 1,
                TIER3_MAX_RETRIES, TIER3_RETRY_INTERVAL_MIN,
            )
            _schedule_tier3_retry(TIER3_RETRY_INTERVAL_MIN, probe_date)
        else:
            logger.error(
                "[Tier3] Forecast still unavailable after %d retries — skipping this cycle",
                TIER3_MAX_RETRIES,
            )
            _tier3_retry_counts.pop(probe_date.isoformat(), None)
        return

    _tier3_retry_counts.pop(probe_date.isoformat(), None)
    _tier_last_run["tier3"] = now_utc.strftime("%H:%M UTC")
    push_event("tier_heartbeat", _tier_last_run)

    try:
        _p4_forecasts.update(_p4_fetcher.fetch_all(probe_date))
        logger.info("[Phase4] Fetched %d station forecasts for %s", len(_p4_forecasts), probe_date)
    except Exception as _p4_exc:
        logger.warning("[Phase4] fetch_all failed: %s", _p4_exc)
    bias_df = _load_bias_table()
    pattern = classify_pattern(probe_date)
    kalshi  = get_kalshi()
    signals: dict[str, TradeSignal] = {}

    for idx, station in enumerate(STATIONS):
        if idx > 0:
            _time.sleep(3)   # pace Open-Meteo free-tier (20 req/min)
        # Refresh timestamp per station — the loop takes ~3 min and station entry
        # windows can open/close mid-loop.
        target_date = _get_entry_event_date(station, datetime.now(timezone.utc))
        if target_date is None:
            logger.info("[Tier3] %s — in gap window, skipping signal", station)
            continue
        try:
            _p4_fc = _p4_forecasts.get(station)
            sig = generate_signal(
                station, target_date, kalshi, bias_df, pattern, rm.state.bankroll,
                p4_forecast_f=_p4_fc.blended_f if _p4_fc is not None else None,
                p4_model=_p4_fc.preferred_model if _p4_fc is not None else "PHASE4",
                p4_station_kelly_mult=_STATION_KELLY_MULT.get(station, 1.0),
                p4_gfs_raw=_p4_fc.gfs_f if _p4_fc is not None else None,
                p4_ecmwf_raw=_p4_fc.ecmwf_f if _p4_fc is not None else None,
            )
            signals[station] = sig
        except Exception as exc:
            logger.error("[Tier3] Signal failed for %s: %s", station, exc, exc_info=True)

    # Store updated distributions — Tier 1 reads these on every 5-min cycle
    with _latest_signals_lock:
        _latest_signals.update(signals)
    for sig in signals.values():
        _push_signal_update(sig)

    # Log WATCH/SKIP/HARD_SKIP decisions to Sheets for review
    sheets = get_sheets_logger()
    for sig in signals.values():
        if sig.decision in ("WATCH", "SKIP", "HARD_SKIP"):
            sheets.log_skipped_signal(sig, sig.decision)

    summary = rm.summary()
    push_event("state_update", summary)
    sheets.update_dashboard(summary, mode="DEMO" if USE_DEMO else "LIVE")
    trade_count = sum(1 for s in signals.values() if s.decision == "TRADE")
    logger.info(
        "[Tier3] Recompute complete | %d TRADE / %d total | "
        "bankroll=$%.2f | open=%d | Tier 1 will act within ~5 min",
        trade_count, len(signals),
        summary["bankroll"], summary["open_positions"],
    )


# ---------------------------------------------------------------------------
# Adjacent bucket expansion helpers
# ---------------------------------------------------------------------------

# Ordered bucket list — adjacency is determined by index distance of 1
_ALL_BUCKETS = [
    KALSHI_BUCKET_LOWER_TAIL,
    *KALSHI_BUCKET_STARTS,
    KALSHI_BUCKET_UPPER_TAIL,
]


def _buckets_adjacent(a: int, b: int) -> bool:
    """True if buckets a and b are exactly one step apart in the Kalshi ladder."""
    try:
        return abs(_ALL_BUCKETS.index(a) - _ALL_BUCKETS.index(b)) == 1
    except ValueError:
        return False


def _bucket_distance(a: int, b: int) -> int:
    """
    Number of ladder steps between buckets a and b.
    Returns 0 if either bucket is unknown.
    Used to route forecast shifts: 1=expansion, 2=significant, 3+=major reposition.
    """
    try:
        return abs(_ALL_BUCKETS.index(a) - _ALL_BUCKETS.index(b))
    except ValueError:
        return 0


def _evaluate_expansion(
    existing_pos,
    new_sig,
    rm: RiskManager,
    event_date: date,
) -> dict:
    """
    Evaluate whether the new signal qualifies as an adjacent-bucket expansion.

    Returns a dict with:
        eligible  : bool
        reason    : human-readable explanation (always populated for the card)
        guardrails: dict of each check and its result (for dashboard card)
    """
    station     = existing_pos.station
    old_bucket  = existing_pos.bucket_lower
    new_bucket  = new_sig.top_bucket
    new_edge    = new_sig.top_edge
    old_edge    = existing_pos.last_edge   # last edge stored on position

    # Current local time and event-day awareness
    tz          = ZoneInfo(STATION_TIMEZONES[station])
    local_now   = datetime.now(tz)
    local_hour  = local_now.hour
    is_event_day = (date.today() == event_date)
    peak_hour    = get_peak_hour(station, event_date)

    # Before peak hour: always true on Day -1; time-checked on event day
    before_peak = (not is_event_day) or (local_hour < peak_hour)

    checks = {
        "adjacent_bucket":    _buckets_adjacent(old_bucket, new_bucket),
        "new_edge_sufficient": new_edge >= EXPANSION_EDGE_MIN,
        "old_edge_degraded":   old_edge <= EXPANSION_CURRENT_EDGE_MAX,
        "before_peak_hour":    before_peak,
        "expansion_allowed":   rm.can_expand_station(station)[0],
    }

    ok, expand_reason = rm.can_expand_station(station)
    checks["expansion_allowed"] = ok

    eligible = all(checks.values())

    if eligible:
        reason = (
            f"Adjacent expansion: {old_bucket}°F edge degraded to {old_edge:+.3f} "
            f"(≤ {EXPANSION_CURRENT_EDGE_MAX}). New bucket {new_bucket}°F edge "
            f"{new_edge:+.3f} (≥ {EXPANSION_EDGE_MIN}). "
            f"{'Day-1 window' if not is_event_day else f'Event day, {local_hour:02d}h < peak {peak_hour:02d}h'}. "
            f"Holding {old_bucket}°F — both positions open, loser exits automatically."
        )
    else:
        failed = [k for k, v in checks.items() if not v]
        reason = f"Expansion blocked — failed: {', '.join(failed)}"
        if not ok:
            reason += f" ({expand_reason})"

    return {"eligible": eligible, "reason": reason, "guardrails": checks}


def _evaluate_reposition(
    existing_pos,
    new_sig,
    required_edge: float,
    rm: RiskManager,
    event_date: date,
) -> tuple[bool, str]:
    """
    Evaluate whether a significant (2-step) or major (3+step) reposition is allowed.

    Conditions:
      - New bucket edge ≥ required_edge (0.22 for 2-step, 0.25 for 3+step)
      - Old position edge has degraded (last_edge ≤ EXPANSION_CURRENT_EDGE_MAX)
      - Before peak heating hour (no repositioning after peak has passed)
      - Station is NOT reversal-blocked (blocks new entry, not reposition close)

    Returns (allowed: bool, reason: str).
    """
    station    = existing_pos.station
    old_bucket = existing_pos.bucket_lower
    new_bucket = new_sig.top_bucket
    new_edge   = new_sig.top_edge
    old_edge   = existing_pos.last_edge

    tz         = ZoneInfo(STATION_TIMEZONES[station])
    local_hour = datetime.now(tz).hour
    is_event_day = (date.today() == event_date)
    peak_hour    = get_peak_hour(station, event_date)
    before_peak  = (not is_event_day) or (local_hour < peak_hour)

    if not before_peak:
        return False, f"Past peak hour ({local_hour:02d}h ≥ {peak_hour:02d}h) — no reposition after peak"

    if new_edge < required_edge:
        return False, (
            f"New bucket {new_bucket}°F edge {new_edge:+.3f} < required {required_edge:+.3f} "
            f"for {'2-step' if required_edge == SIGNIFICANT_REPOSITION_EDGE_MIN else '3+-step'} reposition"
        )

    if old_edge > EXPANSION_CURRENT_EDGE_MAX:
        return False, (
            f"Old bucket {old_bucket}°F still has edge {old_edge:+.3f} "
            f"(> {EXPANSION_CURRENT_EDGE_MAX}) — not degraded enough to reposition"
        )

    # Note: reversal block only prevents NEW independent entries, not repositions
    # that close the old position first. We intentionally skip that check here.
    return True, "OK"


def _execute_reposition(
    existing_pos,
    new_sig,
    new_market_id: str,
    required_edge: float,
    dist: int,
    event_date: date,
    kalshi: KalshiClient,
    rm: RiskManager,
):
    """
    Close the old position (without triggering reversal block) and
    open a new one in the shifted bucket.

    The close reason is deliberately worded to avoid the "reversal" keyword
    so the station remains eligible for re-entry via the new position.
    """
    station    = new_sig.station
    old_bucket = existing_pos.bucket_lower
    new_bucket = new_sig.top_bucket
    label      = "Significant" if dist == 2 else "Major"

    logger.info(
        "[Tier3] %s REPOSITION (%s, %d-step): %d°F → %d°F | new_edge=%+.3f (min=%.2f)",
        station, label, dist, old_bucket, new_bucket, new_sig.top_edge, required_edge,
    )

    # ── Step 1: Close old position ────────────────────────────────────────
    old_snap = kalshi.get_market_snapshot(station, event_date, old_bucket)
    if old_snap is None:
        logger.error("[Tier3] Cannot fetch old market snapshot for %s bucket %d — aborting reposition", station, old_bucket)
        return

    close_result = kalshi.close_position(
        existing_pos.market_id,
        existing_pos.contracts,
        old_snap.yes_bid,
    )

    if not close_result.success:
        logger.error(
            "[Tier3] Failed to close old position %s before reposition: %s",
            existing_pos.market_id, close_result.error,
        )
        return

    # Record close — reason avoids "reversal" so station stays unblocked
    close_reason = (
        f"{label} forecast reposition: model shifted {dist} buckets "
        f"({old_bucket}°F → {new_bucket}°F). Closing old position to open new."
    )
    realized = rm.close_position(existing_pos.market_id, old_snap.yes_bid, close_reason)
    get_sheets_logger().log_trade_closed(station, existing_pos.market_id, old_snap.yes_bid, realized, close_reason)
    logger.info(
        "[Tier3] Old position closed | %s | realized P/L $%+.4f",
        existing_pos.market_id, realized,
    )

    # ── Step 2: Open new position ─────────────────────────────────────────
    ok, reason = rm.can_open_position(new_sig.kelly_stake_usd, station=station)
    if not ok:
        logger.warning("[Tier3] Reposition open blocked for %s: %s", station, reason)
        return

    max_price = new_sig.top_yes_ask

    result = kalshi.place_order_with_fill_check(
        market_id=new_market_id,
        contracts=new_sig.kelly_contracts,
        limit_price=max_price,
        side="yes",
    )

    if result.success:
        rm.open_position(
            station=station,
            market_id=new_market_id,
            bucket_lower=new_bucket,
            contracts=new_sig.kelly_contracts,
            entry_price=max_price,
            event_date=event_date,
        )
        note = (
            f"{label} reposition ({dist}-step): forecast shifted from {old_bucket}°F "
            f"to {new_bucket}°F bucket. Old position closed at ${old_snap.yes_bid:.2f} "
            f"(P/L ${realized:+.4f}). New position opened at ${max_price:.2f}."
        )
        if new_sig.reasoning:
            new_sig.reasoning.expansion_note = note
        with _latest_signals_lock:
            _latest_signals[station] = new_sig
        logger.info("[Tier3] Reposition complete: %s → %s", existing_pos.market_id, new_market_id)
    else:
        logger.error(
            "[Tier3] Reposition new-entry order failed for %s: %s",
            new_market_id, result.error,
        )
        alert_order_failure(station, new_market_id, result.error or "unknown")


def _execute_expansion(
    existing_pos,
    sig,
    market_id: str,
    event_date: date,
    expansion_decision: dict,
    kalshi: KalshiClient,
    rm: RiskManager,
):
    """Place order for the new adjacent bucket and record the expansion."""
    station = sig.station
    logger.info(
        "[Tier3] EXPANSION %s → bucket %d°F | edge=%+.3f | %s",
        station, sig.top_bucket, sig.top_edge, expansion_decision["reason"],
    )

    ok, reason = rm.can_open_position(sig.kelly_stake_usd, station=station)
    if not ok:
        logger.warning("[Tier3] Expansion exposure check failed for %s: %s", station, reason)
        return

    result = kalshi.place_order(
        market_id=market_id,
        contracts=sig.kelly_contracts,
        limit_price=sig.top_yes_ask,
        side="yes",
    )

    if result.success:
        rm.open_position(
            station=station,
            market_id=market_id,
            bucket_lower=sig.top_bucket,
            contracts=sig.kelly_contracts,
            entry_price=sig.top_yes_ask,
            event_date=event_date,
        )
        rm.record_expansion(station)

        logger.info(
            "[Tier3] Expansion complete: %s | new=%s @ $%.2f | "
            "holding %s @ $%.2f | combined exposure $%.2f",
            station, market_id, sig.top_yes_ask,
            existing_pos.market_id, existing_pos.entry_price,
            rm.total_exposure(),
        )

        # Store expansion note in reasoning for dashboard card
        if sig.reasoning:
            sig.reasoning.expansion_note      = expansion_decision["reason"]
            sig.reasoning.expansion_guardrails = expansion_decision.get("guardrails", {})
        with _latest_signals_lock:
            _latest_signals[station] = sig
    else:
        logger.error("[Tier3] Expansion order failed for %s: %s", market_id, result.error)


# ---------------------------------------------------------------------------
# Liquidity retry — re-checks spread/volume for a signal that was ready but
# the market book hadn't filled in yet at entry time
# ---------------------------------------------------------------------------

def _schedule_liquidity_retry(
    station: str,
    event_date_iso: str,
    bucket_lower: int,
    delay_min: int,
):
    """
    Schedule a one-shot DateTrigger job to re-attempt entry once the market
    has had time to attract liquidity. Increments the per-market retry counter.
    """
    global _scheduler
    if _scheduler is None:
        logger.error("[LiqRetry] Scheduler not initialized — cannot schedule retry")
        return

    event_date = date.fromisoformat(event_date_iso)
    market_id  = build_market_id(station, event_date, bucket_lower)
    attempt    = _liquidity_retry_counts.get(market_id, 0) + 1
    _liquidity_retry_counts[market_id] = attempt

    run_at = datetime.now(timezone.utc) + timedelta(minutes=delay_min)
    job_id = f"liq_retry_{market_id}_{attempt}"

    _scheduler.add_job(
        _attempt_liquidity_entry,
        trigger=DateTrigger(run_date=run_at, timezone="UTC"),
        args=[station, event_date_iso, bucket_lower],
        id=job_id,
        name=f"Liquidity Retry {attempt}/{LIQUIDITY_MAX_RETRIES} — {market_id}",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    logger.info(
        "[LiqRetry] Retry %d/%d scheduled for %s at %s UTC",
        attempt, LIQUIDITY_MAX_RETRIES, market_id,
        run_at.strftime("%H:%M"),
    )


def _attempt_liquidity_entry(station: str, event_date_iso: str, bucket_lower: int):
    """
    Liquidity retry job — called by the DateTrigger scheduled above.

    Flow:
      1. Already in this position? Done.
      2. Re-run signal to confirm still TRADE for this bucket (freshness check).
      3. Re-check spread and volume.
         - Still illiquid + retries remaining → schedule another retry.
         - Still illiquid + retries exhausted → final skip, log to Sheets.
         - Liquid → place order with full entry logic.
    """
    rm         = get_risk_manager()
    kalshi     = get_kalshi()
    event_date = date.fromisoformat(event_date_iso)
    market_id  = build_market_id(station, event_date, bucket_lower)
    attempt    = _liquidity_retry_counts.get(market_id, 1)

    logger.info(
        "[LiqRetry] Attempt %d/%d — %s", attempt, LIQUIDITY_MAX_RETRIES, market_id
    )

    # Already entered? (e.g. manual entry or another trigger beat us to it)
    if market_id in rm.state.positions:
        logger.info("[LiqRetry] Already in %s — cancelling retries", market_id)
        _liquidity_retry_counts.pop(market_id, None)
        return

    # Re-run signal — confirms edge is still valid and data is fresh
    _single_station_signal_pass(station)
    with _latest_signals_lock:
        sig = _latest_signals.get(station)

    if sig is None or sig.decision != "TRADE" or sig.top_bucket != bucket_lower:
        decision = sig.decision if sig else "None"
        logger.info(
            "[LiqRetry] %s signal no longer TRADE for bucket %d (now: %s) — final skip",
            station, bucket_lower, decision,
        )
        if sig:
            get_sheets_logger().log_skipped_signal(
                sig, f"Liquidity retry {attempt}: signal shifted away from bucket {bucket_lower}"
            )
        _liquidity_retry_counts.pop(market_id, None)
        return

    # Re-check market
    snap = kalshi.get_market_snapshot(station, event_date, bucket_lower)
    if snap is None or not snap.is_open:
        logger.info("[LiqRetry] %s market closed — final skip", market_id)
        _liquidity_retry_counts.pop(market_id, None)
        return

    spread    = snap.yes_ask - snap.yes_bid
    spread_ok = spread <= MAX_BID_ASK_SPREAD
    volume_ok = snap.volume >= MIN_MARKET_VOLUME

    if not (spread_ok and volume_ok):
        if attempt < LIQUIDITY_MAX_RETRIES:
            issues = []
            if not spread_ok:
                issues.append(f"spread={spread:.2f}")
            if not volume_ok:
                issues.append(f"vol={snap.volume}")
            logger.info(
                "[LiqRetry] %s still illiquid (%s) — retry %d/%d in %d min",
                market_id, ", ".join(issues),
                attempt + 1, LIQUIDITY_MAX_RETRIES, LIQUIDITY_RETRY_INTERVAL_MIN,
            )
            _schedule_liquidity_retry(station, event_date_iso, bucket_lower, LIQUIDITY_RETRY_INTERVAL_MIN)
        else:
            final_reason = (
                f"Liquidity retry exhausted after {LIQUIDITY_MAX_RETRIES} attempts "
                f"({LIQUIDITY_MAX_RETRIES * LIQUIDITY_RETRY_INTERVAL_MIN} min window): "
                f"spread={spread:.2f}, volume={snap.volume}"
            )
            logger.info("[LiqRetry] %s — %s", market_id, final_reason)
            sig.decision = "SKIP"
            get_sheets_logger().log_skipped_signal(sig, final_reason)
            _liquidity_retry_counts.pop(market_id, None)
        return

    # ── Liquidity cleared — enter now ────────────────────────────────────
    _liquidity_retry_counts.pop(market_id, None)
    logger.info(
        "[LiqRetry] %s liquidity cleared (spread=%.2f vol=%d) — entering",
        market_id, spread, snap.volume,
    )

    ok, reason = rm.can_open_position(sig.kelly_stake_usd, station=station)
    if not ok:
        logger.warning("[LiqRetry] Risk check failed for %s: %s", station, reason)
        get_sheets_logger().log_skipped_signal(sig, f"Liquidity retry entry blocked: {reason}")
        return

    max_price = snap.yes_ask

    real_market_id = snap.market_id  # use API ticker (avoids B68 vs T68 mismatch)
    result = kalshi.place_order_with_fill_check(
        market_id=real_market_id,
        contracts=sig.kelly_contracts,
        limit_price=max_price,
        side="yes",
    )

    if result.success:
        rm.open_position(
            station=station,
            market_id=real_market_id,
            bucket_lower=bucket_lower,
            contracts=sig.kelly_contracts,
            entry_price=max_price,
            event_date=event_date,
        )
        get_sheets_logger().log_trade_opened(
            station=station,
            event_date=event_date,
            market_id=real_market_id,
            bucket_lower=bucket_lower,
            entry_price=max_price,
            contracts=sig.kelly_contracts,
            stake_usd=sig.kelly_stake_usd,
            sig=sig,
        )
        logger.info(
            "[LiqRetry] Entry complete: %s | %d contracts @ $%.2f",
            real_market_id, sig.kelly_contracts, max_price,
        )
    else:
        logger.error("[LiqRetry] Order failed: %s — %s", market_id, result.error)
        alert_order_failure(station, market_id, result.error or "unknown")


# ---------------------------------------------------------------------------
# Settlement sweep — runs at 09:00 UTC after LCD is published
# ---------------------------------------------------------------------------

def tier_settlement_sweep():
    """
    Check for settlements on all open positions.

    Runs at 09:00 UTC (morning LCD sweep for yesterday's markets) and also
    intraday every 30 min to catch same-day positions marked pending_settlement.

    For each locally-open position:
      - Calls get_settled_markets() for its event_date
      - If Kalshi has confirmed settlement: records close and fires alert
      - If not yet on the feed: leaves open (pending_settlement stays True)
    """
    _tier_last_run["settlement"] = datetime.now(timezone.utc).strftime("%H:%M UTC")
    push_event("tier_heartbeat", _tier_last_run)
    now_utc   = datetime.now(timezone.utc)
    yesterday = (now_utc - timedelta(days=1)).date()
    today     = now_utc.date()
    rm        = get_risk_manager()
    kalshi    = get_kalshi()

    # Group open positions by their event_date
    by_date: dict[date, dict] = {}
    for mid, pos in rm.state.positions.items():
        pos_date = date.fromisoformat(pos.event_date)
        if pos_date in (yesterday, today):
            by_date.setdefault(pos_date, {})[mid] = pos

    if not by_date:
        logger.info("[Settlement] No positions to sweep")
        return

    sheets = get_sheets_logger()
    any_closed = False

    for sweep_date, positions_to_check in by_date.items():
        label = "yesterday" if sweep_date == yesterday else "today"
        logger.info("[Settlement] Checking %d %s position(s) for %s",
                    len(positions_to_check), label, sweep_date)

        settlements = kalshi.get_settled_markets(sweep_date)
        if not settlements:
            logger.info("[Settlement] No settlements from Kalshi for %s yet", sweep_date)
            continue

        for market_id, pos in positions_to_check.items():
            if market_id in settlements:
                settlement_value = settlements[market_id]
                close_reason = f"Settlement sweep — LCD verified at ${settlement_value:.2f}"
                realized = rm.close_position(market_id, exit_price=settlement_value, reason=close_reason)
                sheets.log_trade_closed(pos.station, market_id, settlement_value, realized, close_reason)
                alert_settlement_detected(pos.station, market_id, realized)
                any_closed = True

                bucket_hit = settlement_value >= 0.95
                with _latest_signals_lock:
                    prior_sig = _latest_signals.get(pos.station)
                if prior_sig:
                    sheets.log_model_accuracy(
                        station=pos.station,
                        event_date=sweep_date,
                        cluster_id=prior_sig.cluster_id,
                        season=prior_sig.season,
                        forecast_raw=prior_sig.forecast_raw,
                        forecast_adjusted=prior_sig.forecast_adjusted,
                        bias_mean=prior_sig.bias_mean,
                        bias_std=prior_sig.bias_std,
                        n_obs=prior_sig.n_obs,
                        observed_high=None,
                        bucket_hit=bucket_hit,
                    )

                logger.info("[Settlement] %s settled | value=%.2f | P/L $%+.4f",
                            market_id, settlement_value, realized)
            else:
                logger.info("[Settlement] %s not yet in Kalshi feed — leaving open", market_id)

    if any_closed:
        summary = rm.summary()
        mode = "DEMO" if USE_DEMO else "LIVE"
        sheets.update_dashboard(summary, mode=mode)
        sheets.log_eod_summary(summary, session_date=yesterday.isoformat(), mode=mode)
        push_event("state_update", summary)
        logger.info("[Settlement] Sweep complete | bankroll=$%.2f | daily P/L=$%+.2f | open=%d",
                    summary["bankroll"], summary["daily_pnl"], summary["open_positions"])
    else:
        logger.info("[Settlement] Sweep complete — no new settlements")


# ---------------------------------------------------------------------------
# Daily obs update — keeps obs_daily.parquet current (runs at 10:00 UTC)
# ---------------------------------------------------------------------------

def _obs_incremental_update():
    """Append yesterday's observed tmax for all stations to obs_daily.parquet."""
    logger.info("[ObsUpdate] Starting incremental obs update")
    try:
        from scripts.build_obs_database import build_obs_database
        build_obs_database(incremental=True)
        logger.info("[ObsUpdate] Incremental obs update complete")
    except Exception as exc:
        logger.error("[ObsUpdate] Failed: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Monthly z500 pipeline — keeps pattern clusters and bias table current
# Runs on the 1st of each month at 02:00 UTC (NCEP reanalysis ~2 month lag)
# Chain: build_500mb_database → build_pattern_clusters → build_bias_table
# ---------------------------------------------------------------------------

def _monthly_z500_pipeline():
    """Rebuild z500 anomalies, pattern clusters, and bias table."""
    logger.info("[Z500Pipeline] Starting monthly z500 + cluster + bias rebuild")
    try:
        from scripts.build_500mb_database import build_500mb_database
        build_500mb_database()
        logger.info("[Z500Pipeline] z500 anomalies updated")
    except Exception as exc:
        logger.error("[Z500Pipeline] z500 step failed: %s", exc, exc_info=True)
        return

    try:
        from scripts.build_pattern_clusters import build_pattern_clusters
        build_pattern_clusters()
        logger.info("[Z500Pipeline] Pattern clusters rebuilt")
    except Exception as exc:
        logger.error("[Z500Pipeline] Cluster step failed: %s", exc, exc_info=True)
        return

    try:
        from scripts.build_bias_table import build_bias_table
        build_bias_table()
        logger.info("[Z500Pipeline] Bias table rebuilt — monthly pipeline complete")
    except Exception as exc:
        logger.error("[Z500Pipeline] Bias table step failed: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# NWS retry scheduler — fires when Tier 3 couldn't get forecast data
# ---------------------------------------------------------------------------

def _schedule_tier3_retry(delay_min: int, event_date: date):
    """
    Schedule a one-shot Tier 3 retry using a DateTrigger.
    Increments the retry counter for this event_date.
    """
    global _scheduler
    if _scheduler is None:
        logger.error("Cannot schedule retry — scheduler not initialized")
        return

    key = event_date.isoformat()
    _tier3_retry_counts[key] = _tier3_retry_counts.get(key, 0) + 1

    run_at = datetime.now(timezone.utc) + timedelta(minutes=delay_min)
    job_id = f"tier3_retry_{key}_{_tier3_retry_counts[key]}"

    _scheduler.add_job(
        tier3_full_signal_pass,
        trigger=DateTrigger(run_date=run_at, timezone="UTC"),
        id=job_id,
        name=f"Tier3 NWS Retry {_tier3_retry_counts[key]} ({key})",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    logger.info(
        "[Tier3] Retry %d/%d scheduled for %s UTC (in %d min)",
        _tier3_retry_counts[key], TIER3_MAX_RETRIES,
        run_at.strftime("%H:%M"), delay_min,
    )


# ---------------------------------------------------------------------------
# Startup reconciliation — syncs local state with live Kalshi on launch
# ---------------------------------------------------------------------------

def _graceful_shutdown(signum, frame):
    """
    Handle SIGINT (Ctrl+C) and SIGTERM (system shutdown / VPS stop).
    Logs all open positions clearly, saves state, and exits cleanly.
    The bot never mid-exit — positions remain open on Kalshi and are
    reconciled on the next startup.
    """
    sig_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
    logger.warning("Shutdown signal received (%s) — stopping bot", sig_name)

    rm = get_risk_manager()
    open_positions = rm.state.positions

    if open_positions:
        lines = [
            f"\n{'='*60}",
            f"  BOT SHUTTING DOWN WITH {len(open_positions)} OPEN POSITION(S)",
            f"{'='*60}",
        ]
        for mid, pos in open_positions.items():
            lines.append(
                f"  {pos.station:6s} | {mid} | "
                f"{pos.contracts} contracts | entry ${pos.entry_price:.2f} | "
                f"P/L ${pos.unrealized_pnl:+.4f}"
            )
        lines += [
            f"{'='*60}",
            "  These positions remain open on Kalshi.",
            "  Close manually at kalshi.com or restart bot to resume monitoring.",
            f"{'='*60}\n",
        ]
        msg = "\n".join(lines)
        print(msg)
        logger.warning(msg)
    else:
        logger.info("No open positions at shutdown — clean exit")

    stop_scheduler()
    sys.exit(0)


def _run_startup_reconciliation():
    """
    Run in a background thread on scheduler start.
    Syncs local risk state against live Kalshi positions and balance.
    Fires an alert if any mismatches are found.
    """
    try:
        rm     = get_risk_manager()
        kalshi = get_kalshi()
        notes  = rm.reconcile_with_kalshi(kalshi)
        if notes:
            details = "\n".join(f"• {n}" for n in notes)
            alert_reconciliation_mismatch(details)
            logger.warning("[Reconciliation] %d mismatch(es) found and corrected", len(notes))
        else:
            logger.info("[Reconciliation] State matches Kalshi — no corrections needed")
    except Exception as exc:
        logger.error("[Reconciliation] Failed: %s", exc)


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

def start_scheduler() -> BackgroundScheduler:
    """
    Initialize and start the APScheduler with all tiers, settlement sweeps,
    and startup reconciliation.  Only same-day markets are traded.

    Schedule summary (all UTC):
      Tier 1  — sleep-based ~5 min   — METAR + exits + entries (post-ASOS-aligned)
      Tier 2  — every 10 min         — TAF amendments + auto-close on flip
      Tier 3  — 00:30 / 06:30 / 12:30 / 18:30  — GFS-aligned signal recompute
      Sweep   — 09:00               — Morning LCD settlement sweep
      Sweep   — 14-23Z every 30 min — Intraday settlement check for pending positions
    """
    global _scheduler, _risk_manager, _kalshi

    _risk_manager = RiskManager()
    _kalshi       = KalshiClient(demo=USE_DEMO)

    signal.signal(signal.SIGINT,  _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    scheduler = BackgroundScheduler(timezone="UTC")

    # Tier 1 — sleep-based: first run fires 10 s after startup, then
    # self-reschedules TIER1_INTERVAL_SECONDS after each completion.
    # Drifts toward ASOS post times (~:53-:58) to keep obs fresh.
    scheduler.add_job(
        tier1_metar_entries_exits,
        trigger=DateTrigger(
            run_date=datetime.now(timezone.utc) + timedelta(seconds=10),
            timezone="UTC",
        ),
        id="tier1_metar",
        name="METAR + Entries + Exits",
        max_instances=1,
        coalesce=True,
    )

    # Tier 2 — TAF amendment monitor, clock-aligned every 10 min.
    # TAFs update on a schedule (not ASOS cadence), so clock alignment is fine.
    scheduler.add_job(
        tier2_taf_monitor,
        trigger=IntervalTrigger(seconds=TIER2_INTERVAL_SECONDS),
        id="tier2_taf",
        name="TAF Amendment Monitor",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )

    # Tier 3 — GFS cycle aligned: 00:30, 06:30, 12:30, 18:30 UTC
    for hour in [0, 6, 12, 18]:
        scheduler.add_job(
            tier3_full_signal_pass,
            trigger=CronTrigger(hour=hour, minute=30, timezone="UTC"),
            id=f"tier3_{hour:02d}z",
            name=f"Signal Recompute {hour:02d}Z+30",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )

    # Settlement sweep — 09:00 UTC every morning (LCD overnight settlements)
    # and every 30 min from 14:00–23:30 UTC to catch same-day closures early.
    scheduler.add_job(
        tier_settlement_sweep,
        trigger=CronTrigger(hour=SETTLEMENT_SWEEP_UTC_HOUR, minute=0, timezone="UTC"),
        id="settlement_sweep_morning",
        name="Morning Settlement Sweep",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
    )
    scheduler.add_job(
        tier_settlement_sweep,
        trigger=CronTrigger(hour="14-23", minute="0,30", timezone="UTC"),
        id="settlement_sweep_intraday",
        name="Intraday Settlement Check",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )

    # Phase 4 forecast refresh — re-fetch GFS/ECMWF after 12z runs are on Open-Meteo.
    # 15:25 UTC → 12z GFS available (~15:30); 19:25 UTC → 12z ECMWF available (~18:30).
    # Fires 5 min before the Tier 1 gate opens so data is ready before entries attempt.
    scheduler.add_job(
        _phase4_refresh,
        trigger=CronTrigger(hour=15, minute=25, timezone="UTC"),
        id="phase4_refresh_gfs12z",
        name="Phase4 GFS 12z Refresh",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    scheduler.add_job(
        _phase4_refresh,
        trigger=CronTrigger(hour=19, minute=25, timezone="UTC"),
        id="phase4_refresh_ecmwf12z",
        name="Phase4 ECMWF 12z Refresh",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    # Daily obs update — 10:00 UTC (6 AM ET), after CDO overnight publish
    scheduler.add_job(
        _obs_incremental_update,
        trigger=CronTrigger(hour=10, minute=0, timezone="UTC"),
        id="obs_daily_update",
        name="Daily Obs Incremental Update",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
    )

    # Monthly z500 pipeline — 1st of month at 02:00 UTC
    # Rebuilds z500 anomalies, pattern clusters, and bias table in sequence.
    # NCEP reanalysis has a ~2 month lag so monthly is sufficient.
    scheduler.add_job(
        _monthly_z500_pipeline,
        trigger=CronTrigger(day=1, hour=2, minute=0, timezone="UTC"),
        id="monthly_z500_pipeline",
        name="Monthly Z500 + Cluster + Bias Rebuild",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )

    scheduler.start()
    _scheduler = scheduler

    logger.info(
        "Scheduler started | Tier1=~%ds sleep-based | Tier2=%ds TAF | "
        "Tier3=00/06/12/18Z+30 | P4Refresh=15:25Z(GFS)/19:25Z(ECMWF) | "
        "Settlement=%02d:00Z + intraday 14-23Z@:00/:30 | "
        "ObsUpdate=10:00Z | Z500=1st@02:00Z | mode=%s",
        TIER1_INTERVAL_SECONDS, TIER2_INTERVAL_SECONDS,
        SETTLEMENT_SWEEP_UTC_HOUR,
        "DEMO" if USE_DEMO else "LIVE",
    )

    # Startup reconciliation — sync local state vs. live Kalshi (background thread)
    recon_thread = threading.Thread(
        target=_run_startup_reconciliation, daemon=True, name="startup_reconciliation"
    )
    recon_thread.start()

    # Run an immediate Tier 3 pass so dashboard has data right away
    _run_initial_pass()

    return scheduler


def _run_initial_pass():
    """Run Tier 3 immediately on startup in a background thread. Retries once."""
    def _run():
        for attempt in range(2):
            try:
                tier3_full_signal_pass()
                return
            except Exception as exc:
                logger.error(
                    "Initial signal pass failed (attempt %d/2): %s",
                    attempt + 1, exc, exc_info=True,
                )
                if attempt == 0:
                    import time as _time
                    logger.info("Retrying initial signal pass in 30s…")
                    _time.sleep(30)

    t = threading.Thread(target=_run, daemon=True, name="initial_signal_pass")
    t.start()
    logger.info("Initial signal pass launched in background thread")


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


def trigger_signal_pass_now():
    """
    Manually trigger a full Tier 3 signal pass (called from dashboard).
    Runs in a background thread so it doesn't block the HTTP response.
    """
    t = threading.Thread(
        target=tier3_full_signal_pass, daemon=True, name="manual_signal_pass"
    )
    t.start()
    logger.info("Manual Tier 3 signal pass triggered")
