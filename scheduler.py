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

import signal
import sys
import threading
from datetime import date, datetime, timedelta, timezone
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
from utils.dryrun_journal import log_entry as journal_entry, log_snapshot as journal_snapshot, log_exit as journal_exit
from utils.alerting import (
    alert_order_failure,
    alert_reconciliation_mismatch,
    alert_daily_loss_limit,
    alert_settlement_detected,
)
from scripts.signal_engine import run_signal_pass, TradeSignal, check_forecast_availability
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
    TIER2_INTERVAL_SECONDS,
    STALE_SIGNAL_HOURS,
    PRICE_SCAN_INTERVAL_MIN,
    MARKET_OPEN_UTC_HOUR,
    MARKET_OPEN_UTC_MINUTE,
    SETTLEMENT_SWEEP_UTC_HOUR,
    TIER3_RETRY_INTERVAL_MIN,
    TIER3_MAX_RETRIES,
    DAILY_LOSS_LIMIT_PCT,
    MAX_STAKE_PCT,
    USE_DEMO,
    STARTING_BANKROLL,
    SNAPSHOT_INTERVAL_MIN,
)

logger = setup_logging("scheduler")

# Shared state — written by scheduler, read by dashboard
_latest_signals:   dict[str, TradeSignal] = {}
_latest_signals_lock = threading.Lock()

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

    for station in STATIONS:
        try:
            taf = interpret_taf(station)
            if not taf.has_amd:
                continue

            logger.warning("[Tier2] AMD at %s — %s", station, taf.summary)
            _single_station_signal_pass(station)

            with _latest_signals_lock:
                sig = _latest_signals.get(station)

            open_positions = {
                mid: pos for mid, pos in rm.state.positions.items()
                if pos.station == station
            }
            if not open_positions:
                continue

            for market_id, pos in open_positions.items():
                # Skip if already closed by a concurrent Tier 1 run
                if market_id not in rm.state.positions:
                    continue

                if sig is None:
                    continue

                # Close if: weather prohibits trade, or model edge inverted on held bucket
                skip_signal = sig.decision in ("SKIP", "HARD_SKIP")
                edge_inverted = (
                    sig.top_bucket == pos.bucket_lower and sig.top_edge < 0.0
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

                reason = (
                    f"TAF AMD auto-close: signal→{sig.decision} "
                    f"(edge={sig.top_edge:+.3f})"
                    if skip_signal else
                    f"TAF AMD auto-close: edge inverted on bucket {pos.bucket_lower}°F "
                    f"(edge={sig.top_edge:+.3f})"
                )
                logger.warning("[Tier2] Auto-closing %s — %s", market_id, reason)
                _execute_exit(market_id, pos, snap.yes_bid, reason, kalshi, rm)

        except Exception as exc:
            logger.error("[Tier2] Error scanning %s: %s", station, exc)


def _single_station_signal_pass(station: str):
    """Re-run signal generation for one station (called on AMD detection or stale signal)."""
    try:
        from scripts.pattern_classifier import classify_pattern
        from scripts.signal_engine import generate_signal, _load_bias_table

        event_date = date.today() + timedelta(days=1)
        bias_df    = _load_bias_table()
        pattern    = classify_pattern(event_date)
        kalshi     = get_kalshi()
        rm         = get_risk_manager()

        sig = generate_signal(station, event_date, kalshi, bias_df, pattern, rm.state.bankroll)

        with _latest_signals_lock:
            _latest_signals[station] = sig

        logger.info("[SignalRefresh] %s: %s | edge=%+.3f", station, sig.decision, sig.top_edge)

    except Exception as exc:
        logger.error("[SignalRefresh] Failed for %s: %s", station, exc)


# ---------------------------------------------------------------------------
# Tier 1 — METAR + exits + entries (sleep-based, ~5 min after completion)
# ---------------------------------------------------------------------------

def _reschedule_tier1() -> None:
    """
    Schedule the next Tier 1 run for TIER1_INTERVAL_SECONDS from now.

    Called in a finally block so the next fire time is measured from
    completion, not from a fixed clock.  This drifts naturally toward
    ASOS post times (~:53-:58) so obs are always close to fresh.
    """
    global _scheduler
    if _scheduler is None or not _scheduler.running:
        return
    run_at = datetime.now(timezone.utc) + timedelta(seconds=TIER1_INTERVAL_SECONDS)
    _scheduler.add_job(
        tier1_metar_entries_exits,
        trigger=DateTrigger(run_date=run_at, timezone="UTC"),
        id="tier1_metar",
        name="METAR + Entries + Exits",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    logger.debug("[Tier1] Next run scheduled for %s UTC", run_at.strftime("%H:%M"))


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

        event_date = date.today() + timedelta(days=1)
        now_utc    = datetime.now(timezone.utc)

        # ── Pass 1: exits ────────────────────────────────────────────────────
        for station in STATIONS:
            try:
                metar    = get_metar(station)
                obs_temp = metar.temp_f

                if obs_temp is None or obs_temp <= -50.0:
                    logger.warning("[Tier1] %s METAR failed (temp=%s) — skipping exit pass", station, obs_temp)
                    continue

                station_positions = {
                    mid: pos for mid, pos in rm.state.positions.items()
                    if pos.station == station
                }
                if not station_positions:
                    continue

                local_now  = datetime.now(ZoneInfo(STATION_TIMEZONES[station]))
                local_hour = local_now.hour

                rm_data     = running_max_with_confluence(station, date.today())
                running_max = rm_data["running_max_f"]
                if not rm_data["in_confluence"]:
                    logger.warning("[Tier1] %s temp confluence issue: %s", station, rm_data["note"])

                for market_id, pos in station_positions.items():
                    snap = kalshi.get_market_snapshot(
                        station, date.fromisoformat(pos.event_date), pos.bucket_lower
                    )
                    if snap is None:
                        logger.warning("[Tier1] No snapshot for %s", market_id)
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
                    peak_heating_hour = get_peak_hour(station, date.fromisoformat(pos.event_date))

                    exit_decision = rm.update_position(
                        market_id=market_id,
                        current_bid=snap.yes_bid,
                        current_ask=snap.yes_ask,
                        current_edge=current_edge,
                        current_obs_temp=obs_temp,
                        running_max=running_max,
                        local_hour=local_hour,
                        peak_heating_hour=peak_heating_hour,
                    )

                    log_level = (
                        logger.warning if exit_decision.urgency in ("immediate", "warning")
                        else logger.info
                    )
                    log_level(
                        "[Tier1] %s bid=%.2f P/L=$%+.4f (%.1f%%) | [%s] %s",
                        market_id, snap.yes_bid,
                        pos.unrealized_pnl, pos.pnl_pct,
                        exit_decision.urgency.upper(),
                        exit_decision.reason,
                    )

                    if exit_decision.should_exit:
                        _execute_exit(market_id, pos, snap.yes_bid, exit_decision.reason, kalshi, rm)
                        _last_snapshot_time.pop(market_id, None)
                    else:
                        if exit_decision.urgency == "warning":
                            logger.warning(
                                "[Tier1] UNDERSHOOT WARNING on %s — manual close available on dashboard",
                                market_id,
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

        # ── Pass 2: entries ──────────────────────────────────────────────────
        entered = 0
        for station in STATIONS:
            try:
                # Entry lock — prevents double-entry if a previous cycle's fill
                # confirmation is still in flight.
                with _entry_lock:
                    if station in _entry_in_progress:
                        continue
                    _entry_in_progress.add(station)

                try:
                    _tier1_entry_pass(
                        station, event_date, now_utc, rm, kalshi
                    )
                finally:
                    with _entry_lock:
                        _entry_in_progress.discard(station)

            except Exception as exc:
                logger.error("[Tier1] Entry pass error at %s: %s", station, exc)
                with _entry_lock:
                    _entry_in_progress.discard(station)

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

    # No trades on weather prohibits or stale model data
    if sig.decision == "HARD_SKIP":
        return
    age_hours = (now_utc - sig.signal_generated_at).total_seconds() / 3600
    if age_hours > STALE_SIGNAL_HOURS:
        logger.debug("[Tier1] %s signal %.1fh old — skipping entry", station, age_hours)
        return

    # SKIP and CONSTRAINED are never actionable
    if sig.decision in ("SKIP", "CONSTRAINED"):
        return

    existing = rm.station_positions(station)

    # WATCH promotion — fetch a live Kalshi price and check whether edge has
    # crossed the threshold since the last Tier 3 run. Kalshi prices move
    # continuously; waiting up to 6h for the next Tier 3 means we miss
    # intraday opportunities. New positions only — no expansions from WATCH.
    if sig.decision == "WATCH":
        if existing:
            return  # don't expand or reposition from a WATCH signal
        threshold = sig.threshold_result.threshold if sig.threshold_result else 0.12
        snap_pre = kalshi.get_market_snapshot(station, event_date, sig.top_bucket)
        if snap_pre is None or not snap_pre.is_open:
            return
        fresh_edge = sig.top_model_prob - snap_pre.yes_ask
        if fresh_edge < threshold:
            return  # still below threshold — remain WATCH
        logger.info(
            "[Tier1] %s WATCH promoted: live edge=%+.3f ≥ threshold=%.3f — entering",
            station, fresh_edge, threshold,
        )
        # Fall through to new-position entry logic below

    # ── Existing position routing (TRADE signals only) ────────────────────
    if existing:
        existing_pos = existing[0]
        dist = _bucket_distance(existing_pos.bucket_lower, sig.top_bucket)

        if dist == 0:
            return  # already in this bucket

        snap_check = kalshi.get_market_snapshot(station, event_date, sig.top_bucket)
        if snap_check is None or not snap_check.is_open:
            return

        market_id = snap_check.market_id  # use API ticker (avoids B68 vs T68 mismatch)

        if dist == 1:
            expansion_decision = _evaluate_expansion(
                existing_pos=existing_pos,
                new_sig=sig,
                rm=rm,
                event_date=event_date,
            )
            if expansion_decision["eligible"]:
                _execute_expansion(existing_pos, sig, market_id, event_date,
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
                event_date=event_date,
            )
            if reposition_ok:
                _execute_reposition(existing_pos, sig, market_id, required_edge,
                                    dist, event_date, kalshi, rm)
            else:
                logger.info("[Tier1] %s reposition blocked (dist=%d): %s",
                            station, dist, repo_reason)
        return

    # ── New position — skip if at station limit ───────────────────────────
    if len(existing) >= MAX_STATION_POSITIONS:
        return

    snap = kalshi.get_market_snapshot(station, event_date, sig.top_bucket)
    if snap is None or not snap.is_open:
        return

    # Liquidity guard
    spread    = snap.yes_ask - snap.yes_bid
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
            station, event_date.isoformat(), sig.top_bucket, LIQUIDITY_RETRY_INTERVAL_MIN
        )
        return

    # Effective threshold from signal
    threshold = (
        sig.threshold_result.threshold
        if sig.threshold_result
        else 0.12
    )
    fresh_edge = sig.top_model_prob - snap.yes_ask
    if fresh_edge < threshold:
        logger.debug("[Tier1] %s edge %+.3f below threshold %.3f — skip",
                     station, fresh_edge, threshold)
        return

    ok, reason = rm.can_open_position(sig.kelly_stake_usd, station=station)
    if not ok:
        logger.info("[Tier1] %s risk gate: %s", station, reason)
        return

    effective_threshold = (
        sig.threshold_result.threshold
        if sig.threshold_result else 0.12
    )
    max_price = round(sig.top_model_prob - effective_threshold, 4)
    max_price = max(max_price, snap.yes_ask)

    market_id = snap.market_id  # use API ticker (avoids B68 vs T68 mismatch)
    result = kalshi.place_order_with_fill_check(
        market_id=market_id,
        contracts=sig.kelly_contracts,
        limit_price=max_price,
        side="yes",
    )

    if result.success:
        rm.open_position(
            station=station,
            market_id=market_id,
            bucket_lower=sig.top_bucket,
            contracts=sig.kelly_contracts,
            entry_price=max_price,
            event_date=event_date,
        )
        get_sheets_logger().log_trade_opened(
            station=station,
            event_date=event_date,
            market_id=market_id,
            bucket_lower=sig.top_bucket,
            entry_price=max_price,
            contracts=sig.kelly_contracts,
            stake_usd=sig.kelly_stake_usd,
            sig=sig,
            entry_reason="tier1",
        )
        journal_entry(
            station=station,
            market_id=market_id,
            bucket_lower=sig.top_bucket,
            entry_price=max_price,
            contracts=sig.kelly_contracts,
            stake_usd=sig.kelly_stake_usd,
            model_prob=sig.top_model_prob,
            edge=fresh_edge,
            forecast_adjusted=sig.forecast_adjusted,
            sig=sig,
        )
        push_event("position_opened", {
            "market_id":    market_id,
            "station":      station,
            "bucket_lower": sig.top_bucket,
            "entry_price":  max_price,
            "contracts":    sig.kelly_contracts,
            "stake_usd":    sig.kelly_stake_usd,
            "entry_reason": "tier1",
        })
        push_event("state_update", rm.summary())
        logger.info(
            "[Tier1] Entry: %s | bucket %d | %d contracts @ $%.2f | "
            "edge %+.3f | stake $%.2f",
            station, sig.top_bucket, sig.kelly_contracts, max_price,
            fresh_edge, sig.kelly_stake_usd,
        )
    else:
        logger.error("[Tier1] Order failed for %s bucket %d: %s",
                     station, sig.top_bucket, result.error)
        alert_order_failure(station, market_id, result.error or "unknown")


def _execute_exit(market_id, pos, bid_price, reason, kalshi, rm):
    """Place sell order and record close."""
    logger.info("[Exit] Executing: %s | reason: %s", market_id, reason)

    result = kalshi.close_position(market_id, pos.contracts, bid_price)
    if result.success:
        realized = rm.close_position(market_id, bid_price, reason)
        get_sheets_logger().log_trade_closed(market_id, bid_price, realized, reason)
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
        push_event("position_closed", {"market_id": market_id, "realized_pnl": realized, "reason": reason})
        push_event("state_update", rm.summary())
        logger.info("[Exit] Complete: %s | realized P/L $%+.4f", market_id, realized)
    else:
        logger.error("[Exit] Order failed for %s: %s", market_id, result.error)
        push_alert(f"Exit failed — {market_id}", result.error or "unknown", "ERROR")




# ---------------------------------------------------------------------------
# Tier 3 — Full signal recompute only (every 6 hours, no order execution)
# ---------------------------------------------------------------------------

def _tier3_day1_market_open():
    """
    Wrapper for the 14:05 UTC Day-1 market-open trigger.
    Kalshi opens tomorrow's markets at ~10:00 AM EDT (14:00 UTC).
    event_date must be tomorrow — computed at job runtime, not at scheduler init.
    """
    tier3_full_signal_pass(event_date=date.today() + timedelta(days=1))


def tier3_full_signal_pass(event_date: date | None = None):
    """
    Full signal recompute for all stations.  Updates _latest_signals so
    Tier 1 can act on fresh distributions at the next 5-min cycle.
    No orders are placed here — all trade execution is Tier 1's job.

    event_date defaults to today. The Day-1 market-open trigger passes tomorrow.

    Before running:
      1. Probe forecast availability (NWS/Open-Meteo). If unavailable,
         schedule a retry job and return — don't run on stale data.
      2. Check daily loss limit alert threshold.
    """
    logger.info("[Tier3] Full signal recompute starting")
    rm = get_risk_manager()

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

    if event_date is None:
        event_date = date.today() + timedelta(days=1)

    # ── Forecast availability probe ───────────────────────────────────────
    avail = check_forecast_availability(event_date)
    if not avail.get("available"):
        retry_count = _tier3_retry_counts.get(event_date.isoformat(), 0)
        if retry_count < TIER3_MAX_RETRIES:
            logger.warning(
                "[Tier3] Forecast unavailable (%s) — scheduling retry %d/%d in %d min",
                avail.get("details", "unknown"), retry_count + 1,
                TIER3_MAX_RETRIES, TIER3_RETRY_INTERVAL_MIN,
            )
            _schedule_tier3_retry(TIER3_RETRY_INTERVAL_MIN, event_date)
        else:
            logger.error(
                "[Tier3] Forecast still unavailable after %d retries — skipping this cycle",
                TIER3_MAX_RETRIES,
            )
            _tier3_retry_counts.pop(event_date.isoformat(), None)
        return

    _tier3_retry_counts.pop(event_date.isoformat(), None)
    _tier_last_run["tier3"] = datetime.now(timezone.utc).strftime("%H:%M UTC")
    push_event("tier_heartbeat", _tier_last_run)

    signals = run_signal_pass(event_date=event_date, bankroll=rm.state.bankroll)

    # Store updated distributions — Tier 1 reads these on every 5-min cycle
    with _latest_signals_lock:
        _latest_signals.update(signals)
    for station, sig in signals.items():
        push_event("signal_update", {"station": station, "decision": sig.decision,
                                     "top_edge": sig.top_edge, "top_bucket": sig.top_bucket})

    # Log WATCH/SKIP/HARD_SKIP decisions to Sheets for review
    sheets = get_sheets_logger()
    for sig in signals.values():
        if sig.decision in ("WATCH", "SKIP", "HARD_SKIP"):
            sheets.log_skipped_signal(sig, sig.decision)

    summary = rm.summary()
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
    get_sheets_logger().log_trade_closed(existing_pos.market_id, old_snap.yes_bid, realized, close_reason)
    logger.info(
        "[Tier3] Old position closed | %s | realized P/L $%+.4f",
        existing_pos.market_id, realized,
    )

    # ── Step 2: Open new position ─────────────────────────────────────────
    ok, reason = rm.can_open_position(new_sig.kelly_stake_usd, station=station)
    if not ok:
        logger.warning("[Tier3] Reposition open blocked for %s: %s", station, reason)
        return

    effective_threshold = (
        new_sig.threshold_result.threshold
        if new_sig.threshold_result else required_edge
    )
    max_price = round(new_sig.top_model_prob - effective_threshold, 4)
    max_price = max(max_price, new_sig.top_yes_ask)

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

        # Store full expansion reasoning in shared signal store for dashboard card
        sig.expansion_note = expansion_decision["reason"]
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

    effective_threshold = (
        sig.threshold_result.threshold if sig.threshold_result else 0.12
    )
    max_price = round(sig.top_model_prob - effective_threshold, 4)
    max_price = max(max_price, snap.yes_ask)

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
    Check for overnight settlements on all open positions.
    Kalshi settles markets once LCD (Local Climatological Data) is released,
    typically before 09:00 UTC the morning after the event day.

    For each locally-open position whose event_date is yesterday:
      - Call get_settled_markets() to fetch settlement values
      - If found: record close at settlement price and fire alert
      - Positions not yet on Kalshi settlement feed: leave open (may still be pending)
    """
    _tier_last_run["settlement"] = datetime.now(timezone.utc).strftime("%H:%M UTC")
    push_event("tier_heartbeat", _tier_last_run)
    logger.info("[Settlement] Running morning settlement sweep")
    rm     = get_risk_manager()
    kalshi = get_kalshi()

    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()

    positions_to_check = {
        mid: pos for mid, pos in rm.state.positions.items()
        if date.fromisoformat(pos.event_date) == yesterday
    }

    if not positions_to_check:
        logger.info("[Settlement] No yesterday positions to sweep")
        return

    settlements = kalshi.get_settled_markets(yesterday)
    if not settlements:
        logger.info("[Settlement] No settlements returned from Kalshi yet — will retry next cycle")
        return

    sheets = get_sheets_logger()
    for market_id, pos in positions_to_check.items():
        if market_id in settlements:
            settlement_value = settlements[market_id]
            close_reason = f"Settlement sweep — LCD verified at ${settlement_value:.2f}"
            realized = rm.close_position(market_id, exit_price=settlement_value, reason=close_reason)
            sheets.log_trade_closed(market_id, settlement_value, realized, close_reason)
            alert_settlement_detected(pos.station, market_id, realized)

            # Log model accuracy row — observed high comes from settlement bucket inference
            # 1.0 = won (bucket correct), 0.0 = lost. Full observed temp requires IEM fetch.
            bucket_hit = settlement_value >= 0.95   # settlement ≈ 1.0 means we won
            with _latest_signals_lock:
                prior_sig = _latest_signals.get(pos.station)
            if prior_sig:
                sheets.log_model_accuracy(
                    station=pos.station,
                    event_date=yesterday,
                    cluster_id=prior_sig.cluster_id,
                    season=prior_sig.season,
                    forecast_raw=prior_sig.forecast_raw,
                    forecast_adjusted=prior_sig.forecast_adjusted,
                    bias_mean=prior_sig.bias_mean,
                    bias_std=prior_sig.bias_std,
                    n_obs=prior_sig.n_obs,
                    observed_high=None,   # IEM fetch not yet implemented — shows blank
                    bucket_hit=bucket_hit,
                )

            logger.info(
                "[Settlement] %s settled | value=%.2f | P/L $%+.4f",
                market_id, settlement_value, realized,
            )
        else:
            logger.info("[Settlement] %s not yet in settlements feed — leaving open", market_id)

    summary = rm.summary()
    mode = "DEMO" if USE_DEMO else "LIVE"
    sheets.update_dashboard(summary, mode=mode)
    sheets.log_eod_summary(summary, session_date=yesterday.isoformat(), mode=mode)
    push_event("state_update", summary)
    logger.info(
        "[Settlement] Sweep complete | bankroll=$%.2f | daily P/L=$%+.2f | open=%d",
        summary["bankroll"], summary["daily_pnl"], summary["open_positions"],
    )


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
    Initialize and start the APScheduler with all tiers plus market-open
    trigger, settlement sweep, and startup reconciliation.

    Schedule summary (all UTC):
      Tier 1  — sleep-based ~5 min   — METAR + exits + entries (post-ASOS-aligned)
      Tier 2  — every 10 min         — TAF amendments + auto-close on flip
      Tier 3  — 00:30 / 06:30 / 12:30 / 18:30  — GFS-aligned signal recompute
      Tier 3  — 14:05               — Kalshi Day-1 market open (signal recompute only)
      Sweep   — 09:00               — Morning settlement sweep
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

    # Tier 3 — Kalshi Day-1 market open: fires at 14:05 UTC every day
    scheduler.add_job(
        _tier3_day1_market_open,
        trigger=CronTrigger(
            hour=MARKET_OPEN_UTC_HOUR,
            minute=MARKET_OPEN_UTC_MINUTE,
            timezone="UTC",
        ),
        id="tier3_market_open",
        name=f"Signal Recompute — Market Open {MARKET_OPEN_UTC_HOUR:02d}:{MARKET_OPEN_UTC_MINUTE:02d}Z",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    # Settlement sweep — 09:00 UTC every morning
    scheduler.add_job(
        tier_settlement_sweep,
        trigger=CronTrigger(hour=SETTLEMENT_SWEEP_UTC_HOUR, minute=0, timezone="UTC"),
        id="settlement_sweep",
        name="Morning Settlement Sweep",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
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
        "Tier3=00/06/12/18Z+30 + %02d:%02dZ market-open | "
        "Settlement=%02d:00Z | ObsUpdate=10:00Z | Z500=1st@02:00Z | mode=%s",
        TIER1_INTERVAL_SECONDS, TIER2_INTERVAL_SECONDS,
        MARKET_OPEN_UTC_HOUR, MARKET_OPEN_UTC_MINUTE,
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
