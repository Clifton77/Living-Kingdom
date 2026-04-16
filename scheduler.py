"""
Multi-tier APScheduler for the weather trading bot.

Tier 1 — every 5 minutes:
    TAF amendment detector. Checks for AMD flags. If a TAF changes
    significantly, re-evaluates open positions and re-runs signal.

Tier 2 — every 30 minutes:
    METAR running high tracker. Fetches current obs for each station.
    Updates P/L on open positions. Evaluates exit conditions.
    Checks early profit exit if temp is locking into a bucket.

Tier 3 — every 6 hours (aligned to GFS cycles: 00Z, 06Z, 12Z, 18Z + 30min):
    Full signal pass. Classifies 500mb pattern, fetches live forecasts,
    computes bias-adjusted distributions, fetches Kalshi prices, generates
    trade signals. Places orders for TRADE decisions.

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
    "price_scan": "never",
    "tier3":      "never",
    "settlement": "never",
}


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
# Tier 1 — TAF amendment monitor (every 5 min)
# ---------------------------------------------------------------------------

def tier1_taf_monitor():
    """
    Check each station's TAF for amendments.
    If an AMD is detected on a station with an open position,
    re-evaluate whether the position still makes sense.
    """
    _tier_last_run["tier1"] = datetime.now(timezone.utc).strftime("%H:%M UTC")
    push_event("tier_heartbeat", _tier_last_run)
    logger.info("[Tier1] TAF amendment scan")
    rm = get_risk_manager()

    if rm.is_halted:
        logger.info("[Tier1] Bot halted — skipping")
        return

    for station in STATIONS:
        try:
            taf = interpret_taf(station)
            if taf.has_amd:
                logger.warning("[Tier1] AMD detected at %s — %s", station, taf.summary)

                # Flag any open positions at this station for immediate re-evaluation
                open_mkt_ids = [
                    mid for mid, pos in rm.state.positions.items()
                    if pos.station == station
                ]
                if open_mkt_ids:
                    logger.warning(
                        "[Tier1] %s has %d open position(s) — triggering Tier3 re-evaluation",
                        station, len(open_mkt_ids),
                    )
                    # Trigger an immediate mini signal pass for this station
                    _single_station_signal_pass(station)

        except Exception as exc:
            logger.error("[Tier1] Error scanning %s: %s", station, exc)


def _single_station_signal_pass(station: str):
    """Re-run signal generation for one station (called on AMD detection)."""
    try:
        import pandas as pd
        from scripts.pattern_classifier import classify_pattern
        from scripts.signal_engine import generate_signal, _load_bias_table

        event_date = date.today()
        bias_df    = _load_bias_table()
        pattern    = classify_pattern(event_date)
        kalshi     = get_kalshi()
        rm         = get_risk_manager()

        sig = generate_signal(station, event_date, kalshi, bias_df, pattern, rm.state.bankroll)

        with _latest_signals_lock:
            _latest_signals[station] = sig

        logger.info("[Tier1] %s re-signal: %s | edge=%+.3f", station, sig.decision, sig.top_edge)

    except Exception as exc:
        logger.error("[Tier1] Single-station signal failed for %s: %s", station, exc)


# ---------------------------------------------------------------------------
# Tier 2 — METAR running high + position management (every 30 min)
# ---------------------------------------------------------------------------

def tier2_metar_and_positions():
    """
    Fetch current METAR obs and IEM 1-min running max for all stations.
    Update open position P/L and evaluate exit conditions including:
      - undershoot warning (approaching peak hour, tracking low)
      - undershoot hard exit (past peak hour, definitive miss)
      - overshoot exit (before peak hour, running max near bucket upper)
      - early profit exit (bid ≥ 85¢)
    Execute exits if triggered. Log warnings for manual review.
    """
    _tier_last_run["tier2"] = datetime.now(timezone.utc).strftime("%H:%M UTC")
    push_event("tier_heartbeat", _tier_last_run)
    logger.info("[Tier2] METAR + position update cycle")
    rm     = get_risk_manager()
    kalshi = get_kalshi()

    if rm.is_halted:
        logger.info("[Tier2] Bot halted — skipping")
        return

    for station in STATIONS:
        try:
            metar = get_metar(station)
            obs_temp = metar.temp_f

            # Find any open positions for this station
            station_positions = {
                mid: pos for mid, pos in rm.state.positions.items()
                if pos.station == station
            }

            if not station_positions:
                continue

            # Current local hour at this station
            local_now  = datetime.now(ZoneInfo(STATION_TIMEZONES[station]))
            local_hour = local_now.hour

            # IEM 1-min running max + METAR confluence
            rm_data     = running_max_with_confluence(station, date.today())
            running_max = rm_data["running_max_f"]
            if not rm_data["in_confluence"]:
                logger.warning("[Tier2] %s temp confluence issue: %s", station, rm_data["note"])

            for market_id, pos in station_positions.items():
                # Fetch current market bid/ask
                snap = kalshi.get_market_snapshot(
                    station, date.fromisoformat(pos.event_date), pos.bucket_lower
                )
                if snap is None:
                    logger.warning("[Tier2] No snapshot for %s", market_id)
                    continue

                # Get current edge from latest signal — re-run if stale
                with _latest_signals_lock:
                    sig = _latest_signals.get(station)

                if sig is not None:
                    age_hours = (
                        datetime.now(timezone.utc) - sig.signal_generated_at
                    ).total_seconds() / 3600
                    if age_hours > STALE_SIGNAL_HOURS:
                        logger.info(
                            "[Tier2] Signal for %s is %.1fh old (> %dh) — refreshing",
                            station, age_hours, STALE_SIGNAL_HOURS,
                        )
                        _single_station_signal_pass(station)
                        with _latest_signals_lock:
                            sig = _latest_signals.get(station)

                current_edge = sig.top_edge if sig and sig.top_bucket == pos.bucket_lower else 0.0

                # Peak heating hour — DOY-smoothed seasonal curve
                peak_heating_hour = get_peak_hour(station, date.fromisoformat(pos.event_date))

                # Update position and evaluate exit
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
                    "[Tier2] %s bid=%.2f P/L=$%+.4f (%.1f%%) | [%s] %s",
                    market_id, snap.yes_bid,
                    pos.unrealized_pnl, pos.pnl_pct,
                    exit_decision.urgency.upper(),
                    exit_decision.reason,
                )

                if exit_decision.should_exit:
                    _execute_exit(market_id, pos, snap.yes_bid, exit_decision.reason, kalshi, rm)
                elif exit_decision.urgency == "warning":
                    # Warning surfaced in logs and dashboard — no auto-exit yet.
                    # Dashboard will show a manual close button on the position card.
                    logger.warning(
                        "[Tier2] UNDERSHOOT WARNING on %s — manual close available on dashboard",
                        market_id,
                    )

        except Exception as exc:
            logger.error("[Tier2] Error processing %s: %s", station, exc)


def _execute_exit(market_id, pos, bid_price, reason, kalshi, rm):
    """Place sell order and record close."""
    logger.info("[Tier2] Executing exit: %s | reason: %s", market_id, reason)

    result = kalshi.close_position(market_id, pos.contracts, bid_price)
    if result.success:
        realized = rm.close_position(market_id, bid_price, reason)
        get_sheets_logger().log_trade_closed(market_id, bid_price, realized, reason)
        mode = "DEMO" if USE_DEMO else "LIVE"
        get_sheets_logger().update_dashboard(rm.summary(), mode=mode)
        push_event("position_closed", {"market_id": market_id, "realized_pnl": realized, "reason": reason})
        push_event("state_update", rm.summary())
        logger.info("[Tier2] Exit complete: %s | realized P/L $%+.4f", market_id, realized)
    else:
        logger.error("[Tier2] Exit order failed for %s: %s", market_id, result.error)
        push_alert(f"Exit failed — {market_id}", result.error or "unknown", "ERROR")


# ---------------------------------------------------------------------------
# Price opportunity scanner — every 60 min (configurable)
#
# Runs between Tier 3 cycles to catch intraday price drops that create edge.
# Example: at market open the 80-81° bucket is priced 35¢ (edge below threshold).
# Two hours later the market re-prices it to 18¢ — our model still says 42%, so
# edge is now +0.24. This scanner catches that without waiting for the next 6-hour
# Tier 3 cycle.
#
# Design: NO model recomputation. Reuses the probability distribution already
# computed by Tier 3 (stored in _latest_signals). Just fetches fresh Kalshi prices
# and recalculates edge. Cheap: 1 API call per station (~8 stations = 8 calls).
#
# Weather guard: threshold is already station-specific (weather penalty baked in
# from when Tier 3 ran). HARD_SKIP stations are always excluded regardless of price.
# ---------------------------------------------------------------------------

def tier2b_price_opportunity_scan():
    """
    Hourly price opportunity scanner.

    For each station without a full position book:
      1. Load last Tier 3 signal from _latest_signals.
         - No signal yet → skip (wait for Tier 3).
         - Signal age > STALE_SIGNAL_HOURS → skip (stale model data, no entry).
         - HARD_SKIP → always skip (weather prohibits trade regardless of price).
      2. Fetch fresh Kalshi prices for all buckets (dynamic series-based lookup).
      3. For each bucket in the signal distribution:
         fresh_edge = bucket.model_prob - fresh_yes_ask
      4. If fresh_edge > sig.threshold_result.threshold for any bucket not already held:
         → check liquidity, check risk, place order.
         → log to Sheets with entry_reason = "price_scan" for post-session review.

    Expansion / reposition decisions are left to Tier 3. This scanner only adds
    fresh first-entry positions where edge has materialized since the last Tier 3 run.
    """
    _tier_last_run["price_scan"] = datetime.now(timezone.utc).strftime("%H:%M UTC")
    push_event("tier_heartbeat", _tier_last_run)
    logger.info("[PriceScan] Hourly price opportunity scan starting")

    rm     = get_risk_manager()
    kalshi = get_kalshi()

    if rm.is_halted:
        logger.info("[PriceScan] Bot halted — skipping")
        return

    event_date = date.today()
    now_utc    = datetime.now(timezone.utc)
    entered    = 0

    for station in STATIONS:
        try:
            # ── Skip if station is already at position limit ──────────────
            existing = rm.station_positions(station)
            if len(existing) >= MAX_STATION_POSITIONS:
                logger.debug("[PriceScan] %s at position limit — skipping", station)
                continue

            # ── Load last signal ──────────────────────────────────────────
            with _latest_signals_lock:
                sig = _latest_signals.get(station)

            if sig is None:
                logger.debug("[PriceScan] %s — no signal yet, skipping", station)
                continue

            # Hard weather skip: price movement irrelevant when conditions prohibit trading
            if sig.decision == "HARD_SKIP":
                logger.debug("[PriceScan] %s — HARD_SKIP, skipping", station)
                continue

            # Stale signal guard: don't place new entries based on old model data
            age_hours = (now_utc - sig.signal_generated_at).total_seconds() / 3600
            if age_hours > STALE_SIGNAL_HOURS:
                logger.info(
                    "[PriceScan] %s — signal is %.1fh old (> %dh stale limit), skipping",
                    station, age_hours, STALE_SIGNAL_HOURS,
                )
                continue

            # Effective threshold — already includes weather penalty from Tier 3
            threshold = (
                sig.threshold_result.threshold
                if sig.threshold_result
                else 0.12
            )

            # ── Fetch fresh Kalshi prices ─────────────────────────────────
            fresh_snaps = kalshi.get_all_snapshots(station, event_date)
            if not fresh_snaps:
                logger.debug("[PriceScan] %s — no live snapshots available", station)
                continue

            # Set of bucket_lowers already held at this station
            held_buckets = {pos.bucket_lower for pos in existing}

            # ── Find best new edge opportunity with fresh prices ──────────
            best_edge    = 0.0
            best_bucket  = None
            best_snap    = None
            best_model_p = 0.0

            for b in sig.buckets:
                if b.bucket_lower in held_buckets:
                    continue   # already in this bucket

                snap = fresh_snaps.get(b.bucket_lower)
                if snap is None or not snap.is_open:
                    continue

                fresh_edge = b.model_prob - snap.yes_ask

                if fresh_edge > best_edge:
                    best_edge    = fresh_edge
                    best_bucket  = b.bucket_lower
                    best_snap    = snap
                    best_model_p = b.model_prob

            if best_bucket is None or best_edge < threshold:
                logger.debug(
                    "[PriceScan] %s — best fresh edge %.3f below threshold %.3f",
                    station, best_edge, threshold,
                )
                continue

            logger.info(
                "[PriceScan] %s bucket %d — fresh edge %+.3f (threshold %.3f) "
                "model=%.1f%% ask=%.1f%% — attempting entry",
                station, best_bucket, best_edge, threshold,
                best_model_p * 100, best_snap.yes_ask * 100,
            )

            # ── Liquidity guard ───────────────────────────────────────────
            spread    = best_snap.yes_ask - best_snap.yes_bid
            spread_ok = spread <= MAX_BID_ASK_SPREAD
            volume_ok = best_snap.volume >= MIN_MARKET_VOLUME

            if not (spread_ok and volume_ok):
                issues = []
                if not spread_ok:
                    issues.append(f"spread={spread:.2f}")
                if not volume_ok:
                    issues.append(f"vol={best_snap.volume}")
                logger.info(
                    "[PriceScan] %s bucket %d illiquid (%s) — scheduling liquidity retry",
                    station, best_bucket, ", ".join(issues),
                )
                _schedule_liquidity_retry(
                    station, event_date.isoformat(), best_bucket, LIQUIDITY_RETRY_INTERVAL_MIN
                )
                continue

            # ── Kelly sizing with fresh price ─────────────────────────────
            # Recalculate stake using fresh ask (price changed since Tier 3 ran).
            # Kelly: stake = (edge / (1/ask - 1)) * bankroll, capped at MAX_STAKE_PCT.
            ask = best_snap.yes_ask
            if ask <= 0 or ask >= 1:
                continue
            kelly_raw  = (best_edge / ((1.0 - ask) / ask)) * rm.state.bankroll
            kelly_usd  = min(kelly_raw, rm.state.bankroll * MAX_STAKE_PCT)
            contracts  = max(1, round(kelly_usd / ask))

            # ── Risk check ────────────────────────────────────────────────
            ok, reason = rm.can_open_position(kelly_usd, station=station)
            if not ok:
                logger.info("[PriceScan] %s risk gate: %s", station, reason)
                continue

            # ── Place order ───────────────────────────────────────────────
            # max_price: most we'll pay and still retain edge ≥ threshold
            max_price = round(best_model_p - threshold, 4)
            max_price = max(max_price, ask)   # never below current ask

            result = kalshi.place_order_with_fill_check(
                market_id=best_snap.market_id,
                contracts=contracts,
                limit_price=max_price,
                side="yes",
            )

            if result.success:
                rm.open_position(
                    station=station,
                    market_id=best_snap.market_id,
                    bucket_lower=best_bucket,
                    contracts=contracts,
                    entry_price=max_price,
                    event_date=event_date,
                )
                get_sheets_logger().log_trade_opened(
                    station=station,
                    event_date=event_date,
                    market_id=best_snap.market_id,
                    bucket_lower=best_bucket,
                    entry_price=max_price,
                    contracts=contracts,
                    stake_usd=kelly_usd,
                    sig=sig,
                    entry_reason="price_scan",   # distinguish from Tier 3 entries in log
                )
                push_event("position_opened", {
                    "market_id":   best_snap.market_id,
                    "station":     station,
                    "bucket_lower": best_bucket,
                    "entry_price": max_price,
                    "contracts":   contracts,
                    "stake_usd":   kelly_usd,
                    "entry_reason": "price_scan",
                })
                push_event("state_update", rm.summary())
                logger.info(
                    "[PriceScan] Entry: %s | bucket %d | %d contracts @ $%.2f | "
                    "edge %+.3f | stake $%.2f",
                    station, best_bucket, contracts, max_price, best_edge, kelly_usd,
                )
                entered += 1
            else:
                logger.error(
                    "[PriceScan] Order failed for %s bucket %d: %s",
                    station, best_bucket, result.error,
                )
                alert_order_failure(station, best_snap.market_id, result.error or "unknown")

        except Exception as exc:
            logger.error("[PriceScan] Error at %s: %s", station, exc)

    logger.info("[PriceScan] Scan complete — %d new entr%s", entered, "y" if entered == 1 else "ies")


# ---------------------------------------------------------------------------
# Tier 3 — Full signal pass + order execution (every 6 hours)
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
    Full signal generation for all stations.
    Places orders for TRADE decisions that pass risk checks.

    event_date defaults to today. The Day-1 market-open trigger passes tomorrow.

    Before running:
      1. Probe forecast availability (NWS/Open-Meteo). If unavailable,
         schedule a retry job and return — don't run on stale data.
      2. Check daily loss limit alert threshold.
    """
    logger.info("[Tier3] Full signal pass starting")
    rm     = get_risk_manager()
    kalshi = get_kalshi()

    if rm.is_halted:
        logger.info("[Tier3] Bot halted — skipping signal pass")
        # Fire daily loss limit alert if that's why we're halted
        if not rm.state.kill_switch_active:
            limit = rm.state.bankroll * DAILY_LOSS_LIMIT_PCT
            alert_daily_loss_limit(rm.state.daily_pnl, limit)
        return

    if event_date is None:
        event_date = date.today()

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

    # Reset retry counter on successful data availability
    _tier3_retry_counts.pop(event_date.isoformat(), None)
    _tier_last_run["tier3"] = datetime.now(timezone.utc).strftime("%H:%M UTC")
    push_event("tier_heartbeat", _tier_last_run)

    signals    = run_signal_pass(event_date=event_date, bankroll=rm.state.bankroll)

    # Update shared signal store and push to dashboard
    with _latest_signals_lock:
        _latest_signals.update(signals)
    for station, sig in signals.items():
        push_event("signal_update", {"station": station, "decision": sig.decision,
                                     "top_edge": sig.top_edge, "top_bucket": sig.top_bucket})

    # ── Priority queue: rank TRADE signals by edge, best first ───────────
    trade_signals = [
        sig for sig in signals.values()
        if sig.decision == "TRADE"
    ]
    trade_signals.sort(key=lambda s: s.top_edge, reverse=True)

    for sig in trade_signals:
        station   = sig.station
        market_id = build_market_id(station, event_date, sig.top_bucket)

        # Skip if already have a position in this exact market
        if market_id in rm.state.positions:
            logger.info("[Tier3] Already in %s — skipping", market_id)
            continue

        # ── Market open + liquidity guards ────────────────────────────────
        snap_check = kalshi.get_market_snapshot(station, event_date, sig.top_bucket)
        if snap_check is None or not snap_check.is_open:
            logger.info("[Tier3] Market not open for %s bucket %d — skipping", station, sig.top_bucket)
            continue

        spread = snap_check.yes_ask - snap_check.yes_bid
        spread_ok = spread <= MAX_BID_ASK_SPREAD
        volume_ok = snap_check.volume >= MIN_MARKET_VOLUME

        if not (spread_ok and volume_ok):
            # Market is young or illiquid right now — don't abandon the signal.
            # Schedule a retry: recheck liquidity in LIQUIDITY_RETRY_INTERVAL_MIN minutes.
            # The signal stays valid; we're just waiting for the book to fill in.
            issues = []
            if not spread_ok:
                issues.append(
                    f"spread {spread:.2f} > {MAX_BID_ASK_SPREAD:.2f} "
                    f"(bid={snap_check.yes_bid:.2f} ask={snap_check.yes_ask:.2f})"
                )
            if not volume_ok:
                issues.append(f"volume {snap_check.volume} < {MIN_MARKET_VOLUME}")
            logger.info(
                "[Tier3] %s bucket %d illiquid (%s) — scheduling liquidity retry in %d min",
                station, sig.top_bucket, ", ".join(issues), LIQUIDITY_RETRY_INTERVAL_MIN,
            )
            _schedule_liquidity_retry(
                station=station,
                event_date_iso=event_date.isoformat(),
                bucket_lower=sig.top_bucket,
                delay_min=LIQUIDITY_RETRY_INTERVAL_MIN,
            )
            continue

        # ── Existing position routing — expansion or reposition ───────────
        existing = rm.station_positions(station)
        if existing:
            existing_pos = existing[0]
            dist = _bucket_distance(existing_pos.bucket_lower, sig.top_bucket)

            if dist == 0:
                # Same bucket — already in this position, nothing to do
                logger.info("[Tier3] Already in bucket %d at %s — skipping", sig.top_bucket, station)
                continue

            elif dist == 1:
                # Adjacent bucket — evaluate expansion (hold both)
                expansion_decision = _evaluate_expansion(
                    existing_pos=existing_pos,
                    new_sig=sig,
                    rm=rm,
                    event_date=event_date,
                )
                if expansion_decision["eligible"]:
                    _execute_expansion(
                        existing_pos=existing_pos,
                        sig=sig,
                        market_id=market_id,
                        event_date=event_date,
                        expansion_decision=expansion_decision,
                        kalshi=kalshi,
                        rm=rm,
                    )
                else:
                    logger.info(
                        "[Tier3] %s expansion ineligible: %s",
                        station, expansion_decision["reason"],
                    )
                continue

            else:
                # 2-step or 3+ step shift — significant or major reposition
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
                    _execute_reposition(
                        existing_pos=existing_pos,
                        new_sig=sig,
                        new_market_id=market_id,
                        required_edge=required_edge,
                        dist=dist,
                        event_date=event_date,
                        kalshi=kalshi,
                        rm=rm,
                    )
                else:
                    logger.info(
                        "[Tier3] %s reposition blocked (dist=%d): %s",
                        station, dist, repo_reason,
                    )
                continue

        # ── Normal new-position entry ─────────────────────────────────────
        ok, reason = rm.can_open_position(sig.kelly_stake_usd, station=station)
        if not ok:
            if "exposure" in reason.lower() or "insufficient" in reason.lower():
                sig.decision = "CONSTRAINED"
                logger.warning(
                    "[Tier3] %s CONSTRAINED (edge=%+.3f stake=$%.2f) — %s",
                    station, sig.top_edge, sig.kelly_stake_usd, reason,
                )
            else:
                logger.warning("[Tier3] Risk check failed for %s: %s", station, reason)
            get_sheets_logger().log_skipped_signal(sig, reason)
            with _latest_signals_lock:
                _latest_signals[station] = sig
            continue

        # Derive max price from edge math: model_prob - threshold = most we'll pay
        # This lets us fill at any ask ≤ max_price (capturing better entries)
        # while ensuring edge is always ≥ threshold at the fill price.
        effective_threshold = (
            sig.threshold_result.effective_threshold
            if sig.threshold_result else 0.12
        )
        max_price = round(sig.top_model_prob - effective_threshold, 4)
        max_price = max(max_price, sig.top_yes_ask)   # never below current ask

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
            )
            push_event("position_opened", {"market_id": market_id, "station": station,
                                           "bucket_lower": sig.top_bucket, "entry_price": max_price,
                                           "contracts": sig.kelly_contracts, "stake_usd": sig.kelly_stake_usd})
            push_event("state_update", rm.summary())
            logger.info(
                "[Tier3] Order filled: %s | %d contracts @ $%.2f | stake $%.2f",
                market_id, sig.kelly_contracts, max_price, sig.kelly_stake_usd,
            )
        else:
            logger.error("[Tier3] Order failed for %s: %s", market_id, result.error)
            alert_order_failure(station, market_id, result.error or "unknown error")
            push_alert(f"Order failed — {station}", result.error or "unknown", "ERROR")
            # TODO (kalshi_client): add fill-retry with fresh edge check
            # retry up to ORDER_FILL_RETRY_MAX times with ORDER_FILL_RETRY_WAIT_SEC gap

    # Log all WATCH/SKIP decisions that made it through the loop without trading
    sheets = get_sheets_logger()
    for sig in signals.values():
        if sig.decision in ("WATCH", "SKIP", "HARD_SKIP"):
            sheets.log_skipped_signal(sig, sig.decision)

    summary = rm.summary()
    sheets.update_dashboard(summary, mode="DEMO" if USE_DEMO else "LIVE")
    logger.info(
        "[Tier3] Cycle complete | bankroll=$%.2f | open=%d | daily P/L=$%+.2f",
        summary["bankroll"], summary["open_positions"], summary["daily_pnl"],
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
        new_sig.threshold_result.effective_threshold
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
        sig.threshold_result.effective_threshold if sig.threshold_result else 0.12
    )
    max_price = round(sig.top_model_prob - effective_threshold, 4)
    max_price = max(max_price, snap.yes_ask)

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
            bucket_lower=bucket_lower,
            contracts=sig.kelly_contracts,
            entry_price=max_price,
            event_date=event_date,
        )
        get_sheets_logger().log_trade_opened(
            station=station,
            event_date=event_date,
            market_id=market_id,
            bucket_lower=bucket_lower,
            entry_price=max_price,
            contracts=sig.kelly_contracts,
            stake_usd=sig.kelly_stake_usd,
            sig=sig,
        )
        logger.info(
            "[LiqRetry] Entry complete: %s | %d contracts @ $%.2f",
            market_id, sig.kelly_contracts, max_price,
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
    Initialize and start the APScheduler with all three tiers plus
    market-open trigger, settlement sweep, and startup reconciliation.

    Schedule summary (all UTC):
      Tier 1  — every 5 min         — TAF amendment monitor
      Tier 2  — every 30 min        — METAR running high + position management
      Tier 3  — 00:30 / 06:30 / 12:30 / 18:30  — GFS-aligned signal pass
      Tier 3  — 14:05               — Kalshi Day-1 market open (5 min after open)
      Sweep   — 09:00               — Morning settlement sweep
    """
    global _scheduler, _risk_manager, _kalshi

    _risk_manager = RiskManager()
    _kalshi       = KalshiClient(demo=USE_DEMO)

    # Register shutdown handlers — fire on Ctrl+C or system SIGTERM (VPS stop/reboot)
    signal.signal(signal.SIGINT,  _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    scheduler = BackgroundScheduler(timezone="UTC")

    # Tier 1 — every 5 minutes
    scheduler.add_job(
        tier1_taf_monitor,
        trigger=IntervalTrigger(seconds=TIER1_INTERVAL_SECONDS),
        id="tier1_taf",
        name="TAF Amendment Monitor",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60,
    )

    # Tier 2 — every 30 minutes
    scheduler.add_job(
        tier2_metar_and_positions,
        trigger=IntervalTrigger(seconds=TIER2_INTERVAL_SECONDS),
        id="tier2_metar",
        name="METAR + Position Manager",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )

    # Price opportunity scanner — every PRICE_SCAN_INTERVAL_MIN minutes (default 60 min)
    # Catches intraday price drops that create edge opportunities between Tier 3 cycles.
    # Reuses last Tier 3 signal's probability distribution — no model recomputation.
    scheduler.add_job(
        tier2b_price_opportunity_scan,
        trigger=IntervalTrigger(minutes=PRICE_SCAN_INTERVAL_MIN),
        id="price_scan",
        name="Price Opportunity Scanner",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    # Tier 3 — GFS cycle aligned: 00:30, 06:30, 12:30, 18:30 UTC
    for hour in [0, 6, 12, 18]:
        scheduler.add_job(
            tier3_full_signal_pass,
            trigger=CronTrigger(hour=hour, minute=30, timezone="UTC"),
            id=f"tier3_{hour:02d}z",
            name=f"Full Signal Pass {hour:02d}Z+30",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )

    # Tier 3 — Kalshi Day-1 market open: fires at 14:05 UTC every day
    # Kalshi opens tomorrow's markets at ~10:00 AM EDT (14:00 UTC).
    # We wait 5 minutes to let the book settle before scanning.
    # Uses _tier3_day1_market_open() wrapper so event_date = tomorrow at runtime.
    scheduler.add_job(
        _tier3_day1_market_open,
        trigger=CronTrigger(
            hour=MARKET_OPEN_UTC_HOUR,
            minute=MARKET_OPEN_UTC_MINUTE,
            timezone="UTC",
        ),
        id="tier3_market_open",
        name=f"Full Signal Pass — Market Open {MARKET_OPEN_UTC_HOUR:02d}:{MARKET_OPEN_UTC_MINUTE:02d}Z",
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
        misfire_grace_time=600,   # 10-min grace — LCD sometimes publishes late
    )

    scheduler.start()
    _scheduler = scheduler

    logger.info(
        "Scheduler started | Tier1=5min | Tier2=30min | PriceScan=%dmin | "
        "Tier3=00/06/12/18Z+30 + %02d:%02dZ market-open | "
        "Settlement=%02d:00Z | mode=%s",
        PRICE_SCAN_INTERVAL_MIN,
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
    """Run Tier 3 immediately on startup in a background thread."""
    def _run():
        try:
            tier3_full_signal_pass()
        except Exception as exc:
            logger.error("Initial signal pass failed: %s", exc)

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
