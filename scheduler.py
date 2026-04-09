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

import threading
from datetime import date, datetime, timezone
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from utils.logging_config import setup_logging
from scripts.signal_engine import run_signal_pass, TradeSignal
from scripts.taf_interpreter import interpret_taf, get_metar
from kalshi_client import KalshiClient, build_market_id
from risk import RiskManager
from config import (
    STATIONS,
    TIER1_INTERVAL_SECONDS,
    TIER2_INTERVAL_SECONDS,
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
    Fetch current METAR obs for all stations.
    Update open position P/L and evaluate exit conditions.
    Execute exits if triggered.
    """
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

            for market_id, pos in station_positions.items():
                # Fetch current market bid/ask
                snap = kalshi.get_market_snapshot(
                    station, date.fromisoformat(pos.event_date), pos.bucket_lower
                )
                if snap is None:
                    logger.warning("[Tier2] No snapshot for %s", market_id)
                    continue

                # Get current edge from latest signal
                with _latest_signals_lock:
                    sig = _latest_signals.get(station)
                current_edge = sig.top_edge if sig and sig.top_bucket == pos.bucket_lower else 0.0

                # Update position and evaluate exit
                exit_decision = rm.update_position(
                    market_id=market_id,
                    current_bid=snap.yes_bid,
                    current_ask=snap.yes_ask,
                    current_edge=current_edge,
                    current_obs_temp=obs_temp,
                )

                logger.info(
                    "[Tier2] %s bid=%.2f P/L=$%+.4f (%.1f%%) | %s",
                    market_id, snap.yes_bid,
                    pos.unrealized_pnl, pos.pnl_pct,
                    exit_decision.reason,
                )

                if exit_decision.should_exit:
                    _execute_exit(market_id, pos, snap.yes_bid, exit_decision.reason, kalshi, rm)

        except Exception as exc:
            logger.error("[Tier2] Error processing %s: %s", station, exc)


def _execute_exit(market_id, pos, bid_price, reason, kalshi, rm):
    """Place sell order and record close."""
    logger.info("[Tier2] Executing exit: %s | reason: %s", market_id, reason)

    result = kalshi.close_position(market_id, pos.contracts, bid_price)
    if result.success:
        realized = rm.close_position(market_id, bid_price, reason)
        logger.info("[Tier2] Exit complete: %s | realized P/L $%+.4f", market_id, realized)
    else:
        logger.error("[Tier2] Exit order failed for %s: %s", market_id, result.error)


# ---------------------------------------------------------------------------
# Tier 3 — Full signal pass + order execution (every 6 hours)
# ---------------------------------------------------------------------------

def tier3_full_signal_pass():
    """
    Full signal generation for all stations.
    Places orders for TRADE decisions that pass risk checks.
    """
    logger.info("[Tier3] Full signal pass starting")
    rm     = get_risk_manager()
    kalshi = get_kalshi()

    if rm.is_halted:
        logger.info("[Tier3] Bot halted — skipping signal pass")
        return

    event_date = date.today()
    signals    = run_signal_pass(event_date=event_date, bankroll=rm.state.bankroll)

    # Update shared signal store
    with _latest_signals_lock:
        _latest_signals.update(signals)

    # Execute trades
    for station, sig in signals.items():
        if sig.decision != "TRADE":
            continue

        market_id = build_market_id(station, event_date, sig.top_bucket)

        # Skip if already have a position for this market
        if market_id in rm.state.positions:
            logger.info("[Tier3] Already have position in %s — skipping", market_id)
            continue

        # Risk check
        ok, reason = rm.can_open_position(sig.kelly_stake_usd)
        if not ok:
            logger.warning("[Tier3] Risk check failed for %s: %s", station, reason)
            continue

        # Place order
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
            logger.info(
                "[Tier3] Order placed: %s | %d contracts @ $%.2f | stake $%.2f",
                market_id, sig.kelly_contracts, sig.top_yes_ask, sig.kelly_stake_usd,
            )
        else:
            logger.error("[Tier3] Order failed for %s: %s", market_id, result.error)

    summary = rm.summary()
    logger.info(
        "[Tier3] Cycle complete | bankroll=$%.2f | open=%d | daily P/L=$%+.2f",
        summary["bankroll"], summary["open_positions"], summary["daily_pnl"],
    )


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

def start_scheduler() -> BackgroundScheduler:
    """
    Initialize and start the APScheduler with all three tiers.
    Returns the scheduler instance (also stored globally for dashboard access).
    """
    global _scheduler, _risk_manager, _kalshi

    _risk_manager = RiskManager()
    _kalshi       = KalshiClient(demo=USE_DEMO)

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

    scheduler.start()
    _scheduler = scheduler

    logger.info(
        "Scheduler started | Tier1=5min | Tier2=30min | Tier3=00/06/12/18Z+30 | mode=%s",
        "DEMO" if USE_DEMO else "LIVE",
    )

    # Run an immediate Tier 3 pass on startup so dashboard has data right away
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
