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
from datetime import date, datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from utils.logging_config import setup_logging
from utils.asos_live import running_max_with_confluence
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
from config import (
    STATIONS,
    STATION_TIMEZONES,
    STATION_PEAK_HOURS,
    KALSHI_BUCKET_LOWER_TAIL,
    KALSHI_BUCKET_UPPER_TAIL,
    KALSHI_BUCKET_STARTS,
    EXPANSION_EDGE_MIN,
    EXPANSION_CURRENT_EDGE_MAX,
    SIGNIFICANT_REPOSITION_EDGE_MIN,
    MAJOR_REPOSITION_EDGE_MIN,
    TIER1_INTERVAL_SECONDS,
    TIER2_INTERVAL_SECONDS,
    STALE_SIGNAL_HOURS,
    MARKET_OPEN_UTC_HOUR,
    MARKET_OPEN_UTC_MINUTE,
    SETTLEMENT_SWEEP_UTC_HOUR,
    TIER3_RETRY_INTERVAL_MIN,
    TIER3_MAX_RETRIES,
    DAILY_LOSS_LIMIT_PCT,
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
    Fetch current METAR obs and IEM 1-min running max for all stations.
    Update open position P/L and evaluate exit conditions including:
      - undershoot warning (approaching peak hour, tracking low)
      - undershoot hard exit (past peak hour, definitive miss)
      - overshoot exit (before peak hour, running max near bucket upper)
      - early profit exit (bid ≥ 85¢)
    Execute exits if triggered. Log warnings for manual review.
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

                # Peak heating hour for this station and event month
                event_month       = date.fromisoformat(pos.event_date).month
                peak_heating_hour = STATION_PEAK_HOURS.get(station, {}).get(event_month, 15)

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

    signals    = run_signal_pass(event_date=event_date, bankroll=rm.state.bankroll)

    # Update shared signal store
    with _latest_signals_lock:
        _latest_signals.update(signals)

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

        # ── Market open guard ─────────────────────────────────────────────
        snap_check = kalshi.get_market_snapshot(station, event_date, sig.top_bucket)
        if snap_check is None or not snap_check.is_open:
            logger.info("[Tier3] Market not open for %s bucket %d — skipping", station, sig.top_bucket)
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
            logger.info(
                "[Tier3] Order filled: %s | %d contracts @ $%.2f (max $%.2f) | stake $%.2f",
                market_id, sig.kelly_contracts, max_price, max_price, sig.kelly_stake_usd,
            )
        else:
            logger.error("[Tier3] Order failed for %s: %s", market_id, result.error)
            alert_order_failure(station, market_id, result.error or "unknown error")
            # TODO (kalshi_client): add fill-retry with fresh edge check
            # retry up to ORDER_FILL_RETRY_MAX times with ORDER_FILL_RETRY_WAIT_SEC gap

    summary = rm.summary()
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
    event_month  = event_date.month
    peak_hour    = STATION_PEAK_HOURS.get(station, {}).get(event_month, 15)

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
    event_month  = event_date.month
    peak_hour    = STATION_PEAK_HOURS.get(station, {}).get(event_month, 15)
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

    for market_id, pos in positions_to_check.items():
        if market_id in settlements:
            settlement_value = settlements[market_id]
            realized = rm.close_position(
                market_id,
                exit_price=settlement_value,
                reason=f"Settlement sweep — LCD verified at ${settlement_value:.2f}",
            )
            alert_settlement_detected(pos.station, market_id, realized)
            logger.info(
                "[Settlement] %s settled | value=%.2f | P/L $%+.4f",
                market_id, settlement_value, realized,
            )
        else:
            logger.info("[Settlement] %s not yet in settlements feed — leaving open", market_id)

    summary = rm.summary()
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

    # Tier 3 — Kalshi Day-1 market open: fires at 14:05 UTC every day
    # Kalshi opens tomorrow's markets at ~10:00 AM EDT (14:00 UTC).
    # We wait 5 minutes to let the book settle before scanning.
    scheduler.add_job(
        tier3_full_signal_pass,
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
        "Scheduler started | Tier1=5min | Tier2=30min | "
        "Tier3=00/06/12/18Z+30 + %02d:%02dZ market-open | "
        "Settlement=%02d:00Z | mode=%s",
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
