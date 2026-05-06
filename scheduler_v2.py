"""
scheduler_v2.py

Hourly HRRR-driven trading scheduler. Replaces scheduler.py.

Loop:
  - Wakes every hour aligned to HRRR run availability (~:30 past the hour)
  - Fetches latest HRRR TMAX for all active stations
  - Runs signal engine — detects divergence from previous run
  - Morning scan at 8:30am ET: flags overpriced buckets
  - Executes trades in dry-run or live mode
  - Hard stop at each station's peak hour
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone

import pytz

from config import (
    HRRR_COLD_BIAS_F,
    HRRR_MATERIAL_MOVE_F,
    HRRR_START_UTC_HOUR,
    HRRR_START_UTC_MINUTE,
    KALSHI_SETTLEMENT_STATION,
    STATION_COORDS,
    STATION_TIMEZONES,
    STATIONS,
    STARTING_BANKROLL,
)
from kalshi_client import KalshiClient, MarketSnapshot
from signal_engine_v2 import HRRRSignalEngine, TradeSignal
from utils.hrrr_fetcher import fetch_station_tmax
from utils.peak_hours import get_peak_hour

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("scheduler_v2")

ET = pytz.timezone("America/New_York")
MORNING_SCAN_ET_HOUR = 8
MORNING_SCAN_ET_MINUTE = 30


def _station_hrrr_coords(station: str) -> tuple[float, float]:
    settlement = KALSHI_SETTLEMENT_STATION.get(station, station)
    return STATION_COORDS[settlement]


def _peak_utc(station: str, event_date: date) -> int:
    tz = pytz.timezone(STATION_TIMEZONES[station])
    local_hour = get_peak_hour(station, event_date)
    local_dt = tz.localize(
        datetime(event_date.year, event_date.month, event_date.day, local_hour)
    )
    return local_dt.utctimetuple().tm_hour


def _active_stations(event_date: date, now_utc: datetime) -> list[str]:
    """Stations where peak hour hasn't passed yet."""
    active = []
    for s in STATIONS:
        peak = _peak_utc(s, event_date)
        if now_utc.hour < peak:
            active.append(s)
    return active


def _get_markets(client: KalshiClient, station: str, event_date: date) -> list[MarketSnapshot]:
    """Fetch Kalshi bucket markets for a station/date. Returns [] on failure."""
    try:
        return client.get_markets_for_station_date(station, event_date)
    except Exception as e:
        logger.warning("%s: failed to fetch markets — %s", station, e)
        return []


def _execute_trade(client: KalshiClient, signal: TradeSignal) -> bool:
    """
    Place order via kalshi_client. Dry run is handled transparently inside
    place_order_with_fill_check — no separate DRY_RUN check needed here.
    """
    logger.info(
        "%s %s bucket %d  contracts=%d @ %.2f  stake=$%.2f  edge=%.3f  p=%.1f%%  delta=%.1fF",
        signal.station, signal.side, signal.bucket_lower,
        signal.contracts, signal.kalshi_price,
        signal.stake, signal.edge, signal.p_model * 100, signal.hrrr_delta_f,
    )
    result = client.place_order_with_fill_check(
        market_id=signal.market_id,
        contracts=signal.contracts,
        limit_price=signal.kalshi_price,
        side=signal.side.lower(),
    )
    if not result.success:
        logger.error("%s: order failed — %s", signal.station, result.error)
    return result.success


def _is_morning_scan(now_et: datetime) -> bool:
    return (
        now_et.hour == MORNING_SCAN_ET_HOUR
        and now_et.minute >= MORNING_SCAN_ET_MINUTE
    )


def run_cycle(
    engine: HRRRSignalEngine,
    client: KalshiClient,
    run_time: datetime,
    event_date: date,
) -> None:
    """One HRRR cycle: fetch, signal, trade."""
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ET)
    morning_scan = _is_morning_scan(now_et)

    active = _active_stations(event_date, now_utc)
    if not active:
        logger.info("All stations past peak — no trades.")
        return

    logger.info(
        "HRRR cycle %s — %d active stations%s",
        run_time.strftime("%Y-%m-%d %Hz"),
        len(active),
        " [MORNING SCAN]" if morning_scan else "",
    )

    # Build coord + peak maps for active stations
    coords = {s: _station_hrrr_coords(s) for s in active}
    peaks = {s: _peak_utc(s, event_date) for s in active}

    # Fetch HRRR
    tmax_by_station = fetch_station_tmax(run_time, coords, peaks)

    for station in active:
        raw_tmax = tmax_by_station.get(station)
        if raw_tmax is None:
            logger.warning("%s: HRRR fetch returned None", station)
            continue

        markets = _get_markets(client, station, event_date)
        if not markets:
            continue

        snapshot = engine.build_snapshot(station, run_time, raw_tmax)
        signals = engine.update(
            station=station,
            snapshot=snapshot,
            markets=markets,
            event_date=event_date,
            is_morning_scan=morning_scan,
        )

        for sig in signals:
            _execute_trade(client, sig)

        logger.debug(
            "%s: HRRR=%.1fF corrected=%.1fF bucket=%d signals=%d",
            station, raw_tmax, snapshot.tmax_corrected_f,
            snapshot.peak_bucket_lower, len(signals),
        )


def _next_run_time() -> datetime:
    """Next HRRR run time to check (~:30 past each UTC hour)."""
    now = datetime.now(timezone.utc)
    candidate = now.replace(minute=HRRR_START_UTC_MINUTE, second=0, microsecond=0)
    if now >= candidate:
        candidate += timedelta(hours=1)
    return candidate


def _sleep_until(target: datetime) -> None:
    delta = (target - datetime.now(timezone.utc)).total_seconds()
    if delta > 0:
        logger.info("Sleeping %.0f seconds until %s", delta, target.strftime("%H:%Mz"))
        time.sleep(delta)


def main() -> None:
    client = KalshiClient()
    engine = HRRRSignalEngine(bankroll=STARTING_BANKROLL)
    logger.info("scheduler_v2 started — dry_run=%s", DRY_RUN)

    while True:
        now_utc = datetime.now(timezone.utc)
        event_date = now_utc.date()
        run_time = now_utc.replace(minute=0, second=0, microsecond=0)

        # Only run during operating window
        if now_utc.hour >= HRRR_START_UTC_HOUR:
            run_cycle(engine, client, run_time, event_date)
        else:
            logger.info("Before operating window — waiting.")

        next_run = _next_run_time()
        _sleep_until(next_run)


if __name__ == "__main__":
    main()
