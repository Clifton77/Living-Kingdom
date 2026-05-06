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
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import pytz

from config import (
    DRY_RUN,
    HRRR_COLD_BIAS_F,
    HRRR_MATERIAL_MOVE_F,
    HRRR_START_UTC_HOUR,
    HRRR_START_UTC_MINUTE,
    KALSHI_SETTLEMENT_STATION,
    STATION_COORDS,
    STATION_TIMEZONES,
    STATIONS,
    STARTING_BANKROLL,
    STOP_LOSS_PCT,
)
from kalshi_client import KalshiClient, MarketSnapshot
from signal_engine_v2 import HRRRSignalEngine, TradeSignal
from utils.asos_live import get_best_obs_temp, get_running_max
from utils.hrrr_fetcher import fetch_station_tmax
from utils.peak_hours import get_peak_hour

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("scheduler_v2")

ET = pytz.timezone("America/New_York")
MORNING_SCAN_ET_HOUR   = 8
MORNING_SCAN_ET_MINUTE = 30
EXIT_POLL_INTERVAL     = 300   # seconds between exit checks when positions are open


@dataclass
class OpenPositionV2:
    station:      str
    market_id:    str
    bucket_lower: int
    contracts:    int
    entry_price:  float        # per contract, 0–1
    side:         str          # "YES" or "NO"
    event_date:   date
    entry_time:   datetime
    # Updated each poll cycle
    current_bid:    float          = 0.0
    current_ask:    float          = 0.0
    unrealized_pnl: float          = 0.0
    current_obs_f:  float | None   = None   # latest METAR temp
    running_max_f:  float | None   = None   # today's high so far (IEM 1-min)
    last_checked:   datetime | None = None


_open_positions: dict[str, OpenPositionV2] = {}
_station_snapshots: dict[str, list[MarketSnapshot]] = {}  # all buckets per station
_positions_lock = threading.Lock()
_trade_history: list[dict] = []
_engine: HRRRSignalEngine | None = None


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
    if result.success:
        now = datetime.now(timezone.utc)
        pos = OpenPositionV2(
            station=signal.station,
            market_id=signal.market_id,
            bucket_lower=signal.bucket_lower,
            contracts=signal.contracts,
            entry_price=signal.kalshi_price,
            side=signal.side,
            event_date=signal.event_date,
            entry_time=now,
        )
        with _positions_lock:
            _open_positions[signal.market_id] = pos
        _trade_history.append({
            "ts":           now.strftime("%H:%Mz"),
            "type":         "OPEN",
            "station":      signal.station,
            "side":         signal.side,
            "bucket_lower": signal.bucket_lower,
            "contracts":    signal.contracts,
            "price":        signal.kalshi_price,
            "signal_type":  signal.signal_type,
            "delta_f":      signal.hrrr_delta_f,
            "edge":         round(signal.edge, 3),
        })
    else:
        logger.error("%s: order failed — %s", signal.station, result.error)
    return result.success


def _exit_position(client: KalshiClient, pos: OpenPositionV2, bid: float, reason: str) -> bool:
    logger.info(
        "[Exit] %s %s bucket %d  bid=%.2f  reason=%s",
        pos.station, pos.side, pos.bucket_lower, bid, reason,
    )
    result = client.close_position(
        pos.market_id, pos.contracts, bid, entry_side=pos.side.lower()
    )
    if result.success:
        pnl = (bid - pos.entry_price) * pos.contracts
        with _positions_lock:
            _open_positions.pop(pos.market_id, None)
        _trade_history.append({
            "ts":           datetime.now(timezone.utc).strftime("%H:%Mz"),
            "type":         "CLOSE",
            "station":      pos.station,
            "side":         pos.side,
            "bucket_lower": pos.bucket_lower,
            "contracts":    pos.contracts,
            "entry_price":  pos.entry_price,
            "exit_price":   bid,
            "pnl":          round(pnl, 2),
            "reason":       reason,
        })
        logger.info(
            "[Exit] Complete: %s  entry=%.2f  exit=%.2f  pnl=$%+.2f",
            pos.market_id, pos.entry_price, bid, pnl,
        )
    else:
        logger.error("[Exit] Failed: %s — %s", pos.market_id, result.error)
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
        with _positions_lock:
            _station_snapshots[station] = markets

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


class ExitMonitor:
    """
    Background thread that checks open positions every EXIT_POLL_INTERVAL seconds.

    Per poll cycle:
      - Groups positions by station
      - Fetches markets + observations once per station (not once per position)
      - Updates _station_snapshots (all buckets) for dashboard consumption
      - Updates per-position fields: current_bid, current_ask, unrealized_pnl,
        current_obs_f, running_max_f, last_checked

    Exit conditions (in priority order):
      1. Market closed — remove from tracking, let settlement sweep handle it
      2. Stop-loss — bid fell to <= STOP_LOSS_PCT of entry price
      3. HRRR reversal — TMAX moved away from YES bucket, or into NO bucket
    """

    def __init__(self, client: KalshiClient, engine: HRRRSignalEngine):
        self._client = client
        self._engine = engine
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="exit-monitor"
        )

    def start(self) -> None:
        logger.info("ExitMonitor started — poll every %ds", EXIT_POLL_INTERVAL)
        self._thread.start()

    def _loop(self) -> None:
        while True:
            time.sleep(EXIT_POLL_INTERVAL)
            with _positions_lock:
                if not _open_positions:
                    continue
                snapshot = dict(_open_positions)

            # Group by station — fetch markets + obs once per station
            by_station: dict[str, list[tuple[str, OpenPositionV2]]] = {}
            for market_id, pos in snapshot.items():
                by_station.setdefault(pos.station, []).append((market_id, pos))

            for station, entries in by_station.items():
                event_date = entries[0][1].event_date
                markets = _get_markets(self._client, station, event_date)

                try:
                    obs_temp    = get_best_obs_temp(station)
                    running_max = get_running_max(station, event_date)
                except Exception as exc:
                    logger.warning("[ExitMonitor] %s: obs fetch failed — %s", station, exc)
                    obs_temp    = None
                    running_max = None

                with _positions_lock:
                    _station_snapshots[station] = markets

                for market_id, pos in entries:
                    try:
                        self._check(market_id, pos, markets, obs_temp, running_max)
                    except Exception as exc:
                        logger.warning("[ExitMonitor] Error checking %s: %s", market_id, exc)

    def _check(
        self,
        market_id: str,
        pos: OpenPositionV2,
        markets: list[MarketSnapshot],
        obs_temp: float | None,
        running_max: float | None,
    ) -> None:
        market = next((m for m in markets if m.market_id == market_id), None)

        if market is None or not market.is_open:
            logger.info("[ExitMonitor] %s market closed — pending settlement", market_id)
            with _positions_lock:
                _open_positions.pop(market_id, None)
            return

        current_bid = market.yes_bid if pos.side == "YES" else (1.0 - market.yes_ask)
        current_ask = market.yes_ask if pos.side == "YES" else (1.0 - market.yes_bid)

        with _positions_lock:
            pos.current_bid    = current_bid
            pos.current_ask    = current_ask
            pos.unrealized_pnl = (current_bid - pos.entry_price) * pos.contracts
            pos.current_obs_f  = obs_temp
            pos.running_max_f  = running_max
            pos.last_checked   = datetime.now(timezone.utc)

        stop_level = pos.entry_price * STOP_LOSS_PCT
        if current_bid <= stop_level:
            _exit_position(
                self._client, pos, current_bid,
                f"stop-loss: bid {current_bid:.2f} <= {stop_level:.2f} ({STOP_LOSS_PCT:.0%} of entry)",
            )
            return

        engine_state = self._engine._state.get(pos.station)
        if engine_state and engine_state.last_tmax_f is not None:
            bucket_center = float(pos.bucket_lower + 1)
            dist = abs(engine_state.last_tmax_f - bucket_center)
            if pos.side == "YES" and dist >= HRRR_MATERIAL_MOVE_F:
                _exit_position(
                    self._client, pos, current_bid,
                    f"HRRR reversal: TMAX={engine_state.last_tmax_f:.1f}F, {dist:.1f}F from bucket {pos.bucket_lower}",
                )
                return
            if pos.side == "NO" and dist < 1.0:
                _exit_position(
                    self._client, pos, current_bid,
                    f"HRRR reversed into NO bucket: TMAX={engine_state.last_tmax_f:.1f}F",
                )
                return


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
    global _engine
    client  = KalshiClient()
    _engine = HRRRSignalEngine(bankroll=STARTING_BANKROLL)
    monitor = ExitMonitor(client, _engine)
    monitor.start()
    logger.info("scheduler_v2 started — dry_run=%s", DRY_RUN)

    while True:
        now_utc = datetime.now(timezone.utc)
        event_date = now_utc.date()
        run_time = now_utc.replace(minute=0, second=0, microsecond=0)

        # Only run during operating window
        if now_utc.hour >= HRRR_START_UTC_HOUR:
            run_cycle(_engine, client, run_time, event_date)
        else:
            logger.info("Before operating window — waiting.")

        next_run = _next_run_time()
        _sleep_until(next_run)


if __name__ == "__main__":
    main()
