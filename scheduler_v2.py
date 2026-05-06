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

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytz

from config import (
    DRY_RUN,
    HRRR_COLD_BIAS_F,
    HRRR_MATERIAL_MOVE_F,
    HRRR_START_UTC_HOUR,
    HRRR_START_UTC_MINUTE,
    KALSHI_SETTLEMENT_STATION,
    LOW_PRICE_STOP_THRESHOLD,
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
from utils.sheets import get_sheets_logger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("scheduler_v2")

ET = pytz.timezone("America/New_York")
MORNING_SCAN_ET_HOUR   = 8
MORNING_SCAN_ET_MINUTE = 30
EXIT_POLL_INTERVAL         = 300   # seconds between exit checks (normal)
EXIT_NEAR_HOUR_INTERVAL    = 60    # seconds during :47–:05 window (ASOS posts ~:53–:58)
EXIT_NEAR_HOUR_START_MIN   = 47    # minute-of-hour where fast polling begins
EXIT_NEAR_HOUR_END_MIN     = 5     # minute-of-hour where fast polling ends


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
    current_bid:       float          = 0.0
    current_ask:       float          = 0.0
    unrealized_pnl:    float          = 0.0
    current_obs_f:     float | None   = None   # latest METAR temp
    running_max_f:     float | None   = None   # today's high so far (IEM 1-min)
    last_checked:      datetime | None = None
    pending_settlement: bool           = False  # market closed, awaiting Kalshi confirmation


_open_positions: dict[str, OpenPositionV2] = {}
_station_snapshots: dict[str, list[MarketSnapshot]] = {}  # all buckets per station
_positions_lock = threading.Lock()
_trade_history: list[dict] = []
_engine: HRRRSignalEngine | None = None

# Running P&L counters — updated by _record_settlement()
_bankroll:   float = STARTING_BANKROLL
_daily_pnl:  float = 0.0
_wins_today:  int  = 0
_losses_today: int = 0

_STATE_DIR       = Path(__file__).parent / "state"
_POSITIONS_FILE  = _STATE_DIR / "positions.json"
_HRRR_STATE_FILE = _STATE_DIR / "hrrr_state.json"


# ---------------------------------------------------------------------------
# State persistence — positions and HRRR baselines survive restarts
# ---------------------------------------------------------------------------

def _save_positions() -> None:
    _STATE_DIR.mkdir(exist_ok=True)
    data = {}
    with _positions_lock:
        for mid, pos in _open_positions.items():
            data[mid] = {
                "station":           pos.station,
                "market_id":         pos.market_id,
                "bucket_lower":      pos.bucket_lower,
                "contracts":         pos.contracts,
                "entry_price":       pos.entry_price,
                "side":              pos.side,
                "event_date":        pos.event_date.isoformat(),
                "entry_time":        pos.entry_time.isoformat(),
                "pending_settlement": pos.pending_settlement,
            }
    _POSITIONS_FILE.write_text(json.dumps(data, indent=2))


def _load_positions() -> None:
    if not _POSITIONS_FILE.exists():
        return
    try:
        data = json.loads(_POSITIONS_FILE.read_text())
        count = 0
        for mid, d in data.items():
            _open_positions[mid] = OpenPositionV2(
                station=d["station"],
                market_id=d["market_id"],
                bucket_lower=d["bucket_lower"],
                contracts=d["contracts"],
                entry_price=d["entry_price"],
                side=d["side"],
                event_date=date.fromisoformat(d["event_date"]),
                entry_time=datetime.fromisoformat(d["entry_time"]),
                pending_settlement=d.get("pending_settlement", False),
            )
            count += 1
        logger.info("Restored %d open position(s) from disk", count)
    except Exception as exc:
        logger.warning("Failed to load positions: %s", exc)


def _save_hrrr_state(engine: HRRRSignalEngine) -> None:
    _STATE_DIR.mkdir(exist_ok=True)
    data = {}
    for station, st in engine._state.items():
        if st.last_tmax_f is None:
            continue
        data[station] = {
            "last_tmax_f":   st.last_tmax_f,
            "last_run_time": st.last_run_time.isoformat() if st.last_run_time else None,
            "traded": [
                [ev_date.isoformat(), bucket_lo, side]
                for (ev_date, bucket_lo, side) in st.traded
            ],
        }
    _HRRR_STATE_FILE.write_text(json.dumps(data, indent=2))


def _load_hrrr_state(engine: HRRRSignalEngine) -> None:
    if not _HRRR_STATE_FILE.exists():
        return
    try:
        data = json.loads(_HRRR_STATE_FILE.read_text())
        count = 0
        for station, sdata in data.items():
            if station not in engine._state:
                continue
            st = engine._state[station]
            st.last_tmax_f  = sdata["last_tmax_f"]
            st.last_run_time = (
                datetime.fromisoformat(sdata["last_run_time"])
                if sdata.get("last_run_time") else None
            )
            for entry in sdata.get("traded", []):
                ev_date_str, bucket_lo, side = entry
                st.traded.add((date.fromisoformat(ev_date_str), int(bucket_lo), side))
            count += 1
        logger.info("Restored HRRR state for %d station(s) from disk", count)
    except Exception as exc:
        logger.warning("Failed to load HRRR state: %s", exc)


# ---------------------------------------------------------------------------
# Settlement helpers
# ---------------------------------------------------------------------------

def _record_settlement(pos: OpenPositionV2, settlement_value: float, reason: str) -> None:
    """
    Record a settled position without placing any Kalshi order.
    settlement_value: per-contract payout (0.0 = lost, 1.0 = won — from Kalshi's perspective).
    For a YES position that won: payout = 1.0/contract; for YES that lost: payout = 0.0.
    For a NO position that won: payout = 1.0/contract; for NO that lost: payout = 0.0.
    pnl = (settlement_value - entry_price) * contracts
    """
    global _bankroll, _daily_pnl, _wins_today, _losses_today

    pnl = (settlement_value - pos.entry_price) * pos.contracts
    _bankroll  += pnl
    _daily_pnl += pnl
    if pnl >= 0:
        _wins_today  += 1
    else:
        _losses_today += 1

    with _positions_lock:
        _open_positions.pop(pos.market_id, None)

    _save_positions()

    _trade_history.append({
        "ts":           datetime.now(timezone.utc).strftime("%H:%Mz"),
        "type":         "SETTLE",
        "station":      pos.station,
        "side":         pos.side,
        "bucket_lower": pos.bucket_lower,
        "contracts":    pos.contracts,
        "entry_price":  pos.entry_price,
        "exit_price":   settlement_value,
        "pnl":          round(pnl, 2),
        "reason":       reason,
    })

    logger.info(
        "[Settlement] %s  entry=%.2f  settle=%.2f  pnl=$%+.2f  bankroll=$%.2f  reason=%s",
        pos.market_id, pos.entry_price, settlement_value, pnl, _bankroll, reason,
    )


def _build_summary() -> dict:
    total_trades = sum(1 for t in _trade_history if t["type"] in ("CLOSE", "SETTLE"))
    wins   = sum(1 for t in _trade_history if t["type"] in ("CLOSE", "SETTLE") and t.get("pnl", 0) >= 0)
    losses = total_trades - wins
    return {
        "bankroll":          round(_bankroll, 2),
        "available_capital": round(_bankroll, 2),
        "daily_pnl":         round(_daily_pnl, 2),
        "realized_pnl":      round(_bankroll - STARTING_BANKROLL, 2),
        "open_positions":    len(_open_positions),
        "trade_count_today": total_trades,
        "wins_today":        wins,
        "losses_today":      losses,
        "win_rate":          round(wins / total_trades * 100, 1) if total_trades > 0 else 0.0,
    }


def _run_settlement_sweep(client: KalshiClient) -> None:
    """
    Check every pending-settlement position (and any position whose event_date
    is in the past) against Kalshi.  For each confirmed settlement:
      - call _record_settlement()
      - log to Google Sheets (trade closed + dashboard)
    Runs at startup (to catch overnight settlements) and every morning at 09:00z.
    """
    sheets = get_sheets_logger()
    now_utc = datetime.now(timezone.utc)
    today   = now_utc.date()

    with _positions_lock:
        candidates = {
            mid: pos for mid, pos in _open_positions.items()
            if pos.pending_settlement or pos.event_date < today
        }

    if not candidates:
        logger.info("[SettlementSweep] No pending positions to check.")
        return

    logger.info("[SettlementSweep] Checking %d candidate(s)…", len(candidates))

    # Fetch bulk feed for all relevant event dates
    dates_needed = {pos.event_date for pos in candidates.values()}
    feed_results: dict[str, float] = {}
    for ev_date in dates_needed:
        try:
            feed_results.update(client.get_settled_markets(ev_date))
        except Exception as exc:
            logger.warning("[SettlementSweep] feed fetch failed for %s: %s", ev_date, exc)

    settled_count = 0
    for market_id, pos in candidates.items():
        # Try the bulk feed first (fast, but misses zero-payout positions)
        if market_id in feed_results:
            settle_val = feed_results[market_id]
            reason = "settlement-feed"
        else:
            # Per-market fallback — handles losing positions the feed skips
            settle_val = client.get_market_result(market_id)
            if settle_val is None:
                logger.info("[SettlementSweep] %s not yet finalized — will retry next sweep", market_id)
                continue
            reason = "market-result"

        _record_settlement(pos, settle_val, reason)
        sheets.log_trade_closed(
            station=pos.station,
            market_id=market_id,
            exit_price=settle_val,
            realized_pnl=(settle_val - pos.entry_price) * pos.contracts,
            exit_reason=f"SETTLED ({reason})",
        )
        settled_count += 1

    if settled_count > 0:
        summary = _build_summary()
        mode = "DRY RUN" if DRY_RUN else "LIVE"
        sheets.update_dashboard(summary, mode=mode)
        logger.info(
            "[SettlementSweep] Settled %d position(s)  bankroll=$%.2f  daily_pnl=$%+.2f",
            settled_count, _bankroll, _daily_pnl,
        )

        # EOD summary if it's the first sweep after midnight ET
        et_now = now_utc.astimezone(pytz.timezone("America/New_York"))
        if et_now.hour < 10:
            sheets.log_eod_summary(
                summary=summary,
                session_date=today.isoformat(),
                mode=mode,
            )


class SettlementSweep:
    """
    Daemon thread: runs _run_settlement_sweep() once on startup, then every
    day at 09:00 UTC (after Kalshi overnight settlement finishes around 08:00z).
    """

    _DAILY_RUN_UTC_HOUR = 9

    def __init__(self, client: KalshiClient):
        self._client = client
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="settlement-sweep"
        )

    def start(self) -> None:
        logger.info("SettlementSweep started")
        self._thread.start()

    def _loop(self) -> None:
        # Immediate startup sweep — catch anything that settled overnight
        try:
            _run_settlement_sweep(self._client)
        except Exception as exc:
            logger.warning("[SettlementSweep] startup sweep error: %s", exc)

        while True:
            now = datetime.now(timezone.utc)
            target = now.replace(hour=self._DAILY_RUN_UTC_HOUR, minute=0, second=0, microsecond=0)
            if now >= target:
                target += timedelta(days=1)
            sleep_s = (target - now).total_seconds()
            logger.info("[SettlementSweep] Next sweep at %s (%.0fs)", target.strftime("%H:%Mz"), sleep_s)
            time.sleep(sleep_s)
            try:
                _run_settlement_sweep(self._client)
            except Exception as exc:
                logger.warning("[SettlementSweep] daily sweep error: %s", exc)


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
        _save_positions()
        get_sheets_logger().log_trade_opened_v2(signal.station, signal)
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
        global _bankroll, _daily_pnl, _wins_today, _losses_today
        pnl = (bid - pos.entry_price) * pos.contracts
        _bankroll  += pnl
        _daily_pnl += pnl
        if pnl >= 0:
            _wins_today   += 1
        else:
            _losses_today += 1
        with _positions_lock:
            _open_positions.pop(pos.market_id, None)
        _save_positions()
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
            "[Exit] Complete: %s  entry=%.2f  exit=%.2f  pnl=$%+.2f  bankroll=$%.2f",
            pos.market_id, pos.entry_price, bid, pnl, _bankroll,
        )
        get_sheets_logger().log_trade_closed(
            station=pos.station,
            market_id=pos.market_id,
            exit_price=bid,
            realized_pnl=pnl,
            exit_reason=reason,
        )
        get_sheets_logger().update_dashboard(_build_summary(), mode="DRY RUN" if DRY_RUN else "LIVE")
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

    # Fetch HRRR — retry every 5 min for up to 25 min while current run posts,
    # then fall back to the previous hour's run as a last resort.
    _HRRR_RETRY_INTERVAL = 300   # seconds between retries
    _HRRR_MAX_RETRIES    = 5     # 5 retries = up to 25 min of waiting

    tmax_by_station = fetch_station_tmax(run_time, coords, peaks)
    for _attempt in range(_HRRR_MAX_RETRIES):
        if any(v is not None for v in tmax_by_station.values()):
            break
        logger.info(
            "HRRR %sz not available yet — retry %d/%d in %ds",
            run_time.strftime("%H"), _attempt + 1, _HRRR_MAX_RETRIES, _HRRR_RETRY_INTERVAL,
        )
        time.sleep(_HRRR_RETRY_INTERVAL)
        tmax_by_station = fetch_station_tmax(run_time, coords, peaks)

    if not any(v is not None for v in tmax_by_station.values()):
        prev_run = run_time - timedelta(hours=1)
        logger.info(
            "HRRR %sz unavailable after %d retries — falling back to %sz",
            run_time.strftime("%H"), _HRRR_MAX_RETRIES, prev_run.strftime("%H"),
        )
        tmax_by_station = fetch_station_tmax(prev_run, coords, peaks)

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

    # Persist HRRR baselines after every cycle so restart can skip the dead
    # first-cycle / baseline-only run and detect divergences immediately.
    _save_hrrr_state(engine)


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
        logger.info(
            "ExitMonitor started — normal poll %ds, near-hour poll %ds (:47–:05)",
            EXIT_POLL_INTERVAL, EXIT_NEAR_HOUR_INTERVAL,
        )
        self._thread.start()

    @staticmethod
    def _poll_interval() -> int:
        """60s near the top of each hour (ASOS posts ~:53–:58), 300s otherwise."""
        m = datetime.now(timezone.utc).minute
        if m >= EXIT_NEAR_HOUR_START_MIN or m < EXIT_NEAR_HOUR_END_MIN:
            return EXIT_NEAR_HOUR_INTERVAL
        return EXIT_POLL_INTERVAL

    def _poll(self) -> None:
        """Single poll cycle: fetch obs + markets, run exit checks."""
        with _positions_lock:
            if not _open_positions:
                return
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

    def _loop(self) -> None:
        # Immediate first poll so newly opened positions show obs right away.
        try:
            self._poll()
        except Exception as exc:
            logger.warning("[ExitMonitor] startup poll error: %s", exc)

        while True:
            time.sleep(self._poll_interval())
            try:
                self._poll()
            except Exception as exc:
                logger.warning("[ExitMonitor] poll error: %s", exc)

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
            with _positions_lock:
                if market_id in _open_positions and not _open_positions[market_id].pending_settlement:
                    _open_positions[market_id].pending_settlement = True
                    _save_positions()
                    logger.info("[ExitMonitor] %s market closed — marked pending settlement", market_id)
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

        if pos.entry_price > LOW_PRICE_STOP_THRESHOLD:
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

    # Restore open positions and HRRR baselines from the previous run.
    # Positions remain live on Kalshi regardless of bot state — we just
    # reload our record of them so ExitMonitor resumes monitoring immediately.
    # HRRR baselines let the first post-restart cycle detect divergences
    # instead of being a dead baseline-only run.
    _load_positions()
    _load_hrrr_state(_engine)

    monitor = ExitMonitor(client, _engine)
    monitor.start()

    sweep = SettlementSweep(client)
    sweep.start()

    logger.info(
        "scheduler_v2 started — dry_run=%s  restored %d position(s)",
        DRY_RUN, len(_open_positions),
    )

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
