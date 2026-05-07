"""
signal_engine_v2.py

HRRR divergence signal engine. Replaces signal_engine.py.

Strategy:
  - Fetch HRRR TMAX every hour for all stations
  - Detect run-to-run moves >= HRRR_MATERIAL_MOVE_F (2°F)
  - Compare to Kalshi prices — trade if market hasn't repriced
  - Morning scan (8:30am ET): flag buckets priced >= 35% as overconfidence candidates
  - HRRR confirmation required for all trades

Two trade types, one code path:
  YES on the bucket HRRR is pointing to (if market underprices it)
  NO  on the bucket the market is overconfident about (if HRRR disagrees)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Optional

import numpy as np
from scipy.stats import norm

from config import (
    HRRR_COLD_BIAS_F,
    HRRR_MARKET_MAX_DISTANCE_F,
    HRRR_MATERIAL_MOVE_F,
    HRRR_STATION_SIGMA,
    KELLY_FRAC,
    KALSHI_SETTLEMENT_STATION,
    MORNING_OVERCONFIDENCE_THRESH,
    MAX_STAKE_PCT,
    MIN_STAKE,
    STARTING_BANKROLL,
    STATION_COORDS,
    STATIONS,
)
from kalshi_client import MarketSnapshot
from utils.peak_hours import get_peak_hour

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class HRRRSnapshot:
    station: str
    run_time: datetime
    tmax_raw_f: float          # straight from HRRR
    tmax_corrected_f: float    # bias-corrected (+1.7°F)
    peak_bucket_lower: int     # bucket lower bound after bias correction
    sigma: float               # station sigma from calibration study


@dataclass
class TradeSignal:
    station: str
    event_date: date
    signal_type: str           # "yes_divergence", "no_divergence", "no_morning"
    side: str                  # "YES" or "NO"
    bucket_lower: int          # target bucket lower bound
    market_id: str             # Kalshi market ticker (from MarketSnapshot)
    p_model: float             # our estimated probability
    kalshi_price: float        # price we pay: YES ask (YES trades) or NO ask (NO trades)
    edge: float                # p_model - kalshi_price
    stake: float               # recommended stake in USD
    contracts: int             # round(stake / kalshi_price)
    hrrr_delta_f: float        # run-to-run move that triggered (0 for morning NO)
    run_time: datetime         # HRRR run that generated this signal


@dataclass
class StationState:
    """Mutable state per station — updated each HRRR cycle."""
    last_tmax_f: Optional[float] = None
    last_run_time: Optional[datetime] = None
    morning_flags: dict[int, float] = field(default_factory=dict)  # bucket_lower → price at flag time
    traded: set[tuple[date, int, str]] = field(default_factory=set)  # (event_date, bucket_lower, side)


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def tmax_to_bucket(tmax_f: float, markets: list[MarketSnapshot]) -> Optional[int]:
    """
    Map a TMAX (°F) to the Kalshi bucket lower bound that contains it.
    Uses the live market list so it works for any bucket alignment
    (odd-start, even-start, or irregular spacing by station/season).
    """
    if not markets:
        return None
    t = round(tmax_f)
    lowers = sorted(m.bucket_lower for m in markets)
    floor_bucket = lowers[0]
    ceil_bucket  = lowers[-1]
    # Below the first interior bucket → floor tail
    if t < (lowers[1] if len(lowers) > 1 else ceil_bucket):
        return floor_bucket
    # At or above the ceiling bucket → ceiling tail
    if t >= ceil_bucket:
        return ceil_bucket
    # Interior: find the bucket [lo, lo+2) that contains t
    for lo in lowers[1:-1]:
        if lo <= t < lo + 2:
            return lo
    return None


def bucket_probability(tmax_corrected_f: float, bucket_lower: int, sigma: float) -> float:
    """
    P(daily high lands in [bucket_lower, bucket_lower+2)) given HRRR forecast.
    Uses Gaussian centered on tmax_corrected_f with station-specific sigma.
    Tail buckets are handled as one-sided integrals.
    """
    center = tmax_corrected_f
    lo = bucket_lower
    hi = lo + 2
    return float(norm.cdf(hi, center, sigma) - norm.cdf(lo, center, sigma))


def compute_stake(p: float, ask_price: float, bankroll: float) -> float:
    """Half-Kelly stake with 2% hard cap and $1 minimum."""
    edge = p - ask_price
    if edge <= 0:
        return 0.0
    kelly_pct = (edge / (1 - ask_price)) * KELLY_FRAC
    raw = kelly_pct * bankroll
    capped = min(raw, bankroll * MAX_STAKE_PCT)
    return round(capped, 2) if capped >= MIN_STAKE else 0.0


def _station_coords_for_hrrr(station: str) -> tuple[float, float]:
    """Return settlement station lat/lon for HRRR extraction."""
    settlement = KALSHI_SETTLEMENT_STATION.get(station, station)
    return STATION_COORDS[settlement]


def _peak_utc(station: str, event_date: date) -> int:
    """Convert local peak hour to UTC using station timezone offset."""
    import pytz
    from config import STATION_TIMEZONES
    tz = pytz.timezone(STATION_TIMEZONES[station])
    local_hour = get_peak_hour(station, event_date)
    # Build a naive local datetime and localize it
    local_dt = tz.localize(datetime(event_date.year, event_date.month, event_date.day, local_hour))
    return local_dt.utctimetuple().tm_hour


# ---------------------------------------------------------------------------
# Signal generation
# ---------------------------------------------------------------------------

class HRRRSignalEngine:
    def __init__(self, bankroll: float = STARTING_BANKROLL):
        self.bankroll = bankroll
        self._state: dict[str, StationState] = {s: StationState() for s in STATIONS}

    def update(
        self,
        station: str,
        snapshot: HRRRSnapshot,
        markets: list[MarketSnapshot],
        event_date: date,
        is_morning_scan: bool = False,
    ) -> list[TradeSignal]:
        """
        Process a new HRRR snapshot for one station.
        Returns list of TradeSignal (may be empty).
        """
        signals: list[TradeSignal] = []
        state = self._state[station]
        sigma = HRRR_STATION_SIGMA.get(station, 3.0)

        def _find_market(bucket_lo: int) -> Optional[MarketSnapshot]:
            return next((m for m in markets if m.bucket_lower == bucket_lo), None)

        def _make_signal(
            signal_type: str, side: str, bucket_lo: int, market: MarketSnapshot,
            p: float, price: float, delta: float,
        ) -> Optional[TradeSignal]:
            pos_key = (event_date, bucket_lo, side)
            if pos_key in state.traded:
                return None
            edge = p - price
            stake = compute_stake(p, price, self.bankroll)
            if stake < MIN_STAKE:
                return None
            contracts = max(1, round(stake / price))
            state.traded.add(pos_key)
            return TradeSignal(
                station=station,
                event_date=event_date,
                signal_type=signal_type,
                side=side,
                bucket_lower=bucket_lo,
                market_id=market.market_id,
                p_model=p,
                kalshi_price=price,
                edge=edge,
                stake=stake,
                contracts=contracts,
                hrrr_delta_f=delta,
                run_time=snapshot.run_time,
            )

        # ── Morning scan: flag overpriced buckets ─────────────────────────
        if is_morning_scan:
            for m in markets:
                if m.yes_ask >= MORNING_OVERCONFIDENCE_THRESH:
                    state.morning_flags[m.bucket_lower] = m.yes_ask
                    logger.info(
                        "%s morning flag: bucket %d priced %.2f",
                        station, m.bucket_lower, m.yes_ask,
                    )

        # ── Check morning flags against current HRRR ─────────────────────
        for bucket_lo in list(state.morning_flags):
            market = _find_market(bucket_lo)
            if not market or not market.is_open:
                del state.morning_flags[bucket_lo]
                continue
            distance = abs(snapshot.tmax_corrected_f - (bucket_lo + 1))
            if distance >= HRRR_MATERIAL_MOVE_F:
                no_ask = 1.0 - market.yes_bid
                p_no = 1.0 - bucket_probability(snapshot.tmax_corrected_f, bucket_lo, sigma)
                sig = _make_signal("no_morning", "NO", bucket_lo, market, p_no, no_ask, 0.0)
                if sig:
                    signals.append(sig)
                del state.morning_flags[bucket_lo]
            # else: HRRR still pointing at bucket — keep flag for next cycle

        # ── HRRR divergence: compare to previous run ──────────────────────
        if state.last_tmax_f is not None:
            # If the gap between runs is > 1.5h (we skipped a run due to HRRR
            # availability delay), normal diurnal drift can exceed HRRR_MATERIAL_MOVE_F
            # without any real forecast shift.  Reset the baseline and skip this cycle.
            run_gap_h = (
                (snapshot.run_time - state.last_run_time).total_seconds() / 3600
                if state.last_run_time is not None else 1.0
            )
            if run_gap_h > 1.5:
                logger.info(
                    "%s: run gap %.1fh > 1.5h — resetting baseline, skipping divergence check",
                    station, run_gap_h,
                )
            else:
                delta = snapshot.tmax_corrected_f - state.last_tmax_f

                if abs(delta) >= HRRR_MATERIAL_MOVE_F:
                    # Gate: HRRR must be within 4°F of Kalshi's peak-priced bucket.
                    # Blocks divergence trades when HRRR is drifting in the wrong space.
                    open_markets = [m for m in markets if m.is_open]
                    peak_market = max(open_markets, key=lambda m: m.yes_ask) if open_markets else None
                    if peak_market is not None:
                        peak_center = peak_market.bucket_lower + 1
                        hrrr_market_distance = abs(snapshot.tmax_corrected_f - peak_center)
                        if hrrr_market_distance > HRRR_MARKET_MAX_DISTANCE_F:
                            logger.info(
                                "%s divergence blocked: HRRR %.1f°F is %.1f°F from Kalshi peak B%d (limit %.1f°F)",
                                station, snapshot.tmax_corrected_f, hrrr_market_distance,
                                peak_market.bucket_lower, HRRR_MARKET_MAX_DISTANCE_F,
                            )
                            state.last_tmax_f = snapshot.tmax_corrected_f
                            state.last_run_time = snapshot.run_time
                            return signals

                    new_bucket = tmax_to_bucket(snapshot.tmax_corrected_f, markets)
                    old_bucket = tmax_to_bucket(state.last_tmax_f, markets)

                    if new_bucket is not None:
                        market = _find_market(new_bucket)
                        if market and market.is_open:
                            p_yes = bucket_probability(snapshot.tmax_corrected_f, new_bucket, sigma)
                            sig = _make_signal("yes_divergence", "YES", new_bucket, market,
                                               p_yes, market.yes_ask, delta)
                            if sig:
                                signals.append(sig)

                    if old_bucket is not None and old_bucket != new_bucket:
                        market = _find_market(old_bucket)
                        if market and market.is_open:
                            no_ask = 1.0 - market.yes_bid
                            p_no = 1.0 - bucket_probability(snapshot.tmax_corrected_f, old_bucket, sigma)
                            sig = _make_signal("no_divergence", "NO", old_bucket, market,
                                               p_no, no_ask, delta)
                            if sig:
                                signals.append(sig)

        # ── Update state ──────────────────────────────────────────────────
        state.last_tmax_f = snapshot.tmax_corrected_f
        state.last_run_time = snapshot.run_time

        return signals

    def build_snapshot(
        self,
        station: str,
        run_time: datetime,
        tmax_raw_f: float,
        bias_f: float | None = None,
        lead_h: int | None = None,
        sigma_live: float | None = None,
    ) -> HRRRSnapshot:
        """
        Build a HRRRSnapshot from a raw HRRR TMAX value.

        bias_f     : live calibration correction (°F). Falls back to HRRR_COLD_BIAS_F.
        lead_h     : hours remaining to peak. Scales sigma up for longer leads via
                     sqrt(lead_h / 6) so a 12h lead is ~40% wider than a 6h lead.
        sigma_live : std of today's HRRR TMAX values for this station. Used as a
                     sigma floor so probability estimates widen automatically on
                     convective days when runs are disagreeing.
        """
        import math
        from config import HRRR_COLD_BIAS_F
        correction = bias_f if bias_f is not None else HRRR_COLD_BIAS_F
        corrected  = tmax_raw_f + correction
        sigma_base = HRRR_STATION_SIGMA.get(station, 3.0)
        # Lead-dependent scaling: longer lead → wider uncertainty
        if lead_h is not None and lead_h > 0:
            sigma_lead = max(1.0, sigma_base * math.sqrt(lead_h / 6.0))
        else:
            sigma_lead = sigma_base
        # Live sigma floor: auto-widens on days when HRRR runs disagree
        sigma_eff = max(sigma_lead, sigma_live) if sigma_live is not None else sigma_lead
        t = round(corrected)
        lower = ((t - 1) // 2) * 2 + 1
        return HRRRSnapshot(
            station=station,
            run_time=run_time,
            tmax_raw_f=tmax_raw_f,
            tmax_corrected_f=corrected,
            peak_bucket_lower=lower,
            sigma=sigma_eff,
        )
