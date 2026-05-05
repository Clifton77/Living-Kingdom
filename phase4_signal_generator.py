"""
phase4_signal_generator.py

Study-derived Phase 4 signal layer for the Kalshi weather trading bot.
Source: 38,174 settled contracts, study/data/aligned.csv.

Three exports used by scheduler.py:
  Phase4Forecaster      — fetch GFS + ECMWF forecasts from Open-Meteo for all 20 stations
  SignalGenerator       — evaluate bucket snapshots → BUY_YES / BUY_NO / PASS
  build_phase4_signals  — scheduler adapter for _refresh_all_kalshi_prices
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timezone
from typing import Optional

import requests

logger = logging.getLogger("phase4")

# ── Station coordinates (lat, lon) ────────────────────────────────────────────
# KJFK → Central Park (KNYC), KORD → Midway (KMDW); all others self-map.
_STATION_COORDS: dict[str, tuple[float, float]] = {
    "KJFK": (40.7789,  -73.9692),
    "KORD": (41.7862,  -87.7525),
    "KMIA": (25.7959,  -80.2870),
    "KDFW": (32.8998,  -97.0403),
    "KLAX": (33.9425, -118.4081),
    "KATL": (33.6407,  -84.4277),
    "KDEN": (39.8561, -104.6737),
    "KHOU": (29.6454,  -95.2789),
    "KAUS": (30.1945,  -97.6699),
    "KPHL": (39.8721,  -75.2411),
    "KBOS": (42.3643,  -71.0052),
    "KDCA": (38.8521,  -77.0377),
    "KLAS": (36.0840, -115.1537),
    "KMSP": (44.8848,  -93.2223),
    "KMSY": (29.9934,  -90.2580),
    "KOKC": (35.3931,  -97.6007),
    "KPHX": (33.4373, -112.0078),
    "KSAT": (29.5337,  -98.4698),
    "KSEA": (47.4502, -122.3088),
    "KSFO": (37.6213, -122.3790),
}

# ── Model preference per station ──────────────────────────────────────────────
# GFS   — ECMWF has catastrophic error at coastal/desert stations
# BLEND — average of bias-corrected GFS and ECMWF
# ECMWF — ECMWF consistently tighter than GFS for these 13 stations
_MODEL_PREF: dict[str, str] = {
    # GFS preferred
    "KLAX": "GFS",    # ECMWF bias +5.80°F — catastrophic for coastal LA
    "KLAS": "GFS",    # ECMWF +1.12°F vs GFS +0.28°F
    "KPHX": "ECMWF",  # backtest Oct24-Feb26: ECMWF MAE 0.36°F vs GFS 0.61°F
    "KDCA": "ECMWF",  # backtest Oct24-Feb26: ECMWF MAE 0.75°F vs GFS 2.04°F
    "KMIA": "GFS",    # ECMWF +1.13°F vs GFS +0.84°F
    # BLEND — both models informative, averaging reduces variance
    "KDFW": "GFS",    # backtest Oct24-Feb26: GFS MAE 1.03°F vs BLEND 1.54°F
    "KATL": "BLEND",  # GFS +0.97°F, ECMWF +0.69°F
    # ECMWF preferred for the remaining 13 stations
    "KHOU": "ECMWF",
    "KMSP": "ECMWF",
    "KSFO": "ECMWF",
    "KAUS": "ECMWF",
    "KPHL": "ECMWF",
    "KORD": "ECMWF",
    "KJFK": "ECMWF",
    "KDEN": "ECMWF",
    "KSEA": "ECMWF",
    "KSAT": "ECMWF",
    "KBOS": "ECMWF",
    "KMSY": "ECMWF",
    "KOKC": "GFS",    # backtest Oct24-Feb26: GFS MAE 0.76°F vs ECMWF 1.62°F
}

# ── GFS warm-bias corrections (°F to subtract from raw GFS) ──────────────────
# Negative value = GFS runs cold there (correction adds to forecast).
_GFS_BIAS: dict[str, float] = {
    "KSAT":  3.69,
    "KHOU":  2.49,
    "KMSP":  2.32,
    "KMSY":  2.06,
    "KDCA":  1.48,
    "KORD":  1.40,
    "KDFW":  1.16,
    "KJFK":  1.14,
    "KPHL":  1.05,
    "KATL":  0.97,
    "KAUS":  0.93,
    "KMIA":  0.84,
    "KPHX":  0.75,
    "KBOS":  0.49,
    "KOKC":  0.44,
    "KSFO":  0.42,
    "KLAX":  0.37,
    "KLAS":  0.28,
    "KDEN":  0.15,
    "KSEA": -0.43,
}

# ── ECMWF bias corrections (°F to subtract from raw ECMWF) ───────────────────
_ECMWF_BIAS: dict[str, float] = {
    "KLAX":  5.80,
    "KDFW":  1.53,
    "KMIA":  1.13,
    "KLAS":  1.12,
    "KDCA":  0.91,
    "KSFO":  0.87,
    "KOKC":  0.81,
    "KSEA":  0.78,
    "KATL":  0.69,
    "KJFK":  0.68,
    "KMSY":  0.66,
    "KAUS":  0.58,
    "KBOS":  0.00,
    "KDEN": -0.30,
    "KMSP": -0.31,
    "KORD": -0.22,
    "KPHL": -0.19,
    "KSAT": -0.19,
    "KHOU": -0.21,
    "KPHX": -0.03,
}

# ── Station NO bias for mid-market buckets ────────────────────────────────────
# Negative = market historically overprices YES → NO has structural edge.
# Applied as an additive adjustment to effective edge in the sell zone.
_STATION_NO_BIAS: dict[str, float] = {
    "KPHL": -0.095,
    "KDEN": -0.115,
    "KSEA": -0.297,
    "KSFO": -0.302,
}

# ── Seasonal station bias ─────────────────────────────────────────────────────
# Additional edge adjustment by station+season.
# Positive = YES edge increases; negative = NO edge increases (KJFK Summer).
_SEASONAL_STATION_BIAS: dict[str, dict[str, float]] = {
    "KORD": {"Fall": +0.016, "Winter": -0.030},
    "KJFK": {
        "Fall":   +0.013,
        "Summer": -0.065,  # strong NO signal in summer
        "Winter": -0.032,
        "Spring": -0.032,
    },
}

# ── Seasonal confidence multiplier ───────────────────────────────────────────
_SEASON_CONF: dict[str, float] = {
    "Winter": 1.20,
    "Spring": 1.00,
    "Summer": 0.95,
    "Fall":   0.90,
}

# ── Monthly edge multiplier ───────────────────────────────────────────────────
# Near-zero edge months: reduce position sizing.
_MONTH_MULT: dict[int, float] = {
    5:  0.70,   # May
    10: 0.70,   # October
    9:  0.95,   # September
    11: 0.95,   # November
}

# ── Per-station Kelly multiplier — Phase 5 analysis (40–70¢ zone) ────────────
# Only stations with n ≥ 20 AND CI lower bound on edge > 0 get a non-1.0 value.
# KORD: n=173, edge=+0.090, ci_lo=+0.016 → 2.0× (capped from raw 2.66×)
# KMIA: n=239, edge=-0.025, negative across Winter/Summer → 0.75× (reduce exposure)
# All others: insufficient data or CI includes zero → 1.0× (neutral)
_STATION_KELLY_MULT: dict[str, float] = {
    "KORD": 2.00,
    "KMIA": 0.75,
}

# ── Thin-liquidity stations (cap at 2 contracts) ──────────────────────────────
THIN_LIQUIDITY_STATIONS: frozenset[str] = frozenset({"KSAT", "KOKC"})

# ── Price thresholds ──────────────────────────────────────────────────────────
_NO_BUY_LO       = 0.05
_NO_BUY_HI       = 0.30
_WATCH_LO        = 0.30
_YES_BUY_LO      = 0.40
_YES_BUY_HI      = 0.70
_YES_STRONG_LO   = 0.60
_SKIP_HI         = 0.85

# ── Pattern-season decoder ────────────────────────────────────────────────────
_PATTERN_SEASON: dict[str, str] = {
    "DJF": "Winter", "MAM": "Spring", "JJA": "Summer", "SON": "Fall",
    "Winter": "Winter", "Spring": "Spring", "Summer": "Summer", "Fall": "Fall",
}

# ── Open-Meteo settings ───────────────────────────────────────────────────────
_OM_URL     = "https://api.open-meteo.com/v1/forecast"
_OM_TIMEOUT = 12

# ── 12z model availability cutoffs (UTC) ──────────────────────────────────────
# GFS 12z initializes at 12:00 UTC; Open-Meteo typically ingests by ~15:30 UTC.
# ECMWF 12z initializes at 12:00 UTC; Open-Meteo typically ingests by ~18:30 UTC.
#
# Gate policy: GFS 12z (15:30 UTC) is the universal entry gate for ALL stations.
# Requiring ECMWF 12z (18:30 UTC) would lock out Eastern stations entirely
# (entry cutoff = peak_hour-2h ≈ 17:00 UTC) and Central stations (≈18:00 UTC).
# The 19:25 UTC ECMWF refresh still runs and silently upgrades data quality for
# ECMWF/BLEND stations mid-session — entries are not held waiting for it.
_GFS_12Z_READY   = dtime(15, 30)
_ECMWF_12Z_READY = dtime(18, 30)   # used only for the refresh job, not the entry gate


def is_12z_ready(model_pref: str, now_utc: datetime) -> bool:
    """
    Return True once at least one 12z model run is available on Open-Meteo.

    GFS 12z (15:30 UTC) gates ALL stations regardless of preferred model.
    Using ECMWF 12z as the gate would exclude Eastern (cutoff ~17:00 UTC) and
    Central (cutoff ~18:00 UTC) stations entirely from same-day trading.
    """
    cutoff = datetime.combine(now_utc.date(), _GFS_12Z_READY, tzinfo=timezone.utc)
    return now_utc >= cutoff


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class Phase4ForecastData:
    station:         str
    target_date:     date
    gfs_f:           Optional[float]   # raw GFS daily max (°F)
    ecmwf_f:         Optional[float]   # raw ECMWF daily max (°F)
    gfs_corrected:   Optional[float]   # GFS minus station bias
    ecmwf_corrected: Optional[float]   # ECMWF minus station bias
    blended_f:       Optional[float]   # model-selected / blended output
    preferred_model: str = "ECMWF"    # "GFS", "ECMWF", or "BLEND"
    blended_source:  str = "ECMWF"    # actual model driving blended_f: "GFS", "ECMWF", "BLEND", "GFS_fallback"
    fetched_at:      datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class Phase4Signal:
    station:         str
    bucket_lower:    int
    yes_ask:         float
    action:          str              # BUY_YES | BUY_NO | PASS
    price_zone:      str              # STRONG_BUY | BUY | SELL | SKIP_LOW | SKIP_HIGH | WATCH
    forecast_f:      Optional[float]  # bias-corrected blended forecast
    preferred_model: str              # GFS | ECMWF | BLEND | NONE
    confidence:      float            # effective edge after all multipliers
    month_mult:      float
    max_contracts:   Optional[int]    # None = no cap; 2 = thin liquidity
    reason:          str

    def __str__(self) -> str:
        cap = f" cap={self.max_contracts}ct" if self.max_contracts else ""
        fc  = f" fc={self.forecast_f:.1f}°F" if self.forecast_f is not None else ""
        return (
            f"{self.station} B{self.bucket_lower} ask={self.yes_ask:.2f} "
            f"→ {self.action} [{self.price_zone}]{fc} "
            f"model={self.preferred_model} conf={self.confidence:.3f}"
            f"{cap} | {self.reason}"
        )


# ── Phase4Forecaster ──────────────────────────────────────────────────────────

class Phase4Forecaster:
    """
    Fetches GFS and ECMWF daily max temperature forecasts from Open-Meteo
    for all 20 Kalshi weather stations.  Called once per Tier 3 cycle before
    the main signal pass so fresh model data is available during price refresh.
    """

    def _fetch_model(
        self, lat: float, lon: float, target_date: date, model: str
    ) -> Optional[float]:
        date_str = target_date.isoformat()
        params = {
            "latitude":         lat,
            "longitude":        lon,
            "daily":            "temperature_2m_max",
            "temperature_unit": "fahrenheit",
            "forecast_days":    2,
            "start_date":       date_str,
            "end_date":         date_str,
            "models":           model,
            "timezone":         "UTC",
        }
        try:
            r = requests.get(_OM_URL, params=params, timeout=_OM_TIMEOUT)
            r.raise_for_status()
            temps = r.json().get("daily", {}).get("temperature_2m_max", [])
            if temps and temps[0] is not None:
                return float(temps[0])
        except Exception as exc:
            logger.debug("Open-Meteo %s (%.3f,%.3f): %s", model, lat, lon, exc)
        return None

    def fetch_all(self, target_date: date) -> dict[str, Phase4ForecastData]:
        """
        Fetch GFS + ECMWF forecasts for every station.
        Returns dict[station_label → Phase4ForecastData].
        Per-station failures are logged at DEBUG and produce None values.
        """
        results: dict[str, Phase4ForecastData] = {}

        for station, (lat, lon) in _STATION_COORDS.items():
            gfs_raw   = self._fetch_model(lat, lon, target_date, "gfs_seamless")
            time.sleep(0.15)
            ecmwf_raw = self._fetch_model(lat, lon, target_date, "ecmwf_ifs025")
            time.sleep(0.15)

            gfs_c   = round(gfs_raw   - _GFS_BIAS.get(station,   0.0), 1) if gfs_raw   is not None else None
            ecmwf_c = round(ecmwf_raw - _ECMWF_BIAS.get(station, 0.0), 1) if ecmwf_raw is not None else None

            pref = _MODEL_PREF.get(station, "ECMWF")
            if pref == "GFS":
                blended = gfs_c
                blended_src = "GFS"
            elif pref == "BLEND":
                if gfs_c is not None and ecmwf_c is not None:
                    blended = round((gfs_c + ecmwf_c) / 2.0, 1)
                else:
                    blended = gfs_c if gfs_c is not None else ecmwf_c
                blended_src = "BLEND"
            else:
                # ECMWF preferred — fall back to GFS if ECMWF not yet ingested
                # (ECMWF 12z disseminates ~18:30 UTC; GFS 12z available ~15:30 UTC)
                if ecmwf_c is not None:
                    blended = ecmwf_c
                    blended_src = "ECMWF"
                else:
                    blended = gfs_c
                    blended_src = "GFS_fallback"
                    if gfs_c is not None:
                        logger.info(
                            "[Phase4] %s ECMWF unavailable — using GFS fallback %.1f°F for target",
                            station, gfs_c,
                        )

            results[station] = Phase4ForecastData(
                station=station,
                target_date=target_date,
                gfs_f=gfs_raw,
                ecmwf_f=ecmwf_raw,
                gfs_corrected=gfs_c,
                ecmwf_corrected=ecmwf_c,
                blended_f=blended,
                preferred_model=pref,
                blended_source=blended_src,
            )
            logger.debug(
                "[Phase4] %s  GFS %.1f→%.1f  ECMWF %.1f→%.1f  blend=%.1f [%s/%s]",
                station,
                gfs_raw   or 0.0, gfs_c   or 0.0,
                ecmwf_raw or 0.0, ecmwf_c or 0.0,
                blended   or 0.0, pref, blended_src,
            )

        logger.info("[Phase4] fetch_all complete: %d stations for %s", len(results), target_date)
        return results


# ── SignalGenerator ───────────────────────────────────────────────────────────

class SignalGenerator:
    """
    Evaluates Kalshi bucket snapshots against Phase 4 forecast data
    and price-zone rules from 38,174 historical contracts.

    Price zone win rates (empirical):
      < 5¢         → 0.001   skip — effectively dead money
      5–30¢  (NO)  → 0.074   BUY_NO — market overprices YES
      30–40¢       → 0.331   WATCH
      40–60¢ (YES) → 0.534   BUY_YES    (+0.034 edge)
      60–70¢ (YES) → 0.736   BUY_YES    (+0.081 edge, strong buy)
      70–85¢       → 0.762   WATCH — margin insufficient at this price
      > 85¢        → 0.992   skip — already priced in
    """

    @staticmethod
    def _price_zone(yes_ask: float) -> str:
        if yes_ask < _NO_BUY_LO:          return "SKIP_LOW"
        if yes_ask < _NO_BUY_HI:          return "SELL"
        if yes_ask < _YES_BUY_LO:         return "WATCH"
        if yes_ask < _YES_STRONG_LO:       return "BUY"
        if yes_ask <= _YES_BUY_HI:        return "STRONG_BUY"
        if yes_ask <= _SKIP_HI:           return "WATCH"
        return "SKIP_HIGH"

    @staticmethod
    def _decode_season(sig_season: str) -> str:
        return _PATTERN_SEASON.get(sig_season, "Summer")

    def evaluate(
        self,
        station: str,
        bucket_lower: int,
        yes_ask: float,
        forecast: Optional[Phase4ForecastData],
        sig_season: str,
        month: int,
    ) -> Phase4Signal:
        """
        Evaluate a single bucket snapshot.

        Parameters
        ----------
        station      : Kalshi station label
        bucket_lower : lower bound of the 2°F bucket
        yes_ask      : current Kalshi ask price (0–1)
        forecast     : Phase4ForecastData from fetch_all(), or None
        sig_season   : sig.season field (DJF/MAM/JJA/SON or named)
        month        : calendar month of the event date
        """
        season    = self._decode_season(sig_season)
        zone      = self._price_zone(yes_ask)
        pref      = _MODEL_PREF.get(station, "ECMWF")
        fc_f      = forecast.blended_f if forecast is not None else None
        s_conf    = _SEASON_CONF.get(season, 1.00)
        m_mult    = _MONTH_MULT.get(month, 1.00)
        no_bias   = _STATION_NO_BIAS.get(station, 0.0)
        s_bias    = _SEASONAL_STATION_BIAS.get(station, {}).get(season, 0.0)
        max_ct    = 2 if station in THIN_LIQUIDITY_STATIONS else None

        if zone in ("SKIP_LOW", "SKIP_HIGH"):
            return Phase4Signal(
                station=station, bucket_lower=bucket_lower, yes_ask=yes_ask,
                action="PASS", price_zone=zone, forecast_f=fc_f,
                preferred_model=pref, confidence=0.0, month_mult=m_mult,
                max_contracts=max_ct,
                reason=f"price {yes_ask:.2f} outside tradeable range",
            )

        if zone == "WATCH":
            return Phase4Signal(
                station=station, bucket_lower=bucket_lower, yes_ask=yes_ask,
                action="PASS", price_zone=zone, forecast_f=fc_f,
                preferred_model=pref, confidence=0.0, month_mult=m_mult,
                max_contracts=max_ct,
                reason="neutral zone (30–40¢ or 70–85¢)",
            )

        if zone == "SELL":
            no_adj = abs(no_bias) if no_bias < 0 else 0.0
            conf   = round((0.053 + no_adj + abs(s_bias)) * s_conf * m_mult, 3)
            return Phase4Signal(
                station=station, bucket_lower=bucket_lower, yes_ask=yes_ask,
                action="BUY_NO", price_zone=zone, forecast_f=fc_f,
                preferred_model=pref, confidence=conf, month_mult=m_mult,
                max_contracts=max_ct,
                reason=(
                    f"sell zone 5–30¢ no_bias={no_bias:+.3f} "
                    f"seasonal={s_bias:+.3f} conf={conf:.3f}"
                ),
            )

        # BUY or STRONG_BUY
        edge_base = 0.081 if zone == "STRONG_BUY" else 0.034
        conf = round((edge_base + s_bias) * s_conf * m_mult, 3)
        return Phase4Signal(
            station=station, bucket_lower=bucket_lower, yes_ask=yes_ask,
            action="BUY_YES", price_zone=zone, forecast_f=fc_f,
            preferred_model=pref, confidence=conf, month_mult=m_mult,
            max_contracts=max_ct,
            reason=(
                f"{zone} {yes_ask:.2f} edge_base={edge_base:.3f} "
                f"seasonal={s_bias:+.3f} conf={conf:.3f}"
            ),
        )


# ── Scheduler adapter ─────────────────────────────────────────────────────────

def build_phase4_signals(
    station: str,
    snapshots: dict,
    sig,
    gen: SignalGenerator,
    forecasts: dict,
    season: str = "Summer",
) -> list[Phase4Signal]:
    """
    Scheduler adapter called from _refresh_all_kalshi_prices.

    Evaluates every non-None bucket snapshot for one station and returns
    only actionable signals (BUY_YES or BUY_NO); PASS results are filtered out.

    Parameters
    ----------
    station   : Kalshi station label ("KJFK", "KORD", …)
    snapshots : dict[bucket_lower → snapshot] from kalshi.get_all_snapshots()
    sig       : TradeSignal — provides sig.season and sig.event_date
    gen       : module-level SignalGenerator instance (_p4_gen)
    forecasts : dict[station → Phase4ForecastData] from _p4_fetcher.fetch_all()
    season    : fallback season string if sig.season is absent
    """
    forecast   = forecasts.get(station)
    sig_season = getattr(sig, "season", season) or season
    event_date = getattr(sig, "event_date", None)
    month      = event_date.month if event_date is not None else date.today().month

    results: list[Phase4Signal] = []
    for bucket_lower, snap in snapshots.items():
        if snap is None:
            continue
        yes_ask = getattr(snap, "yes_ask", None)
        if yes_ask is None:
            continue
        p4sig = gen.evaluate(station, bucket_lower, yes_ask, forecast, sig_season, month)
        if p4sig.action != "PASS":
            results.append(p4sig)

    return sorted(results, key=lambda s: s.yes_ask, reverse=True)
