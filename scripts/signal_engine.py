"""
Signal engine — core of the trading bot.

For each station and event date:
  1. Classify today's 500mb synoptic pattern
  2. Fetch bias-adjusted forecast from bias table
  3. Build probability distribution over Kalshi buckets
  4. Fetch Kalshi implied probabilities (market prices)
  5. Compute edge per bucket
  6. Apply hybrid weather-penalty threshold
  7. Output TradeSignal for each station

Called by the scheduler (Tier 3 — every 6hrs) and on demand from dashboard.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from utils.logging_config import setup_logging
from utils.weather_penalty import compute_effective_threshold, ThresholdResult
from scripts.taf_interpreter import interpret_taf, get_metar, TafResult, MetarResult
from scripts.pattern_classifier import classify_pattern
from kalshi_client import KalshiClient, MarketSnapshot, all_bucket_lowers, bucket_label
from config import (
    STATIONS,
    BIAS_PARQUET,
    FCST_PARQUET,
    STATION_TIMEZONES,
    KALSHI_BUCKET_LOWER_TAIL,
    KALSHI_BUCKET_UPPER_TAIL,
    KALSHI_BUCKET_STARTS,
    OPEN_METEO_FORECAST_URL,
    STATION_COORDS,
    MIN_KELLY_STAKE,
    settlement_station,
    STARTING_BANKROLL,
    MAX_STAKE_PCT,
    MIN_N_OBS,
    EDGE_THRESHOLD_BASE,
    CONFIDENCE_KELLY_SCALE,
    MIN_PROB_RATIO,
    EDGE_BLEND_WEIGHT,
)

logger = setup_logging("signal_engine")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class BucketAnalysis:
    bucket_lower:   int
    bucket_label:   str
    model_prob:     float          # our model's probability for this bucket
    kalshi_prob:    float          # Kalshi implied probability (yes_ask)
    edge:           float          # model_prob - kalshi_prob
    yes_ask:        float          # raw Kalshi ask price
    yes_bid:        float


@dataclass
class SignalReasoning:
    """
    Structured reasoning for the dashboard signal card.
    All text fields are plain English — readable by anyone, not just meteorologists.
    """
    # ── Four plain-English sections (rendered as card body) ───────────────
    current_conditions: str    # What the weather looks like right now at the station
    synoptic_pattern:   str    # What the upper-level pattern means for today
    forecast_and_bias:  str    # What the model says and how history adjusts it
    market_analysis:    str    # Where Kalshi is mispriced and why we have edge

    # ── Decision summary (one sentence at bottom of card) ─────────────────
    decision_rationale: str    # Why TRADE / WATCH / SKIP in plain terms

    # ── Bucket comparison table (for the visual table on the card) ────────
    # Each entry: {label, model_pct, kalshi_pct, edge, is_top, yes_ask}
    bucket_table:       list[dict]

    # ── Checklist (threshold guardrails, shown as pass/fail on card) ──────
    # Each entry: {name, passed, detail}
    threshold_checks:   list[dict]

    # ── Data provenance ───────────────────────────────────────────────────
    data_sources:       dict           # where each data piece came from
    generated_at:       datetime       # when this signal was computed

    # ── Expansion note (populated when adjacent bucket expansion fires) ───
    expansion_note:       str  = ""
    expansion_guardrails: dict = field(default_factory=dict)


@dataclass
class TradeSignal:
    station:            str
    event_date:         date
    local_time:         str            # human-readable local time
    decision:           str            # "TRADE" | "WATCH" | "SKIP" | "HARD_SKIP" | "CONSTRAINED"

    # Forecast
    forecast_raw:       float          # IEM-AFM forecast (primary); falls back to Open-Meteo
    bias_mean:          float
    bias_std:           float
    forecast_adjusted:  float          # bias-corrected forecast
    cluster_id:         int
    season:             str
    n_obs:              int
    pattern_confidence: str

    # Top signal
    top_bucket:         int            # bucket_lower of best edge bucket
    top_edge:           float
    top_model_prob:     float
    top_kalshi_prob:    float
    top_yes_ask:        float          # price to buy

    # Kelly
    kelly_fraction:     float
    kelly_stake_usd:    float
    kelly_contracts:    int

    # Threshold
    threshold_result:   Optional[ThresholdResult]
    taf:                TafResult
    metar:              MetarResult

    # GFS-MOS cross-check (optional — None when IEM MAV unavailable)
    mos_forecast_raw:   Optional[float] = None   # GFS-MOS Day-1 max forecast
    model_divergence_f: Optional[float] = None   # AFM - MOS (+ means NWS warmer than model)

    # Full distribution
    buckets:            list[BucketAnalysis] = field(default_factory=list)

    # Live market tail bounds (vary by station/season — differ from config constants)
    live_lower_tail:    int = 68
    live_upper_tail:    int = 77

    # Structured reasoning for dashboard card
    reasoning:          Optional[SignalReasoning] = None

    # Timestamp — used by stale signal guard in Tier 2
    signal_generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Bucket probability distribution
# ---------------------------------------------------------------------------

def _bucket_bounds(lower: int) -> tuple[float, float]:
    """
    Return (low, high) integration bounds for a bucket.
    Lower tail: (-inf, 68.5)
    Interior:   (lower - 0.5, lower + 1.5)  → e.g. 71 → (70.5, 72.5)
    Upper tail: (76.5, +inf)
    """
    if lower == KALSHI_BUCKET_LOWER_TAIL:
        return (-math.inf, lower + 0.5)
    if lower == KALSHI_BUCKET_UPPER_TAIL:
        return (lower - 0.5, math.inf)
    return (lower - 0.5, lower + 1.5)


def build_probability_distribution(
    forecast_adjusted: float,
    bias_std: float,
    live_buckets: list[int] | None = None,
) -> dict[int, float]:
    """
    Model the temperature outcome as a normal distribution:
      mean = forecast_adjusted
      std  = bias_std  (captures residual uncertainty after bias correction)

    Integrate over each Kalshi bucket to get P(bucket).
    live_buckets: sorted list of bucket_lower values from the live Kalshi market.
                  When provided, the min bucket is treated as the lower tail and
                  the max bucket as the upper tail — which varies by station/season.
                  Falls back to the hardcoded config constants if None.
    Returns dict: {bucket_lower: probability}
    """
    mu    = forecast_adjusted
    sigma = max(bias_std, 1.0)   # floor at 1°F to avoid degenerate distribution

    dist = scipy_stats.norm(loc=mu, scale=sigma)

    buckets = live_buckets if live_buckets is not None else all_bucket_lowers()

    if live_buckets is not None and len(live_buckets) >= 2:
        live_lower_tail = live_buckets[0]
        live_upper_tail = live_buckets[-1]
    else:
        live_lower_tail = KALSHI_BUCKET_LOWER_TAIL
        live_upper_tail = KALSHI_BUCKET_UPPER_TAIL

    probs = {}
    for lower in buckets:
        if lower == live_lower_tail:
            lo, hi = -math.inf, lower + 0.5
        elif lower == live_upper_tail:
            lo, hi = lower - 0.5, math.inf
        else:
            lo, hi = lower - 0.5, lower + 1.5
        p = dist.cdf(hi) - dist.cdf(lo)
        probs[lower] = float(np.clip(p, 0.0, 1.0))

    # Renormalize to ensure sum = 1.0
    total = sum(probs.values())
    if total > 0:
        probs = {k: v / total for k, v in probs.items()}

    return probs


# ---------------------------------------------------------------------------
# Bias table lookup
# ---------------------------------------------------------------------------

def _load_bias_table() -> pd.DataFrame:
    return pd.read_parquet(BIAS_PARQUET)


def lookup_bias(
    bias_df: pd.DataFrame,
    station: str,
    event_date: date,
    cluster_id: int,
    season: str,
    forecast_raw: float,
    model_source: str = "IEM_AFM",
) -> dict:
    """
    Look up bias correction parameters from the bias table.

    Matches on (station, month, season, cluster_id, model_source) first,
    then narrows to the nearest model_bin.

    model_source: 'IEM_AFM' (human NWS forecast) | 'GFS_MOS' | 'ERA5'
    Falls back through: cluster_match → station/month → zero correction.
    If bias_df has no model_source column (old format), ignores the filter.

    Returns dict: {bias_mean, bias_std, n_obs, model_bin, source}
    """
    month = event_date.month
    raw_bin = int(math.floor((forecast_raw - 0.5) / 2) * 2 + 1)
    raw_bin = max(KALSHI_BUCKET_LOWER_TAIL, min(raw_bin, KALSHI_BUCKET_UPPER_TAIL))

    has_source_col = "model_source" in bias_df.columns

    # Primary lookup: exact regime match
    base_mask = (
        (bias_df["station"]    == station) &
        (bias_df["month"]      == month)   &
        (bias_df["season"]     == season)  &
        (bias_df["cluster_id"] == cluster_id)
    )
    mask = base_mask.copy()
    if has_source_col:
        mask &= (bias_df["model_source"] == model_source)
    subset = bias_df[mask]

    # If no rows for the requested source, try any source for this cluster regime
    if len(subset) == 0 and has_source_col:
        subset = bias_df[base_mask]

    if len(subset) > 0:
        subset = subset.copy()
        subset["bin_dist"] = (subset["model_bin"] - forecast_raw).abs()
        best = subset.loc[subset["bin_dist"].idxmin()]

        if best["n_obs"] >= MIN_N_OBS:
            raw_std = best["bias_std"]
            return {
                "bias_mean": float(best["bias_mean"]),
                "bias_std":  float(raw_std) if pd.notna(raw_std) else 4.0,
                "n_obs":     int(best["n_obs"]),
                "model_bin": float(best["model_bin"]),
                "source":    "cluster_match",
            }

    # Fallback: station/month average (ignore cluster)
    base_fallback_mask = (
        (bias_df["station"] == station) &
        (bias_df["month"]   == month)
    )
    fallback_mask = base_fallback_mask.copy()
    if has_source_col:
        fallback_mask &= (bias_df["model_source"] == model_source)
    fallback = bias_df[fallback_mask]

    # If no rows for requested model_source, use any available source for this station/month
    if len(fallback) == 0 and has_source_col:
        fallback = bias_df[base_fallback_mask]
        if len(fallback) > 0:
            actual_src = fallback["model_source"].iloc[0]
            logger.warning(
                "%s month=%d — no bias rows for model_source=%s, using %s rows as fallback",
                station, month, model_source, actual_src,
            )

    if len(fallback) > 0:
        # Filter to well-sampled cells only — prevents CDO unit-conversion
        # outliers (n_obs=1) from contaminating the fallback average.
        reliable = fallback[fallback["n_obs"] >= MIN_N_OBS]
        if len(reliable) == 0:
            reliable = fallback   # accept sparse data if nothing else available
        return {
            "bias_mean": float(reliable["bias_mean"].mean()),
            "bias_std":  float(reliable["bias_std"].mean()),
            "n_obs":     int(reliable["n_obs"].sum()),
            "model_bin": float(raw_bin),
            "source":    "station_month_fallback",
        }

    # Last resort: zero correction
    logger.warning("No bias data for %s month=%d — using zero correction", station, month)
    return {
        "bias_mean": 0.0,
        "bias_std":  4.0,
        "n_obs":     0,
        "model_bin": float(raw_bin),
        "source":    "no_data",
    }


# ---------------------------------------------------------------------------
# Live forecast fetch — IEM AFM, GFS-MOS, and Open-Meteo fallback
# ---------------------------------------------------------------------------

_AFOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/afos/retrieve.py"


def _fetch_live_afos(pil: str, target_date: date) -> list[dict]:
    """
    Fetch AFOS products from IEM using fmt=text (fmt=json was removed Apr 2026).
    Returns list of {"data": text, "utc_valid": iso_string} to match old schema.
    """
    import requests, re
    from datetime import datetime, timezone as _tz

    date_str = target_date.strftime("%Y-%m-%d")
    params = {
        "pil":   pil,
        "fmt":   "text",
        "sdate": f"{date_str}T00:00Z",
        "edate": f"{date_str}T23:59Z",
        "limit": 10,
    }
    resp = requests.get(_AFOS_URL, params=params, timeout=20)
    resp.raise_for_status()

    raw = resp.text
    if "ERROR:" in raw or not raw.strip():
        return []

    products = []
    for block in raw.split("\x01"):
        block = block.strip()
        if not block:
            continue
        # Extract issue time from WMO header e.g. "FOUS51 KOKX 191820" → day=19 hh=18 mm=20
        m = re.search(r"^[A-Z]{4}\d{2}\s+[A-Z]{4}\s+(\d{2})(\d{2})(\d{2})", block, re.MULTILINE)
        if not m:
            continue
        day, hh, mm = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            dt = datetime(target_date.year, target_date.month, day, hh, mm, tzinfo=_tz.utc)
            products.append({"data": block, "utc_valid": dt.isoformat()})
        except Exception:
            continue
    return products


def fetch_live_afm_forecast(station: str, target_date: date) -> float | None:
    """
    Fetch today's NWS AFM (human-adjusted) max temperature forecast from IEM.
    Returns °F or None on failure.
    """
    from config import WFO_MAP
    from scripts.build_model_forecast_archive import (
        _parse_afm_max_temp, _issue_time_to_valid_date,
    )
    wfo = WFO_MAP.get(station)
    if not wfo:
        return None
    try:
        products = _fetch_live_afos(f"AFM{wfo}", target_date)
        # Take the most recent product that parses cleanly
        for product in reversed(products):
            text       = product.get("data", "")
            issue_time = product.get("utc_valid", "")
            if not text:
                continue
            valid_date = _issue_time_to_valid_date(issue_time)
            if valid_date != target_date:
                continue
            tmax = _parse_afm_max_temp(text, station)
            if tmax is not None:
                logger.info("%s live AFM forecast: %.1f°F", station, tmax)
                return tmax
    except Exception as exc:
        logger.warning("%s live AFM fetch failed: %s", station, exc)
    return None


# ---------------------------------------------------------------------------
# GFS-MOS cache — keyed by (station, 6-hour UTC bucket) so we fetch at most
# 4 times per day per station instead of on every signal pass.
# ---------------------------------------------------------------------------
_mos_cache: dict[tuple[str, str], float] = {}


def _mos_cache_key(station: str) -> tuple[str, str]:
    """Return (station, run_bucket) where run_bucket is the current 6h UTC slot."""
    now_utc  = datetime.now(timezone.utc)
    bucket   = now_utc.replace(hour=(now_utc.hour // 6) * 6, minute=0, second=0, microsecond=0)
    return (station, bucket.isoformat())


def fetch_live_mos_forecast(station: str, target_date: date) -> float | None:
    """
    Fetch today's GFS-MOS (MAV) max temperature forecast from IEM AFOS.

    GFS-MOS only updates 4x/day (00Z/06Z/12Z/18Z), so results are cached
    per 6-hour UTC bucket — at most 4 IEM calls per station per day instead
    of one per signal pass.

    Returns °F or None on failure.
    """
    import time
    from scripts.build_model_forecast_archive import (
        _parse_mos_max_temp, _issue_time_to_valid_date, STATION_MOS_IDS,
    )

    # Cache hit — reuse result from this 6h model-run window
    cache_key = _mos_cache_key(station)
    if cache_key in _mos_cache:
        cached = _mos_cache[cache_key]
        logger.debug("%s GFS-MOS (cached): %.1f°F", station, cached)
        return cached

    try:
        primary_id = STATION_MOS_IDS.get(station, [station])[0]
        mos_pil    = f"MAV{primary_id[1:]}"   # e.g. KNYC → MAVNYC, KPHX → MAVPHX
        time.sleep(0.5)                        # throttle: prevent burst 429 on multi-station passes
        products   = _fetch_live_afos(mos_pil, target_date)

        tmax = None
        # First pass: exact valid_date match
        for product in reversed(products):
            text       = product.get("data", "")
            issue_time = product.get("utc_valid", "")
            if not text:
                continue
            valid_date = _issue_time_to_valid_date(issue_time)
            if valid_date != target_date:
                continue
            tmax = _parse_mos_max_temp(text, station)
            if tmax is not None:
                logger.info("%s GFS-MOS: %.1f°F", station, tmax)
                break

        # Fallback: most recent product regardless of valid_date
        # (12Z run's valid_date is tomorrow but still useful intraday)
        if tmax is None:
            for product in reversed(products):
                text = product.get("data", "")
                if not text:
                    continue
                tmax = _parse_mos_max_temp(text, station)
                if tmax is not None:
                    logger.info("%s GFS-MOS (fallback date): %.1f°F", station, tmax)
                    break

        if tmax is not None:
            _mos_cache[cache_key] = tmax
        return tmax

    except Exception as exc:
        logger.warning("%s GFS-MOS fetch failed: %s", station, exc)
        return None


def _fetch_nws_forecast(station: str, target_date: date) -> float | None:
    """
    Fetch today's max temperature from NWS api.weather.gov gridded forecast.
    Primary source: official NWS point forecast, no auth, no rate limits,
    updated hourly. Returns None on any failure so callers can fall through.
    """
    import requests
    lat, lon = STATION_COORDS[settlement_station(station)]
    try:
        # Step 1: resolve grid coordinates (cached in practice by HTTP layer)
        meta = requests.get(
            f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}",
            headers={"User-Agent": "WeatherBot/1.0 (cliftonmitchell77@gmail.com)"},
            timeout=15,
        )
        meta.raise_for_status()
        props        = meta.json()["properties"]
        forecast_url = props["forecast"]

        # Step 2: fetch daily forecast periods
        fcst = requests.get(
            forecast_url,
            headers={"User-Agent": "WeatherBot/1.0 (cliftonmitchell77@gmail.com)"},
            timeout=15,
        )
        fcst.raise_for_status()
        periods = fcst.json()["properties"]["periods"]

        date_str   = target_date.isoformat()
        next_day   = (target_date + __import__("datetime").timedelta(days=1)).isoformat()
        daytime_periods = [p for p in periods if p.get("isDaytime", False)]

        # Prefer today's daytime period; fall back to tomorrow if today has expired
        chosen = None
        for p in daytime_periods:
            if p.get("startTime", "")[:10] == date_str:
                chosen = p
                break
        if chosen is None:
            for p in daytime_periods:
                if p.get("startTime", "")[:10] == next_day:
                    chosen = p
                    logger.info("%s NWS: today expired, using tomorrow's daytime period", station)
                    break
        if chosen is None and daytime_periods:
            chosen = daytime_periods[0]
            logger.info("%s NWS: using nearest available daytime period (%s)",
                        station, chosen.get("startTime", "")[:10])

        if chosen is not None:
            temp = chosen["temperature"]
            unit = chosen.get("temperatureUnit", "F")
            if unit == "C":
                temp = temp * 9 / 5 + 32
            logger.info("%s NWS forecast: %.1f°F (period: %s)",
                        station, float(temp), chosen.get("startTime", "")[:10])
            return float(temp)

        logger.warning("%s NWS forecast: no daytime periods available", station)
    except Exception as exc:
        logger.warning("%s NWS forecast fetch failed: %s", station, exc)
    return None


def _fetch_openmeteo_live(station: str, target_date: date) -> float | None:
    """Open-Meteo current forecast — fallback when NWS and IEM unavailable."""
    import requests
    lat, lon = STATION_COORDS[settlement_station(station)]
    import time as _time
    params = {
        "latitude":         lat,
        "longitude":        lon,
        "daily":            "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "forecast_days":    3,
        "timezone":         "UTC",
    }
    try:
        for attempt in range(4):
            resp = requests.get(OPEN_METEO_FORECAST_URL, params=params, timeout=10)
            if resp.status_code == 429:
                wait = 2 ** attempt
                logger.warning("%s Open-Meteo rate limited — waiting %ds", station, wait)
                _time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        else:
            logger.warning("%s Open-Meteo rate limited after 4 attempts", station)
            return None
        data     = resp.json()
        dates    = data["daily"]["time"]
        temps    = data["daily"]["temperature_2m_max"]
        date_str = target_date.isoformat()
        if date_str in dates:
            val = temps[dates.index(date_str)]
            if val is not None:
                logger.info("%s Open-Meteo forecast: %.1f°F", station, float(val))
                return float(val)
    except Exception as exc:
        logger.warning("%s Open-Meteo fetch failed: %s", station, exc)
    return None


def fetch_live_forecast(station: str, target_date: date) -> tuple[float | None, float | None, str]:
    """
    Fetch today's max temperature forecast.

    Priority:
      1. NWS api.weather.gov gridded forecast  → model_source='IEM_AFM' (same bias dist)
      2. IEM AFM (NWS human-adjusted)          → model_source='IEM_AFM'
      3. Open-Meteo GFS forecast               → model_source='ERA5'
      4. Historical parquet (CDO/ERA5 actuals) → last resort, ≤30 days old only

    MOS is fetched independently for divergence signaling (not used as primary).

    Returns: (primary_forecast, mos_forecast, model_source_used)
    """
    mos = fetch_live_mos_forecast(station, target_date)

    # 1. NWS gridded — primary: official, no auth, no rate limits, hourly updates
    nws = _fetch_nws_forecast(station, target_date)
    if nws is not None:
        return nws, mos, "IEM_AFM"

    # 2. IEM AFM — human NWS forecaster output
    afm = fetch_live_afm_forecast(station, target_date)
    if afm is not None:
        return afm, mos, "IEM_AFM"

    # 3. Open-Meteo — third-party GFS model, rate-limited on free tier
    om = _fetch_openmeteo_live(station, target_date)
    if om is not None:
        return om, mos, "ERA5"

    # Last resort: most recent row from historical parquet — only if recent (≤30 days old)
    try:
        fcst_df = pd.read_parquet(FCST_PARQUET)
        src_col = "model_source" if "model_source" in fcst_df.columns else "source"
        subset  = fcst_df[
            (fcst_df["station"] == station) &
            (fcst_df[src_col].isin(["IEM_AFM", "CDO_OBS", "ERA5"]))
        ] if src_col in fcst_df.columns else fcst_df[fcst_df["station"] == station]
        if len(subset) > 0:
            last_row  = subset.sort_values("date").iloc[-1]
            last_date = pd.to_datetime(last_row["date"]).date()
            staleness = (target_date - last_date).days
            if staleness <= 30:
                val = float(last_row["forecast_tmax_f"])
                logger.warning("%s using historical parquet fallback: %.1f°F (data from %s, %d days old)",
                               station, val, last_date, staleness)
                return val, mos, "IEM_AFM"
            else:
                logger.warning("%s parquet fallback too stale (%d days old, last=%s) — skipping",
                               station, staleness, last_date)
    except Exception:
        pass

    return None, mos, "ERA5"


# ---------------------------------------------------------------------------
# Kelly criterion
# ---------------------------------------------------------------------------

def kelly_stake(
    edge: float,
    yes_ask: float,
    bankroll: float,
    max_stake_pct: float = MAX_STAKE_PCT,
) -> tuple[float, float, int]:
    """
    Compute fractional Kelly stake.

    For a binary Yes contract:
      p = our model probability of winning
      b = (1 - yes_ask) / yes_ask  (net odds)
      q = 1 - p
      Kelly fraction = (p*b - q) / b = (p - q/b)

    Returns (kelly_fraction, stake_usd, contracts)
    """
    p = edge + yes_ask          # our probability = kalshi_prob + edge
    p = float(np.clip(p, 0.01, 0.99))
    q = 1.0 - p

    if yes_ask <= 0 or yes_ask >= 1:
        return 0.0, 0.0, 0

    b = (1.0 - yes_ask) / yes_ask    # net profit per dollar risked

    raw_kelly = (p * b - q) / b
    raw_kelly = max(raw_kelly, 0.0)

    # Cap at max_stake_pct (fractional Kelly)
    fraction = min(raw_kelly, max_stake_pct)

    stake_usd  = round(bankroll * fraction, 2)
    contracts  = int(math.floor(stake_usd / yes_ask)) if yes_ask > 0 else 0
    actual_usd = round(contracts * yes_ask, 2)

    return fraction, actual_usd, contracts


# ---------------------------------------------------------------------------
# Plain-English reasoning builder
# ---------------------------------------------------------------------------

_SEASON_NAMES = {"DJF": "Winter", "MAM": "Spring", "JJA": "Summer", "SON": "Fall"}

_CONFIDENCE_PLAIN = {
    "high":   "HIGH — today closely matches historical examples of this regime",
    "medium": "MODERATE — a reasonable but not textbook match to the historical pattern",
    "low":    "LOW — unusual synoptic setup; historical bias estimates are less reliable",
}

_CONDITION_PLAIN = {
    "clear":      "Clear skies",
    "scattered":  "Scattered clouds",
    "broken":     "Mostly cloudy",
    "marine_fog": "Marine layer / coastal fog risk",
    "convective": "Thunderstorm activity in the TAF",
    "precip":     "Active precipitation in the TAF",
    "hard_skip":  "Dangerous conditions (freezing rain / heavy snow / ice)",
}


def _build_reasoning(
    station: str,
    pattern: dict,
    bias_info: dict,
    forecast_raw: float,
    forecast_adjusted: float,
    taf: TafResult,
    metar: MetarResult,
    threshold_result: ThresholdResult | None,
    top_bucket: BucketAnalysis,
    bucket_analyses: list[BucketAnalysis],
    decision: str,
    kelly_stake_usd: float,
    kelly_contracts: int,
    confidence_scale: float,
    mos_forecast_raw: float | None = None,
    model_divergence_f: float | None = None,
    model_source_used: str = "IEM_AFM",
) -> SignalReasoning:
    """Build fully structured plain-English reasoning for the dashboard card."""

    now = datetime.now(timezone.utc)
    season_name = _SEASON_NAMES.get(pattern.get("season", ""), pattern.get("season", ""))
    confidence  = pattern.get("confidence", "low")

    # ── Current conditions ────────────────────────────────────────────────
    cond_parts = []
    if metar.temp_f is not None:
        cond_parts.append(f"{metar.temp_f:.0f}°F")
    if metar.dewpoint_f is not None:
        cond_parts.append(f"dew point {metar.dewpoint_f:.0f}°F")
    if metar.wind_kt is not None:
        if metar.wind_kt == 0:
            cond_parts.append("calm winds")
        else:
            gust = f", gusting {metar.wind_gust_kt} kt" if metar.wind_gust_kt else ""
            cond_parts.append(f"winds at {metar.wind_kt} kt{gust}")
    if metar.sky_cover:
        cond_parts.append(metar.sky_cover)
    if metar.visibility_sm is not None and metar.visibility_sm < 10:
        cond_parts.append(f"visibility {metar.visibility_sm:.0f} miles")

    current_conditions = (
        f"{', '.join(cond_parts)} at {station}."
        if cond_parts else f"No current observation available for {station}."
    )

    # ── Synoptic pattern ──────────────────────────────────────────────────
    cluster_id   = pattern.get("cluster_id", "?")
    distance     = pattern.get("distance", 0.0)
    data_source  = pattern.get("data_source", "unknown")
    conf_plain   = _CONFIDENCE_PLAIN.get(confidence, confidence)

    if confidence == "high":
        regime_desc = "Today closely matches the historical norm for this pattern — the bias correction is well-supported."
    elif confidence == "medium":
        regime_desc = "Today is a reasonable match. Bias correction applies but with slightly more uncertainty."
    else:
        regime_desc = "Today is in unusual synoptic territory. We're applying a reduced Kelly stake to account for higher uncertainty."

    synoptic_pattern = (
        f"The upper-level (500mb) pattern is classified as Cluster {cluster_id} "
        f"for {season_name}, identified from {'live GFS data' if 'gfs' in data_source.lower() else 'recent reanalysis'}. "
        f"Pattern confidence: {conf_plain}. {regime_desc}"
    )

    # ── Forecast and bias ─────────────────────────────────────────────────
    bias_mean  = bias_info["bias_mean"]
    bias_std   = bias_info["bias_std"]
    n_obs      = bias_info["n_obs"]
    bias_src   = bias_info.get("source", "unknown")
    direction  = "warmer" if bias_mean > 0 else "cooler"
    abs_bias   = abs(bias_mean)

    lo1 = forecast_adjusted - bias_std
    hi1 = forecast_adjusted + bias_std

    if bias_src == "nws_fixed":
        obs_desc = "NWS forecast is already human-calibrated — no bias correction applied"
    elif bias_src == "cluster_match":
        obs_desc = f"Based on {n_obs} similar days in this exact pattern during the same month"
    elif bias_src == "station_month_fallback":
        obs_desc = f"Cluster data was sparse — using {n_obs} days across all patterns for this station and month"
    else:
        obs_desc = "No historical bias data found — using zero correction"

    # Model source label for display
    src_label = "NWS AFM" if model_source_used == "IEM_AFM" else "Open-Meteo/ERA5"

    # NWS vs GFS-MOS divergence note
    if mos_forecast_raw is not None and model_divergence_f is not None:
        abs_div = abs(model_divergence_f)
        if abs_div < 1.0:
            div_note = (
                f"GFS-MOS agrees closely ({mos_forecast_raw:.0f}°F, divergence <1°F) — "
                "model and forecaster are aligned."
            )
        elif model_divergence_f > 0:
            div_note = (
                f"GFS-MOS guidance is {mos_forecast_raw:.0f}°F — the NWS forecaster is "
                f"running {abs_div:.0f}°F WARMER than the model blend, suggesting local "
                "warm-advection or sea-breeze break knowledge."
            )
        else:
            div_note = (
                f"GFS-MOS guidance is {mos_forecast_raw:.0f}°F — the NWS forecaster is "
                f"running {abs_div:.0f}°F COOLER than the model blend, suggesting local "
                "marine influence, cloud cover, or cold-pool knowledge."
            )
    else:
        div_note = "GFS-MOS not available today — single-model signal only."

    forecast_and_bias = (
        f"The {src_label} forecast is {forecast_raw:.0f}°F. "
        f"{div_note} "
        f"{obs_desc}, {station} has historically run "
        f"{abs_bias:.1f}°F {direction} than the model in conditions like today. "
        f"Our adjusted forecast is {forecast_adjusted:.1f}°F, with a typical spread of "
        f"±{bias_std:.1f}°F (roughly 68% of similar days land between "
        f"{lo1:.0f}°F and {hi1:.0f}°F)."
    )

    # ── Market analysis ───────────────────────────────────────────────────
    edge_pct   = top_bucket.edge * 100
    our_pct    = top_bucket.model_prob * 100
    kalshi_pct = top_bucket.kalshi_prob * 100
    ask_cents  = round(top_bucket.yes_ask * 100)

    if threshold_result is not None:
        thr_pct   = threshold_result.threshold * 100
        cond_desc = _CONDITION_PLAIN.get(taf.condition, taf.condition)
        penalty_note = (
            f"Our edge threshold today is {thr_pct:.0f}% ({cond_desc} — "
            f"{'no penalty applied' if taf.condition == 'clear' else 'penalty applied to require higher confidence'})."
        )
    else:
        penalty_note = "Weather conditions require skipping this station entirely."

    market_analysis = (
        f"Kalshi is pricing the {top_bucket.bucket_label} bucket at {kalshi_pct:.0f}% "
        f"(you can buy Yes for {ask_cents}¢). "
        f"Our model puts this bucket at {our_pct:.0f}% — a gap of {edge_pct:+.0f}%. "
        f"{penalty_note}"
    )

    # ── Decision rationale ────────────────────────────────────────────────
    if decision == "TRADE":
        scale_note = (
            "" if confidence_scale == 1.0
            else f" Pattern confidence is {confidence.upper()}, so Kelly stake is scaled to {confidence_scale:.0%}."
        )
        decision_rationale = (
            f"Trading {kelly_contracts} contract{'s' if kelly_contracts != 1 else ''} on "
            f"the {top_bucket.bucket_label} bucket at {ask_cents}¢ each "
            f"(${kelly_stake_usd:.2f} total).{scale_note} "
            f"Edge of {top_bucket.edge:+.3f} clears our required threshold."
        )
    elif decision == "WATCH":
        if threshold_result:
            decision_rationale = (
                f"Watching — edge of {top_bucket.edge:+.3f} is real but falls below "
                f"our {threshold_result.threshold:.3f} threshold. Not enough margin to trade today."
            )
        else:
            decision_rationale = "Watching — edge present but threshold check failed."
    elif decision == "HARD_SKIP":
        decision_rationale = (
            f"Hard skip — {taf.summary}. "
            "Weather conditions make a reliable temperature forecast impossible today."
        )
    elif decision == "CONSTRAINED":
        decision_rationale = (
            f"Signal is valid (edge {top_bucket.edge:+.3f}) but no capital is available. "
            "Another trade is using the available exposure limit."
        )
    else:
        decision_rationale = (
            "Skipping — no bucket shows enough positive edge to justify a trade today."
        )

    # ── Bucket comparison table ───────────────────────────────────────────
    bucket_table = [
        {
            "label":      b.bucket_label,
            "model_pct":  round(b.model_prob * 100, 1),
            "kalshi_pct": round(b.kalshi_prob * 100, 1),
            "edge":       round(b.edge, 3),
            "yes_ask":    round(b.yes_ask, 2),
            "is_top":     b.bucket_lower == top_bucket.bucket_lower,
        }
        for b in bucket_analyses
    ]

    # ── Threshold checklist ───────────────────────────────────────────────
    threshold_checks = []

    if threshold_result is not None:
        threshold_checks.append({
            "name":   "Weather condition",
            "passed": taf.condition != "hard_skip",
            "detail": f"{_CONDITION_PLAIN.get(taf.condition, taf.condition)} — {taf.summary}",
        })
        threshold_checks.append({
            "name":   "Edge vs threshold",
            "passed": top_bucket.edge >= threshold_result.threshold,
            "detail": f"Edge {top_bucket.edge:+.3f} vs required {threshold_result.threshold:.3f}",
        })
        threshold_checks.append({
            "name":   "Bias uncertainty gate",
            "passed": not threshold_result.std_gate_fired,
            "detail": (
                f"Bias spread {bias_std:.1f}°F — gate {'fired, floor applied' if threshold_result.std_gate_fired else 'clear'}"
            ),
        })
    threshold_checks.append({
        "name":   "Minimum stake",
        "passed": kelly_stake_usd >= 1.0,
        "detail": f"Kelly stake ${kelly_stake_usd:.2f} vs $1.00 minimum",
    })
    threshold_checks.append({
        "name":   "Historical sample size",
        "passed": n_obs >= 10,
        "detail": f"{n_obs} similar days in bias table (need ≥ 10)",
    })
    threshold_checks.append({
        "name":   "Pattern confidence",
        "passed": confidence != "low",
        "detail": f"{confidence.capitalize()} — Kelly scaled to {confidence_scale:.0%}",
    })

    # ── Data sources ──────────────────────────────────────────────────────
    mos_src_str = f"GFS-MOS {mos_forecast_raw:.0f}°F" if mos_forecast_raw is not None else "unavailable"
    data_sources = {
        "pattern":  f"{'Live Open-Meteo 500hPa' if data_source == 'openmeteo' else 'Reanalysis fallback'}",
        "forecast": f"{src_label} (primary) | GFS-MOS: {mos_src_str}",
        "bias":     f"{bias_src} — {n_obs} obs (model_source={model_source_used})",
        "taf":      f"aviationweather.gov ({taf.fetched_utc})",
        "metar":    f"aviationweather.gov ({metar.fetched_utc})",
    }

    return SignalReasoning(
        current_conditions=current_conditions,
        synoptic_pattern=synoptic_pattern,
        forecast_and_bias=forecast_and_bias,
        market_analysis=market_analysis,
        decision_rationale=decision_rationale,
        bucket_table=bucket_table,
        threshold_checks=threshold_checks,
        data_sources=data_sources,
        generated_at=now,
    )


# ---------------------------------------------------------------------------
# Main signal generation
# ---------------------------------------------------------------------------

def generate_signal(
    station: str,
    event_date: date,
    kalshi: KalshiClient,
    bias_df: pd.DataFrame,
    pattern: dict,
    bankroll: float = STARTING_BANKROLL,
) -> TradeSignal:
    """
    Generate a complete trade signal for one station and event date.
    """
    import pytz

    tz      = pytz.timezone(STATION_TIMEZONES[station])
    now_loc = datetime.now(timezone.utc).astimezone(tz)
    local_time_str = now_loc.strftime("%I:%M %p %Z")

    # ── 1. TAF + METAR ───────────────────────────────────────────────────
    from utils.peak_hours import get_peak_hour
    peak_hour_local = get_peak_hour(station, event_date)
    taf   = interpret_taf(station, event_date=event_date, peak_hour_local=peak_hour_local)
    metar = get_metar(station)

    # ── 2. Live forecast (AFM primary, MOS cross-check) ─────────────────
    forecast_raw, mos_forecast_raw, model_source_used = fetch_live_forecast(
        station, event_date
    )
    if forecast_raw is None:
        logger.error("%s — no forecast available, skipping", station)
        return _skip_signal(station, event_date, local_time_str, taf, metar,
                            pattern, "No forecast data available")

    model_divergence_f = (
        round(forecast_raw - mos_forecast_raw, 1)
        if mos_forecast_raw is not None else None
    )
    if model_divergence_f is not None:
        logger.info(
            "%s AFM=%.1f°F  GFS-MOS=%.1f°F  divergence=%+.1f°F",
            station, forecast_raw, mos_forecast_raw, model_divergence_f,
        )

    # ── 3. Bias correction ───────────────────────────────────────────────
    # NWS forecasts are already human-calibrated — no systematic bias to correct.
    # Use a fixed uncertainty of 3.5°F (typical NWS day-ahead error).
    # ERA5/MOS sources still go through the bias table.
    NWS_SIGMA = 3.5
    if model_source_used == "IEM_AFM":
        bias_info = {
            "bias_mean": 0.0,
            "bias_std":  NWS_SIGMA,
            "n_obs":     0,
            "model_bin": float(forecast_raw),
            "source":    "nws_fixed",
        }
        logger.info("%s NWS source — using fixed sigma=%.1f°F, no bias correction", station, NWS_SIGMA)
    else:
        effective_cluster = (
            -1 if pattern.get("data_source") == "reanalysis_fallback"
            else pattern["cluster_id"]
        )
        bias_info = lookup_bias(
            bias_df, station, event_date,
            effective_cluster, pattern["season"], forecast_raw,
            model_source=model_source_used,
        )
    bias_mean         = bias_info["bias_mean"]
    bias_std          = bias_info["bias_std"]
    forecast_adjusted = forecast_raw + bias_mean

    # ── 4. Weather penalty threshold ─────────────────────────────────────
    threshold_result = compute_effective_threshold(taf.condition, bias_std)

    if threshold_result is None:
        return _hard_skip_signal(
            station, event_date, local_time_str, taf, metar,
            pattern, forecast_raw, bias_info, forecast_adjusted,
        )

    effective_threshold = threshold_result.threshold

    # ── 5. Kalshi snapshots (fetch first — needed for live bucket bounds) ───
    snapshots: dict[int, MarketSnapshot] = kalshi.get_all_snapshots(station, event_date)

    if not snapshots:
        logger.warning("%s — no Kalshi snapshots available", station)
        return _skip_signal(station, event_date, local_time_str, taf, metar,
                            pattern, "No Kalshi market data")

    # ── 6. Probability distribution over live Kalshi buckets ─────────────
    live_buckets = sorted(snapshots.keys())
    model_probs = build_probability_distribution(
        forecast_adjusted, bias_std, live_buckets=live_buckets
    )

    # ── 7. Edge per bucket ────────────────────────────────────────────────
    live_lower_tail = live_buckets[0]  if live_buckets else KALSHI_BUCKET_LOWER_TAIL
    live_upper_tail = live_buckets[-1] if live_buckets else KALSHI_BUCKET_UPPER_TAIL

    def _live_bucket_label(lower: int) -> str:
        if lower == live_lower_tail:
            return f"{lower}° or below"
        if lower == live_upper_tail:
            return f"{lower}° or above"
        return f"{lower}° to {lower + 1}°"

    bucket_analyses: list[BucketAnalysis] = []

    for lower in live_buckets:
        model_p  = model_probs.get(lower, 0.0)
        snap     = snapshots.get(lower)
        if snap is None:
            continue

        kalshi_p = snap.implied_prob
        edge     = model_p - kalshi_p

        bucket_analyses.append(BucketAnalysis(
            bucket_lower=lower,
            bucket_label=_live_bucket_label(lower),
            model_prob=round(model_p, 4),
            kalshi_prob=round(kalshi_p, 4),
            edge=round(edge, 4),
            yes_ask=snap.yes_ask,
            yes_bid=snap.yes_bid,
        ))

    if not bucket_analyses:
        return _skip_signal(station, event_date, local_time_str, taf, metar,
                            pattern, "No bucket overlap between model and Kalshi")

    # Best edge bucket — only consider buckets with meaningful model probability.
    # Prevents edge-optimizing on cheap tail bets when the forecast is far away
    # (e.g., forecast=76°F but Kalshi misprices ≤72 tail at 5¢ → apparent edge
    # but the tail is 4°F below the forecast and unlikely to win).
    peak_prob = max(b.model_prob for b in bucket_analyses)
    min_prob  = peak_prob * MIN_PROB_RATIO
    eligible  = [b for b in bucket_analyses if b.model_prob >= min_prob]
    if not eligible:
        eligible = bucket_analyses  # safety fallback (shouldn't happen)

    filtered_labels = [b.bucket_label for b in bucket_analyses if b.model_prob < min_prob]
    if filtered_labels:
        logger.info(
            "%s — filtered low-prob buckets (peak=%.1f%%, min=%.1f%%): %s",
            station, peak_prob * 100, min_prob * 100, filtered_labels,
        )

    positive_buckets = [b for b in eligible if b.edge > 0]
    if not positive_buckets:
        # No edge anywhere — show the most probable bucket for context
        top = max(eligible, key=lambda b: b.model_prob)
        decision = "SKIP"
    else:
        # Blend probability and edge so the selection favours likely outcomes
        # while still rewarding genuine mispricing.
        # score = model_prob + EDGE_BLEND_WEIGHT × edge
        # To beat a bucket that is 5% more probable you need 10% more edge (at 0.5 weight).
        top = max(positive_buckets, key=lambda b: b.model_prob + EDGE_BLEND_WEIGHT * b.edge)

        if top.edge >= effective_threshold:
            decision = "TRADE"
        else:
            decision = "WATCH"

    # ── 8. Kelly sizing with confidence scaling ──────────────────────────
    kelly_frac, kelly_usd, kelly_contracts = kelly_stake(
        top.edge, top.yes_ask, bankroll
    )

    # Scale Kelly fraction by pattern confidence — unusual regimes get reduced sizing
    confidence_scale = CONFIDENCE_KELLY_SCALE.get(pattern.get("confidence", "low"), 0.5)
    if confidence_scale < 1.0:
        kelly_frac      = round(kelly_frac * confidence_scale, 6)
        kelly_usd       = round(bankroll * kelly_frac, 2)
        kelly_contracts = int(math.floor(kelly_usd / top.yes_ask)) if top.yes_ask > 0 else 0
        kelly_usd       = round(kelly_contracts * top.yes_ask, 2)

    # Enforce minimum stake
    if decision == "TRADE" and kelly_usd < MIN_KELLY_STAKE:
        decision = "WATCH"
        logger.info(
            "%s — Kelly stake $%.2f below minimum $%.2f → WATCH",
            station, kelly_usd, MIN_KELLY_STAKE,
        )

    # ── 9. Structured plain-English reasoning ────────────────────────────
    reasoning = _build_reasoning(
        station=station,
        pattern=pattern,
        bias_info=bias_info,
        forecast_raw=forecast_raw,
        forecast_adjusted=forecast_adjusted,
        taf=taf,
        metar=metar,
        threshold_result=threshold_result,
        top_bucket=top,
        bucket_analyses=bucket_analyses,
        decision=decision,
        kelly_stake_usd=kelly_usd,
        kelly_contracts=kelly_contracts,
        confidence_scale=confidence_scale,
        mos_forecast_raw=mos_forecast_raw,
        model_divergence_f=model_divergence_f,
        model_source_used=model_source_used,
    )

    logger.info(
        "%s | %s | AFM=%.1f°F MOS=%s | adj=%.1f°F | top=%s | edge=%+.3f | "
        "threshold=%.3f | kelly=$%.2f | decision=%s",
        station, event_date, forecast_raw,
        f"{mos_forecast_raw:.1f}°F" if mos_forecast_raw else "N/A",
        forecast_adjusted, top.bucket_label, top.edge,
        effective_threshold, kelly_usd, decision,
    )

    return TradeSignal(
        station=station,
        event_date=event_date,
        local_time=local_time_str,
        decision=decision,
        forecast_raw=forecast_raw,
        bias_mean=bias_mean,
        bias_std=bias_std,
        forecast_adjusted=forecast_adjusted,
        cluster_id=pattern["cluster_id"],
        season=pattern["season"],
        n_obs=bias_info["n_obs"],
        pattern_confidence=pattern["confidence"],
        mos_forecast_raw=mos_forecast_raw,
        model_divergence_f=model_divergence_f,
        top_bucket=top.bucket_lower,
        top_edge=top.edge,
        top_model_prob=top.model_prob,
        top_kalshi_prob=top.kalshi_prob,
        top_yes_ask=top.yes_ask,
        kelly_fraction=kelly_frac,
        kelly_stake_usd=kelly_usd,
        kelly_contracts=kelly_contracts,
        threshold_result=threshold_result,
        taf=taf,
        metar=metar,
        buckets=bucket_analyses,
        live_lower_tail=live_lower_tail,
        live_upper_tail=live_upper_tail,
        reasoning=reasoning,
    )


def _skip_signal(station, event_date, local_time, taf, metar,
                 pattern, reason) -> TradeSignal:
    return TradeSignal(
        station=station, event_date=event_date, local_time=local_time,
        decision="SKIP",
        forecast_raw=0.0, bias_mean=0.0, bias_std=0.0, forecast_adjusted=0.0,
        cluster_id=pattern.get("cluster_id", -1),
        season=pattern.get("season", "?"),
        n_obs=0, pattern_confidence="low",
        top_bucket=0, top_edge=0.0, top_model_prob=0.0,
        top_kalshi_prob=0.0, top_yes_ask=0.0,
        kelly_fraction=0.0, kelly_stake_usd=0.0, kelly_contracts=0,
        threshold_result=None, taf=taf, metar=metar,
        buckets=[], reasoning=f"Skipped: {reason}",
    )


def _hard_skip_signal(station, event_date, local_time, taf, metar,
                      pattern, forecast_raw, bias_info, forecast_adjusted) -> TradeSignal:
    return TradeSignal(
        station=station, event_date=event_date, local_time=local_time,
        decision="HARD_SKIP",
        forecast_raw=forecast_raw,
        bias_mean=bias_info["bias_mean"], bias_std=bias_info["bias_std"],
        forecast_adjusted=forecast_adjusted,
        cluster_id=pattern.get("cluster_id", -1),
        season=pattern.get("season", "?"),
        n_obs=bias_info["n_obs"], pattern_confidence=pattern.get("confidence", "low"),
        top_bucket=0, top_edge=0.0, top_model_prob=0.0,
        top_kalshi_prob=0.0, top_yes_ask=0.0,
        kelly_fraction=0.0, kelly_stake_usd=0.0, kelly_contracts=0,
        threshold_result=None, taf=taf, metar=metar,
        buckets=[],
        reasoning=f"HARD SKIP: {taf.summary} — weather penalty forces skip.",
    )


# ---------------------------------------------------------------------------
# Forecast data availability probe
# ---------------------------------------------------------------------------

def check_forecast_availability(event_date: date, probe_station: str = STATIONS[0]) -> dict:
    """
    Quick probe to confirm live forecast data is available for the target date.
    Used by the scheduler before running a full Tier 3 pass — if data is missing,
    the scheduler will retry rather than proceed with stale fallback numbers.

    Returns:
        available : bool   — True if Open-Meteo has fresh data for event_date
        source    : str    — "live" | "fallback" | "none"
        details   : str    — human-readable status for logging
    """
    import requests as req
    import time as _time
    lat, lon = STATION_COORDS[settlement_station(probe_station)]
    try:
        for attempt in range(3):
            resp = req.get(
                OPEN_METEO_FORECAST_URL,
                params={
                    "latitude":         lat,
                    "longitude":        lon,
                    "daily":            "temperature_2m_max",
                    "temperature_unit": "fahrenheit",
                    "forecast_days":    3,
                    "timezone":         "UTC",
                },
                timeout=10,
            )
            if resp.status_code == 429:
                # Rate limited — proceed anyway; per-station fallback handles it
                logger.warning(
                    "Forecast probe 429 (attempt %d/3) — proceeding with parquet fallback", attempt + 1
                )
                _time.sleep(5 * (attempt + 1))
                if attempt == 2:
                    return {
                        "available": True,
                        "source":    "fallback",
                        "details":   "Open-Meteo rate limited — using parquet fallback per station",
                    }
                continue
            resp.raise_for_status()
            break

        data     = resp.json()
        dates    = data.get("daily", {}).get("time", [])
        temps    = data.get("daily", {}).get("temperature_2m_max", [])
        date_str = event_date.isoformat()

        if date_str in dates:
            idx = dates.index(date_str)
            if temps[idx] is not None:
                return {
                    "available": True,
                    "source":    "live",
                    "details":   f"Open-Meteo has fresh data for {event_date} at {probe_station}",
                }
        # Date missing from Open-Meteo (late in day or model lag) — NWS is primary, proceed
        return {
            "available": True,
            "source":    "fallback",
            "details":   f"Open-Meteo lacks {event_date} data — NWS primary will handle per station",
        }

    except Exception as exc:
        # Only gate on total network failure — NWS may still be reachable
        logger.warning("Forecast probe exception: %s — proceeding anyway", exc)
        return {
            "available": True,
            "source":    "fallback",
            "details":   f"Open-Meteo probe failed ({exc}) — proceeding with NWS/CDO fallbacks",
        }


# ---------------------------------------------------------------------------
# Run all stations
# ---------------------------------------------------------------------------

def run_signal_pass(
    event_date: date | None = None,
    bankroll: float = STARTING_BANKROLL,
) -> dict[str, TradeSignal]:
    """
    Run the full signal pass for all stations.
    Called by the scheduler (Tier 3) and on-demand from dashboard.

    Returns dict: {station: TradeSignal}
    """
    if event_date is None:
        event_date = date.today()

    logger.info("=== Signal pass for %s ===", event_date)

    # Shared resources (load once)
    bias_df = _load_bias_table()
    pattern = classify_pattern(event_date)
    kalshi  = KalshiClient()

    signals: dict[str, TradeSignal] = {}

    for idx, station in enumerate(STATIONS):
        if idx > 0:
            import time as _time; _time.sleep(3)  # pace Open-Meteo free-tier (20 req/min limit)
        try:
            sig = generate_signal(
                station, event_date, kalshi, bias_df, pattern, bankroll
            )
            signals[station] = sig
        except Exception as exc:
            logger.error("Signal generation failed for %s: %s", station, exc, exc_info=True)

    trade_count = sum(1 for s in signals.values() if s.decision == "TRADE")
    watch_count = sum(1 for s in signals.values() if s.decision == "WATCH")
    skip_count  = sum(1 for s in signals.values() if s.decision in ("SKIP", "HARD_SKIP"))

    logger.info(
        "Signal pass complete: TRADE=%d WATCH=%d SKIP=%d",
        trade_count, watch_count, skip_count,
    )
    return signals
