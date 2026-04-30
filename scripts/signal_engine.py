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
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from utils.logging_config import setup_logging
from utils.weather_penalty import compute_weather_gate, describe_gate
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
    STATION_COORDS,
    MIN_KELLY_STAKE,
    settlement_station,
    STARTING_BANKROLL,
    MAX_STAKE_PCT,
    MIN_N_OBS,
    MIN_EDGE,
    BIAS_STD_GATE,
    CONFIDENCE_KELLY_SCALE,
    MIN_PROB_RATIO,
    MIN_BUCKET_PROB,
    MOS_DIVERGENCE_THRESHOLD,
    NWS_BLEND_WEIGHT,
    NBM_BLEND_WEIGHT,
    NBM_DIVERGENCE_GATE,
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

    # Weather gate
    weather_gate:       str            # "trade" | "skip" | "hard_skip"
    taf:                TafResult
    metar:              MetarResult

    # GFS-MOS cross-check (optional — None when IEM MAV unavailable)
    mos_forecast_raw:   Optional[float] = None   # GFS-MOS Day-1 max forecast
    model_divergence_f: Optional[float] = None   # AFM - MOS (+ means NWS warmer than model)

    # NBM cross-check (optional — None when Herbie unavailable)
    nbm_forecast_raw:   Optional[float] = None   # NBM daily max temp forecast
    nbm_divergence_f:   Optional[float] = None   # NWS_AFM - NBM (+ means NWS warmer)

    # Active forecast model — "IEM_AFM" | "GFS" | "ECMWF" | "BLEND" | "OPEN_METEO"
    model_source:       str             = "IEM_AFM"

    # Full distribution
    buckets:            list[BucketAnalysis] = field(default_factory=list)

    # Live market tail bounds (vary by station/season — differ from config constants)
    live_lower_tail:    int = 68
    live_upper_tail:    int = 77

    # Structured reasoning for dashboard card
    reasoning:          Optional[SignalReasoning] = None

    # Plain-English explanation shown on skip/hard-skip station cards
    skip_reason:        str = ""

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


# Per-station NBM cache: key = "{station}_{date_iso}", value = °F or None
_nbm_cache: dict[str, float | None] = {}


def fetch_nbm_forecast(station: str, target_date: date) -> float | None:
    """Fetch NBM daily max temperature for a station. Cached per (station, date)."""
    from utils.herbie_fetcher import fetch_nbm_tmax

    cache_key = f"{station}_{target_date.isoformat()}"
    if cache_key in _nbm_cache:
        return _nbm_cache[cache_key]

    settle = settlement_station(station)
    coords = STATION_COORDS.get(settle)
    if coords is None:
        _nbm_cache[cache_key] = None
        return None

    lat, lon = coords
    result = fetch_nbm_tmax(lat, lon, target_date)
    _nbm_cache[cache_key] = result
    return result


def _blend_nws_nbm(nws: float, nbm: float | None) -> float:
    """60% NWS AFM + 40% NBM when both available; pure NWS when NBM missing."""
    if nbm is None:
        return nws
    return round(NWS_BLEND_WEIGHT * nws + NBM_BLEND_WEIGHT * nbm, 2)


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
        next_day   = (target_date + timedelta(days=1)).isoformat()
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


def fetch_live_forecast(
    station: str, target_date: date
) -> tuple[float | None, float | None, str, float | None]:
    """
    Fetch today's max temperature forecast.

    Priority:
      1. NWS api.weather.gov gridded forecast + NBM blend → model_source='IEM_AFM'
      2. IEM AFM + NBM blend                              → model_source='IEM_AFM'
      3. NBM alone                                        → model_source='NBM'
      4. Historical parquet (CDO/ERA5 actuals)            → last resort, ≤30 days old only

    MOS is fetched independently for divergence signaling (not used as primary).
    NBM is fetched in parallel for blending and divergence checking.

    Returns: (primary_forecast, mos_forecast, model_source_used, nbm_forecast)
    """
    mos = fetch_live_mos_forecast(station, target_date)
    nbm = fetch_nbm_forecast(station, target_date)

    # 1. NWS gridded — primary: official, no auth, no rate limits, hourly updates
    nws = _fetch_nws_forecast(station, target_date)
    if nws is not None:
        blended = _blend_nws_nbm(nws, nbm)
        return blended, mos, "IEM_AFM", nbm

    # 2. IEM AFM — human NWS forecaster output
    afm = fetch_live_afm_forecast(station, target_date)
    if afm is not None:
        blended = _blend_nws_nbm(afm, nbm)
        return blended, mos, "IEM_AFM", nbm

    # 3. NBM alone — NOAA-direct, no throttling
    if nbm is not None:
        logger.info("%s using NBM as primary forecast: %.1f°F", station, nbm)
        return nbm, mos, "NBM", nbm

    # Last resort: most recent row from historical parquet — only if recent (≤30 days old)
    try:
        fcst_df = pd.read_parquet(FCST_PARQUET)
        src_col = "model_source" if "model_source" in fcst_df.columns else "source"
        subset  = fcst_df[
            (fcst_df["station"] == station) &
            (fcst_df[src_col].isin(["IEM_AFM", "CDO_OBS", "ERA5", "NBM"]))
        ] if src_col in fcst_df.columns else fcst_df[fcst_df["station"] == station]
        if len(subset) > 0:
            last_row  = subset.sort_values("date").iloc[-1]
            last_date = pd.to_datetime(last_row["date"]).date()
            staleness = (target_date - last_date).days
            if staleness <= 30:
                val = float(last_row["forecast_tmax_f"])
                logger.warning("%s using historical parquet fallback: %.1f°F (data from %s, %d days old)",
                               station, val, last_date, staleness)
                return val, mos, "IEM_AFM", nbm
            else:
                logger.warning("%s parquet fallback too stale (%d days old, last=%s) — skipping",
                               station, staleness, last_date)
    except Exception:
        pass

    return None, mos, "ERA5", nbm


# ---------------------------------------------------------------------------
# Kelly criterion
# ---------------------------------------------------------------------------

def kelly_stake(
    model_prob: float,
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
    p = float(np.clip(model_prob, 0.01, 0.99))
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
    weather_gate: str,
    bias_std_gate_fired: bool,
    top_bucket: BucketAnalysis,
    bucket_analyses: list[BucketAnalysis],
    decision: str,
    kelly_stake_usd: float,
    kelly_contracts: int,
    confidence_scale: float,
    mos_forecast_raw: float | None = None,
    model_divergence_f: float | None = None,
    model_source_used: str = "IEM_AFM",
    nbm_forecast_raw: float | None = None,
    nbm_divergence_f: float | None = None,
    p4_gfs_raw: float | None = None,
    p4_ecmwf_raw: float | None = None,
) -> SignalReasoning:
    """Build fully structured plain-English reasoning for the dashboard card."""

    now         = datetime.now(timezone.utc)
    season_name = _SEASON_NAMES.get(pattern.get("season", ""), pattern.get("season", ""))
    confidence  = pattern.get("confidence", "low")
    cluster_id  = pattern.get("cluster_id", "?")
    distance    = pattern.get("distance", 0.0)
    data_source = pattern.get("data_source", "unknown")
    conf_plain  = _CONFIDENCE_PLAIN.get(confidence, confidence)
    bias_mean   = bias_info["bias_mean"]
    bias_std    = bias_info["bias_std"]
    n_obs       = bias_info["n_obs"]
    bias_src    = bias_info.get("source", "unknown")
    direction   = "warmer" if bias_mean > 0 else "cooler"
    abs_bias    = abs(bias_mean)
    lo1         = forecast_adjusted - bias_std
    hi1         = forecast_adjusted + bias_std

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
    pattern_src = (
        "live 500hPa analysis"
        if any(k in data_source.lower() for k in ("openmeteo", "gfs", "live"))
        else "recent reanalysis"
    )

    if distance is not None and distance > 0:
        if distance < 0.5:
            distance_note = f"Centroid distance {distance:.2f} — near-textbook match for this cluster."
        elif distance < 1.0:
            distance_note = f"Centroid distance {distance:.2f} — solid match."
        elif distance < 1.5:
            distance_note = f"Centroid distance {distance:.2f} — near the edge of the cluster; use bias estimates with some caution."
        else:
            distance_note = f"Centroid distance {distance:.2f} — today sits at the outer fringe of this cluster."
    else:
        distance_note = ""

    if confidence == "high":
        regime_desc = "The upper-level setup is well-established and closely mirrors historical examples of this regime."
    elif confidence == "medium":
        regime_desc = "A reasonable synoptic match — bias correction applies with moderate confidence."
    else:
        regime_desc = "Unusual synoptic setup; historical parallels are limited and bias estimates carry more uncertainty."

    if n_obs >= 10 and abs_bias >= 0.5:
        hist_note = (
            f"In {n_obs} similar {season_name} days under this regime, {station} has "
            f"verified {abs_bias:.1f}°F {direction} than model guidance — "
            "this tendency is baked into the adjusted forecast."
        )
    elif n_obs >= 10:
        hist_note = (
            f"Across {n_obs} similar {season_name} days, the model has been well-calibrated "
            f"at {station} with negligible systematic bias."
        )
    else:
        hist_note = (
            f"Only {n_obs} similar days in the bias table — "
            "bias estimate is tentative; treat uncertainty bounds as approximate."
        )

    synoptic_pattern = (
        f"{season_name} Cluster {cluster_id} — identified from {pattern_src}. "
        f"{distance_note} "
        f"Confidence: {conf_plain}. "
        f"{regime_desc} "
        f"{hist_note}"
    )

    # ── Model runs & day outlook ──────────────────────────────────────────
    _is_p4 = model_source_used in ("GFS", "ECMWF", "BLEND")

    if _is_p4:
        # ── Phase 4 path ─────────────────────────────────────────────────
        if model_source_used == "BLEND" and p4_gfs_raw is not None and p4_ecmwf_raw is not None:
            model_snapshot = (
                f"Phase 4 [BLEND] {forecast_raw:.0f}°F"
                f"  (GFS {p4_gfs_raw:.0f}°F | ECMWF {p4_ecmwf_raw:.0f}°F)"
            )
            raw_spread = abs(p4_gfs_raw - p4_ecmwf_raw)
            if raw_spread < 1.5:
                convergence_note = (
                    f"GFS and ECMWF are well-aligned (raw spread {raw_spread:.1f}°F). "
                    "Both models were averaged to form the Phase 4 blend for this station."
                )
            else:
                higher = "GFS" if p4_gfs_raw > p4_ecmwf_raw else "ECMWF"
                convergence_note = (
                    f"GFS and ECMWF differ by {raw_spread:.1f}°F (raw). "
                    f"{higher} is running warmer; blend smooths the spread. "
                    "Phase 4 study found both models informative here — averaging reduces variance."
                )
        elif model_source_used == "BLEND":
            model_snapshot = f"Phase 4 [BLEND] {forecast_raw:.0f}°F"
            convergence_note = (
                "Phase 4 blend of GFS and ECMWF (raw inputs unavailable). "
                "Both models were averaged after per-station bias removal."
            )
        else:
            other = "ECMWF" if model_source_used == "GFS" else "GFS"
            if model_source_used == "GFS" and p4_gfs_raw is not None:
                model_snapshot = f"Phase 4 [GFS] {forecast_raw:.0f}°F  (raw {p4_gfs_raw:.0f}°F before correction)"
            elif model_source_used == "ECMWF" and p4_ecmwf_raw is not None:
                model_snapshot = f"Phase 4 [ECMWF] {forecast_raw:.0f}°F  (raw {p4_ecmwf_raw:.0f}°F before correction)"
            else:
                model_snapshot = f"Phase 4 [{model_source_used}] {forecast_raw:.0f}°F"
            convergence_note = (
                f"Station-optimized {model_source_used} selected (Phase 4 study: 38,174 settled contracts). "
                f"{other} was excluded at {station} due to higher bias or larger historical error. "
                "No NWS/NBM divergence checks — Phase 4 uses a standalone model pipeline."
            )

        # Phase 4 bias note
        if bias_std > 0:
            bias_note = (
                f"Phase 4 station-level bias correction pre-applied (bias_mean forced to 0). "
                f"Distributional uncertainty ±{bias_std:.1f}°F from historical {model_source_used} sigma at this cluster "
                f"(~68% of similar days land {lo1:.0f}–{hi1:.0f}°F)."
            )
        else:
            bias_note = (
                "Phase 4 station-level bias correction pre-applied (bias_mean forced to 0). "
                "No distributional width available from bias table."
            )

    else:
        # ── NWS AFM / Open-Meteo path (original logic) ───────────────────
        # Model snapshot line
        model_parts = [f"NWS AFM {forecast_raw:.0f}°F"]
        if nbm_forecast_raw is not None:
            model_parts.append(f"NBM {nbm_forecast_raw:.0f}°F")
        if mos_forecast_raw is not None:
            model_parts.append(f"GFS-MOS {mos_forecast_raw:.0f}°F")
        model_snapshot = "  |  ".join(model_parts)

        # Convergence / divergence narrative
        active_temps = [t for t in [forecast_raw, nbm_forecast_raw, mos_forecast_raw] if t is not None]
        spread = round(max(active_temps) - min(active_temps), 1) if len(active_temps) > 1 else 0.0

        afm_nbm_diff = abs(nbm_divergence_f)  if nbm_divergence_f  is not None else None
        afm_mos_diff = abs(model_divergence_f) if model_divergence_f is not None else None
        nbm_mos_diff = (
            abs(nbm_forecast_raw - mos_forecast_raw)
            if nbm_forecast_raw is not None and mos_forecast_raw is not None else None
        )

        if len(active_temps) == 1:
            convergence_note = (
                "NBM and GFS-MOS are not available — signal based on NWS AFM alone."
            )
        elif len(active_temps) == 2:
            if nbm_forecast_raw is None:
                if spread < 1.5:
                    convergence_note = f"NWS AFM and GFS-MOS are aligned (spread {spread:.1f}°F)."
                elif model_divergence_f > 0:
                    convergence_note = (
                        f"NWS is running {afm_mos_diff:.0f}°F warmer than GFS-MOS. "
                        "The human forecaster may be capturing warm advection or a clearing "
                        "that the model blend hasn't resolved yet."
                    )
                else:
                    convergence_note = (
                        f"NWS is running {afm_mos_diff:.0f}°F cooler than GFS-MOS. "
                        "The forecaster may be factoring in marine influence, "
                        "a cloud deck, or a cold pool the model blend doesn't resolve."
                    )
            else:
                if spread < 1.5:
                    convergence_note = f"NWS AFM and NBM are well-aligned (spread {spread:.1f}°F)."
                elif nbm_divergence_f > 0:
                    convergence_note = (
                        f"NWS AFM is {afm_nbm_diff:.0f}°F warmer than NBM. "
                        "The human forecaster is bullish relative to the automated blend."
                    )
                else:
                    convergence_note = (
                        f"NWS AFM is {afm_nbm_diff:.0f}°F cooler than NBM — "
                        "human forecast is on the cool side; NBM may be overestimating daytime heating."
                    )
        else:
            if spread < 1.5:
                convergence_note = (
                    f"All three model runs are well-aligned (spread {spread:.1f}°F) — "
                    "clean temperature signal, high confidence in the forecast."
                )
            elif afm_mos_diff is not None and afm_mos_diff < 1.0 and nbm_mos_diff is not None and nbm_mos_diff >= 2.0:
                nbm_dir = "warmer" if nbm_divergence_f < 0 else "cooler"
                convergence_note = (
                    f"NWS AFM and GFS-MOS agree; NBM is the outlier, running "
                    f"{afm_nbm_diff:.0f}°F {nbm_dir}. "
                    "The automated blend may be overweighting a model that's out of phase today."
                )
            elif afm_nbm_diff is not None and afm_nbm_diff < 1.0 and nbm_mos_diff is not None and nbm_mos_diff >= 2.0:
                mos_dir = "warmer" if model_divergence_f < 0 else "cooler"
                convergence_note = (
                    f"NWS AFM and NBM are aligned; GFS-MOS is the outlier at {mos_forecast_raw:.0f}°F "
                    f"({afm_mos_diff:.0f}°F {mos_dir}). "
                    "GFS-MOS may be lagging on the latest pattern evolution."
                )
            elif spread >= 3.0:
                convergence_note = (
                    f"Models are spread {spread:.0f}°F apart — genuine forecast uncertainty today. "
                    "NWS AFM is our primary input; use the bias spread as your uncertainty guide."
                )
            else:
                convergence_note = (
                    f"Models show modest spread ({spread:.1f}°F). "
                    "NWS AFM leads; NBM and GFS-MOS are cross-checks."
                )

        # Bias correction note (original logic, only for non-Phase4)
        if bias_src in ("nws_fixed", "nws_era5_sigma"):
            bias_note = (
                f"NWS AFM is already human-calibrated — no additional bias correction applied. "
                f"Uncertainty ±{bias_std:.1f}°F from station-level ERA5 historical error."
            )
        elif abs_bias < 0.3:
            bias_note = (
                f"Cluster/month bias is negligible ({bias_mean:+.1f}°F over {n_obs} days). "
                f"Adjusted forecast: {forecast_adjusted:.1f}°F ± {bias_std:.1f}°F."
            )
        else:
            bias_note = (
                f"Applying a {bias_mean:+.1f}°F cluster/month correction "
                f"({abs_bias:.1f}°F {direction} over {n_obs} similar days). "
                f"Adjusted forecast: {forecast_adjusted:.1f}°F ± {bias_std:.1f}°F "
                f"(~68% of similar days land {lo1:.0f}–{hi1:.0f}°F)."
            )

    # TAF integrated into day outlook
    cond_desc = _CONDITION_PLAIN.get(taf.condition, taf.condition)
    if weather_gate == "hard_skip":
        taf_note = (
            f"TAF ALERT: {taf.summary} — dangerous conditions during peak heating window. "
            "Temperature forecast is unreliable today."
        )
    elif weather_gate == "skip":
        taf_note = (
            f"TAF shows {cond_desc.lower()} during peak heating hours. "
            "Fog/marine layer burn-off timing is uncertain and could suppress or delay the high. "
            "Elevated uncertainty — not trading today."
        )
    elif taf.has_ts:
        taf_note = (
            "TAF mentions convective activity during or near peak heating — "
            "afternoon temperatures may be capped by evaporative cooling."
        )
    elif taf.precip:
        precip_str = ", ".join(taf.precip)
        taf_note = (
            f"TAF flags {precip_str} during the forecast period — "
            "precipitation may limit daytime heating."
        )
    elif taf.condition in ("clear", "scattered", "") or not taf.condition:
        taf_note = (
            f"TAF shows {cond_desc.lower()} through peak heating hours — "
            "favorable sky conditions for maximum daytime warming."
        )
    else:
        taf_note = f"TAF: {taf.summary}."

    if taf.has_amd:
        taf_note += " ⚠ TAF amended since last check — Tier 2 monitor is watching for further changes."

    forecast_and_bias = (
        f"Model runs: {model_snapshot}. "
        f"{convergence_note} "
        f"{bias_note} "
        f"{taf_note} "
        f"Targeting ~{forecast_adjusted:.0f}°F"
        + (f" (±{bias_std:.1f}°F spread)" if bias_std > 0 else "")
        + "."
    )

    # ── Market analysis ───────────────────────────────────────────────────
    edge_pct   = top_bucket.edge * 100
    our_pct    = top_bucket.model_prob * 100
    kalshi_pct = top_bucket.kalshi_prob * 100
    ask_cents  = round(top_bucket.yes_ask * 100)

    cond_desc = _CONDITION_PLAIN.get(taf.condition, taf.condition)
    if weather_gate == "hard_skip":
        penalty_note = f"Weather gate: HARD SKIP ({cond_desc}) — conditions too dangerous to trade."
    elif weather_gate == "skip":
        penalty_note = f"Weather gate: SKIP ({cond_desc}) — elevated uncertainty, not trading today."
    else:
        gate_suffix = " Forecast uncertainty gate fired (bias_std too high)." if bias_std_gate_fired else ""
        penalty_note = f"Weather gate: TRADE ({cond_desc}).{gate_suffix}"

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
        decision_rationale = (
            f"Watching — edge of {top_bucket.edge:+.3f} is real but falls below "
            "the minimum stake threshold. Kelly stake too small to place."
        )
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

    threshold_checks.append({
        "name":   "Weather gate",
        "passed": weather_gate == "trade",
        "detail": describe_gate(taf.condition),
    })
    threshold_checks.append({
        "name":   "Forecast uncertainty",
        "passed": not bias_std_gate_fired,
        "detail": (
            f"Bias spread {bias_std:.1f}°F — "
            f"{'too high, gate fired (>{BIAS_STD_GATE:.0f}°F)' if bias_std_gate_fired else 'acceptable'}"
        ),
    })
    threshold_checks.append({
        "name":   "Edge",
        "passed": True,
        "detail": f"Edge {top_bucket.edge:+.3f} (no floor — Kelly self-regulates)",
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
    nbm_src_str = f"NBM {nbm_forecast_raw:.0f}°F"     if nbm_forecast_raw is not None else "unavailable"
    data_sources = {
        "pattern":  f"{'Live Open-Meteo 500hPa' if data_source == 'openmeteo' else 'Reanalysis fallback'}",
        "forecast": f"{model_source_used} (primary) | NBM: {nbm_src_str} | GFS-MOS: {mos_src_str}",
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
    p4_forecast_f: float | None = None,
    p4_model: str = "PHASE4",
    p4_station_kelly_mult: float = 1.0,
    p4_gfs_raw: float | None = None,
    p4_ecmwf_raw: float | None = None,
) -> TradeSignal:
    """
    Generate a complete trade signal for one station and event date.

    p4_forecast_f:          when provided (Phase 4 bias-corrected GFS/ECMWF), skip the
                            NWS/MOS/NBM fetch entirely and use this value as forecast_raw.
    p4_model:               label for logs/cards, e.g. "GFS", "ECMWF", "BLEND".
    p4_station_kelly_mult:  Phase 5 per-station Kelly multiplier from _STATION_KELLY_MULT.
                            Applied after pattern-confidence scaling. Default 1.0 (neutral).
    """
    import pytz

    tz      = pytz.timezone(STATION_TIMEZONES[station])
    now_loc = datetime.now(timezone.utc).astimezone(tz)
    local_time_str = now_loc.strftime("%I:%M %p %Z")

    # ── 1. TAF + METAR ───────────────────────────────────────────────────
    from utils.peak_hours import get_peak_hour
    peak_hour_local = get_peak_hour(station, event_date)
    taf   = interpret_taf(station, event_date=event_date, peak_hour_local=peak_hour_local)
    metar = get_metar(settlement_station(station))  # KJFK→KNYC, KORD→KMDW

    # ── 2. Forecast (Phase 4 GFS/ECMWF preferred; NWS+NBM fallback) ─────
    if p4_forecast_f is not None:
        # Phase 4 supplies a bias-corrected station-dependent model forecast.
        # MOS/NBM divergence checks are not meaningful against this source.
        forecast_raw       = p4_forecast_f
        mos_forecast_raw   = None
        nbm_forecast_raw   = None
        model_divergence_f = None
        nbm_divergence_f   = None
        model_source_used  = p4_model
        logger.info(
            "%s Phase4 forecast [%s]: %.1f°F",
            station, p4_model, forecast_raw,
        )
    else:
        forecast_raw, mos_forecast_raw, model_source_used, nbm_forecast_raw = (
            fetch_live_forecast(station, event_date)
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

        nbm_divergence_f = (
            round(forecast_raw - nbm_forecast_raw, 1)
            if nbm_forecast_raw is not None else None
        )
        if nbm_divergence_f is not None:
            logger.info(
                "%s NWS=%.1f°F  NBM=%.1f°F  nbm_divergence=%+.1f°F",
                station, forecast_raw, nbm_forecast_raw, nbm_divergence_f,
            )

    # ── 3. Bias correction ───────────────────────────────────────────────
    effective_cluster = (
        -1 if pattern.get("data_source") == "reanalysis_fallback"
        else pattern["cluster_id"]
    )
    if p4_forecast_f is not None:
        # Phase 4 already removed station-level warm/cold bias; set bias_mean=0.
        # Request the matching Open-Meteo source for distribution width (bias_std).
        # lookup_bias() falls through to GFS_MOS / any-source if those rows don't
        # exist yet (i.e. before the bias table is rebuilt).
        _p4_bias_src = (
            "ECMWF_OPENMETEO" if p4_model == "ECMWF"
            else "GFS_OPENMETEO"   # GFS and BLEND both anchor on GFS sigma
        )
        _width_info = lookup_bias(
            bias_df, station, event_date,
            effective_cluster, pattern["season"], forecast_raw,
            model_source=_p4_bias_src,
        )
        bias_info = {
            "bias_mean": 0.0,
            "bias_std":  _width_info["bias_std"],
            "n_obs":     _width_info["n_obs"],
            "model_bin": float(forecast_raw),
            "source":    f"phase4_{p4_model.lower()}_{_width_info['source']}",
        }
        logger.info(
            "%s Phase4 [%s] — sigma=%.1f°F from %s (%s), bias_mean=0",
            station, p4_model, _width_info["bias_std"], _p4_bias_src, _width_info["source"],
        )
    elif model_source_used == "IEM_AFM":
        era5_info = lookup_bias(
            bias_df, station, event_date,
            effective_cluster, pattern["season"], forecast_raw,
            model_source="ERA5",
        )
        bias_info = {
            "bias_mean": 0.0,
            "bias_std":  era5_info["bias_std"],
            "n_obs":     era5_info["n_obs"],
            "model_bin": float(forecast_raw),
            "source":    "nws_era5_sigma",
        }
        logger.info(
            "%s NWS source — station sigma=%.1f°F (ERA5 historical), no bias correction",
            station, era5_info["bias_std"],
        )
    else:
        bias_info = lookup_bias(
            bias_df, station, event_date,
            effective_cluster, pattern["season"], forecast_raw,
            model_source=model_source_used,
        )
    bias_mean         = bias_info["bias_mean"]
    bias_std          = bias_info["bias_std"]
    forecast_adjusted = forecast_raw + bias_mean

    # ── 4. Weather gate ──────────────────────────────────────────────────
    weather_gate = compute_weather_gate(taf.condition)

    if weather_gate == "hard_skip":
        return _hard_skip_signal(
            station, event_date, local_time_str, taf, metar,
            pattern, forecast_raw, bias_info, forecast_adjusted,
        )

    _fcst_kwargs = dict(
        forecast_raw=forecast_raw, forecast_adjusted=forecast_adjusted,
        bias_mean=bias_mean, bias_std=bias_std, n_obs=bias_info["n_obs"],
        mos_forecast_raw=mos_forecast_raw, model_divergence_f=model_divergence_f,
        nbm_forecast_raw=nbm_forecast_raw, nbm_divergence_f=nbm_divergence_f,
    )

    if weather_gate == "skip":
        return _skip_signal(
            station, event_date, local_time_str, taf, metar,
            pattern, f"Weather gate SKIP: {taf.condition} — {taf.summary}",
            **_fcst_kwargs,
        )

    # ── 4b. Forecast uncertainty gate ───────────────────────────────────
    bias_std_gate_fired = bias_std > BIAS_STD_GATE
    if bias_std_gate_fired:
        logger.info(
            "%s bias_std %.1f°F > gate %.1f°F — skipping (forecast too uncertain)",
            station, bias_std, BIAS_STD_GATE,
        )
        return _skip_signal(
            station, event_date, local_time_str, taf, metar,
            pattern, f"Forecast uncertainty too high: bias_std={bias_std:.1f}°F > {BIAS_STD_GATE}°F",
            **_fcst_kwargs,
        )

    # ── 5. Kalshi snapshots (fetch first — needed for live bucket bounds) ───
    snapshots: dict[int, MarketSnapshot] = kalshi.get_all_snapshots(station, event_date)

    if not snapshots:
        logger.warning("%s — no Kalshi snapshots available", station)
        return _skip_signal(station, event_date, local_time_str, taf, metar,
                            pattern, "No Kalshi market data", **_fcst_kwargs)

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

    # Normalize market implied probabilities (ask prices) to sum to 1.0.
    # Kalshi ask prices sum to >100% due to bid-ask spread / market maker edge.
    # Normalizing gives a "shape vs. shape" comparison so spread doesn't
    # systematically deflate our edge estimates.
    raw_market_total = sum(
        s.implied_prob for s in snapshots.values() if s is not None
    )
    market_total = raw_market_total if raw_market_total > 0 else 1.0
    market_vig   = market_total - 1.0
    logger.info(
        "%s market vig: %.1f%% (ask sum=%.3f across %d buckets)",
        station, market_vig * 100, market_total, len(snapshots),
    )

    bucket_analyses: list[BucketAnalysis] = []

    for lower in live_buckets:
        model_p  = model_probs.get(lower, 0.0)
        snap     = snapshots.get(lower)
        if snap is None:
            continue

        kalshi_p = snap.implied_prob / market_total   # normalized fair probability
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
                            pattern, "No bucket overlap between model and Kalshi",
                            **_fcst_kwargs)

    # Log low-probability buckets for informational purposes
    peak_prob = max(b.model_prob for b in bucket_analyses)
    min_prob  = peak_prob * MIN_PROB_RATIO
    filtered_labels = [b.bucket_label for b in bucket_analyses if b.model_prob < min_prob]
    if filtered_labels:
        logger.info(
            "%s — low-prob buckets (modal=%.1f%%): %s",
            station, peak_prob * 100, filtered_labels,
        )

    # Rank buckets by Kalshi yes_ask; top bucket used for TradeSignal metadata.
    # Phase 4 price-zone logic (in scheduler) determines the actual entry bucket.
    ranked = sorted(bucket_analyses, key=lambda b: b.yes_ask, reverse=True)
    top3   = ranked[:3]

    logger.info(
        "%s market top3: %s | forecast=%.1f°F",
        station,
        [(b.bucket_lower, round(b.yes_ask, 3)) for b in top3],
        forecast_adjusted,
    )

    top = top3[0]  # Highest-ask bucket — used for signal metadata, not entry selection

    # Entry decision — Phase 4 price-zone logic handles bucket selection and validation.
    # Kelly self-regulates stake size based on edge magnitude; no edge floor required.
    decision = "TRADE"

    # ── 8. Kelly sizing with confidence scaling ──────────────────────────
    kelly_frac, kelly_usd, kelly_contracts = kelly_stake(
        top.model_prob, top.yes_ask, bankroll
    )

    # Scale Kelly by pattern confidence — unusual regimes get reduced sizing
    confidence_scale = CONFIDENCE_KELLY_SCALE.get(pattern.get("confidence", "low"), 0.5)
    if confidence_scale != 1.0:
        kelly_frac = round(kelly_frac * confidence_scale, 6)

    # Scale Kelly by Phase 5 per-station multiplier (KORD 2.0×, KMIA 0.75×, others 1.0×)
    if p4_station_kelly_mult != 1.0:
        kelly_frac = round(kelly_frac * p4_station_kelly_mult, 6)
        logger.info(
            "%s Phase5 station Kelly mult %.2f× applied — new kelly_frac=%.4f",
            station, p4_station_kelly_mult, kelly_frac,
        )

    if confidence_scale != 1.0 or p4_station_kelly_mult != 1.0:
        kelly_usd       = round(bankroll * kelly_frac, 2)
        kelly_contracts = int(math.floor(kelly_usd / top.yes_ask)) if top.yes_ask > 0 else 0
        kelly_usd       = round(kelly_contracts * top.yes_ask, 2)

    # Enforce minimum stake — floor at MIN_KELLY_STAKE rather than downgrading to WATCH.
    # With market-led selection the top bucket often has yes_ask > model_prob (negative
    # Kelly edge), but we enter anyway with minimum size since the market confirms the range.
    if decision == "TRADE" and kelly_usd < MIN_KELLY_STAKE:
        kelly_contracts = max(1, int(math.floor(MIN_KELLY_STAKE / top.yes_ask))) if top.yes_ask > 0 else 1
        kelly_usd       = round(kelly_contracts * top.yes_ask, 2)
        kelly_frac      = round(kelly_usd / bankroll, 6) if bankroll > 0 else 0.0
        logger.info(
            "%s — Kelly floored to min stake: %d contract(s) @ $%.2f = $%.2f",
            station, kelly_contracts, top.yes_ask, kelly_usd,
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
        weather_gate=weather_gate,
        bias_std_gate_fired=bias_std_gate_fired,
        top_bucket=top,
        bucket_analyses=bucket_analyses,
        decision=decision,
        kelly_stake_usd=kelly_usd,
        kelly_contracts=kelly_contracts,
        confidence_scale=confidence_scale,
        mos_forecast_raw=mos_forecast_raw,
        model_divergence_f=model_divergence_f,
        model_source_used=model_source_used,
        nbm_forecast_raw=nbm_forecast_raw,
        nbm_divergence_f=nbm_divergence_f,
        p4_gfs_raw=p4_gfs_raw,
        p4_ecmwf_raw=p4_ecmwf_raw,
    )

    logger.info(
        "%s | %s | AFM=%.1f°F MOS=%s | adj=%.1f°F | top=%s | edge=%+.3f | "
        "min_edge=%.2f | kelly=$%.2f | decision=%s",
        station, event_date, forecast_raw,
        f"{mos_forecast_raw:.1f}°F" if mos_forecast_raw else "N/A",
        forecast_adjusted, top.bucket_label, top.edge,
        MIN_EDGE, kelly_usd, decision,
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
        nbm_forecast_raw=nbm_forecast_raw,
        nbm_divergence_f=nbm_divergence_f,
        model_source=model_source_used,
        top_bucket=top.bucket_lower,
        top_edge=top.edge,
        top_model_prob=top.model_prob,
        top_kalshi_prob=top.kalshi_prob,
        top_yes_ask=top.yes_ask,
        kelly_fraction=kelly_frac,
        kelly_stake_usd=kelly_usd,
        kelly_contracts=kelly_contracts,
        weather_gate=weather_gate,
        taf=taf,
        metar=metar,
        buckets=bucket_analyses,
        live_lower_tail=live_lower_tail,
        live_upper_tail=live_upper_tail,
        reasoning=reasoning,
    )


def _skip_signal(station, event_date, local_time, taf, metar,
                 pattern, reason,
                 forecast_raw=0.0, forecast_adjusted=0.0,
                 bias_mean=0.0, bias_std=0.0, n_obs=0,
                 mos_forecast_raw=None, model_divergence_f=None,
                 nbm_forecast_raw=None, nbm_divergence_f=None) -> TradeSignal:
    return TradeSignal(
        station=station, event_date=event_date, local_time=local_time,
        decision="SKIP",
        forecast_raw=forecast_raw, bias_mean=bias_mean,
        bias_std=bias_std, forecast_adjusted=forecast_adjusted,
        cluster_id=pattern.get("cluster_id", -1),
        season=pattern.get("season", "?"),
        n_obs=n_obs, pattern_confidence=pattern.get("confidence", "low"),
        top_bucket=0, top_edge=0.0, top_model_prob=0.0,
        top_kalshi_prob=0.0, top_yes_ask=0.0,
        kelly_fraction=0.0, kelly_stake_usd=0.0, kelly_contracts=0,
        weather_gate="skip", taf=taf, metar=metar,
        mos_forecast_raw=mos_forecast_raw, model_divergence_f=model_divergence_f,
        nbm_forecast_raw=nbm_forecast_raw, nbm_divergence_f=nbm_divergence_f,
        buckets=[], reasoning=None, skip_reason=reason,
    )


def _hard_skip_signal(station, event_date, local_time, taf, metar,
                      pattern, forecast_raw, bias_info, forecast_adjusted,
                      reason: str | None = None) -> TradeSignal:
    note = reason or f"HARD SKIP: {taf.summary} — weather gate forces skip."
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
        weather_gate="hard_skip", taf=taf, metar=metar,
        buckets=[], reasoning=None, skip_reason=note,
    )


# ---------------------------------------------------------------------------
# Forecast data availability probe
# ---------------------------------------------------------------------------

def check_forecast_availability(event_date: date, probe_station: str = STATIONS[0]) -> dict:
    """
    Quick probe to confirm live forecast data is available for the target date.
    Used by the scheduler before running a full Tier 3 pass.
    Probes NBM via Herbie — NOAA-direct, no throttling.

    Returns:
        available : bool   — True if NBM or NWS data is reachable
        source    : str    — "live" | "fallback" | "none"
        details   : str    — human-readable status for logging
    """
    try:
        nbm = fetch_nbm_forecast(probe_station, event_date)
        if nbm is not None:
            return {
                "available": True,
                "source":    "live",
                "details":   f"NBM has fresh data for {event_date} at {probe_station} ({nbm:.1f}°F)",
            }
        # NBM unavailable — NWS AFM is primary and doesn't need a probe
        return {
            "available": True,
            "source":    "fallback",
            "details":   f"NBM unavailable for {event_date} — NWS AFM will handle per station",
        }
    except Exception as exc:
        logger.warning("Forecast probe exception: %s — proceeding anyway", exc)
        return {
            "available": True,
            "source":    "fallback",
            "details":   f"Forecast probe failed ({exc}) — proceeding with NWS fallbacks",
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
    skip_count  = sum(1 for s in signals.values() if s.decision in ("SKIP", "HARD_SKIP"))

    logger.info(
        "Signal pass complete: TRADE=%d SKIP=%d",
        trade_count, skip_count,
    )
    return signals
