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
    STARTING_BANKROLL,
    MAX_STAKE_PCT,
    MIN_N_OBS,
    EDGE_THRESHOLD_BASE,
    CONFIDENCE_KELLY_SCALE,
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
    forecast_raw:       float          # ERA5/model raw forecast
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

    # Full distribution
    buckets:            list[BucketAnalysis] = field(default_factory=list)

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
) -> dict[int, float]:
    """
    Model the temperature outcome as a normal distribution:
      mean = forecast_adjusted
      std  = bias_std  (captures residual uncertainty after bias correction)

    Integrate over each Kalshi bucket to get P(bucket).
    Returns dict: {bucket_lower: probability}
    """
    mu    = forecast_adjusted
    sigma = max(bias_std, 1.0)   # floor at 1°F to avoid degenerate distribution

    dist = scipy_stats.norm(loc=mu, scale=sigma)

    buckets = all_bucket_lowers()
    probs   = {}

    for lower in buckets:
        lo, hi = _bucket_bounds(lower)
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
) -> dict:
    """
    Look up bias correction parameters from the bias table.

    Matches on (station, month, season, cluster_id) first,
    then narrows to the nearest model_bin.

    Returns dict: {bias_mean, bias_std, n_obs, model_bin}
    Falls back to station/month average if cluster cell is too sparse.
    """
    month = event_date.month

    # Compute model_bin: 2°F odd-start bins matching Kalshi structure
    # Bins: 69-70, 71-72, 73-74, 75-76; tails at 68, 77
    # For bias table grouping we use the bin lower bound
    raw_bin = int(math.floor((forecast_raw - 0.5) / 2) * 2 + 1)
    raw_bin = max(KALSHI_BUCKET_LOWER_TAIL, min(raw_bin, KALSHI_BUCKET_UPPER_TAIL))

    # Primary lookup: exact match
    mask = (
        (bias_df["station"]    == station) &
        (bias_df["month"]      == month)   &
        (bias_df["season"]     == season)  &
        (bias_df["cluster_id"] == cluster_id)
    )
    subset = bias_df[mask]

    if len(subset) > 0:
        # Find nearest model_bin
        subset = subset.copy()
        subset["bin_dist"] = (subset["model_bin"] - forecast_raw).abs()
        best = subset.loc[subset["bin_dist"].idxmin()]

        if best["n_obs"] >= MIN_N_OBS:
            return {
                "bias_mean": float(best["bias_mean"]),
                "bias_std":  float(best["bias_std"]),
                "n_obs":     int(best["n_obs"]),
                "model_bin": float(best["model_bin"]),
                "source":    "cluster_match",
            }

    # Fallback: station/month average (ignore cluster)
    fallback_mask = (
        (bias_df["station"] == station) &
        (bias_df["month"]   == month)
    )
    fallback = bias_df[fallback_mask]

    if len(fallback) > 0:
        return {
            "bias_mean": float(fallback["bias_mean"].mean()),
            "bias_std":  float(fallback["bias_std"].mean()),
            "n_obs":     int(fallback["n_obs"].sum()),
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
# Live forecast fetch (Open-Meteo current forecast)
# ---------------------------------------------------------------------------

def fetch_live_forecast(station: str, target_date: date) -> float | None:
    """
    Fetch today's maximum temperature forecast from Open-Meteo.
    Uses the standard forecast API (not the archive).
    """
    import requests
    lat, lon = STATION_COORDS[station]
    try:
        resp = requests.get(
            OPEN_METEO_FORECAST_URL,
            params={
                "latitude":          lat,
                "longitude":         lon,
                "daily":             "temperature_2m_max",
                "temperature_unit":  "fahrenheit",
                "forecast_days":     3,
                "timezone":          "UTC",
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        dates = data["daily"]["time"]
        temps = data["daily"]["temperature_2m_max"]
        date_str = target_date.isoformat()

        if date_str in dates:
            idx = dates.index(date_str)
            val = temps[idx]
            if val is not None:
                logger.info("%s live forecast: %.1f°F", station, val)
                return float(val)

    except Exception as exc:
        logger.warning("Live forecast fetch failed for %s: %s", station, exc)

    # Fallback: use most recent value from historical forecast parquet
    try:
        fcst_df = pd.read_parquet(FCST_PARQUET)
        row = fcst_df[fcst_df["station"] == station].sort_values("date").iloc[-1]
        val = float(row["forecast_tmax_f"])
        logger.warning("%s using historical forecast fallback: %.1f°F", station, val)
        return val
    except Exception:
        return None


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

    if bias_src == "cluster_match":
        obs_desc = f"Based on {n_obs} similar days in this exact pattern during the same month"
    elif bias_src == "station_month_fallback":
        obs_desc = f"Cluster data was sparse — using {n_obs} days across all patterns for this station and month"
    else:
        obs_desc = "No historical bias data found — using zero correction"

    forecast_and_bias = (
        f"The model (Open-Meteo / ERA5) is forecasting a high of {forecast_raw:.0f}°F. "
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
    data_sources = {
        "pattern":  f"{'Live GFS 00Z via NOMADS' if 'gfs' in data_source.lower() else 'Reanalysis fallback'}",
        "forecast": "Open-Meteo (current forecast API)",
        "bias":     f"{bias_src} — {n_obs} obs",
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
    taf   = interpret_taf(station)
    metar = get_metar(station)

    # ── 2. Live forecast ─────────────────────────────────────────────────
    forecast_raw = fetch_live_forecast(station, event_date)
    if forecast_raw is None:
        logger.error("%s — no forecast available, skipping", station)
        return _skip_signal(station, event_date, local_time_str, taf, metar,
                            pattern, "No forecast data available")

    # ── 3. Bias lookup ───────────────────────────────────────────────────
    bias_info = lookup_bias(
        bias_df, station, event_date,
        pattern["cluster_id"], pattern["season"], forecast_raw,
    )
    bias_mean        = bias_info["bias_mean"]
    bias_std         = bias_info["bias_std"]
    forecast_adjusted = forecast_raw + bias_mean

    # ── 4. Weather penalty threshold ─────────────────────────────────────
    threshold_result = compute_effective_threshold(taf.condition, bias_std)

    if threshold_result is None:
        return _hard_skip_signal(
            station, event_date, local_time_str, taf, metar,
            pattern, forecast_raw, bias_info, forecast_adjusted,
        )

    effective_threshold = threshold_result.threshold

    # ── 5. Probability distribution ──────────────────────────────────────
    model_probs = build_probability_distribution(forecast_adjusted, bias_std)

    # ── 6. Kalshi snapshots ───────────────────────────────────────────────
    snapshots: dict[int, MarketSnapshot] = kalshi.get_all_snapshots(station, event_date)

    if not snapshots:
        logger.warning("%s — no Kalshi snapshots available", station)
        return _skip_signal(station, event_date, local_time_str, taf, metar,
                            pattern, "No Kalshi market data")

    # ── 7. Edge per bucket ────────────────────────────────────────────────
    bucket_analyses: list[BucketAnalysis] = []

    for lower in all_bucket_lowers():
        model_p  = model_probs.get(lower, 0.0)
        snap     = snapshots.get(lower)
        if snap is None:
            continue

        kalshi_p = snap.implied_prob
        edge     = model_p - kalshi_p

        bucket_analyses.append(BucketAnalysis(
            bucket_lower=lower,
            bucket_label=bucket_label(lower),
            model_prob=round(model_p, 4),
            kalshi_prob=round(kalshi_p, 4),
            edge=round(edge, 4),
            yes_ask=snap.yes_ask,
            yes_bid=snap.yes_bid,
        ))

    if not bucket_analyses:
        return _skip_signal(station, event_date, local_time_str, taf, metar,
                            pattern, "No bucket overlap between model and Kalshi")

    # Best edge bucket (highest positive edge only)
    positive_buckets = [b for b in bucket_analyses if b.edge > 0]
    if not positive_buckets:
        top = max(bucket_analyses, key=lambda b: b.edge)
        decision = "SKIP"
    else:
        top = max(positive_buckets, key=lambda b: b.edge)

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
    )

    logger.info(
        "%s | %s | adj_fcst=%.1f°F | top_bucket=%s | edge=%+.3f | "
        "threshold=%.3f | kelly=$%.2f | decision=%s",
        station, event_date, forecast_adjusted,
        top.bucket_label, top.edge, effective_threshold,
        kelly_usd, decision,
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

def check_forecast_availability(event_date: date, probe_station: str = "KJFK") -> dict:
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
    lat, lon = STATION_COORDS[probe_station]
    try:
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
        resp.raise_for_status()
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
        return {
            "available": False,
            "source":    "fallback",
            "details":   f"Open-Meteo response did not include {event_date} — model run may be delayed",
        }

    except Exception as exc:
        return {
            "available": False,
            "source":    "none",
            "details":   f"Open-Meteo unreachable: {exc}",
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

    for station in STATIONS:
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
