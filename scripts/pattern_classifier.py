"""
Live 500mb synoptic pattern classifier.

Loads the trained seasonal KMeans models (with scalers) from
cluster_centroids.pkl and classifies today's 500mb geopotential
height anomaly field.

Live data source (priority order):
  1. Open-Meteo pressure-level forecast API  — real-time, no auth
  2. Most recent row of z500_anomaly.parquet  — fallback (1-2 day lag)

Open-Meteo replaced NOMADS GFS OPeNDAP after NOAA retired the /dods/
endpoint (SCN 25-81). Open-Meteo returns 500hPa geopotential height
on a global grid; we sample the same 2.5° NCEP grid points used during
training and compute anomalies the same way.

Output dict matches the schema used in pattern_labels.parquet:
  {date, season, cluster_id, distance_to_centroid, confidence}
"""

from __future__ import annotations

import pickle
import numpy as np
import pandas as pd
from datetime import date, datetime, timezone
from pathlib import Path

from utils.logging_config import setup_logging
from config import (
    CLUSTER_PKL,
    Z500_PARQUET,
    LAT_BOUNDS,
    LON_BOUNDS,
    SEASONS,
)

logger = setup_logging("pattern_classifier")

# Confidence distance thresholds (tuned from training centroid spread)
_CONFIDENCE_HIGH   = 3.0
_CONFIDENCE_MEDIUM = 5.5


# ---------------------------------------------------------------------------
# Season assignment
# ---------------------------------------------------------------------------

def assign_season(dt: date) -> str:
    m = dt.month
    if m in (12, 1, 2):
        return "DJF"
    elif m in (3, 4, 5):
        return "MAM"
    elif m in (6, 7, 8):
        return "JJA"
    else:
        return "SON"


# ---------------------------------------------------------------------------
# Live 500mb fetch — GFS via Herbie (NOAA-direct, no throttling)
# ---------------------------------------------------------------------------

def _fetch_herbie_z500(target_date: date) -> pd.Series | None:
    """Fetch GFS 500mb height via Herbie. Replaces Open-Meteo pressure-level API."""
    from utils.herbie_fetcher import fetch_gfs_z500
    return fetch_gfs_z500(target_date)


def _fallback_z500() -> tuple[pd.Series, date]:
    """
    Fall back to the most recent row of the historical z500_anomaly.parquet.
    Returns (Series, date_of_row).
    Note: this is a reanalysis anomaly field, not a raw height field.
    The classifier was trained on anomalies, so we return it as-is.
    """
    logger.info("Using most recent reanalysis z500 row as live proxy")
    z500_df = pd.read_parquet(Z500_PARQUET)
    # Drop the date column if present; return last data row
    if "date" in z500_df.columns:
        last_date = pd.to_datetime(z500_df["date"].iloc[-1]).date()
        feature_row = z500_df.drop(columns=["date"]).iloc[-1]
    else:
        last_date = date.today()
        feature_row = z500_df.iloc[-1]
    logger.info("Fallback z500 row date: %s", last_date)
    return feature_row, last_date


# ---------------------------------------------------------------------------
# Anomaly computation for live Open-Meteo data
# ---------------------------------------------------------------------------

def _compute_anomaly(raw_series: pd.Series, season: str) -> pd.Series | None:
    """
    Compute anomaly for a raw height field by subtracting the climatological
    mean from the training z500 data for the matching season.

    This aligns live Open-Meteo data with the anomaly-based training features.
    """
    try:
        z500_df = pd.read_parquet(Z500_PARQUET)

        # Load pattern labels to get season membership
        from config import PATTERNS_PARQUET
        pat_df = pd.read_parquet(PATTERNS_PARQUET)[["date", "season"]]
        if "date" not in z500_df.columns:
            z500_df["date"] = pd.to_datetime(z500_df.index)

        # Filter to matching season rows
        z500_df = z500_df.merge(pat_df, on="date", how="left")
        season_df = z500_df[z500_df["season"] == season]
        feature_cols = [c for c in z500_df.columns if c not in ("date", "season", "cluster_id")]
        climo_mean = season_df[feature_cols].mean()

        # Align raw_series to feature_cols, compute anomaly
        common_cols = [c for c in feature_cols if c in raw_series.index]
        if len(common_cols) < 100:
            logger.warning("Too few common columns (%d) for anomaly — skipping", len(common_cols))
            return None

        anomaly = raw_series[common_cols] - climo_mean[common_cols]
        return anomaly

    except Exception as exc:
        logger.warning("Anomaly computation failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Main classifier
# ---------------------------------------------------------------------------

def classify_pattern(target_date: date | None = None) -> dict:
    """
    Classify today's 500mb synoptic pattern.

    Returns
    -------
    dict with keys:
        date            : date
        season          : str
        cluster_id      : int
        distance        : float   — distance to nearest centroid in scaled space
        confidence      : str     — "high" / "medium" / "low"
        data_source     : str     — "openmeteo" or "reanalysis_fallback"
    """
    if target_date is None:
        target_date = date.today()

    season = assign_season(target_date)
    logger.info("Classifying pattern for %s (season=%s)", target_date, season)

    # ── Load cluster model ────────────────────────────────────────────────
    with open(CLUSTER_PKL, "rb") as f:
        cluster_models = pickle.load(f)

    if season not in cluster_models:
        raise ValueError(f"No cluster model for season '{season}' in {CLUSTER_PKL}")

    scaler       = cluster_models[season]["scaler"]
    kmeans       = cluster_models[season]["model"]
    feature_cols = cluster_models[season]["feature_cols"]

    # ── Fetch live z500 ───────────────────────────────────────────────────
    data_source = "herbie_gfs"
    raw = _fetch_herbie_z500(target_date)

    if raw is not None:
        # GFS gives raw heights — compute anomaly to match training
        feature_row = _compute_anomaly(raw, season)
        if feature_row is None:
            raw = None  # anomaly failed, fall to reanalysis

    if raw is None:
        feature_row, _ = _fallback_z500()
        data_source = "reanalysis_fallback"

    # ── Align features ────────────────────────────────────────────────────
    available = [c for c in feature_cols if c in feature_row.index]
    if len(available) < len(feature_cols) * 0.90:
        raise RuntimeError(
            f"Only {len(available)}/{len(feature_cols)} features available — "
            "data mismatch between live and training grid"
        )

    X = feature_row[feature_cols].values.reshape(1, -1).astype(np.float64)
    X_scaled = scaler.transform(X).astype(np.float64)

    # ── Classify ──────────────────────────────────────────────────────────
    # Ensure model centroids are float64 (old pkl files may be float32)
    if kmeans.cluster_centers_.dtype != np.float64:
        kmeans.cluster_centers_ = kmeans.cluster_centers_.astype(np.float64)
    cluster_id = int(kmeans.predict(X_scaled)[0])
    centroid   = kmeans.cluster_centers_[cluster_id]
    distance   = float(np.linalg.norm(X_scaled - centroid))

    if distance < _CONFIDENCE_HIGH:
        confidence = "high"
    elif distance < _CONFIDENCE_MEDIUM:
        confidence = "medium"
    else:
        confidence = "low"

    result = {
        "date":        target_date,
        "season":      season,
        "cluster_id":  cluster_id,
        "distance":    round(distance, 3),
        "confidence":  confidence,
        "data_source": data_source,
    }

    logger.info(
        "Pattern: season=%s cluster=%d dist=%.2f confidence=%s source=%s",
        season, cluster_id, distance, confidence, data_source,
    )
    return result
