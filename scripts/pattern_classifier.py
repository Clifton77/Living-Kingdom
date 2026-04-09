"""
Live 500mb synoptic pattern classifier.

Loads the trained seasonal KMeans models (with scalers) from
cluster_centroids.pkl and classifies today's 500mb geopotential
height anomaly field.

Live data source (priority order):
  1. NOMADS GFS 0.25° analysis (00Z) via OPeNDAP  — real-time
  2. Most recent row of z500_anomaly.parquet       — fallback (1-2 day lag)

Output dict matches the schema used in pattern_labels.parquet:
  {date, season, cluster_id, distance_to_centroid, confidence}
"""

from __future__ import annotations

import pickle
import numpy as np
import pandas as pd
import requests
from datetime import date, datetime, timezone
from pathlib import Path

from utils.logging_config import setup_logging
from config import (
    CLUSTER_PKL,
    Z500_PARQUET,
    NOMADS_GFS_URL,
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
# Live 500mb fetch — NOMADS GFS OPeNDAP
# ---------------------------------------------------------------------------

def _fetch_nomads_z500(target_date: date) -> pd.Series | None:
    """
    Fetch today's 500mb geopotential height field from NOMADS GFS.
    Uses the 00Z analysis (forecast hour 0 = analysis).
    Returns a pandas Series with column names matching z500_anomaly.parquet.
    """
    try:
        import netCDF4 as nc  # noqa: F401 — verify available
        date_str = target_date.strftime("%Y%m%d")
        url = NOMADS_GFS_URL.format(date=date_str)
        logger.info("Fetching live z500 from NOMADS: %s", url)

        ds = nc.Dataset(url)

        lats = ds.variables["lat"][:]
        lons = ds.variables["lon"][:]
        hgt  = ds.variables["hgtprs"]  # shape: (time, lev, lat, lon)

        # Pressure levels
        levs = ds.variables["lev"][:]
        p500_idx = int(np.argmin(np.abs(np.array(levs) - 500.0)))

        # Hour 0 (analysis)
        time_idx = 0

        # Subset domain
        lat_mask = (lats >= LAT_BOUNDS[0]) & (lats <= LAT_BOUNDS[1])
        lon_mask = (lons >= LON_BOUNDS[0]) & (lons <= LON_BOUNDS[1])

        lat_sub = lats[lat_mask]
        lon_sub = lons[lon_mask]
        hgt_sub = np.array(hgt[time_idx, p500_idx, :, :])[np.ix_(lat_mask, lon_mask)]

        ds.close()

        # Build Series with column names matching training data
        cols, vals = [], []
        for i, la in enumerate(lat_sub):
            for j, lo in enumerate(lon_sub):
                cols.append(f"lat_{la:.2f}_lon_{lo:.2f}")
                vals.append(float(hgt_sub[i, j]))

        return pd.Series(vals, index=cols)

    except Exception as exc:
        logger.warning("NOMADS z500 fetch failed: %s", exc)
        return None


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
# Anomaly computation for live NOMADS data
# ---------------------------------------------------------------------------

def _compute_anomaly(raw_series: pd.Series, season: str) -> pd.Series | None:
    """
    Compute anomaly for a raw height field by subtracting the climatological
    mean from the training z500 data for the matching season.

    This aligns live NOMADS data with the anomaly-based training features.
    """
    try:
        z500_df = pd.read_parquet(Z500_PARQUET)

        # Load pattern labels to get season membership
        from config import PATTERNS_PARQUET
        pat_df = pd.read_parquet(PATTERNS_PARQUET)[["date", "season"]]
        z500_df["date"] = pd.read_parquet(Z500_PARQUET).index if "date" not in z500_df.columns else z500_df["date"]

        # Filter to matching season rows
        if "date" in z500_df.columns:
            z500_df = z500_df.merge(pat_df, on="date", how="left")
            season_df = z500_df[z500_df["season"] == season]
            feature_cols = [c for c in z500_df.columns if c not in ("date", "season", "cluster_id")]
            climo_mean = season_df[feature_cols].mean()
        else:
            climo_mean = z500_df.mean()
            feature_cols = list(z500_df.columns)

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
        data_source     : str     — "nomads" or "reanalysis_fallback"
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
    data_source = "nomads"
    raw = _fetch_nomads_z500(target_date)

    if raw is not None:
        # NOMADS gives raw heights — compute anomaly to match training
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

    X = feature_row[feature_cols].values.reshape(1, -1)
    X_scaled = scaler.transform(X)

    # ── Classify ──────────────────────────────────────────────────────────
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
