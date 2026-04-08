"""
Script 2: Build 500mb geopotential height anomaly database.

Source: NCEP/NCAR Reanalysis via PSL OPeNDAP (or HTTP fallback)
Domain: 20-55N, 130-60W (230-300E in NCEP 0-360 system)
Output: data/z500_anomaly.parquet
  {date, lat_{x}_lon_{y}...}  (~435 gridpoint columns)
"""
import os
import sys
import time
import pickle
import logging
from datetime import date

import numpy as np
import pandas as pd
from tqdm import tqdm

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    START_DATE, END_DATE, Z500_PARQUET, OBS_PARQUET,
    RAW_DIR, LOGS_DIR, NCEP_OPENDAP_TEMPLATE, NCEP_HTTP_TEMPLATE,
    LAT_BOUNDS, LON_BOUNDS, DATA_DIR,
)
from utils.logging_config import setup_logging

logger = setup_logging("build_500mb_database")


def _load_year_netcdf4(year: int) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Load 500mb height data for a single year via netCDF4.
    Returns (dates_array, lats, lons, data) where data shape is (days, nlat, nlon).
    """
    import netCDF4 as nc

    url = NCEP_OPENDAP_TEMPLATE.format(year=year)
    logger.info("Opening OPeNDAP: %s", url)

    try:
        ds = nc.Dataset(url)
    except Exception:
        logger.warning("OPeNDAP failed, trying HTTP download for year %d", year)
        http_url = NCEP_HTTP_TEMPLATE.format(year=year)
        local_path = os.path.join(RAW_DIR, f"hgt.{year}.nc")
        if not os.path.exists(local_path):
            import urllib.request
            logger.info("Downloading %s → %s", http_url, local_path)
            urllib.request.urlretrieve(http_url, local_path)
        ds = nc.Dataset(local_path)

    # Find 500 hPa level index
    levels = ds.variables["level"][:]
    level_idx = int(np.where(levels == 500)[0][0])

    # Lat/lon arrays
    lats = ds.variables["lat"][:]
    lons = ds.variables["lon"][:]

    # Subset to domain
    lat_mask = (lats >= LAT_BOUNDS[0]) & (lats <= LAT_BOUNDS[1])
    lon_mask = (lons >= LON_BOUNDS[0]) & (lons <= LON_BOUNDS[1])
    lat_idx = np.where(lat_mask)[0]
    lon_idx = np.where(lon_mask)[0]

    # Read data — slice before loading into memory
    data = ds.variables["hgt"][:, level_idx, lat_idx[0]:lat_idx[-1]+1, lon_idx[0]:lon_idx[-1]+1]
    data = np.array(data, dtype=np.float32)

    # Decode times
    time_var = ds.variables["time"]
    times = nc.num2date(time_var[:], time_var.units, time_var.calendar
                        if hasattr(time_var, "calendar") else "standard")
    dates = np.array([date(t.year, t.month, t.day) for t in times])

    subset_lats = lats[lat_idx]
    subset_lons = lons[lon_idx]

    ds.close()
    return dates, subset_lats, subset_lons, data


def build_column_names(lats: np.ndarray, lons: np.ndarray) -> list[str]:
    cols = []
    for lat in lats:
        for lon in lons:
            cols.append(f"lat_{lat:.1f}_lon_{lon:.1f}")
    return cols


def compute_climatology(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute 15-year daily climatological mean by day-of-year.
    Handles leap day (DOY 366) by averaging DOY 365 and DOY 1 of next year.
    Returns DataFrame indexed by DOY (1-366).
    """
    daily_df = daily_df.copy()
    daily_df["doy"] = pd.to_datetime(daily_df["date"]).dt.dayofyear
    climo = daily_df.groupby("doy").mean(numeric_only=True)

    # Handle DOY 366 if missing (non-leap years)
    if 366 not in climo.index:
        d365 = climo.loc[365] if 365 in climo.index else climo.iloc[-1]
        d1   = climo.loc[1]   if 1   in climo.index else climo.iloc[0]
        climo.loc[366] = (d365 + d1) / 2.0
        climo = climo.sort_index()

    return climo


def compute_anomalies(daily_df: pd.DataFrame, climo_df: pd.DataFrame) -> pd.DataFrame:
    """
    Subtract climatological mean for each date's DOY.
    """
    daily_df = daily_df.copy()
    daily_df["doy"] = pd.to_datetime(daily_df["date"]).dt.dayofyear
    feat_cols = [c for c in daily_df.columns if c.startswith("lat_")]

    for col in feat_cols:
        daily_df[col] = daily_df[col] - daily_df["doy"].map(climo_df[col])

    return daily_df.drop(columns=["doy"])


def compute_station_weights(anomaly_df: pd.DataFrame, obs_df: pd.DataFrame) -> None:
    """
    Compute Pearson correlation between each gridpoint anomaly and station tmax.
    Saves weights to data/raw/z500_station_weights.parquet for optional use.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    feat_cols = [c for c in anomaly_df.columns if c.startswith("lat_")]

    anomaly_df = anomaly_df.copy()
    anomaly_df["date"] = pd.to_datetime(anomaly_df["date"])
    obs_df = obs_df.copy()
    obs_df["date"] = pd.to_datetime(obs_df["date"])

    weights = {}
    for station in obs_df["station"].unique():
        station_obs = obs_df[obs_df["station"] == station][["date", "tmax_observed_f"]]
        merged = pd.merge(anomaly_df, station_obs, on="date", how="inner")
        if len(merged) < 100:
            logger.warning("Insufficient overlap for %s weight computation", station)
            continue
        corrs = merged[feat_cols].corrwith(merged["tmax_observed_f"])
        weights[station] = corrs.to_dict()

    weights_df = pd.DataFrame(weights).T
    weights_path = os.path.join(RAW_DIR, "z500_station_weights.parquet")
    weights_df.to_parquet(weights_path)
    logger.info("Station weights saved to %s", weights_path)


def build_500mb_database() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)

    start_year = int(START_DATE[:4])
    end_year   = int(END_DATE[:4])

    all_frames = []
    col_names = None

    for year in tqdm(range(start_year, end_year + 1), desc="Loading 500mb years"):
        try:
            dates, lats, lons, data = _load_year_netcdf4(year)
        except Exception as e:
            logger.error("Failed to load year %d: %s", year, e)
            time.sleep(2)
            continue

        if col_names is None:
            col_names = build_column_names(lats, lons)
            logger.info("Grid: %d lats × %d lons = %d gridpoints",
                        len(lats), len(lons), len(col_names))

        # Flatten spatial dims: (days, nlat*nlon)
        n_days = data.shape[0]
        flat = data.reshape(n_days, -1)

        df = pd.DataFrame(flat, columns=col_names)
        df.insert(0, "date", dates)
        all_frames.append(df)

        time.sleep(2)  # be polite to PSL server

    if not all_frames:
        raise RuntimeError("No 500mb data loaded — check network access to PSL server")

    daily_df = pd.concat(all_frames, ignore_index=True)
    daily_df["date"] = pd.to_datetime(daily_df["date"])
    daily_df = daily_df.sort_values("date").reset_index(drop=True)
    logger.info("Loaded %d daily 500mb fields", len(daily_df))

    # Compute climatology and anomalies
    logger.info("Computing climatology...")
    climo = compute_climatology(daily_df)

    logger.info("Computing anomalies...")
    anomaly_df = compute_anomalies(daily_df, climo)

    # Save
    anomaly_df.to_parquet(Z500_PARQUET, index=False)
    logger.info("Saved z500_anomaly.parquet: shape %s", anomaly_df.shape)

    # Compute station correlation weights (optional, saved for Phase 2 use)
    if os.path.exists(OBS_PARQUET):
        logger.info("Computing station correlation weights...")
        obs_df = pd.read_parquet(OBS_PARQUET)
        compute_station_weights(anomaly_df, obs_df)
    else:
        logger.info("obs_daily.parquet not found — skipping weight computation")

    # Sanity check: mean anomaly should be near zero
    feat_cols = [c for c in anomaly_df.columns if c.startswith("lat_")]
    mean_anom = anomaly_df[feat_cols].mean().mean()
    logger.info("Mean anomaly across all gridpoints: %.4f (should be ~0)", mean_anom)
    if abs(mean_anom) > 5.0:
        logger.warning("Mean anomaly unusually large — check climatology computation")


if __name__ == "__main__":
    build_500mb_database()
