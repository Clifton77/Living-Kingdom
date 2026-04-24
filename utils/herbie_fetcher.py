"""
NOAA model data fetcher via Herbie.
Replaces Open-Meteo for both forecast (NBM TMAX) and z500 (GFS 500mb height).
No API key, no throttling — pulls directly from NOAA NOMADS / AWS S3.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from datetime import date, timedelta, datetime
from typing import Optional

from utils.logging_config import setup_logging

logger = setup_logging("herbie_fetcher")


def fetch_nbm_tmax(lat: float, lon: float, event_date: date) -> Optional[float]:
    """
    Fetch NBM daily max temperature for a point (lat, lon) on event_date.

    Uses the 00Z NBM CONUS run from the prior day, fxx=24 → valid the next
    calendar day. Falls back to fxx=36 then fxx=12 if earlier runs unavailable.
    Returns °F or None on failure.
    """
    try:
        from herbie import Herbie

        run_dt = datetime(event_date.year, event_date.month, event_date.day, 0, 0) - timedelta(days=1)
        # NBM CONUS uses 0-360 longitude convention
        lon360 = lon % 360

        for fxx in (24, 36, 12):
            try:
                H = Herbie(
                    run_dt.strftime("%Y-%m-%d %H:%M"),
                    model="nbm",
                    product="co",
                    fxx=fxx,
                    verbose=False,
                )
                ds = H.xarray(":TMAX:2 m above ground:12-24 hour max fcst:", remove_grib=True)
                tmax_var = [v for v in ds.data_vars if "tmax" in v.lower()][0]
                # Grid uses (y,x) dims — find nearest point by distance
                lats2d = ds["latitude"].values
                lons2d = ds["longitude"].values
                dist   = (lats2d - lat) ** 2 + (lons2d - lon360) ** 2
                yi, xi = np.unravel_index(dist.argmin(), dist.shape)
                val = float(ds[tmax_var].values[yi, xi])
                # NBM TMAX is in Kelvin
                temp_f = (val - 273.15) * 9.0 / 5.0 + 32.0
                if 0.0 < temp_f < 135.0:
                    logger.info(
                        "NBM TMAX (fxx=%d): %.1f°F at (%.2f, %.2f)", fxx, temp_f, lat, lon
                    )
                    return round(temp_f, 1)
            except Exception:
                continue

        logger.warning("fetch_nbm_tmax: all fxx attempts failed for (%.2f, %.2f) %s", lat, lon, event_date)
        return None

    except Exception as exc:
        logger.warning("fetch_nbm_tmax failed: %s", exc)
        return None


def fetch_gfs_z500(target_date: date) -> Optional[pd.Series]:
    """
    Fetch 500mb geopotential height from GFS analysis (fxx=0) on the 0.25° grid,
    then sample at the same 2.5° NCEP grid points used during training.

    Returns pd.Series with column names matching z500_anomaly.parquet:
    lat_{lat:.1f}_lon_{lon:.1f} (lon in 0-360 convention).
    Falls back to None so caller can use z500_anomaly.parquet fallback.
    """
    try:
        from herbie import Herbie
        from config import LAT_BOUNDS, LON_BOUNDS

        # Try 00Z analysis; fall back to previous day 18Z + fxx=6 if too early
        for run_offset, fxx in ((0, 0), (-1, 6)):
            run_dt = datetime(target_date.year, target_date.month, target_date.day, 0, 0)
            run_dt = run_dt + timedelta(days=run_offset)
            try:
                H = Herbie(
                    run_dt.strftime("%Y-%m-%d %H:%M"),
                    model="gfs",
                    product="pgrb2.0p25",
                    fxx=fxx,
                    verbose=False,
                )
                ds = H.xarray(":HGT:500 mb:", remove_grib=True)
                break
            except Exception:
                ds = None
                continue

        if ds is None:
            logger.warning("fetch_gfs_z500: could not fetch GFS for %s", target_date)
            return None

        lats     = np.arange(LAT_BOUNDS[0],  LAT_BOUNDS[1]  + 0.01, 2.5)
        lons_360 = np.arange(LON_BOUNDS[0],  LON_BOUNDS[1]  + 0.01, 2.5)

        gh_vals  = ds["gh"].values  # may be (lat, lon) or (y, x)
        lat_coord = ds["latitude"].values
        lon_coord = ds["longitude"].values

        cols = {}
        for lat in lats:
            for lon in lons_360:
                col = f"lat_{lat:.1f}_lon_{lon:.1f}"
                if lat_coord.ndim == 1:
                    # Regular grid — direct index
                    li = int(np.argmin(np.abs(lat_coord - lat)))
                    lo = int(np.argmin(np.abs(lon_coord - lon)))
                    val = float(gh_vals[li, lo])
                else:
                    # Curvilinear grid (y, x) — find nearest by distance
                    dist = (lat_coord - lat) ** 2 + (lon_coord - lon) ** 2
                    yi, xi = np.unravel_index(dist.argmin(), dist.shape)
                    val = float(gh_vals[yi, xi])
                cols[col] = val

        series = pd.Series(cols)
        n_nan  = int(series.isna().sum())
        if n_nan > len(series) * 0.10:
            logger.warning("GFS z500: %d/%d NaN — falling back", n_nan, len(series))
            return None

        logger.info("GFS z500 fetched via Herbie: %d grid points for %s", len(series), target_date)
        return series

    except Exception as exc:
        logger.warning("fetch_gfs_z500 failed: %s", exc)
        return None
