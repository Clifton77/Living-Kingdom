"""
utils/hrrr_fetcher.py

Fetches HRRR T2m forecasts via Herbie and computes per-station TMAX.
Single responsibility: given a run time, return TMAX forecasts for all stations.

Key facts:
- HRRR uses 0-360 longitude convention — negative coords converted internally
- T2m is in Kelvin — converted to Fahrenheit before returning
- TMAX = max(T2m) across all forecast hours from run time through peak hour
- Nearest grid point selection (3km grid, <1.5km positional error)
"""

from __future__ import annotations

import warnings
from datetime import datetime, date, timedelta
from typing import Optional

import numpy as np

warnings.filterwarnings("ignore")


def _kelvin_to_f(k: float) -> float:
    return (k - 273.15) * 9 / 5 + 32


def _extract_point(ds, lat: float, lon: float) -> float:
    """Extract T2m (°F) at nearest grid point. Handles HRRR's 0-360 lon convention."""
    lon_360 = lon + 360 if lon < 0 else lon
    lats = ds.latitude.values
    lons = ds.longitude.values
    dist = np.sqrt((lats - lat) ** 2 + (lons - lon_360) ** 2)
    idx = np.unravel_index(dist.argmin(), dist.shape)
    return _kelvin_to_f(float(ds["t2m"].values[idx]))


def fetch_station_tmax(
    run_time: datetime,
    station_coords: dict[str, tuple[float, float]],
    peak_utc_by_station: dict[str, int],
) -> dict[str, Optional[float]]:
    """
    Fetch HRRR T2m for a single run and return TMAX (°F) per station.

    Parameters
    ----------
    run_time            : HRRR run datetime (UTC), e.g. datetime(2026, 5, 5, 12)
    station_coords      : {kalshi_label: (lat, lon)} using settlement station coords
    peak_utc_by_station : {kalshi_label: peak_hour_utc} — determines fxx range per station

    Returns
    -------
    dict[station, tmax_f | None]  — None if fetch failed for that station
    """
    from herbie import Herbie

    run_hour = run_time.hour
    max_lead = max(
        (peak - run_hour) for peak in peak_utc_by_station.values()
        if peak > run_hour
    ) if any(p > run_hour for p in peak_utc_by_station.values()) else 0

    if max_lead <= 0:
        return {s: None for s in station_coords}

    # Fetch each forecast hour and cache T2m per station
    station_temps: dict[str, list[float]] = {s: [] for s in station_coords}

    for fxx in range(1, max_lead + 1):
        try:
            H = Herbie(
                run_time.strftime("%Y-%m-%d %H:%M"),
                model="hrrr",
                product="sfc",
                fxx=fxx,
                verbose=False,
            )
            ds = H.xarray("TMP:2 m above ground", remove_grib=True)

            for station, (lat, lon) in station_coords.items():
                peak_utc = peak_utc_by_station.get(station, 0)
                if run_hour + fxx > peak_utc:
                    continue  # beyond this station's peak — skip
                t2m_f = _extract_point(ds, lat, lon)
                station_temps[station].append(t2m_f)

        except Exception:
            continue  # failed fxx — skip, use whatever hours succeeded

    return {
        station: (max(temps) if temps else None)
        for station, temps in station_temps.items()
    }


def get_run_times_since(since_utc: datetime, current_utc: datetime) -> list[datetime]:
    """Return list of HRRR run datetimes between since_utc and current_utc (hourly)."""
    runs = []
    h = since_utc.replace(minute=0, second=0, microsecond=0)
    while h <= current_utc:
        runs.append(h)
        h += timedelta(hours=1)
    return runs
