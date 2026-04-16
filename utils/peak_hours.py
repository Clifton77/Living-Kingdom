"""
Dynamic peak heating hour lookup.

Reads the DOY-smoothed seasonal curve from peak_hours.parquet (built by
scripts/build_peak_hours.py) and returns the p90 peak hour for any
station / date combination.

Lookup priority:
  1. peak_hours.parquet — DOY-smoothed seasonal curve (preferred)
  2. config.STATION_PEAK_HOURS[station][month] — static monthly fallback
  3. 15  (3 PM local) — universal safe default

The parquet is loaded once at first call and cached in-process.
Call invalidate_cache() after running build_peak_hours if the
scheduler is already up and you want it to pick up the new data.

Usage:
    from utils.peak_hours import get_peak_hour, invalidate_cache

    hour = get_peak_hour("KJFK", date(2026, 7, 15))   # → e.g. 16
    hour = get_peak_hour("KLAX", date(2026, 1, 3))    # → e.g. 14

    invalidate_cache()   # force reload after rebuild
"""

from __future__ import annotations

import os
from datetime import date

import pandas as pd

from config import PEAK_HOURS_PARQUET, STATION_PEAK_HOURS
from utils.logging_config import setup_logging

logger = setup_logging("peak_hours")

# In-process cache: dict[(station, doy)] → p90_peak_hour
# None  = not yet loaded (lazy init on first call)
# {}    = parquet missing or unreadable (use config fallback every time)
_cache: dict[tuple[str, int], int] | None = None


def _load_cache() -> dict[tuple[str, int], int]:
    """
    Load peak_hours.parquet into a fast dict keyed by (station, doy).
    Returns an empty dict if the file is absent or cannot be parsed.
    """
    if not os.path.exists(PEAK_HOURS_PARQUET):
        logger.debug(
            "peak_hours.parquet not found at %s — using config fallback",
            PEAK_HOURS_PARQUET,
        )
        return {}

    try:
        df = pd.read_parquet(
            PEAK_HOURS_PARQUET,
            columns=["station", "doy", "p90_peak_hour"],
        )
        result: dict[tuple[str, int], int] = {
            (row.station, int(row.doy)): int(row.p90_peak_hour)
            for row in df.itertuples(index=False)
        }
        logger.info(
            "peak_hours cache loaded: %d entries across %d stations",
            len(result),
            df["station"].nunique(),
        )
        return result

    except Exception as exc:
        logger.warning(
            "Could not load peak_hours.parquet (%s) — using config fallback",
            exc,
        )
        return {}


def invalidate_cache() -> None:
    """
    Clear the in-process cache so the next get_peak_hour() call reloads
    peak_hours.parquet from disk.

    Call this after running build_peak_hours if the scheduler is already
    running and you want it to pick up the updated seasonal curves.
    """
    global _cache
    _cache = None
    logger.info("peak_hours cache invalidated — will reload on next call")


def get_peak_hour(station: str, target_date: date) -> int:
    """
    Return the p90 peak heating hour for a station on a given date.

    The peak heating hour is the local hour by which 90 % of historical
    days at this station (for this point in the season) have already
    reached their daily maximum temperature.  Use it as the overshoot /
    undershoot exit guard cutoff in risk.py.

    Parameters
    ----------
    station     : Kalshi station label, e.g. "KJFK"
    target_date : The event date

    Returns
    -------
    int : Local hour 0–23.  Guaranteed ≥ 12 (the peak_hours builder
          enforces a noon floor; the config fallback default is 15).
    """
    global _cache

    # Lazy-load parquet on first call
    if _cache is None:
        _cache = _load_cache()

    # Leap year: DOY 366 (Feb 29) → 365 so it stays within the 1-365 index
    doy = min(target_date.timetuple().tm_yday, 365)

    # ── 1. Parquet lookup (DOY-smoothed seasonal curve) ──────────────────
    if _cache:
        hour = _cache.get((station, doy))
        if hour is not None:
            return hour
        # Parquet loaded but this (station, doy) is absent — fall through
        logger.debug(
            "get_peak_hour: %s DOY %d not in parquet — trying config fallback",
            station, doy,
        )

    # ── 2. Static monthly dict from config.py ────────────────────────────
    month_hour = STATION_PEAK_HOURS.get(station, {}).get(target_date.month)
    if month_hour is not None:
        return month_hour

    # ── 3. Universal safe default ─────────────────────────────────────────
    logger.debug(
        "get_peak_hour: %s has no config entry — returning default 15",
        station,
    )
    return 15
