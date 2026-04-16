"""
Build seasonal peak heating hours from IEM hourly ASOS archive.

For each station and day-of-year (DOY 1–365), finds the local hour by
which 90% of historical days have reached their daily maximum temperature,
using a ±WINDOW_DAYS rolling window across the full historical record.

This produces a smooth seasonal curve instead of hard monthly bins,
correctly capturing gradual season transitions (e.g. the shift in peak
timing across May as the sun climbs higher each day).

Data source: IEM ASOS hourly archive — settlement station ICAO codes
             (KNYC for KJFK, KMDW for KORD, others match directly)
Date range:  START_DATE → END_DATE from config.py (default 2010–2024)

Cache:  data/hourly_obs.parquet — raw hourly obs per station;
        re-used on subsequent runs so IEM is only contacted once.
        Add --force-fetch to re-pull all stations from IEM.

Output: data/peak_hours.parquet — (station, doy, p50_peak_hour,
        p90_peak_hour, n_days) — one row per station per DOY.

Run via pipeline:
    python run_pipeline.py --only peak_hours
    python run_pipeline.py --only peak_hours --force   # re-fetch IEM

Or standalone:
    python scripts/build_peak_hours.py
    python scripts/build_peak_hours.py --force-fetch

Dynamic lookup (no config.py edits needed):
    from utils.peak_hours import get_peak_hour
    hour = get_peak_hour("KJFK", date(2026, 7, 15))   # → e.g. 16
"""

from __future__ import annotations

import argparse
import os
import time
from io import StringIO

import numpy as np
import pandas as pd
import requests
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logging_config import setup_logging
from config import (
    STATIONS, STATION_TIMEZONES, DATA_DIR, LOGS_DIR,
    START_DATE, END_DATE,
    PEAK_HOURS_PARQUET, HOURLY_OBS_PARQUET,
    settlement_station,
)

logger = setup_logging("build_peak_hours")

IEM_ASOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"

# IEM ASOS overrides for stations whose settlement ICAO is not an airport/ASOS
# station.  KNYC (Central Park) has no ASOS record on IEM — use JFK airport
# instead.  Peak-hour TIMING is effectively identical across a metro area.
_IEM_ASOS_ICAO: dict[str, str] = {
    "KJFK": "KJFK",   # settlement = KNYC (Central Park) — not in IEM ASOS
}

# Rolling window half-width in calendar days.
# ±30 days → each DOY point draws from ~61 calendar days × 15 years ≈ 900 obs days.
WINDOW_DAYS = 30

# Percentile used as the overshoot safety cutoff.
# p90 = by this local hour, 90 % of historical days have already hit their max.
PERCENTILE = 90

# Skip a DOY point if fewer than this many obs days fall in its window.
MIN_WINDOW_OBS = 30

# Require at least this many hourly readings in a day before trusting it.
MIN_HOURLY_OBS_PER_DAY = 6

# Safety floor: never place the peak-hour cutoff before noon local time.
PEAK_HOUR_FLOOR = 12


# ---------------------------------------------------------------------------
# IEM hourly data fetch
# ---------------------------------------------------------------------------

def _fetch_one_year(icao: str, tz: str, year: int) -> pd.DataFrame | None:
    """
    Fetch one calendar year of hourly ASOS obs from IEM for a single station.

    Fetching year-by-year (rather than the full 15-year range in one request)
    avoids IEM's response-size limit, which silently truncates large queries
    and returns only a few hundred rows instead of ~8 700 per year.

    No report_type filter — the filter was excluding most ASOS automated obs
    and is unnecessary here (we only request tmpf, not SPECI/remarks).
    """
    params = {
        "station":  icao,
        "data":     "tmpf",
        "year1":    str(year), "month1": "01", "day1": "01",
        "year2":    str(year), "month2": "12", "day2": "31",
        "tz":       tz,
        "format":   "onlycomma",
        "latlon":   "no",
        "missing":  "M",
        "trace":    "T",
        "direct":   "no",
    }

    for attempt in range(3):
        try:
            resp = requests.get(IEM_ASOS_URL, params=params, timeout=90)
            resp.raise_for_status()

            text = resp.text.strip()
            if not text or len(text) < 30:
                return None   # genuinely no data for this year

            df = pd.read_csv(StringIO(text), na_values=["M", "T", ""])

            if "valid" not in df.columns or "tmpf" not in df.columns:
                return None

            df["valid_local"] = pd.to_datetime(df["valid"], errors="coerce")
            df["tmpf"]        = pd.to_numeric(df["tmpf"], errors="coerce")
            df = df.dropna(subset=["valid_local", "tmpf"])

            return df[["valid_local", "tmpf"]].copy() if not df.empty else None

        except Exception as exc:
            wait = 2 ** attempt
            logger.debug("%s %d: attempt %d failed (%s) — retrying in %ds",
                         icao, year, attempt + 1, exc, wait)
            time.sleep(wait)

    return None


def fetch_hourly_asos(kalshi_label: str, start: str, end: str) -> pd.DataFrame | None:
    """
    Fetch routine hourly temperature obs from IEM ASOS for a station,
    requesting one calendar year at a time to stay within IEM's response
    size limits.

    Station ICAO resolution:
        - Uses _IEM_ASOS_ICAO override table first (e.g. KJFK stays KJFK,
          because KNYC/Central Park has no ASOS record on IEM).
        - Falls back to settlement_station() for all others
          (KORD → KMDW, rest are direct matches).

    Returns DataFrame with columns [station, valid_local, tmpf]:
        station     : Kalshi label (e.g. "KJFK"), NOT the IEM ICAO
        valid_local : local-time timestamp, tz-naive (already in station TZ)
        tmpf        : temperature °F
    Returns None if every year failed.
    """
    icao = _IEM_ASOS_ICAO.get(kalshi_label) or settlement_station(kalshi_label)
    tz   = STATION_TIMEZONES[kalshi_label]

    start_year = int(start[:4])
    end_year   = int(end[:4])

    year_frames: list[pd.DataFrame] = []

    for year in range(start_year, end_year + 1):
        df = _fetch_one_year(icao, tz, year)
        if df is not None and not df.empty:
            year_frames.append(df)
        # Tiny pause — be polite; also avoids IEM rate-limiting on rapid bursts
        time.sleep(0.3)

    if not year_frames:
        logger.error("%s (→ %s): no data returned for any year %d–%d",
                     kalshi_label, icao, start_year, end_year)
        return None

    result = pd.concat(year_frames, ignore_index=True)
    result.insert(0, "station", kalshi_label)

    logger.info("%s (→ %s): fetched %d hourly obs (%d–%d)",
                kalshi_label, icao, len(result), start_year, end_year)
    return result


def load_or_fetch_hourly_obs(force_fetch: bool = False) -> pd.DataFrame:
    """
    Return a combined DataFrame of raw hourly obs for all stations.

    If HOURLY_OBS_PARQUET exists and force_fetch is False, loads the cache
    and only fetches stations that are missing from it.  If force_fetch is
    True, re-fetches every station from IEM and replaces the file.

    Raises RuntimeError if no data is available at all.
    """
    os.makedirs(DATA_DIR, exist_ok=True)

    existing:          pd.DataFrame | None = None
    existing_stations: set[str]            = set()

    if not force_fetch and os.path.exists(HOURLY_OBS_PARQUET):
        try:
            existing = pd.read_parquet(HOURLY_OBS_PARQUET)
            existing_stations = set(existing["station"].unique())
            logger.info(
                "Hourly obs cache: %d rows, %d stations  (%s)",
                len(existing), len(existing_stations),
                ", ".join(sorted(existing_stations)),
            )
        except Exception as exc:
            logger.warning("Could not read hourly obs cache (%s) — re-fetching all", exc)
            existing = None
            existing_stations = set()

    to_fetch = [s for s in STATIONS if force_fetch or s not in existing_stations]

    if not to_fetch:
        logger.info("All stations in cache — skipping IEM fetch")
        return existing

    logger.info("Fetching %d station(s) from IEM: %s", len(to_fetch), to_fetch)

    new_frames: list[pd.DataFrame] = []
    for station in to_fetch:
        df = fetch_hourly_asos(station, START_DATE, END_DATE)
        if df is not None and not df.empty:
            new_frames.append(df)
        time.sleep(2)   # be polite to IEM

    if not new_frames and existing is not None:
        logger.warning("IEM fetch produced no new data — returning cached data only")
        return existing

    parts = ([existing] if existing is not None else []) + new_frames
    if not parts:
        raise RuntimeError(
            "No hourly obs data available — IEM fetch failed for all stations"
        )

    combined = pd.concat(parts, ignore_index=True)

    # Drop duplicates (same station + timestamp) that can appear if a station
    # was partially cached and then re-fetched with --force-fetch.
    combined = combined.drop_duplicates(subset=["station", "valid_local"])

    combined.to_parquet(HOURLY_OBS_PARQUET, index=False)
    logger.info(
        "Saved hourly_obs.parquet: %d rows, %d stations",
        len(combined), combined["station"].nunique(),
    )
    return combined


# ---------------------------------------------------------------------------
# Daily peak-hour extraction
# ---------------------------------------------------------------------------

def _extract_daily_peaks(hourly_all: pd.DataFrame, station: str) -> pd.DataFrame:
    """
    For each calendar day in hourly_all for a given station, determine the
    local hour at which the daily maximum temperature FIRST occurred.

    Returns DataFrame with columns [date, doy, peak_hour]:
        date      : calendar date (datetime.date)
        doy       : day-of-year 1–365 (leap day 366 clamped to 365)
        peak_hour : local hour (0–23) when daily max was first reached
    """
    df = hourly_all[hourly_all["station"] == station].copy()
    if df.empty:
        return pd.DataFrame(columns=["date", "doy", "peak_hour"])

    df["date"]  = df["valid_local"].dt.date
    df["hour"]  = df["valid_local"].dt.hour
    # Clamp leap-year DOY 366 to 365 so the index stays 1-365
    df["doy"]   = df["valid_local"].dt.day_of_year.clip(upper=365).astype(int)

    records = []
    for day, day_df in df.groupby("date"):
        if len(day_df) < MIN_HOURLY_OBS_PER_DAY:
            continue
        max_temp = day_df["tmpf"].max()
        # First hour at which max was reached
        peak_row = day_df[day_df["tmpf"] == max_temp].iloc[0]
        records.append({
            "date":      day,
            "doy":       int(peak_row["doy"]),
            "peak_hour": int(peak_row["hour"]),
        })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# DOY-smoothed peak hour curve
# ---------------------------------------------------------------------------

def compute_peak_hours_doy(daily_peaks: pd.DataFrame, station: str) -> pd.DataFrame:
    """
    Build the DOY-smoothed p90 seasonal curve for a station.

    For each DOY 1–365, gathers all historical days that fall within
    ±WINDOW_DAYS of that DOY (wrapping around the year boundary) and
    computes the p50 and p90 of the daily peak heating hours.

    The ±30-day window means each point draws from ~61 calendar days across
    ~15 years, providing ~900 obs per DOY — enough for stable p90 estimates.

    Returns DataFrame: [station, doy, p50_peak_hour, p90_peak_hour, n_days]
    """
    results = []

    for doy in range(1, 366):
        lo = doy - WINDOW_DAYS
        hi = doy + WINDOW_DAYS

        # Year-wrap: DOY 1 wraps back to DOY 336–365 of the previous year
        if lo < 1 and hi > 365:
            mask = pd.Series(True, index=daily_peaks.index)
        elif lo < 1:
            # Window clips past Jan 1 → wrap to end of year
            mask = (daily_peaks["doy"] <= hi) | (daily_peaks["doy"] >= (365 + lo))
        elif hi > 365:
            # Window clips past Dec 31 → wrap to start of year
            mask = (daily_peaks["doy"] >= lo) | (daily_peaks["doy"] <= (hi - 365))
        else:
            mask = (daily_peaks["doy"] >= lo) & (daily_peaks["doy"] <= hi)

        window = daily_peaks[mask]

        if len(window) < MIN_WINDOW_OBS:
            logger.debug("%s DOY %3d: %d obs in window — below minimum, skipping",
                         station, doy, len(window))
            continue

        hours = window["peak_hour"].values
        p50   = int(np.percentile(hours, 50))
        p90   = int(np.percentile(hours, PERCENTILE))
        p90   = max(p90, PEAK_HOUR_FLOOR)   # never before noon

        results.append({
            "station":       station,
            "doy":           doy,
            "p50_peak_hour": p50,
            "p90_peak_hour": p90,
            "n_days":        len(window),
        })

    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_peak_hours(force_fetch: bool = False) -> None:
    """
    Full build:
      1. Load (or fetch) raw hourly obs → hourly_obs.parquet
      2. Extract daily peak-hour records per station
      3. Compute DOY-smoothed p90 curve per station
      4. Save → peak_hours.parquet

    After this runs, get_peak_hour() in utils/peak_hours.py will
    automatically use the new parquet on next call (or after invalidate_cache()).
    """
    logger.info(
        "build_peak_hours start  window=±%d days  p%d  floor=%dh",
        WINDOW_DAYS, PERCENTILE, PEAK_HOUR_FLOOR,
    )
    logger.info("Stations (%d): %s", len(STATIONS), STATIONS)
    logger.info("Date range: %s → %s", START_DATE, END_DATE)

    hourly_all = load_or_fetch_hourly_obs(force_fetch=force_fetch)

    all_results: list[pd.DataFrame] = []

    for station in STATIONS:
        logger.info("── %s ──────────────────────────────", station)

        daily = _extract_daily_peaks(hourly_all, station)
        if daily.empty:
            logger.warning("%s: no valid daily peaks — skipping", station)
            continue

        logger.info(
            "%s: %d valid station-days, DOY range %d–%d",
            station, len(daily), daily["doy"].min(), daily["doy"].max(),
        )

        doy_df = compute_peak_hours_doy(daily, station)
        if doy_df.empty:
            logger.warning("%s: no DOY results produced — skipping", station)
            continue

        all_results.append(doy_df)

        # Log a seasonal snapshot at four representative DOYs
        for ref_doy, label in [(15, "Jan"), (105, "Apr"), (196, "Jul"), (288, "Oct")]:
            row = doy_df[doy_df["doy"] == ref_doy]
            if not row.empty:
                r = row.iloc[0]
                logger.info(
                    "  %s  DOY %3d (%s):  p50=%02dh  p90=%02dh  n=%d",
                    station, ref_doy, label,
                    r["p50_peak_hour"], r["p90_peak_hour"], r["n_days"],
                )

    if not all_results:
        logger.error(
            "No results produced — check IEM connectivity or hourly_obs.parquet"
        )
        return

    combined = pd.concat(all_results, ignore_index=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    combined.to_parquet(PEAK_HOURS_PARQUET, index=False)

    logger.info(
        "Saved peak_hours.parquet: %d rows  (%d stations × ≤365 DOYs)",
        len(combined), combined["station"].nunique(),
    )
    logger.info(
        "Lookup: from utils.peak_hours import get_peak_hour; "
        "invalidate_cache() if scheduler is already running"
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Build DOY-smoothed peak heating hours from IEM hourly ASOS data"
    )
    ap.add_argument(
        "--force-fetch", action="store_true",
        help="Re-fetch all stations from IEM even if hourly_obs.parquet exists",
    )
    args = ap.parse_args()
    build_peak_hours(force_fetch=args.force_fetch)
