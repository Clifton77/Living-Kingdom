"""
One-time analysis script: compute climatological peak heating hours.

For each station and calendar month, finds the local hour by which
90% of historical days have already reached their daily maximum
temperature. This becomes the cutoff for overshoot exit protection —
after this hour the daily high is almost certainly set, so we hold
the position rather than exiting on overshoot risk.

Data source: IEM ASOS hourly archive (same stations as Phase 1)
Date range:  2010-01-01 to 2024-12-31 (matches training period)
Output:      data/peak_hours.parquet  +  logs/peak_hours_report.txt

Run once:
    python scripts/build_peak_hours.py

Results are then baked into config.py as STATION_PEAK_HOURS.
"""

import os
import time
import calendar
from io import StringIO
from datetime import datetime

import pandas as pd
import numpy as np
import requests

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logging_config import setup_logging
from config import STATIONS, STATION_TIMEZONES, DATA_DIR, LOGS_DIR, START_DATE, END_DATE

logger = setup_logging("build_peak_hours")

OUTPUT_PARQUET = os.path.join(DATA_DIR, "peak_hours.parquet")
OUTPUT_REPORT  = os.path.join(LOGS_DIR, "peak_hours_report.txt")

IEM_ASOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"

# Percentile to use as the safety cutoff
# 90th = by this hour, 90% of historical days have reached their max
PERCENTILE = 90


# ---------------------------------------------------------------------------
# IEM hourly fetch
# ---------------------------------------------------------------------------

def fetch_hourly_asos(station: str, start: str, end: str) -> pd.DataFrame | None:
    """
    Fetch hourly temperature observations from IEM ASOS archive.
    Returns DataFrame with columns: [valid_local, tmpf]

    Temperatures are returned in local time so peak-hour analysis
    is directly interpretable (e.g. "3 PM local").
    """
    tz = STATION_TIMEZONES[station]

    params = {
        "station":  station,
        "data":     "tmpf",
        "year1":    start[:4], "month1": start[5:7], "day1": start[8:10],
        "year2":    end[:4],   "month2": end[5:7],   "day2": end[8:10],
        "tz":       tz,
        "format":   "onlycomma",
        "latlon":   "no",
        "missing":  "M",
        "trace":    "T",
        "direct":   "no",
        "report_type": "1",    # only routine hourly obs
    }

    for attempt in range(4):
        try:
            logger.info("%s: fetching hourly ASOS (attempt %d)...", station, attempt + 1)
            resp = requests.get(IEM_ASOS_URL, params=params, timeout=120)
            resp.raise_for_status()

            text = resp.text.strip()
            if not text or len(text) < 50:
                logger.warning("%s: empty response", station)
                return None

            df = pd.read_csv(
                StringIO(text),
                skiprows=0,
                na_values=["M", "T", ""],
            )

            # IEM returns: station, valid, tmpf
            if "valid" not in df.columns or "tmpf" not in df.columns:
                logger.warning("%s: unexpected columns: %s", station, df.columns.tolist())
                return None

            df["valid_local"] = pd.to_datetime(df["valid"], errors="coerce")
            df = df.dropna(subset=["valid_local", "tmpf"])
            df["tmpf"] = pd.to_numeric(df["tmpf"], errors="coerce")
            df = df.dropna(subset=["tmpf"])

            logger.info("%s: fetched %d hourly obs", station, len(df))
            return df[["valid_local", "tmpf"]].copy()

        except Exception as exc:
            wait = 2 ** attempt
            logger.warning("%s: fetch failed (%s) — retrying in %ds", station, exc, wait)
            time.sleep(wait)

    logger.error("%s: all fetch attempts failed", station)
    return None


# ---------------------------------------------------------------------------
# Peak hour computation
# ---------------------------------------------------------------------------

def compute_peak_hours(hourly_df: pd.DataFrame, station: str) -> pd.DataFrame:
    """
    For each (station, month) combination, compute the local hour by which
    PERCENTILE% of historical days have already reached their daily max.

    Returns DataFrame:
        station | month | p50_peak_hour | p90_peak_hour | n_days | month_name
    """
    df = hourly_df.copy()
    df["date"]  = df["valid_local"].dt.date
    df["month"] = df["valid_local"].dt.month
    df["hour"]  = df["valid_local"].dt.hour

    results = []

    for month in range(1, 13):
        month_df = df[df["month"] == month].copy()

        if len(month_df) < 30:
            logger.warning("%s month=%d: too few obs (%d)", station, month, len(month_df))
            continue

        # For each day, find the hour at which the daily max occurred
        daily_max_hours = []

        for day, day_df in month_df.groupby("date"):
            if len(day_df) < 6:   # need at least 6 hourly obs to trust the day
                continue

            max_temp = day_df["tmpf"].max()
            # Find the FIRST hour at which max was reached
            max_hour_row = day_df[day_df["tmpf"] == max_temp].iloc[0]
            daily_max_hours.append(max_hour_row["hour"])

        if len(daily_max_hours) < 20:
            logger.warning("%s month=%d: too few valid days (%d)", station, month, len(daily_max_hours))
            continue

        hours_arr = np.array(daily_max_hours)
        p50 = int(np.percentile(hours_arr, 50))
        p90 = int(np.percentile(hours_arr, PERCENTILE))

        # Safety floor: never set cutoff earlier than noon local
        p90 = max(p90, 12)

        results.append({
            "station":       station,
            "month":         month,
            "month_name":    calendar.month_abbr[month],
            "p50_peak_hour": p50,
            "p90_peak_hour": p90,
            "n_days":        len(daily_max_hours),
        })

        logger.info(
            "%s %s: p50=%dh p90=%dh (n=%d)",
            station, calendar.month_abbr[month], p50, p90, len(daily_max_hours),
        )

    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Report generator
# ---------------------------------------------------------------------------

def write_report(all_results: pd.DataFrame):
    """Write a human-readable summary for config.py population."""
    lines = ["=" * 70]
    lines.append("PEAK HEATING HOURS — 90th Percentile by Station and Month")
    lines.append("Use p90_peak_hour as STATION_PEAK_HOURS in config.py")
    lines.append("=" * 70)
    lines.append("")

    # Per-station summary: worst-case (latest) p90 by season
    lines.append("── config.py STATION_PEAK_HOURS (p90, use conservative/latest) ──")
    lines.append("")
    lines.append("STATION_PEAK_HOURS = {")

    for station in STATIONS:
        sdf = all_results[all_results["station"] == station]
        if len(sdf) == 0:
            continue

        # Build month dict
        month_dict = {}
        for _, row in sdf.iterrows():
            month_dict[int(row["month"])] = int(row["p90_peak_hour"])

        # Fill any missing months with 15 (3 PM) as safe default
        full_dict = {m: month_dict.get(m, 15) for m in range(1, 13)}

        lines.append(f'    "{station}": {{')
        month_strs = [f"{m}: {h}" for m, h in full_dict.items()]
        lines.append("        " + ", ".join(month_strs[:6]))
        lines.append("        " + ", ".join(month_strs[6:]))
        lines.append("    },")

    lines.append("}")
    lines.append("")

    # Detailed table per station
    for station in STATIONS:
        sdf = all_results[all_results["station"] == station]
        if len(sdf) == 0:
            continue

        lines.append(f"\n── {station} ─────────────────────────────────────────")
        lines.append(f"{'Month':<8} {'p50':>6} {'p90':>6} {'n_days':>8}")
        lines.append("-" * 32)

        for _, row in sdf.sort_values("month").iterrows():
            lines.append(
                f"{row['month_name']:<8} {row['p50_peak_hour']:>6} "
                f"{row['p90_peak_hour']:>6} {row['n_days']:>8}"
            )

    report = "\n".join(lines)
    os.makedirs(LOGS_DIR, exist_ok=True)
    with open(OUTPUT_REPORT, "w") as f:
        f.write(report)

    print("\n" + report)
    logger.info("Report written to %s", OUTPUT_REPORT)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_peak_hours():
    logger.info("Starting peak hours analysis")
    logger.info("Stations: %s", STATIONS)
    logger.info("Date range: %s to %s", START_DATE, END_DATE)

    os.makedirs(DATA_DIR, exist_ok=True)
    all_results = []

    for station in STATIONS:
        logger.info("=" * 50)
        logger.info("Processing %s", station)

        hourly_df = fetch_hourly_asos(station, START_DATE, END_DATE)
        if hourly_df is None or len(hourly_df) == 0:
            logger.error("%s: no data — skipping", station)
            continue

        station_results = compute_peak_hours(hourly_df, station)

        if len(station_results) > 0:
            all_results.append(station_results)

        # Be polite to IEM server
        time.sleep(2)

    if not all_results:
        logger.error("No results produced — check network and IEM availability")
        return

    combined = pd.concat(all_results, ignore_index=True)
    combined.to_parquet(OUTPUT_PARQUET, index=False)
    logger.info("Saved peak_hours.parquet: %d rows", len(combined))

    write_report(combined)
    logger.info("Done. Copy STATION_PEAK_HOURS from %s into config.py", OUTPUT_REPORT)


if __name__ == "__main__":
    build_peak_hours()
