"""
Script 1: Build historical daily max temperature database.

Sources:
  A) Iowa State Mesonet ASOS daily summary (primary)
  B) NOAA CDO GHCND (cross-validation)

Output: data/obs_daily.parquet
  {station, date, tmax_observed_f, source_flag}
"""
import os
import sys
import time
import logging
import csv
import io
from datetime import date, timedelta

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    STATIONS, GHCND_IDS, START_DATE, END_DATE,
    NOAA_CDO_TOKEN, OBS_PARQUET, LOGS_DIR, RAW_DIR,
    settlement_station,
)
from utils.retry import retry_request
from utils.validate import cross_validate_obs
from utils.logging_config import setup_logging

logger = setup_logging("build_obs_database")

IEM_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/daily.py"
CDO_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"


# ---------------------------------------------------------------------------
# IEM fetch
# ---------------------------------------------------------------------------
@retry_request(max_attempts=3, backoff_base=2.0)
def _get_iem_raw(station: str, start: str, end: str) -> str:
    params = {
        "network": "ASOS",
        "stations": station,
        "year1": start[:4], "month1": start[5:7], "day1": start[8:10],
        "year2": end[:4],   "month2": end[5:7],   "day2": end[8:10],
        "format": "csv",
        "what": "download",
        "tz": "UTC",
    }
    resp = requests.get(IEM_URL, params=params, timeout=60)
    resp.raise_for_status()
    return resp.text


def fetch_iem_daily(station: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch daily max temps from Iowa State Mesonet ASOS archive.
    Returns DataFrame: [station, date, tmax_f]
    """
    logger.info("IEM fetch: %s %s → %s", station, start, end)
    raw = _get_iem_raw(station, start, end)

    reader = csv.DictReader(io.StringIO(raw))
    rows = []
    for row in reader:
        tmax_raw = row.get("max_tmpf", "M").strip()
        try:
            tmax_f = float(tmax_raw)
        except ValueError:
            tmax_f = float("nan")
        rows.append({"date": row.get("day", "").strip(), "tmax_f": tmax_f})

    df = pd.DataFrame(rows)
    df["station"] = station
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])
    logger.info("IEM %s: %d rows fetched", station, len(df))
    return df[["station", "date", "tmax_f"]]


# ---------------------------------------------------------------------------
# NOAA CDO fetch
# ---------------------------------------------------------------------------
@retry_request(max_attempts=3, backoff_base=2.0)
def _get_cdo_page(ghcnd_id: str, year: int, offset: int) -> dict:
    headers = {"token": NOAA_CDO_TOKEN}
    params = {
        "datasetid": "GHCND",
        "stationid": f"GHCND:{ghcnd_id}",
        "datatypeid": "TMAX",
        "startdate": f"{year}-01-01",
        "enddate": f"{year}-12-31",
        "units": "standard",
        "limit": 1000,
        "offset": offset,
    }
    resp = requests.get(CDO_URL, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def fetch_noaa_cdo(station: str, start_year: int, end_year: int) -> pd.DataFrame:
    """
    Fetch daily max temps from NOAA CDO GHCND for cross-validation.
    CDO returns TMAX in tenths of °C → convert to °F.
    Returns DataFrame: [station, date, tmax_f]
    """
    if not NOAA_CDO_TOKEN:
        logger.warning("No NOAA_CDO_TOKEN set — skipping CDO fetch for %s", station)
        return pd.DataFrame(columns=["station", "date", "tmax_f"])

    ghcnd_id = GHCND_IDS.get(station)
    if not ghcnd_id:
        logger.warning("No GHCND ID for %s — skipping CDO fetch", station)
        return pd.DataFrame(columns=["station", "date", "tmax_f"])

    all_rows = []
    for year in range(start_year, end_year + 1):
        offset = 1
        while True:
            try:
                data = _get_cdo_page(ghcnd_id, year, offset)
            except Exception as e:
                logger.warning("CDO %s year %d offset %d failed: %s", station, year, offset, e)
                break

            results = data.get("results", [])
            for rec in results:
                # TMAX in tenths of °C → °F
                tmax_c = rec["value"] / 10.0
                tmax_f = tmax_c * 9.0 / 5.0 + 32.0
                all_rows.append({
                    "date": rec["date"][:10],
                    "tmax_f": tmax_f,
                })

            meta = data.get("metadata", {}).get("resultset", {})
            count = meta.get("count", 0)
            if offset + 999 >= count:
                break
            offset += 1000
            time.sleep(0.2)  # respect CDO rate limit

        time.sleep(0.5)

    df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=["date", "tmax_f"])
    df["station"] = station
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date"])
    logger.info("CDO %s: %d rows fetched", station, len(df))
    return df[["station", "date", "tmax_f"]]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def build_obs_database() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(LOGS_DIR, exist_ok=True)

    start_year = int(START_DATE[:4])
    end_year   = int(END_DATE[:4])

    all_dfs = []
    missing_log = []

    for station in STATIONS:
        # Use the NWS settlement station for data fetching (may differ from Kalshi label).
        # KJFK → fetch KNYC (Central Park), KORD → fetch KMDW (Midway), others unchanged.
        # Results are stored under the original Kalshi label so downstream joins work.
        settle = settlement_station(station)
        logger.info("Processing station: %s (settlement: %s)", station, settle)

        # --- IEM ---
        try:
            iem_df = fetch_iem_daily(settle, START_DATE, END_DATE)
            iem_df["station"] = station   # relabel KNYC→KJFK, KMDW→KORD, etc.
        except Exception as e:
            logger.error("IEM fetch failed for %s: %s", station, e)
            iem_df = pd.DataFrame(columns=["station", "date", "tmax_f"])

        # --- CDO ---
        try:
            cdo_df = fetch_noaa_cdo(settle, start_year, end_year)
            cdo_df["station"] = station   # relabel
        except Exception as e:
            logger.error("CDO fetch failed for %s: %s", station, e)
            cdo_df = pd.DataFrame(columns=["station", "date", "tmax_f"])

        # --- Cross-validate ---
        validated = cross_validate_obs(iem_df, cdo_df, station)

        # --- Track missing ---
        missing = validated[validated["source_flag"] == "MISSING"]
        for _, row in missing.iterrows():
            missing_log.append({"station": station, "date": row["date"]})

        all_dfs.append(validated)
        logger.info("%s: %d records, %d missing", station, len(validated), len(missing))

    combined = pd.concat(all_dfs, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"])

    # Save
    combined.to_parquet(OBS_PARQUET, index=False)
    logger.info("Saved obs_daily.parquet: %d rows", len(combined))

    # Save missing log
    if missing_log:
        missing_df = pd.DataFrame(missing_log)
        missing_path = os.path.join(LOGS_DIR, "obs_missing.csv")
        missing_df.to_csv(missing_path, index=False)
        logger.warning("Missing obs logged to %s (%d rows)", missing_path, len(missing_df))

    # Sanity check
    expected_rows = len(STATIONS) * (end_year - start_year + 1) * 365
    actual_rows = len(combined)
    ratio = actual_rows / expected_rows
    if ratio < 0.90:
        logger.error(
            "Row count sanity check FAILED: expected ~%d got %d (%.1f%%)",
            expected_rows, actual_rows, ratio * 100
        )
    else:
        logger.info(
            "Row count sanity check passed: %d rows (%.1f%% of expected)",
            actual_rows, ratio * 100
        )


if __name__ == "__main__":
    build_obs_database()
