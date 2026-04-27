"""
Fast builder: model_fcst.parquet using only Open-Meteo historical forecast API.

20 stations × 2 models (GFS + ECMWF) = 40 batch requests, ~1 minute.
Produces GFS_OPENMETEO and ECMWF_OPENMETEO rows — enough for build_bias_table.py
to compute the sigma (distribution width) used by the Phase 4 signal path.

Run instead of the full build_model_forecast_archive.py when you only need
to rebuild the bias table for GFS/ECMWF-based signals.
"""
import os
import sys
import time
import logging

import requests
import pandas as pd
from tqdm import tqdm

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import STATIONS, STATION_COORDS, FCST_PARQUET, LOGS_DIR, START_DATE, END_DATE, settlement_station
from utils.retry import retry_request
from utils.logging_config import setup_logging
from datetime import datetime

logger = setup_logging("build_om_forecast_archive")

HIST_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

# Historical forecast API data starts ~2021 for GFS, ~2023 for ECMWF IFS.
OM_START = "2021-01-01"


@retry_request(max_attempts=3, backoff_base=2.0)
def _fetch(lat: float, lon: float, start: str, end: str, model: str) -> dict:
    params = {
        "latitude":         lat,
        "longitude":        lon,
        "daily":            "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "start_date":       start,
        "end_date":         end,
        "models":           model,
        "timezone":         "UTC",
    }
    r = requests.get(HIST_FORECAST_URL, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def fetch_station(station: str, start: str, end: str) -> list[dict]:
    settle = settlement_station(station)
    coords = STATION_COORDS.get(settle) or STATION_COORDS.get(station)
    if coords is None:
        logger.warning("No coords for %s — skipping", station)
        return []

    lat, lon = coords
    rows: list[dict] = []

    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt   = datetime.strptime(end,   "%Y-%m-%d")

    for om_model, label in [("gfs_seamless", "GFS_OPENMETEO"), ("ecmwf_ifs025", "ECMWF_OPENMETEO")]:
        cur = start_dt
        while cur <= end_dt:
            chunk_end = min(datetime(cur.year, 12, 31), end_dt)
            try:
                data  = _fetch(lat, lon, cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d"), om_model)
                dates = data.get("daily", {}).get("time", [])
                temps = data.get("daily", {}).get("temperature_2m_max", [])
                for d, t in zip(dates, temps):
                    if t is not None:
                        rows.append({
                            "station":         station,
                            "date":            pd.Timestamp(d),
                            "forecast_tmax_f": float(t),
                            "model_source":    label,
                        })
            except Exception as exc:
                logger.warning("%s %s %d: %s", station, om_model, cur.year, exc)
            cur = datetime(cur.year + 1, 1, 1)
            time.sleep(0.5)

    gfs_cnt  = sum(1 for r in rows if r["model_source"] == "GFS_OPENMETEO")
    ecmwf_cnt = sum(1 for r in rows if r["model_source"] == "ECMWF_OPENMETEO")
    logger.info("%s — GFS: %d  ECMWF: %d", station, gfs_cnt, ecmwf_cnt)
    return rows


def main():
    os.makedirs(LOGS_DIR, exist_ok=True)
    all_rows: list[dict] = []

    # If an existing parquet already has AFM/MOS/NBM rows, preserve them.
    if os.path.exists(FCST_PARQUET):
        existing = pd.read_parquet(FCST_PARQUET)
        keep = existing[~existing["model_source"].isin(["GFS_OPENMETEO", "ECMWF_OPENMETEO"])]
        logger.info("Loaded existing model_fcst.parquet: %d rows kept (non-OM sources)", len(keep))
        all_rows.extend(keep.to_dict("records"))
    else:
        logger.info("No existing model_fcst.parquet — building from scratch")

    effective_start = max(START_DATE, OM_START)
    logger.info("Fetching Open-Meteo historical: %s → %s", effective_start, END_DATE)

    for station in tqdm(STATIONS, desc="Open-Meteo GFS/ECMWF"):
        all_rows.extend(fetch_station(station, effective_start, END_DATE))
        time.sleep(0.25)

    df = pd.DataFrame(all_rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["station", "model_source", "date"]).drop_duplicates(
        subset=["station", "model_source", "date"], keep="last"
    )
    df.to_parquet(FCST_PARQUET, index=False)

    logger.info("Saved model_fcst.parquet: %d total rows", len(df))
    for src, cnt in df["model_source"].value_counts().items():
        logger.info("  %-18s: %d rows", src, cnt)


if __name__ == "__main__":
    main()
