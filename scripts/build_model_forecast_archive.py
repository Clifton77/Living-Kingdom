"""
Script 4: Build NWS forecast archive.

Primary source: IEM AFM (Area Forecast Matrix) text archive
  - Correct params: sdate/edate (NOT sts/ets — those return 422)

Fallback: Open-Meteo ERA5 archive
  - Free, no API key, covers 2010–present
  - ERA5 reanalysis max temp is a valid model-output proxy for bias correction

Output: data/model_fcst.parquet
  {station, date, forecast_tmax_f, source}
"""
import os
import sys
import re
import time
import logging
from datetime import date, datetime, timedelta

import requests
import pandas as pd
from tqdm import tqdm

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    STATIONS, WFO_MAP, STATION_COORDS, START_DATE, END_DATE,
    FCST_PARQUET, LOGS_DIR, RAW_DIR,
)
from utils.retry import retry_request
from utils.logging_config import setup_logging

logger = setup_logging("build_model_forecast_archive")

AFOS_URL      = "https://mesonet.agron.iastate.edu/cgi-bin/afos/retrieve.py"
OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/archive"

# Station names as they appear in AFM text (partial match sufficient)
STATION_AFM_NAMES = {
    "KJFK": ["NEW YORK", "JFK", "KENNEDY"],
    "KORD": ["CHICAGO", "O'HARE", "OHARE"],
    "KMIA": ["MIAMI"],
    "KDFW": ["DALLAS", "FORT WORTH", "DFW"],
    "KLAX": ["LOS ANGELES", "LAX"],
    "KATL": ["ATLANTA"],
    "KDEN": ["DENVER"],
    "KHOU": ["HOUSTON", "HOBBY"],
}


# ---------------------------------------------------------------------------
# IEM AFM fetch — fixed parameters (sdate/edate not sts/ets)
# ---------------------------------------------------------------------------
@retry_request(max_attempts=3, backoff_base=2.0)
def _fetch_afm_products(wfo: str, start_str: str, end_str: str) -> list[dict]:
    """
    Fetch AFM text products from IEM AFOS archive.
    IMPORTANT: IEM requires sdate/edate — sts/ets return 422.
    """
    pil = f"AFM{wfo}"
    params = {
        "pil":   pil,
        "fmt":   "json",
        "sdate": f"{start_str}T00:00Z",
        "edate": f"{end_str}T23:59Z",
        "limit": 500,
    }
    resp = requests.get(AFOS_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])


def _parse_afm_max_temp(text: str, station: str) -> float | None:
    """
    Parse Day-1 max temperature from AFM fixed-width text.
    Returns °F or None if not parseable.
    """
    search_names = STATION_AFM_NAMES.get(station, [])
    lines = text.upper().split("\n")

    max_col = None
    header_line_idx = None
    for i, line in enumerate(lines):
        if re.search(r"\bMAX\b", line) and re.search(r"\bMIN\b", line):
            match = re.search(r"\bMAX\b", line)
            if match:
                max_col = match.start()
                header_line_idx = i
                break

    if max_col is None:
        return None

    for i in range(header_line_idx + 1, min(header_line_idx + 30, len(lines))):
        line = lines[i].upper()
        if any(name in line for name in search_names):
            window_start = max(0, max_col - 4)
            window_end   = min(len(line), max_col + 10)
            window = lines[i][window_start:window_end]
            nums = re.findall(r"\d{2,3}", window)
            if nums:
                try:
                    val = float(nums[0])
                    if -20 <= val <= 130:
                        return val
                except ValueError:
                    pass
    return None


def _issue_time_to_valid_date(issue_time_str: str) -> date | None:
    """
    AFMs issued before 12Z → same-day high.
    AFMs issued after 12Z → next-day high.
    """
    try:
        dt = datetime.fromisoformat(issue_time_str.replace("Z", "+00:00"))
        if dt.hour < 12:
            return dt.date()
        else:
            return (dt + timedelta(days=1)).date()
    except Exception:
        return None


def fetch_afm_forecasts(station: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch and parse AFM archive for a station.
    Returns DataFrame: [station, date, forecast_tmax_f, source]
    """
    wfo = WFO_MAP.get(station)
    if not wfo:
        logger.warning("No WFO mapping for %s", station)
        return pd.DataFrame(columns=["station", "date", "forecast_tmax_f", "source"])

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date, "%Y-%m-%d")

    records = {}
    current = start_dt

    while current < end_dt:
        chunk_end = min(current + timedelta(days=180), end_dt)
        chunk_start_str = current.strftime("%Y-%m-%d")
        chunk_end_str   = chunk_end.strftime("%Y-%m-%d")

        try:
            products = _fetch_afm_products(wfo, chunk_start_str, chunk_end_str)
        except Exception as e:
            logger.warning("AFM fetch failed %s %s→%s: %s",
                           station, chunk_start_str, chunk_end_str, e)
            current = chunk_end + timedelta(days=1)
            time.sleep(1)
            continue

        for product in products:
            issue_time = product.get("utc_valid", "")
            text = product.get("data", "")
            if not text:
                continue
            valid_date = _issue_time_to_valid_date(issue_time)
            if valid_date is None:
                continue
            tmax = _parse_afm_max_temp(text, station)
            if tmax is None:
                continue
            records[valid_date] = tmax

        current = chunk_end + timedelta(days=1)
        time.sleep(0.5)

    if not records:
        return pd.DataFrame(columns=["station", "date", "forecast_tmax_f", "source"])

    df = pd.DataFrame([
        {"station": station, "date": pd.Timestamp(d),
         "forecast_tmax_f": v, "source": "AFM"}
        for d, v in records.items()
    ])
    logger.info("AFM %s: %d forecasts parsed", station, len(df))
    return df


# ---------------------------------------------------------------------------
# Open-Meteo ERA5 fallback
# ---------------------------------------------------------------------------
@retry_request(max_attempts=3, backoff_base=2.0)
def _fetch_openmeteo_era5(lat: float, lon: float,
                          start_date: str, end_date: str) -> dict:
    params = {
        "latitude":         lat,
        "longitude":        lon,
        "start_date":       start_date,
        "end_date":         end_date,
        "daily":            "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "timezone":         "UTC",
    }
    resp = requests.get(OPENMETEO_URL, params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()


def fetch_era5_fallback(station: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch ERA5 daily max temp from Open-Meteo as model forecast proxy.
    Returns DataFrame: [station, date, forecast_tmax_f, source]
    """
    coords = STATION_COORDS.get(station)
    if not coords:
        logger.warning("No coordinates for %s — skipping ERA5 fallback", station)
        return pd.DataFrame(columns=["station", "date", "forecast_tmax_f", "source"])

    lat, lon = coords
    logger.info("Fetching ERA5 for %s (%s → %s)...", station, start_date, end_date)

    # Open-Meteo archive has a max range — chunk by year to be safe
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date, "%Y-%m-%d")

    all_rows = []
    current = start_dt
    while current <= end_dt:
        chunk_end = min(datetime(current.year, 12, 31), end_dt)
        try:
            data = _fetch_openmeteo_era5(
                lat, lon,
                current.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d"),
            )
            dates  = data.get("daily", {}).get("time", [])
            temps  = data.get("daily", {}).get("temperature_2m_max", [])
            for d, t in zip(dates, temps):
                if t is not None:
                    all_rows.append({
                        "station": station,
                        "date": pd.Timestamp(d),
                        "forecast_tmax_f": float(t),
                        "source": "ERA5",
                    })
        except Exception as e:
            logger.warning("ERA5 fetch failed %s year %d: %s",
                           station, current.year, e)

        current = datetime(current.year + 1, 1, 1)
        time.sleep(0.5)

    if not all_rows:
        return pd.DataFrame(columns=["station", "date", "forecast_tmax_f", "source"])

    df = pd.DataFrame(all_rows)
    logger.info("ERA5 %s: %d records", station, len(df))
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def build_model_forecast_archive() -> None:
    os.makedirs(LOGS_DIR, exist_ok=True)
    os.makedirs(RAW_DIR, exist_ok=True)

    start_year = int(START_DATE[:4])
    end_year   = int(END_DATE[:4])
    all_dates  = pd.date_range(START_DATE, END_DATE, freq="D")

    all_dfs    = []
    missing_log = []

    for station in tqdm(STATIONS, desc="Building forecast archive"):
        logger.info("Processing forecast archive: %s", station)

        # --- Primary: IEM AFM ---
        afm_df = fetch_afm_forecasts(station, START_DATE, END_DATE)

        # Determine AFM coverage
        if len(afm_df) > 0:
            afm_df["date"] = pd.to_datetime(afm_df["date"])
            afm_dates = set(afm_df["date"].dt.date.tolist())
        else:
            afm_dates = set()

        gap_pct = (1 - len(afm_dates) / len(all_dates)) * 100
        logger.info("%s: AFM coverage %.1f%% (%d gaps)",
                    station, 100 - gap_pct, len(all_dates) - len(afm_dates))

        # --- Fallback: ERA5 for gaps ---
        if gap_pct > 2:
            logger.info("%s: fetching ERA5 fallback for gaps...", station)
            era5_df = fetch_era5_fallback(station, START_DATE, END_DATE)

            if len(era5_df) > 0:
                era5_df["date"] = pd.to_datetime(era5_df["date"])
                # Only use ERA5 where AFM is missing
                era5_df = era5_df[~era5_df["date"].dt.date.isin(afm_dates)]
                combined = pd.concat([afm_df, era5_df], ignore_index=True)
            else:
                combined = afm_df
        else:
            combined = afm_df

        combined = combined.sort_values("date").drop_duplicates(
            subset=["date"], keep="last"
        )
        all_dfs.append(combined)

        # Log remaining gaps
        if len(combined) > 0:
            combined["date"] = pd.to_datetime(combined["date"])
            final_dates = set(combined["date"].dt.date.tolist())
        else:
            final_dates = set()

        still_missing = [d for d in all_dates if d.date() not in final_dates]
        for d in still_missing:
            missing_log.append({"station": station, "date": d.date()})

        logger.info("%s: final coverage %d/%d days",
                    station, len(combined), len(all_dates))

    result = pd.concat(all_dfs, ignore_index=True)
    result["date"] = pd.to_datetime(result["date"])
    result.to_parquet(FCST_PARQUET, index=False)
    logger.info("Saved model_fcst.parquet: %d rows", len(result))

    # Source breakdown
    if len(result) > 0:
        src_counts = result["source"].value_counts()
        for src, cnt in src_counts.items():
            logger.info("  Source %-10s: %d rows (%.1f%%)",
                        src, cnt, cnt / len(result) * 100)

    if missing_log:
        missing_df = pd.DataFrame(missing_log)
        missing_path = os.path.join(LOGS_DIR, "fcst_missing.csv")
        missing_df.to_csv(missing_path, index=False)
        logger.warning("Missing forecasts: %d rows → %s",
                       len(missing_df), missing_path)


if __name__ == "__main__":
    build_model_forecast_archive()
