"""
Script 4: Build IEM archived NWS point forecast database.

Primary source: IEM AFOS AFM text archive (Area Forecast Matrix)
Fallback: IEM climodat_dd.py JSON

The AFM is a structured NWS product containing Day-1 high temperature
forecasts issued by local WFOs. These represent the forecaster-adjusted
forecast — the closest proxy to what Kalshi market participants see.

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
    STATIONS, WFO_MAP, START_DATE, END_DATE,
    FCST_PARQUET, LOGS_DIR, RAW_DIR,
)
from utils.retry import retry_request
from utils.logging_config import setup_logging

logger = setup_logging("build_model_forecast_archive")

AFOS_URL    = "https://mesonet.agron.iastate.edu/cgi-bin/afos/retrieve.py"
CLIMODAT_URL = "https://mesonet.agron.iastate.edu/json/climodat_dd.py"

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
# AFM fetch and parse
# ---------------------------------------------------------------------------
@retry_request(max_attempts=3, backoff_base=2.0)
def _fetch_afm_products(wfo: str, start_dt: str, end_dt: str) -> list[dict]:
    """
    Fetch AFM text products from IEM AFOS archive for a WFO.
    Returns list of {valid_time, text_content} dicts.
    """
    pil = f"AFM{wfo}"
    params = {
        "pil": pil,
        "fmt": "json",
        "sts": f"{start_dt}T00:00Z",
        "ets": f"{end_dt}T23:59Z",
        "limit": 500,
    }
    resp = requests.get(AFOS_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])


def _parse_afm_max_temp(text: str, station: str) -> float | None:
    """
    Parse Day-1 max temperature from AFM fixed-width text.

    AFM format example:
      CITY/AREA          12HR  MAX  MIN  ...
      NEW YORK (JFK)      ..    72   55  ...

    Returns Day-1 MAX in °F, or None if not parseable.
    """
    search_names = STATION_AFM_NAMES.get(station, [])

    lines = text.upper().split("\n")

    # Find the MAX row header to determine column position
    max_col = None
    header_line_idx = None
    for i, line in enumerate(lines):
        if re.search(r"\bMAX\b", line) and re.search(r"\bMIN\b", line):
            # Identify column position of MAX
            match = re.search(r"\bMAX\b", line)
            if match:
                max_col = match.start()
                header_line_idx = i
                break

    if max_col is None:
        return None

    # Search for the station row after the header
    for i in range(header_line_idx + 1, min(header_line_idx + 30, len(lines))):
        line = lines[i]
        line_upper = line.upper()
        if any(name in line_upper for name in search_names):
            # Extract number near max_col position
            # Scan a window of ±8 chars around max_col
            window_start = max(0, max_col - 4)
            window_end   = min(len(line), max_col + 10)
            window = line[window_start:window_end]
            nums = re.findall(r"\d{2,3}", window)
            if nums:
                try:
                    val = float(nums[0])
                    # Sanity check: temp should be between -20 and 130°F
                    if -20 <= val <= 130:
                        return val
                except ValueError:
                    pass

    return None


def _issue_time_to_valid_date(issue_time_str: str) -> date | None:
    """
    Convert AFM issue time string to the valid forecast date.
    AFMs issued before ~12Z refer to same-day high.
    AFMs issued after ~12Z refer to next-day high.
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
    Fetch and parse AFM archive for a station over the date range.
    Returns DataFrame: [station, date, forecast_tmax_f, source]
    """
    wfo = WFO_MAP.get(station)
    if not wfo:
        logger.warning("No WFO mapping for %s", station)
        return pd.DataFrame(columns=["station", "date", "forecast_tmax_f", "source"])

    # Chunk into 6-month windows to avoid IEM timeouts
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date, "%Y-%m-%d")

    records = {}  # date → forecast_tmax_f (keep latest issuance per date)
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

            # Keep latest successfully parsed issuance per day
            if valid_date not in records:
                records[valid_date] = tmax
            # (products are returned in chronological order; later ones overwrite)
            records[valid_date] = tmax

        current = chunk_end + timedelta(days=1)
        time.sleep(0.5)

    if not records:
        return pd.DataFrame(columns=["station", "date", "forecast_tmax_f", "source"])

    df = pd.DataFrame([
        {"station": station, "date": d, "forecast_tmax_f": v, "source": "AFM"}
        for d, v in records.items()
    ])
    df["date"] = pd.to_datetime(df["date"])
    logger.info("AFM %s: %d forecasts parsed", station, len(df))
    return df


# ---------------------------------------------------------------------------
# Climodat fallback
# ---------------------------------------------------------------------------
@retry_request(max_attempts=3, backoff_base=2.0)
def _fetch_climodat_year(station: str, year: int) -> dict:
    params = {"station": station, "year": year}
    resp = requests.get(CLIMODAT_URL, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def fetch_climodat_fallback(station: str, start_year: int, end_year: int) -> pd.DataFrame:
    """
    Fetch NWS forecast vs observed data from IEM climodat as fallback.
    Returns DataFrame: [station, date, forecast_tmax_f, source]
    """
    rows = []
    for year in range(start_year, end_year + 1):
        try:
            data = _fetch_climodat_year(station, year)
        except Exception as e:
            logger.warning("climodat %s year %d failed: %s", station, year, e)
            continue

        for rec in data.get("climatology", []):
            fcst = rec.get("high", None)
            if fcst is None:
                continue
            try:
                tmax_f = float(fcst)
                rows.append({
                    "station": station,
                    "date": rec.get("valid", ""),
                    "forecast_tmax_f": tmax_f,
                    "source": "CLIMODAT",
                })
            except (ValueError, TypeError):
                continue
        time.sleep(0.3)

    if not rows:
        return pd.DataFrame(columns=["station", "date", "forecast_tmax_f", "source"])

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    logger.info("climodat %s: %d forecasts", station, len(df))
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def build_model_forecast_archive() -> None:
    os.makedirs(LOGS_DIR, exist_ok=True)
    os.makedirs(RAW_DIR, exist_ok=True)

    start_year = int(START_DATE[:4])
    end_year   = int(END_DATE[:4])

    all_dfs = []
    missing_log = []

    for station in tqdm(STATIONS, desc="Building forecast archive"):
        logger.info("Processing forecast archive: %s", station)

        # Primary: AFM
        afm_df = fetch_afm_forecasts(station, START_DATE, END_DATE)

        # Fill gaps with climodat fallback
        afm_dates = set(afm_df["date"].dt.date.tolist()) if len(afm_df) > 0 else set()

        # Build expected date range
        all_dates = pd.date_range(START_DATE, END_DATE, freq="D")
        missing_dates = [d for d in all_dates if d.date() not in afm_dates]
        gap_pct = len(missing_dates) / len(all_dates) * 100
        logger.info("%s: AFM coverage %.1f%% (%d gaps)", station, 100 - gap_pct, len(missing_dates))

        if gap_pct > 5:
            logger.info("%s: fetching climodat fallback for gaps...", station)
            climo_df = fetch_climodat_fallback(station, start_year, end_year)
            # Only use climodat where AFM is missing
            climo_df = climo_df[~climo_df["date"].dt.date.isin(afm_dates)]
            combined = pd.concat([afm_df, climo_df], ignore_index=True)
        else:
            combined = afm_df

        combined = combined.sort_values("date").drop_duplicates(subset=["date"], keep="last")
        all_dfs.append(combined)

        # Log remaining gaps
        final_dates = set(combined["date"].dt.date.tolist())
        still_missing = [d for d in all_dates if d.date() not in final_dates]
        for d in still_missing:
            missing_log.append({"station": station, "date": d.date()})

        logger.info("%s: final coverage %d/%d days", station, len(combined), len(all_dates))

    result = pd.concat(all_dfs, ignore_index=True)
    result.to_parquet(FCST_PARQUET, index=False)
    logger.info("Saved model_fcst.parquet: %d rows", len(result))

    if missing_log:
        missing_df = pd.DataFrame(missing_log)
        missing_path = os.path.join(LOGS_DIR, "fcst_missing.csv")
        missing_df.to_csv(missing_path, index=False)
        logger.warning("Missing forecasts logged: %d rows → %s", len(missing_df), missing_path)


if __name__ == "__main__":
    build_model_forecast_archive()
