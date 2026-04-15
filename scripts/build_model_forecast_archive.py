"""
Script 4: Build NWS forecast archive.

Two model sources are tracked:
  IEM_AFM  — IEM AFM (Area Forecast Matrix) text archive: human NWS forecaster output
  GFS_MOS  — IEM MAV (GFS Model Output Statistics) text archive: pure model guidance

When AFM and GFS-MOS diverge, the forecaster has applied local knowledge (sea breeze,
terrain, urban heat) that the model missed — that divergence is itself a trading signal.

Fallback: Open-Meteo ERA5 archive (source=ERA5) fills gaps in both model sources.

Output: data/model_fcst.parquet
  {station, date, forecast_tmax_f, model_source}
  model_source values: IEM_AFM | GFS_MOS | ERA5
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
    settlement_station,
)
from utils.retry import retry_request
from utils.logging_config import setup_logging

logger = setup_logging("build_model_forecast_archive")

AFOS_URL      = "https://mesonet.agron.iastate.edu/cgi-bin/afos/retrieve.py"
OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/archive"

# Station names as they appear in AFM text (partial match sufficient)
STATION_AFM_NAMES = {
    # Original 8
    "KJFK": ["NEW YORK", "JFK", "KENNEDY", "CENTRAL PARK"],
    "KORD": ["CHICAGO", "MIDWAY", "MDW"],
    "KMIA": ["MIAMI"],
    "KDFW": ["DALLAS", "FORT WORTH", "DFW"],
    "KLAX": ["LOS ANGELES", "LAX"],
    "KATL": ["ATLANTA"],
    "KDEN": ["DENVER"],
    "KHOU": ["HOUSTON", "HOBBY"],
    # New 12
    "KAUS": ["AUSTIN", "BERGSTROM"],
    "KPHL": ["PHILADELPHIA", "PHL"],
    "KBOS": ["BOSTON", "LOGAN"],
    "KDCA": ["WASHINGTON", "REAGAN", "NATIONAL", "DCA"],
    "KLAS": ["LAS VEGAS"],
    "KMSP": ["MINNEAPOLIS", "ST PAUL", "MSP"],
    "KMSY": ["NEW ORLEANS", "MOISANT", "MSY"],
    "KOKC": ["OKLAHOMA CITY", "WILL ROGERS", "OKC"],
    "KPHX": ["PHOENIX", "SKY HARBOR"],
    "KSAT": ["SAN ANTONIO", "SAT"],
    "KSEA": ["SEATTLE", "SEA-TAC", "SEATAC"],
    "KSFO": ["SAN FRANCISCO", "SFO"],
}

# ICAO codes to search for in GFS-MOS (MAV) text, in priority order.
# MAV uses 4-letter ICAO identifiers for station sections.
# KNYC (Central Park) rarely appears in MAV; fall back to nearby airports.
STATION_MOS_IDS: dict[str, list[str]] = {
    "KJFK": ["KNYC", "KJFK", "KLGA"],    # settlement=KNYC; try airports as fallback
    "KORD": ["KMDW", "KORD"],             # settlement=KMDW
    "KMIA": ["KMIA"],
    "KDFW": ["KDFW"],
    "KLAX": ["KLAX"],
    "KATL": ["KATL"],
    "KDEN": ["KDEN"],
    "KHOU": ["KHOU"],
    "KAUS": ["KAUS"],
    "KPHL": ["KPHL"],
    "KBOS": ["KBOS"],
    "KDCA": ["KDCA", "KIAD"],             # try Reagan then Dulles
    "KLAS": ["KLAS"],
    "KMSP": ["KMSP"],
    "KMSY": ["KMSY"],
    "KOKC": ["KOKC"],
    "KPHX": ["KPHX"],
    "KSAT": ["KSAT"],
    "KSEA": ["KSEA"],
    "KSFO": ["KSFO"],
}


# ---------------------------------------------------------------------------
# IEM AFOS fetch — shared by AFM and MAV (GFS-MOS)
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


# ---------------------------------------------------------------------------
# GFS-MOS (MAV) fetch and parse
# ---------------------------------------------------------------------------

@retry_request(max_attempts=3, backoff_base=2.0)
def _fetch_mos_products(wfo: str, start_str: str, end_str: str) -> list[dict]:
    """
    Fetch GFS-MOS (MAV) text products from IEM AFOS archive.
    pil = MAV{WFO}, same endpoint and params as AFM.
    """
    pil = f"MAV{wfo}"
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


def _parse_mos_max_temp(text: str, station: str) -> float | None:
    """
    Parse Day-1 max temperature from GFS-MOS (MAV) text.

    MAV format has station sections identified by 4-letter ICAO codes.
    Each section contains a MAX/MIN line with alternating max and min temps.
    The first integer in the MAX/MIN line is the Day-1 afternoon high.

    Returns °F or None if not parseable.
    """
    target_ids = STATION_MOS_IDS.get(station, [station])
    lines = text.upper().split("\n")

    station_line_idx = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        for target in target_ids:
            # Station section lines are just the 4-letter ICAO, optionally followed by spaces
            if stripped == target or stripped.startswith(target + " ") or stripped.startswith(target + "\t"):
                station_line_idx = i
                break
        if station_line_idx is not None:
            break

    if station_line_idx is None:
        return None

    # Scan up to 35 lines after the station identifier for the MAX/MIN row
    for i in range(station_line_idx + 1, min(station_line_idx + 35, len(lines))):
        line = lines[i]
        if "MAX/MIN" in line:
            nums = re.findall(r"\d{2,3}", line)
            if nums:
                try:
                    val = float(nums[0])
                    if -20 <= val <= 130:
                        return val
                except ValueError:
                    pass
            return None  # found MAX/MIN line but couldn't parse it
        # Stop if we hit another station section (4-letter K-code on its own line)
        stripped = line.strip()
        if len(stripped) == 4 and stripped.isalpha() and stripped.startswith("K"):
            break

    return None


def fetch_gfs_mos_forecasts(station: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch and parse GFS-MOS (MAV) archive for a station.
    Returns DataFrame: [station, date, forecast_tmax_f, model_source]
    """
    wfo = WFO_MAP.get(station)
    if not wfo:
        logger.warning("No WFO mapping for %s — skipping GFS-MOS", station)
        return pd.DataFrame(columns=["station", "date", "forecast_tmax_f", "model_source"])

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date, "%Y-%m-%d")

    records = {}
    current = start_dt

    while current < end_dt:
        chunk_end = min(current + timedelta(days=180), end_dt)
        chunk_start_str = current.strftime("%Y-%m-%d")
        chunk_end_str   = chunk_end.strftime("%Y-%m-%d")

        try:
            products = _fetch_mos_products(wfo, chunk_start_str, chunk_end_str)
        except Exception as e:
            logger.warning("MAV fetch failed %s %s->%s: %s",
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
            tmax = _parse_mos_max_temp(text, station)
            if tmax is None:
                continue
            records[valid_date] = tmax

        current = chunk_end + timedelta(days=1)
        time.sleep(0.5)

    if not records:
        return pd.DataFrame(columns=["station", "date", "forecast_tmax_f", "model_source"])

    df = pd.DataFrame([
        {"station": station, "date": pd.Timestamp(d),
         "forecast_tmax_f": v, "model_source": "GFS_MOS"}
        for d, v in records.items()
    ])
    logger.info("GFS-MOS %s: %d forecasts parsed", station, len(df))
    return df


def fetch_afm_forecasts(station: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch and parse AFM archive for a station.
    Returns DataFrame: [station, date, forecast_tmax_f, model_source]
    model_source = 'IEM_AFM'
    """
    wfo = WFO_MAP.get(station)
    if not wfo:
        logger.warning("No WFO mapping for %s", station)
        return pd.DataFrame(columns=["station", "date", "forecast_tmax_f", "model_source"])

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
            logger.warning("AFM fetch failed %s %s->%s: %s",
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
        return pd.DataFrame(columns=["station", "date", "forecast_tmax_f", "model_source"])

    df = pd.DataFrame([
        {"station": station, "date": pd.Timestamp(d),
         "forecast_tmax_f": v, "model_source": "IEM_AFM"}
        for d, v in records.items()
    ])
    logger.info("IEM-AFM %s: %d forecasts parsed", station, len(df))
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
    Uses the NWS settlement station's coordinates (e.g. KNYC for KJFK,
    KMDW for KORD) so ERA5 reflects the correct location.
    Returns DataFrame: [station, date, forecast_tmax_f, source]
    """
    settle = settlement_station(station)
    coords = STATION_COORDS.get(settle)
    if not coords:
        logger.warning("No coordinates for %s — skipping ERA5 fallback", station)
        return pd.DataFrame(columns=["station", "date", "forecast_tmax_f", "source"])

    lat, lon = coords
    logger.info("Fetching ERA5 for %s (settle=%s, %s → %s)...", station, settle, start_date, end_date)

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
                        "model_source": "ERA5",
                    })
        except Exception as e:
            logger.warning("ERA5 fetch failed %s year %d: %s",
                           station, current.year, e)

        current = datetime(current.year + 1, 1, 1)
        time.sleep(0.5)

    if not all_rows:
        return pd.DataFrame(columns=["station", "date", "forecast_tmax_f", "model_source"])

    df = pd.DataFrame(all_rows)
    logger.info("ERA5 %s: %d records", station, len(df))
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def _fill_era5_gaps(
    station: str,
    primary_df: pd.DataFrame,
    primary_dates: set,
    all_dates: pd.DatetimeIndex,
) -> pd.DataFrame:
    """
    Fetch ERA5 fallback and fill only dates missing from primary_df.
    Returns combined DataFrame with model_source='ERA5' for gap rows.
    """
    gap_pct = (1 - len(primary_dates) / len(all_dates)) * 100
    if gap_pct <= 2:
        return primary_df

    logger.info("  fetching ERA5 gap-fill (%d gaps, %.1f%%)...",
                len(all_dates) - len(primary_dates), gap_pct)
    era5_df = fetch_era5_fallback(station, START_DATE, END_DATE)
    if len(era5_df) > 0:
        era5_df["date"] = pd.to_datetime(era5_df["date"])
        era5_df = era5_df[~era5_df["date"].dt.date.isin(primary_dates)]
        return pd.concat([primary_df, era5_df], ignore_index=True)
    return primary_df


def build_model_forecast_archive() -> None:
    os.makedirs(LOGS_DIR, exist_ok=True)
    os.makedirs(RAW_DIR, exist_ok=True)

    all_dates   = pd.date_range(START_DATE, END_DATE, freq="D")
    all_dfs     = []
    missing_log = []

    for station in tqdm(STATIONS, desc="Building forecast archive"):
        logger.info("Processing forecast archive: %s", station)

        # ── IEM AFM (human NWS forecast) ────────────────────────────────────
        afm_df = fetch_afm_forecasts(station, START_DATE, END_DATE)
        if len(afm_df) > 0:
            afm_df["date"] = pd.to_datetime(afm_df["date"])
            afm_dates = set(afm_df["date"].dt.date.tolist())
        else:
            afm_dates = set()
        logger.info("%s IEM-AFM: %d/%d days (%.1f%% coverage)",
                    station, len(afm_dates), len(all_dates),
                    100 * len(afm_dates) / len(all_dates))

        afm_df = _fill_era5_gaps(station, afm_df, afm_dates, all_dates)
        afm_df = afm_df.sort_values("date").drop_duplicates(subset=["date"], keep="last")

        # ── GFS-MOS (pure model guidance) ───────────────────────────────────
        mos_df = fetch_gfs_mos_forecasts(station, START_DATE, END_DATE)
        if len(mos_df) > 0:
            mos_df["date"] = pd.to_datetime(mos_df["date"])
            mos_dates = set(mos_df["date"].dt.date.tolist())
        else:
            mos_dates = set()
        logger.info("%s GFS-MOS: %d/%d days (%.1f%% coverage)",
                    station, len(mos_dates), len(all_dates),
                    100 * len(mos_dates) / len(all_dates))

        mos_df = _fill_era5_gaps(station, mos_df, mos_dates, all_dates)
        mos_df = mos_df.sort_values("date").drop_duplicates(subset=["date"], keep="last")

        all_dfs.extend([afm_df, mos_df])

        # Log remaining gaps per model source
        for label, df in [("IEM_AFM", afm_df), ("GFS_MOS", mos_df)]:
            if len(df) > 0:
                final_dates = set(df["date"].dt.date.tolist())
            else:
                final_dates = set()
            still_missing = [d for d in all_dates if d.date() not in final_dates]
            for d in still_missing:
                missing_log.append({"station": station, "model_source": label, "date": d.date()})
            logger.info("%s %s: final %d/%d days",
                        station, label, len(final_dates), len(all_dates))

    result = pd.concat(all_dfs, ignore_index=True)
    result["date"] = pd.to_datetime(result["date"])
    result.to_parquet(FCST_PARQUET, index=False)
    logger.info("Saved model_fcst.parquet: %d rows", len(result))

    # Source breakdown
    if len(result) > 0:
        src_counts = result["model_source"].value_counts()
        for src, cnt in src_counts.items():
            logger.info("  model_source %-10s: %d rows (%.1f%%)",
                        src, cnt, cnt / len(result) * 100)

    if missing_log:
        missing_df = pd.DataFrame(missing_log)
        missing_path = os.path.join(LOGS_DIR, "fcst_missing.csv")
        missing_df.to_csv(missing_path, index=False)
        logger.warning("Missing forecasts: %d rows → %s",
                       len(missing_df), missing_path)


if __name__ == "__main__":
    build_model_forecast_archive()
