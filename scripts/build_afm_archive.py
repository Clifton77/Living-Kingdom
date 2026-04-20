"""
Fetch historical IEM AFM (NWS human forecast) and GFS-MOS tmax for all stations
and append to model_fcst.parquet so build_bias_table.py can compute IEM_AFM biases.

Uses fmt=text (fmt=json was deprecated Apr 2026) with 30-day chunks to avoid
month-boundary ambiguity when reconstructing issue timestamps from WMO headers.

Run once, then re-run build_bias_table.py.

Usage:
    python scripts/build_afm_archive.py [--start 2022-01-01] [--end 2024-12-31]
"""
import os, sys, re, time, argparse
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import STATIONS, WFO_MAP, FCST_PARQUET, LOGS_DIR
from scripts.build_model_forecast_archive import (
    STATION_AFM_NAMES, STATION_MOS_IDS,
    _parse_afm_max_temp, _parse_mos_max_temp, _issue_time_to_valid_date,
)
from utils.logging_config import setup_logging

logger = setup_logging("build_afm_archive")

AFOS_URL   = "https://mesonet.agron.iastate.edu/cgi-bin/afos/retrieve.py"
CHUNK_DAYS  = 30    # small chunks → unambiguous day→month mapping in WMO headers
CHUNK_SLEEP = 2.0   # seconds between chunks — IEM rate limit is aggressive


# ---------------------------------------------------------------------------
# Text-format fetch (works after Apr 2026 IEM changes)
# ---------------------------------------------------------------------------

def _fetch_text_products(pil: str, start: date, end: date) -> list[dict]:
    """
    Fetch AFOS products as raw text, parse into list of {data, utc_valid}.
    Uses 30-day windows so WMO header day numbers map unambiguously to dates.
    """
    params = {
        "pil":   pil,
        "fmt":   "text",
        "sdate": f"{start.isoformat()}T00:00Z",
        "edate": f"{end.isoformat()}T23:59Z",
        "limit": 500,
    }
    resp = requests.get(AFOS_URL, params=params, timeout=60)
    resp.raise_for_status()
    raw = resp.text

    if "ERROR:" in raw or not raw.strip():
        return []

    products = []
    for block in raw.split("\x01"):
        block = block.strip()
        if not block:
            continue
        # WMO header: e.g. "FOUS51 KOKX 191820" → day=19 hh=18 mm=20
        m = re.search(
            r"^[A-Z]{4}\d{2}\s+[A-Z]{4}\s+(\d{2})(\d{2})(\d{2})",
            block, re.MULTILINE,
        )
        if not m:
            continue
        day, hh, mm = int(m.group(1)), int(m.group(2)), int(m.group(3))

        # Determine year+month: the day must fall within [start, end+1]
        # Try start's month first, then end's month (handles month crossings in chunk)
        issue_dt = None
        for ref in (start, end):
            try:
                dt = datetime(ref.year, ref.month, day, hh, mm, tzinfo=timezone.utc)
                if start <= dt.date() <= end + timedelta(days=1):
                    issue_dt = dt
                    break
            except ValueError:
                continue

        if issue_dt is None:
            continue

        products.append({"data": block, "utc_valid": issue_dt.isoformat()})

    return products


# ---------------------------------------------------------------------------
# Station-level fetch
# ---------------------------------------------------------------------------

def _fetch_station_afm(station: str, start: date, end: date) -> dict[date, float]:
    wfo = WFO_MAP.get(station)
    if not wfo:
        logger.warning("%s — no WFO mapping, skipping AFM", station)
        return {}

    pil = f"AFM{wfo}"
    records: dict[date, float] = {}
    current = start

    while current <= end:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS - 1), end)
        try:
            products = _fetch_text_products(pil, current, chunk_end)
        except Exception as e:
            logger.warning("AFM %s %s→%s: %s", station, current, chunk_end, e)
            current = chunk_end + timedelta(days=1)
            time.sleep(1)
            continue

        for prod in products:
            valid_date = _issue_time_to_valid_date(prod["utc_valid"])
            if valid_date is None:
                continue
            tmax = _parse_afm_max_temp(prod["data"], station)
            if tmax is None:
                continue
            # Keep the most recently issued product for each valid date
            if valid_date not in records:
                records[valid_date] = tmax

        current = chunk_end + timedelta(days=1)
        time.sleep(CHUNK_SLEEP)

    logger.info("AFM %s: %d forecasts (%s→%s)", station, len(records), start, end)
    return records


def _fetch_station_mos(station: str, start: date, end: date) -> dict[date, float]:
    wfo = WFO_MAP.get(station)
    if not wfo:
        logger.warning("%s — no WFO mapping, skipping MOS", station)
        return {}
    pil = f"MAV{wfo}"   # e.g. WFO=OKX → MAVOKX (area product containing KJFK, KLGA, etc.)

    records: dict[date, float] = {}
    current = start

    while current <= end:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS - 1), end)
        try:
            products = _fetch_text_products(pil, current, chunk_end)
        except Exception as e:
            logger.warning("MOS %s %s→%s: %s", station, current, chunk_end, e)
            current = chunk_end + timedelta(days=1)
            time.sleep(1)
            continue

        for prod in products:
            valid_date = _issue_time_to_valid_date(prod["utc_valid"])
            if valid_date is None:
                continue
            tmax = _parse_mos_max_temp(prod["data"], station)
            if tmax is None:
                continue
            if valid_date not in records:
                records[valid_date] = tmax

        current = chunk_end + timedelta(days=1)
        time.sleep(CHUNK_SLEEP)

    logger.info("MOS %s: %d forecasts (%s→%s)", station, len(records), start, end)
    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2022-01-01")
    parser.add_argument("--end",   default=str(date.today() - timedelta(days=1)))
    parser.add_argument("--mos",   action="store_true", default=True,
                        help="Also fetch GFS-MOS (default: yes)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end   = date.fromisoformat(args.end)
    logger.info("Fetching AFM+MOS for %d stations | %s → %s", len(STATIONS), start, end)

    new_rows = []

    for i, station in enumerate(STATIONS):
        logger.info("[%d/%d] %s — fetching AFM...", i + 1, len(STATIONS), station)
        afm_records = _fetch_station_afm(station, start, end)
        for d, tmax in afm_records.items():
            new_rows.append({
                "station":         station,
                "date":            pd.Timestamp(d),
                "forecast_tmax_f": float(tmax),
                "model_source":    "IEM_AFM",
            })

        time.sleep(2)   # extra pause between stations

    if not new_rows:
        logger.error("No data fetched — aborting")
        return

    new_df = pd.DataFrame(new_rows)
    logger.info("Fetched %d total rows (AFM+MOS)", len(new_df))

    # Load existing parquet and drop any IEM_AFM/GFS_MOS rows in the date range
    # (so re-running this script doesn't accumulate duplicates)
    if os.path.exists(FCST_PARQUET):
        existing = pd.read_parquet(FCST_PARQUET)
        existing["date"] = pd.to_datetime(existing["date"])
        src_col = "model_source" if "model_source" in existing.columns else "source"
        mask = (
            existing[src_col].isin(["IEM_AFM", "GFS_MOS"]) &
            (existing["date"] >= pd.Timestamp(start)) &
            (existing["date"] <= pd.Timestamp(end))
        )
        existing = existing[~mask]
        logger.info("Existing parquet: %d rows after removing overlap", len(existing))
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.sort_values(["station", "date", "model_source"]).reset_index(drop=True)
    combined.to_parquet(FCST_PARQUET, index=False)
    logger.info("Saved model_fcst.parquet: %d total rows", len(combined))

    # Summary
    src_counts = combined["model_source"].value_counts()
    print("\nRows by model_source:")
    print(src_counts.to_string())

    afm_only = new_df[new_df["model_source"] == "IEM_AFM"]
    mos_only = new_df[new_df["model_source"] == "GFS_MOS"]
    print(f"\nNew IEM_AFM rows: {len(afm_only)}")
    print(f"New GFS_MOS rows: {len(mos_only)}")
    print("\nNext step: python scripts/build_bias_table.py")


if __name__ == "__main__":
    main()
