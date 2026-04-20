"""
Append recent observed tmax (last N days) for all stations into model_fcst.parquet.

Uses NOAA CDO GHCND API (same source as build_obs_database.py).
Observed actuals are the ground truth for past dates — a much better fallback
than stale model output from months ago.

Run any time model_fcst.parquet is stale (e.g. last row is from Dec 2024).
"""
import os, sys, time
from datetime import date, timedelta

import requests
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    STATIONS, GHCND_IDS, NOAA_CDO_TOKEN, FCST_PARQUET,
    settlement_station,
)
from utils.logging_config import setup_logging

logger = setup_logging("update_recent_forecasts")

CDO_URL      = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
LOOKBACK_DAYS = 90


def fetch_cdo_recent(station: str, start: date, end: date) -> pd.DataFrame:
    """
    Fetch observed TMAX from NOAA CDO for the given date range.
    Returns DataFrame with columns: station, date, forecast_tmax_f, model_source
    """
    settle = settlement_station(station)
    ghcnd_id = GHCND_IDS.get(settle)
    if not ghcnd_id:
        logger.warning("%s — no GHCND ID for settlement station %s, skipping", station, settle)
        return pd.DataFrame()

    headers = {"token": NOAA_CDO_TOKEN}
    params = {
        "datasetid":  "GHCND",
        "stationid":  f"GHCND:{ghcnd_id}",
        "datatypeid": "TMAX",
        "startdate":  start.isoformat(),
        "enddate":    end.isoformat(),
        "units":      "standard",
        "limit":      1000,
        "offset":     1,
    }

    rows = []
    try:
        for attempt in range(3):
            resp = requests.get(CDO_URL, headers=headers, params=params, timeout=60)
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning("%s CDO rate limited — waiting %ds", station, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        else:
            logger.error("%s CDO rate limited after 3 attempts", station)
            return pd.DataFrame()

        data = resp.json()
        for rec in data.get("results", []):
            if rec.get("datatype") != "TMAX":
                continue
            # units=standard → CDO returns whole-degree Fahrenheit directly
            tmax_f = float(rec["value"])
            rows.append({
                "station":         station,
                "date":            rec["date"][:10],
                "forecast_tmax_f": round(tmax_f, 1),
                "model_source":    "CDO_OBS",
            })

        logger.info("%s CDO: %d days fetched (%s→%s)", station, len(rows), start, end)
        time.sleep(0.5)  # CDO free-tier pacing

    except Exception as exc:
        logger.error("%s CDO fetch failed: %s", station, exc)

    return pd.DataFrame(rows)


def main():
    end   = date.today() - timedelta(days=1)   # yesterday (today not yet settled)
    start = end - timedelta(days=LOOKBACK_DAYS)

    # Load existing parquet
    if os.path.exists(FCST_PARQUET):
        existing = pd.read_parquet(FCST_PARQUET)
        existing["date"] = pd.to_datetime(existing["date"])
        if "source" in existing.columns and "model_source" not in existing.columns:
            existing = existing.rename(columns={"source": "model_source"})
        logger.info("Loaded existing parquet: %d rows", len(existing))
    else:
        existing = pd.DataFrame()
        logger.info("No existing parquet — creating fresh")

    new_dfs = []
    for station in STATIONS:
        df = fetch_cdo_recent(station, start, end)
        if len(df) > 0:
            new_dfs.append(df)

    if not new_dfs:
        logger.error("No data fetched — aborting")
        return

    new_data = pd.concat(new_dfs, ignore_index=True)
    new_data["date"] = pd.to_datetime(new_data["date"])

    if len(existing) > 0:
        # Drop any existing CDO_OBS rows in the same date range (avoid dupes)
        mask = (
            (existing["model_source"] == "CDO_OBS") &
            (existing["date"] >= pd.Timestamp(start)) &
            (existing["date"] <= pd.Timestamp(end))
        )
        existing = existing[~mask]
        combined = pd.concat([existing, new_data], ignore_index=True)
    else:
        combined = new_data

    combined = combined.sort_values(["station", "date"]).reset_index(drop=True)
    combined.to_parquet(FCST_PARQUET, index=False)

    logger.info("Archive updated: %d total rows (+%d new CDO rows)", len(combined), len(new_data))

    # Latest date per station
    latest = combined.groupby("station")["date"].max().reset_index()
    latest.columns = ["station", "latest_date"]
    print("\nLatest date per station:")
    print(latest.sort_values("station").to_string(index=False))


if __name__ == "__main__":
    main()
