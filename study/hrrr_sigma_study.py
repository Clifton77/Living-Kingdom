#!/usr/bin/env python3
"""
study/hrrr_sigma_study.py

Validates Option A sigma estimates by pulling historical HRRR T2m forecasts
at two run times (12z = ~6h lead, 18z = ~2-3h lead) and comparing the
resulting TMAX forecasts to observed TMAX in obs_daily.parquet.

Outputs MAE, bias, sigma, and calibration check per lead time bin.
Run from the project root: python study/hrrr_sigma_study.py
"""

import sys
import warnings
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
sys.path.insert(0, ".")

from herbie import Herbie

# ---------------------------------------------------------------------------
# Settlement station coordinates
# KJFK → KNYC (Central Park), KORD → KMDW (Midway)
# ---------------------------------------------------------------------------
SETTLEMENT_COORDS = {
    "KATL": (33.6407, -84.4277),
    "KAUS": (30.1945, -97.6699),
    "KBOS": (42.3606, -71.0097),
    "KDCA": (38.8521, -77.0380),
    "KDEN": (39.8561, -104.6737),
    "KDFW": (32.8998, -97.0403),
    "KHOU": (29.6454, -95.2789),
    "KJFK": (40.7789, -73.9692),
    "KLAS": (36.0840, -115.1537),
    "KLAX": (33.9425, -118.4081),
    "KMIA": (25.7959, -80.2870),
    "KMSP": (44.8848, -93.2223),
    "KMSY": (29.9934, -90.2580),
    "KOKC": (35.3931, -97.6007),
    "KORD": (41.7868, -87.7522),
    "KPHL": (39.8721, -75.2411),
    "KPHX": (33.4373, -112.0078),
    "KSAT": (29.5337, -98.4698),
    "KSEA": (47.4502, -122.3088),
    "KSFO": (37.6213, -122.3790),
}

# 12z run: fxx 1-8 covers 13z-20z (afternoon peak for eastern/central stations)
# 18z run: fxx 1-6 covers 19z-00z (afternoon peak for western stations)
RUN_CONFIGS = {
    12: {"fxx_range": range(1, 9),  "label": "12z (~6h lead)"},
    18: {"fxx_range": range(1, 7),  "label": "18z (~2-3h lead)"},
}


def extract_point(ds, lat, lon):
    """Extract T2m at nearest grid point from a 2D HRRR xarray dataset."""
    lats = ds.latitude.values
    lons = ds.longitude.values
    lon_360 = lon + 360 if lon < 0 else lon  # HRRR uses 0-360 convention
    dist = np.sqrt((lats - lat) ** 2 + (lons - lon_360) ** 2)
    idx = np.unravel_index(dist.argmin(), dist.shape)
    t2m_k = float(ds["t2m"].values[idx])
    return (t2m_k - 273.15) * 9 / 5 + 32  # Kelvin → Fahrenheit


def fetch_run_tmax(run_dt_str, fxx_range):
    """
    Fetch HRRR T2m for a run across all forecast hours in fxx_range.
    Returns dict: station → TMAX forecast (°F), or None if fetch failed.
    """
    station_temps = {s: [] for s in SETTLEMENT_COORDS}

    for fxx in fxx_range:
        try:
            H = Herbie(run_dt_str, model="hrrr", product="sfc", fxx=fxx, verbose=False)
            ds = H.xarray("TMP:2 m above ground", remove_grib=True)
            for station, (lat, lon) in SETTLEMENT_COORDS.items():
                t2m_f = extract_point(ds, lat, lon)
                station_temps[station].append(t2m_f)
        except Exception as e:
            print(f"    fxx={fxx} failed: {e}")

    result = {}
    for station, temps in station_temps.items():
        if temps:
            result[station] = max(temps)
    return result


def run_study(days_back=14):
    # Load observed TMAX
    obs = pd.read_parquet("data/obs_daily.parquet")
    obs["date"] = pd.to_datetime(obs["date"]).dt.date
    end_date = date(2026, 5, 1)
    start_date = end_date - timedelta(days=days_back)
    obs = obs[(obs["date"] >= start_date) & (obs["date"] <= end_date)]
    obs_lookup = {(r.station, r.date): r.tmax_observed_f for r in obs.itertuples()}

    rows = []
    study_dates = pd.date_range(start_date, end_date, freq="D")

    for dt in study_dates:
        study_date = dt.date()
        for run_hour, cfg in RUN_CONFIGS.items():
            run_dt_str = f"{study_date.strftime('%Y-%m-%d')} {run_hour:02d}:00"
            print(f"Fetching HRRR {run_dt_str} ({cfg['label']})...")
            tmax_by_station = fetch_run_tmax(run_dt_str, cfg["fxx_range"])

            for station, hrrr_tmax in tmax_by_station.items():
                obs_tmax = obs_lookup.get((station, study_date))
                if obs_tmax is None:
                    continue
                rows.append({
                    "station":      station,
                    "date":         study_date,
                    "run_hour_utc": run_hour,
                    "lead_label":   cfg["label"],
                    "hrrr_tmax_f":  hrrr_tmax,
                    "obs_tmax_f":   obs_tmax,
                    "error_f":      hrrr_tmax - obs_tmax,
                })

    df = pd.DataFrame(rows)
    if df.empty:
        print("No results — check Herbie connectivity.")
        return df

    print("\n" + "=" * 55)
    print("HRRR TMAX Accuracy by Lead Time")
    print("=" * 55)

    summary = (
        df.groupby("lead_label")["error_f"]
        .agg(
            n="count",
            mae=lambda x: np.mean(np.abs(x)),
            bias="mean",
            sigma="std",
        )
        .round(2)
    )
    print(summary.to_string())

    print("\nCalibration check -- fraction of obs within +/-sigma (target ~68%):")
    for label, grp in df.groupby("lead_label"):
        sigma = grp["error_f"].std()
        within = (np.abs(grp["error_f"]) <= sigma).mean()
        print(f"  {label}: sigma={sigma:.2f}F  within +/-sigma={within:.1%}")

    print("\nPer-station sigma (12z run):")
    per_stn = (
        df[df["run_hour_utc"] == 12]
        .groupby("station")["error_f"]
        .agg(n="count", sigma="std", mae=lambda x: np.mean(np.abs(x)))
        .round(2)
        .sort_values("sigma", ascending=False)
    )
    print(per_stn.to_string())

    out_path = "study/data/hrrr_sigma_results.csv"
    df.to_csv(out_path, index=False)
    print(f"\nFull results saved to {out_path}")
    return df


if __name__ == "__main__":
    run_study(days_back=14)
