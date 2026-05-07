"""
utils/live_bias.py

Live HRRR bias calibration using lead-time-matched forecast vs. observed comparisons.

Strategy
--------
At run hour R with peak hour P, the forecast horizon is lead_h = P - R.
For each prior run at hour H (H < R), that run's fxx=lead_h forecast had a valid
time of H + lead_h.  If H + lead_h <= R, that valid time is in the past and an
observation exists.  Comparing forecast to obs at the same lead gives a bias
estimate that is representative of how the current run will perform at the same
forecast horizon.

Example: R=19z, peak=23z, lead_h=4
  12z run fxx=4 → valid 16z  obs 16z = 83°F  fcst = 80°F  err = +3°F (HRRR cold)
  13z run fxx=4 → valid 17z  obs 17z = 83°F  fcst = 81°F  err = +2°F
  14z run fxx=4 → valid 18z  obs 18z = 83°F  fcst = 82°F  err = +1°F
  15z run fxx=4 → valid 19z  obs 19z = 83°F  fcst = 83°F  err =  0°F
  bias = mean(+3, +2, +1, 0) = +1.5°F  →  add +1.5°F to current HRRR TMAX

Batch design
------------
compute_live_bias_batch() accepts all active stations at once and opens each
unique (prior_run_hour, fxx) GRIB file exactly once, extracting every station
that needs it in one pass.  Prior-run GRIBs are already cached by Herbie from
the scheduler's own hourly cycles.

Fallback
--------
Returns None per station when fewer than MIN_SAMPLES pairs are available.
Caller should fall back to config.HRRR_COLD_BIAS_F in that case.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from io import StringIO
from typing import Optional

import numpy as np
import pandas as pd
import requests

from config import HRRR_COLD_BIAS_F, KALSHI_SETTLEMENT_STATION

logger = logging.getLogger(__name__)

IEM_1MIN_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py"
MIN_SAMPLES  = 3    # pairs needed before applying live calibration
MAX_BACK_H   = 13   # look back up to 13h so 00z runs are reachable from any 12z+ cycle


def _iem_code(icao: str) -> str:
    """Convert 4-letter ICAO to IEM 3-letter station code (KDFW → DFW, KNYC → NYC)."""
    return icao[1:] if icao.startswith("K") and len(icao) == 4 else icao


def fetch_hourly_obs_utc(station: str, event_date: date) -> dict[int, float]:
    """
    Fetch IEM 1-min ASOS data in UTC for event_date.
    Returns {utc_hour: temp_f} — one obs per hour, closest to top-of-hour.
    Uses the Kalshi settlement station so it matches the HRRR grid point location.
    Returns {} on any failure.
    """
    settlement = KALSHI_SETTLEMENT_STATION.get(station, station)
    iem_code   = _iem_code(settlement)

    params = {
        "station": iem_code,
        "vars":    "tmpf",
        "year1":   event_date.year,  "month1": event_date.month,  "day1": event_date.day,
        "year2":   event_date.year,  "month2": event_date.month,  "day2": event_date.day,
        "tz":      "UTC",
        "format":  "onlycomma",
        "latlon":  "no",
        "missing": "M",
        "direct":  "no",
    }

    try:
        resp = requests.get(IEM_1MIN_URL, params=params, timeout=30)
        resp.raise_for_status()
        text = resp.text.strip()
        if not text or "station" not in text.lower():
            logger.debug("%s (%s): empty IEM 1-min response", station, iem_code)
            return {}

        df = pd.read_csv(StringIO(text), na_values=["M", ""], low_memory=False)
        if "valid" not in df.columns or "tmpf" not in df.columns:
            logger.debug("%s: unexpected IEM columns: %s", station, df.columns.tolist())
            return {}

        df["valid_utc"] = pd.to_datetime(df["valid"], errors="coerce")
        df["tmpf"]      = pd.to_numeric(df["tmpf"],  errors="coerce")
        df = df.dropna(subset=["valid_utc", "tmpf"])
        if df.empty:
            return {}

        # Per UTC hour: pick the obs closest to :00
        df["mins_from_top"] = df["valid_utc"].dt.minute + df["valid_utc"].dt.second / 60.0
        hourly: dict[int, float] = {}
        for hour, grp in df.groupby(df["valid_utc"].dt.hour):
            best = grp.loc[grp["mins_from_top"].idxmin()]
            hourly[int(hour)] = round(float(best["tmpf"]), 1)

        logger.debug("%s: %d hourly UTC obs", station, len(hourly))
        return hourly

    except Exception as exc:
        logger.warning("%s fetch_hourly_obs_utc: %s", station, exc)
        return {}


def _fetch_hrrr_points(
    run_time: datetime,
    fxx: int,
    station_coords: dict[str, tuple[float, float]],
) -> dict[str, Optional[float]]:
    """
    Open one HRRR T2m GRIB file and extract instantaneous T2m (°F) at each
    station's grid point.  Herbie uses its local cache if the file was already
    downloaded during the scheduler's own HRRR cycle for that run hour.

    Returns {station: temp_f | None}.  None if the file is unavailable or the
    station point cannot be extracted.
    """
    import warnings
    warnings.filterwarnings("ignore")
    from herbie import Herbie

    results: dict[str, Optional[float]] = {s: None for s in station_coords}
    try:
        H = Herbie(
            run_time.strftime("%Y-%m-%d %H:%M"),
            model="hrrr", product="sfc", fxx=fxx, verbose=False,
        )
        ds    = H.xarray("TMP:2 m above ground", remove_grib=True)
        lats  = ds.latitude.values
        lons  = ds.longitude.values

        for station, (lat, lon) in station_coords.items():
            lon_360 = lon + 360 if lon < 0 else lon
            dist    = np.sqrt((lats - lat) ** 2 + (lons - lon_360) ** 2)
            idx     = np.unravel_index(dist.argmin(), dist.shape)
            t2m_k   = float(ds["t2m"].values[idx])
            results[station] = round((t2m_k - 273.15) * 9 / 5 + 32, 2)

    except Exception as exc:
        logger.debug(
            "_fetch_hrrr_points %sz fxx=%d: %s",
            run_time.strftime("%H"), fxx, exc,
        )

    return results


def compute_live_bias_batch(
    stations: list[str],
    current_run_time: datetime,
    peaks_utc: dict[str, int],
    coords: dict[str, tuple[float, float]],
    event_date: date,
) -> dict[str, Optional[float]]:
    """
    Compute live HRRR bias for all active stations in one batched pass.

    Parameters
    ----------
    stations         : Kalshi station labels to calibrate
    current_run_time : the HRRR run we are about to use for trading
    peaks_utc        : {station: peak_utc_hour} for event_date
    coords           : {station: (lat, lon)} at settlement station location
    event_date       : the trading date (today)

    Returns
    -------
    {station: bias_f | None}
      bias_f  — correction to ADD to raw HRRR TMAX (positive = HRRR ran cold)
      None    — fewer than MIN_SAMPLES pairs; use config.HRRR_COLD_BIAS_F fallback
    """
    run_hour   = current_run_time.hour
    start_hour = max(0, run_hour - MAX_BACK_H)

    # ── Lead per station ──────────────────────────────────────────────────────
    lead_by: dict[str, int] = {}
    for s in stations:
        lead = peaks_utc.get(s, 0) - run_hour
        if lead > 0:
            lead_by[s] = lead

    if not lead_by:
        logger.info("[LiveBias] All stations at or past peak — no calibration needed")
        return {s: None for s in stations}

    # ── Hourly obs: one HTTP request per station ──────────────────────────────
    obs_by: dict[str, dict[int, float]] = {}
    for s in lead_by:
        obs_by[s] = fetch_hourly_obs_utc(s, event_date)

    # ── Map (prior_run_hour, fxx) → stations that need it ────────────────────
    # A station needs (H, lead_h) when:
    #   • H + lead_h <= run_hour   (valid time is in the past — obs exists)
    #   • H + lead_h < 24          (valid time is same calendar day UTC)
    #   • obs exists for valid hour
    needed: dict[tuple[int, int], list[str]] = {}
    for s, lead_h in lead_by.items():
        if not obs_by.get(s):
            continue
        for prior_h in range(start_hour, run_hour):
            valid_h = prior_h + lead_h
            if valid_h > run_hour or valid_h >= 24:
                continue
            if valid_h not in obs_by[s]:
                continue
            needed.setdefault((prior_h, lead_h), []).append(s)

    if not needed:
        logger.info("[LiveBias] No usable calibration pairs (run=%dz, leads=%s)",
                    run_hour, {s: v for s, v in lead_by.items()})
        return {s: None for s in stations}

    # ── Fetch HRRR: one GRIB open per unique (run_hour, fxx) ─────────────────
    errors_by: dict[str, list[float]] = {s: [] for s in stations}

    for (prior_h, fxx), station_list in sorted(needed.items()):
        prior_run_dt = current_run_time.replace(
            hour=prior_h, minute=0, second=0, microsecond=0,
        )
        point_temps = _fetch_hrrr_points(prior_run_dt, fxx,
                                          {s: coords[s] for s in station_list})
        valid_h = prior_h + fxx
        for s, fcst_f in point_temps.items():
            if fcst_f is None:
                continue
            obs_f = obs_by[s].get(valid_h)
            if obs_f is None:
                continue
            err = obs_f - fcst_f
            errors_by[s].append(err)
            logger.debug(
                "[LiveBias] %s  %dz→%dz fxx=%d  fcst=%.1f°F  obs=%.1f°F  err=%+.1f°F",
                s, prior_h, valid_h, fxx, fcst_f, obs_f, err,
            )

    # ── Summarise ─────────────────────────────────────────────────────────────
    result: dict[str, Optional[float]] = {}
    for s in stations:
        errs = errors_by[s]
        n    = len(errs)
        if n < MIN_SAMPLES:
            logger.info(
                "[LiveBias] %s: %d/%d pairs — fallback to fixed %+.1f°F",
                s, n, MIN_SAMPLES, HRRR_COLD_BIAS_F,
            )
            result[s] = None
        else:
            bias = round(sum(errs) / n, 2)
            logger.info(
                "[LiveBias] %s: bias=%+.2f°F  n=%d  fixed_was=%+.1f°F",
                s, bias, n, HRRR_COLD_BIAS_F,
            )
            result[s] = bias

    return result
