"""
Live ASOS data utilities for intraday running maximum tracking.

Primary path:  IEM 1-minute ASOS data   → high precision (~0.1°F)
Confluence:    METAR T-group parsing     → 0.1°C precision
Official obs:  METAR whole-degree temp   → 1°C precision (fallback)

The running max is used by the overshoot exit guard in risk.py:
    if running_max >= bucket_upper - OVERSHOOT_EXIT_BUFFER_F
    and local_hour < peak_heating_hour
    → exit to avoid riding the position back down

IEM 1-minute endpoint:
    https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py
    params: station, year1/month1/day1, year2/month2/day2, vars=tmpf, tz, format=onlycomma
"""

from __future__ import annotations

import re
import time
from datetime import date, datetime, timezone
from io import StringIO
from typing import Optional

import numpy as np
import pandas as pd
import requests

from utils.logging_config import setup_logging
from config import STATION_TIMEZONES

logger = setup_logging("asos_live")

IEM_1MIN_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py"
AVWX_METAR_URL = "https://aviationweather.gov/api/data/metar"

# ---------------------------------------------------------------------------
# IEM 1-minute running max
# ---------------------------------------------------------------------------

def fetch_1min_temps(station: str, event_date: date) -> pd.DataFrame | None:
    """
    Fetch IEM 1-minute ASOS temperatures for a station on a given date.

    Returns DataFrame with columns: [valid_local, tmpf]
    All times in station local time (so hour comparisons work directly).
    Returns None on failure.
    """
    tz = STATION_TIMEZONES[station]
    ds = event_date.strftime("%Y-%m-%d")

    # IEM 1-min uses 3-letter codes (e.g. JFK, not KJFK)
    iem_station = station[1:] if station.startswith("K") and len(station) == 4 else station

    params = {
        "station":  iem_station,
        "vars":     "tmpf",
        "year1":    event_date.year,
        "month1":   event_date.month,
        "day1":     event_date.day,
        "year2":    event_date.year,
        "month2":   event_date.month,
        "day2":     event_date.day,
        "tz":       tz,
        "format":   "onlycomma",
        "latlon":   "no",
        "missing":  "M",
        "direct":   "no",
    }

    for attempt in range(3):
        try:
            resp = requests.get(IEM_1MIN_URL, params=params, timeout=30)
            resp.raise_for_status()

            text = resp.text.strip()
            if not text or "station" not in text.lower():
                logger.debug("%s %s: empty 1-min response", station, ds)
                return None

            df = pd.read_csv(
                StringIO(text),
                na_values=["M", ""],
                low_memory=False,
            )

            if "valid" not in df.columns or "tmpf" not in df.columns:
                logger.debug("%s: unexpected 1-min columns: %s", station, df.columns.tolist())
                return None

            df["valid_local"] = pd.to_datetime(df["valid"], errors="coerce")
            df["tmpf"] = pd.to_numeric(df["tmpf"], errors="coerce")
            df = df.dropna(subset=["valid_local", "tmpf"])

            logger.debug("%s %s: fetched %d 1-min obs", station, ds, len(df))
            return df[["valid_local", "tmpf"]].copy()

        except Exception as exc:
            wait = 2 ** attempt
            logger.warning("%s: 1-min fetch failed (%s) — retrying in %ds", station, exc, wait)
            time.sleep(wait)

    logger.error("%s: all 1-min fetch attempts failed", station)
    return None


def get_running_max(station: str, event_date: date) -> float | None:
    """
    Return the highest temperature (°F) observed so far today at this station,
    using IEM 1-minute ASOS data.

    Returns None if data is unavailable.
    """
    df = fetch_1min_temps(station, event_date)
    if df is None or df.empty:
        logger.warning("%s: no 1-min data available for running max", station)
        return None

    running_max = float(df["tmpf"].max())
    logger.debug("%s: running max = %.1f°F (%d 1-min obs)", station, running_max, len(df))
    return running_max


def get_obs_context(
    station: str,
    event_date: date,
    trend_window_minutes: int = 20,
) -> dict:
    """
    Single IEM fetch returning running max, current temp, and short-term trend.

    Returns dict with keys:
        running_max_f    : today's highest temp (°F), None if unavailable
        current_temp_f   : most recent observed temp (°F), None if unavailable
        trend_f_per_min  : linear trend over last trend_window_minutes of 1-min
                           data (positive = warming, negative = cooling).
                           None if fewer than 3 minutes of data.

    Falls back to METAR for current_temp_f when 1-min data is unavailable.
    """
    df = fetch_1min_temps(station, event_date)

    result: dict = {"running_max_f": None, "current_temp_f": None, "trend_f_per_min": None}

    if df is not None and not df.empty:
        result["running_max_f"]  = float(df["tmpf"].max())
        result["current_temp_f"] = float(df["tmpf"].iloc[-1])

        now_local = df["valid_local"].max()
        cutoff    = now_local - pd.Timedelta(minutes=trend_window_minutes)
        recent    = df[df["valid_local"] >= cutoff].copy()
        if len(recent) >= 3:
            x = (recent["valid_local"] - recent["valid_local"].min()).dt.total_seconds() / 60.0
            slope, _ = np.polyfit(x.values, recent["tmpf"].values, 1)
            result["trend_f_per_min"] = round(float(slope), 4)
    else:
        # 1-min unavailable — fall back to METAR for current temp
        result["current_temp_f"] = get_best_obs_temp(station)

    logger.debug(
        "%s obs context: max=%.1f  cur=%.1f  trend=%s°F/min",
        station,
        result["running_max_f"]  or float("nan"),
        result["current_temp_f"] or float("nan"),
        f"{result['trend_f_per_min']:+.4f}" if result["trend_f_per_min"] is not None else "N/A",
    )
    return result


# ---------------------------------------------------------------------------
# METAR T-group parser (0.1°C precision)
# ---------------------------------------------------------------------------

# T-group regex: T + sign_dry(0/1) + 3-digit dry + sign_dew(0/1) + 3-digit dew
# Example: T02280178 = dry 22.8°C (sign=0 → positive), dew 17.8°C
_TGROUP_RE = re.compile(r"\bT([01])(\d{3})([01])(\d{3})\b")


def parse_tgroup_temp(raw_metar: str) -> float | None:
    """
    Extract the T-group dry-bulb temperature from a raw METAR string.
    Returns temperature in °F to 0.18°F precision, or None if absent.

    T-group format: T{s1}{ddd}{s2}{ddd}
        s1: 0 = positive, 1 = negative (dry bulb sign)
        ddd: dry bulb in tenths of °C
        s2: 0 = positive, 1 = negative (dew point sign)
        ddd: dew point in tenths of °C

    Example: T02280178
        dry  = +22.8°C = 73.04°F
        dew  = +17.8°C
    """
    m = _TGROUP_RE.search(raw_metar)
    if not m:
        return None

    sign_dry = -1 if m.group(1) == "1" else 1
    temp_c   = sign_dry * int(m.group(2)) / 10.0
    temp_f   = temp_c * 9.0 / 5.0 + 32.0

    return round(temp_f, 2)


def parse_whole_temp(raw_metar: str) -> float | None:
    """
    Extract the standard METAR temperature field (whole degrees).
    Format: TT/DD where TT = temperature in whole °C (M = negative prefix).
    Returns °F or None.

    Example: 23/17 → 23°C = 73.4°F
             M02/M05 → -2°C = 28.4°F
    """
    # Match: space-delimited TT/DD field
    m = re.search(r"\b(M?\d{2})/(M?\d{2})\b", raw_metar)
    if not m:
        return None
    raw_t = m.group(1)
    sign  = -1 if raw_t.startswith("M") else 1
    temp_c = sign * int(raw_t.lstrip("M"))
    return round(temp_c * 9.0 / 5.0 + 32.0, 1)


# ---------------------------------------------------------------------------
# Live METAR fetch and parse
# ---------------------------------------------------------------------------

def fetch_raw_metar(station: str) -> str | None:
    """
    Fetch the most recent raw METAR string from aviationweather.gov.
    Returns the raw string or None on failure.
    """
    params = {
        "ids":    station,
        "format": "raw",
        "hours":  2,
    }
    for attempt in range(3):
        try:
            resp = requests.get(AVWX_METAR_URL, params=params, timeout=20)
            resp.raise_for_status()
            text = resp.text.strip()
            if not text:
                return None
            # Take the first (most recent) METAR block
            return text.split("\n")[0].strip()
        except Exception as exc:
            wait = 2 ** attempt
            logger.warning("%s: METAR fetch failed (%s) — retrying in %ds", station, exc, wait)
            time.sleep(wait)
    return None


def get_metar_temp(station: str) -> tuple[float | None, float | None, str]:
    """
    Fetch the current METAR and extract temperature with best available precision.

    Returns: (tgroup_temp_f, whole_temp_f, source)
        tgroup_temp_f : T-group temp in °F (0.1°C precision) or None
        whole_temp_f  : Whole-degree METAR temp in °F or None
        source        : "tgroup" | "whole" | "none"

    The caller decides which to use and how to weight them.
    Typical usage: prefer tgroup_temp_f; fall back to whole_temp_f.
    """
    raw = fetch_raw_metar(station)
    if not raw:
        logger.warning("%s: could not fetch METAR", station)
        return None, None, "none"

    tgroup = parse_tgroup_temp(raw)
    whole  = parse_whole_temp(raw)

    if tgroup is not None:
        source = "tgroup"
        logger.debug("%s METAR T-group: %.2f°F  whole: %s", station, tgroup,
                     f"{whole:.1f}°F" if whole else "N/A")
    elif whole is not None:
        source = "whole"
        logger.debug("%s METAR whole: %.1f°F (no T-group)", station, whole)
    else:
        source = "none"
        logger.warning("%s: METAR temp not found in: %s", station, raw[:80])

    return tgroup, whole, source


def get_best_obs_temp(station: str) -> float | None:
    """
    Return the best available current temperature for a station in °F.
    Priority: T-group > whole-degree METAR.
    Returns None if both unavailable.
    """
    tgroup, whole, _ = get_metar_temp(station)
    return tgroup if tgroup is not None else whole


# ---------------------------------------------------------------------------
# Confluence check
# ---------------------------------------------------------------------------

def running_max_with_confluence(
    station: str,
    event_date: date,
    tolerance_f: float = 2.0,
) -> dict:
    """
    Fetch running max from IEM 1-min AND current METAR temp for confluence.

    Returns dict:
        running_max_f    : best running max (1-min preferred, METAR fallback)
        iem_max_f        : IEM 1-min running max (None if unavailable)
        metar_temp_f     : best METAR temp (T-group or whole)
        metar_source     : "tgroup" | "whole" | "none"
        in_confluence    : True if |iem_max - metar_temp| <= tolerance_f
        data_source      : "iem_1min" | "metar" | "none"
        note             : human-readable description

    "in_confluence" is informational — the exit logic always uses running_max_f.
    """
    iem_max  = get_running_max(station, event_date)
    tgroup, whole, metar_source = get_metar_temp(station)
    metar_temp = tgroup if tgroup is not None else whole

    # Determine best running max
    if iem_max is not None:
        running_max = iem_max
        data_source = "iem_1min"
    elif metar_temp is not None:
        running_max = metar_temp
        data_source = "metar"
    else:
        running_max = None
        data_source = "none"

    # Confluence
    if iem_max is not None and metar_temp is not None:
        delta = abs(iem_max - metar_temp)
        in_confluence = delta <= tolerance_f
        note = (
            f"IEM {iem_max:.1f}°F vs METAR {metar_temp:.1f}°F — "
            f"{'✓ in confluence' if in_confluence else f'⚠ delta {delta:.1f}°F'}"
        )
    elif iem_max is not None:
        in_confluence = True   # only one source, no conflict
        note = f"IEM only: {iem_max:.1f}°F (no METAR)"
    elif metar_temp is not None:
        in_confluence = True
        note = f"METAR only: {metar_temp:.1f}°F (no IEM 1-min)"
    else:
        in_confluence = False
        note = "No temperature data available"

    logger.info(
        "%s running max: %s | %s",
        station,
        f"{running_max:.1f}°F" if running_max is not None else "N/A",
        note,
    )

    return {
        "running_max_f":  running_max,
        "iem_max_f":      iem_max,
        "metar_temp_f":   metar_temp,
        "metar_source":   metar_source,
        "in_confluence":  in_confluence,
        "data_source":    data_source,
        "note":           note,
    }
