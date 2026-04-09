"""
TAF interpreter — fetches and parses TAF text for each station.

Returns:
  - dominant weather condition for the heating window (12Z–00Z local)
  - penalty category for weather_penalty.py
  - METAR current conditions for dashboard display
  - human-readable summary

Source: aviationweather.gov public API (no key required)
"""

from __future__ import annotations

import re
import requests
from datetime import datetime, timezone
from dataclasses import dataclass, field

from utils.logging_config import setup_logging
from config import STATIONS, COASTAL_STATIONS, AVWX_TAF_URL, AVWX_METAR_URL

logger = setup_logging("taf_interpreter")

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TafResult:
    station:          str
    condition:        str          # penalty category
    sky_cover:        str          # worst sky in heating window
    precip:           list[str]    # precip codes found
    has_ts:           bool
    has_fog:          bool
    has_amd:          bool         # amendment issued since last check
    taf_raw:          str
    summary:          str          # one-liner for dashboard
    fetched_utc:      str          # ISO timestamp

@dataclass
class MetarResult:
    station:          str
    temp_f:           float | None
    dewpoint_f:       float | None
    wind_dir:         int | None
    wind_kt:          int | None
    wind_gust_kt:     int | None
    visibility_sm:    float | None
    sky_cover:        str
    wx_codes:         list[str]
    raw:              str
    fetched_utc:      str


# ---------------------------------------------------------------------------
# TAF fetch
# ---------------------------------------------------------------------------

def fetch_taf_raw(station: str) -> str | None:
    """Fetch raw TAF text from aviationweather.gov."""
    try:
        resp = requests.get(
            AVWX_TAF_URL,
            params={"ids": station, "format": "raw", "hours": 30},
            timeout=10,
        )
        resp.raise_for_status()
        text = resp.text.strip()
        if not text or len(text) < 20:
            return None
        return text
    except Exception as exc:
        logger.warning("TAF fetch failed %s: %s", station, exc)
        return None


def fetch_metar_raw(station: str) -> str | None:
    """Fetch most recent METAR from aviationweather.gov."""
    try:
        resp = requests.get(
            AVWX_METAR_URL,
            params={"ids": station, "format": "raw", "hours": 2},
            timeout=10,
        )
        resp.raise_for_status()
        text = resp.text.strip()
        if not text or len(text) < 10:
            return None
        # Return first line only (most recent obs)
        return text.splitlines()[0].strip()
    except Exception as exc:
        logger.warning("METAR fetch failed %s: %s", station, exc)
        return None


# ---------------------------------------------------------------------------
# TAF parsing
# ---------------------------------------------------------------------------

# Condition priority — higher number = more penalized
_CONDITION_PRIORITY = {
    "hard_skip":  100,
    "precip":      80,
    "convective":  70,
    "marine_fog":  50,
    "broken":      30,
    "scattered":   20,
    "clear":       10,
}

_SKY_PRIORITY = {"OVC": 4, "BKN": 3, "SCT": 2, "FEW": 1, "CLR": 0, "SKC": 0}

_HARD_SKIP_PATTERNS = [
    r"\bFZRA\b", r"\bFZDZ\b", r"\bIC\b", r"\bPL\b",
    r"\+SN\b", r"\bBLSN\b",
]

_PRECIP_CODES = [
    "TSRA", "+RA", "-RA", "RA", "+SN", "-SN", "SN",
    "RASN", "SNRA", "+RASN", "SHRA", "DZ", "FZRA",
]


def _worst_sky(taf_upper: str) -> str:
    tokens = re.findall(r"\b(SKC|CLR|FEW\d*|SCT\d*|BKN\d*|OVC\d*)\b", taf_upper)
    worst = "SKC"
    for tok in tokens:
        base = tok[:3]
        if _SKY_PRIORITY.get(base, 0) > _SKY_PRIORITY.get(worst[:3], 0):
            worst = base
    return worst


def parse_taf(taf_text: str, station: str) -> TafResult:
    """
    Parse TAF text and classify the dominant condition for the heating window.
    Uses worst-case condition within the full TAF valid period.
    """
    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if not taf_text:
        return TafResult(
            station=station, condition="clear", sky_cover="SKC",
            precip=[], has_ts=False, has_fog=False, has_amd=False,
            taf_raw="", summary="No TAF — assuming clear", fetched_utc=now_utc,
        )

    upper = taf_text.upper()

    # ── Hard skip check ───────────────────────────────────────────────────
    for pat in _HARD_SKIP_PATTERNS:
        if re.search(pat, upper):
            token = pat.replace(r"\b", "").strip()
            return TafResult(
                station=station, condition="hard_skip", sky_cover="OVC",
                precip=[token], has_ts=False, has_fog=False,
                has_amd="AMD" in upper,
                taf_raw=taf_text,
                summary=f"HARD SKIP — {token} in TAF",
                fetched_utc=now_utc,
            )

    # ── Feature extraction ────────────────────────────────────────────────
    has_ts  = bool(re.search(r"\bTS\b|\bVCTS\b|\bTSRA\b|\bTSGR\b", upper))
    has_fog = bool(re.search(r"\bFG\b|\bBR\b|\bMIFG\b|\bBCFG\b|\bPRFG\b", upper))
    has_amd = "AMD" in upper

    precip_found = [c for c in _PRECIP_CODES if c in upper]
    sky = _worst_sky(upper)
    is_coastal = station in COASTAL_STATIONS

    # ── Classify condition ────────────────────────────────────────────────
    if has_ts:
        condition = "convective"
    elif precip_found:
        condition = "precip"
    elif has_fog and is_coastal:
        condition = "marine_fog"
    elif sky[:3] in ("BKN", "OVC"):
        condition = "broken"
    elif sky[:3] == "SCT":
        condition = "scattered"
    else:
        condition = "clear"

    # ── Summary ───────────────────────────────────────────────────────────
    parts = []
    if has_ts:
        parts.append("TS in TAF")
    if precip_found:
        parts.append(f"Precip: {' '.join(precip_found[:3])}")
    if has_fog:
        parts.append("Fog/BR")
    if has_amd:
        parts.append("AMD issued")
    parts.append(f"Sky: {sky}")
    summary = " · ".join(parts)

    return TafResult(
        station=station,
        condition=condition,
        sky_cover=sky,
        precip=precip_found,
        has_ts=has_ts,
        has_fog=has_fog,
        has_amd=has_amd,
        taf_raw=taf_text,
        summary=summary,
        fetched_utc=now_utc,
    )


# ---------------------------------------------------------------------------
# METAR parsing
# ---------------------------------------------------------------------------

def _c_to_f(c: float) -> float:
    return c * 9 / 5 + 32


def parse_metar(raw: str, station: str) -> MetarResult:
    """Parse a raw METAR string into structured fields."""
    now_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if not raw:
        return MetarResult(
            station=station, temp_f=None, dewpoint_f=None,
            wind_dir=None, wind_kt=None, wind_gust_kt=None,
            visibility_sm=None, sky_cover="SKC", wx_codes=[],
            raw="", fetched_utc=now_utc,
        )

    upper = raw.upper()

    # Temperature / dewpoint: M02/M08 or 22/14
    temp_f = dewpoint_f = None
    t_match = re.search(r"\b(M?\d{2})/(M?\d{2})\b", raw)
    if t_match:
        def parse_c(s: str) -> float:
            return -float(s[1:]) if s.startswith("M") else float(s)
        temp_f     = round(_c_to_f(parse_c(t_match.group(1))), 1)
        dewpoint_f = round(_c_to_f(parse_c(t_match.group(2))), 1)

    # Wind: 27012KT or VRB05KT or 27012G22KT
    wind_dir = wind_kt = wind_gust_kt = None
    w_match = re.search(r"\b(\d{3}|VRB)(\d{2,3})(?:G(\d{2,3}))?KT\b", upper)
    if w_match:
        dir_str = w_match.group(1)
        wind_dir      = int(dir_str) if dir_str != "VRB" else None
        wind_kt       = int(w_match.group(2))
        wind_gust_kt  = int(w_match.group(3)) if w_match.group(3) else None

    # Visibility
    vis_sm = None
    v_match = re.search(r"\b(\d+(?:\s\d+/\d+)?)\s*SM\b", raw)
    if v_match:
        try:
            parts = v_match.group(1).split()
            if len(parts) == 2:  # e.g. "1 1/2"
                whole, frac = parts
                n, d = frac.split("/")
                vis_sm = int(whole) + int(n) / int(d)
            elif "/" in parts[0]:
                n, d = parts[0].split("/")
                vis_sm = int(n) / int(d)
            else:
                vis_sm = float(parts[0])
        except Exception:
            pass

    # Sky cover (worst)
    sky = _worst_sky(upper)

    # Present weather codes
    wx_pattern = re.compile(
        r"\b([-+]?(?:TS|RA|SN|DZ|FG|BR|HZ|FU|RASN|TSRA|FZRA|SHRA|SQ))\b"
    )
    wx_codes = wx_pattern.findall(upper)

    return MetarResult(
        station=station,
        temp_f=temp_f,
        dewpoint_f=dewpoint_f,
        wind_dir=wind_dir,
        wind_kt=wind_kt,
        wind_gust_kt=wind_gust_kt,
        visibility_sm=vis_sm,
        sky_cover=sky,
        wx_codes=wx_codes,
        raw=raw,
        fetched_utc=now_utc,
    )


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def interpret_taf(station: str) -> TafResult:
    """Fetch and parse TAF. Main entry point for the signal engine."""
    raw = fetch_taf_raw(station)
    result = parse_taf(raw or "", station)
    logger.info(
        "%s TAF | %s | penalty_cat=%s | AMD=%s",
        station, result.summary, result.condition, result.has_amd,
    )
    return result


def get_metar(station: str) -> MetarResult:
    """Fetch and parse most recent METAR. Used for intraday obs tracking."""
    raw = fetch_metar_raw(station)
    result = parse_metar(raw or "", station)
    logger.info(
        "%s METAR | %.1f°F/%.1f°F | %s | wind %s°@%skt",
        station,
        result.temp_f or -99,
        result.dewpoint_f or -99,
        result.sky_cover,
        result.wind_dir,
        result.wind_kt,
    )
    return result


def interpret_all_stations() -> dict[str, TafResult]:
    """Fetch and interpret TAFs for all configured stations."""
    return {s: interpret_taf(s) for s in STATIONS}
