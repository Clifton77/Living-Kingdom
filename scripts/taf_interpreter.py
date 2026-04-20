"""
TAF interpreter — fetches and parses TAF text for each station.

Returns:
  - dominant weather condition for the peak heating window
  - penalty category for weather_penalty.py
  - METAR current conditions for dashboard display
  - human-readable summary

Peak-window scoring: only TAF periods that overlap [peak_hour − PRE_PEAK_WINDOW,
peak_hour + POST_PEAK_WINDOW] (local time) are scored. This prevents overnight
or morning precip from penalising tomorrow's afternoon trade.

Source: aviationweather.gov public API (no key required)
"""

from __future__ import annotations

import re
import requests
from datetime import datetime, timedelta, timezone, date as date_type
from dataclasses import dataclass, field
from typing import Optional

from utils.logging_config import setup_logging
from config import (
    STATIONS, COASTAL_STATIONS, AVWX_TAF_URL, AVWX_METAR_URL,
    STATION_TIMEZONES, TAF_PRE_PEAK_WINDOW_HOURS, TAF_POST_PEAK_WINDOW_HOURS,
)

logger = setup_logging("taf_interpreter")

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TafPeriod:
    """One time-indexed forecast group from a TAF."""
    start_utc:  datetime
    end_utc:    datetime
    raw:        str
    group_type: str   # "base", "FM", "BECMG", "TEMPO"


@dataclass
class TafResult:
    station:          str
    condition:        str          # penalty category for the peak window
    sky_cover:        str          # worst sky in peak window
    precip:           list[str]    # precip codes found in peak window
    has_ts:           bool
    has_fog:          bool
    has_amd:          bool         # amendment issued since last check
    taf_raw:          str
    summary:          str          # one-liner for dashboard
    fetched_utc:      str          # ISO timestamp
    peak_window_utc:  tuple        # (start, end) UTC datetimes scored

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


def _taf_ddhh_to_utc(dd: int, hh: int, ref_utc: datetime) -> datetime:
    """Resolve a TAF day+hour (UTC) to an absolute datetime using ref_utc's month/year.
    TAFs use 2400 to mean midnight end-of-day; normalize to 00:00 next day."""
    if hh == 24:
        # Advance day by 1, use hh=0
        base = _taf_ddhh_to_utc(dd, 0, ref_utc)
        return base + timedelta(days=1)
    year, month = ref_utc.year, ref_utc.month
    try:
        return datetime(year, month, dd, hh, 0, tzinfo=timezone.utc)
    except ValueError:
        # Day doesn't exist in current month — roll to next month
        if month == 12:
            return datetime(year + 1, 1, dd, hh, 0, tzinfo=timezone.utc)
        return datetime(year, month + 1, dd, hh, 0, tzinfo=timezone.utc)


def _parse_group_start(header: str, ref_utc: datetime) -> datetime:
    """Extract the UTC start time from a TAF group header string."""
    upper = header.upper()
    fm_m = re.match(r"FM(\d{2})(\d{2})\d{2}", upper)
    if fm_m:
        return _taf_ddhh_to_utc(int(fm_m.group(1)), int(fm_m.group(2)), ref_utc)
    bt_m = re.search(r"(\d{2})(\d{2})/\d{4}", upper)
    if bt_m:
        return _taf_ddhh_to_utc(int(bt_m.group(1)), int(bt_m.group(2)), ref_utc)
    return ref_utc


def _parse_group_end(header: str, ref_utc: datetime) -> Optional[datetime]:
    """Extract the UTC end time from a BECMG/TEMPO group header (FM has no explicit end)."""
    upper = header.upper()
    bt_m = re.search(r"\d{4}/(\d{2})(\d{2})", upper)
    if bt_m:
        return _taf_ddhh_to_utc(int(bt_m.group(1)), int(bt_m.group(2)), ref_utc)
    return None


def parse_taf_into_periods(taf_text: str) -> list[TafPeriod]:
    """
    Split a TAF into time-indexed forecast periods.
    FM groups are permanent changes; BECMG/TEMPO have explicit end times.
    Returns list sorted by start_utc.
    """
    now_utc = datetime.now(timezone.utc)
    upper   = taf_text.upper()

    # TAF valid period header: DDHH/DDHH
    valid_m = re.search(r"\b(\d{2})(\d{2})/(\d{2})(\d{2})\b", upper)
    if not valid_m:
        return []

    taf_start = _taf_ddhh_to_utc(int(valid_m.group(1)), int(valid_m.group(2)), now_utc)
    taf_end   = _taf_ddhh_to_utc(int(valid_m.group(3)), int(valid_m.group(4)), now_utc)
    if taf_end <= taf_start:
        taf_end += timedelta(days=1)

    # Find all group boundaries
    group_pat = re.compile(
        r"(?:^|\s)(FM\d{6}|BECMG\s+\d{4}/\d{4}|TEMPO\s+\d{4}/\d{4})",
        re.IGNORECASE | re.MULTILINE,
    )
    headers = list(group_pat.finditer(taf_text))
    periods: list[TafPeriod] = []

    # Base period: valid_start → first group start (or taf_end)
    base_end_dt = _parse_group_start(headers[0].group(1), now_utc) if headers else taf_end
    base_raw    = taf_text[: headers[0].start() + 1] if headers else taf_text
    periods.append(TafPeriod(
        start_utc=taf_start, end_utc=base_end_dt,
        raw=base_raw, group_type="base",
    ))

    for i, m in enumerate(headers):
        hdr       = m.group(1).strip()
        grp_type  = hdr[:2].upper()   # FM / BE / TE
        grp_start = _parse_group_start(hdr, now_utc)
        explicit_end = _parse_group_end(hdr, now_utc)

        # End = explicit end (BECMG/TEMPO) OR start of next FM group OR taf_end
        if explicit_end:
            grp_end = explicit_end
        elif i + 1 < len(headers):
            grp_end = _parse_group_start(headers[i + 1].group(1).strip(), now_utc)
        else:
            grp_end = taf_end

        raw_start = m.start()
        raw_end   = headers[i + 1].start() if i + 1 < len(headers) else len(taf_text)
        periods.append(TafPeriod(
            start_utc=grp_start, end_utc=grp_end,
            raw=taf_text[raw_start:raw_end], group_type=grp_type,
        ))

    return sorted(periods, key=lambda p: p.start_utc)


def _classify_text(text: str, station: str) -> tuple[str, str, list[str], bool, bool]:
    """
    Classify a raw TAF snippet into (condition, sky, precip_list, has_ts, has_fog).
    Mirrors the logic in parse_taf() but operates on any text fragment.
    """
    upper = text.upper()

    # Hard skip check
    for pat in _HARD_SKIP_PATTERNS:
        if re.search(pat, upper):
            return "hard_skip", "OVC", [], False, False

    has_ts  = bool(re.search(r"\bTS\b|\bVCTS\b|\bTSRA\b|\bTSGR\b", upper))
    has_fog = bool(re.search(r"\bFG\b|\bBR\b|\bMIFG\b|\bBCFG\b|\bPRFG\b", upper))
    precip  = [c for c in _PRECIP_CODES if c in upper]
    sky     = _worst_sky(upper)
    coastal = station in COASTAL_STATIONS

    if has_ts:
        cond = "convective"
    elif precip:
        cond = "precip"
    elif has_fog and coastal:
        cond = "marine_fog"
    elif sky[:3] in ("BKN", "OVC"):
        cond = "broken"
    elif sky[:3] == "SCT":
        cond = "scattered"
    else:
        cond = "clear"

    return cond, sky, precip, has_ts, has_fog


def score_taf_for_window(
    taf_text: str,
    station: str,
    window_start_utc: datetime,
    window_end_utc: datetime,
) -> tuple[str, str, list[str], bool, bool]:
    """
    Score a TAF for the worst condition within [window_start_utc, window_end_utc].

    TEMPO groups (temporary, <60 min) are checked but at reduced weight —
    a TEMPO precip doesn't trigger a full precip penalty unless there's
    also a base-layer or FM precip in the window.

    Falls back to the nearest period if no groups overlap the window.

    Returns (condition, sky_cover, precip_list, has_ts, has_fog).
    """
    periods = parse_taf_into_periods(taf_text)
    if not periods:
        return _classify_text(taf_text, station)

    overlapping = [
        p for p in periods
        if p.start_utc < window_end_utc and p.end_utc > window_start_utc
    ]

    if not overlapping:
        # No TAF coverage for the window — use nearest period
        nearest = min(periods, key=lambda p: min(
            abs((p.start_utc - window_start_utc).total_seconds()),
            abs((p.end_utc   - window_start_utc).total_seconds()),
        ))
        logger.debug(
            "%s TAF: no coverage for window %s–%s, using nearest period (%s–%s)",
            station,
            window_start_utc.strftime("%H:%MZ"),
            window_end_utc.strftime("%H:%MZ"),
            nearest.start_utc.strftime("%H:%MZ"),
            nearest.end_utc.strftime("%H:%MZ"),
        )
        return _classify_text(nearest.raw, station)

    # Score each overlapping period and take worst non-TEMPO, then
    # only elevate based on TEMPO if the base condition already shows precip risk.
    best_cond  = "clear"
    best_sky   = "SKC"
    all_precip: list[str] = []
    any_ts     = False
    any_fog    = False
    tempo_cond = "clear"

    for p in overlapping:
        cond, sky, precip, has_ts, has_fog = _classify_text(p.raw, station)
        if p.group_type == "TE":  # TEMPO — track separately
            if _CONDITION_PRIORITY.get(cond, 0) > _CONDITION_PRIORITY.get(tempo_cond, 0):
                tempo_cond = cond
            continue
        if _CONDITION_PRIORITY.get(cond, 0) > _CONDITION_PRIORITY.get(best_cond, 0):
            best_cond = cond
        if _SKY_PRIORITY.get(sky[:3], 0) > _SKY_PRIORITY.get(best_sky[:3], 0):
            best_sky = sky
        all_precip.extend(p for p in precip if p not in all_precip)
        any_ts  = any_ts  or has_ts
        any_fog = any_fog or has_fog

    # Only include TEMPO severity if the base already has precip-level risk,
    # or if TEMPO is hard_skip / convective (those always matter).
    if tempo_cond in ("hard_skip", "convective"):
        if _CONDITION_PRIORITY.get(tempo_cond, 0) > _CONDITION_PRIORITY.get(best_cond, 0):
            best_cond = tempo_cond

    logger.debug(
        "%s TAF window %s–%s: %d overlapping periods → condition=%s",
        station,
        window_start_utc.strftime("%H:%MZ"),
        window_end_utc.strftime("%H:%MZ"),
        len(overlapping), best_cond,
    )
    return best_cond, best_sky, all_precip, any_ts, any_fog


def parse_taf(
    taf_text: str,
    station: str,
    peak_window_utc: tuple[datetime, datetime] | None = None,
) -> TafResult:
    """
    Parse TAF text and classify the dominant condition.

    If peak_window_utc is provided, only TAF periods overlapping that window
    are scored — overnight or morning weather doesn't penalise an afternoon trade.
    Falls back to full-TAF worst-case when peak_window_utc is None.
    """
    now_utc_str = datetime.now(timezone.utc).isoformat(timespec="seconds")
    has_amd     = "AMD" in (taf_text or "").upper()

    if not taf_text:
        return TafResult(
            station=station, condition="clear", sky_cover="SKC",
            precip=[], has_ts=False, has_fog=False, has_amd=False,
            taf_raw="", summary="No TAF — assuming clear",
            fetched_utc=now_utc_str, peak_window_utc=peak_window_utc or (None, None),
        )

    upper = taf_text.upper()

    # ── Hard skip check — applies to entire TAF regardless of window ──────
    # Dangerous conditions anywhere in the TAF are always a hard skip.
    for pat in _HARD_SKIP_PATTERNS:
        if re.search(pat, upper):
            # Only hard-skip if the dangerous condition falls in the peak window
            # (or we have no window info — be conservative).
            if peak_window_utc is None:
                token = pat.replace(r"\b", "").strip()
                return TafResult(
                    station=station, condition="hard_skip", sky_cover="OVC",
                    precip=[token], has_ts=False, has_fog=False,
                    has_amd=has_amd, taf_raw=taf_text,
                    summary=f"HARD SKIP — {token} in TAF",
                    fetched_utc=now_utc_str,
                    peak_window_utc=(None, None),
                )
            # With a peak window: check if dangerous condition is in window
            win_start, win_end = peak_window_utc
            cond_in_window, _, _, _, _ = score_taf_for_window(
                taf_text, station, win_start, win_end
            )
            if cond_in_window == "hard_skip":
                token = pat.replace(r"\b", "").strip()
                return TafResult(
                    station=station, condition="hard_skip", sky_cover="OVC",
                    precip=[token], has_ts=False, has_fog=False,
                    has_amd=has_amd, taf_raw=taf_text,
                    summary=f"HARD SKIP — {token} in peak window",
                    fetched_utc=now_utc_str,
                    peak_window_utc=peak_window_utc,
                )
            # Dangerous condition exists but NOT in peak window — continue scoring
            break

    # ── Score: peak window or full TAF ───────────────────────────────────
    if peak_window_utc is not None:
        win_start, win_end = peak_window_utc
        condition, sky, precip_found, has_ts, has_fog = score_taf_for_window(
            taf_text, station, win_start, win_end
        )
    else:
        condition, sky, precip_found, has_ts, has_fog = _classify_text(taf_text, station)

    # ── Summary ───────────────────────────────────────────────────────────
    parts = []
    if peak_window_utc and peak_window_utc[0]:
        win_s, win_e = peak_window_utc
        parts.append(f"Peak window {win_s.strftime('%H:%MZ')}–{win_e.strftime('%H:%MZ')}")
    if has_ts:
        parts.append("TS in window")
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
        fetched_utc=now_utc_str,
        peak_window_utc=peak_window_utc or (None, None),
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

def interpret_taf(
    station: str,
    event_date: date_type | None = None,
    peak_hour_local: int | None = None,
) -> TafResult:
    """
    Fetch and parse TAF for a station.

    When event_date and peak_hour_local are provided, only TAF periods
    overlapping the peak heating window are scored — preventing overnight
    or morning weather from penalising an afternoon high-temperature trade.

    Falls back to full-TAF worst-case scoring when either is None.
    """
    raw = fetch_taf_raw(station)

    peak_window_utc = None
    if event_date is not None and peak_hour_local is not None:
        try:
            import pytz
            tz = pytz.timezone(STATION_TIMEZONES[station])
            # Build naive local datetime for the peak hour on the event date
            peak_local = datetime(
                event_date.year, event_date.month, event_date.day,
                peak_hour_local, 0, 0,
            )
            peak_local = tz.localize(peak_local)
            peak_utc   = peak_local.astimezone(timezone.utc)
            win_start  = peak_utc - timedelta(hours=TAF_PRE_PEAK_WINDOW_HOURS)
            win_end    = peak_utc + timedelta(hours=TAF_POST_PEAK_WINDOW_HOURS)
            peak_window_utc = (win_start, win_end)
        except Exception as exc:
            logger.warning("%s: could not build peak window: %s", station, exc)

    result = parse_taf(raw or "", station, peak_window_utc=peak_window_utc)
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
