"""
Discover all active Kalshi HIGH temperature series via the API.

Finds every KXHIGH* series, identifies which are already configured
in config.py, and reports new ones with:
  - City name (parsed from title/rules)
  - Settlement station hint (ICAO codes found in rules_primary)
  - NWS WFO (auto-looked up from NWS API if coordinates known)
  - Ready-to-paste config.py snippet

Run on demand:
  python scripts/discover_markets.py

Output also saved to: data/raw/discovered_series.json
"""

import os
import sys
import re
import json
import time
from datetime import date

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KALSHI_STATION_SERIES, KALSHI_SETTLEMENT_STATION
from kalshi_client import KalshiClient
from utils.logging_config import setup_logging

logger = setup_logging("discover_markets")

# Only high temperature markets
TEMP_PREFIXES = ("KXHIGH",)

# Matches any ICAO-style 4-letter code starting with K
ICAO_RE = re.compile(r"\b(K[A-Z]{3})\b")

# NWS well-known station -> coordinates (lat, lon)
# Used to look up WFO if we can identify the settlement station
KNOWN_NWS_COORDS: dict[str, tuple[float, float]] = {
    "KNYC": (40.7789, -73.9692),
    "KMDW": (41.7862, -87.7525),
    "KMIA": (25.7959, -80.2870),
    "KDFW": (32.8998, -97.0403),
    "KLAX": (33.9425, -118.4081),
    "KATL": (33.6407, -84.4277),
    "KDEN": (39.8561, -104.6737),
    "KHOU": (29.6454, -95.2789),
    "KSEA": (47.4502, -122.3088),
    "KPHX": (33.4373, -112.0078),
    "KLAS": (36.0840, -115.1537),
    "KMSP": (44.8848, -93.2223),
    "KBOS": (42.3643, -71.0052),
    "KPHL": (39.8721, -75.2411),
    "KDTW": (42.2124, -83.3534),
    "KSFO": (37.6213, -122.3790),
    "KSLC": (40.7884, -111.9778),
    "KSTL": (38.7487, -90.3700),
    "KPIT": (40.4915, -80.2329),
    "KCLT": (35.2140, -80.9431),
    "KIAD": (38.9531, -77.4565),
    "KDCA": (38.8521, -77.0377),
    "KBWI": (39.1754, -76.6683),
    "KMCO": (28.4312, -81.3081),
    "KTPA": (27.9755, -82.5332),
    "KPHX": (33.4373, -112.0078),
    "KABQ": (35.0402, -106.6090),
    "KSAN": (32.7336, -117.1896),
    "KSAC": (38.5126, -121.4944),
    "KPDX": (45.5898, -122.5951),
}


# ---------------------------------------------------------------------------
# API pagination helpers
# ---------------------------------------------------------------------------

def _fetch_all_series(client: KalshiClient) -> set[str]:
    """
    Paginate through all open Kalshi events and collect unique series tickers
    that match any TEMP_PREFIXES. Uses cursor-based pagination.
    """
    found: set[str] = set()

    for prefix in TEMP_PREFIXES:
        cursor = None
        page   = 0

        while True:
            params: dict = {"status": "open", "limit": 200}
            if cursor:
                params["cursor"] = cursor

            try:
                data   = client._get("/events", params=params)
            except Exception as exc:
                logger.error("Events page %d failed for prefix %s: %s", page, prefix, exc)
                break

            events  = data.get("events", [])
            matches = [
                e.get("series_ticker", "")
                for e in events
                if e.get("series_ticker", "").startswith(prefix)
            ]
            found.update(s for s in matches if s)

            cursor = data.get("cursor")
            page  += 1
            logger.info(
                "  %s* -- page %d: %d events, %d matching, %d total so far",
                prefix, page, len(events), len(matches), len(found),
            )

            if not cursor or len(events) < 200:
                break

            time.sleep(0.25)

        time.sleep(0.5)

    return found


def _get_series_detail(client: KalshiClient, series_ticker: str) -> dict:
    """
    Fetch one market from the series and extract:
      - title / subtitle
      - rules_primary (first 500 chars)
      - ICAO codes mentioned in rules
      - city name hint
      - market type (HIGH / LOW)
    """
    try:
        data    = client._get("/markets", params={
            "series_ticker": series_ticker,
            "status":        "open",
            "limit":         1,
        })
        markets = data.get("markets", [])
        if not markets:
            return {"series_ticker": series_ticker, "error": "no markets found"}

        m       = markets[0]
        rules   = m.get("rules_primary", "")
        title   = m.get("title", m.get("subtitle", ""))

        # Find ICAO codes in rules text
        icao_hits = ICAO_RE.findall(rules)
        # Deduplicate preserving order; skip very generic ones
        seen: set[str] = set()
        icao_hints: list[str] = []
        for code in icao_hits:
            if code not in seen:
                seen.add(code)
                icao_hints.append(code)

        # Best-guess city from title/rules
        # "high temp in Dallas", "temperature at Phoenix", "max temp for Seattle"
        city_m = re.search(
            r"(?:in|at|for)\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)?)",
            title or rules,
        )
        city_hint = city_m.group(1).strip() if city_m else ""

        market_type = "HIGH"

        return {
            "series_ticker": series_ticker,
            "market_type":   market_type,
            "title":         title[:120],
            "rules_snippet": rules[:500],
            "city_hint":     city_hint,
            "icao_hints":    icao_hints,
        }

    except Exception as exc:
        return {"series_ticker": series_ticker, "error": str(exc)}


# ---------------------------------------------------------------------------
# NWS / NOAA lookups (best-effort, non-blocking)
# ---------------------------------------------------------------------------

def _lookup_wfo(icao: str) -> str | None:
    """
    Look up the NWS WFO (office) for an ICAO station using NWS API.
    Returns WFO code (e.g. "OKX") or None on failure.
    """
    coords = KNOWN_NWS_COORDS.get(icao)
    if not coords:
        return None

    lat, lon = coords
    try:
        resp = requests.get(
            f"https://api.weather.gov/points/{lat},{lon}",
            headers={"User-Agent": "kalshi-weather-bot/1.0 (github.com/Clifton77/Living-Kingdom)"},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json().get("properties", {}).get("cwa")
    except Exception:
        pass
    return None


def _lookup_ghcnd(icao: str) -> str | None:
    """
    Search NOAA CDO for the GHCND station ID matching an ICAO code.
    Returns GHCND ID string (e.g. "USW00094728") or None.
    """
    try:
        resp = requests.get(
            "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations",
            headers={"token": os.environ.get("NOAA_CDO_TOKEN", "")},
            params={"datatypeid": "TMAX", "stationid": f"WBAN:{icao[1:]}",
                    "limit": 5},
            timeout=10,
        )
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            if results:
                return results[0].get("id", "").replace("GHCND:", "")
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Main discovery logic
# ---------------------------------------------------------------------------

def discover_all_series(client: KalshiClient) -> dict:
    """
    Full discovery scan. Returns:
      {
        "known":    [series already in config],
        "new":      [new series with full detail],
        "all":      [all found series, sorted],
        "scan_date": ISO date string,
      }
    """
    known_series  = set(KALSHI_STATION_SERIES.values())

    logger.info("=" * 72)
    logger.info("  KALSHI TEMPERATURE MARKET DISCOVERY")
    logger.info("=" * 72)
    logger.info("  Scanning KXHIGH* series across all open events...")

    all_found = _fetch_all_series(client)

    known_found = sorted(all_found & known_series)
    new_found   = sorted(all_found - known_series)

    logger.info("Total series found : %d", len(all_found))
    logger.info("Already configured : %d", len(known_found))
    logger.info("NEW (unconfigured) : %d", len(new_found))

    # Known series summary
    logger.info("-" * 72)
    logger.info("CONFIGURED SERIES")
    logger.info("-" * 72)
    label_by_series = {v: k for k, v in KALSHI_STATION_SERIES.items()}
    for s in sorted(known_found):
        station = label_by_series.get(s, "?")
        settle  = KALSHI_SETTLEMENT_STATION.get(station, station)
        logger.info("  %-22s  station=%-6s  settle=%s", s, station, settle)

    # New series deep scan
    new_details: list[dict] = []

    if new_found:
        logger.info("-" * 72)
        logger.info("NEW SERIES -- DETAILED SCAN")
        logger.info("-" * 72)

        for series in new_found:
            logger.info(">> %s", series)
            info = _get_series_detail(client, series)
            new_details.append(info)

            if "error" in info:
                logger.warning("  ERROR: %s", info["error"])
                time.sleep(0.3)
                continue

            logger.info("  Type    : %s", info["market_type"])
            logger.info("  Title   : %s", info["title"])
            logger.info("  City    : %s", info["city_hint"] or "(not parsed)")
            logger.info("  ICAO hints: %s", info["icao_hints"] or "none found")
            logger.info("  Rules   : %s", info["rules_snippet"][:200])

            # Best-effort WFO lookup
            if info["icao_hints"]:
                candidate = info["icao_hints"][0]
                wfo = _lookup_wfo(candidate)
                info["wfo_hint"] = wfo
                logger.info("  WFO     : %s", wfo or "(unknown)")
            else:
                info["wfo_hint"] = None

            time.sleep(0.3)
    else:
        logger.info("No new series found -- all active KXHIGH* series are configured.")

    return {
        "known":     known_found,
        "new":       new_details,
        "all":       sorted(all_found),
        "scan_date": str(date.today()),
    }


def _log_config_snippet(new_details: list[dict]) -> None:
    """Log a ready-to-paste config.py block for new stations."""
    if not new_details:
        return

    logger.info("=" * 72)
    logger.info("CONFIG.PY ADDITIONS (verify settlement station from rules first)")
    logger.info("=" * 72)

    logger.info("# KALSHI_STATION_SERIES -- assign a Kalshi label (e.g. KSEA, KPHX):")
    for info in new_details:
        s    = info["series_ticker"]
        city = info.get("city_hint", "???")
        logger.info('    "K???": "%s",   # %s', s, city)

    logger.info("# KALSHI_SETTLEMENT_STATION -- VERIFY from full rules_primary:")
    for info in new_details:
        icao = (info.get("icao_hints") or ["K???"])[0]
        city = info.get("city_hint", "???")
        logger.info('    "K???": "%s",   # %s -- confirm from rules', icao, city)

    logger.info("# GHCND_IDS -- look up at ncdc.noaa.gov/cdo-web/search:")
    for info in new_details:
        icao = (info.get("icao_hints") or ["K???"])[0]
        logger.info('    "%s": "USW000XXXXX",', icao)

    logger.info("# STATION_COORDS (lat, lon):")
    for info in new_details:
        icao = (info.get("icao_hints") or ["K???"])[0]
        logger.info('    "%s": (XX.XXXX, -XXX.XXXX),', icao)

    logger.info("# WFO_MAP:")
    for info in new_details:
        wfo = info.get("wfo_hint") or "???"
        logger.info('    "K???": "%s",', wfo)

    logger.info("# STATION_TIMEZONES:")
    for info in new_details:
        city = info.get("city_hint", "???")
        logger.info('    "K???": "America/???",   # %s', city)

    logger.info("# After adding, run:")
    logger.info("#   python run_pipeline.py --only obs --force")
    logger.info("#   python run_pipeline.py --only forecasts --force")
    logger.info("#   python run_pipeline.py --only bias --force")


def main() -> None:
    client  = KalshiClient(demo=False)
    results = discover_all_series(client)

    _log_config_snippet(results["new"])

    # Save full results for reference
    os.makedirs(os.path.join("data", "raw"), exist_ok=True)
    out_path = os.path.join("data", "raw", "discovered_series.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    logger.info("Full results saved -> %s", out_path)
    logger.info("Scan date: %s", results["scan_date"])


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        import traceback
        print(f"\nFATAL ERROR: {exc}", flush=True)
        traceback.print_exc()
        sys.exit(1)
