"""
Kalshi API probe — verify correct series tickers and live bucket discovery.
Run:  python test_kalshi.py
"""
import json
from datetime import date, timedelta
from kalshi_client import KalshiClient, get_series_ticker, _date_tag
from config import KALSHI_STATION_SERIES

client = KalshiClient(demo=False)
today    = date.today()
tomorrow = today + timedelta(days=1)

# ── 1. Verify our station→series mapping ─────────────────────────────────────
print("=== Station → Series mapping (from config) ===")
for station, series in KALSHI_STATION_SERIES.items():
    print(f"  {station}  →  {series}")

# ── 2. Query each series for open markets → confirm series tickers work ───────
print("\n=== Open markets per series (first 3 buckets each) ===")
for station, series in KALSHI_STATION_SERIES.items():
    try:
        data = client._get("/markets", params={"series_ticker": series, "status": "open", "limit": 20})
        markets = data.get("markets", [])
        print(f"\n  {station} ({series}) → {len(markets)} open markets")
        for m in markets[:3]:
            ticker   = m.get("ticker", "?")
            subtitle = m.get("subtitle", m.get("title", "?"))[:50]
            yes_ask  = m.get("yes_ask", "?")
            print(f"    {ticker:55s}  subtitle={subtitle!r:35s}  yes_ask={yes_ask}")
    except Exception as e:
        print(f"  {station} ({series}) → FAILED: {e}")

# ── 3. Live dynamic bucket discovery for today and tomorrow ──────────────────
print("\n=== Dynamic bucket discovery (get_markets_for_station_date) ===")
for check_date in [today, tomorrow]:
    label = "TODAY" if check_date == today else "TOMORROW"
    print(f"\n--- {label} ({check_date}) ---")
    for station in list(KALSHI_STATION_SERIES)[:3]:   # first 3 stations to keep output short
        snapshots = client.get_markets_for_station_date(station, check_date)
        if snapshots:
            print(f"  {station} — {len(snapshots)} buckets:")
            for s in snapshots:
                print(
                    f"    {s.bucket_label:20s}  yes_ask={s.yes_ask:.2f}  "
                    f"yes_bid={s.yes_bid:.2f}  vol={s.volume}  open={s.is_open}"
                )
        else:
            print(f"  {station} — no markets found")

# ── 4. Raw dump of a single bucket market to see all available fields ─────────
# Use KORD as anchor (confirmed working), then try other stations as fallback.
# Purpose: see exact field names for status, yes_ask, yes_bid, no_ask, no_bid.
print("\n=== Raw dump of first open bucket market (all fields) ===")
_dump_done = False
for _station in ["KJFK", "KORD", "KMIA", "KDFW", "KLAX"]:
    try:
        _series = get_series_ticker(_station)
        _data   = client._get("/markets", params={"series_ticker": _series, "status": "open", "limit": 3})
        _mkts   = _data.get("markets", [])
        if _mkts:
            print(f"  Station: {_station}  Series: {_series}  ({len(_mkts)} open returned)")
            print(json.dumps(_mkts[0], indent=2))
            _dump_done = True
            break
    except Exception as _e:
        print(f"  {_station} FAILED: {_e}")
if not _dump_done:
    print("  No open markets found for any station")

# ── 5. Individual market GET (single ticker) — may include different fields ───
print("\n=== Single-market GET for first open KORD market ===")
try:
    _series = get_series_ticker("KORD")
    _data   = client._get("/markets", params={"series_ticker": _series, "status": "open", "limit": 1})
    _mkts   = _data.get("markets", [])
    if _mkts:
        _ticker = _mkts[0].get("ticker", "")
        print(f"  Fetching /markets/{_ticker}")
        _single = client._get(f"/markets/{_ticker}")
        print(json.dumps(_single, indent=2))
    else:
        print("  No open KORD markets to fetch individually")
except Exception as _e:
    print(f"  FAILED: {_e}")
