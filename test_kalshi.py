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
print("\n=== Raw dump of first bucket market (KJFK or first available) ===")
try:
    series  = get_series_ticker("KJFK")
    data    = client._get("/markets", params={"series_ticker": series, "status": "open", "limit": 1})
    markets = data.get("markets", [])
    if markets:
        print(json.dumps(markets[0], indent=2))
    else:
        # Fall back to any series
        series  = list(KALSHI_STATION_SERIES.values())[0]
        data    = client._get("/markets", params={"series_ticker": series, "status": "open", "limit": 1})
        markets = data.get("markets", [])
        if markets:
            print(json.dumps(markets[0], indent=2))
        else:
            print("  No open markets found")
except Exception as e:
    print(f"  FAILED: {e}")
