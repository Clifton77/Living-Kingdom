"""
Quick Kalshi API probe — run this locally to verify auth and bucket discovery.
Usage:  python test_kalshi.py
"""
import json
from datetime import date, timedelta
from kalshi_client import KalshiClient, build_event_id

client = KalshiClient()          # reads key from .env / config.py

# ── 1. Balance check (auth test) ─────────────────────────────────────────────
print("=== Auth / Balance ===")
try:
    bal = client.get_balance()
    print(f"  Balance: ${bal:.2f}")
except Exception as e:
    print(f"  FAILED: {e}")

# ── 2. Raw market list for tomorrow's KJFK ───────────────────────────────────
tomorrow = date.today() + timedelta(days=1)
event    = build_event_id("KJFK", tomorrow)
print(f"\n=== Raw markets for {event} ===")
try:
    data = client._get("/markets", params={"event_ticker": event, "limit": 50})
    markets = data.get("markets", [])
    print(f"  {len(markets)} markets returned")
    if markets:
        print("  First market raw keys:", list(markets[0].keys()))
        print("  First market sample:")
        print(json.dumps(markets[0], indent=4))
except Exception as e:
    print(f"  FAILED: {e}")

# ── 3. Parsed snapshots via get_markets_for_station_date ─────────────────────
print(f"\n=== Parsed snapshots for KJFK {tomorrow} ===")
try:
    snaps = client.get_markets_for_station_date("KJFK", tomorrow)
    if snaps:
        for s in snaps:
            print(
                f"  bucket_lower={s.bucket_lower:3d}  label={s.bucket_label:<18s}"
                f"  bid={s.yes_bid:.2f}  ask={s.yes_ask:.2f}"
                f"  vol={s.volume:5d}  open={s.is_open}"
            )
    else:
        print("  No snapshots parsed — check raw output above for clues")
except Exception as e:
    print(f"  FAILED: {e}")

# ── 4. Try KLAX as a second station ──────────────────────────────────────────
print(f"\n=== Parsed snapshots for KLAX {tomorrow} ===")
try:
    snaps = client.get_markets_for_station_date("KLAX", tomorrow)
    for s in snaps:
        print(f"  bucket_lower={s.bucket_lower:3d}  label={s.bucket_label:<18s}  bid={s.yes_bid:.2f}  ask={s.yes_ask:.2f}")
    if not snaps:
        print("  No snapshots")
except Exception as e:
    print(f"  FAILED: {e}")
