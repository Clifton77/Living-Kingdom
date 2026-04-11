"""
Kalshi API probe — run locally after setting up .env.

Required .env entries:
    KALSHI_API_KEY=aecddeb6-4791-4bb1-93dd-d8aa666a560b
    KALSHI_PRIVATE_KEY_PATH=C:/Users/clift/weather-bot/kalshi_private_key.pem

The private key PEM file comes from Kalshi → Account → API Keys → Download.
Run:  python test_kalshi.py
"""
import json
from datetime import date, timedelta
from kalshi_client import KalshiClient, build_event_id

client = KalshiClient(demo=False)   # use live API — real market data

today    = date.today()
tomorrow = today + timedelta(days=1)

# ── 1. Balance (auth test) ───────────────────────────────────────────────────
print("=== Auth / Balance ===")
try:
    bal = client.get_balance()
    print(f"  Balance: ${bal:.2f}  ✓ auth working")
except Exception as e:
    print(f"  FAILED: {e}")

# ── 2. Raw market list for today's KJFK ─────────────────────────────────────
for d in [today, tomorrow]:
    event = build_event_id("KJFK", d)
    print(f"\n=== Raw markets for {event} ===")
    try:
        data    = client._get("/markets", params={"event_ticker": event, "limit": 20})
        markets = data.get("markets", [])
        print(f"  {len(markets)} markets")
        if markets:
            print("  Keys:", list(markets[0].keys()))
            print("  First market:")
            print(json.dumps(markets[0], indent=4))
            print("\n  All tickers:")
            for m in markets:
                print(f"    {m.get('ticker','?'):45s} "
                      f"bid={m.get('yes_bid','?'):>4}  ask={m.get('yes_ask','?'):>4}  "
                      f"vol={m.get('volume','?'):>6}  status={m.get('status','?')}")
    except Exception as e:
        print(f"  FAILED: {e}")

# ── 3. Parsed snapshots ──────────────────────────────────────────────────────
print(f"\n=== Parsed snapshots KJFK {today} ===")
try:
    snaps = client.get_markets_for_station_date("KJFK", today)
    for s in snaps:
        print(f"  lower={s.bucket_lower:3d}  label={s.bucket_label:<18s} "
              f"bid={s.yes_bid:.2f}  ask={s.yes_ask:.2f}  vol={s.volume}")
    if not snaps:
        print("  None — try tomorrow:")
        snaps = client.get_markets_for_station_date("KJFK", tomorrow)
        for s in snaps:
            print(f"  lower={s.bucket_lower:3d}  label={s.bucket_label:<18s} "
                  f"bid={s.yes_bid:.2f}  ask={s.yes_ask:.2f}  vol={s.volume}")
except Exception as e:
    print(f"  FAILED: {e}")
