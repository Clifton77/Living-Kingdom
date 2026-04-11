"""
Kalshi API probe — find correct event ticker format for temperature markets.
Run:  python test_kalshi.py
"""
import json
from kalshi_client import KalshiClient

client = KalshiClient(demo=False)

# ── 1. List all available series ─────────────────────────────────────────────
print("=== Available series (first 30) ===")
try:
    data = client._get("/series", params={"limit": 30})
    for s in data.get("series", []):
        print(f"  {s.get('ticker','?'):30s}  {s.get('title','?')}")
except Exception as e:
    print(f"  FAILED: {e}")

# ── 2. Search events with "temperature" or "high" ────────────────────────────
print("\n=== Events matching 'temperature' ===")
try:
    data = client._get("/events", params={"limit": 20, "status": "open"})
    for ev in data.get("events", []):
        t = ev.get("title", "")
        if any(w in t.lower() for w in ["temp", "high", "weather", "kx"]):
            print(f"  {ev.get('event_ticker','?'):40s}  {t}")
except Exception as e:
    print(f"  FAILED: {e}")

# ── 3. Broad market search — any open market with KXHIGH or temp ─────────────
print("\n=== Open markets matching KXHIGH ===")
try:
    data = client._get("/markets", params={"limit": 20, "series_ticker": "KXHIGH"})
    markets = data.get("markets", [])
    print(f"  {len(markets)} markets found")
    for m in markets[:5]:
        print(f"  {m.get('ticker','?'):50s}  {m.get('title',m.get('subtitle','?'))[:60]}")
except Exception as e:
    print(f"  FAILED: {e}")

# ── 4. Try alternate ticker formats for JFK tomorrow ─────────────────────────
print("\n=== Trying alternate event ticker formats ===")
from datetime import date, timedelta
tomorrow = date.today() + timedelta(days=1)

formats = [
    f"KXHIGHJFK-{tomorrow.strftime('%y%b%d').upper()}",
    f"KXHIGH-JFK-{tomorrow.strftime('%y%b%d').upper()}",
    f"HIGHTEMP-JFK-{tomorrow.strftime('%Y-%m-%d')}",
    f"KXHIGH-KJFK-{tomorrow.strftime('%y%b%d').upper()}",
    f"KXHIGHJFK{tomorrow.strftime('%y%b%d').upper()}",
]
for ticker in formats:
    try:
        data = client._get("/markets", params={"event_ticker": ticker, "limit": 5})
        n = len(data.get("markets", []))
        print(f"  {ticker:45s}  → {n} markets")
        if n:
            for m in data["markets"]:
                print(f"      {m.get('ticker','?')}")
    except Exception as e:
        print(f"  {ticker:45s}  → ERROR: {e}")

# ── 5. Raw dump of first open event to see structure ─────────────────────────
print("\n=== First open event (raw) ===")
try:
    data = client._get("/events", params={"limit": 1, "status": "open"})
    events = data.get("events", [])
    if events:
        print(json.dumps(events[0], indent=2))
    else:
        print("  No open events returned")
except Exception as e:
    print(f"  FAILED: {e}")
