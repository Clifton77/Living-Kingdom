"""
Kalshi API probe — run locally to verify auth and bucket discovery.
Usage:  python test_kalshi.py
"""
import json, requests
from datetime import date, timedelta

KEY  = "aecddeb6-4791-4bb1-93dd-d8aa666a560b"
DEMO = "https://demo-api.kalshi.co/trade-api/v2"
LIVE = "https://trading-api.kalshi.com/trade-api/v2"

def try_get(base, path, headers, params=None, label=""):
    url = f"{base}{path}"
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        print(f"  [{r.status_code}] {label or path}")
        if r.status_code == 200:
            return r.json()
        else:
            print(f"         {r.text[:200]}")
    except Exception as e:
        print(f"  [ERR] {label or path}: {e}")
    return None

# ── Try auth header variants ─────────────────────────────────────────────────
print("=== Auth variants ===")
auth_variants = {
    "Bearer":  {"Authorization": f"Bearer {KEY}", "Accept": "application/json"},
    "ApiKey":  {"KALSHI-ACCESS-KEY": KEY, "Accept": "application/json"},
    "Token":   {"Authorization": f"Token {KEY}",  "Accept": "application/json"},
}

working_headers = None
working_base    = None

for label, headers in auth_variants.items():
    for base_label, base in [("DEMO", DEMO), ("LIVE", LIVE)]:
        data = try_get(base, "/portfolio/balance", headers, label=f"{label} / {base_label}")
        if data is not None:
            print(f"  ✓ Auth works: {label} on {base_label}")
            print(f"  Balance: {json.dumps(data, indent=4)}")
            working_headers = headers
            working_base    = base
            break
    if working_headers:
        break

if not working_headers:
    # Fall through — markets endpoint may still be public
    print("  No auth worked — trying public market endpoints anyway")
    working_headers = auth_variants["Bearer"]
    working_base    = LIVE

# ── Search for any KXHIGH markets (broad search) ────────────────────────────
print("\n=== Broad KXHIGH market search ===")
for base_label, base in [("LIVE", LIVE), ("DEMO", DEMO)]:
    data = try_get(base, "/markets",
                   working_headers,
                   params={"series_ticker": "KXHIGH", "limit": 5},
                   label=f"series search / {base_label}")
    if data and data.get("markets"):
        print(f"  Found {len(data['markets'])} markets on {base_label}")
        print(json.dumps(data["markets"][0], indent=4))
        break

# ── Today and tomorrow for KJFK ─────────────────────────────────────────────
today    = date.today()
tomorrow = today + timedelta(days=1)

for label, base in [("LIVE", LIVE), ("DEMO", DEMO)]:
    for d in [today, tomorrow]:
        tag   = d.strftime("%y%b%d").upper()
        event = f"KXHIGHJFK-{tag}"
        print(f"\n=== {event} ({label}) ===")
        data = try_get(base, "/markets",
                       working_headers,
                       params={"event_ticker": event, "limit": 20},
                       label=event)
        if data and data.get("markets"):
            mks = data["markets"]
            print(f"  {len(mks)} markets found")
            print("  First market:")
            print(json.dumps(mks[0], indent=4))
            print("\n  All tickers:")
            for m in mks:
                print(f"    {m.get('ticker','?'):40s}  "
                      f"bid={m.get('yes_bid','?'):4}  ask={m.get('yes_ask','?'):4}  "
                      f"vol={m.get('volume','?'):6}  status={m.get('status','?')}")
            break   # found — skip remaining date/base combos
