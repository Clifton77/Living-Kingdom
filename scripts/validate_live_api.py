"""
scripts/validate_live_api.py

Validates that get_markets_for_station_date returns well-formed MarketSnapshot
objects against the live Kalshi API, and that signal_engine_v2's bucket-mapping
and probability functions work correctly with those snapshots.

Run manually before first live trading:
    python scripts/validate_live_api.py

Exits with code 1 if any assertion fails.
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, datetime, timezone

from kalshi_client import KalshiClient, MarketSnapshot
from signal_engine_v2 import tmax_to_bucket, bucket_probability
from config import HRRR_STATION_SIGMA, HRRR_COLD_BIAS_F, STATION_CITY_NAMES

# Stations to validate: one desert (tight sigma), one mid-tier, one convective
TEST_STATIONS = ["KPHX", "KJFK", "KORD", "KLAX", "KDFW"]

PASS = "[PASS]"
FAIL = "[FAIL]"
WARN = "[WARN]"


def check(label: str, condition: bool, detail: str = "") -> bool:
    icon = PASS if condition else FAIL
    suffix = f"  {detail}" if detail else ""
    print(f"  {icon}  {label}{suffix}")
    return condition


def validate_station(client: KalshiClient, station: str, event_date: date) -> bool:
    city = STATION_CITY_NAMES.get(station, station)
    print(f"\n{'-'*60}")
    print(f"  {station}  {city}  ({event_date})")
    print(f"{'-'*60}")

    markets = client.get_markets_for_station_date(station, event_date)
    all_ok = True

    # -- Basic return -------------------------------------------------
    ok = check("Markets returned", len(markets) > 0, f"got {len(markets)}")
    all_ok &= ok
    if not ok:
        return False

    # -- Print bucket table --------------------------------------------
    print(f"\n  {'Bucket':<12} {'Ticker':<40} {'Bid':>6} {'Ask':>6} {'Vol':>6} {'Open'}")
    print(f"  {'-'*12} {'-'*40} {'-'*6} {'-'*6} {'-'*6} {'-'*4}")
    for m in markets:
        status = "yes" if m.is_open else "no"
        print(f"  {m.bucket_label:<12} {m.market_id:<40} {m.yes_bid:>6.3f} {m.yes_ask:>6.3f} {m.volume:>6} {status}")

    print()

    # -- Per-field assertions -----------------------------------------
    for m in markets:
        label = f"bucket {m.bucket_lower}"

        ok = check(f"{label}: bucket_lower is int",
                   isinstance(m.bucket_lower, int),
                   str(type(m.bucket_lower)))
        all_ok &= ok

        ok = check(f"{label}: bucket_lower in plausible range",
                   0 <= m.bucket_lower <= 130,
                   str(m.bucket_lower))
        all_ok &= ok

        ok = check(f"{label}: yes_bid in [0, 1]",
                   0.0 <= m.yes_bid <= 1.0,
                   f"{m.yes_bid:.4f}")
        all_ok &= ok

        ok = check(f"{label}: yes_ask in [0, 1]",
                   0.0 <= m.yes_ask <= 1.0,
                   f"{m.yes_ask:.4f}")
        all_ok &= ok

        ok = check(f"{label}: bid < ask (or both 0)",
                   m.yes_bid <= m.yes_ask,
                   f"bid={m.yes_bid:.3f} ask={m.yes_ask:.3f}")
        all_ok &= ok

        ok = check(f"{label}: market_id non-empty", bool(m.market_id), m.market_id)
        all_ok &= ok

        ok = check(f"{label}: station matches", m.station == station,
                   f"got {m.station!r}")
        all_ok &= ok

    # -- Market coverage ----------------------------------------------
    open_markets = [m for m in markets if m.is_open]
    ok = check("At least one open market", len(open_markets) > 0,
               f"{len(open_markets)}/{len(markets)} open")
    all_ok &= ok

    price_sum = sum(m.yes_ask for m in open_markets)
    ok = check("Ask prices sum ~1.0 (market complete)",
               0.9 <= price_sum <= 1.3,
               f"sum={price_sum:.3f}")
    all_ok &= ok

    # -- tmax_to_bucket mapping ----------------------------------------
    print(f"\n  tmax_to_bucket mapping:")
    lowers = sorted(m.bucket_lower for m in markets)
    floor_b = lowers[0]
    ceil_b  = lowers[-1]
    # Test a range of temps including tails and interior
    test_temps = [floor_b - 5, floor_b, floor_b + 1, floor_b + 3,
                  ceil_b - 1, ceil_b, ceil_b + 5]
    for t in test_temps:
        result = tmax_to_bucket(float(t), markets)
        ok = check(f"  tmax={t}F -> bucket {result}",
                   result is not None,
                   "(None = unmapped!)")
        all_ok &= ok

    # -- bucket_probability with station sigma -------------------------
    sigma = HRRR_STATION_SIGMA.get(station, 3.0)
    # Simulate a HRRR corrected TMAX near the middle bucket
    mid_bucket = lowers[len(lowers) // 2]
    tmax_test  = float(mid_bucket + 1)  # center of middle bucket
    p = bucket_probability(tmax_test, mid_bucket, sigma)
    ok = check(f"bucket_probability(tmax={tmax_test}, bucket={mid_bucket}, sigma={sigma})",
               0.0 < p < 1.0,
               f"p={p:.4f}")
    all_ok &= ok

    # NO ask computation (used in signal engine)
    for m in markets[:3]:
        no_ask_computed = 1.0 - m.yes_bid
        ok = check(f"bucket {m.bucket_lower}: no_ask = 1 - yes_bid = {no_ask_computed:.3f}",
                   0.0 <= no_ask_computed <= 1.0)
        all_ok &= ok

    return all_ok


def main() -> None:
    print("=" * 60)
    print("  Kalshi live API validation — signal_engine_v2 compatibility")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%Mz')}")
    print("=" * 60)

    client     = KalshiClient()
    event_date = date.today()
    results    = {}

    for station in TEST_STATIONS:
        try:
            results[station] = validate_station(client, station, event_date)
        except Exception as exc:
            print(f"\n  {FAIL}  {station}: EXCEPTION — {exc}")
            results[station] = False

    print(f"\n{'='*60}")
    print("  Summary")
    print(f"{'-'*60}")
    all_passed = True
    for station, ok in results.items():
        icon = PASS if ok else FAIL
        city = STATION_CITY_NAMES.get(station, station)
        print(f"  {icon}  {station}  {city}")
        all_passed &= ok

    print()
    if all_passed:
        print(f"  {PASS}  All checks passed — safe to run scheduler_v2")
    else:
        print(f"  {FAIL}  Some checks failed — review output above before trading")
    print("=" * 60)

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
