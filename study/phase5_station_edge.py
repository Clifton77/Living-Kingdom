"""
Phase 5 — Per-Station Edge Analysis (40–70¢ tradeable zone).

Filters aligned.csv to the price range where Phase 4 places BUY_YES orders,
then computes win rate, edge, and Wilson 95% CI per station.
Suggests a Kelly multiplier: edge / 0.034 (baseline BUY edge from study),
capped to [0.5, 2.0].  Stations with n < 20 get 1.0 (neutral).

Outputs:
  study/data/phase5_station_edge.json
  study/data/phase5_station_edge.csv
"""

import json
import logging
import math
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("phase5")

# ── Constants ─────────────────────────────────────────────────────────────────
ALIGNED_CSV   = Path(__file__).parent / "data" / "aligned.csv"
OUT_JSON      = Path(__file__).parent / "data" / "phase5_station_edge.json"
OUT_CSV       = Path(__file__).parent / "data" / "phase5_station_edge.csv"

BUY_LO        = 0.40
BUY_HI        = 0.70
STRONG_BUY_LO = 0.60
MIN_N         = 20            # below this → insufficient data, mult = 1.0
BASELINE_EDGE = 0.034         # overall BUY_YES edge from Phase 4 study
MULT_FLOOR    = 0.50
MULT_CEIL     = 2.00

SEASON_MAP = {
    12: "Winter", 1: "Winter", 2: "Winter",
    3:  "Spring",  4: "Spring", 5: "Spring",
    6:  "Summer",  7: "Summer", 8: "Summer",
    9:  "Fall",   10: "Fall",  11: "Fall",
}


# ── Wilson 95% confidence interval for a proportion ───────────────────────────
def wilson_ci(wins: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (0.0, 0.0)
    p_hat = wins / n
    denom = 1 + z**2 / n
    centre = (p_hat + z**2 / (2 * n)) / denom
    margin = z * math.sqrt(p_hat * (1 - p_hat) / n + z**2 / (4 * n**2)) / denom
    return (round(max(centre - margin, 0.0), 4), round(min(centre + margin, 1.0), 4))


def _kelly_mult(edge: float, n: int, ci_lo_edge: float) -> tuple[float, str]:
    """
    Suggest a Kelly multiplier for a station.

    Rules:
    - n < MIN_N                     → 1.0 (insufficient data)
    - 95% CI lower bound on edge ≤ 0 → 1.0 (edge not reliably positive)
    - otherwise                     → edge / BASELINE_EDGE, capped [FLOOR, CEIL]
    """
    if n < MIN_N:
        return 1.0, f"INSUFFICIENT (n={n} < {MIN_N})"
    if ci_lo_edge <= 0.0:
        return 1.0, f"CI_INCLUDES_ZERO (ci_lo_edge={ci_lo_edge:+.4f})"
    raw = edge / BASELINE_EDGE
    mult = round(min(max(raw, MULT_FLOOR), MULT_CEIL), 2)
    return mult, f"edge={edge:+.4f} / baseline={BASELINE_EDGE} → raw={raw:.2f} → capped={mult}"


def analyse(df: pd.DataFrame, label: str) -> dict:
    """Compute stats for one DataFrame slice (station or station×season)."""
    n        = len(df)
    wins     = int(df["settled_yes"].sum())
    win_rate = round(float(df["settled_yes"].mean()), 4) if n > 0 else 0.0
    mean_ask = round(float(df["last_price"].mean()), 4) if n > 0 else 0.0
    edge     = round(win_rate - mean_ask, 4)
    ci_lo_wr, ci_hi_wr = wilson_ci(wins, n)
    ci_lo_edge = round(ci_lo_wr - mean_ask, 4)
    mult, note = _kelly_mult(edge, n, ci_lo_edge)

    # Sub-zone split
    strong = df[df["last_price"] >= STRONG_BUY_LO]
    buy    = df[df["last_price"] <  STRONG_BUY_LO]

    def _sub(sub: pd.DataFrame) -> dict:
        if len(sub) == 0:
            return {"n": 0}
        wr = round(float(sub["settled_yes"].mean()), 4)
        ma = round(float(sub["last_price"].mean()), 4)
        return {"n": len(sub), "win_rate": wr, "mean_ask": ma, "edge": round(wr - ma, 4)}

    return {
        "label":        label,
        "n":            n,
        "wins":         wins,
        "win_rate":     win_rate,
        "mean_ask":     mean_ask,
        "edge":         edge,
        "ci_wr_lo":     ci_lo_wr,
        "ci_wr_hi":     ci_hi_wr,
        "ci_edge_lo":   ci_lo_edge,
        "kelly_mult":   mult,
        "kelly_note":   note,
        "BUY":          _sub(buy),
        "STRONG_BUY":   _sub(strong),
    }


def run() -> None:
    if not ALIGNED_CSV.exists():
        log.error("aligned.csv not found at %s", ALIGNED_CSV)
        sys.exit(1)

    df = pd.read_csv(ALIGNED_CSV)
    df["settlement_date"] = pd.to_datetime(df["settlement_date"], errors="coerce")
    df["month"]  = df["settlement_date"].dt.month
    df["season"] = df["month"].map(SEASON_MAP)

    total = len(df)
    df = df[df["last_price"].between(BUY_LO, BUY_HI)].copy()
    log.info("Loaded %d total contracts → %d in %.0f–%.0f¢ tradeable zone",
             total, len(df), BUY_LO * 100, BUY_HI * 100)

    if len(df) == 0:
        log.error("No contracts in tradeable zone — check aligned.csv column 'last_price'")
        sys.exit(1)

    # ── Overall zone summary ───────────────────────────────────────────────────
    overall = analyse(df, "OVERALL")
    log.info("OVERALL  n=%d  win_rate=%.3f  mean_ask=%.3f  edge=%+.4f",
             overall["n"], overall["win_rate"], overall["mean_ask"], overall["edge"])

    # ── Per-station ────────────────────────────────────────────────────────────
    stations_sorted = sorted(df["station"].unique())
    station_results = {}
    rows = []

    log.info("")
    log.info("%-8s  %5s  %8s  %8s  %8s  %9s  %9s  %6s  %s",
             "Station", "n", "win_rate", "mean_ask", "edge",
             "ci_edge_lo", "ci_wr_hi", "k_mult", "note")
    log.info("-" * 100)

    for station in stations_sorted:
        sub = df[df["station"] == station]
        result = analyse(sub, station)
        station_results[station] = result

        log.info("%-8s  %5d  %8.3f  %8.3f  %+8.4f  %+9.4f  %9.3f  %6.2f  %s",
                 station,
                 result["n"],
                 result["win_rate"],
                 result["mean_ask"],
                 result["edge"],
                 result["ci_edge_lo"],
                 result["ci_wr_hi"],
                 result["kelly_mult"],
                 result["kelly_note"],
                 )

        rows.append({
            "station":     station,
            "n":           result["n"],
            "wins":        result["wins"],
            "win_rate":    result["win_rate"],
            "mean_ask":    result["mean_ask"],
            "edge":        result["edge"],
            "ci_edge_lo":  result["ci_edge_lo"],
            "ci_wr_lo":    result["ci_wr_lo"],
            "ci_wr_hi":    result["ci_wr_hi"],
            "kelly_mult":  result["kelly_mult"],
            "kelly_note":  result["kelly_note"],
            "n_BUY":       result["BUY"].get("n", 0),
            "win_BUY":     result["BUY"].get("win_rate", None),
            "edge_BUY":    result["BUY"].get("edge", None),
            "n_STRONG":    result["STRONG_BUY"].get("n", 0),
            "win_STRONG":  result["STRONG_BUY"].get("win_rate", None),
            "edge_STRONG": result["STRONG_BUY"].get("edge", None),
        })

    # ── Per-station × season ───────────────────────────────────────────────────
    log.info("")
    log.info("Per-station × season breakdown:")
    log.info("%-8s  %-7s  %5s  %8s  %8s  %8s  %6s",
             "Station", "Season", "n", "win_rate", "mean_ask", "edge", "k_mult")
    log.info("-" * 70)

    season_results: dict[str, dict] = {}
    for station in stations_sorted:
        season_results[station] = {}
        for season in ["Winter", "Spring", "Summer", "Fall"]:
            sub = df[(df["station"] == station) & (df["season"] == season)]
            r = analyse(sub, f"{station}/{season}")
            season_results[station][season] = r
            if r["n"] > 0:
                log.info("%-8s  %-7s  %5d  %8.3f  %8.3f  %+8.4f  %6.2f",
                         station, season, r["n"], r["win_rate"],
                         r["mean_ask"], r["edge"], r["kelly_mult"])

    # ── Suggested _STATION_KELLY_MULT dict (for copy-paste into code) ─────────
    log.info("")
    log.info("Suggested _STATION_KELLY_MULT dict (paste into phase4_signal_generator.py):")
    log.info("_STATION_KELLY_MULT: dict[str, float] = {")
    for station in stations_sorted:
        r = station_results[station]
        log.info('    "%-5s: %.2f,   # n=%d  edge=%+.4f  %s',
                 station + '"', r["kelly_mult"], r["n"], r["edge"], r["kelly_note"])
    log.info("}")

    # ── Save outputs ──────────────────────────────────────────────────────────
    output = {
        "overall":  overall,
        "stations": station_results,
        "by_station_season": season_results,
    }
    OUT_JSON.write_text(json.dumps(output, indent=2, default=str))
    log.info("Saved %s", OUT_JSON)

    pd.DataFrame(rows).to_csv(OUT_CSV, index=False)
    log.info("Saved %s", OUT_CSV)


if __name__ == "__main__":
    run()
