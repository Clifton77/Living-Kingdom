"""
NBM vs Phase 4 backtest.

Compares four forecast sources as distribution means against 38K settled
Kalshi contracts from aligned.csv:
  - GFS_corrected   : GFS_high_f minus per-station Phase4 bias
  - ECMWF_corrected : ECMWF_high_f minus per-station Phase4 bias
  - Phase4_blended  : model-selected bias-corrected forecast (_MODEL_PREF)
  - NBM             : NBM_high_f from ncep_nbm_conus (no additional bias)

All four sources use the same ERA5 sigma from the bias table so the mean
is the only variable — a clean apples-to-apples comparison.

Metrics:
  MAE          — mean absolute error vs observed TMAX
  Brier score  — calibration of bucket probabilities (lower = better)
  Bucket hit   — top-prob bucket matches the settling bucket
  Sim P&L      — hypothetical profit buying 1 contract when edge >= MIN_EDGE
"""
from __future__ import annotations

import math
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats
from collections import defaultdict

from config import MIN_EDGE
from scripts.signal_engine import lookup_bias
from phase4_signal_generator import _MODEL_PREF, _GFS_BIAS, _ECMWF_BIAS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bucket_prob(mu: float, sigma: float, lower: int,
                 live_lower: int, live_upper: int) -> float:
    sigma = max(sigma, 1.0)
    dist  = scipy_stats.norm(loc=mu, scale=sigma)
    if lower == live_lower:
        lo, hi = -math.inf, lower + 0.5
    elif lower == live_upper:
        lo, hi = lower - 0.5, math.inf
    else:
        lo, hi = lower - 0.5, lower + 1.5
    return float(np.clip(dist.cdf(hi) - dist.cdf(lo), 0.0, 1.0))


def _build_dist(mu: float, sigma: float,
                buckets: list[int], live_lower: int, live_upper: int) -> dict[int, float]:
    raw   = {b: _bucket_prob(mu, sigma, b, live_lower, live_upper) for b in buckets}
    total = sum(raw.values())
    if total > 0:
        return {b: p / total for b, p in raw.items()}
    return raw


def _phase4_blended(station: str, gfs: float | None, ecmwf: float | None) -> float | None:
    pref   = _MODEL_PREF.get(station, "ECMWF")
    gfs_c  = gfs  - _GFS_BIAS.get(station, 0.0)  if gfs  is not None else None
    ecmwf_c= ecmwf- _ECMWF_BIAS.get(station, 0.0) if ecmwf is not None else None
    if pref == "GFS":
        return gfs_c
    if pref == "ECMWF":
        return ecmwf_c
    # BLEND
    if gfs_c is not None and ecmwf_c is not None:
        return (gfs_c + ecmwf_c) / 2.0
    return gfs_c or ecmwf_c


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # ── Load data ────────────────────────────────────────────────────────────
    print("Loading data...")
    df = pd.read_csv("study/data/aligned.csv", low_memory=False)
    df = df[df["NBM_high_f"].notna()].copy()
    df["date"]            = pd.to_datetime(df["date"]).dt.date
    df["settlement_date"] = pd.to_datetime(df["settlement_date"]).dt.date

    pl = pd.read_parquet("data/pattern_labels.parquet")[["date", "season", "cluster_id"]]
    pl["date"] = pd.to_datetime(pl["date"]).dt.date
    df = df.merge(pl, on="date", how="left")

    bias_df = pd.read_parquet("data/bias_table.parquet")

    # Reconstruct corrected forecasts
    df["gfs_c"]    = df.apply(lambda r: r["GFS_high_f"]  - _GFS_BIAS.get(r["station"], 0.0)
                               if pd.notna(r["GFS_high_f"])   else None, axis=1)
    df["ecmwf_c"]  = df.apply(lambda r: r["ECMWF_high_f"] - _ECMWF_BIAS.get(r["station"], 0.0)
                               if pd.notna(r["ECMWF_high_f"]) else None, axis=1)
    df["phase4_f"] = df.apply(lambda r: _phase4_blended(r["station"], r["GFS_high_f"], r["ECMWF_high_f"]), axis=1)

    required = ["phase4_f", "gfs_c", "ecmwf_c", "NBM_high_f", "observed_high_f", "cluster_id"]
    df = df.dropna(subset=required)
    print(f"  {len(df):,} contracts | {df['date'].nunique()} dates | {df['station'].nunique()} stations")

    SOURCES = {
        "GFS_corrected":   "gfs_c",
        "ECMWF_corrected": "ecmwf_c",
        "Phase4_blended":  "phase4_f",
        "NBM":             "NBM_high_f",
    }

    # ── Sigma cache (ERA5 — same for all sources for fair mean comparison) ──
    sigma_cache: dict[tuple, float] = {}

    def get_sigma(station: str, dt, cluster_id: int, season: str) -> float:
        key = (station, dt.month, season, int(cluster_id))
        if key not in sigma_cache:
            info = lookup_bias(bias_df, station, dt, int(cluster_id),
                               season, 70.0, model_source="ERA5")
            sigma_cache[key] = max(info["bias_std"], 1.0)
        return sigma_cache[key]

    # ── Accumulators ─────────────────────────────────────────────────────────
    mae_abs  = defaultdict(list)   # (source) → list of |forecast - observed|
    brier    = defaultdict(list)   # (source) → list of (p - outcome)^2
    hit      = defaultdict(list)   # (source) → list of bool (top bucket = settling)
    pnl      = defaultdict(list)   # (source) → list of contract P&L when edge >= MIN_EDGE
    n_trades = defaultdict(int)

    # ── Per station/date loop ─────────────────────────────────────────────────
    print("Running backtest...")
    groups = list(df.groupby(["station", "date"]))
    for i, ((station, dt), group) in enumerate(groups):
        if i % 500 == 0:
            print(f"  {i}/{len(groups)} station-dates processed...")

        cluster_id = int(group["cluster_id"].iloc[0])
        season     = group["season"].iloc[0]
        sigma      = get_sigma(station, dt, cluster_id, season)

        buckets    = sorted(group["temp_low"].dropna().astype(int).unique())
        if len(buckets) < 2:
            continue
        live_lower = buckets[0]
        live_upper = buckets[-1]

        # Find settling bucket (settled_yes == 1)
        settled_rows = group[group["settled_yes"] == 1]
        if settled_rows.empty:
            continue
        settling_bucket = int(settled_rows["temp_low"].iloc[0])

        observed = group["observed_high_f"].iloc[0]

        for label, col in SOURCES.items():
            mu = group[col].iloc[0]
            if pd.isna(mu):
                continue

            # MAE
            mae_abs[label].append(abs(mu - observed))

            # Full distribution (renormalized)
            dist = _build_dist(float(mu), sigma, buckets, live_lower, live_upper)

            # Bucket hit rate
            top_bucket = max(dist, key=dist.get)
            hit[label].append(top_bucket == settling_bucket)

            # Per-contract Brier + simulated P&L
            for _, crow in group.iterrows():
                b          = int(crow["temp_low"])
                outcome    = int(crow["settled_yes"])
                last_price = float(crow["last_price"])
                p          = dist.get(b, 0.0)

                brier[label].append((p - outcome) ** 2)

                edge = p - last_price
                if edge >= MIN_EDGE:
                    profit = (1.0 - last_price) if outcome == 1 else -last_price
                    pnl[label].append(profit)
                    n_trades[label] += 1

    # ── Results ───────────────────────────────────────────────────────────────
    print("\n" + "="*70)
    print("BACKTEST RESULTS  (NBM window: Oct 2024 - Feb 2026)")
    print("="*70)

    print(f"\n{'Source':<20} {'MAE (degF)':>10} {'Brier':>8} {'Hit Rate':>10} {'Sim P&L':>10} {'# Trades':>10}")
    print("-"*70)
    for label in SOURCES:
        mae_val  = np.mean(mae_abs[label]) if mae_abs[label]  else float("nan")
        brier_val= np.mean(brier[label])   if brier[label]    else float("nan")
        hit_rate = np.mean(hit[label])     if hit[label]      else float("nan")
        pnl_val  = sum(pnl[label])
        nt       = n_trades[label]
        print(f"{label:<20} {mae_val:>10.2f} {brier_val:>8.4f} {hit_rate:>9.1%} {pnl_val:>10.2f} {nt:>10,}")

    # ── Per-station MAE breakdown ─────────────────────────────────────────────
    print("\n-- Per-station MAE (degF) --")
    stations = sorted(df["station"].unique())
    header = f"{'Station':<8}" + "".join(f"{s[:12]:>14}" for s in SOURCES)
    print(header)
    print("-" * (8 + 14 * len(SOURCES)))

    for station in stations:
        sub = df[df["station"] == station]
        row_str = f"{station:<8}"
        for label, col in SOURCES.items():
            mae_s = (sub[col] - sub["observed_high_f"]).abs().mean()
            row_str += f"{mae_s:>14.2f}"
        print(row_str)

    print("\nDone.")


if __name__ == "__main__":
    main()
