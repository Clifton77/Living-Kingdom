"""
Script 5: Build conditional bias correction table.

Joins:
  obs_daily.parquet      (station, date, tmax_observed_f)
  pattern_labels.parquet (date, season, cluster_id)
  model_fcst.parquet     (station, date, forecast_tmax_f, source)

Groups by (station, month, cluster_id, model_bin) and computes:
  bias_mean, bias_std, bias_skew, n_obs

Output: data/bias_table.parquet

NOTE: cluster_id is season-scoped — cluster 3 in DJF != cluster 3 in JJA.
The join includes season to preserve this distinction.
"""
import os
import sys
import logging

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    OBS_PARQUET, PATTERNS_PARQUET, FCST_PARQUET, BIAS_PARQUET,
    MODEL_BIN_SIZE, MIN_N_OBS, LOGS_DIR,
)
from utils.logging_config import setup_logging

logger = setup_logging("build_bias_table")


def bin_forecast(forecast_f: float, bin_size: int = MODEL_BIN_SIZE) -> float:
    """
    Round forecast to nearest bin_size bucket.
    E.g. bin_size=2: 73.1→74, 72.9→72, 73.0→74 (round-half-up)
    """
    return round(forecast_f / bin_size) * bin_size


def compute_bias_stats(group: pd.Series) -> pd.Series:
    """
    Compute bias statistics for a group of (observed - forecast) values.
    Returns {bias_mean, bias_std, bias_skew, n_obs}.
    """
    n = len(group.dropna())
    if n == 0:
        return pd.Series({
            "bias_mean": float("nan"),
            "bias_std":  float("nan"),
            "bias_skew": float("nan"),
            "n_obs": 0,
        })
    bias_mean = group.mean()
    bias_std  = group.std(ddof=1) if n >= 2 else float("nan")
    bias_skew = stats.skew(group.dropna(), bias=False) if n >= 3 else float("nan")
    return pd.Series({
        "bias_mean": float(bias_mean),
        "bias_std":  float(bias_std),
        "bias_skew": float(bias_skew),
        "n_obs":     int(n),
    })


def build_bias_table() -> None:
    os.makedirs(LOGS_DIR, exist_ok=True)

    # Load inputs
    for path, name in [
        (OBS_PARQUET,      "obs_daily.parquet"),
        (PATTERNS_PARQUET, "pattern_labels.parquet"),
        (FCST_PARQUET,     "model_fcst.parquet"),
    ]:
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"{name} not found at {path}. Run preceding pipeline scripts first."
            )

    logger.info("Loading obs_daily.parquet...")
    obs_df = pd.read_parquet(OBS_PARQUET)
    obs_df["date"] = pd.to_datetime(obs_df["date"])

    logger.info("Loading pattern_labels.parquet...")
    pat_df = pd.read_parquet(PATTERNS_PARQUET)
    pat_df["date"] = pd.to_datetime(pat_df["date"])

    logger.info("Loading model_fcst.parquet...")
    fcst_df = pd.read_parquet(FCST_PARQUET)
    fcst_df["date"] = pd.to_datetime(fcst_df["date"])

    logger.info("Obs rows: %d | Pattern rows: %d | Forecast rows: %d",
                len(obs_df), len(pat_df), len(fcst_df))

    # Join obs × patterns
    merged = pd.merge(obs_df, pat_df[["date", "season", "cluster_id"]], on="date", how="inner")
    logger.info("After obs×pattern join: %d rows", len(merged))

    # Join × forecasts (station + date)
    merged = pd.merge(
        merged,
        fcst_df[["station", "date", "forecast_tmax_f"]],
        on=["station", "date"],
        how="inner",
    )
    logger.info("After forecast join: %d rows", len(merged))

    # Drop rows with missing values
    merged = merged.dropna(subset=["tmax_observed_f", "forecast_tmax_f"])
    logger.info("After dropping NaN: %d rows", len(merged))

    # Compute bias and model bin
    merged["bias"] = merged["tmax_observed_f"] - merged["forecast_tmax_f"]
    merged["month"] = merged["date"].dt.month
    merged["model_bin"] = merged["forecast_tmax_f"].apply(bin_forecast)

    # Group and compute stats
    group_cols = ["station", "month", "season", "cluster_id", "model_bin"]
    logger.info("Grouping by %s...", group_cols)

    def _bias_mean(x):
        return float(x.mean())

    def _bias_std(x):
        d = x.dropna()
        return float(d.std(ddof=1)) if len(d) >= 2 else float("nan")

    def _bias_skew(x):
        d = x.dropna()
        return float(stats.skew(d, bias=False)) if len(d) >= 3 else float("nan")

    def _n_obs(x):
        return int(len(x.dropna()))

    bias_table = (
        merged.groupby(group_cols)["bias"]
        .agg(
            bias_mean=_bias_mean,
            bias_std=_bias_std,
            bias_skew=_bias_skew,
            n_obs=_n_obs,
        )
        .reset_index()
    )

    # Cast types
    bias_table["n_obs"]      = bias_table["n_obs"].astype("int16")
    bias_table["cluster_id"] = bias_table["cluster_id"].astype("int8")
    bias_table["month"]      = bias_table["month"].astype("int8")
    bias_table["model_bin"]  = bias_table["model_bin"].astype("float32")
    bias_table["bias_mean"]  = bias_table["bias_mean"].astype("float32")
    bias_table["bias_std"]   = bias_table["bias_std"].astype("float32")
    bias_table["bias_skew"]  = bias_table["bias_skew"].astype("float32")

    # Save
    bias_table.to_parquet(BIAS_PARQUET, index=False)
    logger.info("Saved bias_table.parquet: %d cells", len(bias_table))

    # Summary stats
    stats_path = os.path.join(LOGS_DIR, "bias_table_stats.txt")
    with open(stats_path, "w") as f:
        f.write("=== Bias Table Summary ===\n\n")
        f.write(f"Total cells: {len(bias_table)}\n")
        f.write(f"Cells with n_obs >= {MIN_N_OBS}: "
                f"{(bias_table['n_obs'] >= MIN_N_OBS).sum()}\n")
        f.write(f"Cells with n_obs < {MIN_N_OBS} (low confidence): "
                f"{(bias_table['n_obs'] < MIN_N_OBS).sum()}\n\n")
        f.write("n_obs distribution:\n")
        f.write(bias_table["n_obs"].describe().to_string())
        f.write("\n\nBias mean by station:\n")
        f.write(bias_table.groupby("station")["bias_mean"].mean().to_string())
        f.write("\n\nBias std by station:\n")
        f.write(bias_table.groupby("station")["bias_std"].mean().to_string())

    logger.info("Bias table stats saved to %s", stats_path)

    # Spot check: KLAX summer clusters should show negative bias (marine layer cools vs model)
    klax_summer = bias_table[
        (bias_table["station"] == "KLAX") &
        (bias_table["month"].isin([6, 7, 8]))
    ]
    if len(klax_summer) > 0:
        mean_klax_bias = klax_summer["bias_mean"].mean()
        logger.info(
            "KLAX summer bias_mean: %.2f°F (expect negative — marine layer cool bias)",
            mean_klax_bias
        )


if __name__ == "__main__":
    build_bias_table()
