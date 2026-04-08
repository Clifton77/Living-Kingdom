"""
Script 3: K-means clustering on 500mb anomaly fields.

Builds separate seasonal cluster sets (DJF/MAM/JJA/SON).
Tunes K (8-12) via silhouette score.
Stores fitted StandardScaler + KMeans models for live classification.

Output:
  data/pattern_labels.parquet  {date, season, cluster_id}
  data/cluster_centroids.pkl   {season: {scaler, model, k}}
"""
import os
import sys
import pickle
import logging

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from tqdm import tqdm

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    SEASONS, K_RANGE, CLUSTER_RANDOM_STATE, CLUSTER_N_INIT,
    Z500_PARQUET, PATTERNS_PARQUET, CENTROIDS_PKL, LOGS_DIR,
)
from utils.logging_config import setup_logging

logger = setup_logging("build_pattern_clusters")


def assign_season(date_series: pd.Series) -> pd.Series:
    """
    Map each date to DJF/MAM/JJA/SON.
    December is assigned to DJF (standard meteorological convention).
    """
    months = pd.to_datetime(date_series).dt.month

    def _season(m):
        if m in [3, 4, 5]:
            return "MAM"
        elif m in [6, 7, 8]:
            return "JJA"
        elif m in [9, 10, 11]:
            return "SON"
        else:  # 12, 1, 2
            return "DJF"

    return months.map(_season)


def tune_k(X: np.ndarray, k_range: range, season: str) -> tuple[int, dict]:
    """
    Fit KMeans for each k in k_range and return the best k by silhouette score.
    Returns (best_k, scores_dict).
    """
    scores = {}
    for k in tqdm(k_range, desc=f"Tuning K ({season})"):
        km = KMeans(n_clusters=k, n_init=CLUSTER_N_INIT,
                    random_state=CLUSTER_RANDOM_STATE)
        labels = km.fit_predict(X)
        # Use sample_size for speed on large datasets
        n_samples = min(2000, len(X))
        score = silhouette_score(X, labels, sample_size=n_samples,
                                 random_state=CLUSTER_RANDOM_STATE)
        scores[k] = score
        logger.info("  K=%d silhouette=%.4f", k, score)

    best_k = max(scores, key=scores.get)
    logger.info("Best K for %s: %d (silhouette=%.4f)", season, best_k, scores[best_k])
    return best_k, scores


def fit_seasonal_clusters(
    anomaly_df: pd.DataFrame,
    season: str,
    k_range: range,
) -> tuple[StandardScaler, KMeans, int]:
    """
    Filter to season, scale, tune K, fit final KMeans.
    Returns (scaler, kmeans_model, best_k).
    """
    season_mask = assign_season(anomaly_df["date"]) == season
    subset = anomaly_df[season_mask].copy()

    feat_cols = [c for c in subset.columns if c.startswith("lat_")]
    X_raw = subset[feat_cols].values.astype(np.float32)

    logger.info("Season %s: %d samples, %d features", season, X_raw.shape[0], X_raw.shape[1])

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_raw)

    best_k, scores = tune_k(X_scaled, k_range, season)

    # Fit final model with best K
    km = KMeans(n_clusters=best_k, n_init=CLUSTER_N_INIT,
                random_state=CLUSTER_RANDOM_STATE)
    km.fit(X_scaled)

    return scaler, km, best_k


def build_pattern_clusters() -> None:
    os.makedirs(LOGS_DIR, exist_ok=True)

    if not os.path.exists(Z500_PARQUET):
        raise FileNotFoundError(
            f"z500_anomaly.parquet not found at {Z500_PARQUET}. "
            "Run build_500mb_database.py first."
        )

    logger.info("Loading z500_anomaly.parquet...")
    anomaly_df = pd.read_parquet(Z500_PARQUET)
    anomaly_df["date"] = pd.to_datetime(anomaly_df["date"])

    feat_cols = [c for c in anomaly_df.columns if c.startswith("lat_")]
    logger.info("Anomaly DataFrame: %d rows, %d gridpoint features", len(anomaly_df), len(feat_cols))

    all_labels = []
    centroids = {}

    for season in ["DJF", "MAM", "JJA", "SON"]:
        logger.info("=" * 50)
        logger.info("Processing season: %s", season)

        scaler, km, best_k = fit_seasonal_clusters(anomaly_df, season, K_RANGE)
        centroids[season] = {"scaler": scaler, "model": km, "k": best_k}

        # Predict labels for all dates in this season
        season_mask = assign_season(anomaly_df["date"]) == season
        subset = anomaly_df[season_mask].copy()
        X_raw = subset[feat_cols].values.astype(np.float32)
        X_scaled = scaler.transform(X_raw)
        labels = km.predict(X_scaled)

        season_labels = pd.DataFrame({
            "date": subset["date"].values,
            "season": season,
            "cluster_id": labels.astype(np.int8),
        })
        all_labels.append(season_labels)

        # Save silhouette scores log
        sil_path = os.path.join(LOGS_DIR, f"silhouette_scores_{season}.csv")
        _, scores = tune_k(
            scaler.transform(subset[feat_cols].values.astype(np.float32)),
            K_RANGE, f"{season}_verify"
        )
        pd.Series(scores, name="silhouette").to_csv(sil_path)
        logger.info("Silhouette scores saved to %s", sil_path)

    # Combine all seasons
    pattern_labels = pd.concat(all_labels, ignore_index=True)
    pattern_labels = pattern_labels.sort_values("date").reset_index(drop=True)

    # Save
    pattern_labels.to_parquet(PATTERNS_PARQUET, index=False)
    logger.info("Saved pattern_labels.parquet: %d rows", len(pattern_labels))

    with open(CENTROIDS_PKL, "wb") as f:
        pickle.dump(centroids, f)
    logger.info("Saved cluster_centroids.pkl with seasons: %s", list(centroids.keys()))

    # Summary
    for season in ["DJF", "MAM", "JJA", "SON"]:
        k = centroids[season]["k"]
        n = (pattern_labels["season"] == season).sum()
        logger.info("  %s: k=%d, n=%d days", season, k, n)


if __name__ == "__main__":
    build_pattern_clusters()
