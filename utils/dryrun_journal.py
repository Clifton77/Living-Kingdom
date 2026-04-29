"""
DRY_RUN trade journal — local JSONL audit trail for simulated trades.

Writes to logs/dryrun_journal.jsonl (one JSON object per line).
Only active when DRY_RUN=true; all functions are no-ops otherwise.

Each record has an "event" field: "entry" | "snapshot" | "exit"
plus a UTC timestamp "ts". This file is the primary record for
post-run analysis and bot fine-tuning when Google Sheets is not configured.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from config import DRY_RUN, LOGS_DIR

JOURNAL_PATH = os.path.join(LOGS_DIR, "dryrun_journal.jsonl")


def _write(record: dict) -> None:
    if not DRY_RUN:
        return
    record["ts"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(LOGS_DIR, exist_ok=True)
    with open(JOURNAL_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def log_entry(
    station: str,
    market_id: str,
    bucket_lower: int,
    entry_price: float,
    contracts: int,
    stake_usd: float,
    model_prob: float,
    edge: float,
    forecast_adjusted: float,
    sig,
    entry_side: str = "yes",
) -> None:
    """Record a simulated trade entry."""
    _write({
        "event":              "entry",
        "station":            station,
        "market_id":          market_id,
        "bucket_lower":       bucket_lower,
        "entry_price":        round(entry_price, 4),
        "contracts":          contracts,
        "stake_usd":          round(stake_usd, 2),
        "model_prob":         round(model_prob, 4),
        "edge":               round(edge, 4),
        "forecast_adjusted":  round(forecast_adjusted, 2),
        "cluster_id":         getattr(sig, "cluster_id", None),
        "season":             getattr(sig, "season", None),
        "bias_mean":          round(getattr(sig, "bias_mean", 0.0), 3),
        "bias_std":           round(getattr(sig, "bias_std", 0.0), 3),
        "entry_side":         entry_side,
    })


def log_snapshot(
    station: str,
    market_id: str,
    bucket_lower: int,
    yes_bid: float,
    yes_ask: float,
    model_prob: float,
    edge: float,
    running_max: float | None,
    local_hour: int,
) -> None:
    """Record a periodic price snapshot for an open simulated position."""
    _write({
        "event":        "snapshot",
        "station":      station,
        "market_id":    market_id,
        "bucket_lower": bucket_lower,
        "yes_bid":      round(yes_bid, 4),
        "yes_ask":      round(yes_ask, 4),
        "mid":          round((yes_bid + yes_ask) / 2, 4),
        "model_prob":   round(model_prob, 4),
        "edge":         round(edge, 4),
        "running_max":  round(running_max, 1) if running_max is not None else None,
        "local_hour":   local_hour,
    })


def log_exit(
    station: str,
    market_id: str,
    bucket_lower: int,
    exit_price: float,
    entry_price: float,
    contracts: int,
    reason: str,
) -> None:
    """Record a simulated trade exit with P&L."""
    simulated_pnl = round((exit_price - entry_price) * contracts * 100, 4)
    _write({
        "event":             "exit",
        "station":           station,
        "market_id":         market_id,
        "bucket_lower":      bucket_lower,
        "exit_price":        round(exit_price, 4),
        "entry_price":       round(entry_price, 4),
        "contracts":         contracts,
        "simulated_pnl_usd": simulated_pnl,
        "reason":            reason,
    })
