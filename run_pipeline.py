"""
Phase 1 pipeline orchestrator.

Runs all historical database build scripts in dependency order.
Each script is idempotent — skips if output already exists (unless --force).

Dependency order:
  obs, 500mb, forecasts  → no dependencies (can run in any order)
  clusters               → requires 500mb  (z500_anomaly.parquet)
  bias                   → requires obs, clusters, forecasts
  peak_hours             → independent (fetches its own IEM hourly data)

Usage:
  python run_pipeline.py                          # run all missing outputs
  python run_pipeline.py --force                  # re-run everything
  python run_pipeline.py --only obs               # run only obs database
  python run_pipeline.py --only 500mb             # run only 500mb database
  python run_pipeline.py --only clusters          # run only pattern clusters
  python run_pipeline.py --only forecasts         # run only forecast archive
  python run_pipeline.py --only bias              # run only bias table
  python run_pipeline.py --only peak_hours        # run only peak hours
  python run_pipeline.py --skip 500mb             # skip 500mb, run others
  python run_pipeline.py --only peak_hours --force  # re-fetch IEM hourly data
"""
import os
import sys
import time
import argparse
import logging
from datetime import datetime

from config import (
    OBS_PARQUET, Z500_PARQUET, PATTERNS_PARQUET,
    FCST_PARQUET, BIAS_PARQUET, PEAK_HOURS_PARQUET, LOGS_DIR,
)
from utils.logging_config import setup_logging

logger = setup_logging("run_pipeline")

STEPS = {
    "obs":       {"output": OBS_PARQUET,      "label": "Script 1: Build obs database"},
    "500mb":     {"output": Z500_PARQUET,      "label": "Script 2: Build 500mb database"},
    "clusters":   {"output": PATTERNS_PARQUET,   "label": "Script 3: Build pattern clusters",
                   "requires": ["500mb"]},
    "forecasts":  {"output": FCST_PARQUET,       "label": "Script 4: Build forecast archive"},
    "bias":       {"output": BIAS_PARQUET,       "label": "Script 5: Build bias table",
                   "requires": ["obs", "clusters", "forecasts"]},
    "peak_hours": {"output": PEAK_HOURS_PARQUET, "label": "Script 6: Build peak heating hours"},
}

RUN_ORDER = ["obs", "500mb", "forecasts", "clusters", "bias", "peak_hours"]


def run_step(name: str, force: bool) -> bool:
    """
    Run a pipeline step. Returns True if step ran successfully.
    Skips if output exists and force=False.
    """
    step = STEPS[name]
    output_path = step["output"]

    if not force and os.path.exists(output_path):
        logger.info("SKIP %s — output exists at %s", name, output_path)
        return True

    # Check dependencies
    for dep in step.get("requires", []):
        if not os.path.exists(STEPS[dep]["output"]):
            logger.error(
                "Cannot run '%s' — dependency '%s' output missing. Run '%s' first.",
                name, dep, dep
            )
            return False

    logger.info("=" * 60)
    logger.info("RUNNING: %s", step["label"])
    logger.info("=" * 60)
    start = time.time()

    try:
        if name == "obs":
            from scripts.build_obs_database import build_obs_database
            build_obs_database()
        elif name == "500mb":
            from scripts.build_500mb_database import build_500mb_database
            build_500mb_database()
        elif name == "clusters":
            from scripts.build_pattern_clusters import build_pattern_clusters
            build_pattern_clusters()
        elif name == "forecasts":
            from scripts.build_model_forecast_archive import build_model_forecast_archive
            build_model_forecast_archive()
        elif name == "bias":
            from scripts.build_bias_table import build_bias_table
            build_bias_table()
        elif name == "peak_hours":
            from scripts.build_peak_hours import build_peak_hours
            from utils.peak_hours import invalidate_cache as _phx
            # --force on the pipeline re-fetches IEM hourly obs from scratch
            build_peak_hours(force_fetch=force)
            _phx()   # clear in-process cache so scheduler picks up new curves

        elapsed = time.time() - start
        logger.info("DONE: %s in %.1f seconds", name, elapsed)
        return True

    except Exception as e:
        elapsed = time.time() - start
        logger.error("FAILED: %s after %.1f seconds: %s", name, elapsed, e, exc_info=True)
        return False


def main():
    parser = argparse.ArgumentParser(description="Weather bot Phase 1 pipeline")
    parser.add_argument("--force", action="store_true",
                        help="Re-run all steps even if output exists")
    parser.add_argument("--only", choices=list(STEPS.keys()),
                        help="Run only this step")
    parser.add_argument("--skip", choices=list(STEPS.keys()), action="append",
                        help="Skip this step (repeatable)")
    args = parser.parse_args()

    skip_set = set(args.skip or [])

    if args.only:
        steps_to_run = [args.only]
    else:
        steps_to_run = [s for s in RUN_ORDER if s not in skip_set]

    os.makedirs(LOGS_DIR, exist_ok=True)
    start_time = datetime.utcnow()
    logger.info("Pipeline start: %s", start_time.strftime("%Y-%m-%d %H:%M UTC"))
    logger.info("Steps to run: %s", steps_to_run)

    results = {}
    for step_name in steps_to_run:
        success = run_step(step_name, force=args.force)
        results[step_name] = success
        if not success and step_name != steps_to_run[-1]:
            # Check if any downstream steps depend on this failed step
            downstream = [
                s for s in steps_to_run
                if step_name in STEPS.get(s, {}).get("requires", [])
            ]
            if downstream:
                logger.error(
                    "Halting — '%s' failed and downstream steps %s depend on it",
                    step_name, downstream
                )
                break

    # Summary
    elapsed_total = (datetime.utcnow() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY (%.0f seconds total)", elapsed_total)
    for step_name, success in results.items():
        status = "OK" if success else "FAILED"
        logger.info("  %-12s %s", step_name, status)

    failed = [k for k, v in results.items() if not v]
    if failed:
        logger.error("Pipeline completed with failures: %s", failed)
        sys.exit(1)
    else:
        logger.info("All steps completed successfully.")


if __name__ == "__main__":
    main()
