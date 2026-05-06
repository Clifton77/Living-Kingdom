"""
run_v2.py — HRRR divergence bot entry point.

Starts scheduler_v2 (hourly HRRR loop + exit monitor) in a background
thread, then runs the Flask dashboard in the main thread.

Usage:
    python run_v2.py

Dashboard: http://localhost:5000
"""

from __future__ import annotations

import atexit
import os
import signal
import sys
import threading

from config import DASHBOARD_HOST, DASHBOARD_PORT, DRY_RUN, USE_DEMO
from utils.logging_config import setup_logging

logger = setup_logging("run_v2")

_PID_FILE = os.path.join(os.path.dirname(__file__), "weatherbot_v2.pid")


def _kill_existing() -> None:
    if not os.path.exists(_PID_FILE):
        return
    try:
        with open(_PID_FILE) as f:
            old_pid = int(f.read().strip())
        if old_pid == os.getpid():
            return
        os.kill(old_pid, signal.SIGTERM)
        logger.info("Killed existing instance (PID %d)", old_pid)
        import time; time.sleep(2)
    except (ProcessLookupError, ValueError, OSError):
        pass
    finally:
        try:
            os.remove(_PID_FILE)
        except FileNotFoundError:
            pass


def _write_pid() -> None:
    with open(_PID_FILE, "w") as f:
        f.write(str(os.getpid()))


if __name__ == "__main__":
    _kill_existing()
    _write_pid()
    atexit.register(lambda: os.remove(_PID_FILE) if os.path.exists(_PID_FILE) else None)

    if USE_DEMO:
        mode = "DEMO (paper trading)"
    elif DRY_RUN:
        mode = "LIVE (dry run — no orders placed)"
    else:
        mode = "LIVE TRADING"

    logger.info("=" * 60)
    logger.info("WeatherBot v2 starting — mode: %s", mode)
    logger.info("Dashboard: http://%s:%d", DASHBOARD_HOST, DASHBOARD_PORT)
    logger.info("=" * 60)

    # Start the scheduler loop in a daemon thread so it dies cleanly with the process
    import scheduler_v2
    sched_thread = threading.Thread(
        target=scheduler_v2.main,
        name="scheduler-v2",
        daemon=True,
    )
    sched_thread.start()
    logger.info("Scheduler thread started")

    # Flask in the main thread — use_reloader=False required (reloader would fork
    # the process and start a second scheduler loop)
    from dashboard_v2 import app
    app.run(
        host=DASHBOARD_HOST,
        port=DASHBOARD_PORT,
        debug=False,
        use_reloader=False,
        threaded=True,
    )
