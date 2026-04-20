"""
WeatherBot entry point.

Starts the APScheduler (background threads) then Flask (foreground).
Ctrl+C triggers the graceful shutdown handler registered in scheduler.py.

Usage:
    python run.py

Dashboard: http://localhost:5000  (or http://your-vps-ip:5000)
"""

from __future__ import annotations

import os
import signal
import sys

from scheduler import start_scheduler
from dashboard import app
from config import DASHBOARD_HOST, DASHBOARD_PORT, USE_DEMO, DRY_RUN
from utils.logging_config import setup_logging

logger = setup_logging("run")

_PID_FILE = os.path.join(os.path.dirname(__file__), "weatherbot.pid")


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
    except (ProcessLookupError, ValueError):
        pass  # already dead
    finally:
        try:
            os.remove(_PID_FILE)
        except FileNotFoundError:
            pass


def _write_pid() -> None:
    with open(_PID_FILE, "w") as f:
        f.write(str(os.getpid()))


def _cleanup_pid() -> None:
    try:
        os.remove(_PID_FILE)
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    _kill_existing()
    _write_pid()
    import atexit
    atexit.register(_cleanup_pid)

    if USE_DEMO:
        mode = "DEMO (paper trading)"
    elif DRY_RUN:
        mode = "LIVE (dry run — no orders placed)"
    else:
        mode = "LIVE TRADING"
    logger.info("=" * 60)
    logger.info("WeatherBot starting — mode: %s", mode)
    logger.info("Dashboard: http://%s:%d", DASHBOARD_HOST, DASHBOARD_PORT)
    logger.info("=" * 60)

    # Start scheduler first — initializes risk manager, Kalshi client,
    # and runs startup reconciliation in background thread.
    scheduler = start_scheduler()

    # Flask in main thread — use_reloader=False is required when running
    # alongside APScheduler (reloader forks the process and breaks the scheduler).
    app.run(
        host=DASHBOARD_HOST,
        port=DASHBOARD_PORT,
        debug=False,
        use_reloader=False,
        threaded=True,
    )
