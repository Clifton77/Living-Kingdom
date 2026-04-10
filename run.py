"""
WeatherBot entry point.

Starts the APScheduler (background threads) then Flask (foreground).
Ctrl+C triggers the graceful shutdown handler registered in scheduler.py.

Usage:
    python run.py

Dashboard: http://localhost:5000  (or http://your-vps-ip:5000)
"""

from __future__ import annotations

from scheduler import start_scheduler
from dashboard import app
from config import DASHBOARD_HOST, DASHBOARD_PORT, USE_DEMO
from utils.logging_config import setup_logging

logger = setup_logging("run")


if __name__ == "__main__":
    mode = "DEMO (paper trading)" if USE_DEMO else "LIVE TRADING"
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
