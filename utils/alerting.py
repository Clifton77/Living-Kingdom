"""
Alert notifications for critical bot events.

Channels (in priority order):
  1. Email via SMTP — configure ALERT_EMAIL_* in .env
  2. Desktop notification — via plyer if installed (silent fallback)

Both channels are optional and fail silently so alerts never crash the bot.

.env keys:
  ALERT_EMAIL_TO       = you@example.com
  ALERT_EMAIL_FROM     = bot@example.com
  ALERT_SMTP_HOST      = smtp.gmail.com
  ALERT_SMTP_PORT      = 587
  ALERT_SMTP_PASSWORD  = your-app-password

Leave ALERT_EMAIL_TO blank to disable email.
"""

from __future__ import annotations

import smtplib
import traceback
from datetime import datetime, timezone
from email.mime.text import MIMEText

from utils.logging_config import setup_logging
from config import (
    ALERT_EMAIL_TO,
    ALERT_EMAIL_FROM,
    ALERT_SMTP_HOST,
    ALERT_SMTP_PORT,
    ALERT_SMTP_PASSWORD,
)

logger = setup_logging("alerting")


# ---------------------------------------------------------------------------
# Core send
# ---------------------------------------------------------------------------

def send_alert(subject: str, message: str, level: str = "WARNING") -> bool:
    """
    Send an alert via all configured channels.
    Returns True if at least one channel succeeded.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    full_subject = f"[WeatherBot {level}] {subject}"
    full_message = f"{timestamp}\n\n{message}"

    sent = False

    # ── Email ─────────────────────────────────────────────────────────────
    if ALERT_EMAIL_TO and ALERT_EMAIL_FROM and ALERT_SMTP_PASSWORD:
        sent = _send_email(full_subject, full_message) or sent
    else:
        logger.debug("Email alerts not configured — skipping")

    # ── Desktop notification ──────────────────────────────────────────────
    sent = _send_desktop(subject, message[:200]) or sent

    if not sent:
        # Last resort — prominent log entry
        logger.critical("ALERT (no channel delivered): %s — %s", subject, message[:200])

    return sent


def _send_email(subject: str, body: str) -> bool:
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"]    = ALERT_EMAIL_FROM
        msg["To"]      = ALERT_EMAIL_TO

        with smtplib.SMTP(ALERT_SMTP_HOST, ALERT_SMTP_PORT, timeout=15) as smtp:
            smtp.starttls()
            smtp.login(ALERT_EMAIL_FROM, ALERT_SMTP_PASSWORD)
            smtp.send_message(msg)

        logger.info("Alert email sent: %s", subject)
        return True

    except Exception as exc:
        logger.warning("Alert email failed: %s", exc)
        return False


def _send_desktop(title: str, message: str) -> bool:
    try:
        from plyer import notification  # type: ignore
        notification.notify(
            title=f"WeatherBot — {title}",
            message=message,
            app_name="WeatherBot",
            timeout=10,
        )
        return True
    except ImportError:
        return False   # plyer not installed — silent fallback
    except Exception as exc:
        logger.debug("Desktop notification failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Typed alert helpers
# ---------------------------------------------------------------------------

def alert_kill_switch(reason: str = "manual"):
    send_alert(
        subject="Kill switch activated",
        message=(
            f"The kill switch has been activated ({reason}).\n"
            "All trading is halted. Review the dashboard to resume."
        ),
        level="CRITICAL",
    )


def alert_order_failure(station: str, market_id: str, error: str):
    send_alert(
        subject=f"Order failure — {station}",
        message=(
            f"Failed to place order for {market_id}.\n"
            f"Error: {error}\n\n"
            "Position was NOT opened. Check Kalshi and the logs."
        ),
        level="ERROR",
    )


def alert_reconciliation_mismatch(details: str):
    send_alert(
        subject="Position reconciliation mismatch",
        message=(
            "Local risk state does not match Kalshi positions on startup.\n\n"
            f"{details}\n\n"
            "State has been auto-corrected. Review the dashboard."
        ),
        level="WARNING",
    )


def alert_daily_loss_limit(daily_pnl: float, limit: float):
    send_alert(
        subject="Daily loss limit reached — trading halted",
        message=(
            f"Daily P/L: ${daily_pnl:+.2f}\n"
            f"Limit: -${limit:.2f}\n\n"
            "Bot has halted new entries. Existing positions still monitored.\n"
            "Deactivate kill switch from dashboard to resume tomorrow."
        ),
        level="WARNING",
    )


def alert_consecutive_errors(station: str, count: int, last_error: str):
    send_alert(
        subject=f"Consecutive errors — {station} ({count}x)",
        message=(
            f"{count} consecutive errors processing {station}.\n"
            f"Last error: {last_error}\n\n"
            "Check network connectivity and API status."
        ),
        level="WARNING",
    )


def alert_settlement_detected(station: str, market_id: str, pnl: float):
    outcome = "WIN" if pnl >= 0 else "LOSS"
    send_alert(
        subject=f"Settlement {outcome} — {station} {pnl:+.2f}",
        message=(
            f"Position settled overnight.\n"
            f"Market:  {market_id}\n"
            f"Station: {station}\n"
            f"Result:  {outcome} — ${pnl:+.4f}"
        ),
        level="INFO",
    )
