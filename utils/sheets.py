"""
Google Sheets live trade logger.

Writes to a single Google Spreadsheet with five tabs:
  Dashboard       — running P&L, bankroll, win rate (updated in place each cycle)
  Trade Log       — every trade open + close with full context
  Skipped Signals — every signal that didn't fire and why
  Model Accuracy  — forecast vs actual observed high per station per day
  EOD Summary     — end-of-day snapshot, one row per session

Authentication uses a Google Service Account (no browser required — ideal for
a 24/7 bot on a VPS). One-time setup:

  1. Go to console.cloud.google.com
  2. Create a project → Enable "Google Sheets API"
  3. IAM & Admin → Service Accounts → Create service account
  4. Create a JSON key → download it → save path in .env as GOOGLE_CREDENTIALS_JSON
  5. Open your Google Sheet → Share → add the service account email (from JSON) as Editor
  6. Copy the Sheet ID from the URL into .env as GOOGLE_SHEET_ID

All methods fail silently — a Sheets outage never crashes the bot.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from utils.logging_config import setup_logging
from config import GOOGLE_SHEET_ID, GOOGLE_CREDENTIALS_JSON, SHEET_TABS, SNAPSHOT_INTERVAL_MIN

logger = setup_logging("sheets")

# ---------------------------------------------------------------------------
# Tab header definitions — written once if sheet is empty
# ---------------------------------------------------------------------------

_HEADERS = {
    SHEET_TABS["dashboard"]: [[
        "Last Updated (UTC)", "Mode", "Bankroll ($)", "Available ($)",
        "Daily P&L ($)", "Total P&L ($)", "Open Positions",
        "Trades Today", "Wins", "Losses", "Win Rate (%)",
    ]],
    SHEET_TABS["trade_log"]: [[
        "Timestamp (UTC)", "Event Date", "Station", "Bucket", "Direction",
        "Entry Price", "Exit Price", "Contracts", "Stake ($)", "P&L ($)",
        "Exit Reason", "Cluster ID", "Season", "Pattern Confidence",
        "Weather Condition", "Edge at Entry", "Forecast Adjusted (°F)",
        "Bias Std (°F)", "N Obs",
    ]],
    SHEET_TABS["skipped"]: [[
        "Timestamp (UTC)", "Event Date", "Station", "Top Bucket",
        "Model Prob (%)", "Kalshi Prob (%)", "Edge", "Threshold",
        "Weather Condition", "Decision", "Skip Reason",
    ]],
    SHEET_TABS["model_accuracy"]: [[
        "Date", "Station", "Cluster ID", "Season",
        "Forecast Raw (°F)", "Forecast Adjusted (°F)",
        "Bias Mean (°F)", "Bias Std (°F)", "N Obs",
        "Observed High (°F)", "Error (°F)", "Bucket Hit",
    ]],
    SHEET_TABS["eod_summary"]: [[
        "Session Date", "Opening Bankroll ($)", "Closing Bankroll ($)",
        "Daily P&L ($)", "Total Trades", "Wins", "Losses",
        "Win Rate (%)", "Best Trade ($)", "Worst Trade ($)",
        "Open Positions at Close",
    ]],
    SHEET_TABS["snapshots"]: [[
        "Timestamp (UTC)", "Station", "Market ID", "Bucket (°F)",
        "Local Hour", "Obs Temp (°F)", "Running Max (°F)",
        "Yes Bid", "Yes Ask", "Edge", "Unrealized P/L ($)", "P/L (%)",
        "Hours Since Entry",
    ]],
}


# ---------------------------------------------------------------------------
# Logger class
# ---------------------------------------------------------------------------

class GoogleSheetsLogger:
    """
    Fail-safe wrapper around the Google Sheets API.
    All public methods catch and log exceptions — the bot never crashes here.
    """

    def __init__(self):
        self._service = None
        self._sheet_id = GOOGLE_SHEET_ID
        self._ready = False
        self._init_service()

    def _init_service(self):
        if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
            logger.warning(
                "Google Sheets not configured — "
                "set GOOGLE_CREDENTIALS_JSON and GOOGLE_SHEET_ID in .env"
            )
            return

        if not os.path.exists(GOOGLE_CREDENTIALS_JSON):
            logger.warning(
                "Credentials file not found: %s", GOOGLE_CREDENTIALS_JSON
            )
            return

        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build

            creds = service_account.Credentials.from_service_account_file(
                GOOGLE_CREDENTIALS_JSON,
                scopes=["https://www.googleapis.com/auth/spreadsheets"],
            )
            self._service = build("sheets", "v4", credentials=creds, cache_discovery=False)
            self._ready = True
            logger.info("Google Sheets connected | sheet_id=%s", self._sheet_id)
            self._ensure_headers()
        except Exception as exc:
            logger.warning("Google Sheets init failed: %s", exc)

    # ── Internal helpers ──────────────────────────────────────────────────

    def _append(self, tab: str, rows: list[list[Any]]):
        """Append one or more rows to a tab."""
        if not self._ready:
            return
        try:
            self._service.spreadsheets().values().append(
                spreadsheetId=self._sheet_id,
                range=f"{tab}!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": rows},
            ).execute()
        except Exception as exc:
            logger.warning("Sheets append to '%s' failed: %s", tab, exc)

    def _update(self, tab: str, cell_range: str, rows: list[list[Any]]):
        """Update a specific range in a tab (overwrites existing values)."""
        if not self._ready:
            return
        try:
            self._service.spreadsheets().values().update(
                spreadsheetId=self._sheet_id,
                range=f"{tab}!{cell_range}",
                valueInputOption="USER_ENTERED",
                body={"values": rows},
            ).execute()
        except Exception as exc:
            logger.warning("Sheets update '%s!%s' failed: %s", tab, cell_range, exc)

    def _get_row_count(self, tab: str) -> int:
        """Return number of rows currently in a tab."""
        if not self._ready:
            return 0
        try:
            result = self._service.spreadsheets().values().get(
                spreadsheetId=self._sheet_id,
                range=f"{tab}!A:A",
            ).execute()
            return len(result.get("values", []))
        except Exception:
            return 0

    def _ensure_headers(self):
        """Write header rows to any tab that is currently empty."""
        for tab, header_rows in _HEADERS.items():
            if self._get_row_count(tab) == 0:
                self._update(tab, "A1", header_rows)
                logger.info("Sheets: wrote headers to tab '%s'", tab)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # ── Public API ────────────────────────────────────────────────────────

    def update_dashboard(self, summary: dict, mode: str = "DEMO"):
        """
        Overwrite row 2 of the Dashboard tab with current bot state.
        Called at the end of every Tier 2 and Tier 3 cycle.
        """
        row = [[
            self._now(),
            mode,
            round(summary.get("bankroll", 0), 2),
            round(summary.get("available_capital", 0), 2),
            round(summary.get("daily_pnl", 0), 2),
            round(summary.get("realized_pnl", 0), 2),
            summary.get("open_positions", 0),
            summary.get("trade_count_today", 0),
            summary.get("wins_today", 0),
            summary.get("losses_today", 0),
            summary.get("win_rate", 0.0),
        ]]
        self._update(SHEET_TABS["dashboard"], "A2", row)

    def log_trade_opened(
        self,
        station: str,
        event_date,
        market_id: str,
        bucket_lower: int,
        entry_price: float,
        contracts: int,
        stake_usd: float,
        sig,                   # TradeSignal — for context fields
    ):
        """Append a row when a position is opened. Exit fields blank until close."""
        try:
            thr = sig.threshold_result
            row = [[
                self._now(),
                str(event_date),
                station,
                bucket_lower,
                "HIGH",
                round(entry_price, 4),
                "",                             # exit price — filled on close
                contracts,
                round(stake_usd, 2),
                "",                             # P&L — filled on close
                "",                             # exit reason — filled on close
                sig.cluster_id,
                sig.season,
                sig.pattern_confidence,
                thr.taf_condition if thr else "",
                round(sig.top_edge, 4),
                round(sig.forecast_adjusted, 1),
                round(sig.bias_std, 2),
                sig.n_obs,
            ]]
            self._append(SHEET_TABS["trade_log"], row)
        except Exception as exc:
            logger.warning("log_trade_opened failed: %s", exc)

    def log_trade_closed(
        self,
        market_id: str,
        exit_price: float,
        realized_pnl: float,
        exit_reason: str,
    ):
        """
        Find the most recent open row for this market_id in Trade Log
        and fill in exit price, P&L, and reason.
        Uses a search approach — scans column A (market_id is in col F area).
        For simplicity we append a close row; the dashboard reads totals from
        the risk summary, not individual rows.
        """
        try:
            row = [[
                self._now(),
                "",         # event_date already in open row
                "",         # station already in open row
                "",         # bucket already in open row
                "CLOSE",
                "",         # entry price in open row
                round(exit_price, 4),
                "",         # contracts in open row
                "",         # stake in open row
                round(realized_pnl, 4),
                exit_reason,
                "", "", "", "", "", "", "", "",
            ]]
            self._append(SHEET_TABS["trade_log"], row)
        except Exception as exc:
            logger.warning("log_trade_closed failed: %s", exc)

    def log_skipped_signal(self, sig, skip_reason: str):
        """Append a row to Skipped Signals for every non-TRADE decision."""
        try:
            thr = sig.threshold_result
            row = [[
                self._now(),
                str(sig.event_date),
                sig.station,
                sig.top_bucket,
                round(sig.top_model_prob * 100, 1),
                round(sig.top_kalshi_prob * 100, 1),
                round(sig.top_edge, 4),
                round(thr.threshold, 4) if thr else "",
                thr.taf_condition if thr else "",
                sig.decision,
                skip_reason,
            ]]
            self._append(SHEET_TABS["skipped"], row)
        except Exception as exc:
            logger.warning("log_skipped_signal failed: %s", exc)

    def log_model_accuracy(
        self,
        station: str,
        event_date,
        cluster_id: int,
        season: str,
        forecast_raw: float,
        forecast_adjusted: float,
        bias_mean: float,
        bias_std: float,
        n_obs: int,
        observed_high: float | None,
        bucket_hit: bool | None,
    ):
        """
        Append a row to Model Accuracy after settlement.
        observed_high and bucket_hit are None if IEM data not yet available.
        """
        try:
            error = round(observed_high - forecast_adjusted, 2) if observed_high is not None else ""
            row = [[
                str(event_date),
                station,
                cluster_id,
                season,
                round(forecast_raw, 1),
                round(forecast_adjusted, 1),
                round(bias_mean, 2),
                round(bias_std, 2),
                n_obs,
                round(observed_high, 1) if observed_high is not None else "",
                error,
                "Yes" if bucket_hit else ("No" if bucket_hit is False else ""),
            ]]
            self._append(SHEET_TABS["model_accuracy"], row)
        except Exception as exc:
            logger.warning("log_model_accuracy failed: %s", exc)

    def log_position_snapshot(
        self,
        station: str,
        market_id: str,
        bucket_lower: int,
        local_hour: int,
        obs_temp: float,
        running_max: float,
        yes_bid: float,
        yes_ask: float,
        edge: float,
        unrealized_pnl: float,
        pnl_pct: float,
        hours_since_entry: float,
    ):
        """Append a row to Position Snapshots (called every SNAPSHOT_INTERVAL_MIN while trade is open)."""
        try:
            row = [[
                self._now(),
                station,
                market_id,
                bucket_lower,
                local_hour,
                round(obs_temp, 1),
                round(running_max, 1),
                round(yes_bid, 4),
                round(yes_ask, 4),
                round(edge, 4),
                round(unrealized_pnl, 4),
                round(pnl_pct, 2),
                round(hours_since_entry, 2),
            ]]
            self._append(SHEET_TABS["snapshots"], row)
        except Exception as exc:
            logger.warning("log_position_snapshot failed: %s", exc)

    def log_eod_summary(self, summary: dict, session_date: str, mode: str = "DEMO"):
        """Append an end-of-day summary row."""
        try:
            positions = summary.get("open_positions", 0)
            row = [[
                session_date,
                "",                                   # opening bankroll not stored here
                round(summary.get("bankroll", 0), 2),
                round(summary.get("daily_pnl", 0), 2),
                summary.get("trade_count_today", 0),
                summary.get("wins_today", 0),
                summary.get("losses_today", 0),
                summary.get("win_rate", 0.0),
                "",   # best trade — would need per-trade tracking to compute
                "",   # worst trade
                positions,
            ]]
            self._append(SHEET_TABS["eod_summary"], row)
        except Exception as exc:
            logger.warning("log_eod_summary failed: %s", exc)


# ---------------------------------------------------------------------------
# Module-level singleton — import and use directly
# ---------------------------------------------------------------------------

_sheets_logger: GoogleSheetsLogger | None = None


def get_sheets_logger() -> GoogleSheetsLogger:
    global _sheets_logger
    if _sheets_logger is None:
        _sheets_logger = GoogleSheetsLogger()
    return _sheets_logger
