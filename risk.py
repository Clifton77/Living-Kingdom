"""
Risk management module.

Responsibilities:
  - Track open positions and bankroll state
  - Enforce stop-loss (static: 40% of entry value)
  - Detect signal reversal stop (edge flips negative past threshold)
  - Early profit exit (bid reaches 85¢ and high is locked in bucket)
  - Daily loss limit halt
  - Position sizing guard (max 50% exposure)
  - Per-position exit decision on each Tier 2 (30-min) cycle

State is stored in-memory during a session and persisted to a JSON
file so the bot can resume after a restart.
"""

from __future__ import annotations

import json
import os
import math
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timezone
from typing import Optional

from utils.logging_config import setup_logging
from config import (
    STARTING_BANKROLL,
    STOP_LOSS_PCT,
    REVERSAL_EDGE_THRESHOLD,
    DAILY_LOSS_LIMIT_PCT,
    MAX_EXPOSURE_PCT,
    EARLY_EXIT_BID_THRESHOLD,
    LOGS_DIR,
    MIN_KELLY_STAKE,
)

logger = setup_logging("risk")

STATE_FILE = os.path.join(LOGS_DIR, "risk_state.json")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class OpenPosition:
    station:        str
    market_id:      str
    bucket_lower:   int
    contracts:      int
    entry_price:    float          # per contract, 0–1 fraction
    entry_usd:      float          # total invested
    entry_time:     str            # ISO UTC
    event_date:     str            # YYYY-MM-DD

    # Updated on each Tier 2 cycle
    current_bid:    float  = 0.0
    current_ask:    float  = 0.0
    unrealized_pnl: float  = 0.0
    pnl_pct:        float  = 0.0
    exit_value:     float  = 0.0
    last_edge:      float  = 0.0   # most recent edge from signal engine

    # Flags
    stop_loss_triggered:    bool = False
    reversal_triggered:     bool = False
    early_exit_triggered:   bool = False
    manually_closed:        bool = False


@dataclass
class RiskState:
    bankroll:           float
    starting_bankroll:  float
    daily_pnl:          float          = 0.0
    realized_pnl:       float          = 0.0
    session_date:       str            = ""
    kill_switch_active: bool           = False
    positions:          dict[str, OpenPosition] = field(default_factory=dict)
    trade_count_today:  int            = 0
    wins_today:         int            = 0
    losses_today:       int            = 0
    # Stations blocked from re-entry today (reversal stop fired)
    reversal_blocked:   list[str]      = field(default_factory=list)


# ---------------------------------------------------------------------------
# Exit decision
# ---------------------------------------------------------------------------

@dataclass
class ExitDecision:
    should_exit:    bool
    reason:         str
    urgency:        str        # "immediate" | "recommended" | "none"


def evaluate_exit(
    pos: OpenPosition,
    current_bid: float,
    current_edge: float,
    current_obs_temp: float | None = None,
) -> ExitDecision:
    """
    Evaluate whether an open position should be exited.

    Checks in priority order:
    1. Static stop-loss     — bid fell to ≤ STOP_LOSS_PCT of entry
    2. Signal reversal stop — edge reversed past REVERSAL_EDGE_THRESHOLD
    3. Early profit exit    — bid ≥ EARLY_EXIT_BID_THRESHOLD (85¢) AND
                               current observed temp is in or above bucket
    4. Hold                 — no exit condition met

    Parameters
    ----------
    pos              : open position being evaluated
    current_bid      : current Yes bid price (0–1)
    current_edge     : most recent edge from signal engine (can be negative)
    current_obs_temp : current METAR observed temperature in °F (optional)
    """
    # ── 1. Static stop-loss ───────────────────────────────────────────────
    stop_trigger = pos.entry_price * STOP_LOSS_PCT
    if current_bid <= stop_trigger:
        return ExitDecision(
            should_exit=True,
            reason=(
                f"Stop-loss triggered: bid {current_bid:.2f} ≤ "
                f"{stop_trigger:.2f} ({STOP_LOSS_PCT:.0%} of entry {pos.entry_price:.2f})"
            ),
            urgency="immediate",
        )

    # ── 2. Signal reversal stop ───────────────────────────────────────────
    if current_edge < REVERSAL_EDGE_THRESHOLD:
        return ExitDecision(
            should_exit=True,
            reason=(
                f"Reversal stop: edge {current_edge:+.3f} below "
                f"threshold {REVERSAL_EDGE_THRESHOLD:+.3f}"
            ),
            urgency="immediate",
        )

    # ── 3. Early profit exit ──────────────────────────────────────────────
    if current_bid >= EARLY_EXIT_BID_THRESHOLD:
        # Only exit early if current temp is already in or above the bucket
        if current_obs_temp is not None:
            bucket_lo = pos.bucket_lower
            bucket_hi = bucket_lo + 2          # 2°F wide bin
            temp_in_or_above_bucket = current_obs_temp >= bucket_lo

            if temp_in_or_above_bucket:
                return ExitDecision(
                    should_exit=True,
                    reason=(
                        f"Early profit exit: bid {current_bid:.2f} ≥ {EARLY_EXIT_BID_THRESHOLD:.2f} "
                        f"and current temp {current_obs_temp:.1f}°F is in/above bucket "
                        f"{bucket_lo}–{bucket_hi}°F. Locking in profit."
                    ),
                    urgency="recommended",
                )
        else:
            # No METAR data — take profit at high bid anyway
            return ExitDecision(
                should_exit=True,
                reason=(
                    f"Early profit exit: bid {current_bid:.2f} ≥ {EARLY_EXIT_BID_THRESHOLD:.2f}. "
                    "No METAR available to confirm — exiting conservatively."
                ),
                urgency="recommended",
            )

    return ExitDecision(should_exit=False, reason="Hold — no exit condition met", urgency="none")


# ---------------------------------------------------------------------------
# Risk manager
# ---------------------------------------------------------------------------

class RiskManager:
    """
    Stateful risk manager. One instance per bot session.
    Persists state to JSON for crash recovery.
    """

    def __init__(self):
        self.state = self._load_state()
        self._reset_if_new_day()

    # ── State persistence ─────────────────────────────────────────────────

    def _load_state(self) -> RiskState:
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    data = json.load(f)
                positions = {
                    k: OpenPosition(**v)
                    for k, v in data.pop("positions", {}).items()
                }
                state = RiskState(**data)
                state.positions = positions
                logger.info(
                    "Loaded risk state: bankroll=$%.2f, %d open positions",
                    state.bankroll, len(state.positions),
                )
                return state
            except Exception as exc:
                logger.warning("Failed to load risk state: %s — starting fresh", exc)

        return RiskState(
            bankroll=STARTING_BANKROLL,
            starting_bankroll=STARTING_BANKROLL,
            session_date=date.today().isoformat(),
        )

    def _save_state(self):
        data = asdict(self.state)
        os.makedirs(LOGS_DIR, exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def _reset_if_new_day(self):
        today = date.today().isoformat()
        if self.state.session_date != today:
            logger.info("New trading day — resetting daily counters")
            self.state.daily_pnl         = 0.0
            self.state.trade_count_today = 0
            self.state.wins_today        = 0
            self.state.losses_today      = 0
            self.state.reversal_blocked  = []   # clear reversal blocks each day
            self.state.session_date      = today
            self._save_state()

    def is_reversal_blocked(self, station: str) -> bool:
        """True if a reversal stop fired for this station today — no re-entry allowed."""
        return station in self.state.reversal_blocked

    def _block_station(self, station: str):
        """Mark station as blocked from re-entry for the rest of today."""
        if station not in self.state.reversal_blocked:
            self.state.reversal_blocked.append(station)
            self._save_state()
            logger.warning(
                "%s blocked from re-entry today (reversal stop fired)", station
            )

    # ── Kill switch ───────────────────────────────────────────────────────

    def activate_kill_switch(self):
        self.state.kill_switch_active = True
        self._save_state()
        logger.warning("KILL SWITCH ACTIVATED — all trading halted")

    def deactivate_kill_switch(self):
        self.state.kill_switch_active = False
        self._save_state()
        logger.info("Kill switch deactivated")

    @property
    def is_halted(self) -> bool:
        if self.state.kill_switch_active:
            return True
        daily_loss = -self.state.daily_pnl
        limit = self.state.bankroll * DAILY_LOSS_LIMIT_PCT
        if daily_loss >= limit:
            logger.warning(
                "Daily loss limit reached: -$%.2f ≥ $%.2f (%.0f%% of bankroll)",
                daily_loss, limit, DAILY_LOSS_LIMIT_PCT * 100,
            )
            return True
        return False

    # ── Exposure check ────────────────────────────────────────────────────

    def total_exposure(self) -> float:
        """Total USD currently at risk across all open positions."""
        return sum(p.entry_usd for p in self.state.positions.values())

    def can_open_position(self, stake_usd: float, station: str = "") -> tuple[bool, str]:
        """Check if opening a new position of stake_usd is within risk limits."""
        if self.is_halted:
            return False, "Trading halted (kill switch or daily loss limit)"

        if station and self.is_reversal_blocked(station):
            return False, f"{station} blocked from re-entry today (reversal stop fired earlier)"

        max_exposure = self.state.bankroll * MAX_EXPOSURE_PCT
        if self.total_exposure() + stake_usd > max_exposure:
            return False, (
                f"Exposure limit: current ${self.total_exposure():.2f} + "
                f"new ${stake_usd:.2f} > max ${max_exposure:.2f} "
                f"({MAX_EXPOSURE_PCT:.0%} of bankroll)"
            )

        if self.state.bankroll - self.total_exposure() < stake_usd:
            return False, f"Insufficient available capital (${self.available_capital:.2f})"

        return True, "OK"

    @property
    def available_capital(self) -> float:
        return max(0.0, self.state.bankroll - self.total_exposure())

    # ── Position lifecycle ────────────────────────────────────────────────

    def open_position(
        self,
        station: str,
        market_id: str,
        bucket_lower: int,
        contracts: int,
        entry_price: float,
        event_date: date,
    ) -> OpenPosition | None:
        """
        Record a new open position. Returns None if risk checks fail.
        """
        stake_usd = round(entry_price * contracts, 4)
        ok, reason = self.can_open_position(stake_usd)
        if not ok:
            logger.warning("Cannot open position for %s: %s", station, reason)
            return None

        pos = OpenPosition(
            station=station,
            market_id=market_id,
            bucket_lower=bucket_lower,
            contracts=contracts,
            entry_price=entry_price,
            entry_usd=stake_usd,
            entry_time=datetime.now(timezone.utc).isoformat(timespec="seconds"),
            event_date=event_date.isoformat(),
            current_bid=entry_price,
            current_ask=entry_price,
        )

        self.state.positions[market_id] = pos
        self.state.trade_count_today += 1
        self._save_state()

        logger.info(
            "Position opened: %s | %s | %d contracts @ $%.2f | total $%.2f",
            station, market_id, contracts, entry_price, stake_usd,
        )
        return pos

    def update_position(
        self,
        market_id: str,
        current_bid: float,
        current_ask: float,
        current_edge: float,
        current_obs_temp: float | None = None,
    ) -> ExitDecision:
        """
        Update position market data and evaluate exit conditions.
        Returns ExitDecision for the scheduler to act on.
        """
        pos = self.state.positions.get(market_id)
        if pos is None:
            return ExitDecision(False, "Position not found", "none")

        pos.current_bid      = current_bid
        pos.current_ask      = current_ask
        pos.last_edge        = current_edge
        pos.unrealized_pnl   = round((current_bid - pos.entry_price) * pos.contracts, 4)
        pos.pnl_pct          = round(pos.unrealized_pnl / pos.entry_usd * 100, 2) if pos.entry_usd else 0.0
        pos.exit_value       = round(current_bid * pos.contracts, 4)

        self._save_state()

        return evaluate_exit(pos, current_bid, current_edge, current_obs_temp)

    def close_position(
        self,
        market_id: str,
        exit_price: float,
        reason: str,
    ) -> float:
        """
        Record a closed position. Returns realized P/L in USD.
        """
        pos = self.state.positions.pop(market_id, None)
        if pos is None:
            logger.warning("close_position: %s not found in positions", market_id)
            return 0.0

        realized = round((exit_price - pos.entry_price) * pos.contracts, 4)
        self.state.daily_pnl    += realized
        self.state.realized_pnl += realized
        self.state.bankroll     += realized    # paper trading: update bankroll

        if realized >= 0:
            self.state.wins_today += 1
        else:
            self.state.losses_today += 1

        # Block re-entry if exit was triggered by a reversal stop
        if "reversal" in reason.lower():
            self._block_station(pos.station)

        self._save_state()

        logger.info(
            "Position closed: %s | exit $%.2f | P/L $%+.4f | reason: %s",
            market_id, exit_price, realized, reason,
        )
        return realized

    # ── Summary ───────────────────────────────────────────────────────────

    def summary(self) -> dict:
        return {
            "bankroll":          round(self.state.bankroll, 2),
            "available_capital": round(self.available_capital, 2),
            "total_exposure":    round(self.total_exposure(), 2),
            "daily_pnl":         round(self.state.daily_pnl, 2),
            "realized_pnl":      round(self.state.realized_pnl, 2),
            "unrealized_pnl":    round(
                sum(p.unrealized_pnl for p in self.state.positions.values()), 2
            ),
            "open_positions":    len(self.state.positions),
            "trade_count_today": self.state.trade_count_today,
            "wins_today":        self.state.wins_today,
            "losses_today":      self.state.losses_today,
            "win_rate":          (
                round(self.state.wins_today /
                      (self.state.wins_today + self.state.losses_today) * 100, 1)
                if (self.state.wins_today + self.state.losses_today) > 0 else 0.0
            ),
            "kill_switch":          self.state.kill_switch_active,
            "is_halted":            self.is_halted,
            "reversal_blocked":     list(self.state.reversal_blocked),
        }
