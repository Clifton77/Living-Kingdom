"""
Risk management module.

Responsibilities:
  - Track open positions and bankroll state
  - Enforce stop-loss (static: 40% of entry value)
  - Detect signal reversal stop (edge flips negative past threshold)
  - Early profit exit (bid reaches 85¢ and high is locked in bucket)
  - Overshoot exit (running max ≥ bucket_upper − buffer, before peak hour)
  - Undershoot warning (running max below bucket, approaching peak hour)
  - Undershoot exit (running max below bucket, past peak hour — definitive miss)
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
    OVERSHOOT_EXIT_BUFFER_F,
    UNDERSHOOT_EXIT_BUFFER_F,
    UNDERSHOOT_WARNING_LEAD_HOURS,
    MAX_STATION_POSITIONS,
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
    # Stations blocked from re-entry today (reversal stop or stop-loss fired)
    reversal_blocked:   list[str]      = field(default_factory=list)
    # Stations that have already used their one adjacent-bucket expansion today
    expansion_used:     list[str]      = field(default_factory=list)
    # Stop-loss count per station today — hard cap at 2 regardless of block state
    stop_loss_count:    dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Exit decision
# ---------------------------------------------------------------------------

@dataclass
class ExitDecision:
    should_exit:    bool
    reason:         str
    urgency:        str        # "immediate" | "recommended" | "warning" | "none"


def evaluate_exit(
    pos: OpenPosition,
    current_bid: float,
    current_edge: float,
    current_obs_temp: float | None = None,
    running_max: float | None = None,
    local_hour: int | None = None,
    peak_heating_hour: int | None = None,
) -> ExitDecision:
    """
    Evaluate whether an open position should be exited.

    Checks in priority order:
    1. Static stop-loss      — bid fell to ≤ STOP_LOSS_PCT of entry
    2. Signal reversal stop  — edge reversed past REVERSAL_EDGE_THRESHOLD
    3. Undershoot exit       — past peak hour, running max below bucket (definitive miss)
    4. Undershoot warning    — approaching peak hour, running max below bucket (alert only)
    5. Overshoot exit        — before peak hour, running max ≥ bucket_upper (lock profit)
    6. Early profit exit     — bid ≥ EARLY_EXIT_BID_THRESHOLD (85¢), temp in bucket
    7. Hold                  — no exit condition met

    Parameters
    ----------
    pos               : open position being evaluated
    current_bid       : current Yes bid price (0–1)
    current_edge      : most recent edge from signal engine (can be negative)
    current_obs_temp  : current METAR observed temperature in °F (optional)
    running_max       : highest observed temp today from IEM 1-min data (optional)
    local_hour        : current local hour at the station (0–23, optional)
    peak_heating_hour : station/month 90th-pct peak hour from STATION_PEAK_HOURS (optional)
    """
    if current_bid is None or current_edge is None:
        return ExitDecision(False, "Missing market data — hold", "none")
    if not pos.entry_price:
        return ExitDecision(False, "Position entry_price missing — hold", "none")

    bucket_lo = pos.bucket_lower
    bucket_hi = bucket_lo + 2    # 2°F wide bin

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

    # ── 3 & 4. Undershoot protection (requires intraday running max) ──────
    if running_max is not None and local_hour is not None and peak_heating_hour is not None:
        undershoot_trigger = bucket_lo - UNDERSHOOT_EXIT_BUFFER_F

        if running_max < undershoot_trigger:
            if local_hour >= peak_heating_hour:
                # Hard exit — peak hour passed, temperature can't recover
                return ExitDecision(
                    should_exit=True,
                    reason=(
                        f"Undershoot exit: running max {running_max:.1f}°F is "
                        f"{undershoot_trigger - running_max:.1f}°F below trigger "
                        f"({undershoot_trigger:.1f}°F) and peak hour has passed "
                        f"(local {local_hour:02d}h ≥ peak {peak_heating_hour:02d}h). "
                        f"Bucket {bucket_lo}–{bucket_hi}°F not reachable."
                    ),
                    urgency="immediate",
                )

            elif local_hour >= peak_heating_hour - UNDERSHOOT_WARNING_LEAD_HOURS:
                # Warning — approaching peak hour, tracking low, manual close available
                hours_left = peak_heating_hour - local_hour
                return ExitDecision(
                    should_exit=False,
                    reason=(
                        f"Undershoot warning: running max {running_max:.1f}°F is "
                        f"{undershoot_trigger - running_max:.1f}°F below trigger "
                        f"({undershoot_trigger:.1f}°F) with ~{hours_left}h until peak. "
                        f"Bucket {bucket_lo}–{bucket_hi}°F at risk — review position."
                    ),
                    urgency="warning",
                )

    # ── 5. Overshoot exit (lock profit before temp retreats) ─────────────
    if running_max is not None and local_hour is not None and peak_heating_hour is not None:
        overshoot_trigger = bucket_hi - OVERSHOOT_EXIT_BUFFER_F
        if running_max >= overshoot_trigger and local_hour < peak_heating_hour:
            return ExitDecision(
                should_exit=True,
                reason=(
                    f"Overshoot exit: running max {running_max:.1f}°F ≥ "
                    f"{overshoot_trigger:.1f}°F ({bucket_hi}°F upper − {OVERSHOOT_EXIT_BUFFER_F}°F buffer) "
                    f"before peak hour ({local_hour:02d}h < {peak_heating_hour:02d}h). "
                    f"Locking in profit before potential retreat."
                ),
                urgency="recommended",
            )

    # ── 6. Early profit exit ──────────────────────────────────────────────
    if current_bid >= EARLY_EXIT_BID_THRESHOLD:
        past_peak          = (local_hour is not None and peak_heating_hour is not None
                              and local_hour >= peak_heating_hour)
        confirmed_in_bucket = (running_max is not None and running_max >= bucket_lo)

        if past_peak and confirmed_in_bucket:
            # Peak window closed, temp confirmed in our bucket — hold to settlement
            # for full $1.00 rather than exiting at 85¢+
            return ExitDecision(
                should_exit=False,
                reason=(
                    f"Hold to settlement: bid {current_bid:.2f} but peak hour passed "
                    f"({local_hour:02d}h ≥ {peak_heating_hour:02d}h) and running max "
                    f"{running_max:.1f}°F confirmed in bucket {bucket_lo}–{bucket_hi}°F. "
                    f"Maximizing profit to $1.00."
                ),
                urgency="none",
            )

        if current_obs_temp is not None:
            if current_obs_temp >= bucket_lo:
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
            # No METAR data and before peak — take profit at high bid conservatively
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
                    k: OpenPosition(
                        station       = v.get("station", ""),
                        market_id     = v.get("market_id", k),
                        bucket_lower  = int(v.get("bucket_lower", 0)),
                        contracts     = int(v.get("contracts", 0)),
                        entry_price   = float(v.get("entry_price") or 0.0),
                        entry_usd     = float(v.get("entry_usd") or 0.0),
                        entry_time    = v.get("entry_time", ""),
                        event_date    = v.get("event_date", ""),
                        current_bid   = float(v.get("current_bid") or 0.0),
                        current_ask   = float(v.get("current_ask") or 0.0),
                        unrealized_pnl= float(v.get("unrealized_pnl") or 0.0),
                        pnl_pct       = float(v.get("pnl_pct") or 0.0),
                        exit_value    = float(v.get("exit_value") or 0.0),
                        last_edge     = float(v.get("last_edge") or 0.0),
                        stop_loss_triggered  = bool(v.get("stop_loss_triggered", False)),
                        reversal_triggered   = bool(v.get("reversal_triggered", False)),
                        early_exit_triggered = bool(v.get("early_exit_triggered", False)),
                        manually_closed      = bool(v.get("manually_closed", False)),
                    )
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
            self.state.expansion_used    = []   # clear expansion flags each day
            self.state.stop_loss_count   = {}   # clear stop-loss counts each day
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

    # ── Station position helpers ──────────────────────────────────────────

    def station_positions(self, station: str) -> list[OpenPosition]:
        """All open positions for a given station."""
        return [p for p in self.state.positions.values() if p.station == station]

    def station_position_count(self, station: str) -> int:
        return len(self.station_positions(station))

    def can_expand_station(self, station: str) -> tuple[bool, str]:
        """
        Check whether an adjacent-bucket expansion is allowed for this station.
        Returns (allowed, reason).
        """
        if self.is_reversal_blocked(station):
            return False, f"{station} is reversal-blocked — no expansion allowed today"
        if station in self.state.expansion_used:
            return False, f"{station} has already expanded once today"
        if self.station_position_count(station) >= MAX_STATION_POSITIONS:
            return False, (
                f"{station} already has {MAX_STATION_POSITIONS} open positions "
                f"(max {MAX_STATION_POSITIONS})"
            )
        return True, "OK"

    def record_expansion(self, station: str):
        """Mark that this station has used its one expansion for today."""
        if station not in self.state.expansion_used:
            self.state.expansion_used.append(station)
            self._save_state()
            logger.info("%s expansion recorded — no further expansions today", station)

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

        if station and self.state.stop_loss_count.get(station, 0) >= 2:
            return False, f"{station} hard-blocked — 2 stop-losses today"

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
        ok, reason = self.can_open_position(stake_usd, station)
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
        running_max: float | None = None,
        local_hour: int | None = None,
        peak_heating_hour: int | None = None,
    ) -> ExitDecision:
        """
        Update position market data and evaluate exit conditions.
        Returns ExitDecision for the scheduler to act on.

        running_max, local_hour, peak_heating_hour are used by the intraday
        undershoot/overshoot guards. All are optional — guards are skipped if
        any are None (e.g. IEM data unavailable).
        """
        pos = self.state.positions.get(market_id)
        if pos is None:
            return ExitDecision(False, "Position not found", "none")
        if current_bid is None or current_ask is None:
            return ExitDecision(False, "No market snapshot data — hold", "none")

        pos.current_bid      = current_bid
        pos.current_ask      = current_ask
        pos.last_edge        = current_edge if current_edge is not None else 0.0
        pos.unrealized_pnl   = round((current_bid - (pos.entry_price or 0.0)) * pos.contracts, 4)
        pos.pnl_pct          = round(pos.unrealized_pnl / pos.entry_usd * 100, 2) if pos.entry_usd else 0.0
        pos.exit_value       = round(current_bid * pos.contracts, 4)

        self._save_state()

        return evaluate_exit(
            pos, current_bid, current_edge,
            current_obs_temp=current_obs_temp,
            running_max=running_max,
            local_hour=local_hour,
            peak_heating_hour=peak_heating_hour,
        )

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

        # Block re-entry on reversal stops and stop-losses
        reason_lower = reason.lower()
        if "stop-loss" in reason_lower or "stop_loss" in reason_lower:
            station = pos.station
            self.state.stop_loss_count[station] = (
                self.state.stop_loss_count.get(station, 0) + 1
            )
            self._block_station(station)
        elif "reversal" in reason_lower:
            self._block_station(pos.station)

        self._save_state()

        logger.info(
            "Position closed: %s | exit $%.2f | P/L $%+.4f | reason: %s",
            market_id, exit_price, realized, reason,
        )
        return realized

    # ── Startup reconciliation ────────────────────────────────────────────

    def reconcile_with_kalshi(self, kalshi) -> list[str]:
        """
        On startup: sync local risk state against live Kalshi portfolio.

        Actions taken:
          1. Update bankroll from live Kalshi balance
          2. Find positions in local state that no longer exist on Kalshi
             (settled overnight or manually closed) → close them in state
          3. Find positions on Kalshi not in local state (manual trades) → warn

        Returns list of human-readable reconciliation notes for alerting.
        """
        notes = []

        # ── 1. Sync bankroll ──────────────────────────────────────────────
        live_balance = kalshi.get_balance()
        if live_balance > 0:
            old = self.state.bankroll
            self.state.bankroll = live_balance
            if abs(old - live_balance) > 0.01:
                note = f"Bankroll updated: ${old:.2f} → ${live_balance:.2f} (live Kalshi balance)"
                notes.append(note)
                logger.info(note)
        else:
            logger.warning("Could not fetch live Kalshi balance — keeping stored value")

        # ── 2. Find locally-open positions that Kalshi doesn't know about ─
        kalshi_positions = kalshi.get_positions()
        kalshi_tickers   = {p.get("market_ticker", p.get("ticker", "")) for p in kalshi_positions}

        for market_id in list(self.state.positions.keys()):
            if market_id not in kalshi_tickers:
                pos     = self.state.positions[market_id]
                # Best guess at settlement: if market is expired, treat as $0 (loss)
                # The settlement sweep will correct this with actual values later
                realized = self.close_position(market_id, exit_price=0.0,
                                               reason="reconciliation — not found on Kalshi")
                note = (
                    f"Reconciliation: {market_id} missing from Kalshi — "
                    f"removed from state (P/L ${realized:+.4f}). "
                    f"Settlement sweep will correct if this was a win."
                )
                notes.append(note)
                logger.warning(note)

        # ── 3. Positions on Kalshi not in local state ─────────────────────
        local_ids = set(self.state.positions.keys())
        for kp in kalshi_positions:
            ticker = kp.get("market_ticker", kp.get("ticker", ""))
            if ticker and ticker not in local_ids:
                note = (
                    f"Reconciliation: {ticker} found on Kalshi but not in local state — "
                    f"possible manual trade. Review dashboard."
                )
                notes.append(note)
                logger.warning(note)

        self._save_state()
        return notes

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
            "expansion_used":       list(self.state.expansion_used),
            "stop_loss_count":      dict(self.state.stop_loss_count),
        }
