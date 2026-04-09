"""
Kalshi REST API client — paper trading (demo) mode.

Handles authentication, market lookup, order placement,
position polling, and account balance queries.

Market ID format (confirmed from live data):
  Series:  KXHIGH{4-char}              e.g. KXHIGHLAX
  Event:   KXHIGH{4-char}-{YY}{MON}{DD}   e.g. KXHIGHLAX-26APR08
  Market:  KXHIGH{4-char}-{YY}{MON}{DD}-B{center}  e.g. KXHIGHLAX-26APR08-B71.5

Bucket centers (2°F odd-start bins):
  ≤68    → B68
  69–70  → B69.5
  71–72  → B71.5
  73–74  → B73.5
  75–76  → B75.5
  ≥77    → B77

Authentication: Bearer token (API key from .env → KALSHI_API_KEY)
"""

from __future__ import annotations

import time
import logging
from datetime import date, datetime
from dataclasses import dataclass

import requests

from config import (
    KALSHI_API_KEY,
    KALSHI_DEMO_URL,
    KALSHI_LIVE_URL,
    USE_DEMO,
    KALSHI_SERIES_PREFIX,
    KALSHI_BUCKET_CENTERS,
    KALSHI_BUCKET_LOWER_TAIL,
    KALSHI_BUCKET_UPPER_TAIL,
    KALSHI_BUCKET_STARTS,
)
from utils.logging_config import setup_logging

logger = setup_logging("kalshi_client")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class MarketSnapshot:
    market_id:      str
    station:        str
    bucket_lower:   int          # lower bound of bucket (68 = lower tail, 77 = upper tail)
    bucket_label:   str          # e.g. "71 to 72" or "68 or below"
    yes_bid:        float        # best Yes bid (cents → fraction, 0–1)
    yes_ask:        float
    no_bid:         float
    no_ask:         float
    implied_prob:   float        # Kalshi implied probability = yes_ask (cost to buy Yes)
    volume:         int
    is_open:        bool

@dataclass
class Position:
    market_id:      str
    station:        str
    bucket_lower:   int
    contracts:      int
    entry_price:    float        # per contract, 0–1
    current_bid:    float
    current_ask:    float
    unrealized_pnl: float        # dollars
    pnl_pct:        float
    exit_now_value: float        # contracts × current_bid

@dataclass
class OrderResult:
    success:        bool
    order_id:       str | None
    market_id:      str
    side:           str          # "yes"
    contracts:      int
    price:          float
    error:          str | None


# ---------------------------------------------------------------------------
# Market ID helpers
# ---------------------------------------------------------------------------

def _station_suffix(station: str) -> str:
    """KJFK → JFK (drop the K prefix, 3 chars)."""
    return station[1:] if station.startswith("K") else station


def _date_tag(d: date) -> str:
    """date(2026,4,8) → '26APR08'"""
    return d.strftime("%y%b%d").upper()


def bucket_lower_to_center(bucket_lower: int) -> str:
    """Map bucket lower bound to Kalshi center string."""
    return KALSHI_BUCKET_CENTERS.get(bucket_lower, str(bucket_lower))


def build_market_id(station: str, event_date: date, bucket_lower: int) -> str:
    """
    Build Kalshi market ticker.
    e.g. build_market_id("KLAX", date(2026,4,8), 71) → "KXHIGHLAX-26APR08-B71.5"
    """
    suffix = _station_suffix(station)
    dtag   = _date_tag(event_date)
    center = bucket_lower_to_center(bucket_lower)
    return f"{KALSHI_SERIES_PREFIX}{suffix}-{dtag}-B{center}"


def build_event_id(station: str, event_date: date) -> str:
    """e.g. "KXHIGHLAX-26APR08" """
    suffix = _station_suffix(station)
    dtag   = _date_tag(event_date)
    return f"{KALSHI_SERIES_PREFIX}{suffix}-{dtag}"


def all_bucket_lowers() -> list[int]:
    """Return all bucket lower bounds in order."""
    return [KALSHI_BUCKET_LOWER_TAIL] + KALSHI_BUCKET_STARTS + [KALSHI_BUCKET_UPPER_TAIL]


def bucket_label(lower: int) -> str:
    """Human-readable bucket label."""
    if lower == KALSHI_BUCKET_LOWER_TAIL:
        return f"{lower}° or below"
    if lower == KALSHI_BUCKET_UPPER_TAIL:
        return f"{lower}° or above"
    return f"{lower}° to {lower + 1}°"


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

class KalshiClient:
    """
    Thin wrapper around the Kalshi REST API.
    Operates in demo mode by default (USE_DEMO=True in config).
    """

    def __init__(self, api_key: str = KALSHI_API_KEY, demo: bool = USE_DEMO):
        self.base_url = KALSHI_DEMO_URL if demo else KALSHI_LIVE_URL
        self.api_key  = api_key
        self.demo     = demo
        self.session  = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        })
        logger.info(
            "KalshiClient initialized | mode=%s | base=%s",
            "DEMO" if demo else "LIVE", self.base_url,
        )

    # ── Internal helpers ──────────────────────────────────────────────────

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=10)
                if resp.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning("Rate limited — waiting %ds", wait)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.HTTPError as exc:
                logger.error("GET %s failed: %s", path, exc)
                raise
        raise RuntimeError(f"GET {path} failed after 3 attempts")

    def _post(self, path: str, body: dict) -> dict:
        url = f"{self.base_url}{path}"
        for attempt in range(3):
            try:
                resp = self.session.post(url, json=body, timeout=10)
                if resp.status_code == 429:
                    wait = 2 ** attempt
                    logger.warning("Rate limited — waiting %ds", wait)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.HTTPError as exc:
                logger.error("POST %s failed: %s", path, exc)
                raise
        raise RuntimeError(f"POST {path} failed after 3 attempts")

    # ── Account ───────────────────────────────────────────────────────────

    def get_balance(self) -> float:
        """Return available balance in USD."""
        try:
            data = self._get("/portfolio/balance")
            # Kalshi returns balance in cents
            return data.get("balance", 0) / 100.0
        except Exception as exc:
            logger.error("get_balance failed: %s", exc)
            return 0.0

    # ── Market data ───────────────────────────────────────────────────────

    def get_market(self, market_id: str) -> dict | None:
        """Fetch raw market data by ticker."""
        try:
            data = self._get(f"/markets/{market_id}")
            return data.get("market", data)
        except Exception as exc:
            logger.warning("get_market %s failed: %s", market_id, exc)
            return None

    def get_market_snapshot(
        self,
        station: str,
        event_date: date,
        bucket_lower: int,
    ) -> MarketSnapshot | None:
        """
        Fetch current bid/ask for a specific bucket market.
        Returns None if market not found or not open.
        """
        market_id = build_market_id(station, event_date, bucket_lower)
        raw = self.get_market(market_id)
        if not raw:
            return None

        # Kalshi prices are in cents (0–100); convert to 0–1
        yes_bid = raw.get("yes_bid", 0) / 100.0
        yes_ask = raw.get("yes_ask", 1) / 100.0
        no_bid  = raw.get("no_bid",  0) / 100.0
        no_ask  = raw.get("no_ask",  1) / 100.0

        return MarketSnapshot(
            market_id=market_id,
            station=station,
            bucket_lower=bucket_lower,
            bucket_label=bucket_label(bucket_lower),
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            implied_prob=yes_ask,     # cost to buy Yes = implied probability
            volume=raw.get("volume", 0),
            is_open=raw.get("status", "") == "open",
        )

    def get_all_snapshots(
        self,
        station: str,
        event_date: date,
    ) -> dict[int, MarketSnapshot]:
        """
        Fetch snapshots for all buckets for a station/date.
        Returns dict keyed by bucket_lower.
        """
        snapshots = {}
        for lower in all_bucket_lowers():
            snap = self.get_market_snapshot(station, event_date, lower)
            if snap:
                snapshots[lower] = snap
        logger.info(
            "%s %s — fetched %d/%d bucket snapshots",
            station, event_date, len(snapshots), len(all_bucket_lowers()),
        )
        return snapshots

    # ── Order management ──────────────────────────────────────────────────

    def place_order(
        self,
        market_id: str,
        contracts: int,
        limit_price: float,         # 0–1 fraction; converted to cents internally
        side: str = "yes",
    ) -> OrderResult:
        """
        Place a limit order. In demo mode this hits the paper trading endpoint.
        limit_price: fraction (e.g. 0.28 = 28¢)
        """
        price_cents = round(limit_price * 100)

        body = {
            "ticker":  market_id,
            "action":  "buy",
            "side":    side,
            "type":    "limit",
            "count":   contracts,
            "yes_price": price_cents if side == "yes" else None,
            "no_price":  price_cents if side == "no"  else None,
        }
        # Remove None values
        body = {k: v for k, v in body.items() if v is not None}

        try:
            resp = self._post("/portfolio/orders", body)
            order = resp.get("order", resp)
            logger.info(
                "Order placed | %s | %d contracts @ %.2f | id=%s",
                market_id, contracts, limit_price,
                order.get("order_id", "?"),
            )
            return OrderResult(
                success=True,
                order_id=order.get("order_id"),
                market_id=market_id,
                side=side,
                contracts=contracts,
                price=limit_price,
                error=None,
            )
        except Exception as exc:
            logger.error("place_order failed %s: %s", market_id, exc)
            return OrderResult(
                success=False,
                order_id=None,
                market_id=market_id,
                side=side,
                contracts=contracts,
                price=limit_price,
                error=str(exc),
            )

    def close_position(self, market_id: str, contracts: int, bid_price: float) -> OrderResult:
        """
        Exit an open position by placing a sell limit order at the current bid.
        """
        body = {
            "ticker":    market_id,
            "action":    "sell",
            "side":      "yes",
            "type":      "limit",
            "count":     contracts,
            "yes_price": round(bid_price * 100),
        }
        try:
            resp = self._post("/portfolio/orders", body)
            order = resp.get("order", resp)
            logger.info(
                "Exit order placed | %s | %d contracts @ %.2f",
                market_id, contracts, bid_price,
            )
            return OrderResult(
                success=True,
                order_id=order.get("order_id"),
                market_id=market_id,
                side="yes",
                contracts=contracts,
                price=bid_price,
                error=None,
            )
        except Exception as exc:
            logger.error("close_position failed %s: %s", market_id, exc)
            return OrderResult(
                success=False, order_id=None,
                market_id=market_id, side="yes",
                contracts=contracts, price=bid_price,
                error=str(exc),
            )

    # ── Positions ─────────────────────────────────────────────────────────

    def get_positions(self) -> list[dict]:
        """Return all open positions from the portfolio."""
        try:
            data = self._get("/portfolio/positions")
            return data.get("market_positions", [])
        except Exception as exc:
            logger.error("get_positions failed: %s", exc)
            return []

    def get_position_pnl(
        self,
        market_id: str,
        station: str,
        bucket_lower: int,
        entry_price: float,
        contracts: int,
        current_bid: float,
        current_ask: float,
    ) -> Position:
        """Compute P/L for an open position from live market data."""
        invested       = entry_price * contracts
        current_value  = current_bid * contracts
        unrealized_pnl = current_value - invested
        pnl_pct        = (unrealized_pnl / invested * 100) if invested > 0 else 0.0

        return Position(
            market_id=market_id,
            station=station,
            bucket_lower=bucket_lower,
            contracts=contracts,
            entry_price=entry_price,
            current_bid=current_bid,
            current_ask=current_ask,
            unrealized_pnl=round(unrealized_pnl, 4),
            pnl_pct=round(pnl_pct, 2),
            exit_now_value=round(current_bid * contracts, 4),
        )
