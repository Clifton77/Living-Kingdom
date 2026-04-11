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
import base64
import logging
from datetime import date, datetime
from dataclasses import dataclass
from urllib.parse import urlparse

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from config import (
    KALSHI_API_KEY,
    KALSHI_PRIVATE_KEY_PATH,
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
        self.base_url   = KALSHI_DEMO_URL if demo else KALSHI_LIVE_URL
        self.api_key    = api_key
        self.demo       = demo
        # Extract the path prefix from base_url (e.g. "/trade-api/v2")
        parsed          = urlparse(self.base_url)
        self._path_base = parsed.path.rstrip("/")   # "/trade-api/v2"

        # Load RSA private key for request signing
        self._private_key = None
        if KALSHI_PRIVATE_KEY_PATH:
            try:
                with open(KALSHI_PRIVATE_KEY_PATH, "rb") as f:
                    self._private_key = serialization.load_pem_private_key(f.read(), password=None)
                logger.info("RSA private key loaded from %s", KALSHI_PRIVATE_KEY_PATH)
            except Exception as exc:
                logger.error("Failed to load RSA private key: %s", exc)

        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept":       "application/json",
        })
        logger.info(
            "KalshiClient initialized | mode=%s | base=%s | rsa=%s",
            "DEMO" if demo else "LIVE", self.base_url,
            "yes" if self._private_key else "no",
        )

    def _signed_headers(self, method: str, path: str) -> dict:
        """
        Build Kalshi RSA-signed request headers.
        Kalshi signs: timestamp_ms + METHOD + /trade-api/v2/path (no query string).
        """
        timestamp_ms = str(int(time.time() * 1000))
        full_path    = self._path_base + path          # e.g. /trade-api/v2/markets
        msg          = (timestamp_ms + method.upper() + full_path).encode()

        headers = {
            "KALSHI-ACCESS-KEY":       self.api_key,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        }

        if self._private_key:
            sig = self._private_key.sign(msg, padding.PKCS1v15(), hashes.SHA256())
            headers["KALSHI-ACCESS-SIGNATURE"] = base64.b64encode(sig).decode()
        else:
            logger.warning("No RSA key — request will likely fail auth")

        return headers

    # ── Internal helpers ──────────────────────────────────────────────────

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        for attempt in range(3):
            try:
                headers = {**self.session.headers, **self._signed_headers("GET", path)}
                resp    = requests.get(url, headers=headers, params=params, timeout=10)
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
                import json as _json
                body_bytes = _json.dumps(body).encode()
                headers    = {**self.session.headers, **self._signed_headers("POST", path)}
                resp       = requests.post(url, headers=headers, data=body_bytes, timeout=10)
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

    def get_markets_for_station_date(
        self,
        station: str,
        event_date: date,
    ) -> list[MarketSnapshot]:
        """
        Dynamically discover all available bucket markets from Kalshi for a
        station + date, parse actual boundaries from the response, and return
        a list of MarketSnapshot objects sorted by bucket_min.

        This replaces the hardcoded all_bucket_lowers() approach so the signal
        logic is correct regardless of how Kalshi structures buckets that day.
        """
        event_ticker = build_event_id(station, event_date)
        try:
            data = self._get("/markets", params={"event_ticker": event_ticker, "limit": 50})
        except Exception as exc:
            logger.error("get_markets_for_station_date %s %s failed: %s", station, event_date, exc)
            return []

        raw_markets = data.get("markets", [])
        if not raw_markets:
            logger.warning("No markets returned for event %s", event_ticker)
            return []

        logger.info("Event %s — %d raw markets from Kalshi", event_ticker, len(raw_markets))

        snapshots = []
        for m in raw_markets:
            snap = self._parse_market_snapshot(m, station)
            if snap:
                snapshots.append(snap)

        snapshots.sort(key=lambda s: (s.bucket_lower is None, s.bucket_lower or 0))
        logger.info(
            "%s %s — parsed %d/%d bucket snapshots dynamically",
            station, event_date, len(snapshots), len(raw_markets),
        )
        return snapshots

    def _parse_market_snapshot(self, raw: dict, station: str) -> MarketSnapshot | None:
        """
        Parse a single raw Kalshi market dict into a MarketSnapshot.
        Derives bucket_lower and bucket_label from the market ticker and/or title.
        Prices are in cents (0–100) in the API; converted to 0–1 fractions here.
        """
        import re

        ticker = raw.get("ticker", "")
        title  = raw.get("title", raw.get("subtitle", ""))
        status = raw.get("status", "")

        if not ticker:
            return None

        # ── Parse bucket boundaries ───────────────────────────────────────
        # Strategy 1: extract center from ticker suffix (e.g. "B71.5" → 71–72)
        bucket_lower: int | None = None
        bucket_label_str         = ""

        center_match = re.search(r"-B([\d.]+)$", ticker)
        if center_match:
            center = float(center_match.group(1))
            # Determine width by checking if it's a tail bucket
            # Centers like 68 (floor) and 77 (ceiling) are special-cased
            if center == KALSHI_BUCKET_LOWER_TAIL:
                bucket_lower     = int(center)
                bucket_label_str = f"{int(center)}° or below"
            elif center == KALSHI_BUCKET_UPPER_TAIL:
                bucket_lower     = int(center)
                bucket_label_str = f"{int(center)}° or above"
            else:
                bucket_lower     = int(center - 0.5)   # e.g. 71.5 → 71
                bucket_upper     = int(center + 0.5)   # e.g. 71.5 → 72
                bucket_label_str = f"{bucket_lower}° to {bucket_upper}°"

        # Strategy 2: parse from title text (catches non-standard centers)
        if bucket_lower is None and title:
            # "between X and Y" / "X to Y" / "above X" / "below X" / "at or below X"
            between = re.search(r"(\d+)\s*(?:°F)?\s*(?:to|and|-)\s*(\d+)\s*(?:°F)?", title, re.I)
            above   = re.search(r"(?:above|at or above|or above)\s+(\d+)\s*(?:°F)?", title, re.I)
            below   = re.search(r"(?:below|at or below|or below)\s+(\d+)\s*(?:°F)?", title, re.I)

            if between:
                lo, hi           = int(between.group(1)), int(between.group(2))
                bucket_lower     = lo
                bucket_label_str = f"{lo}° to {hi}°"
            elif above:
                bucket_lower     = int(above.group(1))
                bucket_label_str = f"{bucket_lower}° or above"
            elif below:
                bucket_lower     = int(below.group(1))
                bucket_label_str = f"{bucket_lower}° or below"

        if bucket_lower is None:
            logger.debug("Could not parse bucket from ticker=%s title=%s", ticker, title)
            return None

        # ── Prices (cents → fraction) ─────────────────────────────────────
        yes_bid = raw.get("yes_bid", 0) / 100.0
        yes_ask = raw.get("yes_ask", 100) / 100.0
        no_bid  = raw.get("no_bid",  0) / 100.0
        no_ask  = raw.get("no_ask",  100) / 100.0

        return MarketSnapshot(
            market_id    = ticker,
            station      = station,
            bucket_lower = bucket_lower,
            bucket_label = bucket_label_str or bucket_label(bucket_lower),
            yes_bid      = yes_bid,
            yes_ask      = yes_ask,
            no_bid       = no_bid,
            no_ask       = no_ask,
            implied_prob = yes_ask,
            volume       = raw.get("volume", 0),
            is_open      = status == "open",
        )

    def get_all_snapshots(
        self,
        station: str,
        event_date: date,
    ) -> dict[int, MarketSnapshot]:
        """
        Fetch snapshots for all buckets for a station/date.
        Uses dynamic bucket discovery (get_markets_for_station_date) so the
        result always reflects whatever Kalshi is offering that day.
        Returns dict keyed by bucket_lower.
        """
        snaps = self.get_markets_for_station_date(station, event_date)
        result = {s.bucket_lower: s for s in snaps}
        logger.info(
            "%s %s — %d buckets discovered: %s",
            station, event_date, len(result),
            sorted(result.keys()),
        )
        return result

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

    # ── Order status & fill confirmation ─────────────────────────────────

    def get_order_status(self, order_id: str) -> dict | None:
        """Fetch current status of an order. Returns None on failure."""
        try:
            data = self._get(f"/portfolio/orders/{order_id}")
            return data.get("order", data)
        except Exception as exc:
            logger.warning("get_order_status failed for %s: %s", order_id, exc)
            return None

    def wait_for_fill(
        self,
        order_id: str,
        timeout_seconds: int = 30,
        poll_interval: int = 3,
    ) -> tuple[bool, int]:
        """
        Poll until order is filled or timeout expires.

        Returns (filled: bool, filled_contracts: int).
        A limit order at the ask should fill near-instantly in a liquid market.
        If it doesn't fill within timeout, the order likely needs to be cancelled
        and the price re-evaluated.
        """
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            status = self.get_order_status(order_id)
            if status is None:
                break

            order_status    = status.get("status", "")
            filled_count    = status.get("contracts_count", 0) - status.get("remaining_count", 0)

            if order_status == "executed" or filled_count > 0:
                logger.info("Order %s filled: %d contracts", order_id, filled_count)
                return True, filled_count

            if order_status in ("canceled", "expired", "rejected"):
                logger.warning("Order %s ended with status: %s", order_id, order_status)
                return False, 0

            time.sleep(poll_interval)

        logger.warning("Order %s did not fill within %ds", order_id, timeout_seconds)
        return False, 0

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order. Returns True on success."""
        try:
            self._post(f"/portfolio/orders/{order_id}/cancel", {})
            logger.info("Order %s cancelled", order_id)
            return True
        except Exception as exc:
            logger.warning("cancel_order failed for %s: %s", order_id, exc)
            return False

    def place_order_with_fill_check(
        self,
        market_id: str,
        contracts: int,
        limit_price: float,
        side: str = "yes",
        fill_timeout: int = 30,
    ) -> OrderResult:
        """
        Place a limit order and confirm it actually fills.
        If not filled within fill_timeout seconds, cancel and return failure.
        This prevents ghost positions (recorded as open locally but not filled on Kalshi).
        """
        result = self.place_order(market_id, contracts, limit_price, side)
        if not result.success or not result.order_id:
            return result

        filled, filled_count = self.wait_for_fill(result.order_id, timeout_seconds=fill_timeout)
        if not filled:
            cancelled = self.cancel_order(result.order_id)
            return OrderResult(
                success=False,
                order_id=result.order_id,
                market_id=market_id,
                side=side,
                contracts=contracts,
                price=limit_price,
                error=f"Order did not fill within {fill_timeout}s — {'cancelled' if cancelled else 'cancel failed'}",
            )

        return result

    # ── Positions ─────────────────────────────────────────────────────────

    def get_positions(self) -> list[dict]:
        """Return all open positions from the portfolio."""
        try:
            data = self._get("/portfolio/positions")
            return data.get("market_positions", [])
        except Exception as exc:
            logger.error("get_positions failed: %s", exc)
            return []

    def get_settled_markets(self, event_date) -> dict[str, float]:
        """
        Return settled markets for a given event date.
        Dict maps market_id → settlement_value (1.0 = won, 0.0 = lost).

        Used during the morning settlement sweep to auto-close positions
        that resolved overnight without explicit bot action.
        """
        try:
            # Fetch settled positions from portfolio history
            data = self._get(
                "/portfolio/settlements",
                params={"limit": 100},
            )
            settlements = data.get("settlements", [])

            result = {}
            event_str = event_date.strftime("%y%b%d").upper() if hasattr(event_date, "strftime") else str(event_date)

            for s in settlements:
                ticker = s.get("market_ticker", "")
                if event_str in ticker:
                    # Settlement value: revenue / (contracts * 100) → fraction
                    revenue   = s.get("revenue", 0)
                    contracts = s.get("contracts_count", 1)
                    value     = (revenue / 100.0 / contracts) if contracts > 0 else 0.0
                    result[ticker] = round(value, 4)

            logger.info(
                "get_settled_markets %s: found %d settlements",
                event_date, len(result),
            )
            return result

        except Exception as exc:
            logger.error("get_settled_markets failed: %s", exc)
            return {}

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
