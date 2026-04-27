"""
Kalshi REST API client — paper trading (demo) mode.

Handles authentication, market lookup, order placement,
position polling, and account balance queries.

Market ID format (confirmed from live API, Apr 2026):
  Series:  per-station ticker            e.g. KXHIGHLAX, KXHIGHNY0, KXHIGHCHI
  Event:   {series}-{YYMONDD}            e.g. KXHIGHLAX-26APR14
  Market:  {series}-{YYMONDD}-B{center}  e.g. KXHIGHLAX-26APR14-B80.5

Bucket structure (2°F wide, even-start, station/season dependent):
  Floor:    "77° or below"  → B77    (actual floor value varies)
  Interior: "78° to 79°"   → B78.5  (center of range)
  Interior: "80° to 81°"   → B80.5
  Ceiling:  "86° or above" → B86    (actual ceiling value varies)
  Buckets are discovered dynamically per station/date — never hardcoded.

Authentication: RSA-PSS signed headers (KALSHI-ACCESS-KEY / SIGNATURE / TIMESTAMP)
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
    DRY_RUN,
    KALSHI_STATION_SERIES,
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

def get_series_ticker(station: str) -> str:
    """
    Return the Kalshi series ticker for a station.
    e.g. get_series_ticker("KLAX") → "KXHIGHLAX"
         get_series_ticker("KJFK") → "KXHIGHNY"
    Raises KeyError if station is not in KALSHI_STATION_SERIES.
    """
    if station not in KALSHI_STATION_SERIES:
        raise KeyError(
            f"No Kalshi series ticker configured for station {station!r}. "
            f"Known stations: {sorted(KALSHI_STATION_SERIES)}"
        )
    return KALSHI_STATION_SERIES[station]


def _date_tag(d: date) -> str:
    """date(2026,4,14) → '26APR14'"""
    return d.strftime("%y%b%d").upper()


def _parse_date_from_ticker(ticker: str) -> date | None:
    """
    Extract event date from a market ticker.
    e.g. "KXHIGHLAX-26APR14-B80.5" → date(2026, 4, 14)
    Returns None if not parseable.
    """
    import re
    from datetime import datetime
    m = re.search(r"-(\d{2}[A-Z]{3}\d{2})-", ticker)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%y%b%d").date()
    except ValueError:
        return None


def bucket_lower_to_center(bucket_lower: int) -> str:
    """Map bucket lower bound to Kalshi center string (legacy helper)."""
    return KALSHI_BUCKET_CENTERS.get(bucket_lower, str(bucket_lower))


def all_bucket_lowers() -> list[int]:
    """Return hardcoded bucket lower bounds (legacy — prefer dynamic discovery)."""
    return [KALSHI_BUCKET_LOWER_TAIL] + KALSHI_BUCKET_STARTS + [KALSHI_BUCKET_UPPER_TAIL]


def bucket_label(lower: int) -> str:
    """Human-readable bucket label from lower bound (legacy fallback)."""
    if lower == KALSHI_BUCKET_LOWER_TAIL:
        return f"{lower}° or below"
    if lower == KALSHI_BUCKET_UPPER_TAIL:
        return f"{lower}° or above"
    return f"{lower}° to {lower + 1}°"


def build_market_id(station: str, event_date: date, bucket_lower: int) -> str:
    """
    Build a Kalshi market ticker from components.
    e.g. build_market_id("KLAX", date(2026,4,14), 80) → "KXHIGHLAX-26APR14-B80.5"
    """
    series = get_series_ticker(station)
    center = bucket_lower_to_center(bucket_lower)
    return f"{series}-{_date_tag(event_date)}-B{center}"


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
            sig = self._private_key.sign(
                msg,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
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
        Uses dynamic discovery to find the market — avoids hardcoded ticker
        construction which is fragile given Kalshi's per-station ticker naming.
        Returns None if market not found or not open.
        """
        snaps = self.get_markets_for_station_date(station, event_date)
        for s in snaps:
            if s.bucket_lower == bucket_lower:
                return s
        logger.warning(
            "get_market_snapshot: bucket_lower=%d not found for %s %s",
            bucket_lower, station, event_date,
        )
        return None

    def get_markets_for_station_date(
        self,
        station: str,
        event_date: date,
    ) -> list[MarketSnapshot]:
        """
        Dynamically discover all bucket markets from Kalshi for a station + date.

        Strategy:
          1. Look up the correct series ticker for the station (e.g. KXHIGHNY0 for KJFK).
          2. Query GET /markets?series_ticker=...&status=open&limit=100 to get all open
             markets in the series (today's and possibly tomorrow's buckets).
          3. Filter to markets whose ticker contains the event_date string.
          4. Parse bucket boundaries from title text (most reliable) or ticker center.

        Returns MarketSnapshot list sorted by bucket_lower ascending.
        """
        try:
            series_ticker = get_series_ticker(station)
        except KeyError as exc:
            logger.error("%s", exc)
            return []

        date_tag = _date_tag(event_date)   # e.g. "26APR14"

        # Step 1: try event_ticker query (most precise — single event's buckets)
        event_ticker = f"{series_ticker}-{date_tag}"
        raw_markets: list[dict] = []

        try:
            data = self._get("/markets", params={"event_ticker": event_ticker, "limit": 100})
            raw_markets = data.get("markets", [])
            logger.debug(
                "event_ticker=%s → %d markets", event_ticker, len(raw_markets)
            )
        except Exception as exc:
            logger.warning("event_ticker query failed (%s), falling back to series query: %s", event_ticker, exc)

        # Step 2: fallback — query by series_ticker and filter by date string in ticker
        if not raw_markets:
            try:
                data = self._get(
                    "/markets",
                    params={"series_ticker": series_ticker, "status": "open", "limit": 100},
                )
                all_in_series = data.get("markets", [])
                raw_markets = [
                    m for m in all_in_series
                    if date_tag in m.get("ticker", "")
                ]
                logger.info(
                    "series_ticker=%s → %d total, %d match date %s",
                    series_ticker, len(all_in_series), len(raw_markets), date_tag,
                )
            except Exception as exc:
                logger.error("series query failed for %s: %s", series_ticker, exc)
                return []

        if not raw_markets:
            logger.warning(
                "No markets found for %s %s (series=%s event=%s)",
                station, event_date, series_ticker, event_ticker,
            )
            return []

        snapshots = []
        for m in raw_markets:
            snap = self._parse_market_snapshot(m, station)
            if snap:
                snapshots.append(snap)

        snapshots.sort(key=lambda s: (s.bucket_lower is None, s.bucket_lower or 0))
        logger.info(
            "%s %s — parsed %d/%d bucket snapshots (series=%s)",
            station, event_date, len(snapshots), len(raw_markets), series_ticker,
        )
        return snapshots

    def _parse_market_snapshot(self, raw: dict, station: str) -> MarketSnapshot | None:
        """
        Parse a single raw Kalshi market dict into a MarketSnapshot.

        Bucket boundary parsing strategy (in priority order):
          1. Title text — most reliable. Kalshi subtitles use two formats:
             a. Short:  "81° or above", "72° or below", "79° to 80°"
             b. Long:   "Will the high temp in LA be >73° on Apr 15, 2026?"
                        "Will the high temp in LA be <66° on Apr 15, 2026?"
                        "Will the high temp in LA be 72-73° on Apr 15, 2026?"
          2. Ticker B/T-suffix — fallback.

        Prices: Kalshi API v2 returns _dollars fields as strings already in
        the 0–1 fraction range ("0.0300" = 3¢ = 3%).  Legacy integer cent
        fields (yes_ask, yes_bid) may not be present.
        Status: Kalshi uses "active" for tradeable markets, not "open".
        Volume: returned as "volume_fp" (string float), not "volume".
        """
        import re

        ticker = raw.get("ticker", "")
        # Kalshi uses "subtitle" for the per-bucket label.
        # Long-form questions appear as "title"; subtitle has the shorter version.
        title  = raw.get("subtitle", raw.get("title", ""))
        status = raw.get("status", "")

        if not ticker:
            return None

        bucket_lower: int | None = None
        bucket_label_str         = ""

        # ── Strategy 1: parse from subtitle / title text ──────────────────
        if title:
            # Interior bucket: "79° to 80°", "82-83°", "72-73°"
            between = re.search(
                r"(\d+)\s*°?\s*(?:to|and|-)\s*(\d+)\s*°?",
                title, re.I,
            )
            # Upper tail:
            #   Short form:  "81° or above", "above 81°", "at or above 81°"
            #   Long form:   ">80°"  (strict greater → bucket starts at 81°)
            above = re.search(
                r"(\d+)\s*°?\s*or\s+above"         # group 1: "81° or above"
                r"|above\s+(\d+)\s*°?"             # group 2: "above 81°"
                r"|at\s+or\s+above\s+(\d+)\s*°?"  # group 3: "at or above 81°"
                r"|>\s*(\d+)\s*°?",                # group 4: ">80°"
                title, re.I,
            )
            # Lower tail:
            #   Short form:  "72° or below", "below 72°", "at or below 72°"
            #   Long form:   "<73°"  (strict less → bucket ends at 72°)
            below = re.search(
                r"(\d+)\s*°?\s*or\s+below"         # group 1: "72° or below"
                r"|below\s+(\d+)\s*°?"             # group 2: "below 72°"
                r"|at\s+or\s+below\s+(\d+)\s*°?"  # group 3: "at or below 72°"
                r"|<\s*(\d+)\s*°?",                # group 4: "<73°"
                title, re.I,
            )

            if between:
                lo, hi           = int(between.group(1)), int(between.group(2))
                bucket_lower     = lo
                bucket_label_str = f"{lo}° to {hi}°"
            elif below:
                g1, g2, g3, g4 = below.groups()
                if g1 or g2 or g3:
                    # "72° or below" → bucket_lower = 72
                    val = int(g1 or g2 or g3)
                    bucket_lower     = val
                    bucket_label_str = f"{val}° or below"
                else:
                    # "<73°" → bucket is "72° or below" → bucket_lower = 72
                    val = int(g4)
                    bucket_lower     = val - 1
                    bucket_label_str = f"{val - 1}° or below"
            elif above:
                g1, g2, g3, g4 = above.groups()
                if g1 or g2 or g3:
                    # "81° or above" → bucket_lower = 81
                    val = int(g1 or g2 or g3)
                    bucket_lower     = val
                    bucket_label_str = f"{val}° or above"
                else:
                    # ">80°" → bucket is "81° or above" → bucket_lower = 81
                    val = int(g4)
                    bucket_lower     = val + 1
                    bucket_label_str = f"{val + 1}° or above"

        # ── Strategy 2: ticker B/T-suffix (fallback) ──────────────────────
        if bucket_lower is None:
            center_match = re.search(r"-B([\d.]+)$", ticker)
            if center_match:
                center = float(center_match.group(1))
                if center != int(center):
                    lo               = int(center - 0.5)
                    hi               = int(center + 0.5)
                    bucket_lower     = lo
                    bucket_label_str = f"{lo}° to {hi}°"
                else:
                    bucket_lower     = int(center)
                    bucket_label_str = f"{int(center)}°"

        if bucket_lower is None:
            logger.debug("Could not parse bucket from ticker=%s title=%r", ticker, title)
            return None

        # ── Prices ────────────────────────────────────────────────────────
        # Kalshi API v2: prices in *_dollars fields as strings, already 0–1.
        # ("0.0300" = $0.03 = 3¢ = 3% implied probability)
        # Fall back to integer cent fields if _dollars fields are absent.
        if "yes_ask_dollars" in raw:
            yes_bid = float(raw.get("yes_bid_dollars") or "0")
            yes_ask = float(raw.get("yes_ask_dollars") or "1")
            no_bid  = float(raw.get("no_bid_dollars")  or "0")
            no_ask  = float(raw.get("no_ask_dollars")  or "1")
        else:
            yes_bid = (raw.get("yes_bid") or 0)   / 100.0
            yes_ask = (raw.get("yes_ask") or 100) / 100.0
            no_bid  = (raw.get("no_bid")  or 0)   / 100.0
            no_ask  = (raw.get("no_ask")  or 100) / 100.0

        # ── Volume ────────────────────────────────────────────────────────
        # API v2: "volume_fp" (string float). Legacy: "volume" (int).
        vol_raw = raw.get("volume_fp") or raw.get("volume", 0)
        volume  = int(float(vol_raw)) if vol_raw else 0

        # ── Status ────────────────────────────────────────────────────────
        # Kalshi uses "active" for tradeable markets. Accept both for safety.
        is_open = status in ("open", "active")

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
            volume       = volume,
            is_open      = is_open,
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

        if DRY_RUN:
            import uuid
            fake_id = f"dryrun-{uuid.uuid4().hex[:8]}"
            logger.info(
                "[DRY RUN] Order skipped | %s | %d contracts @ %.2f | fake_id=%s",
                market_id, contracts, limit_price, fake_id,
            )
            return OrderResult(
                success=True,
                order_id=fake_id,
                market_id=market_id,
                side=side,
                contracts=contracts,
                price=limit_price,
                error=None,
            )

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
        if DRY_RUN:
            import uuid
            fake_id = f"dryrun-{uuid.uuid4().hex[:8]}"
            logger.info(
                "[DRY RUN] Close skipped | %s | %d contracts @ %.2f | fake_id=%s",
                market_id, contracts, bid_price, fake_id,
            )
            return OrderResult(
                success=True,
                order_id=fake_id,
                market_id=market_id,
                side="yes",
                contracts=contracts,
                price=bid_price,
                error=None,
            )

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

        # Dry run orders have a fake ID — skip real API fill check
        if result.order_id.startswith("dryrun-"):
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

    # ── Market discovery ──────────────────────────────────────────────────

    def discover_new_series(self) -> list[str]:
        """
        Lightweight startup check: scan all open KXHIGH* events and
        return any series tickers not yet in KALSHI_STATION_SERIES.

        Called once on bot startup. If non-empty, logs a warning and the
        dashboard should surface it as an "unconfigured markets" alert.

        Run `python scripts/discover_markets.py` for full details on new series.
        """
        import re as _re
        known = set(KALSHI_STATION_SERIES.values())
        new_series: list[str] = []

        for prefix in ("KXHIGH",):
            cursor = None
            while True:
                params: dict = {"status": "open", "limit": 200}
                if cursor:
                    params["cursor"] = cursor
                try:
                    data   = self._get("/events", params=params)
                    events = data.get("events", [])
                    for e in events:
                        s = e.get("series_ticker", "")
                        if s.startswith(prefix) and s not in known and s not in new_series:
                            new_series.append(s)
                    cursor = data.get("cursor")
                    if not cursor or len(events) < 200:
                        break
                except Exception as exc:
                    logger.warning("discover_new_series %s scan failed: %s", prefix, exc)
                    break

        if new_series:
            logger.warning(
                "NEW Kalshi temperature series found (not in config): %s  "
                "Run `python scripts/discover_markets.py` for full details.",
                new_series,
            )
        else:
            logger.info("discover_new_series: no new series found")

        return new_series

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
