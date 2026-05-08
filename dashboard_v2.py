"""
dashboard_v2.py

HRRR divergence bot dashboard. Reads live state from scheduler_v2 module.
Start alongside scheduler_v2 via run_v2.py.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict
from datetime import date, datetime, timezone
from functools import wraps
from typing import Any
from zoneinfo import ZoneInfo

import config as cfg
from flask import Flask, Response, jsonify, render_template, request, stream_with_context
from utils.events import get_alert_history, push_event, register_client, unregister_client
from utils.logging_config import setup_logging

logger = setup_logging("dashboard_v2")

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
_BUILD_VERSION = str(int(time.time()))


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not cfg.DASHBOARD_PASSWORD:
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or auth.username != cfg.DASHBOARD_USER or auth.password != cfg.DASHBOARD_PASSWORD:
            return Response(
                "Unauthorized",
                401,
                {"WWW-Authenticate": 'Basic realm="WeatherBot"'},
            )
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def _safe_dict(obj) -> Any:
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _safe_dict(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_safe_dict(i) for i in obj]
    if hasattr(obj, "__dataclass_fields__"):
        return {k: _safe_dict(v) for k, v in asdict(obj).items()}
    return str(obj)


# ---------------------------------------------------------------------------
# State builder
# ---------------------------------------------------------------------------

def _get_full_state() -> dict:
    try:
        import scheduler_v2 as sched
    except ImportError:
        return {"error": "scheduler_v2 not loaded"}

    now_utc = datetime.now(timezone.utc)

    with sched._positions_lock:
        positions_raw  = dict(sched._open_positions)
        snapshots_raw  = dict(sched._station_snapshots)

    # ── Open positions ────────────────────────────────────────────────────
    positions_out   = {}
    total_unrealized = 0.0

    for mid, pos in positions_raw.items():
        d = _safe_dict(pos)
        station = pos.station
        tz = ZoneInfo(cfg.STATION_TIMEZONES.get(station, "UTC"))

        try:
            entry_utc   = pos.entry_time
            elapsed_s   = int((now_utc - entry_utc).total_seconds())
            entry_local = entry_utc.astimezone(tz)
            h    = entry_local.hour % 12 or 12
            ampm = "AM" if entry_local.hour < 12 else "PM"
            d["entry_time_display"] = (
                f"{h}:{entry_local.strftime('%M')} {ampm} {entry_local.strftime('%Z')}"
            )
            if elapsed_s < 60:
                d["elapsed"] = "just now"
            elif elapsed_s < 3600:
                d["elapsed"] = f"{elapsed_s // 60}m ago"
            else:
                d["elapsed"] = f"{elapsed_s // 3600}h {(elapsed_s % 3600) // 60}m ago"
        except Exception:
            d["entry_time_display"] = "—"
            d["elapsed"] = "—"

        d["city"] = cfg.STATION_CITY_NAMES.get(station, station)
        # Use the real Kalshi label from the snapshot (e.g. "79° to 80°").
        # Fall back to constructing it only if no snapshot is available yet.
        station_markets = snapshots_raw.get(station, [])
        snap = next((m for m in station_markets if m.market_id == mid), None)
        d["bucket_label"] = snap.bucket_label if snap else f"{pos.bucket_lower}° to {pos.bucket_lower + 1}°"
        total_unrealized += pos.unrealized_pnl or 0.0
        positions_out[mid] = d

    # ── Station bucket snapshots ──────────────────────────────────────────
    snapshots_out = {}
    for station, markets in snapshots_raw.items():
        snapshots_out[station] = [
            _safe_dict(m)
            for m in sorted(markets, key=lambda m: m.bucket_lower)
        ]

    # ── HRRR engine state per station ─────────────────────────────────────
    hrrr_out = {}
    engine = getattr(sched, "_engine", None)
    if engine is not None:
        for station, state in engine._state.items():
            if state.last_tmax_f is None:
                continue
            hrrr_out[station] = {
                "city":           cfg.STATION_CITY_NAMES.get(station, station),
                "last_tmax_f":    round(state.last_tmax_f, 1),
                "last_run_time":  (
                    state.last_run_time.strftime("%H:%Mz")
                    if state.last_run_time else None
                ),
                "morning_flags":  list(state.morning_flags.keys()),
                "trades_today":   len(state.traded),
            }

    # ── Summary ───────────────────────────────────────────────────────────
    daily_pnl = round(getattr(sched, "_daily_pnl", 0.0), 2)
    summary = {
        "open_positions":    len(positions_out),
        "total_unrealized":  round(total_unrealized, 2),
        "daily_pnl":         daily_pnl,
        "total_pnl":         round(daily_pnl + total_unrealized, 2),
        "bankroll":          cfg.STARTING_BANKROLL,
        "mode":              (
            "DEMO" if cfg.USE_DEMO
            else ("DRY RUN" if cfg.DRY_RUN else "LIVE")
        ),
    }

    return {
        "summary":           summary,
        "positions":         positions_out,
        "station_snapshots": snapshots_out,
        "hrrr_state":        hrrr_out,
        "trade_history":     list(reversed(getattr(sched, "_trade_history", [])[-100:])),
        "alerts":            get_alert_history(),
        "city_names":        dict(cfg.STATION_CITY_NAMES),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
@_require_auth
def index():
    state = _get_full_state()
    return render_template("index_v2.html", initial_state=state, config_version=_BUILD_VERSION)


@app.route("/stream")
@_require_auth
def stream():
    client_queue = register_client()

    def event_generator():
        try:
            state = _get_full_state()
            yield f"event: full_state\ndata: {json.dumps(state)}\n\n"
        except Exception as exc:
            logger.warning("SSE initial state failed: %s", exc)
        try:
            while True:
                try:
                    event = client_queue.get(timeout=25)
                    yield f"event: {event['type']}\ndata: {json.dumps(event['data'])}\n\n"
                except Exception:
                    yield "event: heartbeat\ndata: {}\n\n"
        finally:
            unregister_client(client_queue)

    return Response(
        stream_with_context(event_generator()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/state")
@_require_auth
def api_state():
    return jsonify(_get_full_state())


@app.route("/api/close-position/<path:market_id>", methods=["POST"])
@_require_auth
def close_position(market_id: str):
    import scheduler_v2 as sched
    from kalshi_client import KalshiClient

    with sched._positions_lock:
        pos = sched._open_positions.get(market_id)
    if pos is None:
        return jsonify({"ok": False, "error": "Position not found"}), 404

    client  = KalshiClient()
    markets = sched._get_markets(client, pos.station, pos.event_date)
    market  = next((m for m in markets if m.market_id == market_id), None)
    if not market:
        return jsonify({"ok": False, "error": "Could not fetch market snapshot"}), 500

    bid = market.yes_bid if pos.side == "YES" else (1.0 - market.yes_ask)
    sched._exit_position(client, pos, bid, "Manual close via dashboard")
    push_event("position_closed", {"market_id": market_id})
    return jsonify({
        "ok":  True,
        "pnl": round((bid - pos.entry_price) * pos.contracts, 4),
    })


@app.route("/api/close-all", methods=["POST"])
@_require_auth
def close_all():
    import scheduler_v2 as sched
    from kalshi_client import KalshiClient

    with sched._positions_lock:
        snapshot = dict(sched._open_positions)

    client  = KalshiClient()
    results = []
    for market_id, pos in snapshot.items():
        markets = sched._get_markets(client, pos.station, pos.event_date)
        market  = next((m for m in markets if m.market_id == market_id), None)
        if not market:
            results.append({"market_id": market_id, "ok": False, "error": "No snapshot"})
            continue
        bid = market.yes_bid if pos.side == "YES" else (1.0 - market.yes_ask)
        ok  = sched._exit_position(client, pos, bid, "Close all via dashboard")
        results.append({"market_id": market_id, "ok": ok})

    push_event("position_closed", {"market_id": "all"})
    return jsonify({"ok": True, "results": results})
