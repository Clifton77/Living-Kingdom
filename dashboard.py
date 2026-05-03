"""
WeatherBot web dashboard.

Single-page Flask app. Connects to the running scheduler via shared
module state (get_risk_manager, get_latest_signals) and pushes live
updates to browsers via Server-Sent Events.

Start via run.py — do not run this file directly.
"""

from __future__ import annotations

import json
import math
import os
import threading
import time
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from functools import wraps
from typing import Any
from zoneinfo import ZoneInfo

_BUILD_VERSION = str(int(time.time()))

import config as cfg
from flask import Flask, Response, jsonify, render_template, request, stream_with_context
from utils.events import get_alert_history, push_event, register_client, unregister_client
from utils.logging_config import setup_logging

logger = setup_logging("dashboard")

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True

# ---------------------------------------------------------------------------
# Settings override — live threshold adjustments persist across restarts
# ---------------------------------------------------------------------------
_SETTINGS_FILE = os.path.join(cfg.LOGS_DIR, "settings_override.json")

_ADJUSTABLE_SETTINGS = {
    "STOP_LOSS_PCT":                 ("Stop-loss %",              0.1,  0.9),
    "PROFIT_REVERSAL_THRESHOLD":     ("Profit reversal threshold", 0.0, 0.5),
    "EXPANSION_EDGE_MIN":            ("Expansion edge min",        0.1,  0.5),
    "SIGNIFICANT_REPOSITION_EDGE_MIN": ("Significant reposition edge", 0.1, 0.5),
    "MAJOR_REPOSITION_EDGE_MIN":     ("Major reposition edge",    0.1,  0.5),
    "MIN_EDGE":                       ("Min edge to trade",        0.01, 0.20),
    "MIN_KELLY_STAKE":               ("Min Kelly stake ($)",       0.5, 10.0),
}


def _load_settings_overrides():
    """Apply any saved overrides to the live config module."""
    if not os.path.exists(_SETTINGS_FILE):
        return
    try:
        with open(_SETTINGS_FILE) as f:
            overrides = json.load(f)
        for key, value in overrides.items():
            if key in _ADJUSTABLE_SETTINGS and hasattr(cfg, key):
                setattr(cfg, key, value)
                logger.info("Settings override applied: %s = %s", key, value)
    except Exception as exc:
        logger.warning("Failed to load settings overrides: %s", exc)


def _save_settings_overrides(overrides: dict):
    os.makedirs(cfg.LOGS_DIR, exist_ok=True)
    with open(_SETTINGS_FILE, "w") as f:
        json.dump(overrides, f, indent=2)


_load_settings_overrides()


# ---------------------------------------------------------------------------
# Basic Auth
# ---------------------------------------------------------------------------

def _require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not cfg.DASHBOARD_PASSWORD:
            # No password set — allow access (dev mode)
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or auth.username != cfg.DASHBOARD_USER or auth.password != cfg.DASHBOARD_PASSWORD:
            return Response(
                "Unauthorized — set DASHBOARD_USER / DASHBOARD_PASSWORD in .env",
                401,
                {"WWW-Authenticate": 'Basic realm="WeatherBot"'},
            )
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# State serialization helpers
# ---------------------------------------------------------------------------

def _safe_dict(obj) -> Any:
    """Recursively convert dataclasses / dates / non-serializable objects."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _safe_dict(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_dict(i) for i in obj]
    if hasattr(obj, "__dataclass_fields__"):
        return {k: _safe_dict(v) for k, v in asdict(obj).items()}
    return str(obj)


def _enrich_position_display(pos: dict, now_utc: datetime) -> dict:
    """Add display-friendly fields to a serialized position dict."""
    station = pos.get("station", "")
    tz      = ZoneInfo(cfg.STATION_TIMEZONES.get(station, "UTC"))

    # ── Entry time: local clock + relative elapsed ────────────────────────
    entry_iso = pos.get("entry_time", "")
    if entry_iso:
        try:
            entry_utc   = datetime.fromisoformat(entry_iso)
            entry_local = entry_utc.astimezone(tz)
            # Cross-platform 12-hour format (avoid %-I which is Linux-only)
            h    = entry_local.hour % 12 or 12
            ampm = "AM" if entry_local.hour < 12 else "PM"
            tz_abbr = entry_local.strftime("%Z")
            local_str = f"{h}:{entry_local.strftime('%M')} {ampm} {tz_abbr}"

            elapsed_s = int((now_utc - entry_utc).total_seconds())
            if elapsed_s < 60:
                rel = "just now"
            elif elapsed_s < 3600:
                rel = f"{elapsed_s // 60}m ago"
            else:
                rel = f"{elapsed_s // 3600}h ago"

            pos["entry_time_display"] = f"{local_str} · {rel}"
        except Exception:
            pos["entry_time_display"] = entry_iso
    else:
        pos["entry_time_display"] = "—"

    # ── Event date display ────────────────────────────────────────────────
    event_date_str = pos.get("event_date", "")
    if event_date_str:
        try:
            event_d   = date.fromisoformat(event_date_str)
            month_day = event_d.strftime("%b ") + str(event_d.day)
            if event_d == now_utc.date():
                pos["event_date_display"] = f"Today ({month_day})"
            else:
                pos["event_date_display"] = month_day
        except Exception:
            pos["event_date_display"] = event_date_str
    else:
        pos["event_date_display"] = "—"

    # ── Pending settlement display ────────────────────────────────────────
    if pos.get("pending_settlement"):
        entry_price = pos.get("entry_price", 0.0) or 0.0
        contracts   = pos.get("contracts", 0) or 0
        # Estimated max payout if this side wins at $1.00/contract
        pos["pending_payout_usd"] = round((1.0 - entry_price) * contracts, 2)

    return pos


def _get_full_state() -> dict:
    """Build a complete state snapshot for initial page render or /api/state."""
    from scheduler import get_risk_manager, get_latest_signals, _tier_last_run, get_trade_history

    rm      = get_risk_manager()
    summary = rm.summary()
    signals = get_latest_signals()

    now_utc = datetime.now(timezone.utc)
    positions_out = {}
    for mid, pos in rm.state.positions.items():
        positions_out[mid] = _enrich_position_display(_safe_dict(pos), now_utc)

    signals_out = {}
    for station, sig in signals.items():
        signals_out[station] = _safe_dict(sig)

    settings_current = {
        k: getattr(cfg, k) for k in _ADJUSTABLE_SETTINGS
    }

    # Bias table last-updated timestamp
    bias_updated = "—"
    if os.path.exists(cfg.BIAS_PARQUET):
        try:
            ts = os.path.getmtime(cfg.BIAS_PARQUET)
            bias_updated = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            pass

    return {
        "summary":      summary,
        "positions":    positions_out,
        "signals":      signals_out,
        "alerts":       get_alert_history(),
        "tier_status":  dict(_tier_last_run),
        "mode":         "DEMO" if cfg.USE_DEMO else ("LIVE (dry run)" if cfg.DRY_RUN else "LIVE"),
        "is_halted":    summary["is_halted"],
        "kill_switch":  summary["kill_switch"],
        "settings":     settings_current,
        "adjustable_settings": {
            k: {"label": v[0], "min": v[1], "max": v[2], "value": getattr(cfg, k)}
            for k, v in _ADJUSTABLE_SETTINGS.items()
        },
        "city_names":      dict(cfg.STATION_CITY_NAMES),
        "display_station_ids": {"KJFK": "KNYC", "KORD": "KMDW"},
        "bias_updated":    bias_updated,
        "trade_history":   list(reversed(get_trade_history())),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
@_require_auth
def index():
    state = _get_full_state()
    return render_template("index.html", initial_state=state, config_version=_BUILD_VERSION)


@app.route("/stream")
@_require_auth
def stream():
    """SSE endpoint — each browser gets its own queue."""
    client_queue = register_client()

    def event_generator():
        # Send full state on connect so the client is immediately in sync
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
                    # Timeout — send heartbeat to keep connection alive
                    yield "event: heartbeat\ndata: {}\n\n"
        finally:
            unregister_client(client_queue)

    return Response(
        stream_with_context(event_generator()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":  "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/api/state")
@_require_auth
def api_state():
    return jsonify(_get_full_state())


@app.route("/api/kill-switch/activate", methods=["POST"])
@_require_auth
def kill_switch_activate():
    from scheduler import get_risk_manager
    from utils.alerting import alert_kill_switch
    rm = get_risk_manager()
    rm.activate_kill_switch()
    alert_kill_switch("dashboard")
    push_event("state_update", rm.summary())
    return jsonify({"ok": True, "is_halted": True})


@app.route("/api/kill-switch/deactivate", methods=["POST"])
@_require_auth
def kill_switch_deactivate():
    from scheduler import get_risk_manager
    rm = get_risk_manager()
    rm.deactivate_kill_switch()
    push_event("state_update", rm.summary())
    return jsonify({"ok": True, "is_halted": False})


@app.route("/api/reset-daily-pnl", methods=["POST"])
@_require_auth
def reset_daily_pnl():
    from scheduler import get_risk_manager
    import datetime
    rm = get_risk_manager()
    rm.state.daily_pnl = 0.0
    rm.state.realized_pnl = 0.0
    rm.state.session_date = datetime.date.today().isoformat()
    rm.state.trade_count_today = 0
    rm.state.wins_today = 0
    rm.state.losses_today = 0
    rm.state.reversal_blocked = []
    rm.state.stop_loss_count = {}
    rm._save_state()
    push_event("state_update", rm.summary())
    return jsonify({"ok": True, "is_halted": rm.is_halted})


@app.route("/api/signal-pass", methods=["POST"])
@_require_auth
def manual_signal_pass():
    from scheduler import trigger_signal_pass_now
    trigger_signal_pass_now()
    return jsonify({"ok": True, "message": "Signal pass triggered — updates arriving via SSE"})


@app.route("/api/close-position/<path:market_id>", methods=["POST"])
@_require_auth
def close_position(market_id: str):
    from scheduler import get_risk_manager, get_kalshi, append_trade_history
    rm     = get_risk_manager()
    kalshi = get_kalshi()

    pos = rm.state.positions.get(market_id)
    if pos is None:
        return jsonify({"ok": False, "error": "Position not found"}), 404

    snap = kalshi.get_market_snapshot(pos.station, date.fromisoformat(pos.event_date), pos.bucket_lower)
    if snap is None:
        return jsonify({"ok": False, "error": "Could not fetch market snapshot"}), 500

    entry_side = getattr(pos, "entry_side", "yes")
    bid = snap.no_bid if entry_side == "no" else snap.yes_bid
    result = kalshi.close_position(market_id, pos.contracts, bid, entry_side=entry_side)
    if not result.success:
        return jsonify({"ok": False, "error": result.error}), 500

    station      = pos.station
    bucket_lower = pos.bucket_lower
    entry_price  = pos.entry_price

    realized = rm.close_position(market_id, bid, "Manual close via dashboard")

    tz = ZoneInfo(cfg.STATION_TIMEZONES.get(station, "UTC"))
    now_local = datetime.now(tz)
    h = now_local.hour % 12 or 12
    ampm = "AM" if now_local.hour < 12 else "PM"
    ts = f"{now_local.strftime('%b')} {now_local.day} {h}:{now_local.strftime('%M')} {ampm} {now_local.strftime('%Z')}"

    from utils.sheets import get_sheets_logger
    get_sheets_logger().log_trade_closed(station, market_id, bid, realized, "Manual close via dashboard")

    closed_record = {
        "ts":           ts,
        "type":         "CLOSE",
        "market_id":    market_id,
        "station":      station,
        "bucket_lower": bucket_lower,
        "entry_price":  entry_price,
        "exit_price":   bid,
        "realized_pnl": realized,
        "reason":       "Manual close via dashboard",
    }
    append_trade_history(closed_record)
    push_event("state_update", rm.summary())
    push_event("position_closed", closed_record)
    return jsonify({"ok": True, "realized_pnl": round(realized, 4)})


@app.route("/api/close-all", methods=["POST"])
@_require_auth
def close_all():
    from scheduler import get_risk_manager, get_kalshi, append_trade_history
    rm     = get_risk_manager()
    kalshi = get_kalshi()

    results = []
    for market_id, pos in list(rm.state.positions.items()):
        snap = kalshi.get_market_snapshot(pos.station, date.fromisoformat(pos.event_date), pos.bucket_lower)
        entry_side = getattr(pos, "entry_side", "yes")
        bid = (snap.no_bid if entry_side == "no" else snap.yes_bid) if snap else 0.0
        result = kalshi.close_position(market_id, pos.contracts, bid, entry_side=entry_side)
        if result.success:
            station      = pos.station
            bucket_lower = pos.bucket_lower
            entry_price  = pos.entry_price
            realized = rm.close_position(market_id, bid, "Close all via dashboard")

            tz = ZoneInfo(cfg.STATION_TIMEZONES.get(station, "UTC"))
            now_local = datetime.now(tz)
            h = now_local.hour % 12 or 12
            ampm = "AM" if now_local.hour < 12 else "PM"
            ts = f"{now_local.strftime('%b')} {now_local.day} {h}:{now_local.strftime('%M')} {ampm} {now_local.strftime('%Z')}"

            closed_record = {
                "ts":           ts,
                "type":         "CLOSE",
                "market_id":    market_id,
                "station":      station,
                "bucket_lower": bucket_lower,
                "entry_price":  entry_price,
                "exit_price":   bid,
                "realized_pnl": round(realized, 4),
                "reason":       "Close all via dashboard",
            }
            append_trade_history(closed_record)
            push_event("position_closed", closed_record)
            results.append({"market_id": market_id, "realized_pnl": round(realized, 4), "ok": True})
        else:
            results.append({"market_id": market_id, "ok": False, "error": result.error})

    push_event("state_update", rm.summary())
    return jsonify({"ok": True, "results": results})


@app.route("/api/rebuild-bias", methods=["POST"])
@_require_auth
def rebuild_bias():
    """
    Trigger a bias table rebuild in a background thread.
    Progress arrives via SSE events: bias_rebuild_start → bias_rebuild_done.
    """
    import subprocess
    import sys

    def _run():
        push_event("bias_rebuild_start", {"message": "Bias table rebuild started…"})
        try:
            script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "scripts", "build_bias_table.py")
            result = subprocess.run(
                [sys.executable, script],
                capture_output=True, text=True,
                cwd=os.path.dirname(os.path.abspath(__file__)),
                timeout=120,
            )
            if result.returncode == 0:
                # Refresh timestamp
                bias_updated = "—"
                if os.path.exists(cfg.BIAS_PARQUET):
                    try:
                        ts = os.path.getmtime(cfg.BIAS_PARQUET)
                        bias_updated = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                    except Exception:
                        pass
                push_event("bias_rebuild_done", {
                    "ok": True,
                    "message": "Bias table rebuilt successfully.",
                    "bias_updated": bias_updated,
                })
                logger.info("Bias table rebuild completed via dashboard")
            else:
                err = (result.stderr or result.stdout or "Unknown error")[-500:]
                push_event("bias_rebuild_done", {"ok": False, "message": f"Rebuild failed: {err}"})
                logger.warning("Bias rebuild failed: %s", err)
        except subprocess.TimeoutExpired:
            push_event("bias_rebuild_done", {"ok": False, "message": "Rebuild timed out (>2 min)."})
        except Exception as exc:
            push_event("bias_rebuild_done", {"ok": False, "message": str(exc)})
            logger.error("Bias rebuild exception: %s", exc)

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "message": "Rebuild started — progress via SSE"})


@app.route("/api/settings", methods=["GET"])
@_require_auth
def get_settings():
    return jsonify({
        k: {"label": v[0], "min": v[1], "max": v[2], "value": getattr(cfg, k)}
        for k, v in _ADJUSTABLE_SETTINGS.items()
    })


@app.route("/api/settings", methods=["POST"])
@_require_auth
def save_settings():
    data = request.get_json() or {}
    saved = {}
    errors = []

    for key, value in data.items():
        if key not in _ADJUSTABLE_SETTINGS:
            errors.append(f"Unknown setting: {key}")
            continue
        _, min_val, max_val = _ADJUSTABLE_SETTINGS[key]
        try:
            value = float(value)
        except (TypeError, ValueError):
            errors.append(f"{key}: not a number")
            continue
        if not (min_val <= value <= max_val):
            errors.append(f"{key}: {value} out of range [{min_val}, {max_val}]")
            continue
        setattr(cfg, key, value)
        saved[key] = value

    if saved:
        _save_settings_overrides({k: getattr(cfg, k) for k in _ADJUSTABLE_SETTINGS})
        logger.info("Settings updated: %s", saved)

    return jsonify({"ok": not errors, "saved": saved, "errors": errors})
