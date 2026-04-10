"""
Server-Sent Events broadcaster.

Scheduler pushes events here after every state change.
Dashboard SSE endpoint distributes them to all connected browsers.

Each browser connection gets its own Queue so no client misses an event
that another client already consumed.
"""

from __future__ import annotations

import queue
import threading
from datetime import datetime, timezone
from typing import Any

_clients: list[queue.Queue] = []
_clients_lock = threading.Lock()

_alert_history: list[dict] = []
_MAX_ALERTS = 100


# ---------------------------------------------------------------------------
# Client registration (called by SSE endpoint on connect/disconnect)
# ---------------------------------------------------------------------------

def register_client() -> queue.Queue:
    """Create and register a new per-client event queue."""
    q: queue.Queue = queue.Queue(maxsize=200)
    with _clients_lock:
        _clients.append(q)
    return q


def unregister_client(q: queue.Queue) -> None:
    with _clients_lock:
        try:
            _clients.remove(q)
        except ValueError:
            pass


# ---------------------------------------------------------------------------
# Event broadcasting
# ---------------------------------------------------------------------------

def push_event(event_type: str, data: dict[str, Any]) -> None:
    """
    Broadcast an event to every connected browser.
    Dead/full queues are silently removed.
    """
    payload = {"type": event_type, "data": data}
    with _clients_lock:
        dead: list[queue.Queue] = []
        for q in _clients:
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(q)
        for q in dead:
            try:
                _clients.remove(q)
            except ValueError:
                pass


def push_alert(title: str, message: str, level: str = "WARNING") -> None:
    """
    Push an alert event AND persist it to in-memory history.
    Level: INFO | WARNING | ERROR | CRITICAL
    """
    record = {
        "title":     title,
        "message":   message,
        "level":     level,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }
    _alert_history.append(record)
    if len(_alert_history) > _MAX_ALERTS:
        _alert_history.pop(0)
    push_event("alert", record)


def get_alert_history() -> list[dict]:
    """Return a copy of the persistent alert log (newest last)."""
    return list(_alert_history)
