"""
companion_state.py - Bitfocus Companion's posted current event/heat per pool.

Lives in its own leaf module (not routes.py) so database.py's
get_current_heat_state() can read it directly without a circular import --
routes.py already imports both database.py and dolphin5_control.py, so
either of those importing routes.py back would cycle.

In-memory only, not DB-persisted: this mirrors Companion's own continuous
re-posting (confirmed live 2026-08-02, ~every 10s regardless of change),
so a restart just waits for the next post rather than needing history.
"""

import threading
import time

STALE_SECONDS = 30  # ~3x Companion's confirmed ~10s periodic re-post interval

_lock = threading.Lock()
_state = {1: None, 2: None}  # pool -> {"event_id", "heat", "last_seen"} or None


def set_heat(pool, event_id, heat):
    with _lock:
        _state[pool] = {"event_id": event_id, "heat": heat, "last_seen": time.time()}


def clear_heat(pool):
    with _lock:
        _state[pool] = None


def get_raw(pool):
    """Last posted {"event_id","heat","last_seen"} for this pool, or None --
    regardless of staleness. Used for dashboard highlighting/"connected"
    status, which predates and is independent of the freshness gate below."""
    with _lock:
        s = _state.get(pool)
        return dict(s) if s else None


def get_fresh(pool):
    """{"event_id","heat"} if a value exists and was posted within
    STALE_SECONDS, else None. Used by get_current_heat_state() to decide
    whether Companion's value is trustworthy enough to drive chase-GEN7 and
    the Companion GET endpoints."""
    with _lock:
        s = _state.get(pool)
    if not s or (time.time() - s["last_seen"] > STALE_SECONDS):
        return None
    return {"event_id": s["event_id"], "heat": s["heat"]}
