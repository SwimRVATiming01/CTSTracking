"""
dolphin5_events.py - Manual "load event list" for one pool's Dolphin5 unit.

Sends clearEvents, then setEventInfo for event indexes 1..EVENT_COUNT (one
heat each -- the heat number written to Dolphin5's .xml is correct
regardless of the configured heat count). Wire format per DolphinTCPTest's
README (both commands confirmed working over TCP, "Events cleared" /
"Event set" replies).

Kept out of dolphin5_control.py on purpose: that module guarantees
structurally that the automatic chase loop can never construct these
commands. This module is only ever reached from a dashboard button click
(with a "no race running" confirmation in the browser), never from any
loop or timer. Known Dolphin5 bug: clearEvents/setEventInfo sent while a
race is displayed in the Live view can crash it (DeletedRowInaccessible-
Exception dialog) -- restart Dolphin5 if that happens.

Reuses the pool's existing chase connection, so TCP control must be started.
"""

import logging
import threading
import time

import dolphin5_control

log = logging.getLogger("cts_tracker")

EVENT_COUNT = 200
SEND_GAP_SECONDS = 0.05  # small pacing so Dolphin5 isn't flooded with 200 lines at once

_load_lock = threading.Lock()  # one load at a time, across both pools


def load_events(pool_num):
    """Clear and reload events 1..EVENT_COUNT. Returns (ok, message)."""
    conn = dolphin5_control.get_connection(pool_num)
    if not conn or not conn.is_connected():
        return False, f"Pool {pool_num} Dolphin5 not connected (is TCP control ON?)"
    if not _load_lock.acquire(blocking=False):
        return False, "An event load is already in progress"
    try:
        log.warning(f"Dolphin5 pool {pool_num}: manual event load started (clearEvents + {EVENT_COUNT} events)")
        conn.send("clearEvents")
        time.sleep(0.5)
        for n in range(1, EVENT_COUNT + 1):
            conn.send(f"setEventInfo,{n},{n},Event {n},1,1,A")
            time.sleep(SEND_GAP_SECONDS)
    except (RuntimeError, OSError) as e:
        log.warning(f"Dolphin5 pool {pool_num}: event load failed: {e}")
        return False, f"Send failed partway: {e}"
    finally:
        _load_lock.release()
    log.info(f"Dolphin5 pool {pool_num}: manual event load finished")
    return True, f"Loaded events 1-{EVENT_COUNT} on pool {pool_num}"
