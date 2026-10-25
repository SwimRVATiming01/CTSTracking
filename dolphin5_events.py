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
PLACEHOLDER_EVENT = 999  # Streamline streaming placeholder; unloaded it shows up as blank-event .xml files
PLACEHOLDER_NAME = "Streamline"
SEND_GAP_SECONDS = 0.05  # small pacing so Dolphin5 isn't flooded with 200 lines at once

_load_lock = threading.Lock()  # one load at a time, across both pools


def _send_event_list(pool_num, events, what):
    """clearEvents, then setEventInfo for each (number, name) in order, one
    heat each. Returns (ok, message)."""
    conn = dolphin5_control.get_connection(pool_num)
    if not conn or not conn.is_connected():
        return False, f"Pool {pool_num} Dolphin5 not connected (is TCP control ON?)"
    if not _load_lock.acquire(blocking=False):
        return False, "An event load is already in progress"
    try:
        log.warning(f"Dolphin5 pool {pool_num}: manual event load started (clearEvents + {what})")
        conn.send("clearEvents")
        time.sleep(0.5)
        for idx, (num, name) in enumerate(events, start=1):
            name = name.replace(",", " ")  # commas would split the TCP command's fields
            conn.send(f"setEventInfo,{idx},{num},{name},1,1,A")
            time.sleep(SEND_GAP_SECONDS)
        # Streamline placeholder always lives in slot 999, not after the list
        conn.send(f"setEventInfo,{PLACEHOLDER_EVENT},{PLACEHOLDER_EVENT},{PLACEHOLDER_NAME},1,1,A")
    except (RuntimeError, OSError) as e:
        log.warning(f"Dolphin5 pool {pool_num}: event load failed: {e}")
        return False, f"Send failed partway: {e}"
    finally:
        _load_lock.release()
    log.info(f"Dolphin5 pool {pool_num}: manual event load finished")
    return True, f"Loaded {what} on pool {pool_num}"


def load_events(pool_num):
    """Clear and reload events 1..EVENT_COUNT plus the 999 placeholder."""
    events = [(n, f"Event {n}") for n in range(1, EVENT_COUNT + 1)]
    return _send_event_list(pool_num, events, f"events 1-{EVENT_COUNT} + {PLACEHOLDER_EVENT}")


def load_program_events(pool_num):
    """Clear and load every event in the active meet's Meet Program / Session
    Report (both pools get the full list -- no pool split), plus 999. Events
    only: one heat each, the .xml heat number is correct regardless."""
    import database
    meet = database.get_active_meet()
    if not meet:
        return False, "No active meet -- import a Meet Program first"
    events = []
    for event_id, name in database.get_event_names(meet["meet_id"]).items():
        try:
            num = int(str(event_id).strip())
        except ValueError:
            continue
        if num != PLACEHOLDER_EVENT:
            events.append((num, name or f"Event {num}"))
    if not events:
        return False, "No events found for the active meet"
    events.sort()
    return _send_event_list(pool_num, events, f"{len(events)} program events + {PLACEHOLDER_EVENT}")
