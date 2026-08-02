"""
dolphin5_control.py - TCP control client for Dolphin5 timing units.

Drives each pool's Dolphin5 unit to follow GEN7's real ingested progress
("chase GEN7"), using the wire protocol validated in
Documents/Scripts/DolphinTCPTest (CRLF-terminated commands, setEventAndHeat
confirmed working end to end). One persistent connection per pool, built
from config.DOLPHIN5_CONFIGS.

Ports DolphinTCPTest's DolphinClient near-verbatim, plus two real fixes it
was missing: a send lock (nothing there previously prevented two callers
from interleaving mid-write) and an actual reconnect-on-drop loop (that
project's own connect_with_retry exists but is dead code, never called).

Command ownership: Bitfocus Companion only ever sends reset. This module is
the only thing that may send setEventAndHeat, and structurally never
constructs a reset, clearEvents, or setEventInfo command string anywhere in
this file — there is simply no code path that could send one, a stronger
guarantee than a runtime check would be (the latter two have a known
Dolphin5 crash bug when sent during an active race; reset pops a
human-confirmation dialog that blocks all TCP processing, which is why
Companion owns it exclusively).

This module is purely about sending — it is NOT imported by ingestion.py.
The exact-match tier there sources race identity only from each XML file's
own content, never from this module's in-memory "last commanded" state,
per an explicit user decision (see the Dolphin5 plan for the reasoning).
"""

import logging
import re
import socket
import threading
import time

import config
import database
from database import get_active_meet, get_current_heat_state

log = logging.getLogger("cts_tracker")

# Matches the trailing state-refresh push: CurrentEventAndHeat,(version),
# (eventIndex),(eventNumber),(heat),(race) — also the reply shape for
# getCurrentEventAndHeat. eventNumber is frequently blank.
_CURRENT_EVENT_AND_HEAT_RE = re.compile(
    r"^CurrentEventAndHeat,([^,]*),([^,]*),([^,]*),([^,]*),([^,]*)"
)

_state = {}              # pool_num -> diagnostic status dict, see get_connection_status()
_state_lock = threading.Lock()
_connections = {}        # pool_num -> live _Dolphin5Connection
_connections_lock = threading.Lock()
_stop_event = threading.Event()
_started = False
_started_lock = threading.Lock()


class _Dolphin5Connection:
    """One persistent CRLF-framed TCP connection to a single Dolphin5 unit."""

    def __init__(self, pool_num, host, port, on_message=None):
        self.pool_num = pool_num
        self.host = host
        self.port = port
        self.on_message = on_message
        self._sock = None
        self._send_lock = threading.Lock()
        self._buffer = b""
        self._connected = threading.Event()

    def connect(self):
        self._sock = socket.create_connection(
            (self.host, self.port), timeout=config.DOLPHIN5_CONNECT_TIMEOUT_SECONDS
        )
        self._sock.settimeout(None)
        self._connected.set()
        threading.Thread(target=self._read_loop, daemon=True).start()

    def is_connected(self):
        return self._connected.is_set()

    def send(self, command):
        """Send a raw command string. CRLF is appended automatically."""
        with self._send_lock:
            if not self._sock or not self._connected.is_set():
                raise RuntimeError(f"Dolphin5 pool {self.pool_num}: not connected")
            payload = (command.strip() + "\r\n").encode("ascii")
            self._sock.sendall(payload)

    def close(self):
        self._connected.clear()
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._sock.close()

    def _read_loop(self):
        while self._connected.is_set():
            try:
                chunk = self._sock.recv(4096)
            except OSError:
                break
            if not chunk:
                break
            self._buffer += chunk
            while b"\n" in self._buffer:
                line, self._buffer = self._buffer.split(b"\n", 1)
                text = line.decode("ascii", errors="replace").strip("\r")
                if text and self.on_message:
                    self.on_message(self.pool_num, text)
        self._connected.clear()


def _init_state(pool_num):
    with _state_lock:
        _state[pool_num] = {
            "connected": False,
            "last_sent_event_index": None,
            "last_sent_heat": None,
            "last_seen_event_index": None,
            "last_seen_event_number": None,
            "last_message": None,
            "last_message_at": None,
        }


def _on_message(pool_num, text):
    """
    Track every raw line received (for the dashboard's "last TCP response"
    display, regardless of type), plus — observational only — both the
    event index this module last sent via setEventAndHeat (recorded in
    _send_set_event_and_heat) and the event number seen in any incoming
    CurrentEventAndHeat push, a field that carries a real eventNumber over
    the wire even though it's never written into the logged XML file.
    Purely diagnostic, exposed via get_connection_status() so behavior can
    be watched during a live meet; never wired into any matching decision.
    """
    with _state_lock:
        st = _state[pool_num]
        st["last_message"] = text
        st["last_message_at"] = time.time()

    m = _CURRENT_EVENT_AND_HEAT_RE.match(text)
    if not m:
        return
    event_index, event_number = m.group(2).strip(), m.group(3).strip()
    with _state_lock:
        st = _state[pool_num]
        st["last_seen_event_index"] = event_index or None
        st["last_seen_event_number"] = event_number or None
    log.debug(f"Dolphin5 pool {pool_num} push: index={event_index!r} number={event_number!r}")


def _effective_config(pool_num):
    """
    Merge the dashboard-saved config (database.dolphin5_config, if any) over
    config.DOLPHIN5_CONFIGS' hardcoded fallback. Queried fresh on every
    connection attempt rather than cached, so a dashboard Save takes effect
    without needing a restart -- see force_reconnect for making that happen
    immediately even for an already-connected pool.
    """
    base = dict(config.DOLPHIN5_CONFIGS.get(pool_num, {}))
    try:
        saved = database.get_dolphin5_configs().get(pool_num, {})
    except Exception:
        saved = {}
    if saved.get("host"):
        base["host"] = saved["host"]
    if saved.get("port"):
        base["port"] = saved["port"]
    return base


def get_effective_config(pool_num):
    """Public wrapper for the dashboard to display the current host/port."""
    return _effective_config(pool_num)


def update_config(pool_num, host=None, port=None):
    """Persist a dashboard-saved host/port for one pool and reconnect it
    immediately with the new value, rather than waiting for its next
    natural disconnect."""
    database.save_dolphin5_config(pool_num, host, port)
    force_reconnect(pool_num)


def force_reconnect(pool_num):
    """Close a pool's current connection (if any) so its supervisor loop
    immediately retries with whatever config is now in effect."""
    with _connections_lock:
        conn = _connections.get(pool_num)
    if conn:
        conn.close()


def _connection_supervisor(pool_num):
    """Keep exactly one connection alive per pool, reconnecting on drop."""
    while not _stop_event.is_set():
        cfg = _effective_config(pool_num)
        host, port = cfg.get("host"), cfg.get("port")
        if not host or not port:
            log.debug(f"Dolphin5 pool {pool_num}: no host/port configured yet, waiting")
            time.sleep(config.DOLPHIN5_RECONNECT_DELAY_SECONDS)
            continue

        conn = _Dolphin5Connection(pool_num, host, port, on_message=_on_message)
        try:
            conn.connect()
            conn.send("autoSendUpdates,ON")
        except OSError as e:
            log.warning(
                f"Dolphin5 pool {pool_num}: connect failed ({e}), "
                f"retrying in {config.DOLPHIN5_RECONNECT_DELAY_SECONDS}s"
            )
            time.sleep(config.DOLPHIN5_RECONNECT_DELAY_SECONDS)
            continue

        with _connections_lock:
            _connections[pool_num] = conn
        with _state_lock:
            _state[pool_num]["connected"] = True
        log.info(f"Dolphin5 pool {pool_num}: connected to {host}:{port}")

        while conn.is_connected() and not _stop_event.is_set():
            time.sleep(0.5)

        with _state_lock:
            _state[pool_num]["connected"] = False
        conn.close()
        if not _stop_event.is_set():
            log.warning(
                f"Dolphin5 pool {pool_num}: disconnected, "
                f"reconnecting in {config.DOLPHIN5_RECONNECT_DELAY_SECONDS}s"
            )
            time.sleep(config.DOLPHIN5_RECONNECT_DELAY_SECONDS)


_warned_disconnected = set()  # pool_nums already warned about, until reconnected -- logging noise reduction only, no correctness impact if this races


def _send_set_event_and_heat(pool_num, event_id, heat):
    """
    The only command this module constructs and sends besides
    autoSendUpdates,ON at connect time. There is no other send() call site
    in this file — reset/clearEvents/setEventInfo are never constructed
    anywhere here.
    """
    with _connections_lock:
        conn = _connections.get(pool_num)
    if not conn or not conn.is_connected():
        # The chase loop retries every poll tick while disconnected (so it
        # fires immediately once reconnected) -- log the first occurrence at
        # WARNING, then drop to debug so a stretch of downtime doesn't spam
        # one warning line every DOLPHIN5_POLL_INTERVAL_SECONDS.
        if pool_num not in _warned_disconnected:
            log.warning(f"Dolphin5 pool {pool_num}: chase send skipped, not connected")
            _warned_disconnected.add(pool_num)
        else:
            log.debug(f"Dolphin5 pool {pool_num}: chase send skipped, still not connected")
        return False
    _warned_disconnected.discard(pool_num)

    try:
        event_int = int(event_id)
    except (TypeError, ValueError):
        log.warning(
            f"Dolphin5 pool {pool_num}: non-numeric event_id {event_id!r} "
            f"(relay/exhibition event?), skipping chase send this tick"
        )
        return False

    try:
        conn.send(f"setEventAndHeat,{event_int},{heat}")
    except (RuntimeError, OSError) as e:
        log.warning(f"Dolphin5 pool {pool_num}: setEventAndHeat send failed: {e}")
        return False

    with _state_lock:
        _state[pool_num]["last_sent_event_index"] = event_int
        _state[pool_num]["last_sent_heat"] = heat
    log.info(f"Dolphin5 pool {pool_num}: sent setEventAndHeat,{event_int},{heat}")
    return True


def _chase_loop():
    """
    Polls get_current_heat_state() every DOLPHIN5_POLL_INTERVAL_SECONDS.

    Debounced, not immediate: a changed (event, heat) value starts (or
    updates) a per-pool candidate, and setEventAndHeat is only sent once
    that candidate has held steady for DOLPHIN5_CHASE_DEBOUNCE_SECONDS. If
    GEN7's state changes again before the debounce elapses, the candidate
    resets to the new value and the timer restarts. This prevents a human
    quickly stepping through events/heats on GEN7 from firing one
    setEventAndHeat per poll tick — Dolphin5 only ever receives the value
    GEN7 actually settled on.
    """
    candidates = {}   # pool_num -> (value, first_seen_monotonic)
    last_sent = {}    # pool_num -> value already pushed to Dolphin5

    while not _stop_event.is_set():
        active = get_active_meet()
        if active:
            state = get_current_heat_state(active["meet_id"])
            now = time.monotonic()
            for pool_num, key in ((1, "pool1"), (2, "pool2")):
                pool_state = state.get(key) or {}
                if not pool_state.get("active"):
                    continue
                value = (pool_state.get("current_event"), pool_state.get("current_heat"))
                if value == (None, None):
                    continue

                if candidates.get(pool_num, (None, None))[0] != value:
                    candidates[pool_num] = (value, now)

                held_value, first_seen = candidates[pool_num]
                settled = (now - first_seen) >= config.DOLPHIN5_CHASE_DEBOUNCE_SECONDS
                if held_value != last_sent.get(pool_num) and settled:
                    event_id, heat = held_value
                    if _send_set_event_and_heat(pool_num, event_id, heat):
                        last_sent[pool_num] = held_value

        time.sleep(config.DOLPHIN5_POLL_INTERVAL_SECONDS)


def is_running():
    """Whether start() has been called yet this process (idempotent check)."""
    return _started


def start():
    """
    Start one persistent connection per pool plus the chase-loop thread.

    Called explicitly — either config-gated at boot from cts_tracker.py, or
    on demand via the dashboard's Dolphin5 panel (see routes.py) so TCP
    control can be turned on for a running session without a full server
    restart. Not an import-time auto-start either way — this is the one
    module that writes to live hardware the moment it runs; an implicit
    auto-start would make that side effect invisible to anyone reading the
    startup sequence. Idempotent: safe to call more than once (e.g. a
    dashboard button clicked twice) — only the first call does anything.
    """
    global _started
    with _started_lock:
        if _started:
            log.info("Dolphin5 control start() called again, already running — ignoring")
            return
        _started = True

    for pool_num in config.DOLPHIN5_CONFIGS:
        _init_state(pool_num)
        threading.Thread(target=_connection_supervisor, args=(pool_num,), daemon=True).start()
    threading.Thread(target=_chase_loop, daemon=True).start()
    log.info("Dolphin5 control started (chase-GEN7 TCP control)")


def get_connection_status(pool_num):
    """Cached diagnostic status for a dashboard health indicator. Non-blocking."""
    with _state_lock:
        return dict(_state.get(pool_num, {}))
