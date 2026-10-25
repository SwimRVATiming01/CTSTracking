"""
companion_peers.py - Which computer runs each Bitfocus Companion, and is it up.

Three Companions: pool 1, pool 2 (host/port set from the dashboard, since
they can be on different computers meet to meet) and the streaming PC's
(fixed in config.COMPANION_STREAMING; posts event/heat for both pools).

Two independent signals per Companion:
  - reachable: a background thread GETs each one's web UI every
    COMPANION_PROBE_INTERVAL_SECONDS. Works even when Companion is idle.
  - last heard: when a request from that IP last hit any /api/companion
    route (recorded by routes.py). Requests from IPs not assigned to any
    Companion are kept too, so a stray Companion shows up instead of
    silently doing nothing.

Purely read-only toward Companion -- never sends it commands.
"""

import logging
import threading
import time
import urllib.error
import urllib.request

import config
import database

log = logging.getLogger("cts_tracker")

ROLES = ("pool1", "pool2", "streaming")

_lock = threading.Lock()
_probe = {}          # role -> {"reachable": bool, "checked_at": float, "error": str|None}
_heard = {}          # ip -> {"last_heard": float, "path": str}
_started = False


def get_config(role):
    """{"host", "port"} for a role; pool roles merge the saved dashboard value."""
    if role == "streaming":
        return dict(config.COMPANION_STREAMING)
    pool_num = 1 if role == "pool1" else 2
    try:
        saved = database.get_companion_configs().get(pool_num, {})
    except Exception:
        saved = {}
    return {"host": saved.get("host"), "port": saved.get("port") or config.COMPANION_DEFAULT_PORT}


def pool_for_ip(ip):
    """Which pool (1/2) Peripherals currently assigns to this Companion IP, or None."""
    for n in (1, 2):
        if get_config(f"pool{n}").get("host") == ip:
            return n
    return None


def update_config(pool_num, host=None, port=None):
    database.save_companion_config(pool_num, host, port)
    # Re-probe right away so the dashboard reflects the new host promptly.
    threading.Thread(target=_probe_role, args=(f"pool{pool_num}",), daemon=True).start()


def record_request(ip, path):
    """Called by routes.py for every /api/companion request."""
    with _lock:
        _heard[ip] = {"last_heard": time.time(), "path": path}


def _probe_role(role):
    cfg = get_config(role)
    if not cfg.get("host"):
        with _lock:
            _probe.pop(role, None)
        return
    url = f"http://{cfg['host']}:{cfg['port']}/"
    try:
        with urllib.request.urlopen(url, timeout=config.COMPANION_PROBE_TIMEOUT_SECONDS):
            pass
        result = {"reachable": True, "error": None}
    except urllib.error.HTTPError:
        # Any HTTP answer at all means Companion's web server is up.
        result = {"reachable": True, "error": None}
    except Exception as e:
        result = {"reachable": False, "error": str(e)}
    result["checked_at"] = time.time()
    with _lock:
        was = _probe.get(role, {}).get("reachable")
        _probe[role] = result
    if was is not None and was != result["reachable"]:
        log.info(f"Companion {role} ({url}) is now {'reachable' if result['reachable'] else 'UNREACHABLE'}")


def _probe_loop():
    while True:
        for role in ROLES:
            _probe_role(role)
        time.sleep(config.COMPANION_PROBE_INTERVAL_SECONDS)


def start():
    global _started
    if _started:
        return
    _started = True
    threading.Thread(target=_probe_loop, daemon=True).start()
    log.info("Companion peer monitor started")


def get_status():
    """Everything the dashboard and checklist need, in one snapshot."""
    now = time.time()
    with _lock:
        probe = {k: dict(v) for k, v in _probe.items()}
        heard = {k: dict(v) for k, v in _heard.items()}

    peers = {}
    assigned_ips = set()
    for role in ROLES:
        cfg = get_config(role)
        host = cfg.get("host")
        if host:
            assigned_ips.add(host)
        h = heard.get(host) if host else None
        p = probe.get(role)
        peers[role] = {
            "host": host,
            "port": cfg.get("port"),
            "configured": bool(host),
            "reachable": p["reachable"] if p else None,
            "checked_ago": round(now - p["checked_at"]) if p else None,
            "error": p["error"] if p else None,
            "last_heard_ago": round(now - h["last_heard"]) if h else None,
            "last_path": h["path"] if h else None,
        }

    unassigned = [
        {"ip": ip, "last_heard_ago": round(now - h["last_heard"]), "last_path": h["path"]}
        for ip, h in sorted(heard.items()) if ip not in assigned_ips
    ]
    return {"peers": peers, "unassigned": unassigned}
