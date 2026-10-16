"""
checklist.py - Pre-session checklist auto-check logic.

Each checklist_items.checker_type maps to a function in CHECKERS below. A
checker takes (params, context) -- params is that item's own checker_params
JSON (parsed), context is live system state the caller (routes.py) gathers --
and returns (ok, detail): ok is True/False/None (None = "no data yet, not a
failure"), detail is a short human-readable status string.

Adding a checklist item that reuses an existing checker_type is a data-only
change (insert a row via database.py). Only a genuinely new *kind* of check
needs a new function + CHECKERS entry here.
"""

import logging
import os
import re
from datetime import datetime

import companion_peers
import config
import database
import dolphin5_control

log = logging.getLogger("cts_tracker")


def _check_path_reachable(params, context):
    path = params.get("path")
    if not path:
        return None, "No path configured"
    ok = os.path.exists(path)
    return ok, (path if ok else f"{path} not reachable")


def _session_label(s):
    """Strip the redundant "Meet Program - " prefix MM's own session text
    carries, matching the same display convention already used client-side
    (routes.py's sessionLabel JS helper)."""
    if not s:
        return s
    stripped = re.sub(r"^Meet Program\s*-?\s*", "", s, flags=re.IGNORECASE).strip()
    return stripped or s


def _time_ago(dt_str):
    """dt_str is a sqlite datetime('now') value, which is UTC -- must compare
    against UTC "now" too, not local time, or the delta comes out wrong
    (negative in timezones behind UTC, permanently showing "just now")."""
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return dt_str
    secs = (datetime.utcnow() - dt).total_seconds()
    if secs < 60:
        return "just now"
    mins = int(secs // 60)
    if mins < 60:
        return f"{mins}m ago"
    hours = mins // 60
    if hours < 24:
        return f"{hours}h {mins % 60}m ago"
    return f"{hours // 24}d ago"


def _check_schedule_imported(params, context):
    meet = context.get("meet")
    if not meet:
        return False, "No active meet"
    rows = database.get_schedule(meet["meet_id"])
    if not rows:
        return False, "No schedule imported yet"
    sessions = [_session_label(s) for s in database.get_sessions(meet["meet_id"])]
    last_imported = max(r["imported_at"] for r in rows if r.get("imported_at"))
    return True, f"{meet['meet_name']}\n{', '.join(sessions)}\nimported {_time_ago(last_imported)}"


def _check_companion_connected(params, context):
    """Green when the streaming Companion and every assigned pool Companion
    answer the reachability probe. An unassigned pool is noted, not failed
    (a one-pool session only needs one)."""
    status = companion_peers.get_status()
    lines, ok = [], True
    for role, name in (("streaming", "Streaming"), ("pool1", "Pool 1"), ("pool2", "Pool 2")):
        p = status["peers"][role]
        if not p["configured"]:
            lines.append(_line(None, f"{name}: not assigned"))
            continue
        if p["reachable"] is None:
            state = "checking..."
            ok = False
        elif p["reachable"]:
            state = "reachable"
        else:
            state = "UNREACHABLE"
            ok = False
        heard = ("never heard this run" if p["last_heard_ago"] is None
                 else f"last heard {_secs_ago(p['last_heard_ago'])}")
        lines.append(_line(p["reachable"], f"{name}: {state} ({p['host']}:{p['port']}), {heard}"))
    for u in status["unassigned"]:
        lines.append(_line(None, f"Unassigned IP {u['ip']}: last heard {_secs_ago(u['last_heard_ago'])}"))
    return ok, "\n".join(lines)


def _line(ok, text):
    """Tag one detail line so the checklist page gives it its own dot:
    True = green, False = red, None = grey."""
    return f"[{'ok' if ok else 'unknown' if ok is None else 'fail'}] {text}"


def _secs_ago(s):
    if s < 60:
        return f"{s}s ago"
    if s < 3600:
        return f"{round(s / 60)} min ago"
    return f"{round(s / 3600)} h ago"


def _latest_client_version():
    """CLIENT_VERSION from this repo's own client.py -- the build that should be
    deployed. Read as text: importing client.py would start its logging."""
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "client.py"),
                  encoding="utf-8") as f:
            m = re.search(r'^CLIENT_VERSION\s*=\s*"([^"]+)"', f.read(), re.M)
        return m.group(1) if m else None
    except OSError:
        return None


def _share_machine_folders():
    """Names of the ip<octet>_<machine> subfolders in WATCH_DIR, or None if
    the share can't be listed."""
    try:
        return sorted(e.name for e in os.scandir(config.WATCH_DIR)
                      if e.is_dir() and e.name.startswith("ip"))
    except OSError:
        return None


def _check_timing_clients(params, context):
    clients = context.get("clients", [])
    if not clients:
        return False, "No clients reporting in"
    latest = _latest_client_version()
    lines = []
    any_online = False
    dolphin5_problem = False
    for c in clients:
        any_online = any_online or c["online"]
        line = f"{c['machine_id']}: {'online' if c['online'] else 'OFFLINE'}, last seen {c['last_seen']}"
        problems = []
        if c["machine_id"].endswith("_DOLPHIN5_AHK"):
            if not c["online"]:
                dolphin5_problem = True
            elif c.get("dolphin5_running") is False:
                dolphin5_problem = True
                problems.append("Dolphin5 closed")
            elif c.get("dolphin5_running"):
                line += " (Dolphin5 open)"
        if c.get("share_ok") is False:
            problems.append("share unreachable")
        # No C:\CTSDolphin is normal on a GEN7 machine -- only a problem
        # where Dolphin5 is open and has nowhere to save.
        if c.get("dolphin_ok") is False and c.get("dolphin5_running"):
            problems.append("no Dolphin folder")
        if c.get("vicreo_running") is False:
            problems.append("Vicreo not running")
        version = c.get("client_version")
        if latest and version != latest:
            problems.append(f"client {version or 'pre-2.0'}, latest is {latest}")
        folder = c.get("share_folder")
        if folder:
            line += f" -> {folder}"
            if folder.startswith("ipunknown_"):
                problems.append("no timing-network IP")
        if problems:
            line += f" ({', '.join(problems)})"
        lines.append(_line(c["online"] and not problems, line))

    # Share folders no online client claims: a renamed machine, a changed IP,
    # or a machine whose client isn't running.
    folders = _share_machine_folders()
    claimed = {c.get("share_folder") for c in clients if c["online"]}
    for name in folders or []:
        if name not in claimed:
            lines.append(_line(None, f"Share folder {name}: no client reporting it"))
    return any_online and not dolphin5_problem, "\n".join(lines)


def _check_session_report_imported(params, context):
    """Optional -- a meet always gets its schedule from the Meet Program,
    but a Session Report may or may not also be ingested. Unlike
    _check_schedule_imported, no Session Report is not a failure (ok=None,
    i.e. "no data yet" rather than red)."""
    meet = context.get("meet")
    if not meet:
        return None, "No active meet"
    rows = database.get_schedule(meet["meet_id"])
    times = [r["session_report_imported_at"] for r in rows if r.get("session_report_imported_at")]
    if not times:
        return None, "No session report ingested (optional)"
    return True, f"Session report last ingested {_time_ago(max(times))}"


def _check_dolphin5_connected(params, context):
    if not dolphin5_control.is_running():
        return False, "TCP control OFF"
    lines = []
    any_connected = False
    for pool in (1, 2):
        cfg = dolphin5_control.get_effective_config(pool)
        if not cfg.get("host"):
            lines.append(_line(None, f"Pool {pool}: not configured"))
            continue
        status = dolphin5_control.get_connection_status(pool)
        connected = bool(status.get("connected"))
        any_connected = any_connected or connected
        state = "connected" if connected else "not connected"
        lines.append(_line(connected, f"Pool {pool}: {state} ({cfg['host']}:{cfg['port']})"))
    return any_connected, "\n".join(lines)


CHECKERS = {
    "path_reachable":      _check_path_reachable,
    "schedule_imported":   _check_schedule_imported,
    "session_report_imported": _check_session_report_imported,
    "companion_connected": _check_companion_connected,
    "timing_clients":      _check_timing_clients,
    "dolphin5_connected":  _check_dolphin5_connected,
}


def evaluate_items(items, context):
    """items: list of checklist_items dicts (checker_params still a JSON string).
    Returns the same list with auto_status ('ok'/'fail'/'unknown'/None-for-manual)
    and auto_detail added to each item.
    """
    import json
    for item in items:
        if item["category"] != "auto" or not item["checker_type"]:
            item["auto_status"] = None
            item["auto_detail"] = None
            continue
        checker = CHECKERS.get(item["checker_type"])
        if not checker:
            item["auto_status"] = "unknown"
            item["auto_detail"] = f"Unknown checker_type: {item['checker_type']}"
            continue
        try:
            params = json.loads(item["checker_params"]) if item["checker_params"] else {}
            ok, detail = checker(params, context)
        except Exception as e:
            log.error(f"Checklist checker '{item['checker_type']}' failed: {e}")
            ok, detail = None, f"Check failed: {e}"
        item["auto_status"] = "ok" if ok else ("unknown" if ok is None else "fail")
        item["auto_detail"] = detail
    return items
