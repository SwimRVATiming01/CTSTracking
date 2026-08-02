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
    ok = bool(context.get("companion_connected"))
    return ok, ("Connected" if ok else "No Companion contact yet")


def _check_timing_clients(params, context):
    clients = context.get("clients", [])
    if not clients:
        return False, "No clients reporting in"
    lines = []
    any_online = False
    for c in clients:
        any_online = any_online or c["online"]
        line = f"{c['machine_id']}: {'online' if c['online'] else 'OFFLINE'}, last seen {c['last_seen']}"
        problems = []
        if c.get("share_ok") is False:
            problems.append("share unreachable")
        if c.get("dolphin_ok") is False:
            problems.append("no Dolphin folder")
        if c.get("cts_ok") is False:
            problems.append("no CTS folder")
        if c.get("vicreo_running") is False:
            problems.append("Vicreo not running")
        if problems:
            line += f" ({', '.join(problems)})"
        lines.append(line)
    return any_online, "\n".join(lines)


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
        return False, "TCP control not started"
    lines = []
    any_connected = False
    for pool in (1, 2):
        cfg = dolphin5_control.get_effective_config(pool)
        if not cfg.get("host"):
            lines.append(f"Pool {pool}: not configured")
            continue
        status = dolphin5_control.get_connection_status(pool)
        connected = bool(status.get("connected"))
        any_connected = any_connected or connected
        state = "connected" if connected else "not connected"
        lines.append(f"Pool {pool}: {state} ({cfg['host']}:{cfg['port']})")
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
