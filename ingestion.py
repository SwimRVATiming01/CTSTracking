"""
ingestion.py - Meet Manager CSV import, CTS/Dolphin ingestion pipeline,
               CTS<->Dolphin correlation, and schedule approval workflow.
"""

import csv
import json
import logging
import os
import re
import shutil
import threading
import time
from datetime import datetime, timedelta

import config
from database import (
    get_active_meet, get_conn, get_write_conn, _log_ingestion,
    wipe_database, create_meet, export_race_log_csv, snapshot_db,
)
from parsers import parse_cts_filename, parse_dolphin_filename, parse_cts_file

log = logging.getLogger("cts_tracker")


# ===========================================================================
# MEET MANAGER CSV PARSER
# ===========================================================================

def _parse_time_to_24h(time_str):
    time_str = time_str.strip()
    for fmt in ("%I:%M %p", "%I:%M%p"):
        try:
            return datetime.strptime(time_str, fmt).strftime("%H:%M")
        except ValueError:
            continue
    return None


def _parse_event_col(event_col):
    m = re.match(r"^#(\w+)\s+(.*)", event_col.strip())
    if m:
        return m.group(1), m.group(2).strip()
    return "", event_col.strip()


def _parse_heat_col(heat_col):
    result = {"heat_num": None, "heat_label": None, "heat_type": None, "start_time": None}
    heat_col = heat_col.strip()

    m = re.match(r"Heat\s+(\d+)", heat_col, re.IGNORECASE)
    if m:
        result["heat_num"] = m.group(1)

    m_time = re.search(r"Starts at\s+(\d{1,2}:\d{2}\s*[AP]M)", heat_col, re.IGNORECASE)
    if m_time:
        result["start_time"] = _parse_time_to_24h(m_time.group(1))

    if re.search(r"\bFinals?\b", heat_col, re.IGNORECASE):
        result["heat_type"] = "Finals"
        m_label = re.search(
            r"Heat\s+\d+\s+(?:of\s+\d+\s+)?(.*?)\s+(?:Final|Starts at)",
            heat_col, re.IGNORECASE
        )
        if m_label:
            result["heat_label"] = m_label.group(1).strip(" -")
    elif re.search(r"\bPrelims?\b", heat_col, re.IGNORECASE):
        result["heat_type"] = "Prelims"
        result["heat_label"] = "Prelims"

    return result


def import_schedule(filepath, meet_id, session_override=None):
    """
    Parse a Meet Manager 8.0 heat sheet CSV and load it into the schedule table.
    Re-import safe — existing manual overrides are always preserved.
    Returns stats dict.
    """
    stats = {"inserted": 0, "updated": 0, "skipped": 0, "errors": [], "session": None, "meet_name": None}
    heats = {}

    try:
        with open(filepath, newline="", encoding="utf-8-sig") as f:
            for line_num, row in enumerate(csv.reader(f), start=1):
                while len(row) <= max(
                    config.MM_COL_SESSION, config.MM_COL_EVENT_FULL,
                    config.MM_COL_HEAT_INFO, config.MM_COL_LANE
                ):
                    row.append("")

                session_raw = row[config.MM_COL_SESSION].strip()
                event_raw   = row[config.MM_COL_EVENT_FULL].strip()
                heat_raw    = row[config.MM_COL_HEAT_INFO].strip()
                meet_raw    = row[config.MM_COL_MEET_NAME].strip()

                if not event_raw.startswith("#") or not heat_raw.lower().startswith("heat"):
                    continue

                session = session_override or session_raw
                if not stats["session"]:
                    stats["session"] = session
                if not stats["meet_name"]:
                    stats["meet_name"] = meet_raw

                event_id, event_name = _parse_event_col(event_raw)
                if not event_id:
                    stats["errors"].append(f"Line {line_num}: bad event col: {repr(event_raw)}")
                    continue

                parsed = _parse_heat_col(heat_raw)
                if not parsed["heat_num"]:
                    continue

                key = (session, event_id, parsed["heat_num"])
                if key not in heats:
                    heats[key] = {
                        "session":    session,
                        "event_id":   event_id,
                        "event_name": event_name,
                        "heat":       parsed["heat_num"],
                        "heat_label": parsed["heat_label"],
                        "heat_type":  parsed["heat_type"],
                        "start_time": parsed["start_time"],
                    }
                elif heats[key]["start_time"] is None and parsed["start_time"] is not None:
                    heats[key]["start_time"] = parsed["start_time"]

    except Exception as e:
        stats["errors"].append(f"File read error: {e}")
        log.error(f"Schedule import error: {e}")
        return stats

    if not heats:
        stats["errors"].append("No valid heat data found.")
        return stats

    def sort_key(item):
        h = item[1]
        t = h["start_time"] or "99:99"
        try:   ev = int(h["event_id"])
        except ValueError: ev = 9999
        try:   ht = int(h["heat"])
        except ValueError: ht = 9999
        return (t, ev, ht)

    ordered = sorted(heats.items(), key=sort_key)

    with get_write_conn() as conn:
        for order, (key, h) in enumerate(ordered, start=1):
            session, event_id, heat_num = key
            existing = conn.execute(
                "SELECT id, override_start FROM schedule WHERE meet_id=? AND session=? AND event_id=? AND heat=?",
                (meet_id, session, event_id, heat_num)
            ).fetchone()

            if existing is None:
                conn.execute(
                    """INSERT INTO schedule
                       (meet_id,session,event_id,event_name,heat,heat_label,heat_type,projected_start,heat_order)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (meet_id, h["session"], h["event_id"], h["event_name"],
                     h["heat"], h["heat_label"], h["heat_type"], h["start_time"], order)
                )
                stats["inserted"] += 1
            else:
                conn.execute(
                    """UPDATE schedule SET event_name=?,heat_label=?,heat_type=?,
                       projected_start=?,heat_order=?,imported_at=datetime('now')
                       WHERE id=?""",
                    (h["event_name"], h["heat_label"], h["heat_type"],
                     h["start_time"], order, existing["id"])
                )
                if existing["override_start"]:
                    stats["skipped"] += 1
                else:
                    stats["updated"] += 1

    log.info(
        f"Schedule imported: meet={meet_id} session={stats['session']} — "
        f"{stats['inserted']} inserted, {stats['updated']} updated, "
        f"{stats['skipped']} overrides preserved, {len(stats['errors'])} errors"
    )
    _log_ingestion(os.path.basename(filepath), "schedule", None, None, "imported")
    return stats


def _extract_meet_info_from_csv(filepath):
    """
    Parse meet name, date and session from MM CSV without fully importing it.
    Returns dict with meet_name, meet_date, session.
    """
    info = {"meet_name": None, "meet_date": None, "session": None}
    try:
        with open(filepath, newline="", encoding="utf-8-sig") as f:
            for row in csv.reader(f):
                while len(row) <= max(config.MM_COL_MEET_NAME, config.MM_COL_EXPORT_INFO, config.MM_COL_SESSION):
                    row.append("")
                meet_raw    = row[config.MM_COL_MEET_NAME].strip()
                export_raw  = row[config.MM_COL_EXPORT_INFO].strip()
                session_raw = row[config.MM_COL_SESSION].strip()
                if meet_raw:
                    info["meet_name"] = meet_raw
                if session_raw:
                    info["session"] = session_raw
                m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", export_raw)
                if m:
                    try:
                        info["meet_date"] = datetime.strptime(m.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
                    except ValueError:
                        pass
                if all(info.values()):
                    break
    except Exception as e:
        log.warning(f"Could not extract meet info from CSV: {e}")
    return info


# ===========================================================================
# RAW FILE BACKUP
# ===========================================================================

def _backup_raw_file(src_path, file_type, retries=5, delay=0.5):
    """
    Copy incoming file to backups dir with timestamp prefix.
    Retries if the file is still locked by another process.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dest = os.path.join(config.BACKUP_DIR, f"{timestamp}_{file_type}_{os.path.basename(src_path)}")
    for attempt in range(1, retries + 1):
        try:
            shutil.copy2(src_path, dest)
            log.debug(f"Backed up: {dest}")
            return dest
        except PermissionError:
            if attempt < retries:
                log.warning(f"File locked, retrying in {delay}s ({attempt}/{retries}): {os.path.basename(src_path)}")
                time.sleep(delay)
            else:
                log.error(f"Could not back up {os.path.basename(src_path)} after {retries} attempts — file still locked")
                raise


# ===========================================================================
# INGESTION PIPELINE
# ===========================================================================

def ingest_cts_file(filepath):
    """Full CTS ingestion pipeline."""
    filename = os.path.basename(filepath)
    _backup_raw_file(filepath, "cts")
    fn = parse_cts_filename(filename)
    cts_data = parse_cts_file(filepath)

    if cts_data is None:
        msg = "Failed to parse CTS file — check log for details"
        _log_ingestion(filename, "cts", fn.get("machine_id"), fn.get("file_time"), "error", msg)
        return {"status": "error", "message": msg}

    active = get_active_meet()
    if not active:
        _add_pending_cts(cts_data, fn, filename)
        _log_ingestion(filename, "cts", fn.get("machine_id"), fn.get("file_time"), "pending", "No active meet")
        return {"status": "pending", "message": "No active meet"}

    race_id = _write_race_log_from_cts(cts_data, fn, active["meet_id"], filename)
    matched = _attempt_dolphin_correlation(race_id, fn.get("file_time"))
    status = "matched" if matched else "pending"
    _log_ingestion(filename, "cts", fn.get("machine_id"), fn.get("file_time"), status)
    snapshot_db("ingest")
    return {"status": status, "race_log_id": race_id, "dolphin_matched": matched}


def ingest_dolphin_file(filepath):
    """Full Dolphin ingestion pipeline."""
    filename = os.path.basename(filepath)
    _backup_raw_file(filepath, "dolphin")
    fn = parse_dolphin_filename(filename)

    if fn["dolphin_race_num"] is None:
        msg = "Could not extract race number"
        _log_ingestion(filename, "dolphin", fn.get("machine_id"), fn.get("file_time"), "error", msg)
        return {"status": "error", "message": msg}

    matched_id = None
    if fn["file_time"]:
        matched_id = _match_dolphin_to_cts(
            fn["dolphin_race_num"], fn["machine_id"], fn["file_time"], filename
        )

    if matched_id:
        _log_ingestion(filename, "dolphin", fn.get("machine_id"), fn.get("file_time"), "matched")
        snapshot_db("ingest")
        return {"status": "matched", "race_log_id": matched_id}
    else:
        _add_pending_dolphin(fn, filename)
        _log_ingestion(filename, "dolphin", fn.get("machine_id"), fn.get("file_time"), "pending", "No CTS match found")
        snapshot_db("ingest")
        return {"status": "pending", "message": "Saved to pending"}


# ===========================================================================
# SCHEDULE APPROVAL WORKFLOW
# ===========================================================================

# Holds a pending schedule file waiting for operator approval
_pending_schedule = {}
_pending_schedule_lock = threading.Lock()


def queue_schedule_for_approval(filepath):
    """
    Called by the watchdog when a CSV lands in SCHEDULE_DIR.
    Stores the pending file info and waits for operator approval via the dashboard.
    """
    info = _extract_meet_info_from_csv(filepath)
    with _pending_schedule_lock:
        _pending_schedule.clear()
        _pending_schedule.update({
            "filepath":    filepath,
            "filename":    os.path.basename(filepath),
            "meet_name":   info["meet_name"],
            "meet_date":   info["meet_date"],
            "session":     info["session"],
            "detected_at": datetime.now().isoformat(),
        })
    log.info(f"Schedule queued for approval: {os.path.basename(filepath)} ({info['meet_name']})")


def get_pending_schedule():
    """Return the pending schedule info dict, or None."""
    with _pending_schedule_lock:
        return dict(_pending_schedule) if _pending_schedule else None


def dismiss_pending_schedule():
    """Clear the pending schedule without importing."""
    with _pending_schedule_lock:
        _pending_schedule.clear()


def approve_schedule(scrub_races=True):
    """
    Called when operator approves a pending schedule via the dashboard.

    scrub_races=True  -> Full wipe then import (new meet, clean slate)
    scrub_races=False -> Keep race data, wipe and reimport schedule only
    """
    with _pending_schedule_lock:
        if not _pending_schedule:
            return {"status": "error", "message": "No pending schedule"}
        pending = dict(_pending_schedule)

    filepath  = pending["filepath"]
    meet_name = pending["meet_name"] or "Unknown Meet"
    meet_date = pending["meet_date"]
    meet_id   = meet_date or datetime.now().strftime("%Y-%m-%d")

    # Pre-action exports — always, before any changes
    active = get_active_meet()
    if active:
        log.info("Pre-import: exporting race log CSV...")
        export_race_log_csv(active["meet_id"])
        log.info("Pre-import: taking DB snapshot...")
        snapshot_db("pre_import")

    _backup_raw_file(filepath, "schedule")

    if scrub_races:
        wipe_database()
    else:
        with get_write_conn() as conn:
            conn.execute("DELETE FROM schedule")
            conn.execute("DELETE FROM meets")

    create_meet(meet_id, meet_name, meet_date, set_active=True)
    result = import_schedule(filepath, meet_id)

    with _pending_schedule_lock:
        _pending_schedule.clear()

    snapshot_db("ingest")
    log.info(f"Schedule imported: meet={meet_id} meet_name={meet_name} scrub={scrub_races}")
    return {"status": "imported", "meet_id": meet_id, "scrubbed": scrub_races, **result}


def ingest_schedule_file(filepath, meet_id=None, session_override=None):
    """
    Called by watchdog for CSV files in SCHEDULE_DIR.
    Queues the file for operator approval via the dashboard modal.
    Direct import (bypassing modal) still available via meet_id parameter.
    """
    if meet_id:
        _backup_raw_file(filepath, "schedule")
        result = import_schedule(filepath, meet_id, session_override)
        snapshot_db("ingest")
        return {"status": "imported", "meet_id": meet_id, **result}

    queue_schedule_for_approval(filepath)
    return {"status": "pending_approval", "filename": os.path.basename(filepath)}


# ===========================================================================
# CORRELATION HELPERS
# ===========================================================================

def _write_race_log_from_cts(cts_data, fn, meet_id, filename):
    ft = fn["file_time"].isoformat() if fn["file_time"] else None
    active_lanes_str = ",".join(str(l) for l in cts_data.get("active_lanes") or [])
    off_times_str = json.dumps(cts_data.get("off_times") or [])
    with get_write_conn() as conn:
        cur = conn.execute(
            """INSERT INTO race_log
               (meet_id,event_id,heat,cts_race_num,cts_start_time,
                cts_file_time,cts_source_machine,cts_filename,
                active_lanes,missing_lanes,off_times)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (meet_id, cts_data.get("event_id"), cts_data.get("heat"),
             cts_data.get("cts_race_num"), cts_data.get("cts_start_time"),
             ft, fn.get("machine_id"), filename,
             active_lanes_str,
             cts_data.get("missing_lanes_str", ""),
             off_times_str)
        )
        return cur.lastrowid


def _add_pending_cts(cts_data, fn, filename):
    ft = fn["file_time"].isoformat() if fn["file_time"] else None
    with get_write_conn() as conn:
        conn.execute(
            """INSERT INTO pending_cts
               (cts_race_num,event_id,heat,cts_start_time,file_time,source_machine,filename,raw_data)
               VALUES (?,?,?,?,?,?,?,?)""",
            (cts_data.get("cts_race_num"), cts_data.get("event_id"), cts_data.get("heat"),
             cts_data.get("cts_start_time"), ft, fn.get("machine_id"), filename, json.dumps(cts_data))
        )


def _add_pending_dolphin(fn, filename):
    ft = fn["file_time"].isoformat() if fn["file_time"] else None
    with get_write_conn() as conn:
        conn.execute(
            "INSERT INTO pending_dolphin (dolphin_race_num,file_time,source_machine,filename) VALUES (?,?,?,?)",
            (fn["dolphin_race_num"], ft, fn.get("machine_id"), filename)
        )


def _match_dolphin_to_cts(dolphin_race_num, machine_id, file_time, filename):
    """Find closest unmatched CTS race_log entry within the time window."""
    window = timedelta(seconds=config.DOLPHIN_MATCH_WINDOW_SECONDS)
    low  = (file_time - window).isoformat()
    high = (file_time + window).isoformat()

    with get_conn() as conn:
        row = conn.execute(
            """SELECT id, cts_file_time FROM race_log
               WHERE matched=0 AND cts_file_time BETWEEN ? AND ?
               ORDER BY ABS(julianday(cts_file_time)-julianday(?)) LIMIT 1""",
            (low, high, file_time.isoformat())
        ).fetchone()

    if not row:
        return None

    delta = abs((file_time - datetime.fromisoformat(row["cts_file_time"])).total_seconds())
    ft = file_time.isoformat()

    with get_write_conn() as conn:
        conn.execute(
            """UPDATE race_log SET dolphin_race_num=?,dolphin_file_time=?,
               dolphin_source_machine=?,dolphin_filename=?,match_delta_sec=?,matched=1
               WHERE id=?""",
            (dolphin_race_num, ft, machine_id, filename, delta, row["id"])
        )
    log.info(f"Dolphin #{dolphin_race_num} matched to race_log id={row['id']} (Δ{delta:.1f}s)")
    return row["id"]


def _attempt_dolphin_correlation(race_log_id, cts_file_time):
    """After CTS arrives, check pending_dolphin for a waiting match."""
    if not cts_file_time:
        return False

    window = timedelta(seconds=config.DOLPHIN_MATCH_WINDOW_SECONDS)
    low  = (cts_file_time - window).isoformat()
    high = (cts_file_time + window).isoformat()

    with get_conn() as conn:
        pending = conn.execute(
            """SELECT * FROM pending_dolphin
               WHERE file_time BETWEEN ? AND ?
               ORDER BY ABS(julianday(file_time)-julianday(?)) LIMIT 1""",
            (low, high, cts_file_time.isoformat())
        ).fetchone()

    if not pending:
        return False

    delta = abs((cts_file_time - datetime.fromisoformat(pending["file_time"])).total_seconds())

    with get_write_conn() as conn:
        conn.execute(
            """UPDATE race_log SET dolphin_race_num=?,dolphin_file_time=?,
               dolphin_source_machine=?,dolphin_filename=?,match_delta_sec=?,matched=1
               WHERE id=?""",
            (pending["dolphin_race_num"], pending["file_time"],
             pending["source_machine"], pending["filename"], delta, race_log_id)
        )
        conn.execute("DELETE FROM pending_dolphin WHERE id=?", (pending["id"],))

    log.info(f"Retroactive match: pending Dolphin #{pending['dolphin_race_num']} -> race_log id={race_log_id} (Δ{delta:.1f}s)")
    return True
