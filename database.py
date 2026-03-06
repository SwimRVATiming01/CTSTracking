"""
database.py - Database connection, schema, and all query/write functions.
"""

import csv
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime

import config

log = logging.getLogger("cts_tracker")

# ===========================================================================
# CONNECTION MANAGEMENT
# ===========================================================================

_write_lock = threading.Lock()


@contextmanager
def get_conn():
    """Yield a read-only connection. WAL mode allows unlimited concurrent readers."""
    conn = sqlite3.connect(config.DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_write_conn():
    """Yield an exclusive write connection protected by a thread lock."""
    with _write_lock:
        conn = sqlite3.connect(config.DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


# ===========================================================================
# SCHEMA
# ===========================================================================

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meets (
    meet_id     TEXT PRIMARY KEY,
    meet_name   TEXT NOT NULL,
    meet_date   TEXT,
    location    TEXT,
    active      INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS schedule (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    meet_id          TEXT NOT NULL REFERENCES meets(meet_id),
    session          TEXT NOT NULL,
    event_id         TEXT NOT NULL,
    event_name       TEXT NOT NULL,
    heat             TEXT NOT NULL,
    heat_label       TEXT,
    heat_type        TEXT,
    projected_start  TEXT,
    heat_order       INTEGER,
    override_start   TEXT,
    imported_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(meet_id, session, event_id, heat)
);

CREATE TABLE IF NOT EXISTS race_log (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    meet_id                 TEXT NOT NULL REFERENCES meets(meet_id),
    session                 TEXT,
    event_id                TEXT,
    heat                    TEXT,
    cts_race_num            INTEGER,
    cts_start_time          TEXT,
    cts_file_time           TEXT,
    cts_source_machine      TEXT,
    cts_filename            TEXT,
    dolphin_race_num        INTEGER,
    dolphin_file_time       TEXT,
    dolphin_source_machine  TEXT,
    dolphin_filename        TEXT,
    match_delta_sec         REAL,
    matched                 INTEGER NOT NULL DEFAULT 0,
    manually_edited         INTEGER NOT NULL DEFAULT 0,
    active_lanes            TEXT,
    missing_lanes           TEXT,
    off_times               TEXT,
    ingested_at             TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pending_dolphin (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    dolphin_race_num  INTEGER NOT NULL,
    file_time         TEXT NOT NULL,
    source_machine    TEXT NOT NULL,
    filename          TEXT NOT NULL,
    arrived_at        TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pending_cts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    cts_race_num    INTEGER,
    event_id        TEXT,
    heat            TEXT,
    cts_start_time  TEXT,
    file_time       TEXT,
    source_machine  TEXT,
    filename        TEXT,
    raw_data        TEXT,
    arrived_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    filename        TEXT NOT NULL,
    file_type       TEXT NOT NULL,
    source_machine  TEXT,
    file_time       TEXT,
    status          TEXT NOT NULL,
    error_message   TEXT,
    ingested_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS snapshots (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_file  TEXT NOT NULL,
    trigger        TEXT NOT NULL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def init_db():
    """Create all tables if they don't exist. Safe to call on every startup."""
    log.info(f"Initializing database at {config.DB_PATH}")
    with get_write_conn() as conn:
        conn.executescript(SCHEMA)
    log.info("Database ready.")


# ===========================================================================
# MEET MANAGEMENT
# ===========================================================================

def create_meet(meet_id, meet_name, meet_date=None, location=None, set_active=True):
    try:
        with get_write_conn() as conn:
            if set_active:
                conn.execute("UPDATE meets SET active=0")
            conn.execute(
                "INSERT INTO meets (meet_id, meet_name, meet_date, location, active) VALUES (?,?,?,?,?)",
                (meet_id, meet_name, meet_date, location, 1 if set_active else 0)
            )
        log.info(f"Meet created: {meet_id} ({meet_name})")
        return True
    except sqlite3.IntegrityError:
        log.warning(f"Meet already exists: {meet_id}")
        return False


def get_active_meet():
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM meets WHERE active=1 ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def get_all_meets():
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM meets ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]


def set_active_meet(meet_id):
    with get_write_conn() as conn:
        conn.execute("UPDATE meets SET active=0")
        result = conn.execute("UPDATE meets SET active=1 WHERE meet_id=?", (meet_id,))
    return result.rowcount > 0


# ===========================================================================
# SCHEDULE
# ===========================================================================

def get_schedule(meet_id, session=None):
    query = """
        SELECT *, COALESCE(override_start, projected_start) AS effective_start
        FROM schedule WHERE meet_id=?
    """
    params = [meet_id]
    if session:
        query += " AND session=?"
        params.append(session)
    query += " ORDER BY heat_order ASC"
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_sessions(meet_id):
    """Return list of distinct session names for a meet, ordered by first appearance."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT session FROM schedule WHERE meet_id=? GROUP BY session ORDER BY MIN(heat_order)",
            (meet_id,)
        ).fetchall()
    return [r["session"] for r in rows]


def override_start_time(meet_id, session, event_id, heat, new_time):
    """Manually set start time for a heat. Survives re-imports."""
    with get_write_conn() as conn:
        result = conn.execute(
            "UPDATE schedule SET override_start=? WHERE meet_id=? AND session=? AND event_id=? AND heat=?",
            (new_time, meet_id, session, event_id, heat)
        )
    return result.rowcount > 0


def clear_override(meet_id, session, event_id, heat):
    with get_write_conn() as conn:
        result = conn.execute(
            "UPDATE schedule SET override_start=NULL WHERE meet_id=? AND session=? AND event_id=? AND heat=?",
            (meet_id, session, event_id, heat)
        )
    return result.rowcount > 0


def reorder_heats(meet_id, session, ordered_ids):
    """Reorder heats by providing schedule row IDs in desired order."""
    with get_write_conn() as conn:
        for new_order, row_id in enumerate(ordered_ids, start=1):
            conn.execute(
                "UPDATE schedule SET heat_order=? WHERE id=? AND meet_id=? AND session=?",
                (new_order, row_id, meet_id, session)
            )
    return True


def add_manual_heat(meet_id, session, event_id, event_name, heat,
                    projected_start=None, heat_label=None, heat_type=None):
    """Manually add a heat to the schedule."""
    with get_conn() as conn:
        max_order = conn.execute(
            "SELECT MAX(heat_order) FROM schedule WHERE meet_id=? AND session=?",
            (meet_id, session)
        ).fetchone()[0] or 0

    with get_write_conn() as conn:
        conn.execute(
            """INSERT INTO schedule
               (meet_id, session, event_id, event_name, heat, heat_label,
                heat_type, projected_start, heat_order)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (meet_id, session, event_id, event_name, heat,
             heat_label, heat_type, projected_start, max_order + 1)
        )
    log.info(f"Manual heat added: meet={meet_id} event={event_id} heat={heat}")
    return True


# ===========================================================================
# RACE LOG
# ===========================================================================

# Pool split threshold — CTS race numbers below this are Pool 1, at or above are Pool 2
POOL2_THRESHOLD = 2000


def get_race_dashboard(meet_id, session=None):
    """
    Main dashboard query. Schedule rows joined with race_log data.

    - Only the highest CTS race number per event/heat is joined (last run wins)
    - Adds pool assignment (1 or 2) based on CTS race number vs POOL2_THRESHOLD
    - Adds current_heat flag per pool (row with highest CTS num in that pool)
    - Adds cts_gap_flag and dolphin_gap_flag for sequence break detection
    """
    query = """
        SELECT
            s.id AS schedule_id,
            s.event_id,
            s.event_name,
            s.heat,
            s.heat_label,
            s.heat_type,
            s.session,
            s.heat_order,
            COALESCE(s.override_start, s.projected_start) AS effective_start,
            s.projected_start,
            s.override_start,
            s.override_start IS NOT NULL AS has_override,
            r.id AS race_log_id,
            r.cts_race_num,
            r.cts_start_time,
            r.cts_file_time,
            r.cts_source_machine,
            r.dolphin_race_num,
            r.dolphin_file_time,
            r.dolphin_source_machine,
            r.match_delta_sec,
            r.matched,
            r.manually_edited,
            r.active_lanes,
            r.missing_lanes,
            r.ingested_at,
            CASE
                WHEN r.cts_start_time IS NOT NULL
                 AND COALESCE(s.override_start, s.projected_start) IS NOT NULL
                THEN ROUND(
                    (strftime('%s','1970-01-01 '||r.cts_start_time) -
                     strftime('%s','1970-01-01 '||COALESCE(s.override_start,s.projected_start))
                    ) / 60.0, 2)
                ELSE NULL
            END AS delta_minutes
        FROM schedule s
        LEFT JOIN race_log r
            ON r.meet_id = s.meet_id
            AND r.event_id = s.event_id
            AND r.heat = s.heat
            AND r.cts_race_num = (
                SELECT MAX(cts_race_num) FROM race_log
                WHERE meet_id = s.meet_id
                AND event_id = s.event_id
                AND heat = s.heat
            )
        WHERE s.meet_id=?
    """
    params = [meet_id]
    if session:
        query += " AND s.session=?"
        params.append(session)
    query += " ORDER BY s.heat_order ASC"

    with get_conn() as conn:
        rows = [dict(r) for r in conn.execute(query, params).fetchall()]

    if not rows:
        return rows

    # Pool assignment
    for row in rows:
        n = row.get("cts_race_num")
        if n is None:
            row["pool"] = None
        elif n < POOL2_THRESHOLD:
            row["pool"] = 1
        else:
            row["pool"] = 2

    # Current heat per pool (row with highest CTS race number in each pool)
    p1_max = max((r["cts_race_num"] for r in rows if r["pool"] == 1), default=None)
    p2_max = max((r["cts_race_num"] for r in rows if r["pool"] == 2), default=None)
    for row in rows:
        n = row.get("cts_race_num")
        row["is_current_p1"] = (n is not None and row["pool"] == 1 and n == p1_max)
        row["is_current_p2"] = (n is not None and row["pool"] == 2 and n == p2_max)

    # Sequence gap flagging per pool
    for pool_num, threshold_check in [(1, lambda n: n < POOL2_THRESHOLD),
                                       (2, lambda n: n >= POOL2_THRESHOLD)]:
        pool_rows = [r for r in rows if r.get("cts_race_num") is not None
                     and threshold_check(r["cts_race_num"])]
        pool_rows_sorted = sorted(pool_rows, key=lambda r: r["heat_order"])

        flagged_ids = set()
        for i in range(1, len(pool_rows_sorted)):
            prev = pool_rows_sorted[i-1]["cts_race_num"]
            curr = pool_rows_sorted[i]["cts_race_num"]
            if curr != prev + 1:
                flagged_ids.add(pool_rows_sorted[i-1]["schedule_id"])
                flagged_ids.add(pool_rows_sorted[i]["schedule_id"])

        for row in rows:
            if "cts_gap_flag" not in row:
                row["cts_gap_flag"] = row["schedule_id"] in flagged_ids
            else:
                row["cts_gap_flag"] = row["cts_gap_flag"] or (row["schedule_id"] in flagged_ids)

    # Dolphin gap flagging (single sequence, no pool split)
    dolphin_rows = [r for r in rows if r.get("dolphin_race_num") is not None]
    dolphin_sorted = sorted(dolphin_rows, key=lambda r: r["heat_order"])
    dolphin_flagged = set()
    for i in range(1, len(dolphin_sorted)):
        prev = dolphin_sorted[i-1]["dolphin_race_num"]
        curr = dolphin_sorted[i]["dolphin_race_num"]
        if curr != prev + 1:
            dolphin_flagged.add(dolphin_sorted[i-1]["schedule_id"])
            dolphin_flagged.add(dolphin_sorted[i]["schedule_id"])
    for row in rows:
        row["dolphin_gap_flag"] = row["schedule_id"] in dolphin_flagged
        if "cts_gap_flag" not in row:
            row["cts_gap_flag"] = False

    return rows


def get_full_log(meet_id):
    """
    Return raw race_log for a meet, all entries unfiltered.
    Includes orphan flag (no matching schedule row).
    """
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT r.*,
                      CASE WHEN s.id IS NULL THEN 1 ELSE 0 END AS is_orphan
               FROM race_log r
               LEFT JOIN schedule s
                   ON s.meet_id = r.meet_id
                   AND s.event_id = r.event_id
                   AND s.heat = r.heat
               WHERE r.meet_id=?
               ORDER BY r.ingested_at ASC""",
            (meet_id,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_current_heat_state(meet_id):
    """
    Return current heat and next-heat state for each pool.
    Used by Bitfocus Companion endpoints.
    """
    rows = get_race_dashboard(meet_id)
    result = {}

    for pool_num, is_current_key in [(1, "is_current_p1"), (2, "is_current_p2")]:
        current_row = next((r for r in rows if r.get(is_current_key)), None)
        if not current_row:
            result[f"pool{pool_num}"] = {
                "active": False,
                "current_event": None,
                "current_heat": None,
                "next_event": None,
                "next_heat": None,
                "next_is_new_event": None,
            }
            continue

        current_order = current_row["heat_order"]
        next_row = next(
            (r for r in sorted(rows, key=lambda x: x["heat_order"])
             if r["heat_order"] > current_order),
            None
        )

        result[f"pool{pool_num}"] = {
            "active": True,
            "current_event": current_row["event_id"],
            "current_heat": current_row["heat"],
            "current_event_name": current_row["event_name"],
            "cts_race_num": current_row["cts_race_num"],
            "next_event": next_row["event_id"] if next_row else None,
            "next_heat": next_row["heat"] if next_row else None,
            "next_is_new_event": (
                next_row["event_id"] != current_row["event_id"]
                if next_row else None
            ),
        }

    return result


def add_manual_race_entry(meet_id, event_id, heat, cts_race_num=None,
                           cts_start_time=None, dolphin_race_num=None):
    with get_write_conn() as conn:
        cursor = conn.execute(
            """INSERT INTO race_log
               (meet_id, event_id, heat, cts_race_num, cts_start_time,
                dolphin_race_num, manually_edited)
               VALUES (?,?,?,?,?,?,1)""",
            (meet_id, event_id, heat, cts_race_num, cts_start_time, dolphin_race_num)
        )
    log.info(f"Manual race entry: meet={meet_id} event={event_id} heat={heat}")
    return cursor.lastrowid


def update_race_entry(race_log_id, **fields):
    allowed = {"cts_race_num", "cts_start_time", "dolphin_race_num", "matched"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return False
    updates["manually_edited"] = 1
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [race_log_id]
    with get_write_conn() as conn:
        result = conn.execute(f"UPDATE race_log SET {set_clause} WHERE id=?", values)
    return result.rowcount > 0


def get_pending_summary():
    with get_conn() as conn:
        pd = conn.execute("SELECT COUNT(*) FROM pending_dolphin").fetchone()[0]
        pc = conn.execute("SELECT COUNT(*) FROM pending_cts").fetchone()[0]
        um = conn.execute("SELECT COUNT(*) FROM race_log WHERE matched=0").fetchone()[0]
    return {"pending_dolphin": pd, "pending_cts": pc, "unmatched_log": um}


def get_ingestion_log(limit=100):
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM ingestion_log ORDER BY ingested_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def _log_ingestion(filename, file_type, machine_id, file_time, status, error_message=None):
    ft = file_time.isoformat() if isinstance(file_time, datetime) else file_time
    with get_write_conn() as conn:
        conn.execute(
            "INSERT INTO ingestion_log (filename,file_type,source_machine,file_time,status,error_message) VALUES (?,?,?,?,?,?)",
            (filename, file_type, machine_id, ft, status, error_message)
        )


def export_race_log_csv(meet_id):
    """Export current race_log to a timestamped CSV in BACKUP_DIR. Returns path."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    export_path = os.path.join(config.BACKUP_DIR, f"{timestamp}_race_log_export_{meet_id}.csv")
    rows = get_full_log(meet_id) if meet_id else []
    if rows:
        with open(export_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        log.info(f"Race log exported: {export_path}")
    return export_path


def wipe_database():
    """
    Wipe all operational data and re-initialize schema.
    Leaves the DB file intact but removes all meets, schedule, race data, and pending entries.
    Backup files on disk are never touched.
    """
    with get_write_conn() as conn:
        conn.execute("DELETE FROM race_log")
        conn.execute("DELETE FROM schedule")
        conn.execute("DELETE FROM meets")
        conn.execute("DELETE FROM pending_dolphin")
        conn.execute("DELETE FROM pending_cts")
    log.info("Database wiped — ready for new meet")


# ===========================================================================
# BACKUP & SNAPSHOT
# ===========================================================================

def snapshot_db(trigger="scheduled"):
    """Live SQLite backup using the backup API. Safe during active writes."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    snap_file = f"cts_tracker_{timestamp}.db"
    snap_path = os.path.join(config.SNAPSHOT_DIR, snap_file)
    try:
        src = sqlite3.connect(config.DB_PATH)
        dst = sqlite3.connect(snap_path)
        src.backup(dst)
        dst.close()
        src.close()
        with get_write_conn() as conn:
            conn.execute("INSERT INTO snapshots (snapshot_file,trigger) VALUES (?,?)", (snap_file, trigger))
        if config.SNAPSHOT_KEEP_COUNT > 0:
            _rotate_snapshots()
        log.info(f"Snapshot saved: {snap_path} (trigger={trigger})")
        return snap_path
    except Exception as e:
        log.error(f"Snapshot failed: {e}")
        return None


def _rotate_snapshots():
    with get_conn() as conn:
        all_snaps = conn.execute("SELECT id, snapshot_file FROM snapshots ORDER BY created_at ASC").fetchall()
    excess = len(all_snaps) - config.SNAPSHOT_KEEP_COUNT
    if excess <= 0:
        return
    for snap in all_snaps[:excess]:
        path = os.path.join(config.SNAPSHOT_DIR, snap["snapshot_file"])
        if os.path.exists(path):
            os.remove(path)
        with get_write_conn() as conn:
            conn.execute("DELETE FROM snapshots WHERE id=?", (snap["id"],))


def _snapshot_scheduler():
    """Background thread: take a scheduled snapshot every N minutes."""
    interval = config.SNAPSHOT_INTERVAL_MINUTES * 60
    while True:
        time.sleep(interval)
        snapshot_db("scheduled")
