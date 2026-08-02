"""
config.py - CTS Tracker Configuration
All paths, thresholds, and settings live here.
Edit this file to adapt the system to a new environment.
"""

import os

# ---------------------------------------------------------------------------
# BASE PATHS
# ---------------------------------------------------------------------------

# Root directory of the project (folder containing this file)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Data directory - all runtime data lives here
DATA_DIR = os.path.join(os.environ.get("USERPROFILE", os.path.expanduser("~")), "Documents", "cts_tracker")

# SQLite database file
DB_PATH = os.path.join(DATA_DIR, "cts_tracker.db")

# Watchdog monitors this folder for incoming CTS (.oxps) and Dolphin (.do3) files.
# This should be the network share where client machines drop their files.
WATCH_DIR = r"\\CSAC-001\swmeets8\racenumbers"

# Drop Meet Manager schedule CSVs here for automatic import.
# Defaults to the current user's Documents folder — works on any Windows machine
# without knowing the username. Change this to a shared path if preferred.
SCHEDULE_DIR = os.path.join(os.path.expanduser("~"), "Documents")

# Raw incoming files are copied here with timestamps before processing
BACKUP_DIR = os.path.join(DATA_DIR, "backups")

# Periodic full database snapshots are saved here
SNAPSHOT_DIR = os.path.join(DATA_DIR, "snapshots")

# ---------------------------------------------------------------------------
# FILE TYPE CONFIGURATION
# ---------------------------------------------------------------------------

# File extensions the watchdog should react to
CTS_EXTENSION      = ".oxps"
GEN_EXTENSION      = ".gen"
DOLPHIN_EXTENSION  = ".do3"
SCHEDULE_EXTENSION = ".csv"

# Set False to stop ingesting .oxps entirely now that .gen has taken over.
# .gen already wins priority over .oxps per event/heat regardless of this
# setting -- this just stops new .oxps rows from being written at all, e.g.
# to cut down on log/backup noise once .gen has full confirmed coverage.
# Flip back to True any time to resume .oxps ingestion, no other changes needed.
INGEST_CTS_ENABLED = False

# TEMPORARILY False (2026-08-02): this instance is a disposable shadow test
# of the Dolphin5 XML/TCP feature, running alongside the real production
# server while the live meet is still officially on Dolphin4. Don't want
# this test instance also ingesting real Dolphin4 .do3 traffic from the live
# meet. Flip back to True (or remove) once this stops being a throwaway test
# run -- this is not meant to be a permanent toggle like INGEST_CTS_ENABLED.
INGEST_DOLPHIN_ENABLED = False

# ---------------------------------------------------------------------------
# DOLPHIN CORRELATION
# ---------------------------------------------------------------------------

# Maximum number of seconds between a CTS file timestamp and a Dolphin file
# timestamp for them to be considered the same race.
# Races are rarely less than 30 seconds apart, so 15s is a safe window.
# Widen this if you see missed correlations; tighten it if you see false matches.
DOLPHIN_MATCH_WINDOW_SECONDS = 15

# How long (seconds) to keep an unmatched Dolphin entry in pending_dolphin
# before flagging it as unresolvable. 0 = keep forever (manual review).
DOLPHIN_PENDING_TIMEOUT_SECONDS = 0

# ---------------------------------------------------------------------------
# DOLPHIN5 XML INGESTION + TCP CONTROL
# ---------------------------------------------------------------------------

DOLPHIN5_XML_EXTENSION = ".xml"

# Set False to stop ingesting Dolphin5 .xml files. Passive/read-only — safe
# to leave on by default, unlike DOLPHIN5_TCP_ENABLED below.
INGEST_DOLPHIN5_XML_ENABLED = True

# Master switch for the "chase GEN7" TCP control feature (dolphin5_control.py).
# This is the only part of the Dolphin5 feature that writes to live hardware,
# so it defaults OFF until deliberately enabled per venue.
DOLPHIN5_TCP_ENABLED = False

# Per-pool Dolphin5 unit connection info. host/port are just hardcoded
# fallbacks used only until something's been saved via the dashboard's
# Dolphin5 panel (database.get_dolphin5_configs()/save_dolphin5_config() --
# see dolphin5_control.py) -- once saved, the dashboard's value persists
# across restarts and wins over whatever's here. Left blank (None) rather
# than guessed: Pool 1's unit is usually the same physical machine but not
# guaranteed, and Pool 2's is rarely the same machine twice, so there's no
# safe default to bake into committed code for either -- set both from the
# dashboard instead. 13382 is DolphinTCPTest's confirmed-working protocol
# port (not a guess, unlike host).
#
# machine_id doubles as the expected name of that unit's subfolder under
# WATCH_DIR (e.g. WATCH_DIR\DOLPHIN5-P1\) -- point each Dolphin5 unit's
# Settings > LogFileDirectory at WATCH_DIR\<machine_id>\ directly (confirmed
# live 2026-08-02: Dolphin5 writes .xml straight to the share, no client.py
# relay). Separate subfolders per unit are required, not just tidy: Dolphin5's
# filenames carry no pool/unit identifier at all, and its Meet Number is
# auto-generated by the software itself rather than something set per unit,
# so it can't be trusted to keep two units' files from colliding -- two units
# writing the same heat/race in the same minute would otherwise produce
# byte-identical filenames and silently overwrite each other in a shared
# folder. watchdog_monitor.py watches WATCH_DIR recursively and derives each
# file's machine identity from whichever subfolder it's found in.
DOLPHIN5_CONFIGS = {
    1: {"host": None, "port": 13382, "machine_id": "DOLPHIN5-P1"},
    2: {"host": None, "port": 13382, "machine_id": "DOLPHIN5-P2"},
}

# How often (seconds) the chase loop polls get_current_heat_state().
DOLPHIN5_POLL_INTERVAL_SECONDS = 2

# A GEN7 event/heat change must hold steady for this long before the chase
# loop pushes setEventAndHeat, so a human quickly stepping through events
# manually doesn't spam Dolphin5 with one command per poll tick.
DOLPHIN5_CHASE_DEBOUNCE_SECONDS = 3

# Reconnect behavior for the persistent per-pool TCP connection.
DOLPHIN5_CONNECT_TIMEOUT_SECONDS = 5
DOLPHIN5_RECONNECT_DELAY_SECONDS = 5

# ---------------------------------------------------------------------------
# BACKUP & SNAPSHOT SETTINGS
# ---------------------------------------------------------------------------

# Snapshots are taken only on schedule import (pre_import / post_import).
# Scheduled periodic snapshots are disabled — SQLite WAL handles crash recovery.
SNAPSHOT_INTERVAL_MINUTES = 0   # 0 = disabled

# Number of snapshots to keep before rotating old ones out (0 = keep all)
SNAPSHOT_KEEP_COUNT = 0   # 0 = keep all snapshots, never auto-delete

# ---------------------------------------------------------------------------
# WATCHDOG SETTINGS
# ---------------------------------------------------------------------------

# Seconds to wait after a file is detected before reading it.
# Prevents reading a file that is still being written.
WATCHDOG_DEBOUNCE_SECONDS = 2.0

# ---------------------------------------------------------------------------
# FLASK SERVER SETTINGS
# ---------------------------------------------------------------------------

# Host and port the Flask server listens on.
# 0.0.0.0 means it accepts connections from any machine on the LAN.
FLASK_HOST = "0.0.0.0"
FLASK_PORT = 5000
FLASK_DEBUG = False  # Set True only during development

# How often the dashboard polls the server for updates (milliseconds)
DASHBOARD_POLL_INTERVAL_MS = 5000

# ---------------------------------------------------------------------------
# MEET MANAGER CSV FORMAT
# ---------------------------------------------------------------------------

# Fixed column indices in the MM heat sheet CSV export (0-based)
MM_COL_LICENSE     = 0
MM_COL_EXPORT_INFO = 1   # Contains MM version and export timestamp
MM_COL_MEET_NAME   = 2   # Meet name and date range
MM_COL_SESSION     = 5   # Report type marker: "Meet Program" or "Session Report" (also carries session text on some exports)
MM_COL_EVENT_FULL  = 6   # e.g. "#1 Girls 13 & Over 100 Yard Breaststroke"
MM_COL_HEAT_INFO   = 73  # e.g. "Heat   1 of 4   Prelims   Starts at 08:30 AM"
MM_COL_LANE        = 74  # Lane number as string

# Report type markers found in MM_COL_SESSION — used to tell the two MM export
# types apart by content rather than filename (operators can name exports anything)
MM_REPORT_TYPE_PROGRAM = "Meet Program"
MM_REPORT_TYPE_SESSION = "Session Report"

# Session Report-only columns (0-based) — see MM_REPORT_TYPE_SESSION above.
# Gives event->session mapping; the heat sheet's own MM_COL_SESSION is a
# constant "Meet Program" and carries no real per-session info.
MM_COL_SR_SESSION_LABEL = 6   # e.g. "Session: 1   8 and Unders"
MM_COL_SR_EVENT_NUM     = 15  # Event number, e.g. "25"
MM_COL_SR_EVENT_NAME    = 16  # Event name, or "  Break: 5 Minutes:" for break rows
MM_COL_SR_STARTS_AT     = 20  # Event's projected start time, e.g. "05:30 PM" — first heat only, not per-heat

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

LOG_LEVEL   = "INFO"   # DEBUG, INFO, WARNING, ERROR
LOG_TO_FILE = True
LOG_FILE    = os.path.join(DATA_DIR, "cts_tracker.log")

# ---------------------------------------------------------------------------
# ENSURE LOCAL DATA DIRECTORIES EXIST
# WATCH_DIR and SCHEDULE_DIR are not created here — they may be network paths
# or pre-existing folders that the server does not own.
# ---------------------------------------------------------------------------

for _dir in (DATA_DIR, BACKUP_DIR, SNAPSHOT_DIR):
    os.makedirs(_dir, exist_ok=True)
