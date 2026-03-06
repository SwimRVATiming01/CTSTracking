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
DOLPHIN_EXTENSION  = ".do3"
SCHEDULE_EXTENSION = ".csv"

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
# BACKUP & SNAPSHOT SETTINGS
# ---------------------------------------------------------------------------

# How often (in minutes) to take a full database snapshot
SNAPSHOT_INTERVAL_MINUTES = 30

# Number of snapshots to keep before rotating old ones out (0 = keep all)
SNAPSHOT_KEEP_COUNT = 48  # 48 x 30min = 24 hours of history

# ---------------------------------------------------------------------------
# WATCHDOG SETTINGS
# ---------------------------------------------------------------------------

# Seconds to wait after a file is detected before reading it.
# Prevents reading a file that is still being written.
WATCHDOG_DEBOUNCE_SECONDS = 0.5

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
MM_COL_SESSION     = 5   # e.g. "Meet Program - Friday Finals"
MM_COL_EVENT_FULL  = 6   # e.g. "#1 Girls 13 & Over 100 Yard Breaststroke"
MM_COL_HEAT_INFO   = 73  # e.g. "Heat   1 of 4   Prelims   Starts at 08:30 AM"
MM_COL_LANE        = 74  # Lane number as string

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
