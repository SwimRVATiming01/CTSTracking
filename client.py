"""
client.py - CTS Tracker Client
================================
Runs on CTS and Dolphin machines.
Monitors the local timing software output folder, captures file metadata,
and forwards files to the server's network watch folder.

Setup:
  1. Set MACHINE_TYPE below ("cts" or "dolphin")
  2. Run:  python client.py

Requirements:
    pip install watchdog
"""

import logging
import os
import platform
import shutil
import sys
import time
import threading
from datetime import datetime

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# ===========================================================================
# CONFIGURATION — only one thing to set per machine
# ===========================================================================

# *** Set this to "cts" or "dolphin" ***
MACHINE_TYPE = "dolphin"

# ---------------------------------------------------------------------------
# Everything below is automatic — no need to edit
# ---------------------------------------------------------------------------

# Machine ID pulled from Windows computer name
MACHINE_ID = platform.node().replace(" ", "_") or "UNKNOWN"

# Watch folder and file extension determined by machine type
if MACHINE_TYPE == "cts":
    # CTS software saves to Documents\CTS
    # AHK script already names files: YYYY-MM-DD_HH-MM-SS.oxps
    WATCH_FOLDER = os.path.join(os.environ.get("USERPROFILE", os.path.expanduser("~")), "Documents", "CTS")
    WATCHED_EXTENSION = ".oxps"
elif MACHINE_TYPE == "dolphin":
    # Dolphin software always saves to C:\CTSDolphin
    WATCH_FOLDER = r"C:\CTSDolphin"
    WATCHED_EXTENSION = ".do3"
else:
    print(f"ERROR: MACHINE_TYPE must be 'cts' or 'dolphin', got: {repr(MACHINE_TYPE)}")
    sys.exit(1)

# Network path to server watch folder — same on every client machine
SERVER_WATCH_FOLDER = r"\\CSAC-001\swmeets8\racenumbers"

# Seconds to wait after detection before forwarding
# Prevents reading a file still being written
DEBOUNCE_SECONDS = 0.75

# Retry logic if network share is temporarily unavailable
RETRY_ATTEMPTS = 5
RETRY_DELAY_SECONDS = 3

# ===========================================================================
# LOGGING
# ===========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "client.log")
        ),
    ],
)
log = logging.getLogger("client")

# ===========================================================================
# HELPERS
# ===========================================================================

def get_file_ctime(filepath):
    """
    Get file creation time as datetime.
    Called immediately on detection to preserve the original timestamp.
    On Windows getctime() returns true creation time.
    """
    try:
        return datetime.fromtimestamp(os.path.getctime(filepath))
    except OSError:
        return datetime.now()


def build_dest_filename(original_filename, machine_id, ctime=None):
    """
    Build the forwarded filename with machine ID embedded.

    CTS:     YYYY-MM-DD_HH-MM-SS.oxps    -> YYYY-MM-DD_HH-MM-SS__MACHINEID.oxps
             (timestamp already in name from AHK, just append machine ID)

    Dolphin: 039-000-00F0073.do3         -> 039-000-00F0073__MACHINEID__YYYYMMDDTHHMMSS.do3
             (no timestamp in name, embed ctime)
    """
    stem, ext = os.path.splitext(original_filename)

    if MACHINE_TYPE == "cts":
        # Timestamp already in filename from AHK — just append machine ID
        return f"{stem}__{machine_id}{ext}"

    else:  # dolphin
        # Embed machine ID and ctime
        timestamp = (ctime or datetime.now()).strftime("%Y%m%dT%H%M%S")
        return f"{stem}__{machine_id}__{timestamp}{ext}"


def copy_to_server(src_path, dest_filename):
    """
    Copy file to server watch folder with retry logic.
    Failures are logged but never crash the client or affect timing software.
    Returns True on success, False after all retries exhausted.
    """
    dest_path = os.path.join(SERVER_WATCH_FOLDER, dest_filename)

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            if not os.path.isdir(SERVER_WATCH_FOLDER):
                raise OSError(f"Server watch folder not accessible: {SERVER_WATCH_FOLDER}")
            shutil.copy2(src_path, dest_path)
            log.info(f"Forwarded: {dest_filename}")
            return True
        except OSError as e:
            if attempt < RETRY_ATTEMPTS:
                log.warning(f"Copy failed (attempt {attempt}/{RETRY_ATTEMPTS}): {e} — retrying in {RETRY_DELAY_SECONDS}s")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                log.error(f"Failed to forward {dest_filename} after {RETRY_ATTEMPTS} attempts: {e}")
                return False


# ===========================================================================
# WATCHDOG
# ===========================================================================

class ForwardHandler(FileSystemEventHandler):
    """
    Detects new files in the local watch folder.
    Captures ctime immediately on detection, debounces, then forwards.
    """

    def __init__(self):
        self._pending = {}   # filepath -> (ctime, fire_at)
        self._lock = threading.Lock()
        self._start_debounce_loop()

    def _start_debounce_loop(self):
        def loop():
            while True:
                time.sleep(0.05)
                now = time.time()
                to_fire = []
                with self._lock:
                    for path, (ctime, fire_at) in list(self._pending.items()):
                        if now >= fire_at:
                            to_fire.append((path, ctime))
                            del self._pending[path]
                for path, ctime in to_fire:
                    self._process(path, ctime)
        threading.Thread(target=loop, daemon=True).start()

    def _should_handle(self, filepath):
        return os.path.splitext(filepath)[1].lower() == WATCHED_EXTENSION.lower()

    def on_created(self, event):
        if not event.is_directory and self._should_handle(event.src_path):
            # Capture ctime IMMEDIATELY before debounce delay
            ctime = get_file_ctime(event.src_path)
            log.debug(f"Detected: {os.path.basename(event.src_path)} (ctime={ctime})")
            with self._lock:
                self._pending[event.src_path] = (ctime, time.time() + DEBOUNCE_SECONDS)

    def on_moved(self, event):
        if not event.is_directory and self._should_handle(event.dest_path):
            ctime = get_file_ctime(event.dest_path)
            log.debug(f"Moved in: {os.path.basename(event.dest_path)} (ctime={ctime})")
            with self._lock:
                self._pending[event.dest_path] = (ctime, time.time() + DEBOUNCE_SECONDS)

    def _process(self, filepath, ctime):
        if not os.path.exists(filepath):
            log.warning(f"File gone before processing: {filepath}")
            return
        filename = os.path.basename(filepath)
        dest_filename = build_dest_filename(filename, MACHINE_ID, ctime)
        log.info(f"Processing: {filename} -> {dest_filename}")
        copy_to_server(filepath, dest_filename)


# ===========================================================================
# STARTUP CHECKS
# ===========================================================================

def check_config():
    errors = []

    if not os.path.isdir(WATCH_FOLDER):
        errors.append(f"Local watch folder does not exist: {WATCH_FOLDER}")

    if errors:
        for e in errors:
            log.error(f"Config error: {e}")
        sys.exit(1)

    if not os.path.isdir(SERVER_WATCH_FOLDER):
        log.warning(
            f"Server watch folder not currently accessible: {SERVER_WATCH_FOLDER} — "
            f"client will start anyway and retry when files arrive."
        )


# ===========================================================================
# MAIN
# ===========================================================================

if __name__ == "__main__":
    log.info("=" * 50)
    log.info("CTS Tracker Client starting")
    log.info(f"  Machine type: {MACHINE_TYPE.upper()}")
    log.info(f"  Machine ID:   {MACHINE_ID}  (from computer name)")
    log.info(f"  Watching:     {WATCH_FOLDER}")
    log.info(f"  Extension:    {WATCHED_EXTENSION}")
    log.info(f"  Forwarding:   {SERVER_WATCH_FOLDER}")
    log.info("=" * 50)

    check_config()

    handler = ForwardHandler()
    observer = Observer()
    observer.schedule(handler, WATCH_FOLDER, recursive=False)
    observer.start()
    log.info("Watching for files... (Ctrl+C to stop)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down.")
        observer.stop()

    observer.join()
