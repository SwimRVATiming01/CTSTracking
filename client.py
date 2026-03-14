"""
client.py - CTS Tracker Client
================================
Runs on any timing machine (CTS, Dolphin, or both).
Monitors local timing software output folders, captures file metadata,
and forwards files to the server's network watch folder.

Setup:
  No configuration required — watches both CTS and Dolphin folders automatically.
  Run:  python client.py

Requirements:
    pip install watchdog
"""

import ctypes
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
# CONFIGURATION
# ===========================================================================

# Machine ID pulled from Windows computer name
MACHINE_ID = platform.node().replace(" ", "_") or "UNKNOWN"

# CTS software saves .oxps files here (named by AHK: YYYY-MM-DD_HH-MM-SS.oxps)
CTS_WATCH_FOLDER = os.path.join(os.environ.get("USERPROFILE", os.path.expanduser("~")), "Documents", "CTS")

# Dolphin software always saves .do3 files here
DOLPHIN_WATCH_FOLDER = r"C:\CTSDolphin"

# Network path to server watch folder — same on every client machine
SERVER_WATCH_FOLDER = r"\\CSAC-001\swmeets8\racenumbers"

# Seconds to wait after detection before checking file readiness
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

def warn_popup(message):
    """Show a non-blocking Windows warning dialog in a background thread."""
    def _show():
        ctypes.windll.user32.MessageBoxW(
            0,
            message,
            "CTS Tracker Client — Warning",
            0x30,  # MB_ICONWARNING | MB_OK
        )
    threading.Thread(target=_show, daemon=True).start()


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

    CTS (.oxps):    YYYY-MM-DD_HH-MM-SS.oxps  -> YYYY-MM-DD_HH-MM-SS__MACHINEID.oxps
                    (timestamp already in name from AHK, just append machine ID)

    Dolphin (.do3): 039-000-00F0073.do3        -> 039-000-00F0073__MACHINEID__YYYYMMDDTHHMMSS.do3
                    (no timestamp in name, embed ctime)
    """
    stem, ext = os.path.splitext(original_filename)

    if ext.lower() == ".oxps":
        return f"{stem}__{machine_id}{ext}"
    else:  # .do3
        timestamp = (ctime or datetime.now()).strftime("%Y%m%dT%H%M%S")
        return f"{stem}__{machine_id}__{timestamp}{ext}"


def wait_for_file_ready(filepath, stable_seconds=0.5, timeout=30, poll_interval=0.25):
    """
    Wait until a file is non-empty and its size stops changing.
    Returns True when ready, False if it times out.
    """
    deadline = time.time() + timeout
    last_size = -1
    stable_since = None

    while time.time() < deadline:
        try:
            size = os.path.getsize(filepath)
        except OSError:
            time.sleep(poll_interval)
            continue

        if size == 0:
            last_size = 0
            stable_since = None
            time.sleep(poll_interval)
            continue

        if size != last_size:
            last_size = size
            stable_since = time.time()
        elif stable_since is not None and (time.time() - stable_since) >= stable_seconds:
            return True

        time.sleep(poll_interval)

    log.warning(f"Timed out waiting for file to be ready: {os.path.basename(filepath)} (last size={last_size})")
    return False


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
                warn_popup(
                    f"Failed to send file to server after {RETRY_ATTEMPTS} attempts:\n\n"
                    f"  {dest_filename}\n\n"
                    f"Server folder: {SERVER_WATCH_FOLDER}\n\n"
                    f"Check your network connection. The file was NOT forwarded."
                )
                return False


# ===========================================================================
# WATCHDOG
# ===========================================================================

WATCHED_EXTENSIONS = {".oxps", ".do3"}


class ForwardHandler(FileSystemEventHandler):
    """
    Detects new .oxps and .do3 files in watched folders.
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
        return os.path.splitext(filepath)[1].lower() in WATCHED_EXTENSIONS

    def on_created(self, event):
        if not event.is_directory and self._should_handle(event.src_path):
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
        if not wait_for_file_ready(filepath):
            log.error(f"Skipping {filename}: file never became ready (still empty or changing)")
            return
        dest_filename = build_dest_filename(filename, MACHINE_ID, ctime)
        log.info(f"Processing: {filename} -> {dest_filename}")
        copy_to_server(filepath, dest_filename)


# ===========================================================================
# FOLDER WATCHING WITH RETRY
# ===========================================================================

def _watch_folder_with_retry(observer, handler, path, label, retry_interval=10):
    """
    Try to add a folder to the watchdog observer.
    If the folder doesn't exist yet, keep retrying in the background.
    """
    def attempt():
        while True:
            if os.path.isdir(path):
                try:
                    observer.schedule(handler, path, recursive=False)
                    log.info(f"Watching {label}: {path}")
                    return
                except Exception as e:
                    log.warning(f"Could not watch {label}: {e} — retrying in {retry_interval}s")
            else:
                log.warning(f"{label} not found: {path} — retrying in {retry_interval}s")
            time.sleep(retry_interval)

    threading.Thread(target=attempt, daemon=True).start()


# ===========================================================================
# MAIN
# ===========================================================================

if __name__ == "__main__":
    log.info("=" * 50)
    log.info("CTS Tracker Client starting")
    log.info(f"  Machine ID:  {MACHINE_ID}  (from computer name)")
    log.info(f"  CTS folder:  {CTS_WATCH_FOLDER}")
    log.info(f"  Dolphin folder: {DOLPHIN_WATCH_FOLDER}")
    log.info(f"  Forwarding:  {SERVER_WATCH_FOLDER}")
    log.info("=" * 50)

    if not os.path.isdir(SERVER_WATCH_FOLDER):
        msg = (
            f"Cannot reach the server folder:\n\n"
            f"  {SERVER_WATCH_FOLDER}\n\n"
            f"Check that you are connected to the network and the server is online.\n\n"
            f"The client will start anyway and retry automatically when files arrive."
        )
        log.warning(f"Server watch folder not accessible: {SERVER_WATCH_FOLDER}")
        warn_popup(msg)

    handler = ForwardHandler()
    observer = Observer()
    observer.daemon = True
    observer.start()

    _watch_folder_with_retry(observer, handler, CTS_WATCH_FOLDER, "CTS")
    _watch_folder_with_retry(observer, handler, DOLPHIN_WATCH_FOLDER, "Dolphin")

    log.info("Watching for files... (Ctrl+C to stop)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down.")
        observer.stop()

    observer.join()
