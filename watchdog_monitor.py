"""
watchdog_monitor.py - File system watchdog for CTS, Dolphin, and schedule files.
"""

import logging
import os
import threading
import time

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

import config
from ingestion import ingest_cts_file, ingest_dolphin_file, ingest_schedule_file

log = logging.getLogger("cts_tracker")


class IngestHandler(FileSystemEventHandler):
    """
    Fires when a file is created or moved into the watch folder.
    Debounces to avoid reading files still being written.
    Routes to the correct ingest function based on file extension.
    """

    def __init__(self):
        self._pending = {}   # filepath -> scheduled fire time
        self._lock = threading.Lock()
        self._start_debounce_loop()

    def _start_debounce_loop(self):
        def loop():
            while True:
                time.sleep(0.1)
                now = time.time()
                to_fire = []
                with self._lock:
                    for path, fire_at in list(self._pending.items()):
                        if now >= fire_at:
                            to_fire.append(path)
                            del self._pending[path]
                for path in to_fire:
                    self._process(path)
        t = threading.Thread(target=loop, daemon=True)
        t.start()

    def on_created(self, event):
        if not event.is_directory:
            self._schedule(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._schedule(event.dest_path)

    def _schedule(self, path):
        with self._lock:
            self._pending[path] = time.time() + config.WATCHDOG_DEBOUNCE_SECONDS

    def _process(self, filepath):
        if not os.path.exists(filepath):
            return
        ext = os.path.splitext(filepath)[1].lower()
        filename = os.path.basename(filepath)
        log.info(f"Watchdog detected: {filename}")

        # CSVs are only ingested as schedules when they come from SCHEDULE_DIR.
        # CSVs landing in WATCH_DIR are ignored — that folder is for CTS/Dolphin files only.
        from_schedule_dir = os.path.dirname(os.path.abspath(filepath)) == os.path.abspath(config.SCHEDULE_DIR)

        try:
            if ext == config.CTS_EXTENSION.lower():
                result = ingest_cts_file(filepath)
            elif ext == config.DOLPHIN_EXTENSION.lower():
                result = ingest_dolphin_file(filepath)
            elif ext == config.SCHEDULE_EXTENSION.lower() and from_schedule_dir:
                result = ingest_schedule_file(filepath)
            else:
                log.debug(f"Ignoring: {filename} (wrong folder or unknown type)")
                return
            log.info(f"Ingested {filename}: {result}")
        except Exception as e:
            log.error(f"Error processing {filename}: {e}", exc_info=True)


def _watch_dir_with_retry(observer, handler, path, label, retry_interval=10):
    """
    Try to add a folder to the watchdog observer.
    If the folder isn't accessible yet, keep retrying in the background
    rather than crashing the server. Useful for network shares that may
    take a moment to become available after startup.
    """
    def attempt():
        while True:
            try:
                if os.path.isdir(path):
                    observer.schedule(handler, path, recursive=False)
                    log.info(f"Watchdog monitoring {label}: {path}")
                    return
                else:
                    log.warning(f"{label} not accessible yet: {path} — retrying in {retry_interval}s")
            except Exception as e:
                log.warning(f"{label} watch failed: {e} — retrying in {retry_interval}s")
            time.sleep(retry_interval)

    threading.Thread(target=attempt, daemon=True).start()


def start_watchdog():
    """
    Start the watchdog observer monitoring two folders:
      WATCH_DIR     — network share for CTS and Dolphin files from client machines
      SCHEDULE_DIR  — local Documents folder for MM schedule CSV drops

    Neither folder is required to be accessible at startup — if unreachable,
    the server logs a warning and keeps retrying in the background.
    """
    handler = IngestHandler()
    observer = Observer()
    observer.daemon = True
    observer.start()

    _watch_dir_with_retry(observer, handler, config.WATCH_DIR, "WATCH_DIR")

    if os.path.abspath(config.SCHEDULE_DIR) != os.path.abspath(config.WATCH_DIR):
        _watch_dir_with_retry(observer, handler, config.SCHEDULE_DIR, "SCHEDULE_DIR")

    return observer
