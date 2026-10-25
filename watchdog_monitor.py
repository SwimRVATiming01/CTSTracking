"""
watchdog_monitor.py - File system watchdog for CTS, Dolphin, and schedule files.
"""

import logging
import os
import threading
import time

from watchdog.events import FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

import config
from ingestion import (
    ingest_cts_file, ingest_dolphin_file, ingest_gen_file, ingest_schedule_file,
    ingest_session_report, detect_mm_report_type, ingest_dolphin5_file,
)

log = logging.getLogger("cts_tracker")


class IngestHandler(FileSystemEventHandler):
    """
    Fires when a file is created or moved into the watch folder, or (CSVs
    only) when an existing one is overwritten.
    Debounces to avoid reading files still being written.
    Routes to the correct ingest function based on file extension.
    """

    def __init__(self):
        self._pending = {}   # filepath -> scheduled fire time
        self._seen = set()   # normalized paths already handed to _process (health check)
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

    def on_modified(self, event):
        # Overwrites only count for CSVs: re-exporting a Meet Manager report
        # under the same filename replaces the file in place, which never
        # fires on_created. Other types (e.g. Dolphin5's CurrentSettings.xml)
        # get rewritten constantly and must stay create-only. Each write
        # pushes the debounce back, so a slow network save fires once.
        if not event.is_directory and \
                os.path.splitext(event.src_path)[1].lower() == config.SCHEDULE_EXTENSION.lower():
            self._schedule(event.src_path)

    def _schedule(self, path):
        with self._lock:
            self._pending[path] = time.time() + config.WATCHDOG_DEBOUNCE_SECONDS

    def _process(self, filepath):
        with self._lock:
            self._seen.add(os.path.normcase(os.path.abspath(filepath)))
        if not os.path.exists(filepath):
            return
        ext = os.path.splitext(filepath)[1].lower()
        filename = os.path.basename(filepath)
        log.info(f"Watchdog detected: {filename}")

        # CSVs are ingested as schedules/session reports from either SCHEDULE_DIR
        # (local Documents folder on whichever machine runs the server) or
        # anywhere under WATCH_DIR (the shared network drive, reachable from any
        # machine on the LAN — added 2026-08-02 so meet management CSVs don't
        # require being physically at the server machine to drop one in).
        from_recognized_csv_dir = (
            os.path.dirname(os.path.abspath(filepath)) == os.path.abspath(config.SCHEDULE_DIR)
            or _is_under_watch_dir(filepath)
        )

        try:
            if ext == config.CTS_EXTENSION.lower():
                if not config.INGEST_CTS_ENABLED:
                    log.debug(f"Ignoring {filename}: .oxps ingestion disabled (INGEST_CTS_ENABLED=False)")
                    return
                result = ingest_cts_file(filepath)
            elif ext == config.GEN_EXTENSION.lower():
                result = ingest_gen_file(filepath)
            elif ext == config.DOLPHIN_EXTENSION.lower():
                if not config.INGEST_DOLPHIN_ENABLED:
                    log.debug(f"Ignoring {filename}: .do3 ingestion disabled (INGEST_DOLPHIN_ENABLED=False)")
                    return
                result = ingest_dolphin_file(filepath)
            elif ext == config.DOLPHIN5_XML_EXTENSION.lower():
                if not config.INGEST_DOLPHIN5_XML_ENABLED:
                    log.debug(f"Ignoring {filename}: Dolphin5 XML ingestion disabled (INGEST_DOLPHIN5_XML_ENABLED=False)")
                    return
                result = ingest_dolphin5_file(filepath, machine_id_hint=_folder_machine_id(filepath))
            elif ext == config.SCHEDULE_EXTENSION.lower() and from_recognized_csv_dir:
                report_type = detect_mm_report_type(filepath)
                if report_type == config.MM_REPORT_TYPE_PROGRAM:
                    result = ingest_schedule_file(filepath)
                elif report_type == config.MM_REPORT_TYPE_SESSION:
                    result = ingest_session_report(filepath)
                else:
                    log.warning(
                        f"Ignoring {filename}: could not determine MM CSV report type "
                        f"(unreadable or unrecognized content) — this file was NOT ingested"
                    )
                    return
            else:
                log.debug(f"Ignoring: {filename} (wrong folder or unknown type)")
                return
            log.info(f"Ingested {filename}: {result}")
        except Exception as e:
            log.error(f"Error processing {filename}: {e}", exc_info=True)


def _is_under_watch_dir(filepath):
    """True for anything at WATCH_DIR's root or in any subfolder beneath it."""
    watch_dir_abs = os.path.abspath(config.WATCH_DIR)
    file_dir_abs = os.path.abspath(os.path.dirname(filepath))
    return file_dir_abs == watch_dir_abs or file_dir_abs.startswith(watch_dir_abs + os.sep)


def _folder_machine_id(filepath):
    """
    Derive a machine/unit identity from a file's parent folder, for files
    that land directly on the network share with no client.py relay step
    to embed a machine ID in the filename (e.g. Dolphin5, which writes its
    .xml files straight to the share). Returns None for anything sitting
    directly in WATCH_DIR's root — only files inside a subfolder get an
    identity from this.

    This is the fix for a real collision risk: Dolphin5's filenames carry
    no pool/unit identifier at all (confirmed live 2026-08-02 — its Meet
    Number is auto-generated by the software itself, not something set per
    unit, so it can't be trusted to keep two units' files apart either).
    Two Dolphin5 units writing the same heat/race in the same minute would
    produce byte-identical filenames and silently overwrite each other if
    they wrote into the same flat folder. Giving each physical unit its own
    subfolder (named after the unit, e.g. WATCH_DIR\\DOLPHIN5-P1\\) makes
    that impossible by construction — different paths can't collide — and
    this function recovers the machine identity for display/diagnostics
    from whichever subfolder a file was found in.
    """
    if not _is_under_watch_dir(filepath):
        return None
    file_dir_abs = os.path.abspath(os.path.dirname(filepath))
    if file_dir_abs == os.path.abspath(config.WATCH_DIR):
        return None
    return os.path.basename(file_dir_abs)


def _watch_dir_with_retry(observer, handler, path, label, retry_interval=10, recursive=False):
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
                    observer.schedule(handler, path, recursive=recursive)
                    log.info(f"Watchdog monitoring {label}: {path}" + (" (recursive)" if recursive else ""))
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
      WATCH_DIR     — network share for CTS/Dolphin files from client machines,
                      AND meet program/session report CSVs (anywhere under it —
                      see _is_under_watch_dir), since it's reachable from any
                      machine on the LAN, unlike SCHEDULE_DIR below.
      SCHEDULE_DIR  — local Documents folder on whichever machine runs the
                      server, for MM schedule CSV drops from that machine only.

    Neither folder is required to be accessible at startup — if unreachable,
    the server logs a warning and keeps retrying in the background.

    WATCH_DIR is watched recursively (SCHEDULE_DIR is not — CSVs still only
    land flat there): client.py copies Dolphin files (Dolphin5 .xml,
    Dolphin4 .do3) into each machine's own ip<octet>_<computer name>
    subfolder, so files appear one level down. GEN7 .gen still lands flat
    in WATCH_DIR's root; recursive just means subfolders are ALSO seen.

    Uses PollingObserver rather than the native ReadDirectoryChangesW backend:
    WATCH_DIR is a UNC path, and that native backend's change-notification
    handle can go silently stale after an SMB session drop (share host
    reboot, network blip, machine sleep) — Windows doesn't raise an error,
    the observer just stops delivering events with nothing logged, and only
    a process restart re-opens a working handle. Polling re-lists the
    directory each interval instead of holding a handle, so it can't wedge
    that way.
    """
    handler = IngestHandler()
    observer = PollingObserver()
    observer.daemon = True
    observer.start()

    _watch_dir_with_retry(observer, handler, config.WATCH_DIR, "WATCH_DIR", recursive=True)

    if os.path.abspath(config.SCHEDULE_DIR) != os.path.abspath(config.WATCH_DIR):
        _watch_dir_with_retry(observer, handler, config.SCHEDULE_DIR, "SCHEDULE_DIR")

    _start_health_check(observer, handler)
    return observer


# Watcher health status, read by /api/watch_health.
watch_health = {"ok": True, "last_check": None, "last_problem": None, "missed_total": 0}

def _start_health_check(observer, handler):
    """
    Safety net for the watcher silently going deaf. Seen live 2026-10-11: the
    observer stopped reporting WATCH_DIR files at 12:01 with nothing logged,
    while .gen/.xml files kept landing for over an hour; only a server restart
    revived it.

    Every WATCH_HEALTH_INTERVAL_SECONDS this re-lists WATCH_DIR and looks for
    result files (.gen/.xml/.do3) that arrived after the server started, are
    older than WATCH_HEALTH_GRACE_SECONDS, and were never handed to _process.
    Any found are logged at ERROR, ingested oldest-first (so a .gen still
    lands before or near its Dolphin file, as it would live), and the
    WATCH_DIR watch is re-armed. Only files newer than server start count,
    so a restart never re-ingests the existing backlog.
    """
    exts = {e.lower() for e in (config.GEN_EXTENSION, config.DOLPHIN_EXTENSION,
                                config.DOLPHIN5_XML_EXTENSION)}
    started = time.time()

    def loop():
        while True:
            time.sleep(config.WATCH_HEALTH_INTERVAL_SECONDS)
            try:
                _health_pass(observer, handler, exts, started)
            except Exception as e:
                log.warning(f"Watch health check failed: {e}")

    threading.Thread(target=loop, daemon=True).start()


def _health_pass(observer, handler, exts, started):
    from datetime import datetime
    watch_health["last_check"] = datetime.now().isoformat(timespec="seconds")
    if not os.path.isdir(config.WATCH_DIR):
        watch_health["ok"] = False
        watch_health["last_problem"] = f"{watch_health['last_check']}: WATCH_DIR not reachable"
        log.error(f"Watch health: WATCH_DIR not reachable: {config.WATCH_DIR}")
        return

    cutoff = time.time() - config.WATCH_HEALTH_GRACE_SECONDS
    with handler._lock:
        seen = set(handler._seen) | {os.path.normcase(os.path.abspath(p)) for p in handler._pending}
    missed = []
    for root, _dirs, files in os.walk(config.WATCH_DIR):
        for f in files:
            if os.path.splitext(f)[1].lower() not in exts:
                continue
            if f.lower() == "dolphin_settings.xml":  # Dolphin5 config, not a result -- never ingested
                continue
            path = os.path.join(root, f)
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            if started <= mtime <= cutoff and os.path.normcase(os.path.abspath(path)) not in seen:
                missed.append((mtime, path))

    if not missed:
        watch_health["ok"] = True
        return

    names = ", ".join(os.path.basename(p) for _m, p in sorted(missed))
    log.error(f"Watch health: watcher missed {len(missed)} file(s), ingesting now and re-arming: {names}")
    watch_health["ok"] = False
    watch_health["missed_total"] += len(missed)
    watch_health["last_problem"] = f"{watch_health['last_check']}: missed {len(missed)} file(s): {names}"
    for _mtime, path in sorted(missed):
        handler._process(path)

    try:
        observer.unschedule_all()
        observer.schedule(handler, config.WATCH_DIR, recursive=True)
        if os.path.abspath(config.SCHEDULE_DIR) != os.path.abspath(config.WATCH_DIR):
            observer.schedule(handler, config.SCHEDULE_DIR)
        log.warning("Watch health: WATCH_DIR watch re-armed")
    except Exception as e:
        log.error(f"Watch health: re-arm failed ({e}) -- restart the server")
