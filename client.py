r"""
client.py - CTS Tracker Client
================================
Runs on any Dolphin timing machine (Dolphin4 or Dolphin5).
Watches the local Dolphin output folder and copies race files to this
machine's own subfolder on the server's network watch folder. Originals stay
put -- Meet Manager ingests them locally.

Also runs on GEN7 machines, where there is no C:\CTSDolphin: there it only
sends heartbeats and keeps its share subfolder in place.

Setup:
  No configuration required -- the share subfolder is named automatically
  from this machine's 172.16.0.x address and computer name, and created at
  startup. Deploy the .exe to C:\CTSClient on each machine, not Google
  Drive -- it warns at startup otherwise. Run:  python client.py

Requirements:
    pip install watchdog
"""

import ctypes
import logging
import logging.handlers
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
import threading
import urllib.request
import urllib.error
import json
from datetime import datetime

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

try:
    import psutil
    _PSUTIL = True
except ImportError:
    _PSUTIL = False

# ===========================================================================
# CONFIGURATION
# ===========================================================================

# Bump on every rebuild of dist/CTS_Client.exe. CTSTracking reads this line
# from its own copy of client.py and flags machines reporting anything else.
CLIENT_VERSION = "2.0.1"

# Where the .exe must be deployed on every machine. Running it from Google
# Drive (or anything synced) breaks updates -- the running .exe is locked --
# and puts every machine's log in one shared file.
EXPECTED_INSTALL_DIR = r"C:\CTSClient"

# Machine ID pulled from Windows computer name
MACHINE_ID = platform.node().replace(" ", "_") or "UNKNOWN"

# Dolphin4 (.do3) and Dolphin5 (.xml + .do3) both save here. Dolphin5 used to
# write straight to the share, but crashed doing so (2026-10-10), so it now
# writes locally and is relayed like Dolphin4.
DOLPHIN_WATCH_FOLDER = r"C:\CTSDolphin"

# Network path to server watch folder — same on every client machine
SERVER_WATCH_FOLDER = r"\\CSAC-001\swmeets8\racenumbers"

# Only addresses on the timing network count when naming the share subfolder
# (rules out Wi-Fi and virtual adapters).
TIMING_NETWORK_PREFIX = "172.16.0."

# Dolphin5's own working files -- not race data, never forwarded.
DOLPHIN5_SKIP_FILENAMES = {"currentsettings.xml"}

DOLPHIN5_PROCESS_NAME = "dolphin50.exe"

# Seconds to wait after detection before checking file readiness
DEBOUNCE_SECONDS = 0.75

# Retry logic if network share is temporarily unavailable
RETRY_ATTEMPTS = 5
RETRY_DELAY_SECONDS = 3

# Heartbeat / discovery
DISCOVERY_PORT     = 47200
HEARTBEAT_INTERVAL = 30   # seconds between heartbeats
DISCOVERY_TIMEOUT  = 3    # seconds to wait for server discovery response

# Share upkeep: re-create the share subfolder if it vanishes, and copy
# client.log into it so every machine's log can be read from one place.
SHARE_UPKEEP_INTERVAL = 30    # seconds
LOG_MIRROR_INTERVAL   = 300   # seconds

# How often to look again for C:\CTSDolphin when it isn't there (normal on GEN7)
DOLPHIN_FOLDER_RECHECK = 30   # seconds

# ===========================================================================
# LOGGING
# ===========================================================================

# The .exe's own folder when packaged (PyInstaller unpacks __file__ to a temp dir).
INSTALL_DIR = os.path.dirname(sys.executable if getattr(sys, "frozen", False) else os.path.abspath(__file__))
INSTALL_OK = os.path.normcase(os.path.normpath(INSTALL_DIR)) == os.path.normcase(EXPECTED_INSTALL_DIR)

LOG_PATH = os.path.join(INSTALL_DIR, "client.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        # Capped so the copy mirrored to the share stays small.
        logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=1_000_000, backupCount=3),
    ],
)
log = logging.getLogger("client")

# Set at startup in __main__, once the helpers below exist.
SHARE_SUBFOLDER = None

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


def get_timing_ip():
    """This machine's 172.16.0.x address, or None if it has none.

    Prefers the address Windows actually routes to the share server over,
    since a machine can have extra 172.16.0.x addresses on unused adapters
    (CSAC-009 reported .83 while really reachable at .84, 2026-10-10).
    A UDP connect sends no packets -- it just asks for the route.
    """
    share_host = SERVER_WATCH_FOLDER.lstrip("\\").split("\\")[0]
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect((socket.gethostbyname(share_host), 445))
            ip = s.getsockname()[0]
        if ip.startswith(TIMING_NETWORK_PREFIX):
            return ip
    except OSError:
        pass

    candidates = []
    if _PSUTIL:
        try:
            for addrs in psutil.net_if_addrs().values():
                candidates += [a.address for a in addrs if a.family == socket.AF_INET]
        except Exception:
            pass
    try:
        candidates += socket.gethostbyname_ex(socket.gethostname())[2]
    except OSError:
        pass
    for ip in candidates:
        if ip.startswith(TIMING_NETWORK_PREFIX):
            return ip
    return None


def get_share_subfolder_name():
    """ip<last octet>_<computer name>, e.g. ip84_DOLPHIN-PC1. Each machine
    gets its own subfolder so CTSTracking can tell units apart by folder."""
    ip = get_timing_ip()
    octet = ip.rsplit(".", 1)[1] if ip else "unknown"
    return f"ip{octet}_{MACHINE_ID}"


def dolphin5_running():
    """True if Dolphin5 is running. Decides whether this is a Dolphin5
    machine (forward .xml, keep .do3 local) or Dolphin4 (forward .do3)."""
    if _PSUTIL:
        try:
            return any((p.info["name"] or "").lower() == DOLPHIN5_PROCESS_NAME
                       for p in psutil.process_iter(["name"]))
        except Exception:
            pass
    try:
        out = subprocess.run(["tasklist", "/FI", f"IMAGENAME eq {DOLPHIN5_PROCESS_NAME}", "/NH"],
                             capture_output=True, text=True, timeout=5).stdout
        return DOLPHIN5_PROCESS_NAME in out.lower()
    except Exception:
        return False


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
    Build the forwarded filename.

    Dolphin (.do3):  039-000-00F0073.do3 -> 039-000-00F0073__MACHINEID__YYYYMMDDTHHMMSS.do3
                     (no timestamp in name, embed ctime)

    Dolphin5 (.xml): unchanged -- CTSTracking parses the original name and
                     takes the machine identity from the subfolder instead.
    """
    stem, ext = os.path.splitext(original_filename)
    if ext.lower() == ".xml":
        return original_filename
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
    dest_folder = os.path.join(SERVER_WATCH_FOLDER, SHARE_SUBFOLDER)
    dest_path = os.path.join(dest_folder, dest_filename)

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            if not os.path.isdir(SERVER_WATCH_FOLDER):
                raise OSError(f"Server watch folder not accessible: {SERVER_WATCH_FOLDER}")
            os.makedirs(dest_folder, exist_ok=True)
            shutil.copy2(src_path, dest_path)
            log.info(f"Forwarded: {SHARE_SUBFOLDER}\\{dest_filename}")
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
# HEARTBEAT
# ===========================================================================

def _discover_server():
    """
    Broadcast on the LAN to find the CTS Tracker Flask server.
    Returns (ip, port) tuple or None if no server responds.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(DISCOVERY_TIMEOUT)
        sock.sendto(b"CTS_TRACKER_DISCOVER", ("255.255.255.255", DISCOVERY_PORT))
        data, addr = sock.recvfrom(1024)
        sock.close()
        if data.startswith(b"CTS_TRACKER_HERE:"):
            port = int(data.split(b":")[1])
            return addr[0], port
    except Exception:
        pass
    return None


def _gather_status():
    """Collect machine status for the heartbeat payload."""
    ahk_scripts    = []
    vicreo_running = None
    share_ok       = None
    dolphin_ok     = os.path.isdir(DOLPHIN_WATCH_FOLDER)

    if _PSUTIL:
        try:
            for proc in psutil.process_iter(["name", "cmdline"]):
                name = (proc.info["name"] or "").lower()
                if "autohotkey" in name:
                    cmdline = proc.info["cmdline"] or []
                    scripts = [os.path.basename(a) for a in cmdline if a.lower().endswith(".ahk")]
                    ahk_scripts.extend(scripts)
                if "vicreo" in name:
                    vicreo_running = True
            if vicreo_running is None:
                vicreo_running = False
        except Exception:
            pass

    # Write-access check on network share
    try:
        test_path = os.path.join(SERVER_WATCH_FOLDER, ".cts_hb_check")
        with open(test_path, "w") as f:
            f.write("ok")
        os.remove(test_path)
        share_ok = True
    except Exception:
        share_ok = False

    return {
        "machine_id":       MACHINE_ID,
        "client_version":   CLIENT_VERSION,
        "install_dir":      INSTALL_DIR,
        "install_ok":       INSTALL_OK,
        "share_folder":     SHARE_SUBFOLDER,
        "ahk_scripts":      ahk_scripts,
        "vicreo_running":   vicreo_running,
        "share_ok":         share_ok,
        "dolphin_ok":       dolphin_ok,
        "dolphin5_running": dolphin5_running(),
    }


def _heartbeat_loop():
    """
    Background thread: discover the server via UDP broadcast, then send
    a status heartbeat every HEARTBEAT_INTERVAL seconds.
    Re-discovers if the server becomes unreachable.
    """
    server = None
    while True:
        if server is None:
            log.info("Heartbeat: searching for CTS Tracker server...")
            server = _discover_server()
            if server:
                log.info(f"Heartbeat: server found at {server[0]}:{server[1]}")
            else:
                log.debug("Heartbeat: server not found, retrying in 30s")
                time.sleep(HEARTBEAT_INTERVAL)
                continue

        try:
            payload = json.dumps(_gather_status()).encode()
            url     = f"http://{server[0]}:{server[1]}/api/heartbeat"
            req     = urllib.request.Request(
                url, data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception as e:
            log.debug(f"Heartbeat failed: {e} — will re-discover")
            server = None

        time.sleep(HEARTBEAT_INTERVAL)


# ===========================================================================
# SHARE UPKEEP
# ===========================================================================

def ensure_share_subfolder():
    """Create this machine's share subfolder if missing. Returns True if it
    exists afterwards. Never raises -- an unreachable share is just logged."""
    dest_folder = os.path.join(SERVER_WATCH_FOLDER, SHARE_SUBFOLDER)
    try:
        if os.path.isdir(dest_folder):
            return True
        if not os.path.isdir(SERVER_WATCH_FOLDER):
            return False
        os.makedirs(dest_folder, exist_ok=True)
        log.info(f"Created share folder: {dest_folder}")
        return True
    except OSError as e:
        log.warning(f"Could not create share folder {dest_folder}: {e}")
        return False


def mirror_log_to_share():
    """Copy client.log into this machine's share subfolder."""
    try:
        if os.path.isfile(LOG_PATH) and ensure_share_subfolder():
            shutil.copyfile(LOG_PATH, os.path.join(SERVER_WATCH_FOLDER, SHARE_SUBFOLDER, "client.log"))
    except OSError as e:
        log.debug(f"Log mirror failed: {e}")


def _share_upkeep_loop():
    """Background thread: keep the share subfolder in place, and mirror the
    log every LOG_MIRROR_INTERVAL seconds."""
    last_mirror = 0
    while True:
        ensure_share_subfolder()
        if time.time() - last_mirror >= LOG_MIRROR_INTERVAL:
            mirror_log_to_share()
            last_mirror = time.time()
        time.sleep(SHARE_UPKEEP_INTERVAL)


# ===========================================================================
# WATCHDOG
# ===========================================================================

WATCHED_EXTENSIONS = {".xml", ".do3"}


class ForwardHandler(FileSystemEventHandler):
    """
    Detects new .xml and .do3 files in the Dolphin folder (and re-saved .xml).
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
        return (os.path.splitext(filepath)[1].lower() in WATCHED_EXTENSIONS
                and os.path.basename(filepath).lower() not in DOLPHIN5_SKIP_FILENAMES)

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

    def on_modified(self, event):
        # Dolphin5 may re-save a race's .xml after an edit; resend it. .do3
        # stays create-only. Each write pushes the debounce back.
        path = event.src_path
        if not event.is_directory and self._should_handle(path) \
                and os.path.splitext(path)[1].lower() == ".xml":
            with self._lock:
                ctime = self._pending.get(path, (get_file_ctime(path), 0))[0]
                self._pending[path] = (ctime, time.time() + DEBOUNCE_SECONDS)

    def _process(self, filepath, ctime):
        if not os.path.exists(filepath):
            log.warning(f"File gone before processing: {filepath}")
            return
        filename = os.path.basename(filepath)
        # Dolphin5 writes both .xml and .do3 per race; CTSTracking only wants
        # its .xml. Dolphin4 only writes .do3, which is still needed.
        is_xml = filename.lower().endswith(".xml")
        if is_xml != dolphin5_running():
            reason = ("Dolphin5 running -- its .do3 stays local" if not is_xml
                      else "Dolphin5 not running -- not a Dolphin5 race result")
            log.info(f"Not forwarded: {filename} ({reason})")
            return
        if not wait_for_file_ready(filepath):
            log.error(f"Skipping {filename}: file never became ready (still empty or changing)")
            return
        dest_filename = build_dest_filename(filename, MACHINE_ID, ctime)
        log.info(f"Processing: {filename} -> {dest_filename}")
        copy_to_server(filepath, dest_filename)


# ===========================================================================
# FOLDER WATCHING WITH RETRY
# ===========================================================================

def _watch_folder_when_present(observer, handler, path, label):
    """
    Add a folder to the watchdog observer once it exists. A missing folder is
    normal (GEN7 machines have no C:\\CTSDolphin), so this never pops up and
    never creates the folder -- it logs once and keeps checking quietly.
    """
    def attempt():
        logged_missing = False
        while True:
            if os.path.isdir(path):
                try:
                    observer.schedule(handler, path, recursive=False)
                    log.info(f"Watching {label}: {path}")
                    return
                except Exception as e:
                    log.warning(f"Could not watch {label}: {e} — retrying in {DOLPHIN_FOLDER_RECHECK}s")
            elif not logged_missing:
                log.info(f"{label} folder not present: {path} — no files to forward "
                         f"(normal on a GEN7 machine); will start watching if it appears")
                logged_missing = True
            time.sleep(DOLPHIN_FOLDER_RECHECK)

    threading.Thread(target=attempt, daemon=True).start()


# ===========================================================================
# MAIN
# ===========================================================================

if __name__ == "__main__":
    # Single-instance guard — create a named mutex; if it already exists another
    # copy is running, so show a message and exit immediately.
    _mutex = ctypes.windll.kernel32.CreateMutexW(None, True, "CTSTrackerClient_SingleInstance")
    if ctypes.windll.kernel32.GetLastError() == 183:  # ERROR_ALREADY_EXISTS
        ctypes.windll.user32.MessageBoxW(
            0,
            "CTS Tracker Client is already running on this machine.",
            "CTS Tracker Client",
            0x30,  # MB_ICONWARNING | MB_OK
        )
        sys.exit(0)

    SHARE_SUBFOLDER = get_share_subfolder_name()

    log.info("=" * 50)
    log.info(f"CTS Tracker Client {CLIENT_VERSION} starting")
    log.info(f"  Machine ID:  {MACHINE_ID}  (from computer name)")
    log.info(f"  Running from: {INSTALL_DIR}")
    # Only the packaged .exe is checked -- `python client.py` from the repo is a dev run.
    if getattr(sys, "frozen", False) and not INSTALL_OK:
        log.warning(f"Running from {INSTALL_DIR}, expected {EXPECTED_INSTALL_DIR}")
        warn_popup(
            f"CTS Tracker Client is running from:\n\n  {INSTALL_DIR}\n\n"
            f"It should be installed at {EXPECTED_INSTALL_DIR} on this machine "
            f"(not run from Google Drive or another synced folder).\n\n"
            f"It will keep working, but please copy CTS_Client.exe to "
            f"{EXPECTED_INSTALL_DIR} and run it from there."
        )
    log.info(f"  Dolphin folder: {DOLPHIN_WATCH_FOLDER}")
    log.info(f"  Forwarding:  {os.path.join(SERVER_WATCH_FOLDER, SHARE_SUBFOLDER)}")
    if SHARE_SUBFOLDER.startswith("ipunknown_"):
        log.warning(f"No {TIMING_NETWORK_PREFIX}x address found -- is Ethernet connected?")
        warn_popup(
            f"No timing-network ({TIMING_NETWORK_PREFIX}x) address found on this machine.\n\n"
            f"Files will still be forwarded, to:\n  {SHARE_SUBFOLDER}\n\n"
            f"Check the Ethernet cable, then restart the client."
        )
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
    else:
        ensure_share_subfolder()

    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    threading.Thread(target=_share_upkeep_loop, daemon=True).start()

    handler = ForwardHandler()
    observer = Observer()
    observer.daemon = True
    observer.start()

    _watch_folder_when_present(observer, handler, DOLPHIN_WATCH_FOLDER, "Dolphin")

    log.info("Watching for files... (Ctrl+C to stop)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down.")
        observer.stop()

    observer.join()
