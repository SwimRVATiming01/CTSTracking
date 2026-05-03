"""
obs_control.py - OBS WebSocket v5 controller for two OBS instances.

Each instance has its own stream settings, scene selection, schedule, and early-start offset.
"""

import logging
import threading
import time
from datetime import datetime, timedelta

log = logging.getLogger("cts_tracker")

# Per-instance connection configs
_obs_configs = {
    1: {"host": "172.16.0.119", "port": 4455, "password": ""},
    2: {"host": "172.16.0.119", "port": 4456, "password": ""},
}

# Per-instance scheduled fire times (already offset-adjusted datetimes)
# Each entry: {"fire": datetime, "scene": str or None}
_scheduled = {1: None, 2: None}
_schedule_lock = threading.Lock()

# Per-instance last stream-settings applied timestamp
_settings_applied_at = {1: None, 2: None}

OBS_TIMEOUT  = 3       # seconds per connection attempt
OBS_SCENE    = "Intro" # scene to switch to before starting the stream
OBS_SCENE_DELAY = 5    # seconds to wait after scene switch before starting stream


def _make_client(instance_num):
    """Return an obsws_python ReqClient for the given instance. Caller must close it."""
    try:
        import obsws_python as obs
    except ImportError:
        raise RuntimeError("obsws-python not installed. Run: pip install obsws-python")
    cfg = _obs_configs[instance_num]
    return obs.ReqClient(
        host=cfg["host"],
        port=cfg["port"],
        password=cfg["password"],
        timeout=OBS_TIMEOUT,
    )


def _close(client):
    try:
        client.base_client.ws.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# STATUS
# ---------------------------------------------------------------------------

def get_status(instance_num):
    """
    Returns connection + streaming status for one OBS instance.
    Always returns a dict — never raises.
    """
    try:
        client = _make_client(instance_num)
        try:
            version = client.get_version()
            stream  = client.get_stream_status()
        finally:
            _close(client)
        return {
            "connected":   True,
            "obs_version": getattr(version, "obs_version", "?"),
            "streaming":   bool(getattr(stream, "output_active", False)),
        }
    except Exception as e:
        return {"connected": False, "streaming": False, "error": str(e)}


def get_configs():
    """Return host/port for both instances (passwords omitted)."""
    return {
        i: {"host": c["host"], "port": c["port"]}
        for i, c in _obs_configs.items()
    }


def get_scheduled_times():
    """Return {1: 'HH:MM:SS' or None, 2: 'HH:MM:SS' or None}."""
    with _schedule_lock:
        result = {}
        for i, entry in _scheduled.items():
            result[i] = entry["fire"].strftime("%H:%M:%S") if entry else None
        return result


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

def update_config(instance_num, host=None, port=None, password=None):
    """Update connection config for one OBS instance (in-memory only)."""
    cfg = _obs_configs[instance_num]
    if host     is not None: cfg["host"]     = host
    if port     is not None: cfg["port"]     = int(port)
    if password is not None: cfg["password"] = password


# ---------------------------------------------------------------------------
# STREAM SETTINGS
# ---------------------------------------------------------------------------

def set_stream_settings(instance_num, url, key):
    """
    Push RTMP URL and stream key to one OBS instance.
    Returns {"ok": bool, "error"?: str}.
    """
    try:
        client = _make_client(instance_num)
        try:
            client.set_stream_service_settings(
                "rtmp_custom",
                {
                    "server":   url,
                    "key":      key,
                    "use_auth": False,
                },
            )
        finally:
            _close(client)
        _settings_applied_at[instance_num] = datetime.now()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_settings_applied_at():
    """Return {1: 'HH:MM:SS' or None, 2: ...} for when settings were last pushed."""
    return {
        i: (t.strftime("%H:%M:%S") if t else None)
        for i, t in _settings_applied_at.items()
    }


# ---------------------------------------------------------------------------
# START STREAM
# ---------------------------------------------------------------------------

def start_stream(instance_num):
    """
    Switch to OBS_SCENE, wait OBS_SCENE_DELAY seconds, then start streaming.
    Returns {"ok": bool, "error"?: str}.
    """
    try:
        client = _make_client(instance_num)
        try:
            log.info(f"OBS {instance_num}: switching to scene '{OBS_SCENE}'")
            client.set_current_program_scene(OBS_SCENE)
        finally:
            _close(client)

        log.info(f"OBS {instance_num}: waiting {OBS_SCENE_DELAY}s before starting stream")
        time.sleep(OBS_SCENE_DELAY)

        client = _make_client(instance_num)
        try:
            client.start_stream()
        finally:
            _close(client)

        log.info(f"OBS {instance_num}: stream started")
        return {"ok": True}
    except Exception as e:
        log.warning(f"OBS {instance_num}: start_stream failed — {e}")
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# SCHEDULING
# ---------------------------------------------------------------------------

def schedule_stream_start(instance_num, target_time_str, offset_minutes=10):
    """
    Schedule one OBS instance to switch to OBS_SCENE and start streaming at
    (target_time - offset_minutes).

    target_time_str: "HH:MM" — the event/broadcast time.
    offset_minutes:  how many minutes before that time to actually start OBS.

    Raises ValueError for bad input or if the effective fire time is in the past.
    Returns {"fire_time": "HH:MM:SS", "event_time": "HH:MM:SS", "offset_minutes": int}.
    """
    now = datetime.now()

    # Normalize bare 3-4 digit times: 930 → 9:30, 1700 → 17:00
    s = target_time_str.strip()
    if s.isdigit():
        s = s.zfill(4)
        s = s[:-2] + ":" + s[-2:]

    parsed = None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            t = datetime.strptime(s, fmt)
            parsed = now.replace(
                hour=t.hour, minute=t.minute,
                second=getattr(t, "second", 0), microsecond=0,
            )
            break
        except ValueError:
            continue

    if parsed is None:
        raise ValueError(f"Invalid time format: {target_time_str!r}")

    offset_minutes = int(offset_minutes)
    fire_time = parsed - timedelta(minutes=offset_minutes)

    if fire_time <= now:
        raise ValueError(
            f"Effective start time ({fire_time.strftime('%H:%M:%S')}) is in the past. "
            f"Increase the scheduled time or reduce the offset."
        )

    entry = {"fire": fire_time}

    with _schedule_lock:
        _scheduled[instance_num] = entry

    def _wait_and_start(inst, target, entry_snap):
        fire = entry_snap["fire"]
        log.info(
            f"OBS {inst}: scheduled — fires at {fire.strftime('%H:%M:%S')} "
            f"({offset_minutes} min before {target.strftime('%H:%M:%S')})"
        )
        while True:
            with _schedule_lock:
                if _scheduled[inst] is not entry_snap:
                    log.info(f"OBS {inst}: schedule cancelled or superseded")
                    return
            if datetime.now() >= fire:
                log.info(f"OBS {inst}: fire time reached — starting stream")
                start_stream(inst)
                with _schedule_lock:
                    if _scheduled[inst] is entry_snap:
                        _scheduled[inst] = None
                return
            time.sleep(0.5)

    threading.Thread(
        target=_wait_and_start,
        args=(instance_num, parsed, entry),
        daemon=True,
    ).start()

    return {
        "fire_time":      fire_time.strftime("%H:%M:%S"),
        "event_time":     parsed.strftime("%H:%M:%S"),
        "offset_minutes": offset_minutes,
    }


def cancel_schedule(instance_num):
    """Cancel scheduled start for one OBS instance."""
    with _schedule_lock:
        _scheduled[instance_num] = None
    log.info(f"OBS {instance_num}: stream schedule cancelled")
