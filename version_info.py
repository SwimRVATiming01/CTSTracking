"""
version_info.py - Reports which git commit this running instance is on.

Purpose: with multiple instances of this codebase potentially running at
once (production server, shadow-test instances, client.py machines), it's
otherwise easy to lose track of whether a given running process actually
picked up a specific fix. Shells out to git rather than hand-maintaining a
version number, since git already tracks this accurately.
"""

import logging
import subprocess

import config

log = logging.getLogger("cts_tracker")

_cached = None


def get_git_version():
    """
    Returns {"commit": "<short hash>" or None, "dirty": bool or None}.
    Cached after first call — the running process's commit/dirty state
    can't change without a restart anyway.
    """
    global _cached
    if _cached is not None:
        return _cached

    commit = None
    dirty = None
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=config.BASE_DIR, stderr=subprocess.DEVNULL, text=True,
        ).strip()
        status = subprocess.check_output(
            ["git", "status", "--porcelain"],
            cwd=config.BASE_DIR, stderr=subprocess.DEVNULL, text=True,
        )
        dirty = bool(status.strip())
    except Exception as e:
        log.warning(f"Could not determine git version info: {e}")

    _cached = {"commit": commit, "dirty": dirty}
    return _cached
