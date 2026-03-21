"""
cts_tracker.py - CTS Meet Tracking System
==========================================
Entry point. Initializes logging, database, watchdog, and Flask server.

Run with:
    python cts_tracker.py

Requires:
    pip install flask watchdog
"""

import logging
import threading

import config
from database import init_db, _snapshot_scheduler
from watchdog_monitor import start_watchdog
from routes import app

# ===========================================================================
# LOGGING
# ===========================================================================

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        *(
            [logging.FileHandler(config.LOG_FILE)]
            if config.LOG_TO_FILE
            else []
        ),
    ],
)
log = logging.getLogger("cts_tracker")
logging.getLogger("werkzeug").setLevel(logging.ERROR)


# ===========================================================================
# STARTUP
# ===========================================================================

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("CTS Tracker starting up")
    log.info("=" * 60)

    # Initialize database
    init_db()

    # Start watchdog
    start_watchdog()

    # Start snapshot scheduler in background (only if enabled)
    if config.SNAPSHOT_INTERVAL_MINUTES > 0:
        snap_thread = threading.Thread(target=_snapshot_scheduler, daemon=True)
        snap_thread.start()
        log.info(f"Snapshot scheduler started (every {config.SNAPSHOT_INTERVAL_MINUTES} min)")
    else:
        log.info("Snapshot scheduler disabled — snapshots taken on schedule import only")

    # Start Flask
    log.info(f"Dashboard available at http://{config.FLASK_HOST}:{config.FLASK_PORT}")
    log.info(f"On other LAN machines: http://<this-machine-ip>:{config.FLASK_PORT}")
    app.run(
        host=config.FLASK_HOST,
        port=config.FLASK_PORT,
        debug=config.FLASK_DEBUG,
        use_reloader=False,   # must be False when running background threads
    )
