"""
routes.py - Flask app, dashboard HTML, and all API routes.
"""

import logging
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template_string, request, abort

import config
from database import (
    get_active_meet, get_all_meets, create_meet, set_active_meet,
    get_schedule, get_sessions, override_start_time, clear_override,
    reorder_heats, add_manual_heat,
    get_race_dashboard, get_full_log, get_current_heat_state,
    add_manual_race_entry, update_race_entry,
    get_pending_summary, get_ingestion_log,
    export_race_log_csv, snapshot_db,
)
from ingestion import (
    get_pending_schedule, approve_schedule, dismiss_pending_schedule,
    ingest_schedule_file,
)

log = logging.getLogger("cts_tracker")

app = Flask(__name__)


# ===========================================================================
# DASHBOARD HTML
# ===========================================================================

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>CTS Tracker</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: monospace; font-size: 12px; background: #1a1a2e; color: #e0e0e0; }

    /* ---- STICKY HEADER ---- */
    #sticky-top { position: sticky; top: 0; z-index: 50; }

    header { background: #16213e; padding: 8px 14px; display: flex; align-items: center;
             gap: 12px; border-bottom: 2px solid #0f3460; flex-wrap: wrap; }
    header h1 { font-size: 15px; color: #e94560; letter-spacing: 1px; white-space: nowrap; }
    .meet-name { color: #a0c4ff; font-size: 12px; }
    .status-bar { display: flex; gap: 8px; font-size: 11px; margin-left: auto; flex-wrap: wrap; align-items: center; }
    .status-pill { background: #0f3460; padding: 2px 7px; border-radius: 10px; white-space: nowrap; }
    .status-pill.warn { background: #8b4000; }

    /* NAV */
    nav { background: #16213e; padding: 5px 14px; display: flex; gap: 6px;
          border-bottom: 1px solid #0f3460; align-items: center; }
    #eta-bar { font-size: 11px; color: #6bff6b; display: none; }
    #eta-bar.show { display: inline; }
    .view-btn { border: none; padding: 3px 9px; border-radius: 4px; cursor: pointer;
                font-family: monospace; font-size: 11px; margin-left: auto; }
    #btn-schedule { background: #1a3a1a; color: #6bff6b; }
    #btn-schedule.active { background: #6bff6b; color: #0d1117; }
    #btn-log { background: #0f3460; color: #a0c4ff; }
    #btn-log.active { background: #a0c4ff; color: #0d1117; }
    #btn-restart { background: #3a1a1a; color: #ff6b6b; margin-left: 0; }
    #btn-restart:hover { background: #ff6b6b; color: #0d1117; }

    /* TABLE */
    body { overflow: hidden; }
    .container { overflow-x: auto; overflow-y: auto; height: calc(100vh - var(--header-height, 0px)); }
    table { width: 100%; border-collapse: collapse; }
    th { background: #0f3460; color: #a0c4ff; padding: 5px 6px; text-align: center;
         font-size: 10px; white-space: nowrap; position: sticky; top: 0; z-index: 10; }
    td { padding: 4px 6px; border-bottom: 1px solid #1e2a4a; text-align: center; white-space: nowrap; }
    td.left { text-align: left; }
    tr:hover td { background: #222; }
    tr.unmatched td { color: #555; }
    tr.pending-cts td { color: #ffd700; }

    /* Pool highlights */
    tr.current-p1 td { background: #1a3a2a !important; }
    tr.current-p2 td { background: #1a2a3a !important; }

    /* Lane cells */
    .lane-active  { background: #1a4a1a; color: #1a4a1a; font-weight: bold; border-radius: 3px; }
    .lane-empty   { background: #4a1a1a; color: #ff6b6b; border-radius: 3px; }
    .lane-unknown { color: #333; }

    /* Gap flag */
    td.gap-flag { color: #ff4444 !important; font-weight: bold; }
    td.gap-flag::after { content: " ⚠"; font-size: 9px; }

    /* Delta */
    .late   { color: #ff6b6b; font-weight: bold; }
    .early  { color: #6bff6b; }
    .ontime { color: #ffffff; }

    /* Badges */
    .badge { display:inline-block; padding:1px 4px; border-radius:3px; font-size:10px; }
    .badge-green  { background:#1a4a1a; color:#6bff6b; }
    .badge-yellow { background:#4a4a00; color:#ffd700; }
    .badge-gray   { background:#2a2a2a; color:#888; }

    /* Full log orphans */
    tr.orphan td { color: #ff8c00; font-style: italic; }

    /* Modal */
    #modal-overlay { display:none; position:fixed; top:0; left:0; width:100%; height:100%;
                     background:rgba(0,0,0,0.75); z-index:200; align-items:center; justify-content:center; }
    #modal-overlay.show { display:flex; }
    #modal { background:#16213e; border:2px solid #e94560; border-radius:8px; padding:22px;
             max-width:460px; width:90%; }
    #modal h2 { color:#e94560; margin-bottom:10px; font-size:14px; }
    #modal p  { color:#a0c4ff; margin-bottom:5px; font-size:12px; }
    .meet-info { background:#0f3460; padding:9px; border-radius:4px; margin:10px 0;
                 font-size:12px; line-height:1.9; }
    .meet-info b { color:#a0c4ff; }
    .modal-btn { border:none; padding:7px 14px; border-radius:4px; cursor:pointer;
                 font-family:monospace; font-size:12px; font-weight:bold;
                 margin-top:10px; margin-right:6px; }
    .btn-scrub   { background:#e94560; color:white; }
    .btn-keep    { background:#0f3460; color:#a0c4ff; }
    .btn-dismiss { background:#2a2a2a; color:#888; }
  </style>
</head>
<body>

<!-- Import Modal -->
<div id="modal-overlay">
  <div id="modal">
    <h2>&#x1F4CB; New Schedule Detected</h2>
    <p>A Meet Manager schedule CSV has been dropped into the schedule folder.</p>
    <div class="meet-info">
      <div><b>Meet:</b> <span id="modal-meet-name">&#8212;</span></div>
      <div><b>Date:</b> <span id="modal-meet-date">&#8212;</span></div>
      <div><b>File:</b> <span id="modal-filename">&#8212;</span></div>
    </div>
    <p>How would you like to proceed?</p>
    <button class="modal-btn btn-scrub"   onclick="approveSchedule(true)">Scrub Race Data &amp; Import</button>
    <button class="modal-btn btn-keep"    onclick="approveSchedule(false)">Keep Race Data &amp; Import</button>
    <button class="modal-btn btn-dismiss" onclick="dismissSchedule()">Dismiss</button>
  </div>
</div>

<!-- Sticky top: header + ETA bar + nav -->
<div id="sticky-top">
  <header>
    <h1>CTS TRACKER</h1>
    <span class="meet-name" id="meet-name">Loading...</span>
    <div class="status-bar">
      <span class="status-pill" id="pill-p1">Pool 1: &#8212;</span>
      <span class="status-pill" id="pill-p2">Pool 2: &#8212;</span>
      <span class="status-pill" id="pill-unmatched">Unmatched: &#8212;</span>
      <span class="status-pill" id="last-update">&#8212;</span>
    </div>
  </header>
  <nav>
    <div id="eta-bar"></div>
    <button class="view-btn" id="btn-schedule" onclick="setView('schedule')">Schedule</button>
    <button class="view-btn" id="btn-log"      onclick="setView('log')">Full Log</button>
    <button class="view-btn" id="btn-restart"  onclick="restartServer()">Restart Server</button>
  </nav>
</div>

<!-- Schedule View -->
<div class="container" id="schedule-view">
  <table>
    <thead>
      <tr>
        <th class="left">Event</th>
        <th>Heat</th>
        <th>Projected</th>
        <th>Late(+)<br>Early(-)</th>
        <th>1</th><th>2</th><th>3</th><th>4</th>
        <th>5</th><th>6</th><th>7</th><th>8</th>
        <th>CTS #</th>
        <th>Dolphin #</th>
        <th>Actual Start</th>
        <th>Missing Lanes</th>
        <th>Finish</th>
      </tr>
    </thead>
    <tbody id="race-table"></tbody>
  </table>
</div>

<!-- Full Log View -->
<div class="container" id="log-view" style="display:none">
  <table>
    <thead>
      <tr>
        <th>Event</th><th>Heat</th><th>CTS #</th><th>CTS Start</th>
        <th>Dolphin #</th><th>Dolphin Time</th>
        <th>Machine (CTS)</th><th>Machine (Dolphin)</th>
        <th>Matched</th><th>Ingested</th><th>Notes</th>
      </tr>
    </thead>
    <tbody id="log-table"></tbody>
  </table>
</div>

<script>
let currentView = 'schedule';
let lastEventId = null;

// ---------------------------------------------------------------------------
// VIEW TOGGLE
// ---------------------------------------------------------------------------
function setView(v) {
  currentView = v;
  document.getElementById('schedule-view').style.display = v === 'schedule' ? '' : 'none';
  document.getElementById('log-view').style.display      = v === 'log'      ? '' : 'none';
  document.getElementById('btn-schedule').classList.toggle('active', v === 'schedule');
  document.getElementById('btn-log').classList.toggle('active', v === 'log');
  if (v === 'log') loadFullLog();
}
setView('schedule');  // set initial active state

// ---------------------------------------------------------------------------
// MODAL
// ---------------------------------------------------------------------------
function checkPendingSchedule() {
  fetch('/api/schedule/pending')
    .then(r => r.json())
    .then(data => {
      if (data && data.filename) {
        document.getElementById('modal-meet-name').textContent = data.meet_name || '\u2014';
        document.getElementById('modal-meet-date').textContent = data.meet_date || '\u2014';
        document.getElementById('modal-filename').textContent  = data.filename  || '\u2014';
        document.getElementById('modal-overlay').classList.add('show');
      } else {
        document.getElementById('modal-overlay').classList.remove('show');
      }
    });
}

function approveSchedule(scrub) {
  fetch('/api/schedule/approve', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({scrub_races: scrub})
  }).then(() => {
    document.getElementById('modal-overlay').classList.remove('show');
    loadDashboard();
  });
}

function dismissSchedule() {
  fetch('/api/schedule/dismiss', {method: 'POST'})
    .then(() => document.getElementById('modal-overlay').classList.remove('show'));
}

// ---------------------------------------------------------------------------
// SCHEDULE DASHBOARD
// ---------------------------------------------------------------------------
function loadDashboard() {
  return fetch('/api/dashboard')
    .then(r => r.json())
    .then(data => {
      if (data.meet) document.getElementById('meet-name').textContent = data.meet.meet_name || '';

      // Final Heat ETA bar
      const eta = data.final_eta;
      const etaBar = document.getElementById('eta-bar');
      if (eta && eta.time) {
        const sign = eta.avg_delta > 0 ? '+' : '';
        etaBar.textContent =
          'Final Heat ETA: ' + eta.time +
          '  (projected ' + eta.projected + '  ' + sign + eta.avg_delta + ' min running avg)';
        etaBar.classList.add('show');
      } else {
        etaBar.classList.remove('show');
      }

      // Status pills
      const rows = data.rows || [];
      const p1 = rows.find(r => r.is_current_p1);
      const p2 = rows.find(r => r.is_current_p2);
      document.getElementById('pill-p1').textContent =
        p1 ? 'P1: E' + p1.event_id + 'H' + p1.heat + ' #' + p1.cts_race_num : 'Pool 1: \u2014';
      document.getElementById('pill-p2').textContent =
        p2 ? 'P2: E' + p2.event_id + 'H' + p2.heat + ' #' + p2.cts_race_num : 'Pool 2: \u2014';

      const um = (data.pending || {}).unmatched_log || 0;
      const umPill = document.getElementById('pill-unmatched');
      umPill.textContent = 'Unmatched: ' + um;
      umPill.classList.toggle('warn', um > 0);
      document.getElementById('last-update').textContent = new Date().toLocaleTimeString();

      // Render rows
      lastEventId = null;
      document.getElementById('race-table').innerHTML =
        rows.map(row => renderRow(row)).join('');
    });
}

function renderRow(row) {
  const hasRace = row.cts_race_num !== null && row.cts_race_num !== undefined;

  // Row class
  let cls = '';
  if      (row.is_next_heat)  cls = 'current-p1';
  else if (!hasRace)          cls = 'unmatched';
  else if (!row.matched)      cls = 'pending-cts';

  // Event — hide duplicate
  const showEv = row.event_id !== lastEventId;
  lastEventId = row.event_id;
  const evCell = '<td class="left">' + (showEv ? row.event_id : '') + '</td>';

  // Delta
  let delta = '\u2014';
  if (row.delta_minutes !== null && row.delta_minutes !== undefined) {
    const d = row.delta_minutes;
    const rounded = Math.round(d);
    const dc = rounded > 0 ? 'late' : rounded < 0 ? 'early' : 'ontime';
    delta = '<span class="' + dc + '">' + (rounded > 0 ? '+' : '') + rounded + '</span>';
  }

  // Lane cells
  const active = (hasRace && row.active_lanes)
    ? row.active_lanes.split(',').map(Number)
    : null;
  const lanes = [1,2,3,4,5,6,7,8].map(n => {
    if (active === null) return '<td class="lane-unknown">\u2014</td>';
    return active.includes(n)
      ? '<td class="lane-active">' + n + '</td>'
      : '<td class="lane-empty">' + n + '</td>';
  }).join('');

  // CTS # with gap flag
  const ctsCls = row.cts_gap_flag ? ' class="gap-flag"' : '';
  const ctsCell = '<td' + ctsCls + '>' + (hasRace ? row.cts_race_num : '\u2014') + '</td>';

  // Dolphin # with gap flag
  const dolCls = row.dolphin_gap_flag ? ' class="gap-flag"' : '';
  const dolCell = '<td' + dolCls + '>' +
    (row.dolphin_race_num !== null && row.dolphin_race_num !== undefined
      ? row.dolphin_race_num : '\u2014') + '</td>';

  // Finish = CTS file creation time
  let finish = '\u2014';
  if (row.cts_file_time) {
    const t = row.cts_file_time;
    finish = t.length >= 19 ? t.substring(11, 19) : t;
  }

  return '<tr class="' + cls + '">' +
    evCell +
    '<td>' + row.heat + '</td>' +
    '<td>' + (row.effective_start || '\u2014') + '</td>' +
    '<td>' + delta + '</td>' +
    lanes +
    ctsCell +
    dolCell +
    '<td>' + (row.cts_start_time || '\u2014') + '</td>' +
    '<td>' + (row.missing_lanes || '\u2014') + '</td>' +
    '<td>' + finish + '</td>' +
    '</tr>';
}

// ---------------------------------------------------------------------------
// FULL LOG
// ---------------------------------------------------------------------------
function loadFullLog() {
  fetch('/api/log')
    .then(r => r.json())
    .then(data => {
      document.getElementById('log-table').innerHTML =
        (data.rows || []).map(row => {
          const cls   = row.is_orphan ? 'orphan' : '';
          const notes = row.is_orphan
            ? '<span style="color:#ff8c00">orphan</span>'
            : row.manually_edited
              ? '<span style="color:#ffd700">manual</span>'
              : '';
          const dtime = row.dolphin_file_time
            ? row.dolphin_file_time.substring(11, 19) : '\u2014';
          return '<tr class="' + cls + '">' +
            '<td>' + (row.event_id ?? '\u2014') + '</td>' +
            '<td>' + (row.heat ?? '\u2014') + '</td>' +
            '<td>' + (row.cts_race_num ?? '\u2014') + '</td>' +
            '<td>' + (row.cts_start_time ?? '\u2014') + '</td>' +
            '<td>' + (row.dolphin_race_num ?? '\u2014') + '</td>' +
            '<td>' + dtime + '</td>' +
            '<td>' + (row.cts_source_machine ?? '\u2014') + '</td>' +
            '<td>' + (row.dolphin_source_machine ?? '\u2014') + '</td>' +
            '<td>' + (row.matched ? '&#10003;' : '\u2014') + '</td>' +
            '<td>' + (row.ingested_at ? row.ingested_at.substring(11, 19) : '\u2014') + '</td>' +
            '<td>' + notes + '</td>' +
            '</tr>';
        }).join('');
    });
}

// ---------------------------------------------------------------------------
// RESTART
// ---------------------------------------------------------------------------
function restartServer() {
  if (!confirm('Restart the server?')) return;
  document.getElementById('btn-restart').textContent = 'Restarting...';
  fetch('/admin/restart', {method: 'POST'})
    .then(() => {
      setTimeout(() => { location.reload(); }, 3000);
    })
    .catch(() => {
      setTimeout(() => { location.reload(); }, 3000);
    });
}

// ---------------------------------------------------------------------------
// POLL
// ---------------------------------------------------------------------------
function poll() {
  checkPendingSchedule();
  if (currentView === 'schedule') loadDashboard();
  else loadFullLog();
}

function updateHeaderHeight() {
  const h = document.getElementById('sticky-top').offsetHeight;
  document.documentElement.style.setProperty('--header-height', h + 'px');
}
updateHeaderHeight();
window.addEventListener('resize', updateHeaderHeight);

loadDashboard().then(() => updateHeaderHeight()).catch(() => updateHeaderHeight());
checkPendingSchedule();
setInterval(poll, {{ poll_interval }});
</script>
</body>
</html>
"""


# ===========================================================================
# HELPERS
# ===========================================================================

def _compute_final_eta(rows):
    """
    Compute ETA for the final heat based on the running average schedule delta.

    Takes the average delta_minutes across all heats that have been run,
    applies it to the projected start of the last scheduled heat.

    Returns dict with time, projected, avg_delta — or None if insufficient data.
    """
    deltas = [r["delta_minutes"] for r in rows if r.get("delta_minutes") is not None]
    if not deltas:
        return None

    avg_delta = round(sum(deltas) / len(deltas), 1)

    scheduled = [r for r in rows if r.get("effective_start")]
    if not scheduled:
        return None

    last_heat = max(scheduled, key=lambda r: r["heat_order"])
    projected = last_heat["effective_start"]  # "HH:MM"

    try:
        base = datetime.strptime(projected, "%H:%M")
        eta_dt = base + timedelta(minutes=avg_delta)
        eta_time = eta_dt.strftime("%I:%M %p").lstrip("0")
    except ValueError:
        return None

    return {
        "time":      eta_time,
        "projected": projected,
        "avg_delta": avg_delta,
    }


# ===========================================================================
# ROUTES — DASHBOARD
# ===========================================================================

@app.route("/")
def dashboard():
    return render_template_string(
        DASHBOARD_HTML,
        poll_interval=config.DASHBOARD_POLL_INTERVAL_MS
    )


# ===========================================================================
# ROUTES — API
# ===========================================================================

@app.route("/api/dashboard")
def api_dashboard():
    meet = get_active_meet()
    if not meet:
        return jsonify({"error": "No active meet", "rows": [], "meet": None, "pending": {}})
    session = request.args.get("session")
    rows = get_race_dashboard(meet["meet_id"], session)
    return jsonify({
        "meet":      meet,
        "rows":      rows,
        "pending":   get_pending_summary(),
        "final_eta": _compute_final_eta(rows),
    })


@app.route("/api/sessions")
def api_sessions():
    meet = get_active_meet()
    if not meet:
        return jsonify({"sessions": []})
    return jsonify({"sessions": get_sessions(meet["meet_id"])})


@app.route("/api/meets", methods=["GET"])
def api_meets():
    return jsonify(get_all_meets())


@app.route("/api/meets", methods=["POST"])
def api_create_meet():
    data = request.json or {}
    if not data.get("meet_id") or not data.get("meet_name"):
        abort(400, "meet_id and meet_name are required")
    ok = create_meet(
        data["meet_id"], data["meet_name"],
        data.get("meet_date"), data.get("location"),
        data.get("set_active", True)
    )
    return jsonify({"created": ok})


@app.route("/api/meets/<meet_id>/activate", methods=["POST"])
def api_activate_meet(meet_id):
    return jsonify({"activated": set_active_meet(meet_id)})


@app.route("/api/schedule", methods=["GET"])
def api_schedule():
    meet = get_active_meet()
    if not meet:
        return jsonify({"error": "No active meet", "rows": []})
    session = request.args.get("session")
    return jsonify({"rows": get_schedule(meet["meet_id"], session)})


@app.route("/api/schedule/override", methods=["POST"])
def api_override_start():
    data = request.json or {}
    meet = get_active_meet()
    if not meet:
        abort(400, "No active meet")
    ok = override_start_time(
        meet["meet_id"], data["session"], data["event_id"],
        data["heat"], data["new_time"]
    )
    return jsonify({"updated": ok})


@app.route("/api/schedule/override", methods=["DELETE"])
def api_clear_override():
    data = request.json or {}
    meet = get_active_meet()
    if not meet:
        abort(400, "No active meet")
    ok = clear_override(meet["meet_id"], data["session"], data["event_id"], data["heat"])
    return jsonify({"cleared": ok})


@app.route("/api/schedule/reorder", methods=["POST"])
def api_reorder():
    data = request.json or {}
    meet = get_active_meet()
    if not meet:
        abort(400, "No active meet")
    ok = reorder_heats(meet["meet_id"], data["session"], data["ordered_ids"])
    return jsonify({"reordered": ok})


@app.route("/api/schedule/heat", methods=["POST"])
def api_add_heat():
    data = request.json or {}
    meet = get_active_meet()
    if not meet:
        abort(400, "No active meet")
    ok = add_manual_heat(
        meet["meet_id"], data["session"], data["event_id"], data["event_name"],
        data["heat"], data.get("projected_start"), data.get("heat_label"), data.get("heat_type")
    )
    return jsonify({"added": ok})


@app.route("/api/race", methods=["POST"])
def api_add_race():
    data = request.json or {}
    meet = get_active_meet()
    if not meet:
        abort(400, "No active meet")
    race_id = add_manual_race_entry(
        meet["meet_id"], data["event_id"], data["heat"],
        data.get("cts_race_num"), data.get("cts_start_time"), data.get("dolphin_race_num")
    )
    return jsonify({"race_log_id": race_id})


@app.route("/api/race/<int:race_id>", methods=["PATCH"])
def api_update_race(race_id):
    data = request.json or {}
    ok = update_race_entry(race_id, **data)
    return jsonify({"updated": ok})


@app.route("/api/ingestion_log")
def api_ingestion_log():
    limit = int(request.args.get("limit", 100))
    return jsonify(get_ingestion_log(limit))


@app.route("/api/snapshot", methods=["POST"])
def api_snapshot():
    path = snapshot_db("manual")
    return jsonify({"snapshot": path})


@app.route("/api/pending")
def api_pending():
    return jsonify(get_pending_summary())


@app.route("/health")
def health():
    meet = get_active_meet()
    return jsonify({
        "status": "ok",
        "active_meet": meet["meet_id"] if meet else None,
        "time": datetime.now().isoformat(),
    })


@app.route("/api/log")
def api_full_log():
    meet = get_active_meet()
    if not meet:
        return jsonify({"error": "No active meet", "rows": []})
    return jsonify({"rows": get_full_log(meet["meet_id"])})


# ---------------------------------------------------------------------------
# PENDING SCHEDULE MODAL
# ---------------------------------------------------------------------------

@app.route("/api/schedule/pending")
def api_pending_schedule():
    return jsonify(get_pending_schedule() or {})


@app.route("/api/schedule/approve", methods=["POST"])
def api_approve_schedule():
    data = request.json or {}
    scrub = data.get("scrub_races", True)
    result = approve_schedule(scrub_races=scrub)
    return jsonify(result)


@app.route("/api/schedule/dismiss", methods=["POST"])
def api_dismiss_schedule():
    dismiss_pending_schedule()
    return jsonify({"status": "dismissed"})


# ---------------------------------------------------------------------------
# BITFOCUS COMPANION ENDPOINTS
# ---------------------------------------------------------------------------

@app.route("/api/companion/pool1")
def api_companion_pool1():
    meet = get_active_meet()
    if not meet:
        return jsonify({"active": False})
    state = get_current_heat_state(meet["meet_id"])
    return jsonify(state.get("pool1", {"active": False}))


@app.route("/api/companion/pool2")
def api_companion_pool2():
    meet = get_active_meet()
    if not meet:
        return jsonify({"active": False})
    state = get_current_heat_state(meet["meet_id"])
    return jsonify(state.get("pool2", {"active": False}))


@app.route("/api/companion")
def api_companion_both():
    """Returns state for both pools in one call."""
    meet = get_active_meet()
    if not meet:
        return jsonify({"pool1": {"active": False}, "pool2": {"active": False}})
    return jsonify(get_current_heat_state(meet["meet_id"]))


# ---------------------------------------------------------------------------
# ADMIN
# ---------------------------------------------------------------------------

@app.route("/admin/restart", methods=["POST"])
def admin_restart():
    """Restart the server process. Picks up any code changes."""
    def _do_restart():
        time.sleep(1)  # let Flask finish sending the response
        subprocess.Popen([sys.executable] + sys.argv)
        os._exit(0)
    threading.Thread(target=_do_restart, daemon=True).start()
    log.info("Server restart requested via dashboard")
    return jsonify({"status": "restarting"})


# ---------------------------------------------------------------------------
# RACE LOG EXPORT
# ---------------------------------------------------------------------------

@app.route("/api/export/race_log")
def api_export_race_log():
    meet = get_active_meet()
    if not meet:
        abort(400, "No active meet")
    path = export_race_log_csv(meet["meet_id"])
    return jsonify({"exported": path})
