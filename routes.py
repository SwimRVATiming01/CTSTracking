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
import csv

from database import (
    get_active_meet, get_all_meets, create_meet, set_active_meet,
    get_schedule, get_sessions, override_start_time, clear_override,
    reorder_heats, add_manual_heat,
    get_race_dashboard, get_full_log, get_current_heat_state,
    add_manual_race_entry, update_race_entry,
    get_pending_summary, get_ingestion_log,
    export_race_log_csv, snapshot_db, get_snapshots,
)
from ingestion import (
    get_pending_schedule, approve_schedule, dismiss_pending_schedule,
    ingest_schedule_file,
)

log = logging.getLogger("cts_tracker")

app = Flask(__name__)

# Companion-controlled current heat overrides (None = use auto-detection)
_companion_p1 = None  # {"event_id": str, "heat": str}
_companion_p2 = None


# ===========================================================================
# DASHBOARD HTML
# ===========================================================================

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>CTS Tracker</title>
  <link rel="icon" type="image/png" href="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAACXBIWXMAAB7CAAAewgFu0HU+AAACYklEQVRYhe3XS4jPURQH8M9vjPHKYyavotiQPBZIYpBYSpJHJhsLlKbYsVA2XhFFkpVIEhsJk0d51VBCUeMVkmKSvIqZDKaxuPdnrn9/ozT/LMyp2+/ec07nfH/n9bu/rK2tzb+ksn/qvQtAFwCUZ2ffl8LuQNxCT9yPqyGux/jptLwU3jEJI+N+KOYmsuYIYj8OZs68KxEGhzEZo9CjiPwDqkoVAVgRn4MxFgNwQIgInKd0KUjpTVxVGBJ577GF0nRBf4xBVsC/mvA24UEpAMzBCzzEZSH8sBzjEr07+aazAZwWIgCzsVoowA0Fvuqj7ujOBtCn4NwLNZhQwO+G+WjobADbkv1HXMehDvS7l2IOTMMgXEI1FuBKXOMxS4hAPe7lADJMjcJeeCdMq3rMRG+cEopqRnTUhAsYFp22og23UYG+QhQG4pNQC41owTx8x818DuzDGsWLci/WRZArsTWRZViL9UX0P2I7diSy6zEKeaG2lgk9WxudN6MOL6PCU1TGfU3y9jktxfTkfEb4DvyOqhPn0K08MnNqFXK1RwhtIy5GWS1GFBjcIEy4nE7gaAcA3uIaFuWMMslQEPK2C8cisGl4lKAfXmBwYgLqM0Z34BwOYjGOpwDuCqF8nigOwipsjMotBYbyq3SmfbzWCVHriJri820KAF4JBbYMm4VUEKp5huQCIVTvySLGz/3BeVEqw0KhOuuE3FT6tRsq0C85P8NOfE14n3HjbwCUCzmB7liSGMyEVjqCKUIv98Vu4br1WnuHvMITfBGi1xxtfIvrS7SXg27J+VnXn1EXgP8ewA8IEYfODztEYQAAAABJRU5ErkJggg==">
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
    #pill-p1 { background: #1a4a1a; color: #6bff6b; }
    #pill-p2 { background: #4a1a1a; color: #ff6b6b; }

    /* NAV */
    nav { background: #16213e; padding: 5px 14px; display: flex; gap: 6px;
          border-bottom: 1px solid #0f3460; align-items: center; }
    #eta-bar { font-size: 11px; display: none; }
    #eta-bar.show { display: inline; }
    .view-btn { border: none; padding: 3px 9px; border-radius: 4px; cursor: pointer;
                font-family: monospace; font-size: 11px; margin-left: auto; }
    #btn-schedule { background: #1a3a1a; color: #6bff6b; }
    #btn-schedule.active { background: #6bff6b; color: #0d1117; }
    #btn-log { background: #0f3460; color: #a0c4ff; }
    #btn-log.active { background: #a0c4ff; color: #0d1117; }
    #btn-reorder { background: #0f3460; color: #a0c4ff; }
    #btn-reorder.active { background: #a0c4ff; color: #0d1117; }
    #btn-restart { background: #3a1a1a; color: #ff6b6b; margin-left: 0; }
    #btn-restart:hover { background: #ff6b6b; color: #0d1117; }

    /* Reorder view */
    .reorder-save { background:#0f3460; color:#a0c4ff; border:none; padding:5px 14px;
                    border-radius:4px; cursor:pointer; font-family:monospace; font-size:12px;
                    margin:10px 14px 6px; display:block; }
    .reorder-save:hover { background:#a0c4ff; color:#0d1117; }
    .drag-handle { cursor: grab; color: #555; padding: 0 6px; user-select: none; }
    tr.drag-over td { background: #1a3a5a !important; }

    /* TABLE */
    body { overflow: hidden; }
    .container { overflow-x: auto; overflow-y: auto; height: calc(100vh - var(--header-height, 0px)); }
    table { width: 100%; border-collapse: collapse; margin-bottom: 33vh; }
    th { background: #0f3460; color: #a0c4ff; padding: 5px 6px; text-align: center;
         font-size: 10px; white-space: nowrap; position: sticky; top: 0; z-index: 10; }
    td { padding: 4px 6px; border-bottom: 1px solid #1e2a4a; text-align: center; white-space: nowrap; }
    td.left { text-align: left; }
    tr:hover td { background: #222; }
    tr.unmatched td { color: #555; }
    tr.pending-cts td { color: #ffd700; }

    /* Pool highlights */
    tr.current-p1 td { background: #00c800 !important; color: #0d1117 !important; }
    tr.current-p2 td { background: #00b4ff !important; color: #0d1117 !important; }
    tr.heat-one td { background: #2b2b4d; color: #ffffff; }

    /* Lane cells — !important so they win over row highlight backgrounds */
    .lane-active  { background: #1a4a1a !important; color: #1a4a1a !important; font-weight: bold; border-radius: 3px; }
    .lane-empty   { background: #4a1a1a !important; color: #ff6b6b !important; border-radius: 3px; }
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

    /* History view */
    #btn-history { background: #0f3460; color: #a0c4ff; }
    #btn-history.active { background: #a0c4ff; color: #0d1117; }
    .history-toolbar { padding: 8px 14px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .history-select { background: #0f3460; border: 1px solid #1e2a4a; color: #e0e0e0;
                      font-family: monospace; font-size: 12px; padding: 4px 8px;
                      border-radius: 4px; min-width: 220px; }
    .history-select:focus { outline: 1px solid #a0c4ff; }


    /* Modal */
    .modal-overlay { display:none; position:fixed; top:0; left:0; width:100%; height:100%;
                     background:rgba(0,0,0,0.75); z-index:200; align-items:center; justify-content:center; }
    .modal-overlay.show { display:flex; }
    .modal-box { background:#16213e; border:2px solid #e94560; border-radius:8px; padding:22px;
             max-width:460px; width:90%; }
    .modal-box h2 { color:#e94560; margin-bottom:10px; font-size:14px; }
    .modal-box p  { color:#a0c4ff; margin-bottom:5px; font-size:12px; }
    .meet-info { background:#0f3460; padding:9px; border-radius:4px; margin:10px 0;
                 font-size:12px; line-height:1.9; }
    .meet-info b { color:#a0c4ff; }
    .modal-btn { border:none; padding:7px 14px; border-radius:4px; cursor:pointer;
                 font-family:monospace; font-size:12px; font-weight:bold;
                 margin-top:10px; margin-right:6px; }
    .btn-scrub   { background:#e94560; color:white; }
    .btn-keep    { background:#0f3460; color:#a0c4ff; }
    .btn-dismiss { background:#2a2a2a; color:#888; }
    .btn-add     { background:#1a3a1a; color:#6bff6b; }

    /* Form inputs */
    .modal-form { display:grid; grid-template-columns:1fr 1fr; gap:7px; margin:12px 0; }
    .modal-form label { color:#a0c4ff; font-size:11px; }
    .modal-form input { background:#0f3460; border:1px solid #1e2a4a; border-radius:3px;
                        color:#e0e0e0; font-family:monospace; font-size:12px;
                        padding:4px 7px; width:100%; }
    .modal-form .full-width { grid-column: 1 / -1; }
    .modal-form input:focus { outline:1px solid #a0c4ff; }
  </style>
</head>
<body>

<!-- Import Modal -->
<div id="modal-overlay" class="modal-overlay">
  <div class="modal-box">
    <h2>&#x1F4CB; New Schedule Detected</h2>
    <p>A Meet Manager schedule CSV has been dropped into the schedule folder.</p>
    <div class="meet-info">
      <div><b>Meet:</b> <span id="modal-meet-name">&#8212;</span></div>
      <div><b>Date:</b> <span id="modal-meet-date">&#8212;</span></div>
      <div><b>File:</b> <span id="modal-filename">&#8212;</span></div>
    </div>
    <p>How would you like to proceed?</p>
    <button class="modal-btn btn-scrub"   onclick="approveSchedule('scrub')">Scrub Race Data &amp; Import</button>
    <button class="modal-btn btn-keep"    onclick="approveSchedule('keep')">Keep Race Data &amp; Import</button>
    <button class="modal-btn btn-add"     onclick="approveSchedule('append')">Append to Schedule</button>
    <button class="modal-btn btn-dismiss" onclick="dismissSchedule()">Dismiss</button>
  </div>
</div>

<!-- Add Heat Modal -->
<div id="add-heat-overlay" class="modal-overlay">
  <div class="modal-box">
    <h2>+ Add Schedule Entry</h2>
    <div class="modal-form">
      <div>
        <label>Event #</label>
        <input id="ah-event" type="text" placeholder="e.g. 22">
      </div>
      <div>
        <label>Heat #</label>
        <input id="ah-heat" type="text" placeholder="e.g. 3">
      </div>
      <div class="full-width">
        <label>Event Name</label>
        <input id="ah-name" type="text" placeholder="e.g. Men 200 Butterfly">
      </div>
      <div>
        <label>Projected Start (HH:MM)</label>
        <input id="ah-start" type="text" placeholder="e.g. 09:30">
      </div>
      <div>
        <label>Session</label>
        <input id="ah-session" type="text" placeholder="1" value="1">
      </div>
    </div>
    <div id="ah-error" style="color:#ff6b6b;font-size:11px;min-height:16px;"></div>
    <button class="modal-btn btn-add"     onclick="submitAddHeat()">Add Entry</button>
    <button class="modal-btn btn-dismiss" onclick="closeAddHeat()">Cancel</button>
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
      <span class="status-pill" id="last-update">&#8212;</span>
    </div>
  </header>
  <nav>
    <div id="eta-bar"></div>
    <button class="view-btn" id="btn-schedule" onclick="setView('schedule')">Schedule</button>
    <button class="view-btn" id="btn-log"      onclick="setView('log')">Full Log</button>
    <button class="view-btn" id="btn-reorder"  onclick="setView('reorder')">Reorder</button>
    <button class="view-btn" id="btn-history"  onclick="setView('history')">History</button>
    <button class="view-btn" id="btn-add-heat" onclick="openAddHeat()" style="background:#1a3a1a;color:#6bff6b;">+ Add Heat</button>
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
        <th>Finish</th>
      </tr>
    </thead>
    <tbody id="race-table"></tbody>
  </table>
</div>

<!-- Reorder View -->
<div class="container" id="reorder-view" style="display:none">
  <div style="padding:8px 14px 4px; display:flex; gap:8px;">
    <button class="reorder-save" style="margin:0;" onclick="sortByEventHeat()">&#8597; Sort by Event &rarr; Heat</button>
  </div>
  <table>
    <thead>
      <tr>
        <th style="width:36px"></th>
        <th class="left">Event</th>
        <th>Heat</th>
        <th class="left">Event Name</th>
        <th>Projected</th>
        <th>CTS #</th>
      </tr>
    </thead>
    <tbody id="reorder-table"></tbody>
  </table>
</div>

<!-- History View -->
<div class="container" id="history-view" style="display:none">
  <div class="history-toolbar">
    <select id="history-snapshot-select" class="history-select" style="min-width:380px"
            onchange="onSnapshotChange(this.value)">
      <option value="">-- Select a snapshot --</option>
    </select>
    <button class="reorder-save" style="margin:0;" id="btn-export-csv"
            onclick="exportHistoryCSV()" disabled>Export CSV</button>
    <span id="history-meet-info" style="color:#888;font-size:11px;"></span>
  </div>
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
        <th>Dolphin<br>Dataset</th>
        <th>Actual Start</th>
        <th>Finish</th>
      </tr>
    </thead>
    <tbody id="history-table"></tbody>
  </table>
</div>

<!-- Full Log View -->
<div class="container" id="log-view" style="display:none">
  <table>
    <thead>
      <tr>
        <th>Time</th><th>Type</th><th>Machine</th>
        <th>Event</th><th>Heat</th><th>CTS #</th><th>Dolphin #</th>
        <th>Start</th><th>File</th><th>Status</th>
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
  if (currentView === 'reorder' && v !== 'reorder' && reorderRows.length > 0) saveReorder();
  currentView = v;
  document.getElementById('schedule-view').style.display = v === 'schedule' ? '' : 'none';
  document.getElementById('log-view').style.display      = v === 'log'      ? '' : 'none';
  document.getElementById('reorder-view').style.display  = v === 'reorder'  ? '' : 'none';
  document.getElementById('history-view').style.display  = v === 'history'  ? '' : 'none';
  document.getElementById('btn-schedule').classList.toggle('active', v === 'schedule');
  document.getElementById('btn-log').classList.toggle('active', v === 'log');
  document.getElementById('btn-reorder').classList.toggle('active', v === 'reorder');
  document.getElementById('btn-history').classList.toggle('active', v === 'history');
  if (v === 'log')     loadFullLog();
  if (v === 'reorder') loadReorderView();
  if (v === 'history') loadSnapshots();
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

function approveSchedule(mode) {
  const body = mode === 'append' ? {append: true}
             : mode === 'keep'   ? {scrub_races: false}
             :                     {scrub_races: true};
  fetch('/api/schedule/approve', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
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
// ADD HEAT MODAL
// ---------------------------------------------------------------------------
function openAddHeat() {
  document.getElementById('ah-event').value   = '';
  document.getElementById('ah-heat').value    = '';
  document.getElementById('ah-name').value    = '';
  document.getElementById('ah-start').value   = '';
  document.getElementById('ah-session').value = '1';
  document.getElementById('ah-error').textContent = '';
  // Pre-populate session from active schedule
  fetch('/api/sessions')
    .then(r => r.json())
    .then(data => {
      const sessions = data.sessions || [];
      if (sessions.length > 0)
        document.getElementById('ah-session').value = sessions[0];
    });
  document.getElementById('add-heat-overlay').classList.add('show');
  document.getElementById('ah-event').focus();
}

function closeAddHeat() {
  document.getElementById('add-heat-overlay').classList.remove('show');
}

function submitAddHeat() {
  const event   = document.getElementById('ah-event').value.trim();
  const heat    = document.getElementById('ah-heat').value.trim();
  const name    = document.getElementById('ah-name').value.trim();
  const start   = document.getElementById('ah-start').value.trim();
  const session = document.getElementById('ah-session').value.trim() || '1';
  const errEl   = document.getElementById('ah-error');

  if (!event || !heat) {
    errEl.textContent = 'Event # and Heat # are required.';
    return;
  }

  fetch('/api/schedule/heat', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      event_id: event, heat: heat, event_name: name,
      projected_start: start || null, session: session
    })
  })
  .then(r => r.json())
  .then(data => {
    if (data.added) {
      closeAddHeat();
      loadDashboard();
    } else {
      errEl.textContent = 'Failed to add entry.';
    }
  })
  .catch(() => { errEl.textContent = 'Request failed.'; });
}

// ---------------------------------------------------------------------------
// SCHEDULE DASHBOARD
// ---------------------------------------------------------------------------
function loadDashboard() {
  return fetch('/api/dashboard')
    .then(r => r.json())
    .then(data => {
      if (data.meet) {
        const raw = data.meet.meet_name || '';
        document.getElementById('meet-name').textContent = raw.replace(/\s*-\s*\d{4}-\d{2}-\d{2}\s+to\s+\d{4}-\d{2}-\d{2}\s*$/, '');
      }

      // Final Heat ETA bar
      const eta = data.final_eta;
      const etaBar = document.getElementById('eta-bar');
      if (eta && eta.time) {
        const sign = eta.avg_delta > 0 ? '+' : '';
        etaBar.textContent =
          'Final Heat Start: ' + eta.time + '  (' + sign + eta.avg_delta + ' min)';
        etaBar.style.color = eta.avg_delta > 0 ? '#ff6b6b' : eta.avg_delta < 0 ? '#6bff6b' : '#ffffff';
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

      document.getElementById('last-update').textContent = new Date().toLocaleTimeString();

      // Render rows
      lastEventId = null;
      document.getElementById('race-table').innerHTML =
        rows.map(row => renderRow(row)).join('');
    });
}

function renderRow(row) {
  const hasRace = row.cts_race_num !== null && row.cts_race_num !== undefined;

  // Row class — pool current-heat highlights take priority over heat-one
  let cls = '';
  if      (row.is_next_heat)             cls = 'current-p1';
  else if (row.is_next_heat_p2)          cls = 'current-p2';
  else if (String(row.heat) === '1')     cls = 'heat-one';
  else if (!hasRace)                     cls = 'unmatched';

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
          const time    = row.ingested_at ? row.ingested_at.substring(11, 19) : '\u2014';
          const type    = row.file_type ? row.file_type.toUpperCase() : '\u2014';
          const typeCls = row.file_type === 'cts' ? 'color:#a0c4ff' : row.file_type === 'dolphin' ? 'color:#ffd700' : '';
          const status  = row.status === 'ok'
            ? '<span style="color:#6bff6b">ok</span>'
            : '<span style="color:#ff6b6b">' + (row.error_message || 'error') + '</span>';
          const fname   = row.filename ? row.filename.substring(0, 40) : '\u2014';
          return '<tr>' +
            '<td>' + time + '</td>' +
            '<td style="' + typeCls + '">' + type + '</td>' +
            '<td>' + (row.source_machine ?? '\u2014') + '</td>' +
            '<td>' + (row.event_id ?? '\u2014') + '</td>' +
            '<td>' + (row.heat ?? '\u2014') + '</td>' +
            '<td>' + (row.cts_race_num ?? '\u2014') + '</td>' +
            '<td>' + (row.dolphin_race_num ?? '\u2014') + '</td>' +
            '<td>' + (row.cts_start_time ?? '\u2014') + '</td>' +
            '<td class="left" style="font-size:10px">' + fname + '</td>' +
            '<td>' + status + '</td>' +
            '</tr>';
        }).join('');
    });
}

// ---------------------------------------------------------------------------
// RESTART
// ---------------------------------------------------------------------------
function restartServer() {
  if (!confirm('Restart the server?')) return;
  const btn = document.getElementById('btn-restart');
  btn.textContent = 'Restarting...';
  btn.disabled = true;
  fetch('/admin/restart', {method: 'POST'}).catch(() => {});
  // Poll /health until the server responds again, then reload
  let attempts = 0;
  function waitForServer() {
    fetch('/health').then(r => {
      if (r.ok) { location.reload(); }
      else { if (++attempts < 30) setTimeout(waitForServer, 1000); else location.reload(); }
    }).catch(() => {
      if (++attempts < 30) setTimeout(waitForServer, 1000); else location.reload();
    });
  }
  setTimeout(waitForServer, 2000); // give the old process 2s to exit first
}

// ---------------------------------------------------------------------------
// REORDER
// ---------------------------------------------------------------------------
let reorderRows = [];

function loadReorderView() {
  fetch('/api/dashboard')
    .then(r => r.json())
    .then(data => {
      reorderRows = (data.rows || []).map(r => ({
        id:         r.schedule_id,
        event_id:   r.event_id,
        heat:       r.heat,
        event_name: r.event_name,
        projected:  r.effective_start,
        cts_race_num: r.cts_race_num,
      }));
      renderReorderTable();
    });
}

let dragSrcIndex = null;

function renderReorderTable() {
  document.getElementById('reorder-table').innerHTML = reorderRows.map((row, i) => {
    return '<tr draggable="true" data-index="' + i + '" ' +
      'ondragstart="onDragStart(event,' + i + ')" ' +
      'ondragover="onDragOver(event)" ' +
      'ondragleave="onDragLeave(event)" ' +
      'ondrop="onDrop(event,' + i + ')" ' +
      'ondragend="onDragEnd(event)">' +
      '<td><span class="drag-handle">&#9776;</span></td>' +
      '<td class="left">' + row.event_id + '</td>' +
      '<td>' + row.heat + '</td>' +
      '<td class="left">' + (row.event_name || '\u2014') + '</td>' +
      '<td>' + (row.projected || '\u2014') + '</td>' +
      '<td>' + (row.cts_race_num ?? '\u2014') + '</td>' +
      '</tr>';
  }).join('');
}

function onDragStart(e, i) {
  dragSrcIndex = i;
  e.dataTransfer.effectAllowed = 'move';
}

function onDragOver(e) {
  e.preventDefault();
  e.dataTransfer.dropEffect = 'move';
  e.currentTarget.classList.add('drag-over');
}

function onDragLeave(e) {
  e.currentTarget.classList.remove('drag-over');
}

function onDrop(e, i) {
  e.preventDefault();
  e.currentTarget.classList.remove('drag-over');
  if (dragSrcIndex === null || dragSrcIndex === i) return;
  const moved = reorderRows.splice(dragSrcIndex, 1)[0];
  reorderRows.splice(i, 0, moved);
  dragSrcIndex = null;
  renderReorderTable();
}

function onDragEnd(e) {
  dragSrcIndex = null;
  document.querySelectorAll('#reorder-table tr').forEach(r => r.classList.remove('drag-over'));
}

function saveReorder() {
  fetch('/api/schedule/reorder', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ordered_ids: reorderRows.map(r => r.id)})
  })
  .then(r => r.json())
  .then(() => loadReorderView());
}

function sortByEventHeat() {
  reorderRows.sort((a, b) => {
    const evA = parseFloat(a.event_id) || 0;
    const evB = parseFloat(b.event_id) || 0;
    if (evA !== evB) return evA - evB;
    const hA = parseFloat(a.heat) || 0;
    const hB = parseFloat(b.heat) || 0;
    return hA - hB;
  });
  renderReorderTable();
  saveReorder();
}

// ---------------------------------------------------------------------------
// HISTORY  (reads from snapshot .db files)
// ---------------------------------------------------------------------------
let historyMeetId   = null;
let historySnapFile = null;

function loadSnapshots() {
  fetch('/api/snapshots')
    .then(r => r.json())
    .then(snaps => {
      const sel  = document.getElementById('history-snapshot-select');
      const prev = sel.value;
      sel.innerHTML = '<option value="">-- Select a snapshot --</option>';
      snaps.forEach(s => {
        if (!s.exists) return;
        const opt = document.createElement('option');
        opt.value = s.snapshot_file;
        opt.dataset.meetId = s.meet_id || '';
        const kb   = s.size_bytes ? ' (' + Math.round(s.size_bytes / 1024) + ' KB)' : '';
        const name = s.meet_name ? s.meet_name + '  \u2014  ' : '';
        opt.textContent = name + s.local_time + kb;
        sel.appendChild(opt);
      });
      if (prev) { sel.value = prev; onSnapshotChange(prev); }
    });
}

function onSnapshotChange(filename) {
  historySnapFile = filename || null;
  historyMeetId   = null;
  document.getElementById('history-table').innerHTML = '';
  document.getElementById('history-meet-info').textContent = '';
  document.getElementById('btn-export-csv').disabled = true;
  if (!filename) return;

  const sel    = document.getElementById('history-snapshot-select');
  const meetId = sel.options[sel.selectedIndex].dataset.meetId;
  if (meetId) loadHistoryDashboard(meetId);
}

function loadHistoryDashboard(meetId) {
  if (!meetId || !historySnapFile) {
    document.getElementById('history-table').innerHTML = '';
    document.getElementById('history-meet-info').textContent = '';
    document.getElementById('btn-export-csv').disabled = true;
    historyMeetId = null;
    return;
  }
  historyMeetId = meetId;
  fetch('/api/snapshots/' + encodeURIComponent(historySnapFile) +
        '/dashboard/' + encodeURIComponent(meetId))
    .then(r => r.json())
    .then(data => {
      const meet = data.meet || {};
      document.getElementById('history-meet-info').textContent =
        [meet.location, meet.meet_date].filter(Boolean).join(' \u2014 ');
      document.getElementById('btn-export-csv').disabled = false;
      let lastEv = null;
      document.getElementById('history-table').innerHTML =
        (data.rows || []).map(row => {
          const hasRace = row.cts_race_num !== null && row.cts_race_num !== undefined;
          let cls = String(row.heat) === '1' ? 'heat-one' : (!hasRace ? 'unmatched' : '');
          const showEv = row.event_id !== lastEv;
          lastEv = row.event_id;
          let delta = '\u2014';
          if (row.delta_minutes !== null && row.delta_minutes !== undefined) {
            const rounded = Math.round(row.delta_minutes);
            const dc = rounded > 0 ? 'late' : rounded < 0 ? 'early' : 'ontime';
            delta = '<span class="' + dc + '">' + (rounded > 0 ? '+' : '') + rounded + '</span>';
          }
          const active = (hasRace && row.active_lanes)
            ? row.active_lanes.split(',').map(Number) : null;
          const lanes = [1,2,3,4,5,6,7,8].map(n => {
            if (active === null) return '<td class="lane-unknown">\u2014</td>';
            return active.includes(n)
              ? '<td class="lane-active">' + n + '</td>'
              : '<td class="lane-empty">' + n + '</td>';
          }).join('');
          const ctsCell = '<td' + (row.cts_gap_flag ? ' class="gap-flag"' : '') + '>' +
            (hasRace ? row.cts_race_num : '\u2014') + '</td>';
          const dolCell = '<td' + (row.dolphin_gap_flag ? ' class="gap-flag"' : '') + '>' +
            (row.dolphin_race_num ?? '\u2014') + '</td>';
          const datasetCell = '<td>' + (row.dolphin_dataset ?? '\u2014') + '</td>';
          const finish = row.cts_file_time
            ? (row.cts_file_time.length >= 19 ? row.cts_file_time.substring(11,19) : row.cts_file_time)
            : '\u2014';
          return '<tr class="' + cls + '">' +
            '<td class="left">' + (showEv ? row.event_id : '') + '</td>' +
            '<td>' + row.heat + '</td>' +
            '<td>' + (row.effective_start || '\u2014') + '</td>' +
            '<td>' + delta + '</td>' +
            lanes + ctsCell + dolCell + datasetCell +
            '<td>' + (row.cts_start_time || '\u2014') + '</td>' +
            '<td>' + finish + '</td>' +
            '</tr>';
        }).join('');
    });
}

function exportHistoryCSV() {
  if (!historyMeetId || !historySnapFile) return;
  const btn = document.getElementById('btn-export-csv');
  btn.textContent = 'Exporting...';
  btn.disabled = true;
  fetch('/api/snapshots/' + encodeURIComponent(historySnapFile) +
        '/export/' + encodeURIComponent(historyMeetId), {method: 'POST'})
    .then(r => r.json())
    .then(data => {
      btn.textContent = 'Export CSV';
      btn.disabled = false;
      alert('Exported to: ' + (data.exported || 'unknown path'));
    })
    .catch(() => { btn.textContent = 'Export CSV'; btn.disabled = false; });
}

// ---------------------------------------------------------------------------
// POLL
// ---------------------------------------------------------------------------
function poll() {
  checkPendingSchedule();
  if (currentView === 'schedule') loadDashboard();
  else if (currentView === 'log') loadFullLog();
  // history view is not auto-refreshed — it's read-only static data
}

function updateHeaderHeight() {
  const h = document.getElementById('sticky-top').offsetHeight;
  document.documentElement.style.setProperty('--header-height', h + 'px');
}
updateHeaderHeight();
window.addEventListener('resize', updateHeaderHeight);

function initialLoad(attempt) {
  loadDashboard()
    .then(() => updateHeaderHeight())
    .catch(() => {
      updateHeaderHeight();
      if ((attempt || 0) < 15) setTimeout(() => initialLoad((attempt || 0) + 1), 1000);
    });
}
initialLoad();
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

    Uses the most recently run heat's delta, not a running average.
    Returns dict with time, projected, avg_delta — or None if insufficient data.
    """
    # Use the most recently run heat's delta (last heat with delta data by heat_order)
    run_rows = [r for r in rows if r.get("delta_minutes") is not None]
    if not run_rows:
        return None

    last_run = max(run_rows, key=lambda r: r["heat_order"])
    last_delta = round(last_run["delta_minutes"], 1)

    scheduled = [r for r in rows if r.get("effective_start")]
    if not scheduled:
        return None

    last_heat = max(scheduled, key=lambda r: r["heat_order"])
    projected = last_heat["effective_start"]  # "HH:MM"

    try:
        base = datetime.strptime(projected, "%H:%M")
        eta_dt = base + timedelta(minutes=last_delta)
        eta_time = eta_dt.strftime("%I:%M %p").lstrip("0")
    except ValueError:
        return None

    return {
        "time":       eta_time,
        "projected":  projected,
        "avg_delta":  last_delta,
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

    # Apply Companion heat overrides if set
    if _companion_p1 or _companion_p2:
        for row in rows:
            row["is_next_heat"]    = False
            row["is_next_heat_p2"] = False
        if _companion_p1:
            for row in rows:
                if (str(row.get("event_id")) == str(_companion_p1["event_id"])
                        and (str(row.get("heat")) == str(_companion_p1["heat"])
                             or str(row.get("heat_label") or "") == str(_companion_p1["heat"]))):
                    row["is_next_heat"] = True
        if _companion_p2:
            for row in rows:
                if (str(row.get("event_id")) == str(_companion_p2["event_id"])
                        and (str(row.get("heat")) == str(_companion_p2["heat"])
                             or str(row.get("heat_label") or "") == str(_companion_p2["heat"]))):
                    row["is_next_heat_p2"] = True

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
    ok = reorder_heats(meet["meet_id"], data["ordered_ids"], session=data.get("session"))
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
    append = data.get("append", False)
    scrub = data.get("scrub_races", True)
    result = approve_schedule(scrub_races=scrub, append=append)
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


@app.route("/api/companion/pool1/set_heat", methods=["POST"])
def api_companion_set_heat_p1():
    """Set Pool 1 current heat from Bitfocus Companion.
    POST /api/companion/pool1/set_heat?event=$(streamline:event)&heat=$(streamline:heat)
    """
    global _companion_p1
    event = request.args.get("event")
    heat  = request.args.get("heat")
    if event is None or heat is None:
        return jsonify({"error": "Missing event or heat parameter"}), 400
    _companion_p1 = {"event_id": event, "heat": heat}
    log.info(f"Companion P1 heat set: Event={event} Heat={heat}")
    return jsonify({"status": "ok", "pool": 1, "event_id": event, "heat": heat})


@app.route("/api/companion/pool1/clear_heat", methods=["POST"])
def api_companion_clear_heat_p1():
    """Clear Pool 1 Companion override — reverts to auto-detection."""
    global _companion_p1
    _companion_p1 = None
    log.info("Companion P1 heat override cleared")
    return jsonify({"status": "ok", "pool": 1})


@app.route("/api/companion/pool2/set_heat", methods=["POST"])
def api_companion_set_heat_p2():
    """Set Pool 2 current heat from Bitfocus Companion.
    POST /api/companion/pool2/set_heat?event=$(streamline_2:event)&heat=$(streamline_2:heat)
    """
    global _companion_p2
    event = request.args.get("event")
    heat  = request.args.get("heat")
    if event is None or heat is None:
        return jsonify({"error": "Missing event or heat parameter"}), 400
    _companion_p2 = {"event_id": event, "heat": heat}
    log.info(f"Companion P2 heat set: Event={event} Heat={heat}")
    return jsonify({"status": "ok", "pool": 2, "event_id": event, "heat": heat})


@app.route("/api/companion/pool2/clear_heat", methods=["POST"])
def api_companion_clear_heat_p2():
    """Clear Pool 2 Companion override — reverts to auto-detection."""
    global _companion_p2
    _companion_p2 = None
    log.info("Companion P2 heat override cleared")
    return jsonify({"status": "ok", "pool": 2})


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


# ---------------------------------------------------------------------------
# SNAPSHOT HISTORY ROUTES (read-only, from snapshot .db files)
# ---------------------------------------------------------------------------

def _resolve_snapshot(filename):
    """Validate snapshot filename and return its absolute path, or abort."""
    if not filename.startswith("cts_tracker_"):
        abort(400, "Invalid snapshot filename")
    path = os.path.join(config.SNAPSHOT_DIR, filename)
    snap_root = os.path.normpath(config.SNAPSHOT_DIR) + os.sep
    if not os.path.normpath(path).startswith(snap_root):
        abort(400, "Invalid snapshot path")
    if not os.path.isfile(path):
        abort(404, "Snapshot file not found")
    return path


@app.route("/api/snapshots")
def api_snapshots():
    snaps = get_snapshots()
    for s in snaps:
        path = os.path.join(config.SNAPSHOT_DIR, s["snapshot_file"])
        s["exists"]     = os.path.isfile(path)
        s["size_bytes"] = os.path.getsize(path) if s["exists"] else None
        # created_at is UTC — derive local time from the filename instead
        # filename format: cts_tracker_YYYY-MM-DD_HH-MM-SS.db
        try:
            ts = s["snapshot_file"].replace("cts_tracker_", "").replace(".db", "")
            date_part, time_part = ts.split("_", 1)
            s["local_time"] = date_part + " " + time_part.replace("-", ":")
        except Exception:
            s["local_time"] = s["created_at"]
        # Pull meet name and meet_id directly from the snapshot file
        s["meet_name"] = None
        s["meet_id"]   = None
        if s["exists"]:
            try:
                meets = get_all_meets(db_path=path)
                if meets:
                    s["meet_name"] = meets[0]["meet_name"]
                    s["meet_id"]   = meets[0]["meet_id"]
            except Exception:
                pass
    return jsonify(snaps)


@app.route("/api/snapshots/<filename>/meets")
def api_snapshot_meets(filename):
    snap_path = _resolve_snapshot(filename)
    return jsonify(get_all_meets(db_path=snap_path))


@app.route("/api/snapshots/<filename>/dashboard/<meet_id>")
def api_snapshot_dashboard(filename, meet_id):
    snap_path = _resolve_snapshot(filename)
    meets = {m["meet_id"]: m for m in get_all_meets(db_path=snap_path)}
    if meet_id not in meets:
        abort(404, "Meet not found in snapshot")
    rows = get_race_dashboard(meet_id, db_path=snap_path)
    return jsonify({"meet": meets[meet_id], "rows": rows})


@app.route("/api/snapshots/<filename>/export/<meet_id>", methods=["POST"])
def api_snapshot_export(filename, meet_id):
    snap_path = _resolve_snapshot(filename)
    meets = {m["meet_id"]: m for m in get_all_meets(db_path=snap_path)}
    if meet_id not in meets:
        abort(404, "Meet not found in snapshot")
    rows = get_full_log(meet_id, db_path=snap_path)
    timestamp  = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    snap_tag   = filename.replace("cts_tracker_", "").replace(".db", "")
    export_path = os.path.join(config.BACKUP_DIR, f"{timestamp}_snapshot_{snap_tag}_{meet_id}.csv")
    if rows:
        with open(export_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    return jsonify({"exported": export_path})
