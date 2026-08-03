"""
routes.py - Flask app, dashboard HTML, and all API routes.
"""

import logging
import os
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta

from flask import Flask, jsonify, render_template_string, request, abort

import config
import csv
import json
import companion_state
from version_info import get_git_version

from database import (
    get_active_meet, get_all_meets, create_meet, set_active_meet,
    get_schedule, get_sessions, override_start_time, clear_override,
    reorder_heats, add_manual_heat,
    get_race_dashboard, get_full_log, get_current_heat_state, resolve_heat_row,
    get_harvested_times,
    add_manual_race_entry, update_race_entry,
    get_pending_summary, get_ingestion_log,
    export_race_log_csv, snapshot_db, get_snapshots,
    get_checklist_items, get_checklist_state, set_checklist_state,
    get_checklist_notes, add_checklist_note, delete_checklist_note,
)
from ingestion import (
    get_pending_schedule, approve_schedule, dismiss_pending_schedule,
    ingest_schedule_file, get_session_report_notice, dismiss_session_report_notice,
)
import checklist

log = logging.getLogger("cts_tracker")

app = Flask(__name__)

# ---------------------------------------------------------------------------
# CLIENT HEARTBEAT STATE
# ---------------------------------------------------------------------------

DISCOVERY_PORT   = 47200
HEARTBEAT_STALE  = 60   # seconds before a client is considered offline

_clients      = {}   # machine_id -> last heartbeat dict
_clients_lock = threading.Lock()


def _udp_discovery_listener():
    """
    Listen for UDP broadcast discovery packets from clients.
    Responds with the Flask port so clients can find the server dynamically.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", DISCOVERY_PORT))
        log.info(f"UDP discovery listener on port {DISCOVERY_PORT}")
        while True:
            try:
                data, addr = sock.recvfrom(1024)
                if data == b"CTS_TRACKER_DISCOVER":
                    sock.sendto(f"CTS_TRACKER_HERE:{config.FLASK_PORT}".encode(), addr)
            except Exception as e:
                log.warning(f"UDP discovery error: {e}")
    except Exception as e:
        log.error(f"UDP discovery listener failed: {e}")


threading.Thread(target=_udp_discovery_listener, daemon=True).start()


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
    .version-tag { color: #666; font-size: 10px; }
    .version-tag.dirty { color: #d4a017; }
    .status-bar { display: flex; gap: 12px; font-size: 11px; margin-left: auto; flex-wrap: wrap; align-items: center; }
    .status-pill { background: #0f3460; padding: 2px 7px; border-radius: 10px; white-space: nowrap; }
    .status-pill.warn { background: #8b4000; }
    .pool-block { display: flex; flex-direction: column; gap: 1px; border-left: 3px solid; padding-left: 7px; line-height: 1.5; }
    .pool-block.p1 { border-color: #6bff6b; }
    .pool-block.p2 { border-color: #00cfff; }
    .pool-block .pool-label { font-size: 10px; font-weight: bold; letter-spacing: 1px; }
    .pool-block.p1 .pool-label { color: #6bff6b; }
    .pool-block.p2 .pool-label { color: #00cfff; }
    .pool-block .pool-row { font-size: 10px; color: #888; white-space: nowrap; }
    .pool-block .pool-row span { color: #e0e0e0; }

    /* NAV */
    nav { background: #16213e; padding: 5px 14px; display: flex; gap: 6px;
          border-bottom: 1px solid #0f3460; align-items: center; }
    #eta-bar { font-size: 11px; display: none; }
    #eta-bar.show { display: inline; }
    .view-btn { border: none; padding: 3px 9px; border-radius: 4px; cursor: pointer;
                font-family: monospace; font-size: 11px; margin-left: auto; }
    .sched-toggle-btn { border: none; padding: 3px 9px; border-radius: 4px; cursor: pointer;
                font-family: monospace; font-size: 11px; background: #0f3460; color: #a0c4ff; }
    .sched-toggle-btn.off { background: #2a2a2a; color: #666; }
    #btn-schedule { background: #1a3a1a; color: #6bff6b; }
    #btn-schedule.active { background: #6bff6b; color: #0d1117; }
    #btn-log { background: #0f3460; color: #a0c4ff; }
    #btn-log.active { background: #a0c4ff; color: #0d1117; }
    #btn-reorder { background: #0f3460; color: #a0c4ff; }
    #btn-reorder.active { background: #a0c4ff; color: #0d1117; }
    #btn-settings { background: #0f3460; color: #a0c4ff; }
    #btn-settings.active { background: #a0c4ff; color: #0d1117; }
    #btn-restart { background: #3a1a1a; color: #ff6b6b; margin-left: 0; }
    #btn-restart:hover { background: #ff6b6b; color: #0d1117; }

    /* Settings view */
    .settings-section { border: 1px solid #1e2a4a; border-radius: 6px; margin: 14px; padding: 10px 14px; }
    .settings-section h3 { margin: 0 0 8px; font-size: 13px; color: #a0c4ff; }
    .settings-section .settings-empty { color: #555; font-size: 11px; }
    .settings-row { display: flex; gap: 8px; align-items: center; margin-bottom: 6px; }

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

    /* Session divider — sticks below the table header as its section scrolls by.
       Background/text match the column header (th) styling above. */
    tr.session-divider td { background: #0f3460; color: #a0c4ff; font-weight: bold;
        text-align: center; padding: 6px 10px; font-size: 14px;
        border-bottom: 1px solid #1e2a4a; white-space: nowrap;
        position: sticky; top: calc(var(--thead-height, 24px) - 1px); z-index: 9;
        /* Extend the background 1px upward to cover any sub-pixel rounding gap
           between this row's sticky offset and the thead's true rendered height. */
        box-shadow: 0 -1px 0 0 #0f3460; }
    .session-divider-inner { position: relative; }
    .session-eta { position: absolute; left: 0; top: 50%; transform: translateY(-50%);
        text-align: left; font-weight: normal; }

    /* Lane cells — !important so they win over row highlight backgrounds */
    .lane-active  { background: #1a4a1a !important; color: #1a4a1a !important; font-weight: bold; border-radius: 3px; }
    .lane-empty   { background: #4a1a1a !important; color: #ff6b6b !important; border-radius: 3px; }
    .lane-unknown { color: #333; }

    /* Gap flag */
    td.gap-flag { color: #ff4444 !important; font-weight: bold; }
    td.gap-flag::after { content: " ⚠"; font-size: 9px; }

    /* Re-swim history — superseded attempt(s) stacked above the current one */
    .prior-race-num { text-decoration: line-through; opacity: 0.5; font-size: 11px; }

    /* Dolphin match history — available on hover instead of stacked */
    .has-history { border-bottom: 1px dotted currentColor; cursor: help; }

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

    /* Trends view */
    #btn-trends { background: #0f3460; color: #a0c4ff; }
    #btn-trends.active { background: #a0c4ff; color: #0d1117; }
    #btn-harvested { background: #3a2a1a; color: #ffc06b; }
    #btn-harvested.active { background: #ffc06b; color: #0d1117; }
    .trends-table { border-collapse: collapse; }
    .trends-table th { background: #0f3460; color: #a0c4ff; padding: 5px 7px; text-align: center;
                       font-size: 10px; white-space: nowrap; position: sticky; top: 0; z-index: 10; }
    .trends-table td { padding: 4px 7px; border-bottom: 1px solid #1e2a4a; text-align: center;
                       font-size: 12px; white-space: nowrap; }
    .trends-table td.left { text-align: left; }
    .trends-table td.cell-agree { background: rgba(107,255,107,0.14); }
    .trends-table td.cell-disagree { background: rgba(255,107,107,0.18); }
    #harvested-table-wrap.hide-touchpad .col-touchpad,
    #harvested-table-wrap.hide-button_a .col-button_a,
    #harvested-table-wrap.hide-button_b .col-button_b,
    #harvested-table-wrap.hide-dolphin_a .col-dolphin_a,
    #harvested-table-wrap.hide-dolphin_b .col-dolphin_b,
    #harvested-table-wrap.hide-dolphin_c .col-dolphin_c { display: none; }
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

    /* Checklist View */
    #btn-checklist { background:#1a3a2a; color:#6bffb0; }
    #btn-checklist.active { background:#6bffb0; color:#0d1117; }
    .checklist-table { border-collapse:collapse; width:100%; margin-bottom:20px; }
    .checklist-table th { background:#0f3460; color:#a0c4ff; padding:6px 10px;
                        text-align:left; font-size:10px; white-space:nowrap; }
    .checklist-table td { padding:7px 10px; border-bottom:1px solid #1e2a4a;
                        font-size:12px; vertical-align:middle; }
    .checklist-table tr:hover td { background:#222; }
    .checklist-item-manual { color:#888; font-size:9px; margin-left:6px; }
    .checklist-status { display:flex; align-items:center; gap:6px; font-size:11px; }
    .cl-dot { display:inline-block; width:9px; height:9px; border-radius:50%; flex-shrink:0; }
    .cl-dot.ok      { background:#6bff6b; }
    .cl-dot.fail    { background:#ff6b6b; }
    .cl-dot.unknown { background:#444; }
    .checklist-detail-list { margin:0; padding-left:16px; list-style:disc; }
    .checklist-notes { background:#16213e; border:1px solid #0f3460; border-radius:6px; padding:12px; max-width:640px; }
    .checklist-notes-title { font-size:13px; font-weight:bold; color:#6bffb0; margin-bottom:6px; }
    .checklist-note-input { width:100%; min-height:60px; background:#0f3460; border:1px solid #1e2a4a;
                        border-radius:3px; color:#e0e0e0; font-family:monospace; font-size:12px;
                        padding:7px; resize:vertical; }
    .checklist-note-input:focus { outline:1px solid #6bffb0; }
    .checklist-notes-list { margin-top:12px; display:flex; flex-direction:column; gap:6px; }
    .checklist-note-row { background:#0f3460; border-radius:4px; padding:6px 9px; font-size:12px;
                        display:flex; justify-content:space-between; gap:10px; align-items:flex-start; }
    .checklist-note-text { white-space:pre-wrap; word-break:break-word; }
    .checklist-note-meta { color:#555; font-size:10px; white-space:nowrap; display:flex; align-items:center; gap:8px; }
    .checklist-note-del { background:none; border:none; color:#ff6b6b; cursor:pointer; font-size:13px; padding:0; }
    .checklist-note-del:hover { color:#ff9b9b; }

    /* Peripherals View (OBS + Dolphin5) */
    #btn-peripherals { background:#1a1a3a; color:#c0a0ff; margin-left:0; }
    #btn-peripherals.active { background:#c0a0ff; color:#0d1117; }
    #peripherals-view { padding:14px; display:flex; flex-direction:column; gap:12px; }
    .peripherals-group-title { font-size:11px; color:#666; letter-spacing:1px; text-transform:uppercase;
                                margin:6px 0 -4px; }
    .obs-panels { display:flex; gap:12px; flex-wrap:wrap; }
    .obs-panel { background:#16213e; border:1px solid #0f3460; border-radius:6px; padding:12px; min-width:300px; flex:1; }
    .obs-panel-title { font-size:13px; font-weight:bold; color:#c0a0ff; margin-bottom:10px; }
    .obs-section { background:#16213e; border:1px solid #0f3460; border-radius:6px; padding:12px; }
    .obs-section-title { font-size:10px; color:#888; letter-spacing:1px; margin-bottom:9px; }
    .obs-row { display:flex; gap:8px; align-items:center; flex-wrap:wrap; margin-bottom:7px; }
    .obs-row:last-child { margin-bottom:0; }
    .obs-row label { color:#a0c4ff; font-size:11px; white-space:nowrap; min-width:60px; }
    .obs-input { background:#0f3460; border:1px solid #1e2a4a; border-radius:3px;
                 color:#e0e0e0; font-family:monospace; font-size:12px; padding:4px 7px; }
    .obs-input:focus { outline:1px solid #c0a0ff; }
    .obs-input-wide { width:280px; }
    .obs-input-mid  { width:160px; }
    .obs-input-sm   { width:110px; }
    .obs-btn { border:none; padding:4px 11px; border-radius:4px; cursor:pointer;
               font-family:monospace; font-size:11px; background:#0f3460; color:#a0c4ff; }
    .obs-btn:hover { background:#a0c4ff; color:#0d1117; }
    .obs-btn-start { background:#1a4a1a; color:#6bff6b; font-size:12px; padding:6px 16px; }
    .obs-btn-start:hover { background:#6bff6b; color:#0d1117; }
    .obs-btn-danger { background:#3a1a1a; color:#ff6b6b; }
    .obs-btn-danger:hover { background:#ff6b6b; color:#0d1117; }
    .obs-status-row { display:flex; gap:16px; margin-top:9px; font-size:11px; }
    .obs-indicator { display:flex; align-items:center; gap:5px; }
    .obs-dot { width:9px; height:9px; border-radius:50%; display:inline-block; flex-shrink:0; }
    .obs-dot.ok   { background:#6bff6b; }
    .obs-dot.warn { background:#ffd700; }
    .obs-dot.off  { background:#444; }
    .obs-dot.live { background:#e94560; }
    .obs-msg { font-size:11px; min-height:15px; margin-top:6px; }

    /* Dolphin5 panels -- reuse .obs-* structural classes above */
    .dolphin5-last-msg { font-family:monospace; font-size:11px; color:#a0c4ff;
                          background:#0f3460; border-radius:3px; padding:5px 8px;
                          margin-top:6px; word-break:break-all; }
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
      <div><b>File age:</b> <span id="modal-file-age">&#8212;</span></div>
    </div>
    <p>How would you like to proceed?</p>
    <button class="modal-btn btn-scrub"   onclick="approveSchedule('scrub')">Scrub Race Data &amp; Import</button>
    <button class="modal-btn btn-keep"    onclick="approveSchedule('keep')">Keep Race Data &amp; Import</button>
    <button class="modal-btn btn-add"     onclick="approveSchedule('append')">Append to Schedule</button>
    <button class="modal-btn btn-dismiss" onclick="dismissSchedule()">Dismiss</button>
  </div>
</div>

<!-- Session Report Notice Modal -->
<div id="session-report-overlay" class="modal-overlay">
  <div class="modal-box">
    <h2 id="sr-title">&#x1F4CA; Session Report Ingested</h2>
    <p id="sr-summary"></p>
    <div class="meet-info">
      <div><b>Meet:</b> <span id="sr-meet-name">&#8212;</span></div>
      <div><b>File:</b> <span id="sr-filename">&#8212;</span></div>
      <div><b>File age:</b> <span id="sr-file-age">&#8212;</span></div>
    </div>
    <button class="modal-btn btn-dismiss" onclick="dismissSessionReportNotice()">OK</button>
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
      <div class="pool-block p1" id="block-p1">
        <div class="pool-label">POOL 1</div>
        <div class="pool-row">Last: <span id="p1-last">&#8212;</span></div>
        <div class="pool-row">Current: <span id="p1-current">&#8212;</span></div>
        <div class="pool-row">Next: <span id="p1-next">&#8212;</span></div>
      </div>
      <div class="pool-block p2" id="block-p2">
        <div class="pool-label">POOL 2</div>
        <div class="pool-row">Last: <span id="p2-last">&#8212;</span></div>
        <div class="pool-row">Current: <span id="p2-current">&#8212;</span></div>
        <div class="pool-row">Next: <span id="p2-next">&#8212;</span></div>
      </div>
    </div>
  </header>
  <nav>
    <div id="eta-bar"></div>
    <button class="view-btn" id="btn-schedule" onclick="setView('schedule')">Schedule</button>
    <button class="view-btn" id="btn-log"      onclick="setView('log')">Full Log</button>
    <button class="view-btn" id="btn-reorder"  onclick="setView('reorder')">Reorder</button>
    <button class="view-btn" id="btn-history"  onclick="setView('history')">History</button>
    <button class="view-btn" id="btn-trends"   onclick="setView('trends')">Trends</button>
    <button class="view-btn" id="btn-harvested" onclick="setView('harvested')">Harvested Times</button>
    <button class="view-btn" id="btn-checklist" onclick="setView('checklist')">Checklist</button>
    <button class="view-btn" id="btn-peripherals" onclick="setView('peripherals')">Peripherals</button>
    <button class="view-btn" id="btn-settings" onclick="setView('settings')">Settings</button>
    <button class="view-btn" id="btn-add-heat" onclick="openAddHeat()" style="background:#1a3a1a;color:#6bff6b;">+ Add Heat</button>
    <button class="view-btn" id="btn-restart"  onclick="restartServer()">Restart Server</button>
  </nav>
</div>

<!-- Schedule View -->
<div class="container" id="schedule-view">
  <table>
    <thead id="schedule-thead">
      <tr>
        <th>Event</th>
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
        <th>Event</th>
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

<!-- Trends View -->
<div id="trends-view" style="display:none;flex-direction:column;height:calc(100vh - var(--header-height, 0px));overflow:hidden;">
  <div style="flex-shrink:0;padding:8px 14px;display:flex;gap:16px;align-items:center;border-bottom:1px solid #1e2a4a;flex-wrap:wrap;background:#1a1a2e;">
    <span style="font-size:10px;color:#888;letter-spacing:1px;">LEGEND</span>
    <span style="font-size:11px;display:flex;align-items:center;gap:5px;">
      <span style="display:inline-block;width:11px;height:11px;border-radius:2px;background:#44bb44;"></span> Recorded
    </span>
    <span style="font-size:11px;display:flex;align-items:center;gap:5px;">
      <span style="display:inline-block;width:11px;height:11px;border-radius:2px;background:#ff4444;"></span> Missed
    </span>
    <span style="font-size:11px;display:flex;align-items:center;gap:5px;">
      <span style="display:inline-block;width:11px;height:11px;border-radius:2px;background:#2a2a2a;"></span> Lane not active
    </span>
  </div>
  <div style="flex:1;overflow:auto;">
  <table class="trends-table" style="width:100%">
    <thead>
      <tr>
        <th>Event</th>
        <th>Heat</th>
        <th>Touchpad<br><span style="font-size:9px;color:#888">lanes 1&ndash;8</span></th>
        <th>Button 1<br><span style="font-size:9px;color:#888">lanes 1&ndash;8</span></th>
        <th>Button 2<br><span style="font-size:9px;color:#888">lanes 1&ndash;8</span></th>
        <th>Watch A<br><span style="font-size:9px;color:#888">lanes 1&ndash;8</span></th>
        <th>Watch B<br><span style="font-size:9px;color:#888">lanes 1&ndash;8</span></th>
      </tr>
    </thead>
    <tbody id="trends-table"></tbody>
  </table>
  </div>
</div>

<!-- Harvested Times View -->
<div id="harvested-view" style="display:none;flex-direction:column;height:calc(100vh - var(--header-height, 0px));overflow:hidden;">
  <div style="flex-shrink:0;padding:8px 14px;font-size:11px;color:#888;border-bottom:1px solid #1e2a4a;background:#1a1a2e;">
    Every raw time recorded per lane (touchpad, backup buttons, Dolphin backup watches), the average of whatever's
    available, and each source's deviation from that average. Not an official time &mdash; adjudication rules
    aren't defined yet, this is just a confidence signal alongside the recorder's own judgment.
  </div>
  <div id="harvested-col-toggles" style="flex-shrink:0;padding:6px 14px;display:flex;gap:14px;flex-wrap:wrap;
       align-items:center;border-bottom:1px solid #1e2a4a;background:#141428;font-size:11px;"></div>
  <div style="flex:1;overflow:auto;">
  <table class="trends-table" id="harvested-table-wrap" style="width:100%">
    <thead>
      <tr>
        <th>Event</th>
        <th>Heat</th>
        <th>Lane</th>
        <th>Official</th>
        <th>Confidence</th>
        <th class="col-touchpad">Touchpad</th>
        <th class="col-button_a">Button A</th>
        <th class="col-button_b">Button B</th>
        <th class="col-dolphin_a">Dolphin A</th>
        <th class="col-dolphin_b">Dolphin B</th>
        <th class="col-dolphin_c">Dolphin C</th>
        <th>Avg</th>
        <th>Max &Delta;</th>
      </tr>
    </thead>
    <tbody id="harvested-table"></tbody>
  </table>
  </div>
</div>

<!-- Checklist View -->
<div class="container" id="checklist-view" style="display:none">
  <div style="padding:14px;">
    <div id="checklist-no-meet" style="color:#555;font-size:12px;display:none;margin-bottom:10px;">
      No active meet — auto-checks that depend on meet state will show as unknown, and checkbox state won't be saved.
    </div>
    <table class="checklist-table">
      <thead>
        <tr>
          <th style="width:36px"></th>
          <th class="left">Item</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody id="checklist-table"></tbody>
    </table>

    <div class="checklist-notes">
      <div class="checklist-notes-title">Notes</div>
      <div style="color:#555;font-size:11px;margin-bottom:8px;">
        Jot anything to revisit after the meet — new steps to add, connections that acted up, etc.
        Entries are timestamped and persist across restarts and new meet uploads.
      </div>
      <textarea id="checklist-note-input" class="checklist-note-input"
                placeholder="Type a note and click Add..."></textarea>
      <button class="reorder-save" style="margin-top:6px;" onclick="addChecklistNote()">Add Note</button>
      <div id="checklist-notes-list" class="checklist-notes-list"></div>
    </div>
  </div>
</div>

<!-- Settings View -->
<div class="container" id="settings-view" style="display:none">
  <div class="settings-section">
    <h3>Schedule</h3>
    <div class="settings-row">
      <button class="sched-toggle-btn" id="btn-toggle-session" onclick="toggleSessionGrouping()">Session Grouping: ON</button>
      <button class="sched-toggle-btn" id="btn-toggle-sort" onclick="toggleSortMode()">Sort: Wall Time</button>
      <button class="sched-toggle-btn" id="btn-toggle-time-source" onclick="toggleTimeSource()">Projected Time: Meet Program</button>
    </div>
  </div>
  <div class="settings-section">
    <h3>Full Log</h3>
    <div class="settings-empty">Nothing here yet.</div>
  </div>
  <div class="settings-section">
    <h3>Reorder</h3>
    <div class="settings-empty">Nothing here yet.</div>
  </div>
  <div class="settings-section">
    <h3>History</h3>
    <div class="settings-empty">Nothing here yet.</div>
  </div>
  <div class="settings-section">
    <h3>Trends</h3>
    <div class="settings-empty">Nothing here yet.</div>
  </div>
  <div class="settings-section">
    <h3>Peripherals</h3>
    <div class="settings-empty">Nothing here yet.</div>
  </div>
  <div class="settings-section">
    <h3>About</h3>
    <span class="version-tag" id="version-tag"></span>
  </div>
</div>

<!-- Peripherals View (OBS + Dolphin5) -->
<div class="container" id="peripherals-view" style="display:none">
  <div class="peripherals-group-title">OBS</div>
  <div class="obs-panels">

    <!-- OBS 1 panel -->
    <div class="obs-panel">
      <div class="obs-panel-title">OBS 1 &nbsp;<span style="color:#555;font-size:10px;font-weight:normal">port 4455</span></div>

      <!-- Connection -->
      <div class="obs-section-title" style="margin-top:4px;">CONNECTION</div>
      <div class="obs-row">
        <label>Host</label>
        <input id="obs1-host" class="obs-input obs-input-mid" type="text" value="172.16.0.119" placeholder="172.16.0.119">
        <label>Password</label>
        <input id="obs1-pass" class="obs-input obs-input-sm" type="password" placeholder="optional">
        <button class="obs-btn" onclick="obsSaveConfig(1)">Save</button>
      </div>
      <div class="obs-status-row">
        <div class="obs-indicator"><span class="obs-dot off" id="obs1-conn-dot"></span><span id="obs1-conn-label" style="color:#888">--</span></div>
        <div class="obs-indicator"><span class="obs-dot off" id="obs1-stream-dot"></span><span id="obs1-stream-label" style="color:#888">--</span></div>
      </div>

      <!-- Stream settings -->
      <div class="obs-section-title" style="margin-top:12px;">STREAM SETTINGS</div>
      <div class="obs-row">
        <label>RTMP URL</label>
        <input id="obs1-url" class="obs-input obs-input-wide" type="text" placeholder="rtmp://live.example.com/live">
      </div>
      <div class="obs-row">
        <label>Stream Key</label>
        <input id="obs1-key" class="obs-input obs-input-wide" type="password" placeholder="stream key">
        <button class="obs-btn" onclick="obsApplySettings(1)">Apply</button>
      </div>
      <div class="obs-msg" id="obs1-settings-msg"></div>

      <!-- Schedule / start -->
      <div class="obs-section-title" style="margin-top:12px;">STREAM START</div>
      <div class="obs-row">
        <label>Event time</label>
        <input id="obs1-sched-time" class="obs-input" type="text" placeholder="HH:MM (24hr)">
        <label style="margin-left:4px;">Start</label>
        <input id="obs1-offset" class="obs-input" type="number" value="10" min="0" max="120" style="width:52px;text-align:center;">
        <label>min early</label>
        <button class="obs-btn" onclick="obsSetSchedule(1)">Set</button>
        <button class="obs-btn obs-btn-danger" id="obs1-cancel-btn" onclick="obsCancelSchedule(1)" style="display:none">Cancel</button>
      </div>
      <div style="font-size:11px;color:#ffd700;min-height:15px;margin:3px 0;" id="obs1-sched-label"></div>
      <div class="obs-row">
        <button class="obs-btn obs-btn-start" onclick="obsStartNow(1)">&#9654; Start Now</button>
      </div>
      <div class="obs-msg" id="obs1-start-msg"></div>
    </div>

    <!-- OBS 2 panel -->
    <div class="obs-panel">
      <div class="obs-panel-title">OBS 2 &nbsp;<span style="color:#555;font-size:10px;font-weight:normal">port 4456</span></div>

      <!-- Connection -->
      <div class="obs-section-title" style="margin-top:4px;">CONNECTION</div>
      <div class="obs-row">
        <label>Host</label>
        <input id="obs2-host" class="obs-input obs-input-mid" type="text" value="172.16.0.119" placeholder="172.16.0.119">
        <label>Password</label>
        <input id="obs2-pass" class="obs-input obs-input-sm" type="password" placeholder="optional">
        <button class="obs-btn" onclick="obsSaveConfig(2)">Save</button>
      </div>
      <div class="obs-status-row">
        <div class="obs-indicator"><span class="obs-dot off" id="obs2-conn-dot"></span><span id="obs2-conn-label" style="color:#888">--</span></div>
        <div class="obs-indicator"><span class="obs-dot off" id="obs2-stream-dot"></span><span id="obs2-stream-label" style="color:#888">--</span></div>
      </div>

      <!-- Stream settings -->
      <div class="obs-section-title" style="margin-top:12px;">STREAM SETTINGS</div>
      <div class="obs-row">
        <label>RTMP URL</label>
        <input id="obs2-url" class="obs-input obs-input-wide" type="text" placeholder="rtmp://live.example.com/live">
      </div>
      <div class="obs-row">
        <label>Stream Key</label>
        <input id="obs2-key" class="obs-input obs-input-wide" type="password" placeholder="stream key">
        <button class="obs-btn" onclick="obsApplySettings(2)">Apply</button>
      </div>
      <div class="obs-msg" id="obs2-settings-msg"></div>

      <!-- Schedule / start -->
      <div class="obs-section-title" style="margin-top:12px;">STREAM START</div>
      <div class="obs-row">
        <label>Event time</label>
        <input id="obs2-sched-time" class="obs-input" type="text" placeholder="HH:MM (24hr)">
        <label style="margin-left:4px;">Start</label>
        <input id="obs2-offset" class="obs-input" type="number" value="10" min="0" max="120" style="width:52px;text-align:center;">
        <label>min early</label>
        <button class="obs-btn" onclick="obsSetSchedule(2)">Set</button>
        <button class="obs-btn obs-btn-danger" id="obs2-cancel-btn" onclick="obsCancelSchedule(2)" style="display:none">Cancel</button>
      </div>
      <div style="font-size:11px;color:#ffd700;min-height:15px;margin:3px 0;" id="obs2-sched-label"></div>
      <div class="obs-row">
        <button class="obs-btn obs-btn-start" onclick="obsStartNow(2)">&#9654; Start Now</button>
      </div>
      <div class="obs-msg" id="obs2-start-msg"></div>
    </div>

  </div>

  <div class="peripherals-group-title">DOLPHIN5</div>
  <div class="obs-row">
    <button class="obs-btn obs-btn-start" onclick="dolphin5Start()">&#9654; Start TCP Control</button>
    <span id="dolphin5-running-label" style="font-size:11px;color:#888;margin-left:8px;">Not started this session</span>
  </div>
  <div style="font-size:11px;color:#888;max-width:700px;">
    Sends <code>setEventAndHeat</code> to each pool's Dolphin5 unit to follow GEN7's real progress.
    Starting is a one-way switch for this running session (no separate stop) &mdash; restart the
    server to fully stop it. Nothing is sent until this is started.
  </div>

  <div class="obs-panels">

      <!-- Pool 1 panel -->
      <div class="obs-panel">
        <div class="obs-panel-title">Dolphin5 &mdash; Pool 1</div>

        <div class="obs-section-title" style="margin-top:4px;">CONNECTION</div>
        <div class="obs-row">
          <label>Host</label>
          <input id="dolphin5-1-host" class="obs-input obs-input-mid" type="text" placeholder="e.g. 172.16.0.84">
          <label>Port</label>
          <input id="dolphin5-1-port" class="obs-input obs-input-sm" type="number" placeholder="13382">
          <button class="obs-btn" onclick="dolphin5SaveConfig(1)">Save</button>
        </div>
        <div class="obs-status-row">
          <div class="obs-indicator"><span class="obs-dot off" id="dolphin5-1-conn-dot"></span><span id="dolphin5-1-conn-label" style="color:#888">--</span></div>
        </div>

        <div class="obs-section-title" style="margin-top:12px;">CHASE STATUS</div>
        <div class="obs-row"><label>Last sent</label><span id="dolphin5-1-sent" style="font-size:12px;color:#e0e0e0;">&#8212;</span></div>
        <div class="obs-row"><label>Last seen</label><span id="dolphin5-1-seen" style="font-size:12px;color:#e0e0e0;">&#8212;</span></div>
        <div class="obs-section-title" style="margin-top:12px;">LAST TCP RESPONSE</div>
        <div class="dolphin5-last-msg" id="dolphin5-1-last-msg">&#8212;</div>
        <div style="font-size:10px;color:#555;margin-top:3px;" id="dolphin5-1-last-msg-age"></div>
      </div>

      <!-- Pool 2 panel -->
      <div class="obs-panel">
        <div class="obs-panel-title">Dolphin5 &mdash; Pool 2</div>

        <div class="obs-section-title" style="margin-top:4px;">CONNECTION</div>
        <div class="obs-row">
          <label>Host</label>
          <input id="dolphin5-2-host" class="obs-input obs-input-mid" type="text" placeholder="no default -- varies by meet">
          <label>Port</label>
          <input id="dolphin5-2-port" class="obs-input obs-input-sm" type="number" placeholder="13382">
          <button class="obs-btn" onclick="dolphin5SaveConfig(2)">Save</button>
        </div>
        <div class="obs-status-row">
          <div class="obs-indicator"><span class="obs-dot off" id="dolphin5-2-conn-dot"></span><span id="dolphin5-2-conn-label" style="color:#888">--</span></div>
        </div>

        <div class="obs-section-title" style="margin-top:12px;">CHASE STATUS</div>
        <div class="obs-row"><label>Last sent</label><span id="dolphin5-2-sent" style="font-size:12px;color:#e0e0e0;">&#8212;</span></div>
        <div class="obs-row"><label>Last seen</label><span id="dolphin5-2-seen" style="font-size:12px;color:#e0e0e0;">&#8212;</span></div>
        <div class="obs-section-title" style="margin-top:12px;">LAST TCP RESPONSE</div>
        <div class="dolphin5-last-msg" id="dolphin5-2-last-msg">&#8212;</div>
        <div style="font-size:10px;color:#555;margin-top:3px;" id="dolphin5-2-last-msg-age"></div>
      </div>

  </div>
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
let lastSession = null;

// Schedule tab display toggles — persisted so a page reload mid-meet keeps
// your last choice instead of resetting.
let sessionGroupingOn = localStorage.getItem('sched_session_grouping') !== 'off';
let sortMode = localStorage.getItem('sched_sort_mode') || 'walltime'; // 'walltime' | 'eventheat'
let timeSource = localStorage.getItem('sched_time_source') || 'meetprogram'; // 'meetprogram' | 'sessionreport'
let lastDashboardData = null;

function sessionNum(session) {
  const n = parseInt(session, 10);
  return isNaN(n) ? 0 : n;
}

function compareRows(a, b) {
  if (sessionGroupingOn) {
    const diff = sessionNum(a.session) - sessionNum(b.session);
    if (diff !== 0) return diff;
  }
  if (sortMode === 'walltime') return a.heat_order - b.heat_order;
  const evDiff = (parseFloat(a.event_id) || 0) - (parseFloat(b.event_id) || 0);
  if (evDiff !== 0) return evDiff;
  return (parseFloat(a.heat) || 0) - (parseFloat(b.heat) || 0);
}

function updateScheduleToggleButtons() {
  const sBtn = document.getElementById('btn-toggle-session');
  const oBtn = document.getElementById('btn-toggle-sort');
  const tBtn = document.getElementById('btn-toggle-time-source');
  if (sBtn) {
    sBtn.textContent = 'Session Grouping: ' + (sessionGroupingOn ? 'ON' : 'OFF');
    sBtn.classList.toggle('off', !sessionGroupingOn);
  }
  if (oBtn) {
    oBtn.textContent = 'Sort: ' + (sortMode === 'walltime' ? 'Wall Time' : 'Event/Heat');
  }
  if (tBtn) {
    tBtn.textContent = 'Projected Time: ' + (timeSource === 'meetprogram' ? 'Meet Program' : 'Session Report');
  }
}

function toggleSessionGrouping() {
  sessionGroupingOn = !sessionGroupingOn;
  localStorage.setItem('sched_session_grouping', sessionGroupingOn ? 'on' : 'off');
  updateScheduleToggleButtons();
  renderScheduleRows();
}

function toggleSortMode() {
  sortMode = sortMode === 'walltime' ? 'eventheat' : 'walltime';
  localStorage.setItem('sched_sort_mode', sortMode);
  updateScheduleToggleButtons();
  renderScheduleRows();
}

function toggleTimeSource() {
  timeSource = timeSource === 'meetprogram' ? 'sessionreport' : 'meetprogram';
  localStorage.setItem('sched_time_source', timeSource);
  updateScheduleToggleButtons();
  renderScheduleRows();
}

// Strips the redundant "Meet Program - " prefix MM exports carry on every
// session label; falls back to the raw string for whole-meet exports where
// stripping it would leave nothing (e.g. plain "Meet Program").
function sessionLabel(s) {
  if (!s) return '';
  const stripped = s.replace(/^Meet Program\s*-?\s*/i, '').trim();
  return stripped || s;
}

// Shared formatting for a final-heat ETA object ({time, avg_delta}) — used by
// both the top summary bar and each per-session divider row.
function etaColor(avgDelta) {
  return avgDelta > 0 ? '#ff6b6b' : avgDelta < 0 ? '#6bff6b' : '#ffffff';
}
function etaText(eta) {
  const sign = eta.avg_delta > 0 ? '+' : '';
  return eta.time + '  (' + sign + eta.avg_delta + ' min)';
}

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
  document.getElementById('trends-view').style.display   = v === 'trends'   ? 'flex' : 'none';
  document.getElementById('harvested-view').style.display = v === 'harvested' ? 'flex' : 'none';
  document.getElementById('peripherals-view').style.display = v === 'peripherals' ? '' : 'none';
  document.getElementById('checklist-view').style.display = v === 'checklist' ? '' : 'none';
  document.getElementById('settings-view').style.display = v === 'settings' ? '' : 'none';
  document.getElementById('btn-schedule').classList.toggle('active', v === 'schedule');
  document.getElementById('btn-log').classList.toggle('active', v === 'log');
  document.getElementById('btn-reorder').classList.toggle('active', v === 'reorder');
  document.getElementById('btn-history').classList.toggle('active', v === 'history');
  document.getElementById('btn-trends').classList.toggle('active', v === 'trends');
  document.getElementById('btn-harvested').classList.toggle('active', v === 'harvested');
  document.getElementById('btn-peripherals').classList.toggle('active', v === 'peripherals');
  document.getElementById('btn-checklist').classList.toggle('active', v === 'checklist');
  document.getElementById('btn-settings').classList.toggle('active', v === 'settings');
  if (v === 'log')     loadFullLog();
  if (v === 'reorder') loadReorderView();
  if (v === 'history') loadSnapshots();
  if (v === 'trends')  loadTrends();
  if (v === 'harvested') loadHarvestedTimes();
  if (v === 'peripherals') { loadObsStatus(); loadDolphin5Status(); }
  if (v === 'checklist') loadChecklist();
  if (v === 'settings') updateScheduleToggleButtons();
}
setView('schedule');  // set initial active state

// ---------------------------------------------------------------------------
// MODAL
// ---------------------------------------------------------------------------
function formatAge(sec) {
  if (sec == null || isNaN(sec) || sec < 0) return '\u2014';
  sec = Math.floor(sec);
  if (sec < 60) return sec + 's ago';
  const m = Math.floor(sec / 60), s = sec % 60;
  if (m < 60) return m + 'm ' + s + 's ago';
  const h = Math.floor(m / 60), rm = m % 60;
  return h + 'h ' + rm + 'm ago';
}

function startAgeTicker(fileMtime, elId) {
  const update = () => {
    const el = document.getElementById(elId);
    if (el) el.textContent = fileMtime ? formatAge(Date.now() / 1000 - fileMtime) : '\u2014';
  };
  update();
  return setInterval(update, 1000);
}

let scheduleAgeTimer = null;
let sessionReportAgeTimer = null;

function checkPendingSchedule() {
  fetch('/api/schedule/pending')
    .then(r => r.json())
    .then(data => {
      if (data && data.filename) {
        document.getElementById('modal-meet-name').textContent = data.meet_name || '\u2014';
        document.getElementById('modal-meet-date').textContent = data.meet_date || '\u2014';
        document.getElementById('modal-filename').textContent  = data.filename  || '\u2014';
        clearInterval(scheduleAgeTimer);
        scheduleAgeTimer = startAgeTicker(data.file_mtime, 'modal-file-age');
        document.getElementById('modal-overlay').classList.add('show');
      } else {
        clearInterval(scheduleAgeTimer);
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
    clearInterval(scheduleAgeTimer);
    document.getElementById('modal-overlay').classList.remove('show');
    loadDashboard();
  });
}

function dismissSchedule() {
  fetch('/api/schedule/dismiss', {method: 'POST'})
    .then(() => {
      clearInterval(scheduleAgeTimer);
      document.getElementById('modal-overlay').classList.remove('show');
    });
}

// ---------------------------------------------------------------------------
// SESSION REPORT NOTICE MODAL
// ---------------------------------------------------------------------------
function checkSessionReportNotice() {
  fetch('/api/session_report/notice')
    .then(r => r.json())
    .then(data => {
      // Defer to the schedule-approval modal if both are pending at once —
      // it needs a decision, this one is FYI only and can wait a poll cycle.
      if (data && data.filename && document.getElementById('modal-overlay').classList.contains('show')) {
        return;
      }
      if (data && data.filename) {
        const titles = {
          applied:       '\\ud83d\\udcca Session Report Ingested',
          pending:       '\\ud83d\\udcca Session Report Queued',
          meet_mismatch: '\u26a0\ufe0f Session Report Not Applied',
          error:         '\u26a0\ufe0f Session Report Error',
        };
        document.getElementById('sr-title').textContent = titles[data.status] || '\\ud83d\\udcca Session Report';

        let summary;
        if (data.status === 'applied') {
          summary = 'Updated session for ' + (data.rows_updated ?? 0) + ' event row(s).';
          if (data.unmatched_events && data.unmatched_events.length) {
            summary += ' ' + data.unmatched_events.length + ' event(s) had no matching schedule row \u2014 see Log tab.';
          }
        } else if (data.status === 'pending') {
          summary = 'No active meet yet \u2014 will apply automatically once a schedule is approved.';
        } else if (data.status === 'meet_mismatch') {
          summary = data.message || 'Meet name did not match \u2014 not applied.';
        } else {
          summary = data.message || 'Could not process this file.';
        }
        document.getElementById('sr-summary').textContent = summary;

        document.getElementById('sr-meet-name').textContent = data.meet_name || '\u2014';
        document.getElementById('sr-filename').textContent  = data.filename  || '\u2014';
        clearInterval(sessionReportAgeTimer);
        sessionReportAgeTimer = startAgeTicker(data.file_mtime, 'sr-file-age');
        document.getElementById('session-report-overlay').classList.add('show');
      } else {
        clearInterval(sessionReportAgeTimer);
        document.getElementById('session-report-overlay').classList.remove('show');
      }
    });
}

function dismissSessionReportNotice() {
  fetch('/api/session_report/dismiss', {method: 'POST'})
    .then(() => {
      clearInterval(sessionReportAgeTimer);
      document.getElementById('session-report-overlay').classList.remove('show');
    });
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
      lastDashboardData = data;
      if (data.meet) {
        const raw = data.meet.meet_name || '';
        document.getElementById('meet-name').textContent = raw.replace(/\s*-\s*\d{1,2}\/\d{1,2}\/\d{4}\s+to\s+\d{1,2}\/\d{1,2}\/\d{4}\s*$/, '');
      }

      // Final Heat ETA bar
      const eta = data.final_eta;
      const etaBar = document.getElementById('eta-bar');
      if (eta && eta.time) {
        etaBar.textContent = 'Final Heat Start: ' + etaText(eta);
        etaBar.style.color = etaColor(eta.avg_delta);
        etaBar.classList.add('show');
      } else {
        etaBar.classList.remove('show');
      }

      const sessionEtas = data.session_etas || {};

      // Pool status blocks
      const rows = data.rows || [];
      const p1Last    = rows.find(r => r.is_last_p1);
      const p2Last    = rows.find(r => r.is_last_p2);
      const p1Current = rows.find(r => r.is_current_p1);
      const p2Current = rows.find(r => r.is_current_p2);
      const p1Next = rows.find(r => r.is_next_p1);
      const p2Next = rows.find(r => r.is_next_p2);
      const cp1 = data.companion_p1;
      const cp2 = data.companion_p2;

      const pool2Detected = cp2 || rows.some(r => r.pool === 2);
      document.getElementById('block-p2').style.display = pool2Detected ? '' : 'none';

      const fmtHeat = r => r ? 'Ev ' + r.event_id + '  Heat ' + heatDisplay(r.heat, r.heat_label) : '\u2014';
      const fmtCompanion = (matched, raw) => {
        if (!raw) return '\u2014';
        if (matched) return 'Ev ' + raw.event_id + '  Heat ' + raw.heat;
        return '(no match: Ev ' + raw.event_id + ' Heat ' + raw.heat + ')';
      };
      document.getElementById('p1-last').textContent    = p1Last ? fmtHeat(p1Last) + '  #' + p1Last.cts_race_num : '\u2014';
      document.getElementById('p1-current').textContent = fmtCompanion(p1Current, cp1);
      document.getElementById('p1-next').textContent    = fmtHeat(p1Next);
      document.getElementById('p2-last').textContent    = p2Last ? fmtHeat(p2Last) + '  #' + p2Last.cts_race_num : '\u2014';
      document.getElementById('p2-current').textContent = fmtCompanion(p2Current, cp2);
      document.getElementById('p2-next').textContent    = fmtHeat(p2Next);

      renderScheduleRows();
    });
}

// Sorts/groups the last-fetched rows per the schedule-tab toggles and renders
// them — split out from loadDashboard() so toggling either button re-renders
// instantly from cached data instead of waiting on another fetch.
function renderScheduleRows() {
  if (!lastDashboardData) return;
  const rows = (lastDashboardData.rows || []).slice().sort(compareRows);
  const sessionEtas = lastDashboardData.session_etas || {};
  lastEventId = null;
  lastSession = null;
  document.getElementById('race-table').innerHTML =
    rows.map(row => {
      let html = '';
      if (sessionGroupingOn && row.session !== lastSession) {
        const sEta = sessionEtas[row.session];
        const etaSpan = (sEta && sEta.time)
          ? '<span class="session-eta" style="color:' + etaColor(sEta.avg_delta) + '">' +
            'Final Heat: ' + etaText(sEta) + '</span>'
          : '';
        html += '<tr class="session-divider"><td colspan="16"><div class="session-divider-inner">' +
          etaSpan + sessionLabel(row.session) + '</div></td></tr>';
        lastSession = row.session;
      }
      return html + renderRow(row);
    }).join('');
}

// Returns "3 · A" when heat_label ends with a final letter, otherwise just heat number.
function heatDisplay(heat, heat_label) {
  if (!heat_label) return heat;
  const last = heat_label.trim().split(/\s+/).pop();
  if (/^[A-Z]$/i.test(last)) return heat + ' \u00b7 ' + last.toUpperCase();
  return heat;
}

// The Session Report only gives one time per event (heat 1) — falls back to
// the Meet Program's own effective_start for every other heat, and for heat 1
// itself when no Session Report has been ingested yet.
function projectedTimeFor(row) {
  if (timeSource === 'sessionreport' && row.session_report_start) return row.session_report_start;
  return row.effective_start;
}

function raceNumCell(row) {
  const gapCls = row.cts_gap_flag ? ' class="gap-flag"' : '';
  if (!row.race_num_history) return '<td' + gapCls + '>—</td>';
  const nums = row.race_num_history.split('\\n');
  const lines = nums.map((n, i) =>
    i === nums.length - 1 ? n : '<span class="prior-race-num">' + n + '</span>'
  );
  return '<td' + gapCls + '>' + lines.join('<br>') + '</td>';
}

// Dolphin cell shows only the current matched number — full CTS->Dolphin
// match history (across re-swims) is available on hover, not stacked inline.
function dolphinCell(row) {
  const val = (row.dolphin_race_num !== null && row.dolphin_race_num !== undefined) ? row.dolphin_race_num : '—';
  const history = row.dolphin_num_history ? row.dolphin_num_history.split('\\n') : [];
  const classes = [];
  if (row.dolphin_gap_flag) classes.push('gap-flag');
  if (history.length > 1) classes.push('has-history');
  const classAttr = classes.length ? ' class="' + classes.join(' ') + '"' : '';
  const titleAttr = history.length > 1 ? ' title="' + history.join('\\n').replace(/"/g, '&quot;') + '"' : '';
  return '<td' + classAttr + titleAttr + '>' + val + '</td>';
}

function renderRow(row) {
  const hasRace = row.cts_race_num !== null && row.cts_race_num !== undefined;

  // Row class — pool current-heat highlights take priority over heat-one
  let cls = '';
  if      (row.is_current_p1)            cls = 'current-p1';
  else if (row.is_current_p2)            cls = 'current-p2';
  else if (String(row.heat) === '1')     cls = 'heat-one';
  else if (!hasRace)                     cls = 'unmatched';

  // Event — hide duplicate
  const showEv = row.event_id !== lastEventId;
  lastEventId = row.event_id;
  const evCell = '<td>' + (showEv ? row.event_id : '') + '</td>';

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

  // CTS # with gap flag \u2014 stacked history if there was a re-swim
  const ctsCell = raceNumCell(row);

  // Dolphin # with gap flag and hover history
  const dolCell = dolphinCell(row);

  // Finish = CTS file creation time
  let finish = '\u2014';
  if (row.cts_file_time) {
    const t = row.cts_file_time;
    finish = t.length >= 19 ? t.substring(11, 19) : t;
  }

  return '<tr class="' + cls + '">' +
    evCell +
    '<td>' + heatDisplay(row.heat, row.heat_label) + '</td>' +
    '<td>' + (projectedTimeFor(row) || '\u2014') + '</td>' +
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
          let statusColor, statusText;
          if (row.status === 'error') {
            statusColor = '#ff6b6b';
            statusText  = row.error_message || 'error';
          } else if (row.status === 'pending' || row.status === 'queued') {
            statusColor = '#ffd700';
            statusText  = row.status;
          } else {
            statusColor = '#6bff6b';
            statusText  = row.status || '—';
          }
          const status = '<span style="color:' + statusColor + '">' + statusText + '</span>';
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
      '<td>' + heatDisplay(row.heat, row.heat_label) + '</td>' +
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
          const ctsCell = raceNumCell(row);
          const dolCell = dolphinCell(row);
          const datasetCell = '<td>' + (row.dolphin_dataset ?? '\u2014') + '</td>';
          const finish = row.cts_file_time
            ? (row.cts_file_time.length >= 19 ? row.cts_file_time.substring(11,19) : row.cts_file_time)
            : '\u2014';
          return '<tr class="' + cls + '">' +
            '<td class="left">' + (showEv ? row.event_id : '') + '</td>' +
            '<td>' + heatDisplay(row.heat, row.heat_label) + '</td>' +
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
// TRENDS
// ---------------------------------------------------------------------------
function loadTrends() {
  fetch('/api/trends')
    .then(r => r.json())
    .then(data => {
      if (data.error) {
        document.getElementById('trends-table').innerHTML =
          '<tr><td colspan="13" style="color:#888;padding:14px">' + data.error + '</td></tr>';
        return;
      }

      // Summary cell: one square per lane — green=present, red=missed, gray=inactive
      function summaryCell(active, arr) {
        const squares = [1,2,3,4,5,6,7,8].map(lane => {
          if (!active.includes(lane))
            return '<span style="display:inline-block;width:9px;height:9px;margin:0 1px;border-radius:2px;background:#2a2a2a;"></span>';
          const idx = lane - 1;
          const missed = idx < arr.length && arr[idx] === null;
          const color = missed ? '#ff4444' : '#44bb44';
          return '<span style="display:inline-block;width:9px;height:9px;margin:0 1px;border-radius:2px;background:' + color + ';"></span>';
        }).join('');
        return '<td style="white-space:nowrap">' + squares + '</td>';
      }

      let lastEvent = null;
      document.getElementById('trends-table').innerHTML =
        (data.rows || []).map(row => {
          const active  = row.active  || [];
          const off     = row.off     || [];
          const btnA    = row.btn_a   || [];
          const btnB    = row.btn_b   || [];
          const watchA  = row.watch_a || [];
          const watchB  = row.watch_b || [];
          const watchC  = row.watch_c || [];
          const hasDolphin = row.has_dolphin;
          const showEv = row.event_id !== lastEvent;
          lastEvent = row.event_id;

          function watchCell(active, arr, hasData) {
            if (!hasData) return '<td style="color:#333">\u2014</td>';
            return summaryCell(active, arr);
          }

          return '<tr>' +
            '<td>' + (showEv ? row.event_id : '') + '</td>' +
            '<td>' + heatDisplay(row.heat || '\u2014', row.heat_label) + '</td>' +
            summaryCell(active, off) +
            summaryCell(active, btnA) +
            summaryCell(active, btnB) +
            watchCell(active, watchA, hasDolphin) +
            watchCell(active, watchB, hasDolphin) +
            '</tr>';
        }).join('');
    });
}

// ---------------------------------------------------------------------------
// HARVESTED TIMES
// ---------------------------------------------------------------------------
const HARVESTED_SOURCES = [
  ['touchpad',  'Touchpad'],
  ['button_a',  'Button A'],
  ['button_b',  'Button B'],
  ['dolphin_a', 'Dolphin A'],
  ['dolphin_b', 'Dolphin B'],
  ['dolphin_c', 'Dolphin C'],
];

let harvestedHiddenCols = new Set(
  (localStorage.getItem('harvested_hidden_cols') || '').split(',').filter(Boolean)
);
// Which columns have at least one value in the most recently fetched rows --
// recomputed on every load, not persisted. A column with zero data is hidden
// regardless of the manual checkbox (nothing to show), but the checkbox
// itself still reflects the user's own preference so a column that later
// starts getting data reappears on its own, no re-toggling needed.
let harvestedColHasData = {};

function applyHarvestedColVisibility() {
  const wrap = document.getElementById('harvested-table-wrap');
  HARVESTED_SOURCES.forEach(([col]) => {
    const hide = harvestedHiddenCols.has(col) || harvestedColHasData[col] === false;
    wrap.classList.toggle('hide-' + col, hide);
  });
}

function toggleHarvestedCol(col, visible) {
  if (visible) harvestedHiddenCols.delete(col);
  else harvestedHiddenCols.add(col);
  localStorage.setItem('harvested_hidden_cols', Array.from(harvestedHiddenCols).join(','));
  applyHarvestedColVisibility();
}

function renderHarvestedColToggles() {
  document.getElementById('harvested-col-toggles').innerHTML =
    '<span style="color:#888;letter-spacing:1px;font-size:10px;">COLUMNS</span>' +
    HARVESTED_SOURCES.map(([col, label]) => {
      const checked = harvestedHiddenCols.has(col) ? '' : 'checked';
      const noData = harvestedColHasData[col] === false;
      const dimStyle = noData ? 'color:#555' : '';
      return '<label style="display:flex;align-items:center;gap:4px;cursor:pointer;' + dimStyle + '">' +
        '<input type="checkbox" ' + checked + ' onchange="toggleHarvestedCol(\\'' + col + '\\', this.checked)">' +
        label + (noData ? ' <span style="font-size:9px;">(no data)</span>' : '') + '</label>';
    }).join('');
}

function loadHarvestedTimes() {
  fetch('/api/harvested-times')
    .then(r => r.json())
    .then(data => {
      if (data.error) {
        document.getElementById('harvested-table').innerHTML =
          '<tr><td colspan="13" style="color:#888;padding:14px">' + data.error + '</td></tr>';
        return;
      }

      const rows = data.rows || [];
      HARVESTED_SOURCES.forEach(([col]) => {
        harvestedColHasData[col] = rows.some(r => r.sources[col] != null);
      });
      renderHarvestedColToggles();
      applyHarvestedColVisibility();

      const SOURCE_TAGS = { touchpad: 'pad', buttons: 'buttons', dolphin: 'Dolphin' };

      let lastEvent = null;
      document.getElementById('harvested-table').innerHTML =
        rows.map(row => {
          const showEv = row.event_id !== lastEvent;
          lastEvent = row.event_id;
          const cells = HARVESTED_SOURCES.map(([src]) => {
            const v = row.sources[src];
            if (v == null) return '<td class="col-' + src + '"><span style="color:#333">\\u2014</span></td>';
            const dev = row.deviations[src];
            const devStr = dev == null ? '' :
              '<br><span style="font-size:9px;color:#888">' + (dev > 0 ? '+' : '') + dev.toFixed(2) + '</span>';
            // Agreement is against the rule-adjudicated official time, not the
            // plain average -- same 0.30s threshold as the rulebook's own
            // malfunction indicator (102.23.4.C(1)), reused rather than
            // inventing a separate tolerance.
            let agreeClass = '';
            if (row.official_time != null) {
              agreeClass = Math.abs(v - row.official_time) < 0.30 ? ' cell-agree' : ' cell-disagree';
            }
            return '<td class="col-' + src + agreeClass + '">' + v.toFixed(2) + devStr + '</td>';
          }).join('');

          const officialCell = row.official_time == null
            ? '<span style="color:#333">\\u2014</span>'
            : row.official_time.toFixed(2) + '<br><span style="font-size:9px;color:#888">' +
              (SOURCE_TAGS[row.official_source] || row.official_source) + '</span>';

          const confidenceCell = row.flagged
            ? '<span style="color:#ff6b6b">&#9679; Check</span><br><span style="font-size:9px;color:#888">' +
              row.flags.map(escapeHtml).join('<br>') + '</span>'
            : (row.official_time == null
                ? '<span style="color:#555">&#9679; N/A</span>'
                : '<span style="color:#6bff6b">&#9679; OK</span>');

          return '<tr>' +
            '<td>' + (showEv ? row.event_id : '') + '</td>' +
            '<td>' + heatDisplay(row.heat || '\\u2014', row.heat_label) + '</td>' +
            '<td>' + row.lane + '</td>' +
            '<td>' + officialCell + '</td>' +
            '<td>' + confidenceCell + '</td>' +
            cells +
            '<td>' + row.average.toFixed(2) + '</td>' +
            '<td>' + row.max_deviation.toFixed(2) + '</td>' +
            '</tr>';
        }).join('');
    })
    .catch(() => {});
}

// ---------------------------------------------------------------------------
// CHECKLIST
// ---------------------------------------------------------------------------

function escapeHtml(s) {
  const div = document.createElement('div');
  div.textContent = s;
  return div.innerHTML;
}

function loadChecklist() {
  fetch('/api/checklist')
    .then(r => r.json())
    .then(data => {
      document.getElementById('checklist-no-meet').style.display = data.meet ? 'none' : '';
      const items = data.items || [];
      const tbody = document.getElementById('checklist-table');
      tbody.innerHTML = items.map(item => {
        const checkbox = '<input type="checkbox" ' + (item.checked ? 'checked' : '') +
          (data.meet ? '' : ' disabled') +
          ' onchange="toggleChecklistItem(' + item.id + ', this.checked)">';
        const manualTag = item.category === 'manual'
          ? '<span class="checklist-item-manual">MANUAL</span>' : '';
        let status = '<span style="color:#555">—</span>';
        if (item.category === 'auto') {
          const cls = item.auto_status === 'ok' ? 'ok'
                    : item.auto_status === 'fail' ? 'fail' : 'unknown';
          const label = item.auto_detail || (
            cls === 'unknown' ? 'Unknown' : cls === 'ok' ? 'OK' : 'Failed'
          );
          const lines = label.split('\\n');
          const body = lines.length > 1
            ? '<ul class="checklist-detail-list">' + lines.map(l => '<li>' + escapeHtml(l) + '</li>').join('') + '</ul>'
            : label;
          status = '<span class="checklist-status">' +
            '<span class="cl-dot ' + cls + '"></span>' + body + '</span>';
        }
        return '<tr>' +
          '<td>' + checkbox + '</td>' +
          '<td class="left">' + item.label + manualTag + '</td>' +
          '<td class="left">' + status + '</td>' +
          '</tr>';
      }).join('');
    })
    .catch(() => {});
  loadChecklistNotes();
}

function toggleChecklistItem(itemId, checked) {
  fetch('/api/checklist/check', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ item_id: itemId, checked: checked })
  }).catch(() => {});
}

function loadChecklistNotes() {
  fetch('/api/checklist/notes')
    .then(r => r.json())
    .then(data => {
      const notes = data.notes || [];
      const list = document.getElementById('checklist-notes-list');
      if (!notes.length) {
        list.innerHTML = '<div style="color:#555;font-size:11px;">No notes yet.</div>';
        return;
      }
      list.innerHTML = notes.map(n =>
        '<div class="checklist-note-row">' +
          '<div class="checklist-note-text">' + escapeHtml(n.note_text) + '</div>' +
          '<div class="checklist-note-meta">' + n.created_at +
            ' <button class="checklist-note-del" onclick="deleteChecklistNote(' + n.id + ')" title="Remove">&times;</button>' +
          '</div>' +
        '</div>'
      ).join('');
    })
    .catch(() => {});
}

function addChecklistNote() {
  const input = document.getElementById('checklist-note-input');
  const text = input.value.trim();
  if (!text) return;
  fetch('/api/checklist/notes', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ text: text })
  })
    .then(r => r.json())
    .then(() => {
      input.value = '';
      loadChecklistNotes();
    })
    .catch(() => {});
}

function deleteChecklistNote(noteId) {
  fetch('/api/checklist/notes/' + noteId, { method: 'DELETE' })
    .then(() => loadChecklistNotes())
    .catch(() => {});
}

// ---------------------------------------------------------------------------
// OBS CONTROL
// ---------------------------------------------------------------------------

function loadObsStatus() {
  fetch('/api/obs/status')
    .then(r => r.json())
    .then(data => {
      _renderObsInstance(1, data.obs1, data.configs && data.configs['1']);
      _renderObsInstance(2, data.obs2, data.configs && data.configs['2']);
      _renderObsSched(1, data.scheduled && data.scheduled['1']);
      _renderObsSched(2, data.scheduled && data.scheduled['2']);
      _renderObsSettingsAt(1, data.settings_at && data.settings_at['1']);
      _renderObsSettingsAt(2, data.settings_at && data.settings_at['2']);
    })
    .catch(() => {});
}

function _renderObsInstance(num, status, cfg) {
  const connDot     = document.getElementById('obs' + num + '-conn-dot');
  const connLabel   = document.getElementById('obs' + num + '-conn-label');
  const streamDot   = document.getElementById('obs' + num + '-stream-dot');
  const streamLabel = document.getElementById('obs' + num + '-stream-label');

  // Populate host from server config (only if user hasn't typed in the field)
  if (cfg) {
    const hostEl = document.getElementById('obs' + num + '-host');
    if (hostEl && !hostEl.dataset.edited) hostEl.value = cfg.host || 'localhost';
  }

  if (!status || !status.connected) {
    connDot.className     = 'obs-dot off';
    connLabel.textContent = status && status.error
      ? 'Offline \u2014 ' + status.error.substring(0, 50)
      : 'Offline';
    connLabel.style.color = '#888';
    streamDot.className     = 'obs-dot off';
    streamLabel.textContent = '--';
    streamLabel.style.color = '#888';
    return;
  }

  connDot.className     = 'obs-dot ok';
  connLabel.textContent = 'Connected' + (status.obs_version ? '  v' + status.obs_version : '');
  connLabel.style.color = '#6bff6b';

  if (status.streaming) {
    streamDot.className     = 'obs-dot live';
    streamLabel.textContent = 'LIVE';
    streamLabel.style.color = '#e94560';
  } else {
    streamDot.className     = 'obs-dot warn';
    streamLabel.textContent = 'Idle';
    streamLabel.style.color = '#ffd700';
  }
}

function _renderObsSettingsAt(num, t) {
  const msg = document.getElementById('obs' + num + '-settings-msg');
  if (!msg) return;
  // Only update the timestamp line; don't clobber a freshly-set status message
  if (t && !msg._userMsg) {
    msg.textContent = 'Last applied: ' + t;
    msg.style.color = '#555';
  }
}

function _renderObsSched(num, fireTime) {
  const label     = document.getElementById('obs' + num + '-sched-label');
  const cancelBtn = document.getElementById('obs' + num + '-cancel-btn');
  if (fireTime) {
    label.textContent   = 'Stream starts at ' + fireTime;
    cancelBtn.style.display = '';
  } else {
    label.textContent   = '';
    cancelBtn.style.display = 'none';
  }
}

function obsSaveConfig(num) {
  const host = document.getElementById('obs' + num + '-host').value.trim();
  const pass = document.getElementById('obs' + num + '-pass').value;
  const body = {};
  body['obs' + num] = { host: host, password: pass };
  fetch('/api/obs/config', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  }).then(() => loadObsStatus());
}

// Mark host fields as user-edited so auto-populate never overwrites them
['obs1-host','obs2-host'].forEach(function(id) {
  const el = document.getElementById(id);
  if (el) el.addEventListener('input', function() { el.dataset.edited = '1'; });
});

function obsApplySettings(num) {
  const url = document.getElementById('obs' + num + '-url').value.trim();
  const key = document.getElementById('obs' + num + '-key').value.trim();
  const msg = document.getElementById('obs' + num + '-settings-msg');
  if (!url) { msg.textContent = 'RTMP URL is required.'; msg.style.color = '#ff6b6b'; return; }
  msg.textContent = 'Applying...'; msg.style.color = '#888';
  msg._userMsg = true;
  fetch('/api/obs/stream_settings', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ instance: num, url: url, key: key })
  })
  .then(r => r.json())
  .then(data => {
    if (data.ok) {
      msg.textContent = 'Settings applied.'; msg.style.color = '#6bff6b';
    } else {
      msg.textContent = data.error || 'Failed.'; msg.style.color = '#ff6b6b';
    }
    setTimeout(() => { msg._userMsg = false; loadObsStatus(); }, 3000);
  })
  .catch(() => {
    msg.textContent = 'Request failed.'; msg.style.color = '#ff6b6b';
    setTimeout(() => { msg._userMsg = false; }, 3000);
  });
}

function obsStartNow(num) {
  const msg = document.getElementById('obs' + num + '-start-msg');
  msg.textContent = 'Switching to Intro...'; msg.style.color = '#888';
  fetch('/api/obs/start', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ instance: num })
  })
  .then(r => r.json())
  .then(data => {
    if (data.ok) {
      msg.textContent = 'Stream started.'; msg.style.color = '#6bff6b';
    } else {
      msg.textContent = data.error || 'Failed.'; msg.style.color = '#ff6b6b';
    }
    loadObsStatus();
  })
  .catch(() => { msg.textContent = 'Request failed.'; msg.style.color = '#ff6b6b'; });
}

function obsSetSchedule(num) {
  const t      = document.getElementById('obs' + num + '-sched-time').value;
  const offset = parseInt(document.getElementById('obs' + num + '-offset').value, 10) || 10;
  const label  = document.getElementById('obs' + num + '-sched-label');
  if (!t) { label.textContent = 'Enter a time first.'; label.style.color = '#ff6b6b'; return; }
  fetch('/api/obs/schedule', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ instance: num, time: t, offset_minutes: offset })
  })
  .then(r => r.json())
  .then(data => {
    if (data.error) {
      label.textContent = 'Error: ' + data.error; label.style.color = '#ff6b6b';
    } else {
      label.style.color = '#ffd700';
      loadObsStatus();
    }
  })
  .catch(() => { label.textContent = 'Request failed.'; label.style.color = '#ff6b6b'; });
}

function obsCancelSchedule(num) {
  fetch('/api/obs/schedule', {
    method: 'DELETE',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ instance: num })
  }).then(() => loadObsStatus());
}

// ---------------------------------------------------------------------------
// DOLPHIN5 CONTROL
// ---------------------------------------------------------------------------

let dolphin5MsgAgeTimers = { 1: null, 2: null };

function loadDolphin5Status() {
  fetch('/api/dolphin5/status')
    .then(r => r.json())
    .then(data => {
      const label = document.getElementById('dolphin5-running-label');
      label.textContent = data.running ? 'Running this session' : 'Not started this session';
      label.style.color = data.running ? '#6bff6b' : '#888';
      _renderDolphin5Pool(1, data.pool1, data.configs && data.configs['1']);
      _renderDolphin5Pool(2, data.pool2, data.configs && data.configs['2']);
    })
    .catch(() => {});
}

function _renderDolphin5Pool(num, status, cfg) {
  const prefix = 'dolphin5-' + num + '-';

  if (cfg) {
    const hostEl = document.getElementById(prefix + 'host');
    const portEl = document.getElementById(prefix + 'port');
    if (hostEl && !hostEl.dataset.edited) hostEl.value = cfg.host || '';
    if (portEl && !portEl.dataset.edited) portEl.value = cfg.port || '';
  }

  const connDot   = document.getElementById(prefix + 'conn-dot');
  const connLabel = document.getElementById(prefix + 'conn-label');
  if (!status || !status.connected) {
    connDot.className     = 'obs-dot off';
    connLabel.textContent = 'Offline';
    connLabel.style.color = '#888';
  } else {
    connDot.className     = 'obs-dot ok';
    connLabel.textContent = 'Connected';
    connLabel.style.color = '#6bff6b';
  }

  const sentEl = document.getElementById(prefix + 'sent');
  sentEl.textContent = (status && status.last_sent_event_index != null)
    ? ('event ' + status.last_sent_event_index + ' / heat ' + status.last_sent_heat)
    : '—';

  const seenEl = document.getElementById(prefix + 'seen');
  seenEl.textContent = (status && (status.last_seen_event_index != null || status.last_seen_event_number != null))
    ? ('index ' + (status.last_seen_event_index || '—') + ' / number ' + (status.last_seen_event_number || '—'))
    : '—';

  const msgEl = document.getElementById(prefix + 'last-msg');
  msgEl.textContent = (status && status.last_message) ? status.last_message : '—';

  clearInterval(dolphin5MsgAgeTimers[num]);
  dolphin5MsgAgeTimers[num] = (status && status.last_message_at)
    ? startAgeTicker(status.last_message_at, prefix + 'last-msg-age')
    : null;
  if (!status || !status.last_message_at) {
    document.getElementById(prefix + 'last-msg-age').textContent = '';
  }
}

// Mark host/port fields as user-edited so auto-populate never overwrites them mid-typing
['dolphin5-1-host','dolphin5-1-port','dolphin5-2-host','dolphin5-2-port'].forEach(function(id) {
  const el = document.getElementById(id);
  if (el) el.addEventListener('input', function() { el.dataset.edited = '1'; });
});

function dolphin5SaveConfig(num) {
  const host = document.getElementById('dolphin5-' + num + '-host').value.trim();
  const port = document.getElementById('dolphin5-' + num + '-port').value.trim();
  const body = {};
  body['pool' + num] = { host: host, port: port };
  fetch('/api/dolphin5/config', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  }).then(() => {
    // Allow auto-populate to reflect the just-saved value again
    document.getElementById('dolphin5-' + num + '-host').dataset.edited = '';
    document.getElementById('dolphin5-' + num + '-port').dataset.edited = '';
    loadDolphin5Status();
  });
}

function dolphin5Start() {
  fetch('/api/dolphin5/start', { method: 'POST' }).then(() => loadDolphin5Status());
}

// ---------------------------------------------------------------------------
// POLL
// ---------------------------------------------------------------------------
function poll() {
  checkPendingSchedule();
  checkSessionReportNotice();
  if (currentView === 'schedule') loadDashboard();
  else if (currentView === 'log') loadFullLog();
  else if (currentView === 'trends') loadTrends();
  else if (currentView === 'harvested') loadHarvestedTimes();
  else if (currentView === 'peripherals') { loadObsStatus(); loadDolphin5Status(); }
  else if (currentView === 'checklist') loadChecklist();
  // history view is not auto-refreshed — it's read-only static data
}

function updateHeaderHeight() {
  // getBoundingClientRect gives the exact sub-pixel height — offsetHeight rounds
  // to a whole pixel, which left a hairline gap where the sticky session-divider
  // row's computed top didn't quite match the thead's true rendered bottom edge.
  const h = document.getElementById('sticky-top').getBoundingClientRect().height;
  document.documentElement.style.setProperty('--header-height', h + 'px');
  const thead = document.getElementById('schedule-thead');
  if (thead) document.documentElement.style.setProperty('--thead-height', thead.getBoundingClientRect().height + 'px');
}
updateHeaderHeight();
window.addEventListener('resize', updateHeaderHeight);

function loadVersion() {
  fetch('/api/version')
    .then(r => r.json())
    .then(data => {
      const el = document.getElementById('version-tag');
      if (!data.commit) { el.textContent = ''; return; }
      el.textContent = data.commit + (data.dirty ? ' (uncommitted)' : '');
      el.classList.toggle('dirty', !!data.dirty);
    })
    .catch(() => {});
}

function initialLoad(attempt) {
  loadDashboard()
    .then(() => updateHeaderHeight())
    .catch(() => {
      updateHeaderHeight();
      if ((attempt || 0) < 15) setTimeout(() => initialLoad((attempt || 0) + 1), 1000);
    });
}
initialLoad();
loadVersion();
checkPendingSchedule();
checkSessionReportNotice();
updateScheduleToggleButtons();
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

@app.route("/api/version")
def api_version():
    return jsonify(get_git_version())


@app.route("/api/harvested-times")
def api_harvested_times():
    meet = get_active_meet()
    if not meet:
        return jsonify({"error": "No active meet", "rows": [], "meet": None})
    session = request.args.get("session")
    return jsonify({"meet": meet, "rows": get_harvested_times(meet["meet_id"], session)})


@app.route("/api/dashboard")
def api_dashboard():
    meet = get_active_meet()
    if not meet:
        return jsonify({"error": "No active meet", "rows": [], "meet": None, "pending": {}})
    session = request.args.get("session")
    rows = get_race_dashboard(meet["meet_id"], session)

    # Apply Companion current heat overrides if set
    companion_p1 = companion_state.get_raw(1)
    companion_p2 = companion_state.get_raw(2)
    if companion_p1 or companion_p2:
        for row in rows:
            row["is_current_p1"] = False
            row["is_current_p2"] = False

        def _resolve_companion(companion):
            """Return the single schedule row that best matches companion
            {event_id, heat} -- see database.resolve_heat_row() for the
            exact/ordinal-letter matching logic (shared with
            get_current_heat_state() so dashboard highlighting and the
            Dolphin5 chase loop agree on the same resolved heat)."""
            return resolve_heat_row(companion["event_id"], companion["heat"], rows)

        if companion_p1:
            match = _resolve_companion(companion_p1)
            if match:
                match["is_current_p1"] = True
        if companion_p2:
            match = _resolve_companion(companion_p2)
            if match:
                match["is_current_p2"] = True

    # Compute next heat per pool — first unrun row after each pool's current in schedule order
    ordered = sorted(rows, key=lambda r: r["heat_order"])
    for pool_key, next_key in [("is_current_p1", "is_next_p1"), ("is_current_p2", "is_next_p2")]:
        current = next((r for r in ordered if r.get(pool_key)), None)
        if current:
            nxt = next((r for r in ordered if r["heat_order"] > current["heat_order"]), None)
            if nxt:
                nxt[next_key] = True

    # Same ETA logic as the overall final_eta, scoped to each session's own rows
    session_etas = {
        sess: _compute_final_eta([r for r in rows if r["session"] == sess])
        for sess in {r["session"] for r in rows}
    }

    return jsonify({
        "meet":        meet,
        "rows":        rows,
        "pending":     get_pending_summary(),
        "final_eta":   _compute_final_eta(rows),
        "session_etas": session_etas,
        "companion_p1": companion_p1,
        "companion_p2": companion_p2,
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


@app.route("/api/session_report/notice")
def api_session_report_notice():
    return jsonify(get_session_report_notice() or {})


@app.route("/api/session_report/dismiss", methods=["POST"])
def api_dismiss_session_report_notice():
    dismiss_session_report_notice()
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
    event = request.args.get("event")
    heat  = request.args.get("heat")
    if event is None or heat is None:
        return jsonify({"error": "Missing event or heat parameter"}), 400
    companion_state.set_heat(1, event, heat)
    log.info(f"Companion P1 heat set: Event={event} Heat={heat}")
    return jsonify({"status": "ok", "pool": 1, "event_id": event, "heat": heat})


@app.route("/api/companion/pool1/clear_heat", methods=["POST"])
def api_companion_clear_heat_p1():
    """Clear Pool 1 Companion override — reverts to auto-detection."""
    companion_state.clear_heat(1)
    log.info("Companion P1 heat override cleared")
    return jsonify({"status": "ok", "pool": 1})


@app.route("/api/companion/pool2/set_heat", methods=["POST"])
def api_companion_set_heat_p2():
    """Set Pool 2 current heat from Bitfocus Companion.
    POST /api/companion/pool2/set_heat?event=$(streamline_2:event)&heat=$(streamline_2:heat)
    """
    event = request.args.get("event")
    heat  = request.args.get("heat")
    if event is None or heat is None:
        return jsonify({"error": "Missing event or heat parameter"}), 400
    if event == "$NA" or heat == "$NA":
        # Bitfocus Companion's own "variable unresolved" sentinel — not a real
        # heat, so ignore it rather than letting it force the Pool 2 block visible.
        log.info(f"Companion P2 heat set ignored (unresolved $NA): Event={event} Heat={heat}")
        return jsonify({"status": "ignored", "reason": "unresolved $NA"}), 200
    companion_state.set_heat(2, event, heat)
    log.info(f"Companion P2 heat set: Event={event} Heat={heat}")
    return jsonify({"status": "ok", "pool": 2, "event_id": event, "heat": heat})


@app.route("/api/companion/pool2/clear_heat", methods=["POST"])
def api_companion_clear_heat_p2():
    """Clear Pool 2 Companion override — reverts to auto-detection."""
    companion_state.clear_heat(2)
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
# TRENDS
# ---------------------------------------------------------------------------

@app.route("/api/trends")
def api_trends():
    meet = get_active_meet()
    if not meet:
        return jsonify({"error": "No active meet", "rows": []})

    from database import get_conn
    with get_conn() as conn:
        race_rows = conn.execute(
            """SELECT r.event_id, r.heat, r.active_lanes, r.off_times,
                      r.button_a_times, r.button_b_times,
                      r.dolphin_watch_a, r.dolphin_watch_b, r.dolphin_watch_c,
                      r.dolphin_filename IS NOT NULL AS has_dolphin,
                      s.heat_label, s.heat_order
               FROM race_log r
               LEFT JOIN schedule s
                 ON s.meet_id = r.meet_id
                 AND s.event_id = r.event_id
                 AND s.heat = r.heat
               WHERE r.meet_id = ? AND r.active_lanes IS NOT NULL
               ORDER BY r.id ASC""",
            (meet["meet_id"],)
        ).fetchall()

    def parse_arr(raw):
        try:
            return json.loads(raw) if raw else [None] * 8
        except Exception:
            return [None] * 8

    rows = []
    for row in race_rows:
        try:
            active = [int(x) for x in row["active_lanes"].split(",") if x.strip()]
        except Exception:
            active = []
        rows.append({
            "event_id":   row["event_id"],
            "heat":       row["heat"],
            "heat_label": row["heat_label"],
            "active":     active,
            "off":        parse_arr(row["off_times"]),
            "btn_a":      parse_arr(row["button_a_times"]),
            "btn_b":      parse_arr(row["button_b_times"]),
            "has_dolphin": bool(row["has_dolphin"]),
            "watch_a":    parse_arr(row["dolphin_watch_a"]),
            "watch_b":    parse_arr(row["dolphin_watch_b"]),
            "watch_c":    parse_arr(row["dolphin_watch_c"]),
        })

    return jsonify({"meet": meet, "rows": rows})


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


# ---------------------------------------------------------------------------
# CLIENT HEARTBEAT
# ---------------------------------------------------------------------------

@app.route("/api/heartbeat", methods=["POST"])
def api_heartbeat():
    """Receive a status heartbeat from a client machine."""
    data = request.json or {}
    machine_id = data.get("machine_id")
    if not machine_id:
        return jsonify({"error": "machine_id required"}), 400
    with _clients_lock:
        _clients[machine_id] = {
            "machine_id":     machine_id,
            "last_seen":      datetime.now(),
            "ahk_scripts":    data.get("ahk_scripts", []),
            "vicreo_running": data.get("vicreo_running"),
            "share_ok":       data.get("share_ok"),
            "dolphin_ok":     data.get("dolphin_ok"),
            "cts_ok":         data.get("cts_ok"),
        }
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# PRE-SESSION CHECKLIST
# ---------------------------------------------------------------------------

def _client_rows():
    """Last known status for all client.py machines, used by the Timing
    Machine Clients checklist item (this used to be its own Clients tab --
    retired 2026-08-02, folded into the checklist instead)."""
    now = datetime.now()
    with _clients_lock:
        rows = [
            {
                "machine_id":     c["machine_id"],
                "online":         (now - c["last_seen"]).total_seconds() <= HEARTBEAT_STALE,
                "last_seen":      c["last_seen"].strftime("%H:%M:%S"),
                "ahk_scripts":    c["ahk_scripts"],
                "vicreo_running": c["vicreo_running"],
                "share_ok":       c["share_ok"],
                "dolphin_ok":     c["dolphin_ok"],
                "cts_ok":         c["cts_ok"],
            }
            for c in _clients.values()
        ]
    rows.sort(key=lambda r: r["machine_id"])
    return rows


@app.route("/api/checklist")
def api_checklist():
    meet = get_active_meet()
    items = get_checklist_items()
    context = {
        "meet": meet,
        "companion_connected": bool(companion_state.get_raw(1) or companion_state.get_raw(2)),
        "clients": _client_rows(),
    }
    items = checklist.evaluate_items(items, context)
    state = get_checklist_state(meet["meet_id"]) if meet else {}
    for item in items:
        s = state.get(item["id"])
        item["checked"] = bool(s and s["checked"])
        item["checked_at"] = s["checked_at"] if s else None
    return jsonify({"items": items, "meet": meet})


@app.route("/api/checklist/check", methods=["POST"])
def api_checklist_check():
    meet = get_active_meet()
    if not meet:
        return jsonify({"error": "No active meet"}), 400
    data = request.json or {}
    item_id = data.get("item_id")
    checked = bool(data.get("checked"))
    if not item_id:
        return jsonify({"error": "item_id required"}), 400
    set_checklist_state(item_id, meet["meet_id"], checked)
    return jsonify({"ok": True})


@app.route("/api/checklist/notes", methods=["GET"])
def api_checklist_notes_get():
    return jsonify({"notes": get_checklist_notes()})


@app.route("/api/checklist/notes", methods=["POST"])
def api_checklist_notes_post():
    data = request.json or {}
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"error": "text required"}), 400
    note_id = add_checklist_note(text)
    return jsonify({"ok": True, "id": note_id})


@app.route("/api/checklist/notes/<int:note_id>", methods=["DELETE"])
def api_checklist_notes_delete(note_id):
    ok = delete_checklist_note(note_id)
    return jsonify({"ok": ok})


# ---------------------------------------------------------------------------
# OBS CONTROL
# ---------------------------------------------------------------------------

import obs_control


@app.route("/api/obs/status")
def api_obs_status():
    """Return connection + stream status for both OBS instances."""
    return jsonify({
        "obs1":        obs_control.get_status(1),
        "obs2":        obs_control.get_status(2),
        "configs":     {str(k): v for k, v in obs_control.get_configs().items()},
        "scheduled":   {str(k): v for k, v in obs_control.get_scheduled_times().items()},
        "settings_at": {str(k): v for k, v in obs_control.get_settings_applied_at().items()},
    })


@app.route("/api/obs/config", methods=["POST"])
def api_obs_config():
    """Update host/password for one or both OBS instances."""
    data = request.json or {}
    for i in [1, 2]:
        key = f"obs{i}"
        if key in data:
            obs_control.update_config(i, **{
                k: v for k, v in data[key].items()
                if k in ("host", "port", "password")
            })
    return jsonify({"ok": True})


@app.route("/api/obs/stream_settings", methods=["POST"])
def api_obs_stream_settings():
    """Push RTMP URL and stream key to one OBS instance."""
    data     = request.json or {}
    instance = int(data.get("instance", 0))
    url      = data.get("url", "").strip()
    key      = data.get("key", "").strip()
    if instance not in (1, 2):
        return jsonify({"error": "instance must be 1 or 2"}), 400
    if not url:
        return jsonify({"error": "url is required"}), 400
    return jsonify(obs_control.set_stream_settings(instance, url, key))


@app.route("/api/obs/start", methods=["POST"])
def api_obs_start():
    """Switch to Intro scene, wait 5s, then start streaming on one OBS instance."""
    data     = request.json or {}
    instance = int(data.get("instance", 0))
    if instance not in (1, 2):
        return jsonify({"error": "instance must be 1 or 2"}), 400
    return jsonify(obs_control.start_stream(instance))


@app.route("/api/obs/schedule", methods=["POST"])
def api_obs_schedule():
    """Schedule one OBS instance to start streaming at a given time."""
    data           = request.json or {}
    instance       = int(data.get("instance", 0))
    t              = data.get("time", "").strip()
    offset_minutes = int(data.get("offset_minutes", 10))
    if instance not in (1, 2):
        return jsonify({"error": "instance must be 1 or 2"}), 400
    if not t:
        return jsonify({"error": "time is required"}), 400
    try:
        result = obs_control.schedule_stream_start(instance, t, offset_minutes)
        return jsonify(result)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/obs/schedule", methods=["DELETE"])
def api_obs_cancel_schedule():
    """Cancel a pending scheduled stream start for one OBS instance."""
    data     = request.json or {}
    instance = int(data.get("instance", 0))
    if instance not in (1, 2):
        return jsonify({"error": "instance must be 1 or 2"}), 400
    obs_control.cancel_schedule(instance)
    return jsonify({"cancelled": True})


import dolphin5_control


@app.route("/api/dolphin5/status")
def api_dolphin5_status():
    """Return connection/chase status + current config for both pools."""
    return jsonify({
        "running": dolphin5_control.is_running(),
        "pool1":   dolphin5_control.get_connection_status(1),
        "pool2":   dolphin5_control.get_connection_status(2),
        "configs": {
            "1": dolphin5_control.get_effective_config(1),
            "2": dolphin5_control.get_effective_config(2),
        },
    })


@app.route("/api/dolphin5/config", methods=["POST"])
def api_dolphin5_config():
    """Save host/port for one Dolphin5 pool. Persists and reconnects immediately."""
    data = request.json or {}
    for i in (1, 2):
        key = f"pool{i}"
        if key in data:
            host = (data[key].get("host") or "").strip() or None
            port = data[key].get("port")
            port = int(port) if port not in (None, "") else None
            dolphin5_control.update_config(i, host=host, port=port)
    return jsonify({"ok": True})


@app.route("/api/dolphin5/start", methods=["POST"])
def api_dolphin5_start():
    """Start Dolphin5 TCP control for this running session. Idempotent."""
    dolphin5_control.start()
    return jsonify({"running": dolphin5_control.is_running()})
