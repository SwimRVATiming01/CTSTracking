# CTS Tracker

Python/Flask server that tracks swim meet timing data live during competitions: ingests CTS GEN7 timing files and Dolphin backup stopwatch files from a network share, correlates them, imports Meet Manager schedules, and drives a live dashboard plus Bitfocus Companion / OBS integration.

- **Entry point:** `cts_tracker.py` (`python cts_tracker.py`, or `start_server.bat`)
- **Dashboard:** Flask on port 5000 (`http://0.0.0.0:5000`)
- **Database:** SQLite at `~/Documents/cts_tracker/cts_tracker.db` — **outside this repo**, not covered by git or by moving this folder. Also holds `backups/`, `snapshots/`, and `cts_tracker.log`.
- **Dependencies:** `pip install -r requirements.txt`

## Architecture

- `config.py` — all paths/thresholds/Flask settings. `WATCH_DIR` points at `\\CSAC-001\swmeets8\racenumbers` (network share); only reachable on the same LAN as the timing machines.
- `watchdog_monitor.py` — file watcher for incoming `.gen`/`.oxps`/`.do3`/schedule CSVs
- `parsers.py` — `.oxps` (OpenXPS/ZIP/XML glyph extraction, layout-fragile) and `.do3` parsers
- `ingestion.py` — Meet Manager CSV import (heat sheet + Session Report), CTS/Dolphin correlation, schedule approval workflow
- `database.py` — schema + all DB read/write functions
- `routes.py` — Flask routes **and** the entire dashboard HTML/CSS/JS as one big embedded Python string (`DASHBOARD_HTML`) — see gotcha below before editing it
- `checklist.py` — pre-session checklist auto-check logic
- `obs_control.py` — OBS WebSocket control, background-polled status cache
- `client.py` — runs on timing machines, forwards files to the network share (separate from the server; needs `psutil` too)

## ⚠️ Gotcha: editing JS inside `DASHBOARD_HTML`

It's a plain (non-raw) Python triple-quoted string. Python resolves its **own** escapes (`\n`, `\t`, `\uXXXX`) at parse time, before the JS ever reaches a browser — this silently breaks JS that needs those same escapes at runtime, and `py_compile`/`ast.parse` won't catch it (it's valid Python either way).

- `'\n'` meant as a JS newline (e.g. `.split('\n')`) → Python turns it into an actual embedded newline inside the JS string literal → hard JS syntax error, whole `<script>` block dies, dashboard sticks on "Loading..." with zero server-side error.
- An emoji written as a JS surrogate pair (e.g. `'📊'`) → Python resolves each half independently instead of recombining them like JS does → two invalid lone surrogates in the runtime string → Flask 500s the first time that string is actually sent (UTF-8 encoding of a lone surrogate fails). `py_compile` is silent about this too.
- **Fix both cases the same way:** double the backslash (`'\\n'`, `'\\ud83d\\udcca'`) so Python leaves the literal escape text alone for the *browser's* JS engine to interpret. Single-codepoint escapes below U+10000 (`—`, `⚠`, etc.) don't need this — they resolve to one valid character either way.
- **Do NOT "fix" this by making the string raw** (`r"""..."""`) — several spots already correctly use the doubled-backslash workaround for the non-raw regime; a blind raw-string conversion would break those specific spots instead.
- **How to verify a change is actually safe:** `ast.parse` the file, find the `DASHBOARD_HTML` assignment, and call `.encode('utf-8')` on `node.value.value` (the fully-Python-parsed runtime string) — a `UnicodeEncodeError` means a lone-surrogate landmine. This catches what `py_compile` can't. There's no JS linter wired up here (Node wasn't available in the dev environment this was built in), so actual JS syntax validity still needs eyeballing/grep for stray raw newlines inside string literals.

## Recent history (as of 2026-08-01)

A backlog of uncommitted feature work (built over 2026-07-21 to 2026-07-31, never committed) was split into 9 logical commits and pushed: MM Session Report ingestion, combined Girls/Boys heat-sheet event parsing, an `INGEST_CTS_ENABLED` toggle (`.oxps` ingestion is currently OFF — `.gen` has full coverage, `.oxps` code kept until `.gen` is proven rock-solid), the pre-session operator checklist (v1 — see "deferred" below), dashboard sort/session-grouping/gap-flag-accuracy/re-swim-history, a Bitfocus Companion `$NA`-sentinel guard, and moving OBS status polling to a background thread instead of blocking dashboard requests.

Also fixed same day: `watchdog_monitor.py` was using `watchdog.observers.Observer` (native `ReadDirectoryChangesW` on Windows) to watch `WATCH_DIR`, a UNC network path. That backend's change-notification handle can go silently stale after an SMB session drop (share host reboot, network blip, machine sleep) — no error raised, ingestion just stops with nothing logged, until the process is restarted. Switched to `watchdog.observers.polling.PollingObserver`, which re-lists the directory each interval instead of holding a handle. Diagnosed from a live incident: overnight run, ~44 minutes of missing `.gen`/`.do3` ingestion this morning despite meet activity clearly continuing per other logs (Companion heat updates, OBS start), zero watchdog log lines in that window.

## Deliberately deferred / open questions

- **Checklist session-scoping unresolved** — `checklist_state` is meet-scoped only (no `session` column). User's instinct was per-session but there's no session-selector UI to hang it off yet; don't add one speculatively.
- **Append-mode schedule ordering** — heats added via "Append to Schedule" get `heat_order` tacked onto the end of the sequence, not inserted at their true chronological position. User explicitly declined a fix for this ("I can see it going either way") — only revisit if it becomes an actual pain point.
- **No admin UI for checklist items** — items are DB rows, edited directly (in practice, via Claude). Accepted as the interim workflow; build a self-service UI only once real usage shows it's needed.
- **The 4 seeded checklist auto-checks are a framework proof, not a final list** — expect them to be edited/replaced once the user has actually run the pre-meet routine live a few times.
