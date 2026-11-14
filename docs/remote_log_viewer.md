# Remote log viewer (backlog idea, 2026-11-14)

Status: idea only, not built.

## Goal

From inside CTSTracking, view each timing PC's logs without walking to that PC:

- **CTS_Client log**: `C:\CTSClient\client.log`
- **Dolphin5 AHK log**: `C:\Temp\dolphin5_hud.txt` (written by `ahk/Dolphin5_FourMode.ahk`)

## What exists today

- CTS_Client already copies `client.log` to the share every 5 minutes:
  `\\CSAC-001\swmeets8\racenumbers\ip<octet>_<computer>\client.log` (see the log mirror loop in `client.py`).
  No CTSTracking page or API reads it.
- `dolphin5_hud.txt` is not copied anywhere; it only exists on the local Dolphin5 PC.
- CTSTracking only receives each client's heartbeat (version, IP, last seen).

## Options

1. **Show the share copies (recommended first step).** Add a "View log" link to each client row in
   Peripherals that shows the last few hundred lines of the mirrored file. Up to 5 minutes out of date.
   For the Dolphin5 log, CTS_Client would also need to copy `dolphin5_hud.txt` to the share
   (new client version, rolled out to the timing PCs).
2. **Live view.** CTS_Client serves its recent log lines itself (small HTTP port, or sent with the
   heartbeat). Works even when the share is down; needs a new client version on all 4 timing PCs.

## Known limits of option 1

- Only the current `client.log` is copied, not the rotated older files (`client.log.1` and so on).
- No new copies arrive while the share is unreachable.
