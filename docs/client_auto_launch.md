# CTS_Client: launch missing programs (backlog idea, 2026-11-14)

Status: idea only, not built.

## Goal

CTS_Client already reports which programs are running on each timing PC
(`REPORTED_PROGRAMS` in `client.py`: vhui64.exe, dolphin50.exe, gen7swimming.exe, plus running AHK scripts).
Have it also start programs that should be running but aren't.

## Open decisions

1. **Automatic vs. on request.** Auto-restarting Dolphin5 or GEN7 mid-race could interfere. Options:
   - "Launch" button per missing program on the Peripherals client list (operator decides).
   - Automatic relaunch only for programs switched on for it (AHK scripts, VirtualHere are safe fits).
2. **Which programs per PC.** GEN7 and Dolphin PCs differ; keep the per-PC list in CTSTracking and send it
   back in the heartbeat reply so no PC needs editing.
3. **Program paths** on each PC.
4. **Launch at boot** as a one-click pre-meet setup.

Suggested start: Launch button for everything + auto-relaunch only for AHK scripts.
Any version needs a new client release on all 4 timing PCs.

## AHK scripts: launch newest in a folder

Instead of a fixed script path, point CTS_Client at a folder and launch the most recently modified `.ahk`.

Risks: an old script saved by accident becomes "newest"; copying files can reset dates; a shared folder
(e.g. Google Drive) could make a Dolphin PC launch a GEN7 script; `- Copy.ahk` backups count as scripts.

Safeguards:
1. One local folder per PC (e.g. `C:\CTSClient\ahk`), only that PC's scripts, not on Google Drive.
2. Only launch when no AHK script is already running.
3. Show which file was picked and its date on the Peripherals client list.
4. Optional: skip names containing "copy", "old" or "backup".
