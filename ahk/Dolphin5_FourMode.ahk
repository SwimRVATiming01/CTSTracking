#Requires AutoHotkey v2.0
#SingleInstance Force      ; Re-running the script replaces the old copy instead of starting a second one
#Warn                      ; Warn about likely mistakes (e.g. misspelled variables)
SendMode("Input")          ; Fastest, most reliable way to send keystrokes
SetWorkingDir(A_ScriptDir) ; Relative paths resolve from this script's folder

; Dolphin5 + Meet Manager, one pool per machine.
; This script NEVER resets Dolphin5 -- Companion sends `reset` over TCP.
; Companion button order: TCP reset, then press F15 (Next Heat) or F14 (Next Event).
; This script closes Dolphin5's reset confirmation dialog, waits 5s, then drives Meet Manager.
;
; Same script on both pool PCs. CTSTracking works out which pool this PC is by matching its IP
; to that pool's Dolphin5 TCP host, so there is nothing to set per PC.
; Each heat it asks CTSTracking (Peripherals > "AHK MM import", one row per pool) which of four branches to run:
;   none       -- no Meet Manager interaction at all: no window switching, no keys, no F5/^F5/^D
;                 (use while Meet Manager is closed or restarting; CTSTracking also answers this
;                 when this PC matches neither pool's Dolphin5 host)
;   dolphin    -- Dolphin times only (the original behaviour)
;   both       -- pull CTS times (F3) first, then the Dolphin steps
;   adjudicate -- pull CTS times (F3), Dolphin steps, then adjudicate (Ctrl+K, Enter) before advancing
; The reset-dialog auto-close runs in every mode, including none.
; If CTSTracking can't be reached or answers anything else, it runs "none" (leave Meet Manager alone).
;
; Key presses only: CTS_Client.exe reports this PC (and this script) to CTSTracking.

; -------------------------------------------------
; SETTINGS
; -------------------------------------------------
; CTS_Client.exe writes the server address it found here; fallback if it hasn't.
SERVER_FILE        := "C:\CTSClient\server.txt"
FALLBACK_SERVER    := "http://172.16.0.82:5000"
CTS_PULL_WAIT_MS   := 8000   ; wait after F3 for Meet Manager to load CTS times
ADJUDICATE_WAIT_MS := 4000   ; wait after Ctrl+K before pressing Enter
DIALOG_POLL_MS     := 100    ; how often to look for Dolphin5's reset dialog
RESULTS_WAIT_SEC   := 3      ; wait for Dolphin5 to save results before touching Meet Manager

DOLPHIN_EXE        := "ahk_exe Dolphin50.exe"
RESET_DIALOG       := "ahk_class #32770 ahk_exe Dolphin50.exe"
RESET_DIALOG_TEXT  := "This operation will reset the current race"
MEET_MANAGER_WIN   := "Run the Meet"

; -------------------------------------------------
; LOG WINDOW
; -------------------------------------------------
; Own file name so it can't collide with the Dolphin4 script's C:\Temp\cts_hud.txt.
logFile := "C:\Temp\dolphin5_hud.txt"
if !DirExist("C:\Temp")         ; make sure the log folder exists
    DirCreate("C:\Temp")
try FileAppend("", logFile)     ; create the log file if it isn't there yet
; Reuse the log window across reloads instead of opening another one each time.
LOG_WINDOW_TITLE := "Dolphin5 Log"
if !WinExist(LOG_WINDOW_TITLE) && !WinExist("Administrator: " LOG_WINDOW_TITLE) {
    ; PowerShell window that shows the log live (like `tail -f`).
    ; If a read collides with a write, restart the tail (new lines only) instead of dying.
    psLoop := "$Host.UI.RawUI.WindowTitle = '" LOG_WINDOW_TITLE "'; $n = 100; "
            . "while ($true) { try { Get-Content '" logFile "' -Wait -Tail $n -ErrorAction Stop } "
            . "catch { Start-Sleep -Milliseconds 300; $n = 0 } }"
    Run('PowerShell.exe -NoExit -Command "' psLoop '"')
    Sleep(800)                  ; give the window a moment to open
}

; Writes one timestamped line to the log.
; FileOpen shares read/write access (FileAppend doesn't), so the log window can read while
; this writes. A write that still fails is skipped -- logging must never stop the script.
Log(line) {
    global logFile
    try {
        f := FileOpen(logFile, "a", "UTF-8-RAW")
        f.Write(FormatTime(, "HH:mm:ss") " " line "`n")
        f.Close()
    }
}

Log("Dolphin5 script (four-mode) started on " A_ComputerName)

; -------------------------------------------------
; RESET DIALOG AUTO-CLOSE
; Dolphin5 blocks all TCP commands while this dialog is open.
; ControlClick doesn't need focus, so it won't disturb Meet Manager keystrokes.
; -------------------------------------------------
SetTimer(CloseResetDialog, DIALOG_POLL_MS)   ; check for the dialog every 100 ms, forever

CloseResetDialog() {
    global RESET_DIALOG, RESET_DIALOG_TEXT
    static dialogHwnd := 0, clicks := 0
    ; Only a window that no longer exists counts as closed; a failed text read just retries next tick.
    if dialogHwnd && !WinExist("ahk_id " dialogHwnd) {
        Log("Reset dialog closed (" clicks " click" (clicks = 1 ? "" : "s") ")")
        dialogHwnd := 0
    }
    try {
        hwnd := WinExist(RESET_DIALOG)                                  ; any Dolphin5 dialog open?
        if hwnd && InStr(WinGetText(hwnd), RESET_DIALOG_TEXT) {         ; ...and it's the reset one?
            if (hwnd != dialogHwnd) {                                   ; first time we've seen this one
                dialogHwnd := hwnd
                clicks := 0
                Log("Reset dialog detected")
            }
            clicks++
            ControlClick("OK", hwnd)                                    ; press its OK button without focusing it
        }
    }
}

; -------------------------------------------------
; MEET MANAGER SEQUENCE
; -------------------------------------------------
; Logs a per-second countdown while waiting.
Countdown(seconds) {
    Loop seconds {
        Log("    " (seconds - A_Index + 1) "s...")
        Sleep(1000)
    }
}

; Runs once per Companion press. advanceKey is F5 (Next Heat) or Ctrl+F5 (Next Event).
RunMeetManager(action, advanceKey) {
    global MEET_MANAGER_WIN, RESULTS_WAIT_SEC, CTS_PULL_WAIT_MS, ADJUDICATE_WAIT_MS
    Log("")
    Log("==============================")
    Log("ACTION: " action)
    Log("==============================")

    ; Ask CTSTracking which branch to run this heat -- first, so "none" touches nothing.
    mode := AskImportMode()
    Log("--> Mode: " mode)
    if (mode = "none") {
        Log("--- No MM: Meet Manager left alone ---")
        return
    }

    ; Give Dolphin5 time to save this race's results before Meet Manager reads them.
    Log("--> Waiting for Dolphin5 results")
    Countdown(RESULTS_WAIT_SEC)

    ; Bring Meet Manager's Run the Meet window to the front (only done once per heat;
    ; every key press below checks Meet Manager is still in front and stops if not).
    Log("--> Open Run the Meet")
    if !WinExist(MEET_MANAGER_WIN) {
        Log("[ERROR] Run the Meet window not found")
        return
    }
    WinActivate(MEET_MANAGER_WIN)
    if !WinWaitActive(MEET_MANAGER_WIN, , 2) {   ; wait up to 2 s for it to actually come forward
        Log("[ERROR] Could not activate Run the Meet")
        return
    }
    Sleep(1000)                                  ; let Meet Manager settle before keystrokes

    ; Branches "both" and "adjudicate": pull in CTS times first.
    if (mode = "both" || mode = "adjudicate") {
        Log("--> Pull CTS times (F3)")
        if !SendToMM("{F3}")                     ; F3: get CTS (GEN7) times into Meet Manager
            return
        Sleep(CTS_PULL_WAIT_MS)                  ; wait for the CTS times to populate
    }

    ; All branches: the Dolphin steps.
    Log("--> Select Race")
    if !SendToMM("^w")                           ; Ctrl+W: open the dolphin race picker
        return
    Sleep(1000)                                  ; at least 1 s between every command
    if !SendToMM("{Enter}")                      ; Enter: pick the highlighted race
        return
    Sleep(3000)                                  ; wait for the race to load

    Log("--> Verify Dolphin race")
    if !SendToMM("a")                            ; a: verify the Dolphin race
        return
    Sleep(1000)

    ; Branch "adjudicate" only: adjudicate the CTS + Dolphin times, before advancing.
    if (mode = "adjudicate") {
        Log("--> Adjudicate times (Ctrl+K, Enter)")
        if !SendToMM("^k")                       ; Ctrl+K: open Meet Manager's adjudicate step
            return
        Sleep(ADJUDICATE_WAIT_MS)                ; wait for it to come up
        if !SendToMM("{Enter}")                  ; Enter: accept / confirm the adjudication
            return
        Sleep(1000)                              ; let it finish before advancing
    }

    Log("--> Advance to next heat/event")
    if !SendToMM(advanceKey)                     ; F5 / Ctrl+F5: advance to the next heat / event, ready for the next run
        return
    Sleep(5000)                                  ; wait for Meet Manager to advance

    Log("--> Resync Meet Manager")
    if !SendToMM("^d")                           ; Ctrl+D: resync the Meet Manager client with the Meet Manager Access database
        return

    ; No longer switches back to Dolphin5 afterwards (Meet Manager stays in front).
    Log("--- Complete ---")
}

; Asks CTSTracking which branch to run: "none", "dolphin", "both" or "adjudicate".
; Anything else (server down, timeout, error, odd reply) means "none" -- leave Meet Manager alone.
AskImportMode() {
    global SERVER_FILE, FALLBACK_SERVER
    server := FALLBACK_SERVER
    try {
        line := Trim(FileRead(SERVER_FILE), " `t`r`n")   ; address written by CTS_Client.exe
        if (SubStr(line, 1, 7) = "http://")
            server := line
    }
    try {
        req := ComObject("WinHttp.WinHttpRequest.5.1")    ; built-in Windows web request
        req.SetTimeouts(2000, 2000, 2000, 2000)           ; give up after 2 s rather than stall the heat
        req.Open("GET", server "/api/ahk/import_mode?plain=1", false)
        req.Send()
        reply := Trim(req.ResponseText)
        if (req.Status = 200 && (reply = "none" || reply = "dolphin" || reply = "both" || reply = "adjudicate"))
            return reply
        Log("[WARN] CTSTracking reply not understood (status " req.Status ", '" reply "') -- No MM")
    } catch {
        Log("[WARN] CTSTracking not reachable at " server " -- No MM")
    }
    return "none"
}

; Sends keys only if Meet Manager is the window in front; otherwise logs and returns false
; so the sequence stops instead of typing into the wrong program.
; Accepts any Meet Manager window (Ctrl+W opens a race-picker dialog, not "Run the Meet"),
; by matching the active window's process to Run the Meet's process.
SendToMM(keys) {
    global MEET_MANAGER_WIN
    try {
        mmPid := WinGetPID(MEET_MANAGER_WIN)     ; Meet Manager's process
        activePid := WinGetPID("A")              ; the process of whatever is in front
    } catch {
        mmPid := 0, activePid := -1
    }
    if (mmPid != activePid) {
        active := ""
        try active := WinGetTitle("A")
        Log("[ERROR] Meet Manager lost focus (active: " active ") -- stopped before sending " keys)
        return false
    }
    Send(keys)
    return true
}

; -------------------------------------------------
; HOTKEYS (pressed by Companion after its TCP reset)
; -------------------------------------------------
F15::RunMeetManager("Next Heat", "{F5}")     ; F15: next heat in the same event
F14::RunMeetManager("Next Event", "^{F5}")   ; F14: first heat of the next event
