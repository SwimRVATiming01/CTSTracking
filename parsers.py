"""
parsers.py - File parsers for CTS (.oxps) and Dolphin (.do3) files,
             plus filename format parsers for both types.
"""

import logging
import os
import re
import zipfile
from datetime import datetime

log = logging.getLogger("cts_tracker")


# ===========================================================================
# FILENAME PARSERS
# ===========================================================================

def parse_cts_filename(filename):
    """
    Parse a CTS filename forwarded by client.py.

    Format set by client.py on CTS machine:
        YYYY-MM-DD_HH-MM-SS__MACHINEID.oxps

    The timestamp comes from AHK (already in the original filename).
    Machine ID is appended by client.py on copy.

    Examples:
        2026-02-28_13-24-11__GEN7-TIMING-001.oxps
        2026-02-28_12-20-37__GEN7-TIMING-002.oxps

    Returns dict: machine_id, file_time (datetime), original_name
    """
    result = {"machine_id": None, "file_time": None, "original_name": filename}
    stem = os.path.splitext(filename)[0]
    parts = stem.split("__")

    if len(parts) >= 2:
        result["original_name"] = parts[0]
        result["machine_id"]    = parts[1]
        try:
            result["file_time"] = datetime.strptime(parts[0], "%Y-%m-%d_%H-%M-%S")
        except ValueError:
            log.warning(f"Could not parse timestamp from CTS filename: {filename}")
    else:
        result["machine_id"] = "UNKNOWN"
        try:
            result["file_time"] = datetime.strptime(stem, "%Y-%m-%d_%H-%M-%S")
        except ValueError:
            log.warning(f"CTS filename not in expected format: {filename}")

    return result


def parse_dolphin_filename(filename):
    """
    Expected: <original>__<MACHINEID>__<YYYYMMDDTHHMMSS>.do3
    Race number extracted from trailing digits of last segment of original stem.
    e.g. 039-000-00F0073 -> 73

    Returns dict: dolphin_race_num (int), machine_id, file_time (datetime), original_name
    """
    result = {"dolphin_race_num": None, "machine_id": None, "file_time": None, "original_name": filename}
    stem = os.path.splitext(filename)[0]
    parts = stem.split("__")
    result["original_name"] = parts[0]

    if len(parts) >= 3:
        result["machine_id"] = parts[1]
        try:
            result["file_time"] = datetime.strptime(parts[2], "%Y%m%dT%H%M%S")
        except ValueError:
            log.warning(f"Bad timestamp in Dolphin filename: {filename}")

    segments = parts[0].split("-")
    if segments:
        m = re.search(r"(\d+)$", segments[-1])
        if m:
            result["dolphin_race_num"] = int(m.group(1))

    if result["dolphin_race_num"] is None:
        log.warning(f"Could not extract race number from Dolphin filename: {filename}")

    return result


# ===========================================================================
# CTS FILE PARSER
# ===========================================================================

def parse_cts_file(filepath):
    """
    Parse a CTS GEN7 .oxps file (OpenXPS format — a ZIP archive containing XML).

    Extracts from the fpage XML via UnicodeString glyph positions:
      - event_id       : event number string e.g. "27"
      - heat           : heat number string e.g. "6"
      - cts_race_num   : integer race number e.g. 175
      - cts_start_time : "HH:MM" 24h format parsed from start time string
      - active_lanes   : list of active lane numbers e.g. [1,2,3,4,5,6,7,8]
      - missing_lanes  : list of missing lane numbers e.g. [1, 7]
      - missing_lanes_str : comma-separated string e.g. "1, 7"
      - off_times      : list of per-lane finish times in lane order e.g. ["5:56.32", ...]
      - session_meet   : raw session/meet string from file
      - start_time_raw : raw start time string from file

    Returns dict on success, None on unrecoverable error.
    """
    try:
        with zipfile.ZipFile(filepath, 'r') as z:
            content = z.read('Documents/1/Pages/1.fpage').decode('utf-8', errors='replace')
    except Exception as e:
        log.error(f"Could not open .oxps file {filepath}: {e}")
        return None

    # Extract all Glyphs with X, Y, and text
    glyphs = re.findall(
        r'<Glyphs[^>]+OriginX="([^"]+)"[^>]+OriginY="([^"]+)"[^>]+UnicodeString="([^"]+)"',
        content
    )
    if not glyphs:
        log.error(f"No glyph data found in {filepath}")
        return None

    result = {
        "event_id": None, "heat": None, "cts_race_num": None,
        "cts_start_time": None, "start_time_raw": None,
        "active_lanes": [], "missing_lanes": [], "missing_lanes_str": "",
        "off_times": [], "session_meet": None,
    }

    # --- Header row: Event, Heat, Race # (all at Y ~1997) ---
    header = sorted(
        [(float(x), t) for x, y, t in glyphs if abs(float(y) - 1997) < 50],
        key=lambda g: g[0]
    )
    vals = [t for _, t in header if t not in ("Event:", "Heat:", "Race #")]
    if len(vals) >= 3:
        result["event_id"]    = vals[0]
        result["heat"]        = vals[1]
        try:
            result["cts_race_num"] = int(vals[2])
        except ValueError:
            pass

    # --- Session/meet string (Y ~1775) ---
    sess = sorted(
        [(float(x), t) for x, y, t in glyphs if abs(float(y) - 1775) < 50],
        key=lambda g: g[0]
    )
    if sess:
        result["session_meet"] = sess[0][1]

    # --- Start time (label "Start Time:" then value at same Y) ---
    for x, y, t in glyphs:
        if "Start Time:" in t:
            sy = float(y)
            candidates = sorted(
                [(float(cx), ct) for cx, cy, ct in glyphs
                 if abs(float(cy) - sy) < 20 and ct != "Start Time:"],
                key=lambda g: g[0]
            )
            if candidates:
                raw = candidates[0][1].replace("(Manual Start)", "").strip()
                result["start_time_raw"] = raw
                m = re.search(r"(\d{1,2}:\d{2}:\d{2}\s*[AP]M)", raw)
                if m:
                    try:
                        dt = datetime.strptime(m.group(1).strip(), "%I:%M:%S %p")
                        result["cts_start_time"] = dt.strftime("%H:%M")
                    except ValueError:
                        pass
            break

    # --- Active lanes: detect via first timing row below lane headers ---
    #
    # CTS always prints all 8 "Lane N" labels regardless of whether a lane
    # has a swimmer. Instead, look at the first row of per-lane split times
    # below the headers (50yd splits for 100yd+ events). Each active lane
    # has a split time within ~100 units of its lane header X position;
    # empty lanes have no data there.
    lane_labels = [
        (float(x), float(y), int(re.search(r"\d+", t).group()))
        for x, y, t in glyphs if re.match(r"Lane \d+$", t)
    ]

    if lane_labels:
        lane_xs = {lane_num: lx for lx, ly, lane_num in lane_labels}
        lane_header_y = lane_labels[0][1]

        TIME_RE = re.compile(r"^\d+:\d{2}\.\d{2}$|^\d+\.\d{2}$")
        timing_below = [
            (float(x), float(y)) for x, y, t in glyphs
            if float(y) > lane_header_y and TIME_RE.match(t)
        ]

        active = sorted(lane_xs.keys())  # default: all lanes active
        if timing_below:
            first_row_y = min(y for x, y in timing_below)
            first_row_xs = [x for x, y in timing_below if abs(y - first_row_y) < 50]
            matched = set()
            for data_x in first_row_xs:
                best_lane, best_lx = min(lane_xs.items(), key=lambda kv: abs(kv[1] - data_x))
                if abs(best_lx - data_x) < 100:
                    matched.add(best_lane)
            if matched:
                active = sorted(matched)

        result["active_lanes"]      = active
        result["missing_lanes"]     = [l for l in range(1, 9) if l not in active]
        result["missing_lanes_str"] = ", ".join(str(l) for l in result["missing_lanes"])
    else:
        result["active_lanes"]      = list(range(1, 9))
        result["missing_lanes"]     = []
        result["missing_lanes_str"] = ""

    # --- Off Times: label "Off. Time" then concatenated times at same Y ---
    for x, y, t in glyphs:
        if t == "Off. Time":
            oty = float(y)
            candidates = sorted(
                [(float(cx), ct) for cx, cy, ct in glyphs
                 if abs(float(cy) - oty) < 20 and ct != "Off. Time"],
                key=lambda g: g[0]
            )
            if candidates:
                combined = candidates[0][1]
                result["off_times"] = re.findall(r"\d+:\d{2}\.\d{2}", combined)
            break

    if result["event_id"] is None or result["cts_race_num"] is None:
        log.warning(f"Could not parse essential fields from {filepath}: {result}")
        return None

    log.info(
        f"CTS parsed: Event={result['event_id']} Heat={result['heat']} "
        f"Race#={result['cts_race_num']} Start={result['cts_start_time']} "
        f"Lanes={result['active_lanes']} Missing={result['missing_lanes_str']}"
    )
    return result
