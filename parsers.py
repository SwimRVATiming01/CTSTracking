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

    # --- Active lanes ---
    #
    # Primary method: By Lane results table.
    #   CTS prints one row per finishing swimmer in the By Lane table.
    #   Each row has a 2-digit glyph encoding lane+place (e.g. "21" = lane 2,
    #   place 1). The first digit is the lane number. This works for any
    #   event distance and is unaffected by missed touchpads, as long as the
    #   swimmer has a finish time. Assumes 8-lane pool (lane/place always
    #   single digits).
    #
    # Fallback: nearest-lane assignment across all timing rows.
    #   Used when no By Lane entries are found (e.g. test/scratch files).

    lane_labels = [
        (float(x), float(y), int(re.search(r"\d+", t).group()))
        for x, y, t in glyphs if re.match(r"Lane \d+$", t)
    ]

    active = None

    # --- Primary: By Lane table ---
    by_lane_glyph = next(
        ((float(x), float(y)) for x, y, t in glyphs if t == "By Lane"), None
    )
    if by_lane_glyph and lane_labels:
        by_lane_x, by_lane_y = by_lane_glyph
        lane_header_y = lane_labels[0][1]
        # 2-digit lane+place entries sit left of the By Lane header, between
        # the By Lane label Y and the lane header row Y
        by_lane_entries = [
            t for x, y, t in glyphs
            if re.match(r"^\d{2}$", t)
            and float(x) < by_lane_x
            and by_lane_y < float(y) < lane_header_y
            and 1 <= int(t[0]) <= 8
            and 1 <= int(t[1]) <= 8
        ]
        if by_lane_entries:
            active = sorted(set(int(t[0]) for t in by_lane_entries))

    # --- Fallback: timing rows with nearest-lane assignment ---
    if active is None and lane_labels:
        lane_xs = {lane_num: lx for lx, ly, lane_num in lane_labels}
        lane_header_y = lane_labels[0][1]
        TIME_RE = re.compile(r"^\d+:\d{2}\.\d{2}$|^\d+\.\d{2}$")
        timing_below = [
            float(x) for x, y, t in glyphs
            if float(y) > lane_header_y and TIME_RE.match(t)
        ]
        if timing_below:
            matched = set()
            for data_x in timing_below:
                nearest = min(lane_xs.keys(), key=lambda n: abs(lane_xs[n] - data_x))
                matched.add(nearest)
            active = sorted(matched)

    if active is None:
        active = list(range(1, 9))

    result["active_lanes"]      = active
    result["missing_lanes"]     = [l for l in range(1, 9) if l not in active]
    result["missing_lanes_str"] = ", ".join(str(l) for l in result["missing_lanes"])

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
