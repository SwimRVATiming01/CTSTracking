"""
parsers.py - File parsers for CTS (.oxps) and Dolphin (.do3) files,
             plus filename format parsers for both types.
"""

import logging
import os
import re
import time
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
    result = {"dolphin_race_num": None, "dolphin_dataset": None, "machine_id": None, "file_time": None, "original_name": filename}
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
        ds = re.match(r"^(\d+)$", segments[0])
        if ds:
            result["dolphin_dataset"] = int(ds.group(1))

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
    content = None
    _retries = 3
    _delay = 1.5
    for _attempt in range(1, _retries + 2):
        try:
            with zipfile.ZipFile(filepath, 'r') as z:
                content = z.read('Documents/1/Pages/1.fpage').decode('utf-8', errors='replace')
            break
        except zipfile.BadZipFile as e:
            if _attempt <= _retries:
                log.warning(f"Could not open .oxps file {filepath} (attempt {_attempt}/{_retries}): {e} — retrying in {_delay}s")
                time.sleep(_delay)
            else:
                log.error(f"Could not open .oxps file {filepath}: {e}")
                return None
        except Exception as e:
            log.error(f"Could not open .oxps file {filepath}: {e}")
            return None
    if content is None:
        return None

    # Extract all Glyphs with X, Y, Indices, and text
    glyphs = []
    for m in re.finditer(r'<Glyphs\b([^>]+)>', content):
        attrs = m.group(1)
        ox = re.search(r'OriginX="([^"]+)"', attrs)
        oy = re.search(r'OriginY="([^"]+)"', attrs)
        us = re.search(r'UnicodeString="([^"]+)"', attrs)
        ix = re.search(r'Indices="([^"]+)"', attrs)
        if ox and oy and us:
            glyphs.append((ox.group(1), oy.group(1), us.group(1),
                           ix.group(1) if ix else ""))
    if not glyphs:
        log.error(f"No glyph data found in {filepath}")
        return None

    result = {
        "event_id": None, "heat": None, "cts_race_num": None,
        "cts_start_time": None, "start_time_raw": None,
        "active_lanes": [], "missing_lanes": [], "missing_lanes_str": "",
        "off_times": [], "button_a_times": [], "button_b_times": [],
        "session_meet": None,
    }

    # --- Header row: Event, Heat, Race # (all at Y ~1997) ---
    header = sorted(
        [(float(x), t) for x, y, t, _ in glyphs if abs(float(y) - 1997) < 50],
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
        [(float(x), t) for x, y, t, _ in glyphs if abs(float(y) - 1775) < 50],
        key=lambda g: g[0]
    )
    if sess:
        result["session_meet"] = sess[0][1]

    # --- Start time (label "Start Time:" then value at same Y) ---
    for x, y, t, _ in glyphs:
        if "Start Time:" in t:
            sy = float(y)
            candidates = sorted(
                [(float(cx), ct) for cx, cy, ct, _i in glyphs
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
        for x, y, t, _ in glyphs if re.match(r"Lane \d+$", t)
    ]

    active = None

    # --- Primary: By Lane table ---
    by_lane_glyph = next(
        ((float(x), float(y)) for x, y, t, _ in glyphs if t == "By Lane"), None
    )
    if by_lane_glyph and lane_labels:
        by_lane_x, by_lane_y = by_lane_glyph
        lane_header_y = lane_labels[0][1]
        by_lane_entries = [
            t for x, y, t, _ in glyphs
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
            float(x) for x, y, t, _ in glyphs
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

    # --- Shared helper: decode a concatenated timing glyph into an 8-element
    #     per-lane list using the Indices advance widths.
    #
    #     Each time value ends with a large advance width that encodes how many
    #     lane columns to skip before the next value. We calibrate one lane width
    #     from the Off. Time glyph (consecutive, one value per active lane) then
    #     use that unit to decode Button A/B lane spacing.
    # ---
    TIME_RE = re.compile(r"\d+:\d{2}\.\d{2}|\d+\.\d{2}")
    lane_header_xs = sorted(lx for lx, ly, ln in lane_labels) if lane_labels else []

    def _group_advances(combined, indices_str):
        """Sum advance widths per time value group from an Indices string."""
        if not indices_str:
            return []
        times = TIME_RE.findall(combined)
        pairs = indices_str.split(";")
        char_advances = []
        for pair in pairs:
            parts = pair.split(",")
            char_advances.append(int(parts[1]) if len(parts) >= 2 else 0)
        groups = []
        char_pos = 0
        for t in times:
            groups.append(sum(char_advances[char_pos:char_pos + len(t)]))
            char_pos += len(t)
        return groups

    def _times_to_lanes_by_indices(glyph_x, combined, indices_str, lane_unit):
        """Decode per-lane times using Indices advance widths.
        lane_unit: advance width corresponding to exactly one lane column."""
        times = TIME_RE.findall(combined)
        if not times or not lane_header_xs:
            return [None] * 8

        start_lane_idx = min(range(len(lane_header_xs)),
                             key=lambda i: abs(lane_header_xs[i] - glyph_x))

        if not indices_str or len(times) == 1 or not lane_unit:
            per_lane = [None] * 8
            if start_lane_idx < 8:
                per_lane[start_lane_idx] = times[0]
            return per_lane

        groups = _group_advances(combined, indices_str)
        per_lane = [None] * 8
        lane_idx = start_lane_idx
        for i, (t, adv) in enumerate(zip(times, groups)):
            if lane_idx < 8:
                per_lane[lane_idx] = t
            if i < len(times) - 1:
                lane_idx += max(1, round(adv / lane_unit))

        return per_lane

    # --- Off Times + calibrate lane_unit ---
    # Off. Time values are always consecutive one-per-active-lane, so the
    # average group advance gives us the exact 1-lane width in index units.
    lane_unit = None
    for x, y, t, _ in glyphs:
        if t == "Off. Time":
            oty = float(y)
            candidates = sorted(
                [(float(cx), ct, ci) for cx, cy, ct, ci in glyphs
                 if abs(float(cy) - oty) < 20 and ct != "Off. Time"],
                key=lambda g: g[0]
            )
            if candidates:
                off_combined, off_indices = candidates[0][1], candidates[0][2]
                groups = _group_advances(off_combined, off_indices)
                # All groups except the last represent one lane advance each
                if len(groups) > 1:
                    lane_unit = sum(groups[:-1]) / (len(groups) - 1)
                result["off_times"] = _times_to_lanes_by_indices(
                    candidates[0][0], off_combined, off_indices, lane_unit)
            break

    # --- Button A ---
    for x, y, t, _ in glyphs:
        if t == "Button A":
            bay = float(y)
            candidates = sorted(
                [(float(cx), ct, ci) for cx, cy, ct, ci in glyphs
                 if abs(float(cy) - bay) < 20 and ct != "Button A"],
                key=lambda g: g[0]
            )
            if candidates:
                result["button_a_times"] = _times_to_lanes_by_indices(
                    candidates[0][0], candidates[0][1], candidates[0][2], lane_unit)
            break

    # --- Button B ---
    for x, y, t, _ in glyphs:
        if t == "Button B":
            bby = float(y)
            candidates = sorted(
                [(float(cx), ct, ci) for cx, cy, ct, ci in glyphs
                 if abs(float(cy) - bby) < 20 and ct != "Button B"],
                key=lambda g: g[0]
            )
            if candidates:
                result["button_b_times"] = _times_to_lanes_by_indices(
                    candidates[0][0], candidates[0][1], candidates[0][2], lane_unit)
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
