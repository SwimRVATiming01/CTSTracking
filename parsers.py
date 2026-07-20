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


def parse_gen_filename(filename):
    """
    Parse a Gen7 native .gen filename.

    Format: {gen7_session}-{event}-{heat}{round}{race_num}.gen
    e.g. 238-067-02F0276.gen -> gen7_session=238, event_id=67, heat=2, round=F, race_num=276

    gen7_session is Gen7's own internal session/meet bookkeeping number — confirmed
    unrelated to Meet Manager's session concept or the event itself; not used for
    matching, just carried along for reference.
    round: F=Final, P=Prelim, S=Semifinal.
    race_num: unique per race *attempt* (not per event/heat) — a false start or
    re-run gets a new race_num for the same event/heat, same role cts_race_num
    plays for .oxps (last one wins).

    Returns dict: gen7_session, event_id, heat, round, race_num, original_name.
    event_id/heat here have leading zeros stripped to match how the schedule
    table stores them (e.g. filename "067" -> "67"). The .gen file's own header
    line is the authoritative source for event_id/heat/round used at ingestion —
    this filename parse is a secondary cross-check plus the source of race_num,
    which only exists in the filename.
    """
    result = {
        "gen7_session": None, "event_id": None, "heat": None,
        "round": None, "race_num": None, "original_name": filename,
    }
    stem = os.path.splitext(filename)[0]
    m = re.match(r"^(\d+)-(\d+)-(\d+)([A-Za-z])(\d+)$", stem)
    if not m:
        log.warning(f".gen filename not in expected format: {filename}")
        return result

    result["gen7_session"] = m.group(1)
    result["event_id"]     = str(int(m.group(2)))
    result["heat"]          = str(int(m.group(3)))
    result["round"]        = m.group(4).upper()
    result["race_num"]     = int(m.group(5))
    return result


# ===========================================================================
# DOLPHIN FILE PARSER
# ===========================================================================

def parse_dolphin_file(filepath):
    """
    Parse a Dolphin .do3 file (semicolon-separated text).

    Each data line: lane;watch_a;watch_b;watch_c
    Empty fields indicate no time recorded for that watch on that lane.

    Returns dict: watch_a, watch_b, watch_c — each an 8-element list,
                  None where no time was recorded.
    Returns None on file read error.
    """
    result = {
        "watch_a": [None] * 8,
        "watch_b": [None] * 8,
        "watch_c": [None] * 8,
    }
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except Exception as e:
        log.error(f"Could not read Dolphin file {filepath}: {e}")
        return None

    for line in lines:
        parts = line.strip().split(";")
        try:
            lane = int(parts[0].strip())
        except (ValueError, IndexError):
            continue
        if not (1 <= lane <= 8):
            continue
        idx = lane - 1

        def _val(i):
            if i < len(parts):
                v = parts[i].strip()
                return v if v else None
            return None

        result["watch_a"][idx] = _val(1)
        result["watch_b"][idx] = _val(2)
        result["watch_c"][idx] = _val(3)

    log.info(
        f"Dolphin parsed: watch_a={result['watch_a']} "
        f"watch_b={result['watch_b']} watch_c={result['watch_c']}"
    )
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
                m = re.search(r"(\d{1,2}:\d{2}:\d{2})\s*([AaPp][Mm])?", raw)
                if m:
                    try:
                        if m.group(2):
                            dt = datetime.strptime(f"{m.group(1)} {m.group(2).upper()}", "%I:%M:%S %p")
                        else:
                            dt = datetime.strptime(m.group(1), "%H:%M:%S")
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


# ===========================================================================
# GEN7 NATIVE FILE PARSER
# ===========================================================================

def parse_gen_file(filepath):
    """
    Parse a Gen7 native .gen file (semicolon-delimited text, CRLF).

    Header line (line 1): event;heat;touchpad_count;round;100;Gen7 Swimming;version
      Field 4 is always "100" in every sample seen regardless of actual event
      distance/type — meaning unknown, not event distance. Not used here.

    Per-lane data lines (line 2 = lane 1, line 3 = lane 2, ... line 9 = lane 8;
    lines beyond 9 are padding for pools with more than 8 lanes):
      place;split_1;...;split_N;backup_A;backup_B;backup_C;reaction_time;(padding)
      - place = finish place in this heat (NOT lane number — lane is the line
        position). 0 = lane empty / no recorded time.
      - N = touchpad_count from the header. split_N is the official/final time.
      - backup_A/backup_B/backup_C are backups for the FINAL touch only, not
        intermediate splits. backup_C is always empty at this facility (no C
        buttons in use) but the slot is real, not a parsing gap.
      - reaction_time (the trailing "+X.XX" value) = reaction time off the
        blocks / backstroke-ledge takeoff reaction. Not currently stored.

    Returns dict on success, None on unrecoverable error:
      event_id, heat, round, touchpad_count,
      active_lanes, missing_lanes, missing_lanes_str,
      off_times, button_a_times, button_b_times — each an 8-element list
      indexed by lane (index 0 = lane 1), None where no time was recorded.
    """
    try:
        with open(filepath, newline="", encoding="utf-8-sig", errors="replace") as f:
            lines = [line.rstrip("\r\n") for line in f]
    except Exception as e:
        log.error(f"Could not read .gen file {filepath}: {e}")
        return None

    if not lines or not lines[0].strip():
        log.error(f"Empty .gen file: {filepath}")
        return None

    header = lines[0].split(";")
    if len(header) < 5:
        log.error(f"Malformed .gen header in {filepath}: {lines[0]!r}")
        return None

    event_id = header[0].strip()
    heat = header[1].strip()
    round_code = header[3].strip().upper()
    try:
        touchpad_count = int(header[2].strip())
    except ValueError:
        touchpad_count = None

    result = {
        "event_id": event_id, "heat": heat, "round": round_code,
        "touchpad_count": touchpad_count,
        "active_lanes": [], "missing_lanes": [], "missing_lanes_str": "",
        "off_times": [None] * 8, "button_a_times": [None] * 8, "button_b_times": [None] * 8,
    }

    if not event_id or not heat:
        log.warning(f"Could not parse essential fields from {filepath}: {result}")
        return None

    if touchpad_count is None:
        log.warning(f"Could not parse touchpad count from .gen header in {filepath}: {lines[0]!r}")
        return result

    def _val(fields, i):
        if 0 <= i < len(fields):
            v = fields[i].strip()
            return v if v else None
        return None

    active = []
    for idx, line in enumerate(lines[1:9]):
        fields = line.split(";")
        place_raw = _val(fields, 0)
        try:
            place = int(place_raw) if place_raw else 0
        except ValueError:
            place = 0
        if place == 0:
            continue  # lane empty / no recorded time

        lane_num = idx + 1
        active.append(lane_num)

        splits = fields[1:1 + touchpad_count]
        rest = fields[1 + touchpad_count:]
        result["off_times"][idx] = _val(splits, touchpad_count - 1)
        result["button_a_times"][idx] = _val(rest, 0)
        result["button_b_times"][idx] = _val(rest, 1)
        # rest[2] = backup_C (always empty here), rest[3] = reaction_time — not stored

    result["active_lanes"] = active
    result["missing_lanes"] = [l for l in range(1, 9) if l not in active]
    result["missing_lanes_str"] = ", ".join(str(l) for l in result["missing_lanes"])

    log.info(
        f".gen parsed: Event={result['event_id']} Heat={result['heat']} "
        f"Round={result['round']} N={touchpad_count} "
        f"Lanes={active} Missing={result['missing_lanes_str']}"
    )
    return result
