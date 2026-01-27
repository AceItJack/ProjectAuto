#!/usr/bin/env python3
"""
Make daily Lecture Support workbooks from raw web-form export.

Usage:
  python make_daily_workbooks.py input.xlsx --outdir out/ [--single-workbook]

Notes:
- Expects columns similar to your export:
    'Day of Week:', 'Start Date:', 'Start Time:', 'End Time:',
    'Department/Unit:', 'Course Code/Name of Event:', 'Room Assigned:',
    'Support Request:', 'FSS Laptop', 'Data Projector', 'Speakers',
    'Microphone (G102 only)', 'Full Name:', 'Mobile Phone Number:', 'Serial'
- Robust to minor column name variations (fuzzy matching).
"""

# make_daily_workbooks.py
from typing import Dict
import argparse
import os
import sys
import logging
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import re

ROOM_MAP_PATH = "FSS IT Support Quick Check List - Rooms and Needs 20251111.xlsx"


def load_room_setup_requirements(path: str, sheet_name: str = 0) -> Dict[str, str]:
    """
    Read the Quick Check List Excel and return a mapping:
        { normalized_room : equipment_needed_for_work }

    The sheet structure (based on your uploaded file) is:
      - Column 0: Room
      - Column 1: Equipment within the Room
      - Column 2: Equipment Needed for Work
      - Column 3: Notes
    """
    try:
        df = pd.read_excel(path, sheet_name=sheet_name)
    except Exception as e:
        logging.error("Failed to open room setup file '%s': %s", path, e)
        return {}

    # Rename columns explicitly for clarity
    df = df.rename(columns={
        df.columns[0]: "Room",
        df.columns[1]: "EquipmentWithinRoom",
        df.columns[2]: "EquipmentNeededForWork",
        df.columns[3]: "Notes",
    })

    # Drop header row if it repeats the title line
    if "Room" in str(df.iloc[0, 0]):
        df = df.iloc[1:].reset_index(drop=True)

    mapping: Dict[str, str] = {}

    # Build dictionary: room -> Equipment Needed for Work
    for _, row in df.iterrows():
        room = _norm_room_key(row.get("Room"))
        # Normalize the 'needed' value; protect against NaN becoming the string 'nan'
        raw_needed = row.get("EquipmentNeededForWork")
        needed = str(raw_needed or "").strip()
        if needed.lower() in {"", "nan", "-"}:
            continue
        if room:
            mapping[room] = needed

    return mapping


def _norm_room_key(name: str) -> str:
    if pd.isna(name) or not name:
        return ""
    s = str(name).upper().strip()

    # Remove known building/floor prefixes
    s = re.sub(r"\b(FSS_|FST_|FSTC_|FSB_|FSK_|FSS|FST)\b", "", s)

    # Remove words like ROOM, SEMINAR, LAB, CLASSROOM, TR, GRAD, PSYC, CONFERENCE, THE
    s = re.sub(
        r"\b(ROOM|SEMINAR|LAB|CLASSROOM|TR|GRAD|PSYC|CONFERENCE|THE)\b", "", s)

    # Remove punctuation and whitespace
    s = re.sub(r"[^A-Z0-9]", "", s)

    # Keep the last letter+number sequence (matches checklist like S6, F202, G202)
    parts = re.findall(r"[A-Z]?\d+", s)
    if parts:
        return parts[-1]
    return s


# ---------- Config you can tweak ----------
DAYS_ORDER = ["Monday", "Tuesday", "Wednesday",
              "Thursday", "Friday", "Saturday", "Sunday"]

# Desired output column order
OUTPUT_COLS = [
    "Serial",
    "Date",
    "Day",
    "Duty Start time",
    "Duty End time",
    "Department/Unit",
    "Course/Event",
    "Room",
    "Support Request",
    "FSS Laptop",
    "Data Projector",
    "Speakers",
    "Microphone (G102 only)",
    "Requester Name",
    "Requester Phone",
    "PU",
    "SU",
]

# Candidate column name patterns -> canonical keys used internally
FUZZY_MAP = {
    "Serial": ["serial"],
    "Day of Week:": ["day of week", "weekday", "day"],
    "Start Date:": ["start date", "date start", "date"],
    "End Date:": ["end date"],
    "Start Time:": ["start time", "duty start", "time start", "begin time", "from"],
    "End Time:": ["end time", "duty end", "time end", "finish time", "to"],
    "Department/Unit:": ["department/unit", "department", "unit", "dept"],
    "Course Code/Name of Event:": [
        "course code/name of event", "course", "module", "name of event", "class", "event"
    ],
    "Room Assigned:": ["room assigned", "room", "venue", "location", "building"],
    "Support Request:": ["support request", "comments", "request details"],
    "FSS Laptop": ["fss laptop", "laptop (fss)", "laptop"],
    "Data Projector": ["data projector", "projector"],
    "Speakers": ["speakers"],
    "Microphone (G102 only)": ["microphone (g102 only)", "microphone", "mic"],
    "Full Name:": ["full name", "requester name", "name"],
    "Mobile Phone Number:": ["mobile phone number", "requester phone", "phone", "telephone", "contact"],
    "Title": ["title", "title:"],
}

# ---------- Helpers ----------


def _normalize(s: str) -> str:
    return " ".join(str(s).strip().lower().replace("_", " ").split())


def detect_header_row(df: pd.DataFrame, search_token: str = "serial", max_scan_rows: int = 10) -> int:
    """
    Find the row index that contains the header labels.
    Heuristic: first row that includes a cell matching `search_token` (normalized).
    """
    token = _normalize(search_token)
    scan = min(max_scan_rows, len(df))
    for i in range(scan):
        row = df.iloc[i].astype(str).map(_normalize).tolist()
        if token in row:
            logging.info(
                "Detected header row at index %d using token '%s'", i, token)
            return i
    logging.warning(
        "Header row not found by token '%s'. Falling back to row 2 (0-based).", search_token
    )
    return 2


def build_column_map(columns: List[str]) -> Dict[str, Optional[str]]:
    """
    Map fuzzy column names from the raw export to canonical keys.
    Returns a dict of canonical_name -> actual_column_name_or_None.
    """
    norm_to_actual = {_normalize(c): c for c in columns if isinstance(c, str)}
    out: Dict[str, Optional[str]] = {}
    for canonical, candidates in FUZZY_MAP.items():
        actual = None
        # exact first
        for cand in [canonical] + candidates:
            norm = _normalize(cand)
            if norm in norm_to_actual:
                actual = norm_to_actual[norm]
                break
        # substring fallback
        if actual is None:
            cand_norms = [_normalize(c) for c in candidates]
            for norm_key, actual_name in norm_to_actual.items():
                if any(k in norm_key for k in cand_norms):
                    actual = actual_name
                    break
        out[canonical] = actual
        if actual is None:
            logging.warning(
                "Could not find column for canonical key '%s'", canonical)
    return out


def parse_time_flex(x) -> Optional[time]:
    """
    Parse time like '8:00', '08:30', '8:30 AM', '14:00', '08:30:00', 8, '8'.
    Returns datetime.time or NaN.
    """
    if pd.isna(x) or str(x).strip() == "":
        return np.nan
    # Already a datetime/time?
    if isinstance(x, pd.Timestamp):
        return x.time()
    if isinstance(x, time):
        return x

    s = str(x).strip()
    # common split like "8:00 - 9:00"
    if "-" in s and ":" in s:
        s = s.split("-")[0].strip()

    fmts = ["%H:%M", "%I:%M %p", "%H:%M:%S", "%I %p", "%H"]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).time()
        except Exception:
            continue
    # numeric hours like 8 or 14
    try:
        h = int(float(s))
        if 0 <= h < 24:
            return time(hour=h, minute=0)
    except Exception:
        pass
    # Last resort: let pandas try
    try:
        return pd.to_datetime(s).time()
    except Exception:
        return np.nan

# Added by Selena Johnson 22/10/2025


def parse_time_flex_end(x) -> Optional[time]:
    """
    Parse the END part of a time range like '8:00 - 9:00' or '09:00–10:30'.
    """
    if pd.isna(x) or str(x).strip() == "":
        return np.nan

    s = str(x).strip()

    # Normalize dash types
    s = s.replace("–", "-").replace("—", "-")

    if "-" in s:
        parts = [p.strip() for p in s.split("-") if p.strip()]
        if len(parts) > 1:
            s = parts[-1]
        else:
            s = parts[0]

    try:
        return pd.to_datetime(s).time()
    except Exception:
        return np.nan



def _autofit_and_style(ws, df: pd.DataFrame, workbook):
    """Best-effort auto-width + header styles for xlsxwriter."""
    header_fmt = workbook.add_format(
        {"bold": True, "text_wrap": True, "valign": "top"})
    wrap_fmt = workbook.add_format({"text_wrap": True, "valign": "top"})
    date_fmt = workbook.add_format({"num_format": "yyyy-mm-dd"})
    # Write header style
    for col_idx, col_name in enumerate(df.columns):
        ws.write(0, col_idx, col_name, header_fmt)

    # Column widths based on max length of values (bounded)
    max_widths = []
    for col in df.columns:
        # header width as baseline
        max_len = len(str(col))
        # sample up to 500 rows to bound cost
        sample = df[col].astype(str).head(500).tolist()
        if sample:
            max_len = max(max_len, max(len(s) for s in sample))
        max_widths.append(min(60, max(10, int(max_len * 1.1))))

    for idx, w in enumerate(max_widths):
        ws.set_column(idx, idx, w)

    # Freeze header row
    ws.freeze_panes(1, 0)

    # Apply wrapping to body; date formatting for Date column
    for r in range(len(df)):
        for c, col in enumerate(df.columns):
            val = df.iat[r, c]
            if col == "Date":
                try:
                    if pd.isna(val):
                        ws.write(r + 1, c, "", date_fmt)
                    elif isinstance(val, (pd.Timestamp, datetime)):
                        ws.write_datetime(r + 1, c, val, date_fmt)
                    else:
                        ws.write(r + 1, c, str(val), wrap_fmt)
                except Exception:
                    ws.write(r + 1, c, "" if pd.isna(val)
                             else str(val), wrap_fmt)
            else:
                ws.write(r + 1, c, "" if pd.isna(val) else str(val), wrap_fmt)


def _normalize_day_name(s: str) -> str:
    s = (s or "").strip().capitalize()
    for d in DAYS_ORDER:
        if s.startswith(d[:3]):  # allow "Mon", "Mon.", "Monday"
            return d
    return s

def extract_marked_equipment(row) -> list:
    """
    Return ONLY equipment explicitly marked with 'x' in the raw input.
    """
    equipment_map = {
        "FSS Laptop": "Laptop",
        "Data Projector": "Projector",
        "Speakers": "Speakers",
        "Microphone (G102 only)": "Microphone",
    }

    marked = []

    for col, label in equipment_map.items():
        val = row.get(col, "")
        if isinstance(val, str) and val.strip().lower() == "x":
            marked.append(label)

    return marked


# Added by Selena Johnson
# Combine equipment columns into one string
# combine equipment has been completely changed - 17/11/2025


'''


def combine_equipment(row, room_map: Dict[str, str]) -> str:
    """
    Build NOTES equipment list:
      - Room equipment: ALWAYS included
      - Event equipment: ONLY if marked 'x'
    """

    combined = []

    # --- Room equipment (always) ---
    room_name = row.get("Room", "")
    room_key = _norm_room_key(room_name)
    room_eq = room_map.get(room_key, "")

    if room_eq:
        combined.extend(
            [e.strip() for e in str(room_eq).split(",") if e.strip()]
        )

    # --- Event equipment (only if 'x') ---
    for eq in extract_marked_equipment(row):
        if eq not in combined:
            combined.append(eq)

    if not combined:
        return ""

    return ", ".join(combined)

'''
def combine_equipment(row, room_map: Dict[str, str]) -> str:
    """
    Build NOTES equipment list:
      - Room equipment: included only if room exists in map
      - Event equipment: ONLY if marked 'x'
      - NO placeholders, NO text, ONLY equipment
    """

    combined = []

    # --- Room equipment ---
    room_name = row.get("Room Assigned") or row.get("Room")
    room_key = _norm_room_key(room_name)

    if room_key and room_key in room_map:
        combined.extend(
            e.strip()
            for e in str(room_map[room_key]).split(",")
            if e and e.strip().lower() != "nan"
        )

    # --- Event equipment (only if 'x') ---
    for eq in extract_marked_equipment(row):
        if eq not in combined:
            combined.append(eq)

    return ", ".join(combined) if combined else ""


def prepare_schedule_table(raw_df: pd.DataFrame, header_token: str = "serial") -> pd.DataFrame:
    """
    Normalize raw schedule input:
      - Detect header
      - Rename columns to canonical names
      - Parse dates/times
      - Return normalized table with consistent Start/End Dates
    """
    header_row = detect_header_row(raw_df, search_token=header_token)
    data = raw_df.iloc[header_row + 1:].copy()
    data.columns = raw_df.iloc[header_row].tolist()

    # Drop empty rows/columns
    data = data.dropna(axis=1, how="all").dropna(axis=0, how="all")
    data = data.applymap(
    lambda x: x.strip()
    if isinstance(x, str) and x.strip().lower() not in {"nan", ""}
    else (np.nan if isinstance(x, str) else x)
)


    # Build fuzzy column map
    col_map = build_column_map([c for c in data.columns if isinstance(c, str)])

    def col_get(key: str) -> Optional[str]:
        return col_map.get(key)

    out = pd.DataFrame()

    # Serial
    out["Serial"] = data.get(col_get("Serial"), pd.Series(dtype="object"))

    # Start & End Dates
    start_date_col = col_get("Start Date:")
    end_date_col = col_get("End Date:")

    if start_date_col:
        out["Start Date"] = pd.to_datetime(
            data.get(start_date_col), errors="coerce")
        out["_input_start_date_raw"] = data.get(start_date_col)
    else:
        out["Start Date"] = pd.NaT
        out["_input_start_date_raw"] = pd.Series(dtype="object")

    if end_date_col:
        out["End Date"] = pd.to_datetime(
            data.get(end_date_col), errors="coerce")
        out["_input_end_date_raw"] = data.get(end_date_col)
    else:
        out["End Date"] = out["Start Date"]
        out["_input_end_date_raw"] = data.get(start_date_col)

    out["Date"] = out["Start Date"]

    # Day of Week
    day_col = col_get("Day of Week:")
    if day_col and day_col in data.columns:
        out["Day"] = data[day_col].astype(str).map(_normalize_day_name)
    else:
        out["Day"] = [d.day_name() if pd.notna(d) else "" for d in out["Date"]]

    # Times
    start_time_col = col_get("Start Time:")
    end_time_col = col_get("End Time:")
    out["Duty Start time"] = data.get(
        start_time_col, pd.Series(dtype="object"))
    out["Duty End time"] = data.get(end_time_col, pd.Series(dtype="object"))
    out["_sort_start_time"] = data.get(
        start_time_col, pd.Series(dtype="object")).map(parse_time_flex)

    # Basic mapping
    mapping_pairs = [
        ("Department/Unit", "Department/Unit:"),
        ("Course/Event", "Course Code/Name of Event:"),
        ("Room", "Room Assigned:"),
        ("Support Request", "Support Request:"),
        ("FSS Laptop", "FSS Laptop"),
        ("Data Projector", "Data Projector"),
        ("Speakers", "Speakers"),
        ("Microphone (G102 only)", "Microphone (G102 only)"),
        ("Requester Name", "Full Name:"),
        ("Requester Phone", "Mobile Phone Number:"),
        ("Title", "Title"),
    ]

    for new_name, canon in mapping_pairs:
        src = col_get(canon)
        if new_name == "Title" and src is None:
            title_col = next(
                (c for c in data.columns if "title" in str(c).lower()), None)
            if title_col:
                src = title_col
        out[new_name] = data.get(src, pd.Series(dtype="object"))

    # If 'Room' was not detected or is entirely empty, try a looser fallback:
    if ("Room" not in out.columns) or out["Room"].isna().all():
        fallback_col = next((c for c in data.columns
                             if isinstance(c, str) and "room" in c.lower()), None)
        if fallback_col is not None:
            # Clean fallback values: replace NaN-like strings with empty string
            col_vals = data.get(fallback_col)
            try:
                out["Room"] = col_vals.fillna("").astype(str).replace({"nan": ""})
            except Exception:
                out["Room"] = col_vals
        else:
            out["Room"] = pd.Series([""] * len(out), dtype="object")

    # Ensure all output columns exist
    for col in OUTPUT_COLS:
        if col not in out.columns:
            out[col] = "" if col != "Date" else pd.NaT

    # Reorder columns
    extra_cols = ["Title"] if "Title" in out.columns else []
    out = out[[c for c in OUTPUT_COLS] + extra_cols + ["Start Date", "End Date", "_sort_start_time",
                                                       "_input_start_date_raw", "_input_end_date_raw"]]

    # Drop fully empty rows
    key_cols = [c for c in ["Start Date", "Day", "Duty Start time",
                            "Department/Unit", "Course/Event", "Room"] if c in out.columns]
    out = out[~out[key_cols].isna().all(axis=1)].copy()

    return out


# Added by Selena Johnson

# New function below

def build_schedule_format(df: pd.DataFrame, room_map: dict) -> pd.DataFrame:
    """
    Build the formatted duty schedule table from the normalized DataFrame.
    Appends room-mapped equipment to the inputted equipment list.

    Each Course/Event produces two rows:
      - SU (Set Up): 15 min before event start → event start
      - PU (Pick Up): 15 min before event end → 15 min after event end
    """

    def adjust_time(t: time, delta_minutes: int) -> time:
        """Shift a time value by delta_minutes safely."""
        if pd.isna(t):
            return pd.NaT
        try:
            if isinstance(t, str):
                parsed = pd.to_datetime(t, errors="coerce").time()
            elif isinstance(t, pd.Timestamp):
                parsed = t.time()
            elif isinstance(t, time):
                parsed = t
            else:
                parsed = pd.to_datetime(str(t), errors="coerce").time()
            if pd.isna(parsed):
                return pd.NaT
            return (datetime.combine(datetime.today(), parsed) +
                    timedelta(minutes=delta_minutes)).time()
        except Exception:
            return pd.NaT

    # Parse event start/end times
    event_start = df["Duty Start time"].apply(parse_time_flex)
    event_end = df["Duty End time"].apply(parse_time_flex_end)

    # --- SU rows (Set Up) ---
    setup_df = pd.DataFrame({
        "Activity": "SU",
        "Duty Start Time": event_start.apply(lambda t: adjust_time(t, -15)),
        "Duty Anticipated End Time": event_start.apply(lambda t: adjust_time(t, 15)),
        "Event Start Time": event_start,
        "Event End Time": event_end,
    })

    # --- PU rows (Pick Up) ---
    pickup_df = pd.DataFrame({
        "Activity": "PU",
        "Duty Start Time": event_end.apply(lambda t: adjust_time(t, -15)),
        "Duty Anticipated End Time": event_end.apply(lambda t: adjust_time(t, 15)),
        "Event Start Time": event_start,
        "Event End Time": event_end,
    })

    # --- Shared columns copied from input ---
    shared_cols = {
        "Title": df.get("Title", ""),
        "Full Name": df.get("Requester Name", ""),
        "Event/Course": df.get("Course/Event", ""),
        "NOTES": df.apply(
            lambda r: " | ".join(
                part for part in [
                    (
                        r["Support Request"]
                        if (
                            pd.notna(r.get("Support Request"))
                            and isinstance(r.get("Support Request"), str)
                            and r["Support Request"].strip()
                            and r["Support Request"].strip().lower()
                            not in {"technical support (specify in comments)"}
                        )
                        else ""
                    ),
                    combine_equipment(r, room_map),
                ]
                if part
            ),
            axis=1,
        ),
        "List Equipment Used (Laptop, Projector, VGA, Speakers, etc.)": "",
        "Start Date": df.get("_input_start_date_raw", df.get("Start Date", "")),
        "End Date": df.get("_input_end_date_raw", df.get("End Date", "")),
        # Ensure Room Assigned is a cleaned series (no NaN strings)
        "Room Assigned": (
            (df.get("Room") if "Room" in df.columns else df.get("Room Assigned", pd.Series(dtype="object")))
            .fillna("")
            .astype(str)
            .replace({"nan": ""})
        ),
        "Comments": "",
        "Indicate Done(D), Not Needed(X)": "",
        "FSS CL Staff": ""
    }

    for col, values in shared_cols.items():
        setup_df[col] = values
        pickup_df[col] = values

    # Combine both sets of rows (each event -> SU + PU)
    schedule = pd.concat([setup_df, pickup_df], ignore_index=True)

    # Desired column order
    col_order = [
        "FSS CL Staff",
        "Duty Start Time",
        "Duty Anticipated End Time",
        "Event Start Time",
        "Event End Time",
        "Activity",
        "Title",
        "Full Name",
        "Event/Course",
        "Room Assigned",
        "NOTES",
        "Indicate Done(D), Not Needed(X)",
        "List Equipment Used (Laptop, Projector, VGA, Speakers, etc.)",
        "Start Date",
        "End Date",
        "Comments"
    ]

    # Ensure columns exist and are ordered
    for col in col_order:
        if col not in schedule.columns:
            schedule[col] = ""
    schedule = schedule[col_order]

    return schedule


# Added by Selena Johnson


def _write_day_sheet(xw, df: pd.DataFrame, sheet_name: str, room_map: dict):

    """
    Create one worksheet (one day) in the output Excel file.

    - Builds the formatted duty schedule for the given weekday.
    - Preserves Title (Mr, Dr, Miss, etc.) from input.xlsx.
    - Keeps Title as a column beside Full Name (not in the top sheet heading).
    """

    # --- STEP 1: Build formatted schedule from normalized data ---
    # load room_map somewhere
    

    schedule = build_schedule_format(df, room_map=room_map)

    # --- STEP 2: Ensure 'Title' column is preserved and visible ---
    title_source = None
    if "Title" in df.columns and df["Title"].notna().any():
        title_source = df["Title"]
    elif "Title:" in df.columns and df["Title:"].notna().any():
        title_source = df["Title:"]

    if title_source is not None:
        logging.debug(
            f"Title source sample: {title_source.dropna().unique()[:5]}")
    else:
        logging.warning("No Title column found in source DataFrame.")

    # --- Duplicate title values to match the SU/PU expansion ---
    if title_source is not None:
        expanded_titles = np.repeat(
            title_source.values, 2)  # Each event -> SU & PU
        expanded_titles = pd.Series(expanded_titles, index=schedule.index)

        if "Title" not in schedule.columns:
            insert_pos = (
                schedule.columns.get_loc("Full Name")
                if "Full Name" in schedule.columns
                else len(schedule.columns)
            )
            schedule.insert(insert_pos, "Title", expanded_titles)
        else:
            # Replace empty or missing titles
            schedule["Title"] = np.where(
                schedule["Title"].astype(str).str.strip() == "",
                expanded_titles.astype(str).fillna(""),
                schedule["Title"]
            )
    else:
        if "Title" not in schedule.columns:
            insert_pos = (
                schedule.columns.get_loc("Full Name")
                if "Full Name" in schedule.columns
                else len(schedule.columns)
            )
            schedule.insert(insert_pos, "Title", "")

    # --- STEP 3: Create worksheet and define title for the top heading ---
    ws = xw.book.add_worksheet(sheet_name)
    wb = xw.book
    display_title = f"Lecture Support - {sheet_name}"

    # --- STEP 4: Define formatting styles ---
    title_fmt = wb.add_format({
        "bold": True, "align": "center", "valign": "vcenter",
        "font_size": 16, "underline": True
    })
    header_fmt = wb.add_format({
        "bold": True, "align": "center", "valign": "vcenter",
        "text_wrap": True, "border": 1, "italic": True,
        "font_size": 12, "bg_color": "#E2EFDA"
    })
    sub_header_fmt = wb.add_format({
        "align": "left", "valign": "top", "text_wrap": True,
        "border": 1, "italic": True, "font_size": 9, "bg_color": "#E2EFDA"
    })
    cell_fmt = wb.add_format({"valign": "top", "text_wrap": True, "border": 1})
    alt_fmt = wb.add_format({
        "valign": "top", "text_wrap": True, "border": 1,
        "bg_color": "#F2F2F2"
    })
    time_fmt = wb.add_format(
        {"num_format": "hh:mm", "align": "center", "border": 1})
    date_fmt = wb.add_format(
        {"num_format": "dd-mmm-yy", "align": "center", "border": 1})

    # --- STEP 5: Write the top merged title row ---
    ws.merge_range(0, 0, 0, len(schedule.columns) -
                   1, display_title, title_fmt)
    ws.set_row(0, 25)

    # --- STEP 6: Write header row ---
    for c, col_name in enumerate(schedule.columns):
        ws.write(1, c, col_name, header_fmt)
    ws.set_row(1, 35)

    # --- STEP 7: Add legend under "Indicate Done(D), Not Needed(X)" column ---
    indicate_col = None
    for i, col_name in enumerate(schedule.columns):
        if "Indicate" in col_name:
            indicate_col = i
            break
    if indicate_col is not None:
        legend_text = (
            "Indicate:\n• Done (D)\n• Not Needed (X)\n"
            "• If Not Done (leave blank until done)\n"
            "• Task done by (initials)"
        )
        ws.write(2, indicate_col, legend_text, sub_header_fmt)
        ws.set_row(2, 55)

    # --- STEP 8: Write table body ---
    start_row = 3
    for r in range(len(schedule)):
        fmt_row = alt_fmt if (r % 2 == 1) else cell_fmt
        for c, col in enumerate(schedule.columns):
            val = schedule.iat[r, c]
            if "Time" in col:
                ws.write(r + start_row, c, "" if pd.isna(val)
                         else val, time_fmt)
            elif "Date" in col:
                if pd.isna(val):
                    ws.write(r + start_row, c, "", fmt_row)
                elif isinstance(val, str):
                    ws.write(r + start_row, c, val, fmt_row)
                else:
                    ws.write(r + start_row, c, val, date_fmt)
            else:
                ws.write(r + start_row, c, "" if pd.isna(val)
                         else str(val), fmt_row)

    # --- STEP 9: Adjust column widths ---
    widths = {
        "FSS CL Staff": 14,
        "Duty Start Time": 12,
        "Duty Anticipated End Time": 18,
        "Event Start Time": 12,
        "Event End Time": 12,
        "Activity": 10,
        "Title": 10,
        "Full Name": 20,
        "Event/Course": 22,
        "Room": 25,
        "NOTES": 35,
        "Indicate Done(D), Not Needed(X)": 30,
        "List Equipment Used (Laptop, Projector, VGA, Speakers, etc.)": 32,
        "Start Date": 14,
        "End Date": 14,
        "Comments": 28,
    }
    for i, col in enumerate(schedule.columns):
        ws.set_column(i, i, widths.get(col, 18))

    for r in range(start_row, start_row + len(schedule)):
        ws.set_row(r, 30)

    ws.freeze_panes(start_row, 0)
    thick_border = wb.add_format({"border": 2})
    ws.conditional_format(
        0, 0,
        start_row + len(schedule),
        len(schedule.columns) - 1,
        {"type": "no_errors", "format": thick_border}
    )


# Edited by Selena Johnson - put it back to original 15/10/2025

def write_daily_files(df: pd.DataFrame, outdir: str, room_map: dict) -> List[str]:

    os.makedirs(outdir, exist_ok=True)
    written: List[str] = []
    day_series = df["Day"].astype(str).map(_normalize_day_name)

    for day in DAYS_ORDER:
        day_df = df[day_series == day].copy()
        if day_df.empty:
            continue
        day_df = day_df.sort_values(by="_sort_start_time", kind="mergesort")

        path = os.path.join(outdir, f"Lecture Support - {day}.xlsx")
        # Use xlsxwriter with formatting
        with pd.ExcelWriter(path, engine="xlsxwriter") as xw:
            _write_day_sheet(xw, day_df, sheet_name=day, room_map=room_map)
        written.append(path)
        logging.info("Wrote %s (%d rows)", path, len(day_df))
    return written


def write_single_workbook(df: pd.DataFrame, out_path: str, room_map: dict) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        day_series = df["Day"].astype(str).map(_normalize_day_name)
        for day in DAYS_ORDER:
            day_df = df[day_series == day].copy()
            if not day_df.empty:
                day_df = day_df.sort_values(
                    by="_sort_start_time", kind="mergesort")
            else:
                day_df = df.iloc[0:0].copy()

            _write_day_sheet(
                xw,
                day_df,
                sheet_name=day,
                room_map=room_map
            )



def _read_input_excel(path: str, header: Optional[int]) -> pd.DataFrame:
    try:
        if header is None:
            return pd.read_excel(path, sheet_name=0, header=None)
        else:
            return pd.read_excel(path, sheet_name=0, header=header)
    except Exception as e:
        logging.error("Failed to read Excel '%s': %s", path, e)
        sys.exit(2)


# -------------------- Your existing imports --------------------
# from your_module import _read_input_excel, prepare_schedule_table, load_room_setup_requirements, write_daily_files, write_single_workbook

# -------------------- Main Function --------------------
def main(input_path, outdir):
    """
    Generate daily and weekly Lecture Support workbooks.

    Args:
        input_path (str): Path to raw export Excel file.
        outdir (str): Directory to write output workbooks.
    """

    # -------------------- Logging --------------------
    logging.info("Input file: %s", input_path)
    logging.info("Output directory: %s", outdir)

    # -------------------- Read and prepare schedule --------------------
    raw_df = _read_input_excel(input_path, header=None)
    schedule = prepare_schedule_table(raw_df)

    # -------------------- Room Map (embedded) --------------------
    if getattr(sys, 'frozen', False):
        # Running as PyInstaller .exe
        base_path = sys._MEIPASS
    else:
        base_path = os.path.dirname(__file__)

    room_map_path = os.path.join(
        base_path,
        "FSS IT Support Quick Check List - Rooms and Needs 20251111.xlsx"
    )

    room_map = load_room_setup_requirements(room_map_path)

    if not room_map:
        logging.warning(
            "Room setup checklist loaded, but no room mappings were found. "
            "Output will include event equipment only."
        )

    # -------------------- Write Workbooks --------------------
    write_daily_files(schedule, outdir, room_map)

    weekly_path = os.path.join(outdir, "Lecture Support - Weekly.xlsx")
    write_single_workbook(schedule, weekly_path, room_map)
    logging.info("Single weekly workbook written: %s", weekly_path)


# -------------------- CLI Entry Point --------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create daily and weekly Lecture Support workbooks from raw export."
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Path to raw export Excel (.xlsx) file"
    )
    parser.add_argument(
        "--outdir", "-o",
        default="out",
        help="Directory to write output workbooks (default: 'out')"
    )
    parser.add_argument(
        "--log-level", "-l",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity (default: INFO)"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s: %(message)s"
    )

    print(f"Input file: {args.input}")
    print(f"Room map (constant): FSS IT Support Quick Check List - Rooms and Needs 20251111.xlsx")
    print(f"Output folder: {args.outdir}")

    # Call main
    main(args.input, args.outdir)

