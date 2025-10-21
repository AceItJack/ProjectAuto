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

import argparse
import os
import sys
import logging
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

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
                    if pd.notna(val):
                        # ensure it's a date
                        if isinstance(val, pd.Timestamp):
                            ws.write_datetime(r + 1, c, val, date_fmt)
                        else:
                            ts = pd.to_datetime(val, errors="coerce")
                            if pd.notna(ts):
                                ws.write_datetime(r + 1, c, ts, date_fmt)
                            else:
                                ws.write(r + 1, c, "" if pd.isna(val)
                                         else str(val), wrap_fmt)
                    else:
                        ws.write(r + 1, c, "", wrap_fmt)
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

# Added by Selena Johnson
# Combine equipment columns into one string


def combine_equipment(row):
    eq = []
    for col in ["FSS Laptop", "Data Projector", "Speakers", "Microphone (G102 only)"]:
        if pd.notna(row.get(col)) and str(row[col]).strip():
            eq.append(col)
    return ", ".join(eq)


def prepare_schedule_table(raw_df: pd.DataFrame, header_token: str = "serial") -> pd.DataFrame:
    """
    - Detect header
    - Rename to canonical
    - Parse dates/times
    - Add PU/SU
    - Return normalized table
    """
    header_row = detect_header_row(raw_df, search_token=header_token)
    data = raw_df.iloc[header_row + 1:].copy()
    data.columns = raw_df.iloc[header_row].tolist()

    # Drop fully-empty cols/rows and trim whitespace
    data = data.dropna(axis=1, how="all")
    data = data.dropna(axis=0, how="all")
    data = data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Build fuzzy column map
    col_map = build_column_map([c for c in data.columns if isinstance(c, str)])

    def col_get(key: str) -> Optional[str]:
        return col_map.get(key)

    # Build the output frame using canonical names
    out = pd.DataFrame()

    # Serial
    out["Serial"] = data.get(col_get("Serial"), pd.Series(dtype="object"))

    # Dates
    start_date_col = col_get("Start Date:")
    if start_date_col:
        out["Date"] = pd.to_datetime(data.get(start_date_col), errors="coerce")
    else:
        out["Date"] = pd.NaT

    # Day text (infer from Date if missing)
    day_col = col_get("Day of Week:")
    if day_col and day_col in data.columns:
        day_series = data[day_col].astype(str).map(_normalize_day_name)
    else:
        # If no day column, infer from Date
        day_series = pd.Series(
            [d.day_name() if pd.notna(d) else "" for d in out["Date"]], index=data.index
        )
    out["Day"] = day_series

    # Times (keep original text & parse for sorting)
    start_time_col = col_get("Start Time:")
    end_time_col = col_get("End Time:")

    out["Duty Start time"] = data.get(
        start_time_col, pd.Series(dtype="object"))
    out["Duty End time"] = data.get(end_time_col, pd.Series(dtype="object"))

    out["_sort_start_time"] = data.get(
        start_time_col, pd.Series(dtype="object")).map(parse_time_flex)

    # Textual details
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
    ]
    for new_name, canon in mapping_pairs:
        src = col_get(canon)
        out[new_name] = data.get(src, pd.Series(dtype="object"))

    # Add PU/SU
    out["PU"] = ""
    out["SU"] = ""

    # Order columns nicely
    for col in OUTPUT_COLS:
        if col not in out.columns:
            out[col] = "" if col not in ("Date",) else pd.NaT

    out = out[[c for c in OUTPUT_COLS] + ["_sort_start_time"]]

    # Drop rows that are completely empty in key fields
    key_cols = ["Date", "Day", "Duty Start time",
                "Department/Unit", "Course/Event", "Room"]
    out = out[~out[key_cols].isna().all(axis=1)].copy()

    return out

# Added by Selena Johnson


def build_schedule_format(df: pd.DataFrame) -> pd.DataFrame:
    # Transform normalized data into target schedule layout

    schedule = pd.DataFrame()

    # Adjusts time 15 minutes before and after events

    def adjust_time(t: time, delta_minutes: int) -> time:
        if pd.isna(t):
            return np.nan
        try:
            # Parse time safely into a datetime.time
            if isinstance(t, str):  # string input
                parsed = pd.to_datetime(t, errors="coerce").time()

            elif isinstance(t, pd.Timestamp):  # already datetime
                parsed = t.time()
            elif isinstance(t, time):
                parsed = t
            else:
                # try to parse other types
                parsed = pd.to_datetime(t, errors="coerce").time()

            # create a datetime for manipulation
            base_dt = datetime.combine(datetime.today(), parsed)
            adjusted_dt = base_dt + \
                timedelta(minutes=delta_minutes)  # adjust time
            return adjusted_dt.time()  # return only the time part
        except Exception:
            return np.nan  # return NaN on failure

    # Parse event start and end times
    event_start = df["Duty Start time"].apply(parse_time_flex)
    event_end = df["Duty End time"].apply(parse_time_flex)

    # Map the colums
    schedule["FSS CL Staff"] = ""  # assign later/manually
    schedule["Duty Start Time"] = event_start.apply(
        lambda t: adjust_time(t, -15))
    schedule["Duty Anticipated End Time"] = event_end.apply(
        lambda t: adjust_time(t, 15))
    schedule["Event Start Time"] = event_start
    schedule["Event End Time"] = event_end
    schedule["Activity"] = ""  # placeholder
    schedule["Title"] = ""  # placeholder
    schedule["Full Name"] = df["Requester Name"]
    schedule["Event/Course"] = df["Course/Event"]
    schedule["Room Assigned"] = df["Room"]
    schedule["NOTES"] = df["Support Request"]
    schedule["Indicate Done(D), Not Needed(X)"] = ""
    schedule["List Equipment Used (Laptop, Projector, VGA, Speakers, etc.)"] = df.apply(
        combine_equipment, axis=1)
    schedule["Start Date"] = df["Date"].dt.strftime("%d-%b-%y")
    schedule["End Date"] = df["Date"].dt.strftime("%d-%b-%y")
    schedule["Comments"] = ""
    return schedule


'''

    # Combine Course/Event + Requester time
    schedule["Activity Title / Full Name / Event-Course"] = (
        df["Course/Event"].fillna("") + " - " + df["Requester Name"].fillna("")
    ).str.strip(" -")

    schedule["Room Assigned"] = df["Room"]
    schedule["NOTES / Instructions"] = df["Support Request"]

    # Combine equipment into one column
    schedule["Equipment Used"] = df.apply(combine_equipment, axis=1)

    # Dates
    schedule["Start Date"] = df["Date"]
    schedule["End Date"] = df["Date"]

    # Placeholder for comments
    schedule["Comments"] = ""

    return schedule
'''
# Added by Selena Johnson


def _write_day_sheet(xw, df: pd.DataFrame, sheet_name: str):
    # Formatted duty schedule

    schedule = build_schedule_format(df)
    ws = xw.book.add_worksheet(sheet_name)
    wb = xw.book

    # STYLES
    title_fmt = wb.add_format({
        "bold": True, "align": "center", "valign": "vcenter", "font_size": 16, "underline": True
    })

    header_fmt = wb.add_format({
        "bold": True, "align": "center", "valign": "vcenter", "text_wrap": True, "border": 1, "italic": True, "font_size": 19,
        "bg_color": "#E2EFDA"
    })

    sub_header_fmt = wb.add_format({
        "align": "left", "valign": "top", "text_wrap": True,
        "border": 1, "italic": True, "font_size": 9, "bg_color": "#E2EFDA"
    })

    cell_fmt = wb.add_format({
        "valign": "top", "text_wrap": True, "border": 1
    })
    alt_fmt = wb.add_format({
        "valign": "top", "text_wrap": True, "border": 1, "bg_color": "#F2F2F2"
    })
    time_fmt = wb.add_format({
        "num_format": "hh:mm", "align": "center", "border": 1
    })
    date_fmt = wb.add_format({
        "num_format": "dd-mmm-yy", "align": "center", "border": 1
    })

    # TITLE ROW

    ws.merge_range(0, 0, 0, len(schedule.columns) - 1, sheet_name, title_fmt)
    ws.set_row(0, 25)

    # === HEADER ROW ===
    for c, col_name in enumerate(schedule.columns):
        ws.write(1, c, col_name, header_fmt)
    ws.set_row(1, 35)  # header row height

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
        ws.set_row(2, 55)  # give height for the bullet list

    # === WRITE MAIN DATA (starting row 3 in Excel) ===
    start_row = 3
    for r in range(len(schedule)):
        # Alternate row shading: every other block of 3-4 can be shaded
        fmt_row = alt_fmt if (r % 2 == 1) else cell_fmt
        for c, col in enumerate(schedule.columns):
            val = schedule.iat[r, c]
            # time/date formatting
            if "Time" in col:
                ws.write(r + start_row, c, str(val), time_fmt)
            elif "Date" in col:
                ws.write(r + start_row, c, val, date_fmt)
            else:
                ws.write(r + start_row, c, "" if pd.isna(val)
                         else str(val), fmt_row)

    # === COLUMN WIDTHS ===
    widths = {
        "FSS CL Staff": 19, "Duty Start Time": 12, "Duty Anticipated End Time": 25,
        "Event Start Time": 12, "Event End Time": 12,
        "Activity": 10, "Title": 15, "Full Name": 20, "Event/Course": 22,
        "Room:": 25, "NOTES": 35,
        "Indicate: Done (D), Not Needed (X), If Not Done...": 30,
        "List Equipment Used (Laptop, Projector, VGA, Speakers, etc.)": 32,
        "Start Date:": 14, "End Date:": 14, "Comments": 28
    }

    for i, col in enumerate(schedule.columns):
        ws.set_column(i, i, widths.get(col, 18))

# ROW HEIGHTS
    for r in range(start_row, start_row + len(schedule)):
        ws.set_row(r, 35)

# FREEZE the top rows

    ws.freeze_panes(start_row, 0)

# BORDERS
    thick_border = wb.add_format({"border": 2})
    ws.conditional_format(
        0, 0, start_row + len(schedule), len(schedule.columns) - 1,
        {"type": "no_errors", "format": thick_border}
    )


'''
#Sir original
def _write_day_sheet(xw, df: pd.DataFrame, sheet_name: str):
    df_to_write = df.copy()
    if "_sort_start_time" in df_to_write.columns:
        df_to_write = df_to_write.drop(columns=["_sort_start_time"])
    # Maintain column order
    df_to_write = df_to_write[[
        c for c in OUTPUT_COLS if c in df_to_write.columns]]

    # Create sheet and apply styling with autofit
    ws = xw.book.add_worksheet(sheet_name)
    # write headers & body via style helper
    _autofit_and_style(ws, df_to_write, xw.book)

'''
# Edited by Selena Johnson - put it back to original 15/10/2025


def write_daily_files(df: pd.DataFrame, outdir: str) -> List[str]:
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
            _write_day_sheet(xw, day_df, sheet_name=day)
        written.append(path)
        logging.info("Wrote %s (%d rows)", path, len(day_df))
    return written


def write_single_workbook(df: pd.DataFrame, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        day_series = df["Day"].astype(str).map(_normalize_day_name)
        for day in DAYS_ORDER:
            day_df = df[day_series == day].copy()
            if not day_df.empty:
                day_df = day_df.sort_values(
                    by="_sort_start_time", kind="mergesort")
            else:
                # Keep an empty sheet with headers
                day_df = df.iloc[0:0].copy()
            _write_day_sheet(xw, day_df, sheet_name=day)


def _read_input_excel(path: str, header: Optional[int]) -> pd.DataFrame:
    try:
        if header is None:
            return pd.read_excel(path, sheet_name=0, header=None)
        else:
            return pd.read_excel(path, sheet_name=0, header=header)
    except Exception as e:
        logging.error("Failed to read Excel '%s': %s", path, e)
        sys.exit(2)


def main():
    ap = argparse.ArgumentParser(
        description="Create daily Lecture Support workbooks from raw export.")
    ap.add_argument("input", help="Path to raw export .xlsx")
    ap.add_argument("--outdir", default="out",
                    help="Directory to write day workbooks")
    ap.add_argument("--single-workbook", action="store_true",
                    help="Also write a single workbook with 7 sheets")
    ap.add_argument("--single-path", default="out/Lecture Support - Weekly.xlsx",
                    help="Path for the single workbook if --single-workbook is set")
    ap.add_argument("--header-token", default="serial",
                    help="Token to detect header row (case-insensitive). Default: 'serial'")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                    help="Logging verbosity (default: INFO)")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(levelname)s: %(message)s"
    )
# Edited by Selena Johnson
    raw_df = _read_input_excel(args.input, header=None)
    # schedule = prepare_schedule_table(
    # raw_df, header_token=args.header_token)
    schedule = prepare_schedule_table(raw_df)

    write_daily_files(schedule, args.outdir)
    if args.single_workbook:
        write_single_workbook(schedule, os.path.join(
            args.outdir, "Lecture Support - Weekly.xlsx"))


if __name__ == "__main__":
    main()
'''
if args.single_workbook:
    write_single_workbook(schedule, args.single_path)
    print("Also wrote:", args.single_path)

'''
if __name__ == "__main__":
    main()
