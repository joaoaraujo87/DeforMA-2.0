#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DeforMA - RTK Web Viewer (Flask)
--------------------------------
- Template: /opt/DeforMA/webpage/templates/web_rtk.html
- Baselines list: ~/DeforMA/metadata/baselines.yaml
- Daily 1-minute data: ~/DeforMA/workpool/alert_rtk/daily/<BASE>/
  Supported daily filename patterns (plain or .gz), for YYDOY:
    <BASE>/<YYDOY>.RTK
    <BASE>/<BASE>_<YYDOY>.RTK      <-- your current naming
    <BASE>_<YYDOY>.RTK             (at daily root)
    <YYDOY>.RTK                    (at daily root)
- API:
    GET /api/baselines
    GET /api/series?baseline=ALTA-PPAD&window=day|week|month&rate=1min|10min|1hour
"""

from __future__ import annotations
import io
import os
import gzip
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Dict

from flask import Flask, jsonify, request, render_template

# ---------------------------------------------------------------------
# Flask app (template folder set to ./templates relative to this file)
# ---------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
app = Flask(__name__, template_folder=str(BASE_DIR / "templates"))

# ---------------------------------------------------------------------
# Paths (user-level)
# ---------------------------------------------------------------------
HOME = Path.home()
METADATA_YAML = HOME / "DeforMA" / "metadata" / "baselines.yaml"
RTK_DAILY_DIR = HOME / "DeforMA" / "workpool" / "alert_rtk" / "daily"

print(f"[info] RTK_DAILY_DIR = {RTK_DAILY_DIR}")
print(f"[info] baselines.yaml = {METADATA_YAML}")

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _yydoy(dt: datetime) -> str:
    return f"{dt.year % 100:02d}{dt.timetuple().tm_yday:03d}"

def _candidate_paths(daily_dir: Path, base: str, yydoy: str) -> List[Path]:
    """
    Return all filename patterns we accept, in priority order.
    """
    b = base
    candidates = [
        daily_dir / b / f"{yydoy}.RTK",
        daily_dir / b / f"{yydoy}.RTK.gz",
        daily_dir / b / f"{b}_{yydoy}.RTK",      # <-- your current layout
        daily_dir / b / f"{b}_{yydoy}.RTK.gz",
        daily_dir / f"{b}_{yydoy}.RTK",          # at root
        daily_dir / f"{b}_{yydoy}.RTK.gz",
        daily_dir / f"{yydoy}.RTK",              # plain yydoy at root (fallback)
        daily_dir / f"{yydoy}.RTK.gz",
    ]
    return candidates

def _open_text(path: Path):
    if str(path).lower().endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="ignore")
    return open(path, "r", encoding="utf-8", errors="ignore")

def _read_daily_file(path: Path) -> List[Tuple[str, float, float, float, int]]:
    """
    Parse daily RTK file lines like:
      # time(yyyy/MM/dd HH:mm:ss) East(m) North(m) Up(m) Q
      2025/10/15 12:47:00 -329.224850 9111.198600 -88.149300 0
    Returns list of (iso_time, E, N, U, Q).
    """
    out = []
    with _open_text(path) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            # Expect: date, time, E, N, U, [Q]
            if len(parts) < 5:
                continue
            ts = f"{parts[0]} {parts[1]}"
            try:
                dt = datetime.strptime(ts, "%Y/%m/%d %H:%M:%S")
                E = float(parts[2]); N = float(parts[3]); U = float(parts[4])
                Q = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else 0
                out.append((dt.isoformat(sep=" "), E, N, U, Q))
            except Exception:
                continue
    return out

def _list_files_for_window(daily_dir: Path, base: str, window_alias: str) -> List[Path]:
    """
    Collect existing daily files covering a rolling window:
      - day   => last 24 hours
      - week  => last 7 days
      - month => last 30 days
    We map the timespan to a set of YYDOY days and look for any supported file per day.
    """
    now = datetime.utcnow()
    if   window_alias == "week":  start = now - timedelta(days=7)
    elif window_alias == "month": start = now - timedelta(days=30)
    else:                         start = now - timedelta(days=1)   # 24h rolling

    # Iterate full days from start.date() to now.date(), inclusive
    day = datetime(start.year, start.month, start.day)
    end_day = datetime(now.year, now.month, now.day)
    files: List[Path] = []

    while day <= end_day:
        yydoy = _yydoy(day)
        found = None
        for p in _candidate_paths(daily_dir, base, yydoy):
            if p.exists():
                found = p
                break
        if found:
            files.append(found)
        day += timedelta(days=1)

    return sorted(files)

def _downsample(records: List[Tuple[str, float, float, float, int]], rate: str
               ) -> List[Tuple[str, float, float, float, int]]:
    """
    Aggregate by floored timestamp to:
      - 1min   -> return as-is (but still deduplicate within a minute)
      - 10min  -> average per 10-minute bin
      - 1hour  -> average per 1-hour bin
    """
    if not records:
        return []

    def _floor_dt(dt: datetime, minutes: int) -> datetime:
        m = (dt.minute // minutes) * minutes
        return dt.replace(second=0, microsecond=0, minute=m)

    out: Dict[str, List[Tuple[float, float, float, int]]] = {}

    for iso, E, N, U, Q in records:
        dt = datetime.fromisoformat(iso)
        if rate == "1hour":
            key_dt = _floor_dt(dt.replace(minute=0), 60)
        elif rate == "10min":
            key_dt = _floor_dt(dt, 10)
        else:
            key_dt = _floor_dt(dt, 1)

        key = key_dt.isoformat(sep=" ")
        out.setdefault(key, []).append((E, N, U, Q))

    # average per bin
    agg = []
    for key in sorted(out.keys()):
        bucket = out[key]
        n = len(bucket)
        E = sum(b[0] for b in bucket) / n
        N = sum(b[1] for b in bucket) / n
        U = sum(b[2] for b in bucket) / n
        # take worst (max) Q in the bin
        Q = max(b[3] for b in bucket)
        agg.append((key, E, N, U, Q))
    return agg

def _normalize_to_first(records: List[Tuple[str, float, float, float, int]]
                       ) -> List[Tuple[str, float, float, float, int]]:
    """
    Shift E,N,U so the first sample is zero.
    """
    if not records:
        return []
    _, e0, n0, u0, _ = records[0]
    out = []
    for (t, e, n, u, q) in records:
        out.append((t, e - e0, n - n0, u - u0, q))
    return out

# ---------------------------------------------------------------------
# Baselines loader
# ---------------------------------------------------------------------
def load_baselines() -> List[str]:
    """
    baselines.yaml example:
      civisa:
        - ALTA-PPAD
        - CISB-GTM3
      repraa:
        - PPAD-SERR
      other_group:
        - ALTA-SBAR
    Returns a flat unique list in YAML order.
    """
    if not METADATA_YAML.exists():
        return []
    try:
        import yaml
        data = yaml.safe_load(METADATA_YAML.read_text(encoding="utf-8")) or {}
    except Exception:
        return []

    order: List[str] = []
    if isinstance(data, dict):
        for _, arr in data.items():
            if isinstance(arr, list):
                order.extend(str(x).strip() for x in arr if str(x).strip())
    else:
        # if it's just a list at the top
        if isinstance(data, list):
            order = [str(x).strip() for x in data if str(x).strip()]

    # de-duplicate preserving order
    seen = set()
    out = [b for b in order if not (b in seen or seen.add(b))]
    return out

# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("web_rtk.html")

@app.route("/api/baselines")
def api_baselines():
    bl = load_baselines()
    print(f"[info] baselines loaded: {len(bl)} -> {bl}")
    return jsonify(bl)

@app.route("/api/series")
def api_series():
    """
    Query params:
      baseline = 'ALTA-PPAD'
      window   = 'day' | 'week' | 'month'   (default: day)
      rate     = '1min' | '10min' | '1hour' (default: 1min)
      normalize = '1' to shift first sample to zero (default: 1)
    """
    baseline = (request.args.get("baseline") or "").strip()
    window   = (request.args.get("window") or "day").strip().lower()
    rate     = (request.args.get("rate") or "1min").strip().lower()
    norm     = (request.args.get("normalize") or "1").strip()

    if not baseline:
        return jsonify({"error": "baseline required"}), 400
    if window not in {"day", "week", "month"}:
        return jsonify({"error": "invalid window"}), 400
    if rate not in {"1min", "10min", "1hour"}:
        return jsonify({"error": "invalid rate"}), 400

    # collect files for window
    files = _list_files_for_window(RTK_DAILY_DIR, baseline, window)
    if not files:
        print(f"[info] no files for {baseline} in window {window}")
        return jsonify({"t": [], "e": [], "n": [], "u": [], "q": []})

    # read & merge
    recs: List[Tuple[str, float, float, float, int]] = []
    for p in files:
        recs.extend(_read_daily_file(p))
    # sort & unique (by timestamp)
    recs.sort(key=lambda r: r[0])
    # drop duplicates keeping last
    dedup: Dict[str, Tuple[str, float, float, float, int]] = {}
    for r in recs:
        dedup[r[0]] = r
    recs = [dedup[k] for k in sorted(dedup.keys())]

    # downsample to requested rate
    recs = _downsample(recs, rate)

    # normalize (first to zero) by default
    if norm in {"1", "true", "yes"}:
        recs = _normalize_to_first(recs)

    # build JSON arrays
    t = [r[0] for r in recs]
    e = [r[1] for r in recs]
    n = [r[2] for r in recs]
    u = [r[3] for r in recs]
    q = [r[4] for r in recs]

    return jsonify({"t": t, "e": e, "n": n, "u": u, "q": q})

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
if __name__ == "__main__":
    # Run the server
    app.run(host="0.0.0.0", port=8082, debug=True)

