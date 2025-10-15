#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DeforMA - alert_rtk (workpool writer)
-------------------------------------
- Source 1 Hz base files:  ~/DeforMA/database/<BASELINE>.RTK
- Output 1-min daily:      ~/DeforMA/workpool/alert_rtk/daily/<BASELINE>/<BASELINE>_YYDOY.RTK
- Log:                     ~/DeforMA/workpool/alert_rtk/log/alert_rtk.log
- Baselines list:          ~/DeforMA/metadata/baselines.yaml

Env overrides:
  DEFORMA_BASELINES_FILE  -> baselines.yaml path
  DEFORMA_RTK_BASE_DIR    -> base 1 Hz folder (default: ~/DeforMA/database)
  DEFORMA_WORKPOOL_DIR    -> workpool root (default: ~/DeforMA/workpool)

Typical cron (every 10 min):
  */10 * * * * /usr/bin/python3 /opt/DeforMA/source/alert/alert_rtk.py >> /dev/null 2>&1
"""

from __future__ import annotations
import argparse
import io
import gzip
import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import yaml

# ====================== Paths & logging ======================

HOME = Path(os.path.expanduser("~"))

def _default_baselines_yaml() -> Path:
    return HOME / "DeforMA" / "metadata" / "baselines.yaml"

def _default_base_rtk_dir() -> Path:
    return HOME / "DeforMA" / "database"

def _default_workpool_dir() -> Path:
    return HOME / "DeforMA" / "workpool"

BASELINES_YAML = Path(os.environ.get("DEFORMA_BASELINES_FILE") or _default_baselines_yaml())
BASE_RTK_DIR   = Path(os.environ.get("DEFORMA_RTK_BASE_DIR") or _default_base_rtk_dir())
WORKPOOL_DIR   = Path(os.environ.get("DEFORMA_WORKPOOL_DIR") or _default_workpool_dir())

OUT_DAILY_ROOT = WORKPOOL_DIR / "alert_rtk" / "daily"   # per-baseline subfolders inside
LOG_DIR        = WORKPOOL_DIR / "alert_rtk" / "log"
LOG_PATH       = LOG_DIR / "alert_rtk.log"

def _log(msg: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg.rstrip()}\n")

# ====================== Helpers ==============================

def _load_baselines_from_yaml(p: Path) -> List[str]:
    if not p.exists():
        _log(f"[warn] baselines.yaml not found: {p}")
        return []
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception as e:
        _log(f"[warn] cannot read baselines.yaml: {e}")
        return []
    out: List[str] = []
    if isinstance(data, dict):
        for _, arr in data.items():
            if isinstance(arr, list):
                out.extend(str(x).strip().upper() for x in arr if str(x).strip())
    # de-dup preserve order
    seen = set()
    out = [b for b in out if not (b in seen or seen.add(b))]
    return out

def _safe_copy(src: Path, dst: Path) -> bool:
    try:
        if not src.exists():
            return False
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        return True
    except Exception as e:
        _log(f"[warn] copy failed {src} -> {dst}: {e}")
        return False

def _open_text_any(path: Path) -> io.TextIOBase:
    if str(path).lower().endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="ignore")
    return open(path, "r", encoding="utf-8", errors="ignore")

def _yy_doy(dt: datetime) -> Tuple[int,int]:
    return dt.year % 100, int(dt.strftime("%j"))

def _daily_file_name(baseline: str, dt: datetime) -> str:
    yy, doy = _yy_doy(dt)
    return f"{baseline.upper()}_{yy:02d}{doy:03d}.RTK"

# ====================== Parsing & DS =========================

def _parse_base_rtk(path: Path) -> List[Tuple[datetime, float, float, float, int]]:
    """
    Parse base 1 Hz RTK file (.RTK).
    Skips first 2 header lines; each data line has fixed-width:
      [0:19]  timestamp "yyyy/MM/dd HH:mm:ss"
      [24:38] East (m)
      [39:53] North (m)
      [54:68] Up (m)
      [71:72] Quality (single char/int)
    Returns: list of (dt, E, N, U, q)
    """
    out: List[Tuple[datetime, float, float, float, int]] = []
    if not path.exists():
        return out
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception as e:
        _log(f"[warn] failed reading {path}: {e}")
        return out
    if len(lines) <= 2:
        return out

    for line in lines[2:]:
        if len(line) < 72:
            continue
        ts = line[0:19].strip()
        es = line[24:38].strip()
        ns = line[39:53].strip()
        us = line[54:68].strip()
        qs = line[71:72].strip()
        # tolerant parse
        try:
            dt = datetime.strptime(ts, "%Y/%m/%d %H:%M:%S")
        except Exception:
            try:
                y, M, d = ts[0:4], ts[5:7].lstrip("/"), ts[8:10].lstrip("/")
                h, m, s = ts[11:13], ts[14:16], ts[17:19]
                dt = datetime(int(y), int(M), int(d), int(h), int(m), int(s))
            except Exception:
                continue
        try:
            E = float(es); N = float(ns); U = float(us)
        except Exception:
            continue
        try:
            q = int(qs) if qs else 0
        except Exception:
            q = 0
        out.append((dt, E, N, U, q))
    return out

def _downsample_1min(samples: List[Tuple[datetime,float,float,float,int]],
                     method: str = "mean"
                    ) -> Dict[datetime, Tuple[float,float,float,int]]:
    """
    Group 1Hz samples by minute (YYYY-mm-dd HH:MM:00) and aggregate to one point.
    Returns dict { minute_dt : (E, N, U, q) }.
    q aggregated as max (conservative).
    """
    from statistics import mean, median
    buckets: Dict[datetime, List[Tuple[float,float,float,int]]] = {}
    for dt, E, N, U, q in samples:
        minute_dt = dt.replace(second=0, microsecond=0)
        buckets.setdefault(minute_dt, []).append((E, N, U, q))

    out: Dict[datetime, Tuple[float,float,float,int]] = {}
    for k, arr in buckets.items():
        Es = [a[0] for a in arr]; Ns = [a[1] for a in arr]; Us = [a[2] for a in arr]; Qs = [a[3] for a in arr]
        if method == "median":
            aggE = float(median(Es)); aggN = float(median(Ns)); aggU = float(median(Us))
        else:
            aggE = float(mean(Es));   aggN = float(mean(Ns));   aggU = float(mean(Us))
        aggQ = int(max(Qs)) if Qs else 0
        out[k] = (aggE, aggN, aggU, aggQ)
    return out

# ====================== Daily I/O ===========================

def _read_daily_file(path_plain_or_gz: Path) -> Dict[datetime, Tuple[float,float,float,int]]:
    """
    Read a 1-minute daily file (.RTK or .RTK.gz) into {minute_dt: (E,N,U,q)}.
    """
    fp = path_plain_or_gz
    if not fp.exists():
        gz = Path(str(path_plain_or_gz) + ".gz")
        fp = gz if gz.exists() else None
    if fp is None:
        return {}

    try:
        with _open_text_any(fp) as f:
            lines = f.readlines()
    except Exception:
        return {}

    if len(lines) <= 2:
        return {}

    data: Dict[datetime, Tuple[float,float,float,int]] = {}
    for line in lines[2:]:
        if len(line) < 72:
            continue
        ts = line[0:19].strip()
        es = line[24:38].strip()
        ns = line[39:53].strip()
        us = line[54:68].strip()
        qs = line[71:72].strip()
        try:
            dt = datetime.strptime(ts, "%Y/%m/%d %H:%M:%S")
        except Exception:
            try:
                y, M, d = ts[0:4], ts[5:7].lstrip("/"), ts[8:10].lstrip("/")
                h, m, s = ts[11:13], ts[14:16], ts[17:19]
                dt = datetime(int(y), int(M), int(d), int(h), int(m), int(s))
            except Exception:
                continue
        try:
            E = float(es); N = float(ns); U = float(us); q = int(qs) if qs else 0
        except Exception:
            continue
        data[dt] = (E, N, U, q)
    return data

def _write_daily_file(path_plain: Path, data: Dict[datetime, Tuple[float,float,float,int]]) -> None:
    """
    Overwrite plain .RTK with sorted minute rows and a minimal 2-line header.
    """
    path_plain.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(data.items(), key=lambda kv: kv[0])
    with open(path_plain, "w", encoding="utf-8") as f:
        f.write("# DeforMA RTK 1-minute daily file\n")
        f.write("# time(yyyy/MM/dd HH:mm:ss)       East(m)        North(m)          Up(m)   Q\n")
        for dt, (E, N, U, q) in rows:
            f.write(f"{dt.strftime('%Y/%m/%d %H:%M:%S')}    {E:>13.6f}    {N:>13.6f}    {U:>13.6f}   {q:1d}\n")

def _gzip_if_old(path_plain: Path, days_ago: int) -> None:
    """
    Gzip the daily file if its date is older than N days. (Skip if gz already exists.)
    """
    name = path_plain.name  # <BASE>_YYDOY.RTK
    if "_" not in name:
        return
    try:
        suff = name.split("_")[-1].split(".")[0]  # e.g., 25288
        yy = 2000 + int(suff[:2])
        doy = int(suff[2:])
        day_date = datetime(yy, 1, 1) + timedelta(days=doy - 1)
    except Exception:
        return

    if (datetime.now() - day_date).days <= days_ago:
        return

    gz = Path(str(path_plain) + ".gz")
    if gz.exists() or not path_plain.exists():
        return

    try:
        with open(path_plain, "rb") as f_in, gzip.open(gz, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        path_plain.unlink(missing_ok=True)
        _log(f"gzipped {path_plain.name} -> {gz.name}")
    except Exception as e:
        _log(f"[warn] gzip failed for {path_plain}: {e}")

# ====================== Core ================================

def process_baseline(baseline: str, agg_method: str, gzip_days: int) -> None:
    baseline = baseline.upper().strip()
    base_src  = BASE_RTK_DIR / f"{baseline}.RTK"
    base_copy = BASE_RTK_DIR / f"{baseline}.RTK_copy"

    if not _safe_copy(base_src, base_copy):
        _log(f"[info] base file missing or copy failed for {baseline}: {base_src}")
        return

    samples = _parse_base_rtk(base_copy)
    base_copy.unlink(missing_ok=True)

    if not samples:
        _log(f"[info] no samples parsed for {baseline}")
        return

    # Downsample to 1-minute
    per_min = _downsample_1min(samples, method=agg_method)

    # Group per day and write to workpool daily area
    for minute_dt, tup in per_min.items():
        day_dir   = OUT_DAILY_ROOT / baseline
        day_file  = day_dir / _daily_file_name(baseline, minute_dt)

        # Load existing data (plain or gz)
        current = _read_daily_file(day_file)
        current[minute_dt] = tup  # overwrite/insert this minute
        _write_daily_file(day_file, current)
        _log(f"{baseline}: updated {day_file.name} (minute {minute_dt.strftime('%Y-%m-%d %H:%M')})")

        if gzip_days >= 0:
            _gzip_if_old(day_file, gzip_days)

# ====================== CLI ================================

def main():
    parser = argparse.ArgumentParser(description="Maintain 1-minute RTK daily files in workpool/alert_rtk.")
    parser.add_argument("--baselines", default="", help="Comma-separated list (override baselines.yaml)")
    parser.add_argument("--method", choices=["mean", "median"], default="mean", help="Downsample method (default: mean)")
    parser.add_argument("--gzip-days", type=int, default=2, help="Gzip daily files older than N days (default: 2). Use -1 to disable.")
    args = parser.parse_args()

    _log("=== alert_rtk run ===")
    _log(f"BASE_RTK_DIR     = {BASE_RTK_DIR}")
    _log(f"WORKPOOL_DIR     = {WORKPOOL_DIR}")
    _log(f"OUT_DAILY_ROOT   = {OUT_DAILY_ROOT}")
    _log(f"BASELINES_YAML   = {BASELINES_YAML}")
    _log(f"method={args.method} gzip_days={args.gzip_days}")

    OUT_DAILY_ROOT.mkdir(parents=True, exist_ok=True)

    if args.baselines.strip():
        baselines = [b.strip().upper() for b in args.baselines.split(",") if b.strip()]
    else:
        baselines = _load_baselines_from_yaml(BASELINES_YAML)

    if not baselines:
        _log("[warn] no baselines to process (empty list).")
        print("No baselines found.")
        _log("=== alert_rtk done ===")
        return

    for b in baselines:
        try:
            process_baseline(b, agg_method=args.method, gzip_days=args.gzip_days)
        except Exception as e:
            _log(f"[error] baseline {b}: {e}")

    _log("=== alert_rtk done ===")

if __name__ == "__main__":
    main()

