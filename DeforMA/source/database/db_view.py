#!/usr/bin/env python3
"""
DeforMA - db_view
-----------------
Export the user-level time series table (subset) to CSV in the workpool.

- Config is loaded via /opt/DeforMA/source/common/load_config.py
- Default CSV:  ~/DeforMA/workpool/db_view.csv
- Log file:     ~/DeforMA/workpool/log/db_view.log
- Columns:      station,date,reference_frame,x,y,z,n,e,u

Usage:
  db_view
  db_view --out ~/DeforMA/workpool/my_timeseries.csv
  db_view --frames IGS20,IGS14 --stations PDEL,BGIN --date-from 2018-01-01 --date-to 2024-12-31
"""

from __future__ import annotations
import argparse, csv, sqlite3, sys
from pathlib import Path
from typing import List, Optional, Tuple
from datetime import datetime

# --- path bootstrap so "common.load_config" imports work even when run from anywhere ---
_THIS = Path(__file__).resolve()
_SRC = _THIS.parent.parent           
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
# --------------------------------------------------------------------------------------


# ----- config loader -----
try:
    from common.load_config import load_config
except Exception as e:
    print(f"[fatal] cannot import common.load_config.load_config: {e}")
    raise


def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _log(log_path: Path, msg: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")


def _build_query(
    frames: Optional[List[str]],
    stations: Optional[List[str]],
    date_from: Optional[str],
    date_to: Optional[str],
) -> Tuple[str, list]:
    sql = [
        "SELECT station, date, reference_frame, x, y, z, n, e, u",
        "FROM time_series",
        "WHERE 1=1",
    ]
    params: list = []

    if frames:
        placeholders = ",".join("?" for _ in frames)
        sql.append(f"AND reference_frame IN ({placeholders})")
        params.extend(frames)

    if stations:
        placeholders = ",".join("?" for _ in stations)
        sql.append(f"AND station IN ({placeholders})")
        params.extend(stations)

    if date_from:
        sql.append("AND date >= ?")
        params.append(date_from)

    if date_to:
        sql.append("AND date <= ?")
        params.append(date_to)

    sql.append("ORDER BY station, reference_frame, date")
    return " ".join(sql), params


def main():
    parser = argparse.ArgumentParser(description="Export DeforMA user DB time_series to workpool CSV.")
    parser.add_argument("--config", default="/opt/DeforMA/configuration/config.yaml", help="Path to config.yaml")
    parser.add_argument("--out", default="", help="Explicit CSV output path (overrides default workpool path)")
    parser.add_argument("--frames", default="", help="Comma-separated frame filter (e.g., IGS20,IGS14)")
    parser.add_argument("--stations", default="", help="Comma-separated station filter (e.g., PDEL,BGIN)")
    parser.add_argument("--date-from", default="", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--date-to", default="", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    cfg = load_config(args.config)

    user_db  = Path(cfg.user_workspace.database).expanduser()
    workpool = Path(cfg.user_workspace.workpool).expanduser()
    csv_out  = Path(args.out).expanduser() if args.out.strip() else (workpool / "db_view.csv")
    log_path = workpool / "log" / "db_view.log"

    # Filters
    frames   = [s.strip() for s in args.frames.split(",") if s.strip()] or None
    stations = [s.strip().upper() for s in args.stations.split(",") if s.strip()] or None
    date_from = args.date_from.strip() or None
    date_to   = args.date_to.strip() or None

    # Start log
    _log(log_path, "-" * 72)
    _log(log_path, f"db_view start: {datetime.utcnow().isoformat()}Z")
    _log(log_path, f"Config   : {args.config}")
    _log(log_path, f"User DB  : {user_db}")
    _log(log_path, f"CSV out  : {csv_out}")
    if frames:   _log(log_path, f"Frames   : {', '.join(frames)}")
    if stations: _log(log_path, f"Stations : {', '.join(stations)}")
    if date_from or date_to:
        _log(log_path, f"Date rng : {date_from or '…'} → {date_to or '…'}")

    # Console summary
    print(f"Config  : {args.config}")
    print(f"User DB : {user_db}")
    print(f"CSV out : {csv_out}")
    if frames:   print(f"Frames  : {', '.join(frames)}")
    if stations: print(f"Stations: {', '.join(stations)}")
    if date_from or date_to:
        print(f"Date rng: {date_from or '…'} → {date_to or '…'}")

    if not user_db.exists():
        msg = f"[error] user DB not found: {user_db}"
        print(msg)
        _log(log_path, msg)
        raise SystemExit(2)

    sql, params = _build_query(frames, stations, date_from, date_to)

    header = ["station", "date", "reference_frame", "x", "y", "z", "n", "e", "u"]

    total = 0
    try:
        _ensure_parent(csv_out)
        with sqlite3.connect(user_db) as conn, open(csv_out, "w", newline="", encoding="utf-8") as f:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='time_series'")
            if not cur.fetchone():
                msg = "[error] table 'time_series' does not exist in the user DB."
                print(msg)
                _log(log_path, msg)
                raise SystemExit(2)

            cur.execute(sql, params)
            w = csv.writer(f)
            w.writerow(header)

            for row in cur:
                # row = (station, date, reference_frame, x, y, z, n, e, u)
                w.writerow(row)
                total += 1
    except sqlite3.Error as e:
        msg = f"[sqlite] {e}"
        print(msg)
        _log(log_path, msg)
        raise SystemExit(2)

    _log(log_path, f"Rows exported: {total}")
    _log(log_path, "db_view done.")
    print(f"Rows exported: {total}")
    print("Done.")


if __name__ == "__main__":
    main()

