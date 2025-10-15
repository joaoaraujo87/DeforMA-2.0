#!/usr/bin/env python3
"""
DeforMA - db_update (user-level DB)
-----------------------------------
Populate the user-level SQLite time_series table (X/Y/Z + N/E/U) from SINEX solutions.

- Reads config via /opt/DeforMA/source/common/load_config.py
- Only modifies the *user* database (cfg.user_workspace.database)
- SINEX files are read from cfg.files.sinex_root (expanded)
- Creates ~/DeforMA/workpool/log/db_update.log

Usage:
  db_update              # Update with new files only
  db_update --reset      # Drop & recreate the table from scratch
"""

from __future__ import annotations
import argparse, gzip, io, math, re, sqlite3, sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional, List

# --- path bootstrap so "common.load_config" imports work even when run from anywhere ---
from pathlib import Path
_THIS = Path(__file__).resolve()
_SRC = _THIS.parent.parent            # /opt/DeforMA/source
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
# --------------------------------------------------------------------------------------


# ---------- Config loader ----------
try:
    from common.load_config import load_config
except Exception as e:
    print(f"[fatal] cannot import common.load_config.load_config: {e}")
    raise


# ---------- Logging ----------
def _log(msg: str, logfile: Path) -> None:
    logfile.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg.rstrip()}\n")


# ---------- SQL ----------
def create_ts_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS time_series;")
    cur.execute(
        """
        CREATE TABLE time_series (
            station TEXT,
            date TEXT,
            reference_frame TEXT,
            x REAL,
            y REAL,
            z REAL,
            n REAL,
            e REAL,
            u REAL,
            PRIMARY KEY (station, date, reference_frame)
        );
        """
    )
    conn.commit()


# ---------- SINEX parsing ----------
_EST_BLOCK_START = re.compile(r'^\s*\+SOLUTION/ESTIMATE', re.I)
_EST_BLOCK_END   = re.compile(r'^\s*\-SOLUTION/ESTIMATE', re.I)
_SNX_EST_LINE = re.compile(
    r'^\s*\d+\s+'
    r'(STAX|STAY|STAZ)\s+'
    r'([A-Z0-9]{4})\s+[A-Z]\s+\d+\s+'
    r'(\d{2}:\d{3}:\d{5})\s+[a-zA-Z]+\s+\d+\s+'
    r'([+\-]?\d+\.\d*(?:[Ee][+\-]?\d+)?)'
)

def _open_text_any(path: Path) -> io.TextIOBase:
    if str(path).lower().endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", errors="ignore")
    return open(path, "r", encoding="utf-8", errors="ignore")

def _epoch_to_date(yy_doy_sec: str, year_hint: Optional[int]) -> str:
    yy_s, doy_s, sec_s = yy_doy_sec.split(':')
    yy, doy, sec = int(yy_s), int(doy_s), int(sec_s)
    if year_hint is not None and (year_hint % 100) == yy:
        yyyy = year_hint
    else:
        yyyy = 1900 + yy if yy >= 80 else 2000 + yy
    base = datetime(yyyy, 1, 1) + timedelta(days=doy - 1, seconds=sec)
    return base.strftime("%Y-%m-%d")

def parse_sinex_xyz(path: Path, year_hint: Optional[int]) -> Dict[Tuple[str, str], Tuple[float, float, float]]:
    grouped: Dict[Tuple[str, str], Dict[str, float]] = {}
    in_block = False
    with _open_text_any(path) as f:
        for line in f:
            if not in_block:
                if _EST_BLOCK_START.match(line):
                    in_block = True
                continue
            if _EST_BLOCK_END.match(line):
                break
            m = _SNX_EST_LINE.match(line)
            if not m:
                continue
            ptype, sta, epoch, val_s = m.groups()
            try:
                val = float(val_s)
            except Exception:
                continue
            date_str = _epoch_to_date(epoch, year_hint)
            key = (sta.upper(), date_str)
            bucket = grouped.setdefault(key, {})
            bucket[ptype] = val

    out: Dict[Tuple[str, str], Tuple[float, float, float]] = {}
    for key, comp in grouped.items():
        if all(k in comp for k in ("STAX", "STAY", "STAZ")):
            out[key] = (comp["STAX"], comp["STAY"], comp["STAZ"])
    return out


# ---------- Coordinate transform XYZ → NEU ----------
def xyz_to_neu(x: float, y: float, z: float) -> Tuple[float, float, float]:
    """Convert ECEF to NEU (approximate, using reference WGS84)."""
    # WGS84 constants
    a = 6378137.0
    e2 = 6.69437999014e-3

    lon = math.atan2(y, x)
    p = math.sqrt(x**2 + y**2)
    lat = math.atan2(z, p * (1 - e2))
    N = a / math.sqrt(1 - e2 * math.sin(lat)**2)
    h = p / math.cos(lat) - N
    n = N * math.sin(lat)
    e = lon * 180 / math.pi
    u = h
    return n, e, u


# ---------- Importer ----------
def import_sinex_into_db(conn: sqlite3.Connection, snx_file: Path, frame: str, year_hint: Optional[int]) -> int:
    xyz_map = parse_sinex_xyz(snx_file, year_hint=year_hint)
    if not xyz_map:
        return 0

    cur = conn.cursor()
    n = 0
    for (sta, date_str), (x, y, z) in xyz_map.items():
        n_val, e_val, u_val = xyz_to_neu(x, y, z)
        cur.execute(
            """
            INSERT OR REPLACE INTO time_series
                (station, date, reference_frame, x, y, z, n, e, u)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (sta, date_str, frame, x, y, z, n_val, e_val, u_val),
        )
        n += 1
    conn.commit()
    return n


# ---------- File discovery ----------
def discover_sinex_files(root: Path, frames: Optional[list[str]] = None, years: Optional[list[int]] = None) -> list[tuple[str, int | None, Path]]:
    results: list[tuple[str, int | None, Path]] = []
    if not root.exists():
        return results
    frame_dirs = [root / f for f in frames] if frames else [p for p in root.iterdir() if p.is_dir()]
    for fdir in frame_dirs:
        if not fdir.is_dir():
            continue
        frame = fdir.name
        year_dirs = [fdir / str(y) for y in years] if years else [p for p in fdir.iterdir() if p.is_dir() and p.name.isdigit()]
        for ydir in year_dirs:
            year_hint = int(ydir.name) if ydir.name.isdigit() else None
            sol_dir = ydir / "SOL"
            dirs = [sol_dir] if sol_dir.is_dir() else [ydir]
            for base in dirs:
                for pat in ("*.SNX", "*.snx", "*.SNX.gz", "*.snx.gz"):
                    for fp in base.glob(pat):
                        results.append((frame, year_hint, fp))
    return results


# ---------- CLI ----------
def main():
    parser = argparse.ArgumentParser(description="Populate DeforMA user-level DB (XYZ + NEU).")
    parser.add_argument("--config", default="/opt/DeforMA/configuration/config.yaml", help="Path to config.yaml")
    parser.add_argument("--reset", action="store_true", help="Drop & recreate the table before import")
    args = parser.parse_args()

    cfg = load_config(args.config)
    user_db = Path(cfg.user_workspace.database).expanduser()
    workpool = Path(cfg.user_workspace.workpool).expanduser()
    log_path = workpool / "log" / "db_update.log"

    sinex_root = Path(cfg.files.sinex_root).expanduser()
    _log(f"=== db_update started ===", log_path)
    _log(f"Config      : {args.config}", log_path)
    _log(f"User DB     : {user_db}", log_path)
    _log(f"SINEX root  : {sinex_root}", log_path)

    if not sinex_root.exists():
        _log(f"[warn] sinex root not found: {sinex_root}", log_path)
        print(f"[warn] sinex root not found: {sinex_root}")
        return

    files = discover_sinex_files(sinex_root)
    _log(f"SINEX files : {len(files)}", log_path)
    print(f"SINEX files : {len(files)}")

    user_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(user_db)

    if args.reset:
        _log("Resetting database...", log_path)
        create_ts_table(conn)
        _log("✓ Table recreated", log_path)
    else:
        _log("Updating existing database...", log_path)

    total_rows = 0
    for frame, year_hint, fp in files:
        try:
            rows = import_sinex_into_db(conn, fp, frame=frame, year_hint=year_hint)
            total_rows += rows
        except Exception as e:
            _log(f"[warn] import failed for {fp}: {e}", log_path)
            print(f"[warn] import failed for {fp}: {e}")

    conn.close()
    _log(f"Files processed: {len(files)}", log_path)
    _log(f"Rows imported  : {total_rows}", log_path)
    _log("=== db_update completed ===\n", log_path)
    print(f"Files processed: {len(files)}")
    print(f"Rows imported  : {total_rows}")
    print("Done.")


if __name__ == "__main__":
    main()

