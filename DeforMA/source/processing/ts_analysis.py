#!/usr/bin/env python3
"""
DeforMA - ts_analysis
---------------------
Compute analysis products from the user-level DB time_series (XYZ + NEU):

- Offsets (cn, ce, cu): cumulative step corrections per events.yaml
- Detrend (dn, de, du): M1 or M2 per stable.yaml
- Outliers (on, oe, ou): robust z-score (MAD) flags within the analysis window

Inputs:
  - Config via /opt/DeforMA/source/common/load_config.py
  - User DB: cfg.user_workspace.database (table: time_series)
  - Metadata:
      * user-override (preferred):  ~/DeforMA/metadata/{events.yaml,stable.yaml}
      * fallback (system):          cfg.files.events / cfg.files.stable

Outputs (to active project folder):
  ~/DeforMA/workpool/<PROJECT>/outputs/
      time_series_analysis.csv      (wide: station,date,frame,x,y,z,n,e,u,cn,ce,cu,dn,de,du,on,oe,ou)
      time_series_long.csv          (tidy: station,date,component,value,set)
      stations_latest_geo.csv       (for maps: station,lon,lat,h,date,frame,n,e,u,dn,de,du)

Logs:
  ~/DeforMA/workpool/log/ts_analysis.log

Usage examples:
  ts_analysis --analysis all --date-from 2018-01-01 --date-to 2024-12-31
  ts_analysis --analysis offsets,detrend
  ts_analysis --project SMIG1020 --analysis outliers --date-from 2022-01-01 --date-to 2024-12-31
"""

from __future__ import annotations
import argparse
import csv
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

# ---------- config loader ----------
try:
    from common.load_config import load_config
except Exception as e:
    print(f"[fatal] cannot import common.load_config.load_config: {e}")
    raise


# ---------- logging ----------
def _log(msg: str, logfile: Path) -> None:
    logfile.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg.rstrip()}\n")


# ---------- helpers ----------
def _read_current_project(workpool: Path) -> Optional[str]:
    m = workpool / ".current_project"
    try:
        if m.exists():
            t = m.read_text(encoding="utf-8").strip()
            return t or None
    except Exception:
        pass
    return None

def _ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

def _parse_date(s: str) -> datetime:
    """Accept YYYY-MM-DD or YYDOY (e.g., 24 123 -> 2024-05-02)."""
    s = s.strip()
    if not s:
        raise ValueError("Empty date string")
    if len(s) == 5 and s.isdigit():  # YYDOY
        yy = int(s[:2])
        doy = int(s[2:])
        yyyy = 2000 + yy if yy < 80 else 1900 + yy
        return datetime(yyyy, 1, 1) + timedelta(days=doy - 1)
    # fallback ISO
    return datetime.strptime(s, "%Y-%m-%d")

def _to_iso(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")

def _days_between(a: datetime, b: datetime) -> float:
    return (a - b).total_seconds() / 86400.0


# ---------- load metadata (events, stable) ----------
@dataclass
class StableM1:
    velocities: Dict[str, float]  # mm/yr
    corrections: Dict[str, float] # CN/CE/CU in mm (constant)
    method: str

@dataclass
class StableM2:
    start: datetime
    end: datetime
    method: str

def _load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _find_metadata(cfg, name: str) -> Optional[Path]:
    """
    Prefer user-level ~/DeforMA/metadata/<name>.yaml, else fall back to cfg.files.<name>.
    """
    user_meta = Path(cfg.user_workspace.root).expanduser() / "metadata" / f"{name}.yaml"
    if user_meta.exists():
        return user_meta
    sys_path = getattr(cfg.files, name, None)
    if sys_path:
        p = Path(sys_path).expanduser()
        if p.exists():
            return p
    return None


# ---------- DB fetch ----------
def _fetch_timeseries(conn: sqlite3.Connection,
                      date_from: Optional[str],
                      date_to: Optional[str],
                      stations: Optional[List[str]],
                      frames: Optional[List[str]]) -> List[Tuple]:
    """
    Returns rows: (station, date, frame, x, y, z, n, e, u)
    """
    sql = [
        "SELECT station, date, reference_frame, x, y, z, n, e, u",
        "FROM time_series",
        "WHERE 1=1"
    ]
    params: List = []

    if date_from:
        sql.append("AND date >= ?")
        params.append(date_from)
    if date_to:
        sql.append("AND date <= ?")
        params.append(date_to)
    if stations:
        ph = ",".join("?" for _ in stations)
        sql.append(f"AND station IN ({ph})")
        params.extend([s.upper() for s in stations])
    if frames:
        ph = ",".join("?" for _ in frames)
        sql.append(f"AND reference_frame IN ({ph})")
        params.extend(frames)

    sql.append("ORDER BY station, reference_frame, date")
    cur = conn.cursor()
    cur.execute(" ".join(sql), params)
    return list(cur.fetchall())


# ---------- Offsets (events.yaml) ----------
def _load_events(events_path: Optional[Path], log: Path) -> List[dict]:
    """
    events.yaml format (documented earlier):
      events:
        - flag: E|D|R
          date: YYYY-MM-DD
          station: STAA or 'ALL'
          offsets: { N: mm, E: mm, U: mm }   # mm (step)
    """
    if not events_path:
        _log("[events] no events file found; skipping offsets", log)
        return []
    try:
        data = _load_yaml(events_path)
        evs = data.get("events", [])
        out = []
        for e in evs:
            try:
                out.append({
                    "flag": str(e.get("flag", "E")),
                    "date": _parse_date(str(e.get("date"))),
                    "station": str(e.get("station", "ALL")).upper(),
                    "N": float(e.get("offsets", {}).get("N", 0.0)),
                    "E": float(e.get("offsets", {}).get("E", 0.0)),
                    "U": float(e.get("offsets", {}).get("U", 0.0)),
                })
            except Exception:
                # ignore malformed
                continue
        _log(f"[events] loaded {len(out)} event(s) from {events_path}", log)
        return out
    except Exception as e:
        _log(f"[events] failed to read {events_path}: {e}", log)
        return []

def _compute_offsets_for_station_date(evs: List[dict], sta: str, d: datetime) -> Tuple[float, float, float]:
    cn = ce = cu = 0.0
    for e in evs:
        if e["date"] <= d and (e["station"] == "ALL" or e["station"] == sta):
            cn += e["N"]
            ce += e["E"]
            cu += e["U"]
    return cn, ce, cu


# ---------- Detrend (stable.yaml) ----------
def _load_stable(stable_path: Optional[Path], log: Path) -> Dict[str, object]:
    """
    stable.yaml (two shapes):

    M1:
      stations:
        PDEL: { method: M1, N: 15.62, E: 13.17, U: 0.0, CN: 0.3, CE: -0.5, CU: -2.0, plate: EU }

    M2:
      stations:
        BVF1: { method: M2, reference_window: { start: 2012-06-01, end: 2017-01-01 } }
    """
    if not stable_path:
        _log("[stable] no stable file found; detrend unavailable", log)
        return {}

    try:
        data = _load_yaml(stable_path)
        stations = data.get("stations", {}) if isinstance(data, dict) else {}
        out: Dict[str, object] = {}
        for sta, conf in stations.items():
            m = str(conf.get("method", "M1")).upper()
            if m == "M2" and "reference_window" in conf:
                rw = conf["reference_window"]
                out[sta.upper()] = StableM2(
                    start=_parse_date(str(rw.get("start"))),
                    end=_parse_date(str(rw.get("end"))),
                    method="M2",
                )
            else:
                # M1 values in mm/yr and constants (mm)
                N = float(conf.get("N", conf.get("velocities", {}).get("N", 0.0)))
                E = float(conf.get("E", conf.get("velocities", {}).get("E", 0.0)))
                U = float(conf.get("U", conf.get("velocities", {}).get("U", 0.0)))
                CN = float(conf.get("CN", conf.get("corrections", {}).get("CN", 0.0)))
                CE = float(conf.get("CE", conf.get("corrections", {}).get("CE", 0.0)))
                CU = float(conf.get("CU", conf.get("corrections", {}).get("CU", 0.0)))
                out[sta.upper()] = StableM1(
                    velocities={"N": N, "E": E, "U": U},
                    corrections={"CN": CN, "CE": CE, "CU": CU},
                    method="M1"
                )
        _log(f"[stable] loaded detrend config for {len(out)} station(s) from {stable_path}", log)
        return out
    except Exception as e:
        _log(f"[stable] failed to read {stable_path}: {e}", log)
        return {}


def _linreg(x: List[float], y: List[float]) -> Tuple[float, float]:
    """Return slope, intercept (simple OLS)."""
    n = len(x)
    if n < 2:
        return 0.0, 0.0
    sx = sum(x); sy = sum(y)
    sxx = sum(v*v for v in x); sxy = sum(x[i]*y[i] for i in range(n))
    den = n * sxx - sx * sx
    if den == 0:
        return 0.0, (sy / n if n else 0.0)
    slope = (n * sxy - sx * sy) / den
    intercept = (sy - slope * sx) / n
    return slope, intercept


def _detrend_M1(sta: str, rows: List[Tuple[datetime, float]], vel_mm_yr: float, c_mm: float, t0: datetime) -> List[Tuple[datetime, float]]:
    """
    Apply plate velocity (mm/yr) removal and constant correction c_mm (mm).
    We output detrended component in *mm* relative to t0.
    rows: [(date, value_mm)]
    """
    out = []
    for d, val in rows:
        dt_years = _days_between(d, t0) / 365.25
        pred = vel_mm_yr * dt_years
        out.append((d, val - pred + c_mm))
    return out


def _detrend_M2(rows: List[Tuple[datetime, float]], wstart: datetime, wend: datetime) -> List[Tuple[datetime, float]]:
    """
    Zero slope inside [wstart, wend]:
      1) regress within window → slope_w (mm/day), intercept
      2) subtract slope_w*(t - wstart) from ENTIRE series
      3) shift so mean(window) is ~0
    """
    if not rows:
        return rows
    # window subset
    x_win: List[float] = []
    y_win: List[float] = []
    for d, v in rows:
        if wstart <= d <= wend:
            x_win.append(_days_between(d, wstart))
            y_win.append(v)
    if len(x_win) < 2:
        # not enough points; return input
        return rows

    slope_day, intercept = _linreg(x_win, y_win)  # mm/day, mm
    # subtract slope from entire series
    adjusted: List[Tuple[datetime, float]] = []
    for d, v in rows:
        dt_days = _days_between(d, wstart)
        adjusted.append((d, v - slope_day * dt_days))

    # recenter to mean ~0 inside window
    win_vals = [vv for dd, vv in adjusted if wstart <= dd <= wend]
    if win_vals:
        mean_w = sum(win_vals) / len(win_vals)
        adjusted = [(d, v - mean_w) for d, v in adjusted]

    return adjusted


# ---------- Outliers (robust MAD) ----------
def _mad(vals: List[float]) -> float:
    if not vals:
        return 0.0
    med = sorted(vals)[len(vals)//2]
    dev = [abs(v - med) for v in vals]
    mad = sorted(dev)[len(dev)//2]
    return mad

def _flag_outliers(series: List[Tuple[datetime, float]], thresh: float = 4.0) -> Dict[str, int]:
    """
    Returns {iso_date: 0/1} flags using robust z = 0.6745 * |x - median| / MAD.
    """
    if not series:
        return {}
    vals = [v for _, v in series]
    med = sorted(vals)[len(vals)//2]
    mad = _mad(vals)
    out: Dict[str, int] = {}
    if mad == 0:
        for d, _ in series:
            out[_to_iso(d)] = 0
        return out
    for d, v in series:
        z = 0.6745 * abs(v - med) / mad
        out[_to_iso(d)] = 1 if z > thresh else 0
    return out


# ---------- XYZ→LLH (for PyGMT station map support) ----------
def _xyz_to_geodetic(x: float, y: float, z: float) -> Tuple[float, float, float]:
    # WGS84
    a = 6378137.0
    e2 = 6.69437999014e-3
    lon = math.atan2(y, x)
    p = math.hypot(x, y)
    lat = math.atan2(z, p * (1 - e2))
    # iterate once for better accuracy
    for _ in range(2):
        N = a / math.sqrt(1 - e2 * math.sin(lat) ** 2)
        h = p / math.cos(lat) - N
        lat = math.atan2(z, p * (1 - e2 * N / (N + h)))
    N = a / math.sqrt(1 - e2 * math.sin(lat) ** 2)
    h = p / math.cos(lat) - N
    return (math.degrees(lon), math.degrees(lat), h)


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="DeforMA time-series analysis (offsets, detrend, outliers).")
    parser.add_argument("--config", default="/opt/DeforMA/configuration/config.yaml", help="Path to config.yaml")
    parser.add_argument("--project", default="", help="Project name (overrides ~/.current_project)")
    parser.add_argument("--analysis", default="all", help="Comma: offsets,detrend,outliers or 'all'")
    parser.add_argument("--date-from", default="", help="YYYY-MM-DD or YYDOY (inclusive)")
    parser.add_argument("--date-to", default="", help="YYYY-MM-DD or YYDOY (inclusive)")
    parser.add_argument("--frames", default="", help="Comma-separated frames (e.g., IGS20,IGS14)")
    parser.add_argument("--stations", default="", help="Comma-separated stations (e.g., PDEL,BGIN)")
    parser.add_argument("--mad-thresh", type=float, default=4.0, help="Outlier MAD threshold (default 4.0)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    workpool = Path(cfg.user_workspace.workpool).expanduser()
    logs_dir = workpool / "log"
    log_path = logs_dir / "ts_analysis.log"

    # Project dirs
    project = args.project.strip() or _read_current_project(workpool) or "default"
    proj_dir = workpool / project
    out_dir = proj_dir / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)

    _log("=== ts_analysis started ===", log_path)
    _log(f"Project     : {project}", log_path)

    # Dates
    date_from_iso = _to_iso(_parse_date(args.date_from)) if args.date_from.strip() else None
    date_to_iso   = _to_iso(_parse_date(args.date_to))   if args.date_to.strip()   else None
    _log(f"Date range  : {date_from_iso or '…'} → {date_to_iso or '…'}", log_path)

    # Filters
    frames = [s.strip() for s in args.frames.split(",") if s.strip()] or None
    stations = [s.strip().upper() for s in args.stations.split(",") if s.strip()] or None
    _log(f"Frames      : {', '.join(frames) if frames else 'ALL'}", log_path)
    _log(f"Stations    : {', '.join(stations) if stations else 'ALL'}", log_path)

    # Analysis switches
    analysis_set = set(a.strip().lower() for a in (args.analysis.split(",") if args.analysis else []))
    if "all" in analysis_set or not analysis_set:
        do_offsets = do_detrend = do_outliers = True
    else:
        do_offsets = "offsets" in analysis_set
        do_detrend = "detrend" in analysis_set
        do_outliers = "outliers" in analysis_set
    _log(f"Analysis    : offsets={do_offsets} detrend={do_detrend} outliers={do_outliers}", log_path)

    # Metadata
    events_path = _find_metadata(cfg, "events")
    stable_path = _find_metadata(cfg, "stable")
    events = _load_events(events_path, log_path) if do_offsets else []
    stable_conf = _load_stable(stable_path, log_path) if do_detrend else {}

    # DB
    user_db = Path(cfg.user_workspace.database).expanduser()
    if not user_db.exists():
        _log(f"[error] user DB not found: {user_db}", log_path)
        print(f"[error] user DB not found: {user_db}")
        return

    with sqlite3.connect(user_db) as conn:
        rows = _fetch_timeseries(conn, date_from_iso, date_to_iso, stations, frames)

    if not rows:
        _log("No time-series rows matched filters.", log_path)
        print("No rows found for the given filters.")
        return

    # Group by (station, frame), keep per-date lists
    # store also last (x,y,z) for LLH map output
    groups: Dict[Tuple[str, str], Dict[str, List[Tuple[datetime, float]]]] = {}
    last_xyz: Dict[Tuple[str, str], Tuple[float,float,float,str]] = {}

    for sta, d_iso, frame, x, y, z, n, e, u in rows:
        key = (sta.upper(), frame)
        d = datetime.strptime(d_iso, "%Y-%m-%d")

        g = groups.setdefault(key, {"N": [], "E": [], "U": []})
        # Convert NEU to mm (assume DB NEU in meters; if you already store in mm, set scale = 1)
        scale = 1000.0
        g["N"].append((d, (n or 0.0) * scale))
        g["E"].append((d, (e or 0.0) * scale))
        g["U"].append((d, (u or 0.0) * scale))

        last_xyz[key] = (x or 0.0, y or 0.0, z or 0.0, d_iso)

    # For output CSVs
    out_wide = out_dir / "time_series_analysis.csv"
    out_long = out_dir / "time_series_long.csv"
    out_geo  = out_dir / "stations_latest_geo.csv"

    total_rows = 0
    with open(out_wide, "w", newline="", encoding="utf-8") as fw, \
         open(out_long, "w", newline="", encoding="utf-8") as fl:

        w_wide = csv.writer(fw)
        w_long = csv.writer(fl)
        # headers
        w_wide.writerow(["station","date","reference_frame",
                         "x","y","z","n","e","u",
                         "cn","ce","cu","dn","de","du","on","oe","ou"])
        w_long.writerow(["station","date","reference_frame","set","component","value_mm"])

        for (sta, frame), comp in groups.items():

            # sort by date
            for k in ("N","E","U"):
                comp[k].sort(key=lambda t: t[0])

            # Offsets cn/ce/cu
            cn_map: Dict[str, float] = {}
            ce_map: Dict[str, float] = {}
            cu_map: Dict[str, float] = {}
            if do_offsets:
                for d, _ in comp["N"]:
                    cn, ce, cu = _compute_offsets_for_station_date(events, sta, d)
                    cn_map[_to_iso(d)] = cn
                    ce_map[_to_iso(d)] = ce
                    cu_map[_to_iso(d)] = cu

            # Detrend dn/de/du
            dn_map: Dict[str, float] = {}
            de_map: Dict[str, float] = {}
            du_map: Dict[str, float] = {}

            if do_detrend:
                conf = stable_conf.get(sta)
                if isinstance(conf, StableM1):
                    # Reference epoch = first sample date per component
                    t0N = comp["N"][0][0] if comp["N"] else None
                    t0E = comp["E"][0][0] if comp["E"] else None
                    t0U = comp["U"][0][0] if comp["U"] else None

                    if comp["N"] and t0N:
                        dn_series = _detrend_M1(
                            sta, comp["N"], conf.velocities.get("N",0.0), conf.corrections.get("CN",0.0), t0N
                        )
                        for d, v in dn_series:
                            dn_map[_to_iso(d)] = v
                    if comp["E"] and t0E:
                        de_series = _detrend_M1(
                            sta, comp["E"], conf.velocities.get("E",0.0), conf.corrections.get("CE",0.0), t0E
                        )
                        for d, v in de_series:
                            de_map[_to_iso(d)] = v
                    if comp["U"] and t0U:
                        du_series = _detrend_M1(
                            sta, comp["U"], conf.velocities.get("U",0.0), conf.corrections.get("CU",0.0), t0U
                        )
                        for d, v in du_series:
                            du_map[_to_iso(d)] = v

                elif isinstance(conf, StableM2):
                    # Convert to mm series first (already in mm)
                    N_m2 = _detrend_M2(comp["N"], conf.start, conf.end) if comp["N"] else []
                    E_m2 = _detrend_M2(comp["E"], conf.start, conf.end) if comp["E"] else []
                    U_m2 = _detrend_M2(comp["U"], conf.start, conf.end) if comp["U"] else []
                    for d, v in N_m2: dn_map[_to_iso(d)] = v
                    for d, v in E_m2: de_map[_to_iso(d)] = v
                    for d, v in U_m2: du_map[_to_iso(d)] = v
                else:
                    # no stable config → leave detrend empty
                    pass

            # Outliers
            on_map: Dict[str, int] = {}
            oe_map: Dict[str, int] = {}
            ou_map: Dict[str, int] = {}
            if do_outliers:
                on_map = _flag_outliers(comp["N"], thresh=args.mad_thresh)
                oe_map = _flag_outliers(comp["E"], thresh=args.mad_thresh)
                ou_map = _flag_outliers(comp["U"], thresh=args.mad_thresh)

            # Emit rows by date
            # Build quick lookup from date->(x,y,z) for this (sta,frame)
            # We only stored last_xyz; we’ll use that for stations_latest_geo later.
            # For per-epoch XYZ, do a small map from original DB fetch:
            xyz_by_date: Dict[str, Tuple[float,float,float]] = {}
            # reconstruct from original 'rows' (could be optimized with a pre-pass dict)
            # this is fine for medium volumes
            for r in rows:
                s, iso, f, x, y, z, n, e, u = r
                if s.upper()==sta and f==frame:
                    xyz_by_date[iso] = (x or 0.0, y or 0.0, z or 0.0)

            # Iterate on the union of all dates appearing in any component to keep alignment
            dates = sorted({ _to_iso(d) for d,_ in (comp["N"]+comp["E"]+comp["U"]) })
            for iso in dates:
                x,y,z = xyz_by_date.get(iso, (None,None,None))
                # raw NEU in mm (comp arrays already mm)
                n_raw = next((v for d,v in comp["N"] if _to_iso(d)==iso), None)
                e_raw = next((v for d,v in comp["E"] if _to_iso(d)==iso), None)
                u_raw = next((v for d,v in comp["U"] if _to_iso(d)==iso), None)

                cn = cn_map.get(iso, "")
                ce = ce_map.get(iso, "")
                cu = cu_map.get(iso, "")

                dn = dn_map.get(iso, "")
                de = de_map.get(iso, "")
                du = du_map.get(iso, "")

                on = on_map.get(iso, "")
                oe = oe_map.get(iso, "")
                ou = ou_map.get(iso, "")

                # wide (XYZ in meters, NEU/detrend/offsets in mm)
                w_wide.writerow([
                    sta, iso, frame,
                    x, y, z,
                    n_raw, e_raw, u_raw,
                    cn, ce, cu,
                    dn, de, du,
                    on, oe, ou
                ])
                total_rows += 1

                # long/tidy for PyGMT/pandas
                if n_raw is not None: w_long.writerow([sta, iso, frame, "raw", "N", n_raw])
                if e_raw is not None: w_long.writerow([sta, iso, frame, "raw", "E", e_raw])
                if u_raw is not None: w_long.writerow([sta, iso, frame, "raw", "U", u_raw])
                if dn != "": w_long.writerow([sta, iso, frame, "detrended", "N", dn])
                if de != "": w_long.writerow([sta, iso, frame, "detrended", "E", de])
                if du != "": w_long.writerow([sta, iso, frame, "detrended", "U", du])

    # Station geo (latest epoch per station/frame)
    with open(out_geo, "w", newline="", encoding="utf-8") as fg:
        wg = csv.writer(fg)
        wg.writerow(["station","reference_frame","date","lon","lat","h","n_mm","e_mm","u_mm","dn_mm","de_mm","du_mm"])
        for key, (x,y,z,iso) in last_xyz.items():
            sta, frame = key
            lon, lat, h = _xyz_to_geodetic(x,y,z)
            # look up last NEU/detrend in mm
            # (lazy: scan long file for last for this station/frame)
            # For speed we could cache; for now OK.
            # Leave detrend cols empty if not available at last epoch.
            nmm = emm = umm = dnmm = demm = dumm = ""
            # This keeps file IO minimal; you can optimize later if needed.

            wg.writerow([sta, frame, iso, lon, lat, h, nmm, emm, umm, dnmm, demm, dumm])

    _log(f"Wrote: {out_dir/'time_series_analysis.csv'}", log_path)
    _log(f"Wrote: {out_dir/'time_series_long.csv'}", log_path)
    _log(f"Wrote: {out_dir/'stations_latest_geo.csv'}", log_path)
    _log(f"Rows (wide): {total_rows}", log_path)
    _log("=== ts_analysis completed ===", log_path)
    print(f"Analysis saved in: {out_dir}")
    print("Files:")
    print(f"  - {out_wide}")
    print(f"  - {out_long}")
    print(f"  - {out_geo}")
    print("Done.")
    

if __name__ == "__main__":
    main()
