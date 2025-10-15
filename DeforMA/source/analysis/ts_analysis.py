#!/usr/bin/env python3

import argparse, csv, sqlite3, sys
from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# --- path bootstrap so "common.load_config" imports work even when run from anywhere ---
_THIS = Path(__file__).resolve()
_SRC = _THIS.parent.parent           
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
# --------------------------------------------------------------------------------------


try:
    from common.load_config import load_config
except Exception as e:
    print(f"[fatal] cannot import common.load_config.load_config: {e}")
    raise

# ---------- utils ----------
def _yy_doy_to_date(s: str) -> str:
    s = s.strip()
    if len(s) == 10 and s[4] == '-' and s[7] == '-':
        return s
    if len(s) != 7 or not s.isdigit():
        raise ValueError(f"Invalid date token (expected YYYYDOY): {s}")
    yyyy = int(s[:4]); doy = int(s[4:])
    d0 = datetime(yyyy, 1, 1) + timedelta(days=doy-1)
    return d0.strftime("%Y-%m-%d")

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _read_current_project(workpool: Path) -> Optional[str]:
    marker = workpool / ".current_project"
    try:
        if marker.exists():
            t = marker.read_text(encoding="utf-8").strip()
            return t or None
    except Exception:
        pass
    return None

# ---------- logging ----------
def _log(msg: str, log_path: Path) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg.rstrip()}\n")

# ---------- YAML ----------
def _load_yaml(path: Path):
    import yaml
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

@dataclass
class StableEntry:
    method: str
    N: float = 0.0; E: float = 0.0; U: float = 0.0
    CN: float = 0.0; CE: float = 0.0; CU: float = 0.0
    ref_start: Optional[str] = None
    ref_end: Optional[str] = None

def load_stable(stable_yaml: Path, log_path: Path) -> Dict[str, StableEntry]:
    data = _load_yaml(stable_yaml) or {}
    out: Dict[str, StableEntry] = {}
    stations = (data.get("stations") or {}) if "stations" in data else data
    for sta, cfg in stations.items():
        m = str(cfg.get("method","M1")).upper()
        staU = sta.upper()
        if m == "M2":
            rw = cfg.get("reference_window", {})
            out[staU] = StableEntry("M2", ref_start=(str(rw.get("start")) if rw.get("start") else None),
                                          ref_end=(str(rw.get("end")) if rw.get("end") else None))
        else:
            out[staU] = StableEntry(
                "M1",
                N=float(cfg.get("N", cfg.get("velocities", {}).get("N", 0.0))),
                E=float(cfg.get("E", cfg.get("velocities", {}).get("E", 0.0))),
                U=float(cfg.get("U", cfg.get("velocities", {}).get("U", 0.0))),
                CN=float(cfg.get("CN", cfg.get("corrections", {}).get("CN", 0.0))),
                CE=float(cfg.get("CE", cfg.get("corrections", {}).get("CE", 0.0))),
                CU=float(cfg.get("CU", cfg.get("corrections", {}).get("CU", 0.0))),
            )
    _log(f"Loaded stable.yaml for {len(out)} stations", log_path)
    return out

@dataclass
class EventItem:
    flag: str; date: str; N: float; E: float; U: float

def load_events(events_yaml: Path, log_path: Path) -> Dict[str, List[EventItem]]:
    data = _load_yaml(events_yaml) or {}
    evs: Dict[str, List[EventItem]] = defaultdict(list)
    raw = data.get("events", data if isinstance(data, list) else [])
    for it in raw:
        sta = (it.get("station") or "").upper()
        if not sta: continue
        flag = (it.get("flag") or it.get("FLAG") or "").upper()
        date = str(it.get("date") or it.get("DATE") or "").split()[0]
        N = float(it.get("N_OFFSET", it.get("N", it.get("offsets", {}).get("N", 0.0))))
        E = float(it.get("E_OFFSET", it.get("E", it.get("offsets", {}).get("E", 0.0))))
        U = float(it.get("U_OFFSET", it.get("U", it.get("offsets", {}).get("U", 0.0))))
        if not date: continue
        evs.setdefault(sta, []).append(EventItem(flag, date, N, E, U))
    for sta in evs:
        evs[sta].sort(key=lambda e: e.date)
    _log(f"Loaded events.yaml for {len(evs)} stations", log_path)
    return evs

def load_station_groups(stations_yaml: Path, log_path: Path) -> Tuple[set, set]:
    data = _load_yaml(stations_yaml) or {}
    civisa = set(s.upper() for s in (data.get("civisa") or []))
    repraa = set(s.upper() for s in (data.get("repraa") or data.get("repraa") or []))
    if not repraa and "repraa" in data:
        repraa = set(s.upper() for s in data.get("repraa") or [])
    _log(f"Station groups: CIVISA={len(civisa)} REPRAA={len(repraa)}", log_path)
    return civisa, repraa

# ---------- DB ----------
def fetch_timeseries(conn: sqlite3.Connection,
                     frames: Optional[List[str]],
                     stations: Optional[List[str]],
                     dmin: Optional[str],
                     dmax: Optional[str]) -> List[Tuple]:
    sql = ["SELECT station, date, reference_frame, x, y, z, n, e, u FROM time_series WHERE 1=1"]
    params: List = []
    if frames:
        sql.append("AND reference_frame IN (%s)" % ",".join(["?"]*len(frames)))
        params.extend(frames)
    if stations:
        sql.append("AND station IN (%s)" % ",".join(["?"]*len(stations)))
        params.extend(stations)
    if dmin:
        sql.append("AND date >= ?"); params.append(dmin)
    if dmax:
        sql.append("AND date <= ?"); params.append(dmax)
    sql.append("ORDER BY station, reference_frame, date")
    cur = conn.cursor()
    cur.execute(" ".join(sql), params)
    return list(cur.fetchall())

# ---------- analysis ----------
def apply_offsets(rows_by_key, events_map, include_D: bool, log_path: Path):
    allowed = {'E','R'} | ({'D'} if include_D else set())
    out = {}
    for (sta, frame), rows in rows_by_key.items():
        evs = [e for e in events_map.get(sta, []) if e.flag in allowed]
        cumN = cumE = cumU = 0.0; idx = 0
        for _, date, _, *_ in rows:
            while idx < len(evs) and evs[idx].date <= date:
                cumN += evs[idx].N / 1000.0
                cumE += evs[idx].E / 1000.0
                cumU += evs[idx].U / 1000.0
                idx += 1
            out[(sta, frame, date)] = (cumN, cumE, cumU)
    _log("Applied offsets", log_path)
    return out

def detrend_M1(rows, stab: StableEntry):
    if not rows: return {}
    t0 = datetime.strptime(rows[0][1], "%Y-%m-%d")
    vN, vE, vU = stab.N/1000.0, stab.E/1000.0, stab.U/1000.0
    cN, cE, cU = stab.CN/1000.0, stab.CE/1000.0, stab.CU/1000.0
    out = {}
    for _, d, _, _, _, _, n, e, u in rows:
        if n is None or e is None or u is None: out[d]=(None,None,None); continue
        years = (datetime.strptime(d,"%Y-%m-%d")-t0).days/365.25
        out[d] = (n-(vN*years+cN), e-(vE*years+cE), u-(vU*years+cU))
    return out

def detrend_M2(rows, ref_start: Optional[str], ref_end: Optional[str]):
    if not rows: return {}
    T,N,E,U,D=[],[],[],[],[]
    t0 = datetime.strptime(rows[0][1], "%Y-%m-%d")
    for _, d, _, _, _, _, n, e, u in rows:
        if n is None or e is None or u is None: continue
        T.append((datetime.strptime(d,"%Y-%m-%d")-t0).days); N.append(float(n)); E.append(float(e)); U.append(float(u)); D.append(d)
    if len(T)<3: return {d:(None,None,None) for d in (r[1] for r in rows)}
    if ref_start and ref_end:
        ds, de = datetime.strptime(ref_start,"%Y-%m-%d"), datetime.strptime(ref_end,"%Y-%m-%d")
        idxs = [i for i, dd in enumerate(D) if ds <= datetime.strptime(dd,"%Y-%m-%d") <= de]
    else:
        idxs = list(range(len(T)))
    if len(idxs)<2: return {d:(None,None,None) for d in D}
    def slope(x,y,idxs):
        n=len(idxs); sx=sum(x[i] for i in idxs); sy=sum(y[i] for i in idxs)
        sxx=sum(x[i]*x[i] for i in idxs); sxy=sum(x[i]*y[i] for i in idxs)
        den=(n*sxx-sx*sx); return 0.0 if den==0 else (n*sxy-sx*sy)/den
    sN,sE,sU = slope(T,N,idxs), slope(T,E,idxs), slope(T,U,idxs)
    t_ref = sum(T[i] for i in idxs)/len(idxs)
    out={}
    for t,n,e,u,d in zip(T,N,E,U,D):
        out[d]=(n-sN*(t-t_ref), e-sE*(t-t_ref), u-sU*(t-t_ref))
    return out

def detect_outliers_mad(series: List[Tuple[str, Optional[float]]], thr=3.5) -> Dict[str,int]:
    vals=[v for _,v in series if v is not None]
    if len(vals)<5: return {d:0 for d,_ in series}
    vals_sorted=sorted(vals); med=vals_sorted[len(vals_sorted)//2]
    absdev=sorted(abs(v-med) for v in vals); mad=absdev[len(absdev)//2] or 1e-12
    out={}
    for d,v in series:
        if v is None: out[d]=0
        else:
            score=0.6745*(v-med)/mad
            out[d]=1 if abs(score)>thr else 0
    return out

# ---------- GMT ----------
def write_gmt_vectors(out_path: Path, snapshot: Optional[str],
                      rows_by_sta_adj: Dict[str, List[Tuple]],
                      station_ll: Dict[str, Dict[str,float]], log_path: Path)->int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    count=0
    with open(out_path,"w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["lon","lat","vE","vN","station","date"])
        for sta, rows in rows_by_sta_adj.items():
            if sta not in station_ll or not rows: continue
            pick=None
            if snapshot:
                for r in reversed(rows):
                    if r[1] <= snapshot: pick=r; break
            pick = pick or rows[-1]
            _, date, _, _, _, _, n_adj, e_adj, _ = pick
            if n_adj is None or e_adj is None: continue
            w.writerow([station_ll[sta]["lon"], station_ll[sta]["lat"], e_adj, n_adj, sta, date]); count+=1
    _log(f"Wrote GMT vectors: {out_path} ({count} rows)", log_path)
    return count

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="DeforMA time-series analysis for CIVISA + REPRAA (offsets, detrend, outliers, PyGMT).")
    parser.add_argument("--config", default="/opt/DeforMA/configuration/config.yaml")
    parser.add_argument("--project", default="", help="If omitted, use ~/DeforMA/workpool/.current_project or 'default'")
    parser.add_argument("--stations", default="", help="Comma-separated stations (will be intersected with CIVISA*REPRAA)")
    parser.add_argument("--frames", default="", help="Comma-separated frames (e.g., IGS20,IGS14)")
    parser.add_argument("--date", nargs=2, metavar=("START_YYYYDOY","END_YYYYDOY"))
    parser.add_argument("--analysis", default="all", help="offsets,detrend,outliers or all")
    parser.add_argument("--apply-deformation", action="store_true", help="Include 'D' events in offsets")
    parser.add_argument("--gmt-snapshot", default="", help="YYYY-MM-DD for PyGMT vectors (default: latest)")
    args = parser.parse_args()

    cfg = load_config(args.config)

    # Project auto-detect
    workpool = Path(cfg.user_workspace.workpool).expanduser()
    project = args.project.strip() or _read_current_project(workpool) or "default"
    project_dir = workpool / project
    outputs_dir = project_dir / "outputs"
    logs_dir = project_dir / "log"
    _ensure_dir(outputs_dir); _ensure_dir(logs_dir)
    log_path = logs_dir / "ts_analysis.log"

    # Log header
    user_db = Path(cfg.user_workspace.database).expanduser()
    _log("=== ts_analysis started ===", log_path)
    _log(f"Project: {project}", log_path)
    _log(f"User DB: {user_db}", log_path)

    # Metadata
    metadata_dir = Path(cfg.user_workspace.root).expanduser() / "metadata"
    events_yaml = metadata_dir / "events.yaml"
    stable_yaml = metadata_dir / "stable.yaml"
    stations_yaml = metadata_dir / "stations.yaml"

    civisa, repraa = load_station_groups(stations_yaml, log_path)
    allowed = civisa | repraa
    if not allowed:
        _log("No CIVISA/REPRAA stations in stations.yaml; exit.", log_path)
        print("No CIVISA/REPRAA stations configured."); return

    user_filter_stas = [s.strip().upper() for s in args.stations.split(",") if s.strip()] or None
    if user_filter_stas:
        stations = sorted(set(user_filter_stas) & allowed)
    else:
        stations = sorted(allowed)

    frames = [s.strip() for s in args.frames.split(",") if s.strip()] or None
    dmin = dmax = None
    if args.date:
        dmin = _yy_doy_to_date(args.date[0]); dmax = _yy_doy_to_date(args.date[1])
        if dmax < dmin: dmin, dmax = dmax, dmin
    snapshot = args.gmt_snapshot.strip() or None
    analyses = set(a.strip().lower() for a in args.analysis.split(",") if a.strip()) or {"all"}

    stable_map = load_stable(stable_yaml, log_path) if stable_yaml.exists() else {}
    events_map = load_events(events_yaml, log_path) if events_yaml.exists() else {}

    if not user_db.exists():
        _log(f"[error] user DB not found: {user_db}", log_path)
        print(f"[error] user DB not found: {user_db}"); return

    with sqlite3.connect(user_db) as conn:
        rows = fetch_timeseries(conn, frames, stations, dmin, dmax)

    if not rows:
        _log("No rows after filters (stations/frames/dates).", log_path)
        print("No rows found."); return

    rows_by_key: Dict[Tuple[str,str], List[Tuple]] = defaultdict(list)
    for r in rows:
        rows_by_key[(r[0], r[2])].append(r)
    for k in rows_by_key:
        rows_by_key[k].sort(key=lambda r: r[1])

    # Analyses
    cncecu = {}
    dndedu = {}
    onoeou = {}

    if "all" in analyses or "offsets" in analyses:
        cncecu = apply_offsets(rows_by_key, events_map, args.apply_deformation, log_path)

    if "all" in analyses or "detrend" in analyses:
        for (sta, frame), srows in rows_by_key.items():
            stab = stable_map.get(sta)
            if not stab:
                for _, d, _, *_ in srows:
                    dndedu[(sta, frame, d)] = (None, None, None)
                continue
            m = detrend_M2(srows, stab.ref_start, stab.ref_end) if stab.method=="M2" else detrend_M1(srows, stab)
            for d, vals in m.items():
                dndedu[(sta, frame, d)] = vals
        _log("Detrending complete", log_path)

    if "all" in analyses or "outliers" in analyses:
        def _mad(series):
            vals = [v for _, v in series if v is not None]
            if len(vals) < 5: return {d:0 for d,_ in series}
            vs = sorted(vals); med = vs[len(vs)//2]
            ad = sorted(abs(v-med) for v in vals); mad = ad[len(ad)//2] or 1e-12
            res={}
            for d,v in series:
                if v is None: res[d]=0
                else:
                    s=0.6745*(v-med)/mad
                    res[d]=1 if abs(s)>3.5 else 0
            return res
        for (sta, frame), srows in rows_by_key.items():
            Ns=[]; Es=[]; Us=[]
            for r in srows:
                d=r[1]
                dn,de,du=dndedu.get((sta,frame,d),(None,None,None))
                n = dn if dn is not None else r[6]
                e = de if de is not None else r[7]
                u = du if du is not None else r[8]
                Ns.append((d,n)); Es.append((d,e)); Us.append((d,u))
            on = _mad(Ns); oe = _mad(Es); ou = _mad(Us)
            for r in srows:
                d=r[1]; onoeou[(sta,frame,d)] = (on.get(d,0), oe.get(d,0), ou.get(d,0))
        _log("Outlier detection complete (MAD)", log_path)

    # Write analysis CSV
    out_csv = outputs_dir / "time_series_analysis.csv"
    total=0
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w=csv.writer(f)
        w.writerow(["station","date","reference_frame","x","y","z","n","e","u",
                    "cn","ce","cu","dn","de","du","on","oe","ou"])
        for (sta, frame), srows in rows_by_key.items():
            for r in srows:
                station, date, frame, x, y, z, n, e, u = r
                cn,ce,cu = cncecu.get((sta,frame,date),(None,None,None))
                dn,de,du = dndedu.get((sta,frame,date),(None,None,None))
                on,oe,ou = onoeou.get((sta,frame,date),(None,None,None))
                w.writerow([
                    station,date,frame,x,y,z,n,e,u,
                    "" if cn is None else cn, "" if ce is None else ce, "" if cu is None else cu,
                    "" if dn is None else dn, "" if de is None else de, "" if du is None else du,
                    "" if on is None else on, "" if oe is None else oe, "" if ou is None else ou
                ])
                total+=1
    _log(f"Wrote analysis CSV: {out_csv} ({total} rows)", log_path)

    # PyGMT vectors
    station_ll={}
    meta_yaml = _load_yaml(Path(cfg.user_workspace.root).expanduser() / "metadata" / "stations.yaml") or {}
    for sta in stations:
        meta=(meta_yaml.get("stations", {}) or {}).get(sta) or meta_yaml.get(sta) or {}
        if isinstance(meta, dict) and "lon" in meta and "lat" in meta:
            station_ll[sta]={"lon":float(meta["lon"]), "lat":float(meta["lat"])}

    rows_by_sta_adj: Dict[str,List[Tuple]] = defaultdict(list)
    for (sta, frame), srows in rows_by_key.items():
        for r in srows:
            station, date, frame, x, y, z, n, e, u = r
            cn, ce, _ = cncecu.get((sta,frame,date),(0.0,0.0,0.0))
            dn_, de_, _ = dndedu.get((sta,frame,date),(None,None,None))
            n_adj = dn_ if dn_ is not None else (None if n is None else n - cn)
            e_adj = de_ if de_ is not None else (None if e is None else e - ce)
            rows_by_sta_adj[sta].append((station,date,frame,x,y,z,n_adj,e_adj,u))

    gmt_csv = outputs_dir / "gmt_vectors.csv"
    nvec = write_gmt_vectors(gmt_csv, snapshot=args.gmt_snapshot.strip() or None,
                             rows_by_sta_adj=rows_by_sta_adj, station_ll=station_ll, log_path=log_path)

    _log("=== ts_analysis completed ===", log_path)
    print(f"Project           : {project}")
    print(f"User DB           : {user_db}")
    print(f"Analysis rows     : {total}")
    print(f"GMT vectors rows  : {nvec}")
    print(f"Outputs           : {outputs_dir}")
    print(f"Log               : {log_path}")

if __name__ == "__main__":
    main()

