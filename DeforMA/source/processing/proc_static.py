#!/usr/bin/env python3
"""
DeforMA Static Processing (Web + CLI)

This script runs GNSS static processing in sequential modules.
It supports both interactive use and non-interactive (for the web app).

Modules (order):
  PREPARATION - Prepare folders, read config, load stations, TEQC flags, etc.
  IGS - Download IGS RINEX files
  PERMANENT - Download local permanent GNSS stations RINEX files
  TEQC - Run TEQC quality checks and filtering
  COMPRESS - Compress RINEX files
  SURVEY - Download local survey RINEX files (if style=survey)
  CODE - Download CODE RINEX files
  VMF - Download VMF files
  UPLOAD - Upload data to Bernese folders
  UPDATE - Update Bernese configuration files from ftp server
  BERNESE - Run Bernese processing
  FINALIZATION - Store results, clean up, etc.

Each module writes a log file and status markers:
  === MODULE START ===
  === MODULE DONE ===
  === MODULE ERROR === 
"""

import os
import sys
import argparse
import yaml
import json
import glob
import subprocess
import shutil
from contextlib import suppress
from datetime import datetime
from datetime import  timedelta
from typing import Tuple
from typing import Union

# ============================================================
# Logging helpers
# ============================================================

# Define the global log file
log_sum = "0000_summary.log"   # one summary file per run/logs dir

def log_message(msg: str, logfile: str):
    os.makedirs(os.path.dirname(logfile), exist_ok=True)
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")

def _summary_path(logfile: str) -> str:
    """Return the path to the run-level summary log that lives next to this logfile."""
    logs_dir = os.path.dirname(os.path.abspath(logfile))
    return os.path.join(logs_dir, log_sum)

def mark_start(name: str, logfile: str) -> None:
    """Mark a module/submodule as started in its own log and in the summary log."""
    message = f"=== {name.upper()} START"
    log_message(message, logfile)
    log_message(message, _summary_path(logfile))

def mark_done(name: str, logfile: str) -> None:
    """Mark a module/submodule as done in its own log and in the summary log."""
    message = f"=== {name.upper()} DONE"
    log_message(message, logfile)
    log_message(message, _summary_path(logfile))

def mark_error(name: str, logfile: str, err: Union[Exception, str]) -> None:
    """Mark a module/submodule as errored in its own log and in the summary log."""
    msg = str(err)
    message = f"=== {name.upper()} ERROR {msg}"
    log_message(message, logfile)
    log_message(message, _summary_path(logfile))

def write_summary_header(user, mode, style, period, survey, logs_dir):
    hdr = []
    hdr.append(f'=== User: "{user}"')
    hdr.append(f'=== Proc: "{mode}"')
    if style:
        hdr.append(f'=== Style: "{style}"')
    if period:
        hdr.append(f'=== Period: "{period}"')
    if style == "survey" and survey:
        hdr.append(f'=== Survey: "{survey}"')
    hdr.append(f'=== Logs: "{logs_dir}"')
    log_message("\n".join(hdr) + "\n", os.path.join(logs_dir, log_sum))

from datetime import datetime, timedelta

# ============================================================
# Config loader
# ============================================================

def load_config():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_main = os.path.abspath(os.path.join(script_dir, "..", ".."))
    cfg_path = os.path.join(default_main, "configuration", "config.yaml")
    if not os.path.isfile(cfg_path):
        raise FileNotFoundError(f"config.yaml not found at {cfg_path}")

    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    # 1) Resolve main first (so we can derive configuration_dir)
    main = os.path.abspath(os.path.expandvars(cfg.get("main", default_main)))

    # helper that only does ${main} first-pass
    def expand_defor(val: str) -> str:
        return os.path.expandvars((val or "").replace("${main}", main))

    # 2) Now resolve configuration_dir using first-pass expansion
    configuration_dir = os.path.abspath(expand_defor(cfg.get("configuration", "${main}/configuration")))

    # final helper that knows both ${main} and ${configuration}
    def expand(val: str) -> str:
        return os.path.expandvars(
            (val or "")
            .replace("${main}", main)
            .replace("${configuration}",       configuration_dir)
        )

    # Paths
    paths = {
        "main": main,
        "configuration":  configuration_dir,
        "output":  os.path.abspath(expand(cfg.get("output",  "${main}/output"))),
        "external":  os.path.abspath(expand(cfg.get("external",  "${main}/external"))),
        "source":  os.path.abspath(expand(cfg.get("source",  "${main}/source"))),
        "logs": os.path.abspath(expand(cfg.get("logs", "${main}/logs"))),
    }

    # Externals
    externals = {
        "bernese":     expand(cfg.get("bernese", "")),
        "permanent":   expand(cfg.get("permanent", "")),
        "survey":      expand(cfg.get("survey", "")),
        "spider":      expand(cfg.get("spider", "")),
        "monitoring":  expand(cfg.get("monitoring", "")),
        "webservices": expand(cfg.get("webservices", "")),
        "datapool":    expand(cfg.get("datapool", "")),
        "campaign":    expand(cfg.get("campaign", "")),
        "bern54":      expand(cfg.get("bern54", "")),
    }

    # FTP and options
    # Support IGS either as list or comma-separated string in YAML
    igs_val = cfg.get("IGS", [])
    if isinstance(igs_val, str):
        igs_list = [x.strip() for x in igs_val.split(",") if x.strip()]
    elif isinstance(igs_val, list):
        igs_list = igs_val
    else:
        igs_list = []
    ftp = {
        "IGS": igs_list,
        "CODE": expand(cfg.get("CODE", "")),
    }
    teqc_opts = cfg.get("teqc_opts", "")
    rinex_cmp = cfg.get("rinex_compression", "")

    # Station files (now ${configuration} works)
    stations_file       = os.path.abspath(expand(cfg.get("stations_file",        "${configuration}/stations.yaml")))
    stations_teqc_file  = os.path.abspath(expand(cfg.get("stations_teqc_file",   "${configuration}/stations_teqc.yaml")))

    return paths, externals, ftp, teqc_opts, rinex_cmp, stations_file, stations_teqc_file

# ============================================================
# Module skipping
# ============================================================

MODULE_KEYS = [
    "preparation","igs","permanent","teqc","compress",
    "survey","code","vmf","upload","update","bernese","finalization",
]

def _parse_list_arg(val: str):
    if not val:
        return []
    return [x.strip().lower() for x in val.split(",") if x.strip()]

def get_module_selection_from_args(args):
    skip = set(_parse_list_arg(args.skip))
    only = set(_parse_list_arg(args.only))
    bad = (skip | only) - set(MODULE_KEYS)
    if bad:
        print(f"Warning: unknown module keys ignored: {', '.join(sorted(bad))}")
    if only:
        return {k: (k not in only) for k in MODULE_KEYS}   # True == skip
    return {k: (k in skip) for k in MODULE_KEYS}           # True == skip


# ============================================================
# Module helpers
# ============================================================

def run_shell(cmd, log_path, cwd=None, env=None):
    """
    Run a shell command, stream stdout+stderr into log_path, return exit code.

    Args:
        cmd (str or list): The command to run.
        log_path (str): File to append logs to.
        cwd (str): Working directory to run the command from.
        env (dict): Environment variables to use.

    Returns:
        int: return code of the process
    """
    if isinstance(cmd, str):
        shell = True
    else:
        shell = False

    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"$ {cmd if isinstance(cmd, str) else ' '.join(cmd)}\n")
        proc = subprocess.Popen(
            cmd,
            shell=shell,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        for line in proc.stdout:
            f.write(line)
        proc.wait()
        return proc.returncode

# Copy file, overwriting if exists
def copy_file(source: str, dst: str, log_path: str = None):
    try:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(source, dst)  # preserves metadata
        if log_path:
            with open(log_path, "a") as f:
                f.write(f"[copy_file] Copied {source} → {dst}\n")
        return True
    except Exception as e:
        if log_path:
            with open(log_path, "a") as f:
                f.write(f"[copy_file] Failed {source} → {dst}: {e}\n")
        return False

# Move/rename file
def move_file(source: str, dst: str, log_path: str = None):
    try:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.move(source, dst)
        if log_path:
            with open(log_path, "a") as f:
                f.write(f"[move_file] Moved {source} → {dst}\n")
        return True
    except Exception as e:
        if log_path:
            with open(log_path, "a") as f:
                f.write(f"[move_file] Failed {source} → {dst}: {e}\n")
        return False

# Remove a file quietly
def remove_file(path: str, log_path: str = None):
    try:
        if os.path.exists(path):
            os.remove(path)
            if log_path:
                with open(log_path, "a") as f:
                    f.write(f"[remove_file] Deleted {path}\n")
    except Exception as e:
        if log_path:
            with open(log_path, "a") as f:
                f.write(f"[remove_file] Failed to delete {path}: {e}\n")

def load_station_lists(stations_file: str, log_path: str = None):
    """
    Read stations_file (YAML) and return three lists:
      igs_stations, civisa_stations, repraa_stations
    """
    try:
        with open(stations_file, "r") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        if log_path:
            with open(log_path, "a") as f:
                f.write(f"[load_station_lists] Failed to read {stations_file}: {e}\n")
        return [], [], []

    igs    = [s.upper() for s in data.get("igs", [])]
    civisa = [s.upper() for s in data.get("civisa", [])]
    repraa = [s.upper() for s in data.get("repraa", [])]

    if log_path:
        with open(log_path, "a") as f:
            f.write(f"[load_station_lists] Loaded {len(igs)} IGS, "
                    f"{len(civisa)} CIVISA, {len(reprae_list) if (reprae_list:=repraa) else 0} REPRAA stations\n")

    return igs, civisa, repraa

def load_teqc_config(stations_teqc_file: str, log_path: str = None):
    """
    Read stations_teqc_file (YAML) and return a dict:
      station_code -> { flag: value, ... }
    Example:
      {
        "ACOR": { "-O.mo": "Azores", "-O.ag": "IVAR" },
        "PDEL": { "-O.mo": "Azores", "-O.ag": "REPRAA" }
      }
    """
    try:
        with open(stations_teqc_file, "r") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        if log_path:
            with open(log_path, "a") as f:
                f.write(f"[load_teqc_config] Failed to read {stations_teqc_file}: {e}\n")
        return {}

    if log_path:
        with open(log_path, "a") as f:
            f.write(f"[load_teqc_config] Loaded TEQC config for {len(data)} stations\n")

    return data

def _parse_station_list(val: str):
    if not val:
        return set()
    return {s.strip().upper() for s in val.split(",") if s.strip()}

def _remove_excluded_from_repos(dp_root: str,
                                campaign54_root: str,
                                yy: str,
                                doy: str,
                                excluded: set,
                                logs_dir: str):
    """
    Remove any RINEX files for the excluded stations from:
      - DATAPOOL/RINEX
      - CAMPAIGN54/FINAL/RAW

    We match typical daily filenames like STAA<DOY>0.<YY>[oOdD][.Z]
    and both cases, plus common compressions (.Z, .gz).
    """
    if not excluded:
        return

    log_path = os.path.join(logs_dir, "0200_exclude_cleanup.log")
    with open(log_path, "a") as lg:
        lg.write(f"=== EXCLUDE CLEANUP start (YY={yy}, DOY={doy}) ===\n")

    candidates = []

    # 1) DATAPOOL/RINEX
    dp_rinex = os.path.join(dp_root, "RINEX")
    if os.path.isdir(dp_rinex):
        candidates.append(dp_rinex)

    # 2) CAMPAIGN54/FINAL/RAW
    camp_raw = os.path.join(campaign54_root, "FINAL", "RAW")
    if os.path.isdir(camp_raw):
        candidates.append(camp_raw)

    # Build all patterns to search
    # Examples to catch:
    #   ABCD1600.22o, ABCD1600.22O, ABCD1600.22d, ABCD1600.22D
    # with optional .Z/.gz
    suffixes = [
        f".{yy}o", f".{yy}O",
        f".{yy}d", f".{yy}D",
        f".{yy}o.Z", f".{yy}O.Z",
        f".{yy}d.Z", f".{yy}D.Z",
        f".{yy}o.gz", f".{yy}O.gz",
        f".{yy}d.gz", f".{yy}D.gz",
    ]

    removed = 0
    for base in candidates:
        try:
            for sta in excluded:
                stem = f"{sta}{doy}0"
                for suf in suffixes:
                    pat = os.path.join(base, stem + suf)
                    for path in glob.glob(pat):
                        try:
                            os.remove(path)
                            removed += 1
                            with open(log_path, "a") as lg:
                                lg.write(f"removed: {path}\n")
                        except Exception as e:
                            with open(log_path, "a") as lg:
                                lg.write(f"failed remove: {path} -> {e}\n")
        except Exception as e:
            with open(log_path, "a") as lg:
                lg.write(f"scan failed in {base}: {e}\n")

    with open(log_path, "a") as lg:
        lg.write(f"=== EXCLUDE CLEANUP done; files removed: {removed} ===\n")

def _find_monitoring_file(base_dir: str, sta: str, yy: str, doy: str) -> str | None:
    """
    Look for common CIVISA/REPRAA monitoring patterns under base_dir.
    Examples (case-insensitive):
      STA<DOY>0.YYd.Z / .GZ
      STA<DOY>0.YYo.Z / .Z  (some archives keep observation as .o)
      STA<DOY>.YYd.Z  (rare alt)
    Returns first matching absolute path or None.
    """
    staU = sta.upper()
    patterns = [
        f"{staU}{doy}0.{yy}[dD].[zZ]",
        f"{staU}{doy}0.{yy}[dD].[gG][zZ]",
        f"{staU}{doy}0.{yy}[oO].[zZ]",
        f"{staU}{doy}0.{yy}[oO].[gG][zZ]",
        f"{staU}{doy}.{yy}[dD].[zZ]",
        f"{staU}{doy}.{yy}[dD].[gG][zZ]",
    ]
    for pat in patterns:
        hits = glob.glob(os.path.join(base_dir, pat))
        if hits:
            return hits[0]
    return None

def _gps_week_day_from_yy_doy(yy: str, doy: str) -> tuple[str, str, datetime]:
    """
    Build a date from 2-digit year YY and day-of-year DOY, then return (GPSweek, GPSday, date_obj).
    """
    year = 2000 + int(yy)  # CODE years are 20YY
    date_obj = datetime(year, 1, 1) + timedelta(days=int(doy) - 1)
    gps_epoch = datetime(1980, 1, 6)
    delta = date_obj - gps_epoch
    week = delta.days // 7
    day  = delta.days % 7
    return f"{week:04d}", f"{day}", date_obj

# ---------- period helpers ----------
def _parse_yy_doy(yy_doy: str) -> datetime:
    if len(yy_doy) != 5 or not yy_doy.isdigit():
        raise ValueError(f"Invalid YYDOY: {yy_doy}")
    yy = int(yy_doy[:2])
    doy = int(yy_doy[2:])
    year = 2000 + yy  # adjust if you need 1900s
    return datetime(year, 1, 1) + timedelta(days=doy - 1)

def _expand_period(start_yy_doy: str, end_yy_doy: str):
    d0 = _parse_yy_doy(start_yy_doy)
    d1 = _parse_yy_doy(end_yy_doy)
    if d1 < d0:
        d0, d1 = d1, d0
    out = []
    cur = d0
    while cur <= d1:
        out.append(cur)
        cur += timedelta(days=1)
    return out

# ============================================================
# Module implementations
# ============================================================

# ------------------
# Preparation module
# ------------------

def process_preparation(logs_dir: str):
    """
    PREPARATION module
    - Discover main and read configuration/config.yaml
    - Expand paths (configuration,external,source,output,logs) and externals
    - Ensure PATH includes <external>
    - Create run subfolders (observations/, orbits/, vmf/, logs/)
    - Load station lists (igs, civisa, repraa) and TEQC flags
    - Capture skip/only (from run_meta.json if present)
    - Write a detailed log summary
    """

    name = "PREPARATION"
    prep_log = os.path.join(logs_dir, "0101_preparation.log")
    mark_start(name, prep_log)

    try:
        # ---------- derive run_dir & user ----------
        run_dir = os.path.abspath(os.path.join(logs_dir, ".."))
        user    = os.path.basename(os.path.dirname(run_dir))  # <output>/<user>/<timestamp>/logs
        ts_dir  = os.path.basename(run_dir)

        # ---------- locate main & config.yaml ----------
        # assuming this script lives in <main>/source/processing/static_proc.py
        script_dir      = os.path.dirname(os.path.abspath(__file__))
        default_defor   = os.path.abspath(os.path.join(script_dir, "..", ".."))
        cfg_path        = os.path.join(default_defor, "configuration", "config.yaml")
        if not os.path.isfile(cfg_path):
            raise FileNotFoundError(f"config.yaml not found at {cfg_path}")

        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg_raw = yaml.safe_load(f) or {}

        # resolve main (allow override in YAML)
        main = os.path.abspath(os.path.expandvars(cfg_raw.get("main", default_defor)))

        def _expand(val: str) -> str:
            return os.path.expandvars((val or "").replace("${main}", main))

        # ---------- paths ----------
        paths = {}
        for key in ("configuration", "external", "source", "output", "logs"):
            # default fallback if missing
            default_v = {
                "configuration":  "${main}/configuration",
                "external":  "${main}/external",
                "source":  "${main}/source",
                "output":  "${main}/output",
                "logs": "${main}/logs",
            }[key]
            raw = _expand(cfg_raw.get(key, default_v))
            paths[key] = raw

        # ---------- externals (best-effort; optional keys) ----------
        ext_keys = ("bernese","permanent","survey","spider","webservices",
                    "datapool","campaign","bern54")
        externals = {k: _expand(cfg_raw.get(k, "")) for k in ext_keys}

        # ---------- other options ----------
        ftp   = {
            "IGS":  cfg_raw.get("IGS", []),   # may be a list in your YAML
            "CODE": cfg_raw.get("CODE", "")
        }
        teqc_opts       = cfg_raw.get("teqc_opts", "")
        rinex_cmp       = cfg_raw.get("rinex_compression", "")
        stations_file   = _expand(cfg_raw.get("stations_file", "${main}/configuration/stations.yaml"))
        stations_teqc   = _expand(cfg_raw.get("stations_teqc_file", "${main}/configuration/stations_teqc.yaml"))

        # ---------- ensure PATH includes <external> ----------
        os.environ["PATH"] = paths["external"] + os.pathsep + os.environ.get("PATH", "")

        # ---------- ensure run subfolders ----------
        obs_dir   = os.path.join(run_dir, "observations")
        orbits_dir= os.path.join(run_dir, "orbits")
        vmf_dir   = os.path.join(run_dir, "vmf")
        for d in (obs_dir, vmf_dir, logs_dir):
            os.makedirs(d, exist_ok=True)

        # ---------- load station lists ----------
        igs_list, civisa_list, repraa_list = [], [], []
        if os.path.isfile(stations_file):
            try:
                with open(stations_file, "r", encoding="utf-8") as f:
                    sdata = yaml.safe_load(f) or {}
                igs_list    = [s.upper() for s in (sdata.get("igs") or [])]
                civisa_list = [s.upper() for s in (sdata.get("civisa") or [])]
                repraa_list = [s.upper() for s in (sdata.get("repraa") or [])]
            except Exception as e:
                log_message(f"[warn] failed to read stations_file: {e}", prep_log)
        else:
            log_message(f"[warn] stations_file not found: {stations_file}", prep_log)

        # ---------- load TEQC flags ----------
        teqc_map = {}
        if os.path.isfile(stations_teqc):
            try:
                with open(stations_teqc, "r", encoding="utf-8") as f:
                    teqc_map = yaml.safe_load(f) or {}
            except Exception as e:
                log_message(f"[warn] failed to read stations_teqc_file: {e}", prep_log)
        else:
            log_message(f"[warn] stations_teqc_file not found: {stations_teqc}", prep_log)

        # ---------- optional meta from launcher (user/mode/style/period/survey/skip/only) ----------
        meta_path = os.path.join(logs_dir, "run_meta.json")
        meta = {}
        if os.path.isfile(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f) or {}
            except Exception as e:
                log_message(f"[warn] failed to read run_meta.json: {e}", prep_log)

        mode         = meta.get("mode", "manual")
        style        = meta.get("style", "permanent")
        period       = meta.get("period", "")
        survey       = meta.get("survey", "")
        only_modules = meta.get("only", "")
        skip_modules = meta.get("skip", "")

        # ---------- write a readable header ----------
        header_lines = [
            f'=== User: "{meta.get("user", user)}"',
            f'=== Proc: "{mode.capitalize()}"',
            f'=== Style: "{style}"',
            f'=== Period: "{period}"' if period else "",
            f'=== Survey: "{survey}"' if (style == "survey" and survey) else "",
            f'=== Logs: "{logs_dir}"',
            ""
        ]
        header = "\n".join([ln for ln in header_lines if ln])
        log_message(header, prep_log)

        # ---------- summarize config & inputs ----------
        log_message("--- Config & Paths ---", prep_log)
        log_message(f"main: {main}", prep_log)
        for k in ("configuration","external","source","output","logs"):
            log_message(f"{k}: {paths[k]}", prep_log)

        log_message("\n--- Externals ---", prep_log)
        for k, v in externals.items():
            if v:
                log_message(f"{k}: {v}", prep_log)

        log_message("\n--- FTP / Options ---", prep_log)
        log_message(f"IGS endpoints: {ftp['IGS']}", prep_log)
        log_message(f"CODE base: {ftp['CODE']}", prep_log)
        log_message(f"teqc_opts: {teqc_opts}", prep_log)
        log_message(f"rinex_compression: {rinex_cmp}", prep_log)

        log_message("\n--- Stations ---", prep_log)
        log_message(f"stations_file: {stations_file}", prep_log)
        log_message(f"  IGS:    {len(igs_list)}", prep_log)
        log_message(f"  CIVISA: {len(civisa_list)}", prep_log)
        log_message(f"  REPRAA: {len(reprae_list) if (reprae_list:=repraa_list) else 0}", prep_log)
        log_message(f"stations_teqc_file: {stations_teqc}", prep_log)
        log_message(f"  TEQC entries: {len(teqc_map)}", prep_log)

        # ---------- module selection summary ----------
        log_message("\n--- Module selection ---", prep_log)
        if only_modules:
            log_message(f"ONLY: {only_modules}", prep_log)
        elif skip_modules:
            log_message(f"SKIP: {skip_modules}", prep_log)
        else:
            log_message("All modules enabled (default).", prep_log)

        # ---------- confirm folders ----------
        log_message("\n--- Run folders ---", prep_log)
        log_message(f"run_dir:  {run_dir}", prep_log)
        log_message(f"obs_dir:  {obs_dir}", prep_log)
        log_message(f"orbits:   {orbits_dir}", prep_log)
        log_message(f"vmf:     {vmf_dir}", prep_log)
        log_message(f"logs_dir: {logs_dir}", prep_log)

        mark_done(name, prep_log)

    except Exception as e:
        mark_error(name, prep_log, e)
        raise

# ------------------
# IGS module
# ------------------

def process_igs(obs_base, ftp_list, stations, yy, doy, logs_dir):
    """
    IGS module (with procedure-style logging and summaries)

    Inputs:
      obs_base   : absolute path to <run_dir>/observations
      ftp_list   : list of IGS base URLs (e.g., ["ftp://…", "https://…"])
      stations   : list of 4-char station codes to try
      yy, doy    : strings (e.g., "22", "160")
      logs_dir   : absolute path to <run_dir>/logs

    Side effects:
      - Writes detailed log to  logs_dir/0201_igs.log
      - Writes wget output to   logs_dir/0202_igs_wget.log
      - Writes pass list to     logs_dir/0203_igs_pass.log
      - Writes fail list to     logs_dir/0204_igs_fail.log
      - Appends a readable summary
      - Produces files in obs_base; runs gunzip and CRX2RNX on success
    """

    step_name   = "IGS"
    igs_log     = os.path.join(logs_dir, "0201_igs.log")
    igs_wgetlog = os.path.join(logs_dir, "0202_igs_wget.log")
    pass_file   = os.path.join(logs_dir, "0203_igs_pass.log")
    fail_file   = os.path.join(logs_dir, "0204_igs_fail.log")

    mark_start(step_name, igs_log)

    try:
        os.makedirs(obs_base, exist_ok=True)
        original_cwd = os.getcwd()
        os.chdir(obs_base)

        log_message(f"--- IGS start {obs_base} ---", igs_log)
        log_message(f"DOY={doy}  YY={yy}", igs_log)
        log_message(f"FTP endpoints: {ftp_list}", igs_log)

        passed, failed = [], []

        for sta in stations:
            sta = sta.upper().strip()
            if not sta:
                continue

            downloaded = False
            # ---------- 1) NEW FORMAT .crx.gz pattern ----------
            raw_pattern = f"{sta}00???_R_20{yy}{doy}0000_01D_30S_MO.crx.gz"
            target_name = f"{sta}{doy}0.{yy}d.gz"   # convert to old target name

            for ftp in ftp_list:
                if downloaded:
                    break
                url = f"{ftp.rstrip('/')}/20{yy}/{doy}/{raw_pattern}"
                log_message(f"{sta}: trying new-format at {url}", igs_log)
                run_shell(f'wget -r -nd -np --timeout=15 "{url}"', igs_wgetlog)

                matches = glob.glob(raw_pattern)
                if matches:
                    source = matches[0]
                    # stage into classic name (upper)
                    with suppress(Exception):
                        # keep a copy, then remove raw pieces
                        shutil.copy(source, target_name)
                    os.rename(target_name, target_name.upper())
                    for f in glob.glob(raw_pattern):
                        with suppress(Exception):
                            os.remove(f)
                    downloaded = True
                    log_message(f"{sta}: new-format success (→ {target_name.upper()})", igs_log)
                    break

            if downloaded:
                passed.append(sta)
                continue

            # ---------- 2) OLD FORMAT <sta><doy>0.<yy>d.gz ----------
            lower_pat = f"{sta.lower()}{doy}0.{yy}d.gz"
            upper_pat = lower_pat.upper()
            for ftp in ftp_list:
                if downloaded:
                    break
                url = f"{ftp.rstrip('/')}/20{yy}/{doy}/{lower_pat}"
                log_message(f"{sta}: trying old-format d.gz at {url}", igs_log)
                run_shell(f'wget -r -nd -np --timeout=15 "{url}"', igs_wgetlog)

                matches = glob.glob(lower_pat) + glob.glob(upper_pat)
                if matches:
                    source = matches[0]
                    # ensure upper-case classic name
                    os.replace(source, upper_pat)
                    downloaded = True
                    log_message(f"{sta}: old-format d.gz success (→ {upper_pat})", igs_log)
                    break

            if downloaded:
                passed.append(sta)
            else:
                failed.append(sta)
                log_message(f"{sta}: failed both new/d.gz formats", igs_log)

        # ---------- 2) OLD FORMAT <sta><doy>0.<yy>d.Z ----------
            lower_pat = f"{sta.lower()}{doy}0.{yy}d.Z"
            upper_pat = lower_pat.upper()
            for ftp in ftp_list:
                if downloaded:
                    break
                url = f"{ftp.rstrip('/')}/20{yy}/{doy}/{lower_pat}"
                log_message(f"{sta}: trying old-format d.Z at {url}", igs_log)
                run_shell(f'wget -r -nd -np --timeout=15 "{url}"', igs_wgetlog)

                matches = glob.glob(lower_pat) + glob.glob(upper_pat)
                if matches:
                    source = matches[0]
                    # ensure upper-case classic name
                    os.replace(source, upper_pat)
                    downloaded = True
                    log_message(f"{sta}: old-format d.Z success (→ {upper_pat})", igs_log)
                    break

            if downloaded:
                passed.append(sta)
            else:
                failed.append(sta)
                log_message(f"{sta}: failed both new/d.gz/d.Z formats", igs_log)

        # ---------- write pass/fail lists ----------
        with open(pass_file, "w") as f:
            f.write(" ".join(passed))
        with open(fail_file, "w") as f:
            f.write(" ".join(failed))
        # ---------- gunzip + CRX2RNX ----------
        #  .GZ → .(upper/lower); then CRX2RNX on the .D file
        for gz in glob.glob("*.GZ") + glob.glob("*.gz"):
            log_message(f"gunzip -f {gz}", igs_log)
            run_shell(f'gunzip -f "{gz}"', igs_wgetlog)

        for sta in passed:
            rnx = f"{sta}{doy}0.{yy}D"   # upper-case
            if os.path.exists(rnx):
                log_message(f'CRX2RNX "{rnx}"', igs_log)
                run_shell(f'CRX2RNX "{rnx}"', igs_wgetlog)
            else:
                # In some mirrors files may expand as lower-case; try that too.
                rnx_lo = f"{sta.lower()}{doy}0.{yy}d"
                if os.path.exists(rnx_lo):
                    log_message(f'CRX2RNX "{rnx_lo}"', igs_log)
                    run_shell(f'CRX2RNX "{rnx_lo}"', igs_wgetlog)
                else:
                    log_message(f"[warn] RNX not found for {sta} (expected {rnx} or {rnx_lo})", igs_log)

        os.chdir(original_cwd)
        log_message(f"--- IGS complete; passed: {len(passed)}, failed: {len(failed)} ---", igs_log)
        
    except Exception as e:
        # Log error in module log and in summary, then bubble up
        err_line = f"----- IGS -----\nStatus: Error! Check log at: {igs_log}\n\n"
        mark_error(step_name, igs_log, e)
        raise

# ------------------
# Permanent module
# ------------------

def process_permanent(
    obs_base: str,
    yy: str,
    doy: str,
    civisa_stations: list,
    repraa_stations: list,
    spider_dir: str,
    monitoring_dir: str,   # from config.yaml (externals["monitoring"])
    logs_dir: str,
    teqc_opts: str         # kept for signature compatibility; not used here
):
    """
    Download/copy CIVISA (IVAR) and REPRAA RINEX for given YY/DOY.
    - Prefer spider; if not found, fallback to monitoring tree.
    - Files are copied AS-IS (no uncompress/teqc/CRX2RNX).

    Logs produced:
      0301_civisa.log         (overall status)
      0302_civisa_pass.log    (CIVISA ok)
      0303_civisa_fail.log    (CIVISA fail)
      0304_repraa_pass.log    (REPRAA ok)
      0305_repraa_fail.log    (REPRAA fail)
    """
    permanent_log = os.path.join(logs_dir, "0301_civisa.log")
    os.makedirs(obs_base, exist_ok=True)

    try:
        mark_start("Permanent GNSS / Download", permanent_log)

        # ---------- CIVISA (IVAR) ----------
        civ_pass, civ_fail = [], []
        with open(os.path.join(logs_dir, "0302_civisa_pass.log"), "w") as pass_f, \
             open(os.path.join(logs_dir, "0303_civisa_fail.log"), "w") as fail_f:

            for sta in civisa_stations:
                staU = sta.upper()

                # 1) Try spider: IVAR/20YY/RAW/STA<DOY>0.YYo.Z
                source_spider = os.path.join(
                    spider_dir, "IVAR", f"20{yy}", "RAW", f"{staU}{doy}0.{yy}o.Z"
                )

                copied = False
                if os.path.exists(source_spider):
                    shutil.copy(source_spider, os.path.join(obs_base, os.path.basename(source_spider)))
                    copied = True
                else:
                    # 2) Fallback to monitoring: .../Observation/IVAR/20YY/
                    mon_base = os.path.join(monitoring_dir, "20102_Data", "Observation", "IVAR", f"20{yy}")
                    source_mon = _find_monitoring_file(mon_base, staU, yy, doy)
                    if source_mon and os.path.exists(source_mon):
                        shutil.copy(source_mon, os.path.join(obs_base, os.path.basename(source_mon)))
                        copied = True

                if copied:
                    pass_f.write(f"{staU} ")
                    civ_pass.append(staU)
                else:
                    fail_f.write(f"{staU} ")
                    civ_fail.append(staU)

        # ---------- REPRAA ----------
        rep_pass, rep_fail = [], []
        with open(os.path.join(logs_dir, "0304_repraa_pass.log"), "w") as pass_f, \
             open(os.path.join(logs_dir, "0305_repraa_fail.log"), "w") as fail_f:

            for sta in repraa_stations:
                staU = sta.upper()

                # 1) Try spider: REPRAA/20YY/RAW/STA<DOY>0.YYo.Z
                source_spider = os.path.join(
                    spider_dir, "REPRAA", f"20{yy}", "RAW", f"{staU}{doy}0.{yy}o.Z"
                )

                copied = False
                if os.path.exists(source_spider):
                    shutil.copy(source_spider, os.path.join(obs_base, os.path.basename(source_spider)))
                    copied = True
                else:
                    # 2) Fallback: monitoring .../Observation/REPRAA/20YY/
                    mon_base = os.path.join(monitoring_dir, "20102_Data", "Observation", "REPRAA", f"20{yy}")
                    source_mon = _find_monitoring_file(mon_base, staU, yy, doy)
                    if source_mon and os.path.exists(source_mon):
                        shutil.copy(source_mon, os.path.join(obs_base, os.path.basename(source_mon)))
                        copied = True

                if copied:
                    pass_f.write(f"{staU} ")
                    rep_pass.append(staU)
                else:
                    fail_f.write(f"{staU} ")
                    rep_fail.append(staU)

        mark_done("Permanent GNSS / Download", permanent_log)

    except Exception as e:
        mark_error("Permanent GNSS / Download", permanent_log, e)
        raise


# ------------------
# TEQC module
# ------------------

def process_teqc(obs_base, yy, doy, stations_teqc_file, logs_dir):
    """
    Run TEQC header edits ONLY for stations listed in stations_teqc_file (YAML).

    YAML example (stations_teqc.yaml):
      AZTP:
        "-O.r":  "AutoProc"
        "-O.ag": "SRCTE"
        "-O.o":  "DSCIG"
        "-O.mo": "AZTP"
        "-O.rt": "LEICA GRX1200GGPRO"
        "-O.at": "LEIAT504GG      LEIS"
      PDEL:
        "-O.r": "AutoProc"
        ...

    Args:
      obs_base (str): folder with RINEX obs files
      yy (str):  2-digit year (e.g. "22")
      doy (str): 3-digit DOY  (e.g. "160")
      stations_teqc_file (str): path to YAML above
      logs_dir (str): run logs folder
    """
    teqc_log   = os.path.join(logs_dir, "0401_teqc.log")
    pass_list  = os.path.join(logs_dir, "0402_teqc_pass.log")
    fail_list  = os.path.join(logs_dir, "0403_teqc_fail.log")

    mark_start("CIVISA / TEQC", teqc_log)
    log_message(f"=== TEQC started in {obs_base} ===", teqc_log)
    log_message(f"Using stations_teqc_file: {stations_teqc_file}", teqc_log)

    # Load the YAML (station -> flags dict)
    try:
        with open(stations_teqc_file, "r") as f:
            teqc_map = yaml.safe_load(f) or {}
        if not isinstance(teqc_map, dict):
            raise ValueError("stations_teqc_file did not parse to a dict")
    except Exception as e:
        mark_error("CIVISA / TEQC", teqc_log, f"Failed to read YAML: {e}")
        # still create empty lists to avoid frontend confusion
        open(pass_list, "w").close()
        open(fail_list, "w").close()
        raise

    cwd = os.getcwd()
    try:
        os.makedirs(obs_base, exist_ok=True)
        os.chdir(obs_base)

        # gather uncompressed *.{yy}o files
        o_files = sorted(set(
            glob.glob(os.path.join(obs_base, f"*.{yy}o")) +
            glob.glob(os.path.join(obs_base, f"*.{yy}O"))
        ))
        # map lowercase basename -> full path for fast lookup
        name_map = {os.path.basename(p).lower(): p for p in o_files}

        if not o_files:
            z_any = (glob.glob(os.path.join(obs_base, f"*.{yy}o.Z")) +
                     glob.glob(os.path.join(obs_base, f"*.{yy}O.Z")))
            if z_any:
                log_message(
                    f"No uncompressed *.{yy}o files; compressed *.o.Z exist. "
                    f"Run uncompress before TEQC.", teqc_log
                )
            else:
                log_message(f"No *.{yy}o files found; nothing to do.", teqc_log)
            open(pass_list, "w").close()
            open(fail_list, "w").close()
            mark_done("CIVISA / TEQC", teqc_log)
            return

        ok_stations, bad_stations = [], []
        teqc_exe = os.environ.get("TEQC", "teqc")
        log_message(f"Expecting basenames like <STATION>{doy}0.{yy}o (e.g. aztp{doy}0.{yy}o)", teqc_log)

        # Run only for stations present in YAML
        for station, flags in teqc_map.items():
            code = str(station).upper()
            expected_base = f"{code.lower()}{doy}0.{yy}o"
            actual = name_map.get(expected_base)
            if not actual:
                log_message(f"{code}: {expected_base} not found (skipping)", teqc_log)
                continue

            # Build command
            parts = [teqc_exe]
            if isinstance(flags, dict):
                for flag, val in flags.items():
                    parts.append(f'{flag} "{val}"')
            parts.append(os.path.basename(actual))
            cmd = " ".join(parts) + " > temp.o"

            log_message(f"{code}: $ {cmd}", teqc_log)
            res = subprocess.run(cmd, shell=True, text=True, capture_output=True, cwd=obs_base)
            if res.stdout:
                log_message(res.stdout, teqc_log)
            if res.stderr:
                log_message(res.stderr, teqc_log)

            temp_out = os.path.join(obs_base, "temp.o")
            if res.returncode != 0 or not os.path.exists(temp_out):
                log_message(f"{code}: TEQC failed (rc={res.returncode}); keeping original.", teqc_log)
                bad_stations.append(code)
                if os.path.exists(temp_out):
                    try: os.remove(temp_out)
                    except: pass
                continue

            # Replace original
            try:
                os.replace(temp_out, actual)
                ok_stations.append(code)
                log_message(f"{code}: header updated in {os.path.basename(actual)}", teqc_log)
            except Exception as e:
                bad_stations.append(code)
                log_message(f"{code}: replace failed: {e}", teqc_log)
                try:
                    if os.path.exists(temp_out): os.remove(temp_out)
                except:
                    pass

        # Write pass/fail lists
        with open(pass_list, "w") as pf:
            pf.write(" ".join(ok_stations))
        with open(fail_list, "w") as ff:
            ff.write(" ".join(bad_stations))

        log_message(f"TEQC OK: {len(ok_stations)} | TEQC FAIL: {len(bad_stations)}", teqc_log)
        mark_done("CIVISA / TEQC", teqc_log)

    except Exception as e:
        mark_error("CIVISA / TEQC", teqc_log, e)
        raise
    finally:
        os.chdir(cwd)

# ------------------
# Compress module
# ------------------

def process_compress(obs_base: str, yy: str, logs_dir: str) -> None:
    """
    Compress RINEX:
      1) *.YYo / *.YYO  -> RNX2CRX -f  (produces *.YYd)
      2) *.YYd / *.YYD  -> compress -f (produces *.YYd.Z)

    Creates in logs_dir:
      - 0500_compress.log          (full trace)
      - 0501_compress_pass.log     (final .Z files created)
      - 0502_compress_fail.log     (files that failed any step)
    """
    comp_log = os.path.join(logs_dir, "0500_compress.log")
    pass_log = os.path.join(logs_dir, "0501_compress_pass.log")
    fail_log = os.path.join(logs_dir, "0502_compress_fail.log")

    mark_start("COMPRESSION", comp_log)

    made = []   # final .Z paths we produced
    failed = [] # source files that failed

    try:
        # -------- Step 1: Hatanaka compression (*.o -> *.d) --------
        o_files = []
        o_files += glob.glob(os.path.join(obs_base, f"*.{yy}o"))
        o_files += glob.glob(os.path.join(obs_base, f"*.{yy}O"))

        if not o_files:
            log_message(f"[compress] no RINEX .{yy}o files found in {obs_base}", comp_log)
        else:
            for source in sorted(o_files):
                # If a corresponding *.d already exists, skip RNX2CRX
                base, ext = os.path.splitext(source)  # ext should be .YYo or .YYO
                d_lower = f"{base}.{yy}d"
                d_upper = f"{base}.{yy}D"
                if os.path.exists(d_lower) or os.path.exists(d_upper):
                    log_message(f"[compress] skip RNX2CRX (already have .{yy}d): {source}", comp_log)
                    continue

                cmd = f'RNX2CRX -f "{source}"'
                rc = run_shell(cmd, comp_log, cwd=obs_base)
                if rc != 0:
                    failed.append(source)
                    log_message(f"[compress] RNX2CRX FAILED: {source}", comp_log)

        # -------- Step 2: UNIX compress (*.d -> *.d.Z) --------
        d_files = []
        d_files += glob.glob(os.path.join(obs_base, f"*.{yy}d"))
        d_files += glob.glob(os.path.join(obs_base, f"*.{yy}D"))

        if not d_files:
            log_message(f"[compress] no RINEX .{yy}d files found in {obs_base}", comp_log)
        else:
            for d_path in sorted(d_files):
                z_path = d_path + ".Z"
                if os.path.exists(z_path):
                    log_message(f"[compress] already compressed: {os.path.basename(z_path)}", comp_log)
                    made.append(z_path)
                    continue

                cmd = f'compress -f "{d_path}"'  # produces <d_path>.Z
                rc = run_shell(cmd, comp_log, cwd=obs_base)
                if rc == 0 and os.path.exists(z_path):
                    made.append(z_path)
                    log_message(f"[compress] OK: {os.path.basename(z_path)}", comp_log)
                else:
                    failed.append(d_path)
                    log_message(f"[compress] compress FAILED: {d_path}", comp_log)

        # -------- Write pass/fail lists --------
        with open(pass_log, "w", encoding="utf-8") as f:
            for p in made:
                f.write(os.path.basename(p) + "\n")

        with open(fail_log, "w", encoding="utf-8") as f:
            for p in failed:
                f.write(os.path.basename(p) + "\n")

        mark_done("COMPRESSION", comp_log)

    except Exception as e:
        mark_error("COMPRESSION", comp_log, e)
        # also reflect in fail log
        with open(fail_log, "a", encoding="utf-8") as f:
            f.write(f"<< exception: {e} >>\n")
        raise

# ------------------
# Survey module
# ------------------

def process_survey(
    survey_base: str,
    survey_code: str,
    obs_base: str,
    logs_dir: str
):
    """
    Copy pre-cleaned RINEX from survey campaign into obs_base.

    Expected directory pattern (robust via glob):
      <survey_base>/*_Data/*_<STATION>_<YYYY>/RINEX_cleaned/*

    Example:
      code = "SMIG22" -> STATION="SMIG", YYYY="2022"
      matches like: <survey_base>/20202_Data/20202_SMIG_2022/RINEX_cleaned/*

    Logs:
      - logs_dir/0601_survey.log (activity + summary)
      - logs_dir/survey_files.log  (one filename per line that was copied)
    """
    survey_log = os.path.join(logs_dir, "0601_survey.log")
    os.makedirs(obs_base, exist_ok=True)

    def _write_line(path, txt):
        with open(path, "a", encoding="utf-8") as f:
            f.write(txt.rstrip() + "\n")

    try:
        mark_start("SURVEY", survey_log)

        if not survey_code or len(survey_code) < 3:
            raise ValueError(f"Invalid survey code: {survey_code!r}")

        station = survey_code[:-2].upper()
        yy_two  = survey_code[-2:]
        yyyy    = f"20{yy_two}"

        # Find RINEX_cleaned folders in a prefix-agnostic way:
        #   */*_Data/*_{STATION}_{YYYY}/RINEX_cleaned
        pattern = os.path.join(
            survey_base, "*_Data", f"*_{station}_{yyyy}", "RINEX_cleaned"
        )
        candidates = sorted(glob.glob(pattern))

        if not candidates:
            # Try a looser fallback: any *_{station}_YYYY under any *_Data
            alt_pattern = os.path.join(
                survey_base, "*_Data", f"*_{station}_{yyyy}", "*"
            )
            alt_hits = [p for p in glob.glob(alt_pattern) if os.path.basename(p).lower() == "rinex_cleaned"]
            candidates = sorted(alt_hits)

        if not candidates:
            _write_line(survey_log, f"[survey] No RINEX_cleaned found for code={survey_code} (station={station}, year={yyyy})")
            mark_done("SURVEY", survey_log)
            # Still create an empty list file for UI consistency
            open(os.path.join(logs_dir, "0602_survey_files.log"), "w").close()
            return

        rinex_dir = candidates[0]
        _write_line(survey_log, f"[survey] Using source: {rinex_dir}")

        copied = []
        errors = []

        for name in sorted(os.listdir(rinex_dir)):
            source = os.path.join(rinex_dir, name)
            if not os.path.isfile(source):
                continue  # skip subdirs
            dst = os.path.join(obs_base, name)
            try:
                shutil.copy2(source, dst)
                copied.append(name)
                _write_line(survey_log, f"Copied: {name}")
            except Exception as e:
                errors.append((name, str(e)))
                _write_line(survey_log, f"ERROR copying {name}: {e}")

        # Write the flat list of copied filenames for the frontend
        with open(os.path.join(logs_dir, "survey_files.log"), "w", encoding="utf-8") as f:
            for n in copied:
                f.write(n + "\n")

        # Summary
        summary = [
            f"SURVEY code: {survey_code}",
            f"Station: {station}",
            f"Year: {yyyy}",
            f"Source: {rinex_dir}",
            f"Copied files: {len(copied)}",
        ]
        if errors:
            summary.append(f"Errors: {len(errors)}")
        mark_done("SURVEY", survey_log)

    except Exception as e:
        mark_error("SURVEY", survey_log, e)
        raise

# ------------------
# CODE module
# ------------------

def process_code(
    orb_base: str,
    yy: str,
    doy: str,
    ftp_code: str,
    logs_dir: str
):
    """
    Download old-format CODE products + monthly DCBs directly (no recursive wget).
    Saves into orb_base. Writes:
      - logs_dir/0501_code.log      (activity log)
      - logs_dir/code_pass.log      (names that downloaded ok)
      - logs_dir/code_fail.log      (names that failed)
    """
    code_log = os.path.join(logs_dir, "0701_code.log")
    os.makedirs(orb_base, exist_ok=True)

    def fetch_exact(url: str, dst: str) -> bool:
        # no -r/-A; just download exactly this file
        run_shell(f'wget -nv --timeout=30 --tries=2 -O "{dst}" "{url}"', code_log)
        try:
            return os.path.exists(dst) and os.path.getsize(dst) > 0
        except Exception:
            return False

    try:
        mark_start("CODE", code_log)

        # Build date, GPS week/day, and year path
        week, day, date_obj = _gps_week_day_from_yy_doy(yy, doy)
        year_str = f"20{yy}"
        YY = yy                  # 2-digit year for DCB names
        MM = date_obj.strftime("%m")

        base_year_url = f"{ftp_code.rstrip('/')}/{year_str}"

        # Old-format targets for this day + weekly ERP + monthly DCBs
        targets = [
            f"COD{week}{day}.BIA.Z",
            f"COD{week}{day}.CLK.Z",
            f"COD{week}{day}.EPH.Z",
            f"COD{week}{day}.ION.Z",
            f"COD{week}{day}.OBX.Z",
            f"COD{week}{day}.SNX.Z",
            f"COD{week}{day}.TRO.Z",
            f"COD{week}7.ERP.Z",              # weekly ERP (day 7)
            f"P1C1{YY}{MM}.DCB.Z",
            f"P1P2{YY}{MM}.DCB.Z",
        ]

        ok, fail = [], []

        # Attempt each file directly under /CODE/YYYY/
        for name in targets:
            url = f"{base_year_url}/{name}"
            dst = os.path.join(orb_base, name)
            # Log the intent (nice to see in 0501_code.log)
            run_shell(f'echo "Trying {url}"', code_log)
            if fetch_exact(url, dst):
                ok.append(name)
            else:
                fail.append(name)

        # Write pass/fail lists
        with open(os.path.join(logs_dir, "0702_code_pass.log"), "w") as f:
            if ok:
                f.write("\n".join(ok) + "\n")
        with open(os.path.join(logs_dir, "0703_code_fail.log"), "w") as f:
            if fail:
                f.write("\n".join(fail) + "\n")

        # Human-readable summary into the main code log
        summary_lines = []
        summary_lines.append("CODE PRODUCTS (download summary)")
        summary_lines.append(f"  OK   ({len(ok)}): "   + (", ".join(ok)   if ok   else "—"))
        summary_lines.append(f"  FAIL ({len(fail)}): " + (", ".join(fail) if fail else "—"))
        mark_done("CODE", code_log)

    except Exception as e:
        mark_error("CODE", code_log, e)
        raise

# ------------------
# VMF module
# ------------------

import os, glob, shutil
from datetime import datetime, timedelta

def process_vmf(run_dir: str, yy: str, doy: str, logs_dir: str):
    """
    Download VMF3_OP pieces (H00,H06,H12,H18 of D, plus H00 of D+1) and
    concatenate into VMF3_<YYYY><DOY>0.GRD under <run_dir>/vmf3/.

    Logs:
      - 0601_vmf.log         : verbose module log with status markers
      - vmf_pass.log         : list of final GRD files created (one per line)
      - vmf_fail.log         : any failed artifacts (missing pieces / GRD failure)
    """
    vmf_log   = os.path.join(logs_dir, "0801_vmf.log")
    pass_log  = os.path.join(logs_dir, "0802_vmf_pass.log")
    fail_log  = os.path.join(logs_dir, "0803_vmf_fail.log")

    # Status: START
    mark_start("VMF", vmf_log)

    try:
        # Resolve date
        date_obj = datetime.strptime(f"20{yy}-{doy}", "%Y-%j")
        yyyy = date_obj.strftime("%Y")
        mmdd = date_obj.strftime("%m%d")      # not used, just for sanity/debug
        y_doy = date_obj.strftime("%Y%j")
        nextday_obj = date_obj + timedelta(days=1)
        next_ymd    = nextday_obj.strftime("%Y%m%d")
        cur_ymd     = date_obj.strftime("%Y%m%d")

        # Where to store
        vmf_dir = os.path.join(run_dir, "vmf")
        os.makedirs(vmf_dir, exist_ok=True)

        # TU Wien VMF OP BASE
        base_url = f"https://vmf.geo.tuwien.ac.at/trop_products/GRID/1x1/VMF3/VMF3_OP/{yyyy}"

        # pieces: 4 of current day + 1 of next day
        pieces = [
            (f"VMF3_{cur_ymd}.H00", cur_ymd),
            (f"VMF3_{cur_ymd}.H06", cur_ymd),
            (f"VMF3_{cur_ymd}.H12", cur_ymd),
            (f"VMF3_{cur_ymd}.H18", cur_ymd),
            (f"VMF3_{next_ymd}.H00", next_ymd),
        ]

        with open(vmf_log, "a", encoding="utf-8") as lg:
            lg.write(f"Target date: {date_obj.date()} (YYYY/DOY={yyyy}/{doy})\n")
            lg.write(f"VMF dir: {vmf_dir}\n")
            lg.write("Downloading pieces:\n")
            for fname, ymd in pieces:
                lg.write(f"  - {fname}\n")

        # Download pieces
        missing = []
        for fname, ymd in pieces:
            url = f"{base_url}/{fname}"
            dst = os.path.join(vmf_dir, fname)
            if os.path.exists(dst) and os.path.getsize(dst) > 0:
                # already there
                continue
            cmd = f'wget -nv --timeout=30 --tries=2 -O "{dst}" "{url}"'
            ok = run_shell(cmd, vmf_log)
            if not ok or not os.path.exists(dst) or os.path.getsize(dst) == 0:
                if os.path.exists(dst):
                    try: os.remove(dst)
                    except: pass
                missing.append(fname)

        # Concatenate → GRD
        out_name = f"VMF3_{yyyy}{doy}0.GRD"
        out_path = os.path.join(vmf_dir, out_name)

        if missing:
            # If pieces missing, we still try to concatenate the ones we have (best-effort),
            # but mark failure and list what’s missing.
            with open(vmf_log, "a", encoding="utf-8") as lg:
                lg.write("Missing pieces (cannot fully assemble GRD):\n")
                for m in missing:
                    lg.write(f"  - {m}\n")

        # Build GRD best-effort (only the available pieces in correct order)
        with open(out_path, "wb") as out_f:
            for fname, _ in pieces:
                piece_path = os.path.join(vmf_dir, fname)
                if os.path.exists(piece_path) and os.path.getsize(piece_path) > 0:
                    with open(piece_path, "rb") as in_f:
                        out_f.write(in_f.read())

        # Verify GRD result
        grd_ok = os.path.exists(out_path) and os.path.getsize(out_path) > 0

        # Pass/Fail logs
        passed = []
        failed = []

        if grd_ok and not missing:
            # perfect outcome
            passed.append(out_name)
        else:
            # record failures
            if missing:
                failed.extend(missing)
            if not grd_ok:
                failed.append(out_name)

        with open(pass_log, "w", encoding="utf-8") as f:
            for n in sorted(set(passed)):
                f.write(n + "\n")

        with open(fail_log, "w", encoding="utf-8") as f:
            for n in sorted(set(failed)):
                f.write(n + "\n")

        # Status: DONE/ERROR
        if grd_ok:
            mark_done("VMF", vmf_log)
        else:
            # even if mark_error, keep going to raise for caller
            mark_error("VMF", vmf_log, f"GRD not created or empty; missing={missing}")
            # Raise to let main handle global flow if you prefer:
            # raise RuntimeError("VMF GRD creation failed")

    except Exception as e:
        mark_error("VMF", vmf_log, e)
        raise

# ------------------
# Upload module
# ------------------

def process_upload(run_dir: str, logs_dir: str, externals: dict):
    """
    Upload artifacts to DATAPOOL.

    Sources:
      - RINEX:  <run_dir>/observations/*.YYd.Z (and *.YYD.Z)
      - CODE:   <run_dir>/orbits/*  (EPH/CLK/TRO/OBX/SNX/ERP/ION/DCB/...)
      - VMF:   <run_dir>/vmf/*.GRD

    Destinations under externals['datapool']:
      - RINEX  -> DATAPOOL/RINEX
      - ION/DCB-> DATAPOOL/BSW54
      - others -> DATAPOOL/COD
      - VMF   -> DATAPOOL/VMF3
    """
    up_log   = os.path.join(logs_dir, "0901_upload.log")
    pass_log = os.path.join(logs_dir, "0902_upload_pass.log")
    fail_log = os.path.join(logs_dir, "0903_upload_fail.log")

    mark_start("UPLOAD", up_log)

    dp_root = externals.get("datapool", "").strip()
    if not dp_root:
        mark_error("UPLOAD", up_log, "externals['datapool'] is not configured")
        with open(fail_log, "w", encoding="utf-8") as f:
            f.write("CONFIG: externals['datapool'] missing or empty\n")
        return

    # Targets
    dp_rinex = os.path.join(dp_root, "RINEX")
    dp_cod   = os.path.join(dp_root, "COD")
    dp_bsw   = os.path.join(dp_root, "BSW54")
    dp_vmf  = os.path.join(dp_root, "VMF3")

    for d in (dp_rinex, dp_cod, dp_bsw, dp_vmf):
        os.makedirs(d, exist_ok=True)

    obs_dir  = os.path.join(run_dir, "observations")
    orb_dir  = os.path.join(run_dir, "orbits")
    vmf_dir  = os.path.join(run_dir, "vmf")

    passed = []
    failed = []

    def _copy(source: str, dst_dir: str, bucket: str):
        base = os.path.basename(source)
        dst  = os.path.join(dst_dir, base)
        try:
            shutil.copy2(source, dst)
            passed.append(f"{bucket}: {base}")
            return True
        except Exception as e:
            failed.append(f"{bucket}: {base} :: {e}")
            return False

    try:
        # ---- RINEX ----------------------------------------------------------
        with open(up_log, "a", encoding="utf-8") as lg:
            lg.write("Uploading RINEX…\n")

        if os.path.isdir(obs_dir):
            for pat in (f"*.??d.Z", f"*.??D.Z"):  # e.g. *.22d.Z / *.22D.Z
                for source in glob.glob(os.path.join(obs_dir, pat)):
                    _copy(source, dp_rinex, "RINEX")
        else:
            with open(up_log, "a", encoding="utf-8") as lg:
                lg.write(f"  (missing observations dir: {obs_dir})\n")

        # ---- CODE -----------------------------------------------------------
        with open(up_log, "a", encoding="utf-8") as lg:
            lg.write("Uploading CODE products…\n")

        if os.path.isdir(orb_dir):
            code_files = glob.glob(os.path.join(orb_dir, "*"))
            for source in code_files:
                if not os.path.isfile(source):
                    continue
                name = os.path.basename(source)
                low  = name.lower()

                # ION & DCB go to BSW54
                if low.endswith(".ion") or low.endswith(".ion.z"):
                    _copy(source, dp_bsw, "BSW54")
                    continue
                if low.endswith(".dcb") or low.endswith(".dcb.z"):
                    _copy(source, dp_bsw, "BSW54")
                    continue

                # Everything else → COD (EPH/CLK/TRO/OBX/SNX/ERP/…)
                _copy(source, dp_cod, "COD")
        else:
            with open(up_log, "a", encoding="utf-8") as lg:
                lg.write(f"  (missing orbits dir: {orb_dir})\n")

        # ---- VMF -----------------------------------------------------------
        with open(up_log, "a", encoding="utf-8") as lg:
            lg.write("Uploading VMF…\n")

        if os.path.isdir(vmf_dir):
            for source in glob.glob(os.path.join(vmf_dir, "*.GRD")):
                _copy(source, dp_vmf, "VMF")
        else:
            with open(up_log, "a", encoding="utf-8") as lg:
                lg.write(f"  (missing vmf dir: {vmf_dir})\n")

        # Write pass/fail summaries
        with open(pass_log, "w", encoding="utf-8") as f:
            for line in sorted(set(passed)):
                f.write(line + "\n")

        with open(fail_log, "w", encoding="utf-8") as f:
            for line in sorted(set(failed)):
                f.write(line + "\n")

        if failed:
            mark_error("UPLOAD", up_log, f"{len(failed)} failures; see upload_fail.log")
        else:
            mark_done("UPLOAD", up_log)

    except Exception as e:
        # catastrophic error
        try:
            with open(fail_log, "a", encoding="utf-8") as f:
                f.write(f"FATAL: {e}\n")
        except:
            pass
        mark_error("UPLOAD", up_log, e)
        raise

# ------------------
# Update module
# ------------------

def process_update(paths, externals, logs_dir):
    """
    Update module:
      1) Mirror AIUB BSWUSER54 CONFIG, MODEL, REF to local targets.
      2) Copy REF/CODE.STA to <datapool>/REF54/AZOR.STA
      3) Merge local rows (TYPE 001 & TYPE 002) from <configuration>/station_file_merge
         into AZOR.STA (only the rows window; headers remain untouched).
    """
    import tempfile
    import shutil
    import re
    from pathlib import Path

    log = os.path.join(logs_dir, "1001_update.log")

    def mark_start(name):
        log_message(f"----- {name} -----", log)
        log_message("Status: Started", log)

    def mark_done(name):
        log_message(f"Status: Done", log)

    def mark_error(name, e):
        log_message(f"=== {name} ERROR {e}", log)

    # --------------------------
    # Helpers (local to function)
    # --------------------------

    def _wget_tree(source_url: str, dest_dir: str):
        """
        Mirror an HTTP directory tree into dest_dir using wget.
        """
        os.makedirs(dest_dir, exist_ok=True)
        # Use -nH --cut-dirs to avoid creating ftp.aiub.unibe.ch/BSWUSER54/... inside
        cmd = (
            f'wget -r -np -nH --cut-dirs=1 --reject "index.html*" '
            f'-P "{dest_dir}" "{source_url.rstrip("/")}/"'
        )
        run_shell(cmd, log)

    def _sudo_rsync(source_dir: str, dst_dir: str):
        """
        rsync the contents of source_dir → dst_dir (sudo -n expected).
        """
        os.makedirs(source_dir, exist_ok=True)
        os.makedirs(dst_dir, exist_ok=True)
        # Make sure sudoers allows rsync/mkdir/chown/chmod
        cmd = (
            f'sudo -n rsync -a --delete "{source_dir.rstrip(os.sep)}/" '
            f'"{dst_dir.rstrip(os.sep)}/"'
        )
        run_shell(cmd, log)

    # Fuzzy finder for a section’s rows window:
    # Returns (start_idx, end_idx) where the window begins just after the
    # underline line of asterisks and ends right before the next TYPE header.
    def _find_rows_window_fuzzy(text: str, type_id: str, title_keywords: str):
        """
        type_id: e.g. "001" or "002"
        title_keywords: a short phrase expected in the section title,
                        e.g. "RENAMING OF STATIONS" or "STATION INFORMATION"
        """
        # 1) Locate the section title line (case-insensitive, flexible spacing)
        title_re = re.compile(
            rf'^\s*TYPE\s*{type_id}\s*:\s*.*{re.escape(title_keywords)}.*$',
            re.IGNORECASE | re.MULTILINE
        )
        m_title = title_re.search(text)
        if not m_title:
            return (None, None)

        # 2) Locate the underline of dashes (allow one line of dashes after title)
        #    and then the underline of asterisks further down.
        start_pos = m_title.end()
        # first the dashed underline line (optional), stop at its end
        m_dash = re.search(r'^[\-\s]{3,}$', text[start_pos:], re.MULTILINE)
        if m_dash:
            start_pos = start_pos + m_dash.end()

        # find the asterisks header underline (the column ruler)
        m_ast = re.search(r'^\*{6,}.*$', text[start_pos:], re.MULTILINE)
        if not m_ast:
            return (None, None)
        # rows start right after the asterisk line's EOL
        ast_line_end = start_pos + m_ast.end()
        # move to next \n (end of that line), then rows begin
        nl = text.find("\n", ast_line_end)
        rows_start = nl + 1 if nl != -1 else ast_line_end

        # 3) Rows end at the next TYPE xxx: header or EOF
        m_next = re.search(r'^\s*TYPE\s*\d{3}\s*:\s*', text[rows_start:], re.IGNORECASE | re.MULTILINE)
        rows_end = (rows_start + m_next.start()) if m_next else len(text)

        return (rows_start, rows_end)

    def _merge_rows(base_text: str, local_text: str, type_id: str, title_keywords: str) -> Tuple[str, int]:
        """
        Merge unique non-empty lines from local_text TYPE block into base_text TYPE block.
        Returns (new_text, added_count).
        """
        b0, b1 = _find_rows_window_fuzzy(base_text, type_id, title_keywords)
        if b0 is None:
            log_message(f"[update] TYPE {type_id}: '{title_keywords}': base section not found, skipping", log)
            return base_text, 0

        l0, l1 = _find_rows_window_fuzzy(local_text, type_id, title_keywords)
        if l0 is None:
            log_message(f"[update] TYPE {type_id}: '{title_keywords}': local section not found, skipping", log)
            return base_text, 0

        base_rows = base_text[b0:b1]
        local_rows = local_text[l0:l1]

        # Normalize lines: keep exact row text (no trimming headers)
        base_lines = [ln for ln in base_rows.splitlines() if ln.strip() != ""]
        local_lines = [ln for ln in local_rows.splitlines() if ln.strip() != ""]

        base_set = set(base_lines)
        to_add = [ln for ln in local_lines if ln not in base_set]

        if not to_add:
            log_message(f"[update] TYPE {type_id}: nothing new to add", log)
            return base_text, 0

        # Append without creating extra blank lines
        new_rows = base_rows.rstrip("\n")
        if not new_rows.endswith("\n") and len(new_rows) > 0:
            new_rows += "\n"
        new_rows += "\n".join(to_add) + "\n"

        new_text = base_text[:b0] + new_rows + base_text[b1:]
        log_message(f"[update] TYPE {type_id}: added {len(to_add)} row(s)", log)
        return new_text, len(to_add)

    # --------------------------
    # 1) Mirror CONFIG / MODEL / REF
    # --------------------------
    mark_start("UPDATE (mirror BSWUSER54)")
    try:
        bsw_root = "http://ftp.aiub.unibe.ch/BSWUSER54"
        bern54 = externals.get("bern54") or "/external/BERN54"  # fallback
        datapool = externals.get("datapool") or os.path.expanduser("~/GPSDATA/DATAPOOL")

        cfg_dst = os.path.join(bern54, "GLOBAL", "CONFIG")
        mdl_dst = os.path.join(bern54, "GLOBAL", "MODEL")
        ref_dst = os.path.join(datapool, "REF54")

        with tempfile.TemporaryDirectory(prefix="bsw_mirror_") as tmpdir:
            cfg_tmp = os.path.join(tmpdir, "CONFIG")
            mdl_tmp = os.path.join(tmpdir, "MODEL")
            ref_tmp = os.path.join(tmpdir, "REF")

            _wget_tree(f"{bsw_root}/CONFIG", cfg_tmp)
            _wget_tree(f"{bsw_root}/MODEL", mdl_tmp)
            _wget_tree(f"{bsw_root}/REF",   ref_tmp)

            # sudo-rsync into targets
            _sudo_rsync(cfg_tmp, cfg_dst)
            _sudo_rsync(mdl_tmp, mdl_dst)
            _sudo_rsync(ref_tmp, ref_dst)

        log_message(f"[update] Mirrored CONFIG → {cfg_dst}", log)
        log_message(f"[update] Mirrored MODEL  → {mdl_dst}", log)
        log_message(f"[update] Mirrored REF    → {ref_dst}", log)
        mark_done("UPDATE (mirror BSWUSER54)")
    except Exception as e:
        mark_error("UPDATE (mirror BSWUSER54)", e)
        # continue; we still try AZOR.STA refresh

    # --------------------------
    # 2) Refresh AZOR.STA from CODE.STA, then merge local lines
    # --------------------------
    mark_start("UPDATE (AZOR.STA)")
    try:
        ref_dir = os.path.join(externals.get("datapool") or os.path.expanduser("~/GPSDATA/DATAPOOL"), "REF54")
        os.makedirs(ref_dir, exist_ok=True)
        azor_path = os.path.join(ref_dir, "AZOR.STA")
        code_url  = "http://ftp.aiub.unibe.ch/BSWUSER54/REF/CODE.STA"

        # Download CODE.STA to temp and move into AZOR.STA
        with tempfile.TemporaryDirectory(prefix="azor_") as tmpdir:
            tmp_code = os.path.join(tmpdir, "CODE.STA")
            run_shell(f'wget -nv --timeout=30 --tries=2 -O "{tmp_code}" "{code_url}"', log)
            if not os.path.isfile(tmp_code) or os.path.getsize(tmp_code) == 0:
                raise RuntimeError("Failed to download CODE.STA")

            shutil.copyfile(tmp_code, azor_path)
            log_message(f"[update] Wrote base AZOR.STA from CODE.STA → {azor_path}", log)

        # Merge local extra rows from station_file_merge (if present)
        configuration_dir = paths.get("configuration") or os.path.join(paths.get("main", ""), "configuration")
        merge_path = os.path.join(configuration_dir, "station_file_merge")

        if not os.path.isfile(merge_path):
            log_message(f"[update] No station_file_merge at {merge_path}; skip local merge", log)
            mark_done("UPDATE (AZOR.STA)")
            return

        with open(azor_path, "r", encoding="utf-8", errors="ignore") as f:
            base_txt = f.read()
        with open(merge_path, "r", encoding="utf-8", errors="ignore") as f:
            local_txt = f.read()

        added_total = 0
        # TYPE 001: RENAMING OF STATIONS
        base_txt, n1 = _merge_rows(base_txt, local_txt, "001", "RENAMING OF STATIONS")
        added_total += n1
        # TYPE 002: STATION INFORMATION
        base_txt, n2 = _merge_rows(base_txt, local_txt, "002", "STATION INFORMATION")
        added_total += n2

        # Write back if changed
        with open(azor_path, "w", encoding="utf-8") as f:
            f.write(base_txt)

        log_message(f"[update] AZOR.STA merged: +{added_total} new line(s).", log)
        mark_done("UPDATE (AZOR.STA)")
    except Exception as e:
        mark_error("UPDATE (AZOR.STA)", e)
        raise

# ------------------
# Bernese module
# ------------------

def process_bernese(
    run_dir: str,
    logs_dir: str,
    processing_type: str,     # "final" or "rapid"
    date_obj: datetime,
    externals: dict
):
    """
    Run the Bernese perl driver (final.pl or rapid.pl) for a given date.

    - Writes step log: 0800_bernese.log
    - Uses mark_start / mark_done / mark_error for concise status
    - Returns the driver return code
    """
    step_name = "BERNESE"
    bern_log  = os.path.join(logs_dir, "1101_bernese.log")
    mark_start(step_name, bern_log)

    try:
        # Make sure Bernese environment is on PATH if provided
        env = os.environ.copy()
        bern54_bin = externals.get("bern54", "").strip()
        if bern54_bin:
            env["PATH"] = bern54_bin + os.pathsep + env.get("PATH", "")

        # The perl drivers (final.pl / rapid.pl) are expected to be next to this script
        script_dir  = os.path.dirname(os.path.abspath(__file__))
        driver_name = f"{processing_type}.pl"
        driver_path = os.path.join(script_dir, driver_name)

        if not os.path.isfile(driver_path):
            with open(bern_log, "a", encoding="utf-8") as f:
                f.write(f"[process_bernese] Missing driver: {driver_path}\n")
            mark_error(step_name, bern_log, f"Missing driver: {driver_name}")
            return 1

        yyyy = date_obj.strftime("%Y")
        doy  = date_obj.strftime("%j")  # 001..366; Bernese expects DOY0 on the CLI
        cmd  = f'perl "{driver_path}" {yyyy} {doy}0'

        with open(bern_log, "a", encoding="utf-8") as f:
            f.write(f"[process_bernese] CMD: {cmd}\n")

        # Run in the same directory as the driver (so any relative paths work)
        rc = run_shell(cmd, bern_log, cwd=script_dir, env=env)

        if rc == 0:
            mark_done(step_name, bern_log)
        else:
            mark_error(step_name, bern_log, f"Return code {rc}")
        return rc

    except Exception as e:
        mark_error(step_name, bern_log, e)
        raise

# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", required=True, help="Username")
    parser.add_argument("--mode", default="manual", choices=["auto","manual"])
    parser.add_argument("--style", choices=["permanent","survey"], default="permanent")
    parser.add_argument("--period", nargs=2, help="Initial and final dates (YYDOY YYDOY)")
    parser.add_argument("--survey", help="Survey code name (e.g. 'SMIG25')")
    parser.add_argument("--lag",  type=str, default="", help="Comma-separated day offsets in the past for auto mode (e.g. '1,15' for 1-day rapid and 15-day final lag runs)")
    parser.add_argument("--except", dest="exclude", default="", help="Comma-separated IGS station codes to exclude (e.g. 'PDEL,AZGR')")
    parser.add_argument("--non-interactive", action="store_true")
    parser.add_argument("--skip", type=str, default="", help="Modules to skip")
    parser.add_argument("--only", type=str, default="", help="Modules to run exclusively")
    args = parser.parse_args()

    # --------- config + stations ---------
    excluded_igs = _parse_station_list(args.exclude)
    (paths, externals, ftp, teqc_opts, rinex_cmp, stations_file, stations_teqc_file) = load_config()
    igs_list, civisa_list, repraa_list = load_station_lists(stations_file)

    # --------- run dirs ---------
    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_dir  = os.path.join(paths["output"], args.user, now)
    logs_dir = os.path.join(run_dir, "logs")
    obs_base = os.path.join(run_dir, "observations")
    orb_base = os.path.join(run_dir, "orbits")
    os.makedirs(obs_base, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)

    # --------- compute skips using your utility ---------
    skips = get_module_selection_from_args(args)

    # --------- friendly header ---------
    period_str = " ".join(args.period) if args.period else ""
    write_summary_header(
        user=args.user,
        mode=args.mode,
        style=args.style,
        period=period_str,
        survey=(args.survey or ""),
        logs_dir=logs_dir,
    )

    # --------- dates ---------
    if args.mode != "manual":
        raise SystemExit("Frontend uses manual mode. Use --mode manual.")
    if not args.period or len(args.period) != 2:
        raise SystemExit("For manual mode you must supply --period YYDOY YYDOY")
    date_list = _expand_period(args.period[0], args.period[1])

    try:
        # PREPARATION
        if not skips.get("preparation", False):
            process_preparation(logs_dir)

        # ===== per-day loop =====
        for date_obj in date_list:
            yy, doy = date_obj.strftime("%y"), date_obj.strftime("%j")

            # Purge excluded stations from repositories for that day first
            _remove_excluded_from_repos(
                dp_root=externals["datapool"],
                campaign54_root=externals["campaign"],
                yy=yy,
                doy=doy,
                excluded=excluded_igs,
                logs_dir=logs_dir
            )

            # Then prepare the filtered station list for IGS
            igs_for_run = [s for s in igs_list if s.upper() not in excluded_igs]

            # IGS
            if not skips.get("igs", False):
                process_igs(
                    obs_base=obs_base,
                    ftp_list=ftp["IGS"],
                    stations=igs_for_run,
                    yy=yy,
                    doy=doy,
                    logs_dir=logs_dir
                )
            
            # PERMANENT
            if not skips.get("permanent", False):
                process_permanent(
                    obs_base=obs_base,
                    yy=yy,
                    doy=doy,
                    civisa_stations=civisa_list,
                    repraa_stations=repraa_list,
                    spider_dir=externals["spider"],
                    monitoring_dir=externals["monitoring"],
                    logs_dir=logs_dir,
                    teqc_opts=teqc_opts
                )

            # TEQC
            if not skips.get("teqc", False):
                process_teqc(
                    obs_base=obs_base,
                    yy=yy,
                    doy=doy,
                    stations_teqc_file=stations_teqc_file,
                    logs_dir=logs_dir
                )

            # COMPRESSION
            if not skips.get("compress", False):
                process_compress(
                    obs_base=obs_base,
                    yy=yy,
                    logs_dir=logs_dir
                )

            if args.style == "survey" and not skips.get("survey", False):
                process_survey(
                    survey_base=externals["survey"], 
                    survey_code=args.survey,   
                    obs_base=obs_base,
                    logs_dir=logs_dir
                )
            
            # CODE        
            if not skips.get("code", False):
                process_code(
                    orb_base=orb_base,
                    yy=yy,
                    doy=doy,
                    ftp_code=ftp["CODE"],
                    logs_dir=logs_dir
                )

            # VMF
            if not skips.get("vmf", False):
                process_vmf(
                    run_dir=run_dir, 
                    yy=yy, doy=doy, 
                    logs_dir=logs_dir
                )

            # UPLOAD
            if not skips.get("upload", False):
                process_upload(run_dir=run_dir,
                    logs_dir=logs_dir, 
                    externals=externals
                )

            # UPDATE
            if not skips.get("update", False):
                process_update(
                    paths=paths,
                    externals=externals,
                    logs_dir=logs_dir
                )

            # BERNESE
            if not skips.get("bernese", False):
                process_bernese(
                    run_dir=run_dir,
                    logs_dir=logs_dir,
                    processing_type="final",
                    date_obj=date_obj,
                    externals=externals
                )
            
            # FINALIZATION

        sys.exit(0)

    except Exception as e:
        # Error Log
        try:
            mark_error("", os.path.join(logs_dir, "error.log"), e)
        except Exception:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main()
