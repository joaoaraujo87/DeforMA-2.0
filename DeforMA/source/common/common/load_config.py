#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Common configuration loader for DeforMA.

- Safe YAML load
- Recursive ${var} expansion (supports ${main}, ${paths.*}, ${externals.*}, ${USER}, ${HOME}, $HOME, ~)
- Typed access via dataclasses with .to_dict()
- Small utilities to ensure user workspace directories and print a summary
"""

from __future__ import annotations
import os
import sys
import re
import yaml
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional


# ============================================================
# Defaults
# ============================================================

DEFAULT_MAIN = "/opt/DeforMA"
DEFAULT_CONFIG = f"{DEFAULT_MAIN}/configuration/config.yaml"


# ============================================================
# Dataclasses
# ============================================================

@dataclass
class Paths:
    configuration: str
    external: str
    source: str
    output: str
    documentation: str
    install: str
    database: str


@dataclass
class Database:
    core: str


@dataclass
class UserWorkspace:
    root: str
    database: str
    workpool: str
    logs: str
    help: str


@dataclass
class Externals:
    bernese: str
    monitoring: str
    survey: str
    spider: str
    webservices: str
    datapool: str
    campaign: str
    bern54: str


@dataclass
class FTP:
    IGS: list
    CODE: str


@dataclass
class Files:
    stations: str
    stations_teqc: str
    events: str
    stable: str
    trend: str
    sinex_root: str


@dataclass
class Options:
    teqc_opts: str
    rinex_compression: str


@dataclass
class Config:
    version: int
    main: str
    paths: Paths
    database: Database
    user_workspace: UserWorkspace
    externals: Externals
    ftp: FTP
    files: Files
    options: Options

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================
# Expansion helpers
# ============================================================

_VAR_RX = re.compile(r"\$\{([^}]+)\}")

def _env_seed() -> Dict[str, str]:
    """Base environment seed used in expansion."""
    # Ensure USER/HOME for expansion, even if script runs non-interactively
    seed = dict(os.environ)
    if "USER" not in seed:
        try:
            import getpass
            seed["USER"] = getpass.getuser()
        except Exception:
            seed["USER"] = "user"
    if "HOME" not in seed:
        seed["HOME"] = str(Path.home())
    return seed


def _multi_pass_expand(s: str, mapping: Dict[str, Any]) -> str:
    """
    Expand ${...} using 'mapping' (which may itself be nested dicts),
    then environment variables ($HOME, ${HOME}), then ~.
    Runs multiple passes until the string stabilizes or up to 6 passes.
    """
    if not isinstance(s, str):
        return s

    def _lookup(key: str) -> Optional[str]:
        # Support dotted keys: a.b.c
        cur = mapping
        for part in key.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return None
        if isinstance(cur, str):
            return cur
        # For non-strings, don't inline; just skip
        return None

    max_passes = 6
    out = s
    for _ in range(max_passes):
        prev = out

        # Expand ${dotted.keys}
        def repl(m):
            k = m.group(1).strip()
            v = _lookup(k)
            return v if v is not None else m.group(0)

        out = _VAR_RX.sub(repl, out)

        # Expand environment variables ($HOME, ${HOME}, $USER, etc.)
        out = os.path.expandvars(out)
        # Expand ~
        out = os.path.expanduser(out)

        if out == prev:
            break
    return out


def _deep_expand(obj: Any, mapping: Dict[str, Any]) -> Any:
    """Recursively expand strings within dicts/lists using _multi_pass_expand."""
    if isinstance(obj, dict):
        return {k: _deep_expand(v, mapping) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deep_expand(v, mapping) for v in obj]
    if isinstance(obj, str):
        return _multi_pass_expand(obj, mapping)
    return obj


# ============================================================
# Loader
# ============================================================

def _find_config_path(explicit: Optional[str]) -> Path:
    """
    Decide which config to load.
    Priority:
      1) explicit path (arg)
      2) $DEFORMA_CONFIG/config.yaml (if set)
      3) default /opt/DeforMA/configuration/config.yaml
    """
    if explicit:
        p = Path(explicit).expanduser()
        if p.is_file():
            return p
        print(f"[warn] config not found at explicit path: {p}", file=sys.stderr)

    env_cfg_dir = os.environ.get("DEFORMA_CONFIG")
    if env_cfg_dir:
        p = Path(env_cfg_dir).expanduser() / "config.yaml"
        if p.is_file():
            return p
        print(f"[warn] config not found at $DEFORMA_CONFIG/config.yaml: {p}", file=sys.stderr)

    p = Path(DEFAULT_CONFIG)
    if p.is_file():
        return p

    raise FileNotFoundError(f"Could not locate config.yaml. Tried: "
                            f"{explicit or '(explicit not provided)'}, "
                            f"$DEFORMA_CONFIG/config.yaml, {DEFAULT_CONFIG}")


def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load and expand the DeforMA configuration, returning a Config object.
    """
    cfg_file = _find_config_path(config_path)

    with open(cfg_file, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    # 1) Seed mapping with env + raw (so we can resolve ${main} etc.)
    seed = _env_seed()

    # Provide some top-level defaults if missing
    main = raw.get("main", DEFAULT_MAIN)
    seed["main"] = main

    # Placeholders for sections so dotted paths work in first pass
    seed["paths"] = raw.get("paths", {})
    seed["externals"] = raw.get("externals", {})
    seed["user_workspace"] = raw.get("user_workspace", {})
    seed["database"] = raw.get("database", {})
    seed["files"] = raw.get("files", {})
    seed["ftp"] = raw.get("ftp", {})
    seed["options"] = raw.get("options", {})

    # 2) First pass expand to resolve ${main} in the section trees
    pass1 = _deep_expand(raw, seed)

    # 3) Build a richer mapping from pass1 to allow nested references like ${paths.configuration}
    mapping = _deep_expand({
        "main": pass1.get("main", DEFAULT_MAIN),
        "paths": pass1.get("paths", {}),
        "externals": pass1.get("externals", {}),
        "user_workspace": pass1.get("user_workspace", {}),
        "database": pass1.get("database", {}),
        "files": pass1.get("files", {}),
        "ftp": pass1.get("ftp", {}),
        "options": pass1.get("options", {}),
        "HOME": _env_seed().get("HOME"),
        "USER": _env_seed().get("USER"),
    }, _env_seed())

    # 4) Final pass expansion on entire config
    expanded = _deep_expand(pass1, mapping)

    # 5) Fill with sensible defaults if any block is missing
    # Paths
    paths_raw = expanded.get("paths") or {}
    paths = Paths(
        configuration=paths_raw.get("configuration", f"{DEFAULT_MAIN}/configuration"),
        external=paths_raw.get("external", f"{DEFAULT_MAIN}/external"),
        source=paths_raw.get("source", f"{DEFAULT_MAIN}/source"),
        output=paths_raw.get("output", f"{DEFAULT_MAIN}/output"),
        documentation=paths_raw.get("documentation", f"{DEFAULT_MAIN}/documentation"),
        install=paths_raw.get("install", f"{DEFAULT_MAIN}/install"),
        database=paths_raw.get("database", f"{DEFAULT_MAIN}/database"),
    )

    # Database
    db_raw = expanded.get("database") or {}
    database = Database(
        core=db_raw.get("core", f"{paths.database}/DeforMA.core.db")
    )

    # User workspace
    uw_raw = expanded.get("user_workspace") or {}
    user_workspace = UserWorkspace(
        root=uw_raw.get("root", "$HOME/DeforMA"),
        database=uw_raw.get("database", "${user_workspace.root}/database/DeforMA.db"),
        workpool=uw_raw.get("workpool", "${user_workspace.root}/workpool"),
        logs=uw_raw.get("logs", "${user_workspace.root}/workpool/logs"),
        help=uw_raw.get("help", "${user_workspace.root}/help"),
    )
    # Expand again to ensure ${USER}, ${HOME}, ${user_workspace.root} are resolved
    uw_map = {
        "user_workspace": {
            "root": user_workspace.root
        },
        "USER": _env_seed()["USER"],
        "HOME": _env_seed()["HOME"],
    }
    user_workspace = UserWorkspace(
        root=_multi_pass_expand(user_workspace.root, uw_map),
        database=_multi_pass_expand(user_workspace.database, {"user_workspace": {"root": user_workspace.root}, **_env_seed()}),
        workpool=_multi_pass_expand(user_workspace.workpool, {"user_workspace": {"root": user_workspace.root}, **_env_seed()}),
        logs=_multi_pass_expand(user_workspace.logs, {"user_workspace": {"root": user_workspace.root}, **_env_seed()}),
        help=_multi_pass_expand(user_workspace.help, {"user_workspace": {"root": user_workspace.root}, **_env_seed()}),
    )

    # Externals
    ex_raw = expanded.get("externals") or {}
    externals = Externals(
        bernese=ex_raw.get("bernese", ""),
        monitoring=ex_raw.get("monitoring", ""),
        survey=ex_raw.get("survey", ""),
        spider=ex_raw.get("spider", ""),
        webservices=ex_raw.get("webservices", ""),
        datapool=ex_raw.get("datapool", ""),
        campaign=ex_raw.get("campaign", ""),
        bern54=ex_raw.get("bern54", ""),
    )

    # FTP
    ftp_raw = expanded.get("ftp") or {}
    ftp = FTP(
        IGS=ftp_raw.get("IGS", []) or [],
        CODE=ftp_raw.get("CODE", "")
    )

    # Files
    files_raw = expanded.get("files") or {}
    files = Files(
        stations=files_raw.get("stations", f"{paths.configuration}/stations.yaml"),
        stations_teqc=files_raw.get("stations_teqc", f"{paths.configuration}/teqc.yaml"),
        events=files_raw.get("events", f"{paths.configuration}/events.yaml"),
        stable=files_raw.get("stable", f"{paths.configuration}/stable.yaml"),
        trend=files_raw.get("trend", f"{paths.configuration}/trend.yaml"),
        sinex_root=files_raw.get("sinex_root", f"{externals.monitoring}/20103_Process/Bernese"),
    )

    # Options
    opt_raw = expanded.get("options") or {}
    options = Options(
        teqc_opts=opt_raw.get("teqc_opts", "-O.dec 30"),
        rinex_compression=opt_raw.get("rinex_compression", "gzip -f"),
    )

    cfg = Config(
        version=int(expanded.get("version", 1)),
        main=_multi_pass_expand(expanded.get("main", DEFAULT_MAIN), expanded),
        paths=paths,
        database=database,
        user_workspace=user_workspace,
        externals=externals,
        ftp=ftp,
        files=files,
        options=options,
    )

    return cfg


# ============================================================
# Utilities
# ============================================================

def ensure_user_workspace(cfg: Config) -> None:
    """Create the basic user workspace directories if they don't exist."""
    for d in [
        cfg.user_workspace.root,
        str(Path(cfg.user_workspace.database).parent),
        cfg.user_workspace.workpool,
        cfg.user_workspace.logs,
        cfg.user_workspace.help,
    ]:
        try:
            Path(d).expanduser().mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"[warn] could not create {d}: {e}", file=sys.stderr)


def print_config_summary(cfg: Config) -> None:
    """Compact human-readable summary (useful for scripts)."""
    print(f"Config       : {DEFAULT_CONFIG if cfg.main == DEFAULT_MAIN else 'custom'}")
    print(f"Main         : {cfg.main}")
    print(f"Paths.conf   : {cfg.paths.configuration}")
    print(f"User DB      : {cfg.user_workspace.database}")
    print(f"SINEX root   : {cfg.files.sinex_root}")
    print(f"Frames       : IGB08, IGS14, IGS20 (by convention)")
    print(f"Stations.yml : {cfg.files.stations}")


# ============================================================
# CLI (for quick test)
# ============================================================

if __name__ == "__main__":
    pth = sys.argv[1] if len(sys.argv) > 1 else None
    cfg = load_config(pth)
    ensure_user_workspace(cfg)
    print_config_summary(cfg)

