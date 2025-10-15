#!/usr/bin/env python3
"""
config_loader.py — unified config loader for DeforMA

- Loads /opt/DeforMA/configuration/config.yaml (or a provided path)
- Expands ${...} placeholders with dotted keys (e.g., ${externals.monitoring})
- Expands environment variables and ~
- Returns a dataclass with resolved sections and handy helpers
"""

from __future__ import annotations
import os
import re
import yaml
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Iterable

# -----------------------------
# Internal expansion utilities
# -----------------------------

def _expand_env_and_user(s: str) -> str:
    return os.path.expanduser(os.path.expandvars(s))

def _get_from_ctx(ctx: dict, dotted: str):
    cur = ctx
    for part in dotted.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur

_PLACEHOLDER_RE = re.compile(r"\$\{([^}]+)\}")

def _expand_placeholders(val, ctx: dict):
    if not isinstance(val, str):
        return val
    def repl(m):
        key = m.group(1).strip()
        v = _get_from_ctx(ctx, key)
        return str(v) if v is not None else m.group(0)  # leave unknown intact
    out = _PLACEHOLDER_RE.sub(repl, val)
    return _expand_env_and_user(out)

def _deep_expand(data, ctx: dict):
    if isinstance(data, dict):
        return {k: _deep_expand(v, ctx) for k, v in data.items()}
    if isinstance(data, list):
        return [_deep_expand(v, ctx) for v in data]
    if isinstance(data, str):
        return _expand_placeholders(data, ctx)
    return data

# -----------------------------
# Dataclass for resolved config
# -----------------------------

@dataclass
class Config:
    # raw/resolved
    main: str
    paths: Dict[str, str]
    externals: Dict[str, str]
    files: Dict[str, str]
    database: Dict[str, str]
    user_workspace: Dict[str, str]
    ftp: Dict
    options: Dict

    # useful resolved shortcuts
    config_path: str
    user_db: str
    sinex_root: str
    sinex_frames: List[str]
    sinex_sol_dir: str
    sinex_sol_pattern: str

    # Optional: keep whole raw (expanded) for reference
    raw: Dict = field(repr=False, default_factory=dict)

    # -------------------------
    # Helpers
    # -------------------------

    def ensure_user_workspace(self, create: bool = True) -> None:
        """Create user workspace folders (database, workpool, logs, help)."""
        if not create:
            return
        for key in ("database", "workpool", "logs", "help"):
            p = self.user_workspace.get(key)
            if p:
                os.makedirs(p, exist_ok=True)

    def sinex_search_paths(
        self,
        frames: Optional[Iterable[str]] = None,
        years: Optional[Iterable[int]] = None
    ) -> List[str]:
        """
        Build glob patterns for SINEX discovery like:
        <sinex_root>/<FRAME>/<YYYY>/<sinex_sol_dir>/<sinex_sol_pattern>
        """
        f_list = list(frames) if frames is not None else self.sinex_frames
        y_list = list(years) if years is not None else []
        patterns = []
        if not f_list:
            return patterns
        if not y_list:
            # No years specified -> wildcard year
            for fr in f_list:
                patterns.append(os.path.join(self.sinex_root, fr, "*", self.sinex_sol_dir, self.sinex_sol_pattern))
        else:
            for fr in f_list:
                for y in y_list:
                    patterns.append(os.path.join(self.sinex_root, fr, str(y), self.sinex_sol_dir, self.sinex_sol_pattern))
        return patterns

# -----------------------------
# Public loader
# -----------------------------

def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load and resolve DeforMA YAML configuration.
    Resolution order:
      1) main
      2) paths, externals, files, database, user_workspace, ftp, options using dotted placeholders
      3) env vars and ~ expansion

    Returns a Config dataclass with handy helpers.
    """
    if not config_path:
        # Allow override via env; fallback to default
        config_path = os.environ.get("DEFORMA_CONFIG", "/opt/DeforMA/configuration/config.yaml")

    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    # Phase 1: resolve 'main'
    main = raw.get("main", "/opt/DeforMA")
    main = _expand_env_and_user(main)

    # Prepare initial context (raw dicts)
    ctx = {
        "main": main,
        "paths": raw.get("paths", {}),
        "externals": raw.get("externals", {}),
        "files": raw.get("files", {}),
        "database": raw.get("database", {}),
        "user_workspace": raw.get("user_workspace", {}),
        "ftp": raw.get("ftp", {}),
        "options": raw.get("options", {}),
    }

    # Phase 2: expand each block using dotted placeholders
    paths          = _deep_expand(raw.get("paths", {}), ctx)
    ctx["paths"]   = paths
    externals      = _deep_expand(raw.get("externals", {}), ctx)
    ctx["externals"]= externals
    files          = _deep_expand(raw.get("files", {}), ctx)
    ctx["files"]   = files
    database       = _deep_expand(raw.get("database", {}), ctx)
    ctx["database"]= database
    user_ws        = _deep_expand(raw.get("user_workspace", {}), ctx)
    ctx["user_workspace"] = user_ws
    ftp            = _deep_expand(raw.get("ftp", {}), ctx)
    options        = _deep_expand(raw.get("options", {}), ctx)

    # Handy resolved fields with defaults
    user_db = user_ws.get("database") or os.path.join(os.path.expanduser("~"), "DeforMA", "database", "DeforMA.user.db")
    sinex_root = files.get("sinex_root") or externals.get("bernese") or ""
    frames = files.get("sinex_frames") or ["IGB08", "IGS14", "IGS20"]
    sol_dir = files.get("sinex_sol_dir", "SOL")
    pattern = files.get("sinex_sol_pattern", "*.SNX.gz")

    # Normalize paths
    user_db    = os.path.normpath(user_db)
    sinex_root = os.path.normpath(sinex_root)

    # Minimal validation/sanity
    if not sinex_root:
        # Not fatal—caller can still proceed—but it’s useful to surface.
        pass

    return Config(
        main=main,
        paths=paths,
        externals=externals,
        files=files,
        database=database,
        user_workspace=user_ws,
        ftp=ftp,
        options=options,
        config_path=config_path,
        user_db=user_db,
        sinex_root=sinex_root,
        sinex_frames=list(frames),
        sinex_sol_dir=sol_dir,
        sinex_sol_pattern=pattern,
        raw=_deep_expand(raw, ctx),
    )

