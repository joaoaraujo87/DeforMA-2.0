#!/bin/sh
# ============================================================
# DeforMA Environment Setup (POSIX sh, safe to source)
# ------------------------------------------------------------
# Usage:
#   . /opt/DeforMA/install/setup_env.sh
#   . /opt/DeforMA/install/setup_env.sh --persist
# Options:
#   --persist     Append environment block to ~/.bashrc
#   --no-aliases  Skip defining aliases
# ============================================================

# current project helpers
deforma-project-use() {
  # usage: deforma-project-use <NAME>
  if [ -z "$1" ]; then
    echo "Usage: deforma-project-use <NAME>" >&2
    return 2
  fi
  export DEFORMA_PROJECT="$1"
  mkdir -p "$DEFORMA_OUTPUT_DIR/$1/outputs" "$DEFORMA_OUTPUT_DIR/$1/logs"
  echo "$1" > "$HOME/DeforMA/.current_project"
  echo "Current DeforMA project: $DEFORMA_PROJECT"
}

deforma-project-show() {
  echo "${DEFORMA_PROJECT:-$(cat "$HOME/DeforMA/.current_project" 2>/dev/null || echo default)}"
}

# -------- Parse options (POSIX-safe) ------------------------
_PERSIST=0
_NO_ALIASES=0
while [ $# -gt 0 ]; do
  case "$1" in
    --persist) _PERSIST=1 ;;
    --no-aliases) _NO_ALIASES=1 ;;
    *)
      echo "Unknown option: $1" >&2
      (return 2) 2>/dev/null || exit 2
      ;;
  esac
  shift
done

# -------- Base installation paths ---------------------------
[ -n "$DEFORMA_PREFIX" ]    || DEFORMA_PREFIX="/opt/DeforMA"
[ -n "$DEFORMA_SOURCE" ]    || DEFORMA_SOURCE="$DEFORMA_PREFIX/source"
[ -n "$DEFORMA_EXTERNAL" ]  || DEFORMA_EXTERNAL="$DEFORMA_PREFIX/external"
[ -n "$DEFORMA_CONFIG" ]    || DEFORMA_CONFIG="$DEFORMA_PREFIX/configuration"
[ -n "$DEFORMA_WEB" ]       || DEFORMA_WEB="$DEFORMA_PREFIX/webpage"
[ -n "$DEFORMA_DOCS" ]      || DEFORMA_DOCS="$DEFORMA_PREFIX/documentation"
[ -n "$DEFORMA_DB" ]        || DEFORMA_DB="$HOME/DeforMA/database/DeforMA.db"
[ -n "$DEFORMA_OUTPUT_DIR" ]|| DEFORMA_OUTPUT_DIR="$HOME/DeforMA/workpool"
[ -n "$DEFORMA_HELP_DIR" ]  || DEFORMA_HELP_DIR="$HOME/DeforMA/help"
[ -n "$DEFORMA_USER_ROOT" ] || DEFORMA_USER_ROOT="$HOME/DeforMA"
[ -n "$DEFORMA_METADATA" ] || DEFORMA_METADATA="$DEFORMA_USER_ROOT/metadata"

export DEFORMA_PREFIX DEFORMA_SOURCE DEFORMA_EXTERNAL DEFORMA_CONFIG \
       DEFORMA_WEB DEFORMA_DOCS DEFORMA_DB \
       DEFORMA_OUTPUT_DIR DEFORMA_HELP_DIR \
       DEFORMA_USER_ROOT DEFORMA_METADATA

# -------- PATH & PYTHONPATH (POSIX idempotent) --------------
_add_path() {
  d="$1"
  [ -d "$d" ] || return 0
  case ":$PATH:" in *":$d:"*) : ;; *) PATH="$PATH:$d" ;; esac
}

_add_pythonpath() {
  d="$1"
  [ -d "$d" ] || return 0
  case ":$PYTHONPATH:" in *":$d:"*) : ;; *) PYTHONPATH="$PYTHONPATH:$d" ;; esac
}

_add_path "$DEFORMA_SOURCE"
_add_path "$DEFORMA_SOURCE/alert"
_add_path "$DEFORMA_SOURCE/analysis"
_add_path "$DEFORMA_SOURCE/database"
_add_path "$DEFORMA_SOURCE/modeling"
_add_path "$DEFORMA_SOURCE/plot"
_add_path "$DEFORMA_SOURCE/processing"
_add_path "$DEFORMA_EXTERNAL"

# Add DeforMA source root to PYTHONPATH (for imports like "from common.config_loader import load_config")
_add_pythonpath "$DEFORMA_SOURCE"
export PATH PYTHONPATH

# -------- Aliases (optional) --------------------------------
if [ "$_NO_ALIASES" -eq 0 ] && command -v alias >/dev/null 2>&1; then
  alias deforma-root="cd \"$DEFORMA_PREFIX\""
  alias deforma-src="cd \"$DEFORMA_SOURCE\""
  alias deforma-cfg="cd \"$DEFORMA_CONFIG\""
  alias deforma-web="cd \"$DEFORMA_WEB\""
  alias deforma-docs="cd \"$DEFORMA_DOCS\""

  alias db_view='python3 "$DEFORMA_SOURCE/database/db_view.py"'
  alias db_update='python3 "$DEFORMA_SOURCE/database/db_update.py"'
  alias deforma-alert='python3 "$DEFORMA_SOURCE/alert/static_alert.py"'
  alias alert_rtk='python3 "$DEFORMA_SOURCE/alert/alert_rtk.py"'
  alias deforma-webapp='python3 "$DEFORMA_SOURCE/webapp/app.py"'
  alias ts_analysis='python3 "$DEFORMA_SOURCE/analysis/ts_analysis.py"'

fi

# -------- Ensure user directories exist ---------------------
mkdir -p "$DEFORMA_OUTPUT_DIR" "$DEFORMA_OUTPUT_DIR/logs" "$DEFORMA_HELP_DIR" 2>/dev/null || true
mkdir -p "$DEFORMA_USER_ROOT" "$DEFORMA_METADATA" 2>/dev/null || true

# -------- Persist environment if requested ------------------
if [ "$_PERSIST" -eq 1 ]; then
  LOCAL_RC="$HOME/.bashrc"
  MARK_START="# >>> DeforMA environment >>>"
  MARK_END="# <<< DeforMA environment <<<"

  if ! grep -qF "$MARK_START" "$LOCAL_RC" 2>/dev/null; then
    {
      echo ""
      echo "$MARK_START"
      echo "export DEFORMA_PREFIX=\"$DEFORMA_PREFIX\""
      echo "export DEFORMA_SOURCE=\"$DEFORMA_SOURCE\""
      echo "export DEFORMA_EXTERNAL=\"$DEFORMA_EXTERNAL\""
      echo "export DEFORMA_CONFIG=\"$DEFORMA_CONFIG\""
      echo "export DEFORMA_WEB=\"$DEFORMA_WEB\""
      echo "export DEFORMA_DOCS=\"$DEFORMA_DOCS\""
      echo "export DEFORMA_DB=\"$DEFORMA_DB\""
      echo "export DEFORMA_OUTPUT_DIR=\"$DEFORMA_OUTPUT_DIR\""
      echo "export DEFORMA_HELP_DIR=\"$DEFORMA_HELP_DIR\""
      echo ""
      echo "# --- PATH additions ---"
      for d in \
        "\$DEFORMA_SOURCE" \
        "\$DEFORMA_SOURCE/alert" \
        "\$DEFORMA_SOURCE/analysis" \
        "\$DEFORMA_SOURCE/database" \
        "\$DEFORMA_SOURCE/modeling" \
        "\$DEFORMA_SOURCE/plot" \
        "\$DEFORMA_SOURCE/processing" \
        "\$DEFORMA_EXTERNAL"
      do
        echo '[ -d '"$d"' ] && case ":$PATH:" in *":'"$d"':"*) : ;; *) PATH="$PATH:'"$d"'" ;; esac'
      done
      echo "export PATH"
      echo ""
      echo "# --- Python module path ---"
      echo '[ -d "$DEFORMA_SOURCE" ] && case ":$PYTHONPATH:" in *":$DEFORMA_SOURCE:"*) : ;; *) PYTHONPATH="$PYTHONPATH:$DEFORMA_SOURCE" ;; esac'
      echo "export PYTHONPATH"
      echo ""
      echo "# --- Aliases ---"
      echo "alias deforma-root='cd \"\$DEFORMA_PREFIX\"'"
      echo "alias deforma-src='cd \"\$DEFORMA_SOURCE\"'"
      echo "alias deforma-cfg='cd \"\$DEFORMA_CONFIG\"'"
      echo "alias deforma-web='cd \"\$DEFORMA_WEB\"'"
      echo "alias deforma-docs='cd \"\$DEFORMA_DOCS\"'"
      echo "alias db_view='python3 \"\$DEFORMA_SOURCE/database/db_view.py\"'"
      echo "alias db_update='python3 \"\$DEFORMA_SOURCE/database/db_update.py\"'"
      echo "alias deforma-alert='python3 \"\$DEFORMA_SOURCE/alert/static_alert.py\"'"
      echo "alias deforma-webapp='python3 \"\$DEFORMA_SOURCE/webapp/app.py\"'"
      echo "$MARK_END"
    } >> "$LOCAL_RC"
    echo "Persisted DeforMA environment to $LOCAL_RC"
  else
    echo "DeforMA block already present in $LOCAL_RC (skipped)."
  fi
fi

# -------- Summary -------------------------------------------
echo "DeforMA environment ready."
echo "  DEFORMA_PREFIX = $DEFORMA_PREFIX"
echo "  DEFORMA_SOURCE = $DEFORMA_SOURCE"
echo "  DEFORMA_DB     = $DEFORMA_DB"
echo "  PYTHONPATH     = $PYTHONPATH"

