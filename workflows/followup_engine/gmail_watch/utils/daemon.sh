#!/usr/bin/env bash
set -euo pipefail

# === gmail_watch daemon ===
# Path: /Users/kevinnovanta/backend_for_ai_agency/workflows/followup_engine/gmail_watch/utils/daemon.sh
# Version: 2025-08-17-03
# Usage:
#   cd /Users/kevinnovanta/backend_for_ai_agency && export PYTHONPATH=$PWD
#   workflows/followup_engine/gmail_watch/utils/daemon.sh start|start-tail|stop|status|restart

# --- Paths ---------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

# Command to run the watcher (Python module); ensure PYTHONPATH points to repo root
CMD=( env PYTHONPATH="$REPO_ROOT" python3 -m workflows.followup_engine.gmail_watch.main )

# --- Files / PIDs / Logs ------------------------------------------------
PID_FILE="/tmp/gmail_watch.pid"
LOG_FILE="/tmp/gmail_watch.log"
ROT_PID_FILE="/tmp/gmail_watch_rot.pid"

# Log rotation settings
LOG_MAX_LINES=10000
LOG_TRIM_TO=5000
LOG_CHECK_INTERVAL=30

# --- Helpers ------------------------------------------------------------
# returns 0 if PID is alive, 1 otherwise
is_running() {
  local pid=${1:-}
  if [[ -z "${pid}" ]]; then return 1; fi
  if ps -p "$pid" > /dev/null 2>&1; then return 0; else return 1; fi
}

# remove stale PID file if process is not alive
cleanup_stale_pid() {
  local file="$1"
  if [[ -f "$file" ]]; then
    local pid
    pid=$(cat "$file" 2>/dev/null || true)
    if ! is_running "$pid"; then
      echo "[daemon] removing stale PID file $file (pid=$pid not running)"
      rm -f "$file"
    fi
  fi
}

start_rotator() {
  cleanup_stale_pid "$ROT_PID_FILE"
  if [[ -f "$ROT_PID_FILE" ]] && is_running "$(cat "$ROT_PID_FILE" 2>/dev/null)"; then
    echo "Log rotator already running (PID $(cat "$ROT_PID_FILE"))"
    return 0
  fi
  (
    trap "exit 0" TERM INT
    while true; do
      if [[ -f "$LOG_FILE" ]]; then
        local lines
        lines=$(wc -l < "$LOG_FILE" | tr -d ' ')
        if [[ "${lines:-0}" -gt "$LOG_MAX_LINES" ]]; then
          echo "[rotator] trimming log ($lines > $LOG_MAX_LINES)"
          # keep last LOG_TRIM_TO lines
          tail -n "$LOG_TRIM_TO" "$LOG_FILE" > "$LOG_FILE.tmp" 2>/dev/null && mv "$LOG_FILE.tmp" "$LOG_FILE"
        fi
      fi
      sleep "$LOG_CHECK_INTERVAL"
    done
  ) &
  echo $! > "$ROT_PID_FILE"
  echo "Started log rotator (PID $(cat "$ROT_PID_FILE"))."
}

stop_rotator() {
  if [[ -f "$ROT_PID_FILE" ]]; then
    local rpid
    rpid=$(cat "$ROT_PID_FILE" 2>/dev/null || true)
    if is_running "$rpid"; then
      kill -TERM "$rpid" 2>/dev/null || true
      sleep 0.2
    fi
    rm -f "$ROT_PID_FILE"
  fi
}

print_status() {
  if [[ -f "$PID_FILE" ]]; then
    local wpid
    wpid=$(cat "$PID_FILE" 2>/dev/null || true)
    if is_running "$wpid"; then
      echo "Gmail watcher is running (PID $wpid)."
    else
      echo "Gmail watcher PID file exists but process not running. Cleaning up…"
      rm -f "$PID_FILE"
      wpid=""
    fi
  else
    echo "Gmail watcher is not running."
  fi

  if [[ -f "$ROT_PID_FILE" ]]; then
    local rpid
    rpid=$(cat "$ROT_PID_FILE" 2>/dev/null || true)
    if is_running "$rpid"; then
      echo "Log rotator is running (PID $rpid)."
    else
      echo "Log rotator PID file exists but process not running."
    fi
  else
    echo "Log rotator is not running."
  fi

  if [[ -f "$LOG_FILE" ]]; then
    echo "Log lines: $(wc -l < "$LOG_FILE" | tr -d ' ')"
    echo "Log file: $LOG_FILE"
  else
    echo "Log file not found: $LOG_FILE"
  fi
}

usage() {
  echo "Usage: $0 {start|start-tail|stop|status|restart}"
}

case "${1:-}" in
  start|start-tail)
    cleanup_stale_pid "$PID_FILE"
    start_rotator
    if [[ -f "$PID_FILE" ]] && is_running "$(cat "$PID_FILE" 2>/dev/null)"; then
      echo "Daemon already running (PID $(cat "$PID_FILE"))."
      exit 0
    fi
    echo "Starting Gmail watcher…"
    # Ensure log file exists
    touch "$LOG_FILE"
    nohup "${CMD[@]}" >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Started with PID $(cat "$PID_FILE")."

    # optional auto-tail: start-tail subcommand or env TAIL_AFTER_START=1
    if [[ "${1}" = "start-tail" || "${TAIL_AFTER_START:-0}" = "1" ]]; then
      echo "[daemon] tailing log (Ctrl-C to stop tail; daemon continues running): $LOG_FILE"
      tail -n 200 -f "$LOG_FILE"
    fi
    ;;
  stop)
    if [[ -f "$PID_FILE" ]]; then
      wpid=$(cat "$PID_FILE" 2>/dev/null || true)
      if is_running "$wpid"; then
        echo "Stopping Gmail watcher (PID $wpid)…"
        kill -TERM "$wpid" 2>/dev/null || true
        sleep 0.3
      else
        echo "Watcher not running; cleaning stale PID file."
      fi
      rm -f "$PID_FILE"
    else
      echo "Daemon not running."
    fi
    stop_rotator
    ;;
  status)
    print_status
    ;;
  restart)
    "$0" stop
    "$0" start
    ;;
  *)
    usage
    exit 1
    ;;
esac