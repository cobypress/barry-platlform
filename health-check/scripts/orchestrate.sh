#!/usr/bin/env bash
# Runs the health check's orchestrator unattended (docs/orchestration.md): a
# fresh /coordinate session after each one ends, with the guard hook on every
# tool call. Start it from anywhere: scripts/orchestrate.sh
#
# Stop after the current session:  touch "$LEDGER/STOP"
# While $LEDGER/WAITING-FOR-COBY.md exists nothing starts; delete it to resume.
set -uo pipefail

ENGINE="$(cd "$(dirname "$0")/.." && pwd)"
LEDGER="${HC_LEDGER:-$HOME/Desktop/Black Cloud/orchestration/health-check}"
LOGS="$LEDGER/logs"
mkdir -p "$LOGS"

notify() {
  osascript -e "display notification \"$1\" with title \"Health check orchestrator\" sound name \"Glass\"" >/dev/null 2>&1
  # An alert stays until it's dismissed; in the background so the loop doesn't wait on it.
  osascript -e "display alert \"Health check orchestrator\" message \"$1\"" >/dev/null 2>&1 &
}

failures=0
while true; do
  if [ -f "$LEDGER/STOP" ]; then
    echo "$(date +%T) STOP found in $LEDGER. Delete it to run again."
    exit 0
  fi
  if [ -f "$LEDGER/WAITING-FOR-COBY.md" ]; then
    notify "Waiting for you: see WAITING-FOR-COBY.md in orchestration/health-check"
    echo "$(date +%T) waiting for Coby; checking again in an hour"
    sleep 3600
    continue
  fi

  log="$LOGS/session-$(date +%Y%m%d-%H%M%S).jsonl"
  echo "$(date +%T) starting a session, raw log $log"
  (
    cd "$ENGINE" &&
      CLAUDE_CODE_PRINT_BG_WAIT_CEILING_MS=0 caffeinate -dims \
        claude -p "/coordinate" \
        --model fable \
        --permission-mode auto \
        --permission-prompts none \
        --settings "$ENGINE/scripts/orchestrate-settings.json" \
        --output-format stream-json --verbose
  ) | tee "$log" | node "$ENGINE/scripts/orchestrate-view.ts"
  status=${PIPESTATUS[0]}

  if [ "$status" -ne 0 ]; then
    failures=$((failures + 1))
    echo "$(date +%T) session failed (exit $status), $failures in a row"
  else
    failures=0
  fi
  if [ "$failures" -ge 3 ]; then
    cat >"$LEDGER/WAITING-FOR-COBY.md" <<EOF
# Waiting for Coby

Three orchestrator sessions in a row failed, the last at $(date '+%Y-%m-%d %H:%M').
The raw logs are in $LOGS. Read the newest session log, fix what stopped it,
then delete this file to start again.
EOF
    failures=0
  fi
  sleep 30
done
