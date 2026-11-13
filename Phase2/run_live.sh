#!/usr/bin/env bash
# Phase2/run_live.sh — run the team's seats for a live block and keep them up.
#
#   Phase2/run_live.sh                       # trader_1, broker_1 (week 7 lineup)
#   SEATS="stt_11_trader_1" Phase2/run_live.sh
#
# Each seat restarts if its process ever dies, the Mac is kept awake, and every
# seat logs to Phase2/logs/<start time>/<seat>.log, which Phase2/live_compare.py
# reads afterwards. The address comes from .env, so this dials the hosted arena
# unless EXCHANGE_URL is set in the shell. Ctrl-C stops everything.
set -u
cd "$(dirname "$0")/.." || exit 1

VENV="${VENV:-../.venv}"
[ -f "$VENV/bin/activate" ] && . "$VENV/bin/activate"
export PYTHONPATH=engine:.
export PYTHONUNBUFFERED=1

SEATS="${SEATS:-stt_11_trader_1 stt_11_broker_1}"   # add trader_2 only with SIGNAL_SEATS=2
RESTART_DELAY="${RESTART_DELAY:-3}"
LOGDIR="${LOGDIR:-Phase2/logs/$(date +%Y%m%d_%H%M%S)}"
mkdir -p "$LOGDIR"
echo "Logs: $LOGDIR"

run_seat() {
    seat="$1"
    case "$seat" in
        *broker*) module=team.broker ;;
        *)        module=team.trader ;;
    esac
    while true; do
        echo "$(date '+%H:%M:%S')  SUPERVISOR start $seat" >> "$LOGDIR/$seat.log"
        TEAM_ID="$seat" python -m "$module" >> "$LOGDIR/$seat.log" 2>&1 < /dev/null
        code=$?
        echo "$(date '+%H:%M:%S')  SUPERVISOR $seat exited with $code — restarting in ${RESTART_DELAY}s" \
            >> "$LOGDIR/$seat.log"
        sleep "$RESTART_DELAY"
    done
}

trap 'trap - TERM INT; kill 0' TERM INT EXIT
command -v caffeinate > /dev/null && caffeinate -dimsu -w $$ &

for seat in $SEATS; do
    run_seat "$seat" &
    echo "started $seat"
done
wait
