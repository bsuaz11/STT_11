#!/usr/bin/env bash
# scripts/run_week8.sh — the week-8 live block (Fri Nov 20, 15:30–16:30 CST).
#
#   scripts/run_week8.sh                      # trader_1 + broker_1, every venue
#   SEATS="stt_11_trader_1" scripts/run_week8.sh
#
# trader_1  roles signal, probe, ipo, leadlag; connects to the primary and every
#           student venue (TRADER_VENUES=auto) and routes each IOC to the best
#           price after fees (team/venues.py). The LUNA bid goes to the primary.
# broker_1  quotes every venue (BROKER_VENUES=auto) at a width that covers each
#           venue's maker fee, and stands aside where makers pay (the primary).
#
# Each seat restarts if it dies, the Mac stays awake, and logs go to
# Phase4/logs/<start time>/<seat>.log. The address comes from .env (hosted
# arena) unless EXCHANGE_URL is set in the shell. Ctrl-C stops everything.
# Manual kill switch at any time: python scripts/kill_switch.py on [--flatten]
set -u
cd "$(dirname "$0")/.." || exit 1

VENV="${VENV:-../.venv}"
[ -f "$VENV/bin/activate" ] && . "$VENV/bin/activate"
export PYTHONPATH=engine:.
export PYTHONUNBUFFERED=1
export TRADER_VENUES="${TRADER_VENUES:-auto}"
export BROKER_VENUES="${BROKER_VENUES:-auto}"

SEATS="${SEATS:-stt_11_trader_1 stt_11_broker_1}"
RESTART_DELAY="${RESTART_DELAY:-3}"
LOGDIR="${LOGDIR:-Phase4/logs/$(date +%Y%m%d_%H%M%S)}"
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
