#!/bin/bash

set -euo pipefail

# Configuration defaults
export TZ="America/New_York"
RUN_HOUR=${RUN_HOUR:-16}
RUN_MINUTE=${RUN_MINUTE:-45}
WEEKEND_SLEEP_SECONDS=${WEEKEND_SLEEP_SECONDS:-21600}  # 6 hours

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] $*"
}

log "TICKER_OVERRIDE_LIST received in shell script: ${TICKER_OVERRIDE_LIST:-<none>}"
log "Starting script"

log "Scheduled run time set to ${RUN_HOUR}:${RUN_MINUTE} (24-hour format)"

IMMEDIATE_RUN_DONE=false
LAST_RUN_DAY=0  # Track the last weekday number the job was run

if [[ "${RUN_IMMEDIATELY:-false}" == "true" ]]; then
    log "RUN_IMMEDIATELY is set to true. Running the script immediately."
    python /app/main.py --override-tickers "${TICKER_OVERRIDE_LIST:-}"
    log "Initial immediate run complete"
    IMMEDIATE_RUN_DONE=true
fi

while true; do
    DAY_OF_WEEK=$(date +%u)      # 1 (Mon) - 7 (Sun)
    CURRENT_HOUR=$(date +%H)
    CURRENT_MINUTE=$(date +%M)

    if (( DAY_OF_WEEK >= 1 && DAY_OF_WEEK <= 5 )); then
        log "Weekday detected (day $DAY_OF_WEEK). Checking time..."

        CURRENT_TIME_IN_MINUTES=$((10#$CURRENT_HOUR * 60 + 10#$CURRENT_MINUTE))
        TARGET_TIME_IN_MINUTES=$((10#$RUN_HOUR * 60 + 10#$RUN_MINUTE))

        # Run only if current time is >= target time and we haven't run today yet
        if (( CURRENT_TIME_IN_MINUTES >= TARGET_TIME_IN_MINUTES )) && (( DAY_OF_WEEK != LAST_RUN_DAY )); then
            log "It's past scheduled time (${RUN_HOUR}:${RUN_MINUTE}) and job not run yet today. Running script now."
            python /app/main.py
            log "Scheduled run complete"
            LAST_RUN_DAY=$DAY_OF_WEEK

            # Sleep until next day (24h)
            log "Sleeping for 24 hours until next run window..."
            sleep 86400
        else
            # Calculate wait time until scheduled time
            if (( CURRENT_TIME_IN_MINUTES < TARGET_TIME_IN_MINUTES )); then
                MINUTES_TO_WAIT=$(( TARGET_TIME_IN_MINUTES - CURRENT_TIME_IN_MINUTES ))
                SECONDS_TO_WAIT=$(( MINUTES_TO_WAIT * 60 ))
                log "Waiting for $MINUTES_TO_WAIT minutes ($SECONDS_TO_WAIT seconds) until ${RUN_HOUR}:${RUN_MINUTE} EST..."
                sleep $SECONDS_TO_WAIT
            else
                # Already ran today, wait until tomorrow morning to recheck
                TIME_TO_MIDNIGHT=$(( (24*60) - CURRENT_TIME_IN_MINUTES ))
                SECONDS_TO_WAIT=$(( TIME_TO_MIDNIGHT * 60 ))
                log "Job already run today. Sleeping until tomorrow (${SECONDS_TO_WAIT} seconds)..."
                sleep $SECONDS_TO_WAIT
                LAST_RUN_DAY=0
            fi
        fi
    else
        log "Weekend detected (day $DAY_OF_WEEK). Sleeping for configured duration (${WEEKEND_SLEEP_SECONDS} seconds)..."
        sleep $WEEKEND_SLEEP_SECONDS
    fi
done
