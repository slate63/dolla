#!/bin/bash

# Set the timezone to New York for accurate market close time
export TZ="America/New_York"

echo "TICKER_OVERRIDE_LIST received in shell script: $TICKER_OVERRIDE_LIST"
echo "Starting script at $(date)"

# Flag to track if we already did the immediate run
IMMEDIATE_RUN_DONE=false

# Run immediately if requested
if [ "$RUN_IMMEDIATELY" = "true" ]; then
    echo "RUN_IMMEDIATELY is set to true. Running the script immediately."
    python /app/main.py --override-tickers "$TICKER_OVERRIDE_LIST"
    echo "Initial immediate run complete at $(date)"
    IMMEDIATE_RUN_DONE=true
fi

# Now continue to loop daily for scheduled runs
while true; do
    # Get current day, hour, and minute
    DAY_OF_WEEK=$(date +%u)
    CURRENT_HOUR=$(date +%H)
    CURRENT_MINUTE=$(date +%M)

    # Define run time (4:45 PM EST)
    RUN_HOUR=16
    RUN_MINUTE=45

    if (( 10#$DAY_OF_WEEK >= 1 && 10#$DAY_OF_WEEK <= 5 )); then
        echo "It's a weekday. Checking time..."

        CURRENT_TIME_IN_MINUTES=$(( 10#$CURRENT_HOUR * 60 + 10#$CURRENT_MINUTE ))
        TARGET_TIME_IN_MINUTES=$(( RUN_HOUR * 60 + RUN_MINUTE ))

        if (( CURRENT_TIME_IN_MINUTES >= TARGET_TIME_IN_MINUTES )); then
            echo "It's past 4:45 PM EST. Running the script now."
            python /app/main.py
            echo "Scheduled run complete at $(date)"

            # Sleep until next day
            echo "Sleeping until tomorrow..."
            sleep 86400 # 24 hours
        else
            MINUTES_TO_WAIT=$(( TARGET_TIME_IN_MINUTES - CURRENT_TIME_IN_MINUTES ))
            SECONDS_TO_WAIT=$(( MINUTES_TO_WAIT * 60 ))
            echo "Waiting for $MINUTES_TO_WAIT minutes ($SECONDS_TO_WAIT seconds) until 4:30 PM EST..."
            sleep $SECONDS_TO_WAIT
        fi
    else
        echo "It's the weekend. Sleeping for 6 hours before checking again..."
        sleep 21600
    fi
done
