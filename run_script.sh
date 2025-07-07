#!/bin/bash

# Set the timezone to New York for accurate market close time
export TZ="America/New_York"

echo "TICKER_OVERRIDE_LIST received in shell script: $TICKER_OVERRIDE_LIST" # Add this line for debugging

echo "Starting script at $(date)"

# Check for the RUN_IMMEDIATELY environment variable
# If set to "true" (or any non-empty value), bypass all time checks and run immediately.
if [ "$RUN_IMMEDIATELY" = "true" ]; then
    echo "RUN_IMMEDIATELY is set to true. Running the script now, bypassing time checks."
    python /app/main.py --override-tickers $TICKER_OVERRIDE_LIST
    echo "Script finished (immediate run) at $(date)"
    exit 0 # Exit after immediate run
fi

# Get the current day of the week (1=Monday, 7=Sunday)
DAY_OF_WEEK=$(date +%u)

# Get the current hour and minute
CURRENT_HOUR=$(date +%H)
CURRENT_MINUTE=$(date +%M)

# Market close time is 16:00 (4:00 PM) EST.
# We want to run 30 minutes after market close, so 16:30 (4:30 PM) EST.
MARKET_CLOSE_HOUR=16
MARKET_CLOSE_MINUTE=00 # Market close
RUN_HOUR=16 # 4 PM
RUN_MINUTE=30 # 30 minutes after 4 PM

# Check if it's a weekday (Monday=1 to Friday=5)
if (( DAY_OF_WEEK >= 1 && DAY_OF_WEEK <= 5 )); then
    echo "It's a weekday."
    # Calculate current time in minutes since midnight
    CURRENT_TIME_IN_MINUTES=$(( CURRENT_HOUR * 60 + CURRENT_MINUTE ))
    # Calculate target run time in minutes since midnight
    TARGET_TIME_IN_MINUTES=$(( RUN_HOUR * 60 + RUN_MINUTE ))

    if (( CURRENT_TIME_IN_MINUTES >= TARGET_TIME_IN_MINUTES )); then
        echo "It's already past 4:30 PM EST. Running the script now."
        python /app/main.py
    else
        echo "It's before 4:30 PM EST. Waiting until 4:30 PM EST."
        # Calculate time to wait in seconds
        MINUTES_TO_WAIT=$(( TARGET_TIME_IN_MINUTES - CURRENT_TIME_IN_MINUTES ))
        SECONDS_TO_WAIT=$(( MINUTES_TO_WAIT * 60 ))

        echo "Waiting for $MINUTES_TO_WAIT minutes ($SECONDS_TO_WAIT seconds)..."
        sleep $SECONDS_TO_WAIT
        echo "Wake up! Running the script now."
        python /app/main.py
    fi
else
    echo "It's the weekend. Not running the script."
fi

echo "Script finished at $(date)"