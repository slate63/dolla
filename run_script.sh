#!/bin/bash

# Set the timezone to New York for accurate market close time
export TZ="America/New_York"

echo "Starting script at $(date)"

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
    # Check if the current time is after the desired run time (16:30 EST)
    # And also check if we are *not* past midnight of the same day.
    # This prevents the script from running repeatedly if the container restarts after 4:30 PM but before midnight.

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
        # First, calculate minutes to wait
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