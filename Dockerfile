# Use a slim Python base image
FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python script and the shell script into the container
COPY main.py .
COPY run_script.sh .
COPY company_tickers.json .

# Give execute permissions to the shell script
RUN chmod +x run_script.sh

# Set the entrypoint to the shell script
# This ensures that when the container starts, run_script.sh is executed.
ENTRYPOINT ["./run_script.sh"]

# Note: The 'data' directory (and its 'daily_ohlcv' sub-directory)
# will be provided as a volume mount from the host system at runtime.
# Therefore, we do not need to create it within the Docker image itself.