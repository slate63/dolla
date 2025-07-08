import yfinance as yf
import pandas as pd
import os
import pyarrow.parquet as pq
import datetime
import requests
import json
import time
import random
import argparse
import logging

# --- Configuration ---
SEC_TICKERS_URL = "http://sec.gov/files/company_tickers.json"
LOCAL_TICKERS_FILE = "company_tickers.json" # New constant for local file
DEFAULT_OUTPUT_DIRECTORY = '/data/daily_ohlcv'
GLOBAL_START_DATE = '1990-01-01'

MIN_FETCH_DELAY_SECONDS = 15
MAX_FETCH_DELAY_SECONDS = 30
STOCKS_PER_BUNCH = 10

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_and_clean_ohlcv(ticker: str,
                         start: str,
                         end: str = datetime.datetime.now().strftime('%Y-%m-%d'),
                         interval: str = '1d') -> pd.DataFrame:
    """
    Download OHLCV data including dividends and splits from Yahoo via ticker.history().

    Args:
        ticker (str): The stock ticker symbol.
        start (str): The start date for data fetching in 'YYYY-MM-DD' format.
        end (str): The end date for data fetching in 'YYYY-MM-DD' format. Defaults to today.
        interval (str): The data interval (e.g., '1d' for daily).

    Returns:
        pd.DataFrame: A DataFrame containing the cleaned OHLCV data.
    """
    
    # Ensure start date is not after end date
    if datetime.datetime.strptime(start, '%Y-%m-%d') > datetime.datetime.strptime(end, '%Y-%m-%d'):
        logging.warning(f"Start date {start} is after end date {end} for {ticker}. Returning empty DataFrame.")
        return pd.DataFrame()

    try:
        yf_ticker = yf.Ticker(ticker)
        # Fetch historical data, including actions (dividends, splits)
        df = yf_ticker.history(start=start, end=end, interval=interval, actions=True, auto_adjust=False)
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()

    if df.empty:
        logging.warning(f"No data returned for {ticker} within the range {start} to {end}.")
        return pd.DataFrame()

    df.reset_index(inplace=True)

    # Standardize column names to lowercase and strip whitespace
    df.columns = [str(col).strip().lower() for col in df.columns]

    # Rename date/datetime column to 'timestamp'
    if 'date' in df.columns:
        df.rename(columns={'date': 'timestamp'}, inplace=True)
    elif 'datetime' in df.columns:
        df.rename(columns={'datetime': 'timestamp'}, inplace=True)
    else:
        raise ValueError(f"'date' or 'datetime' column not found for {ticker}. Check yfinance output format.")

    # Ensure 'dividends' and 'stock splits' columns exist
    if 'dividends' not in df.columns:
        df['dividends'] = 0.0
    if 'stock splits' not in df.columns:
        df['stock splits'] = 0.0

    # Add the ticker symbol column
    df['symbol'] = ticker

    # Define required columns, including the new moving average columns
    required_columns = [
        'timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume',
        'dividends', 'stock splits', 'sma_50', 'sma_200'
    ]
    
    # Check for missing required columns before adding MAs (MAs will be added later)
    # This check will be more relevant after MA calculation.
    # For now, ensure base OHLCV columns are present.
    base_ohlcv_columns = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock splits']
    missing_columns = set(base_ohlcv_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing base OHLCV columns for {ticker}: {missing_columns}")

    return df # Return df here, MAs will be added in a separate step


def add_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates 50-day and 200-day Simple Moving Averages (SMA) for the 'close' price.

    Args:
        df (pd.DataFrame): DataFrame with OHLCV data, must contain a 'close' column.

    Returns:
        pd.DataFrame: The DataFrame with 'sma_50' and 'sma_200' columns added.
    """
    if df.empty:
        return df

    # Ensure the DataFrame is sorted by timestamp for correct rolling calculations
    df = df.sort_values(by='timestamp').copy()

    # Calculate 50-day Simple Moving Average
    # min_periods=1 allows calculation even if not enough data for full window
    df['sma_50'] = df['close'].rolling(window=50, min_periods=1).mean()

    # Calculate 200-day Simple Moving Average
    df['sma_200'] = df['close'].rolling(window=200, min_periods=1).mean()
    
    return df


def downcast_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Downcasts numeric columns to more memory-efficient types.

    Args:
        df (pd.DataFrame): The DataFrame to downcast.

    Returns:
        pd.DataFrame: The DataFrame with downcasted types.
    """
    if df.empty:
        return df
    
    # Convert timestamp to datetime if not already
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])

    for col in ['open', 'high', 'low', 'close', 'dividends', 'stock splits', 'sma_50', 'sma_200']:
        if col in df.columns: # Check if column exists before trying to downcast
            df[col] = pd.to_numeric(df[col], downcast='float', errors='coerce')
    
    if 'volume' in df.columns:
        df['volume'] = pd.to_numeric(df['volume'], downcast='integer', errors='coerce').astype('Int64')
    
    if 'symbol' in df.columns:
        df['symbol'] = df['symbol'].astype('category')
    
    return df


def get_sec_tickers(url: str, local_file: str) -> list:
    """
    Fetches stock tickers from the SEC website or a local file as a fallback.

    Args:
        url (str): The URL to fetch SEC tickers from.
        local_file (str): The path to the local JSON file for fallback.

    Returns:
        list: A list of uppercase ticker symbols.
    """
    logging.info(f"Attempting to fetch tickers from SEC: {url}")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        logging.info("Successfully fetched tickers from SEC online.")
        return [v.get('ticker').upper() for v in data.values() if v.get('ticker')]
    except requests.exceptions.RequestException as e:
        logging.warning(f"Failed to fetch SEC tickers from online source ({e}). Attempting to load from local file: {local_file}")
        if os.path.exists(local_file):
            try:
                with open(local_file, 'r') as f:
                    data = json.load(f)
                logging.info(f"Successfully loaded tickers from local file: {local_file}")
                return [v.get('ticker').upper() for v in data.values() if v.get('ticker')]
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse JSON from local file {local_file}: {e}")
                return []
            except Exception as e:
                logging.error(f"Failed to load tickers from local file {local_file}: {e}")
                return []
        else:
            logging.warning(f"Local ticker file not found: {local_file}")
            return []


def update_daily_ohlcv_all_at_once(user_ticker_list: list, output_dir: str, global_start_date: str, override_ticker_list: list = None):
    """
    Updates daily OHLCV data for a list of tickers, saving them to Parquet files.

    Args:
        user_ticker_list (list): A list of user-provided tickers.
        output_dir (str): The directory to save the Parquet files.
        global_start_date (str): The earliest date to fetch data from if no existing data.
        override_ticker_list (list, optional): A list of tickers to fetch, overriding others. Defaults to None.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Determine the final list of tickers to process
    tickers = sorted(set(override_ticker_list if override_ticker_list else user_ticker_list + get_sec_tickers(SEC_TICKERS_URL, LOCAL_TICKERS_FILE)))
    if not tickers:
        logging.info("No tickers to process. Exiting.")
        return

    logging.info(f"Processing {len(tickers)} tickers")
    ticker_last_dates = {}
    existing_files = sorted([f for f in os.listdir(output_dir) if f.endswith('.parquet')])
    today = datetime.datetime.now().date()

    # Determine the last fetched date for each ticker from existing files
    for fname in existing_files:
        date_str = fname.replace('.parquet', '')
        try:
            fdate = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            if fdate > today: # Skip future dated files
                continue
            # Read only necessary columns to save memory
            df_meta = pd.read_parquet(os.path.join(output_dir, fname), columns=['timestamp', 'symbol'])
            if not df_meta.empty:
                for symbol, ts in df_meta.groupby('symbol')['timestamp'].max().to_dict().items():
                    # Update last date only if it's newer
                    if symbol not in ticker_last_dates or ts > ticker_last_dates[symbol]:
                        ticker_last_dates[symbol] = ts
        except Exception as e:
            logging.warning(f"Skipping {fname} due to error: {e}")

    fetch_requests = {}
    today_str = today.strftime('%Y-%m-%d')
    for ticker in tickers:
        last_ts = ticker_last_dates.get(ticker)
        if last_ts:
            # Start fetching from the day after the last recorded timestamp
            next_day = (last_ts + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            if next_day <= today_str:
                fetch_requests[ticker] = next_day
        else:
            # If no existing data, fetch from the global start date
            fetch_requests[ticker] = global_start_date

    # Filter out tickers that are already up-to-date
    fetch_list = sorted([t for t, s in fetch_requests.items() if s <= today_str])
    if not fetch_list:
        logging.info("All tickers up-to-date. Exiting.")
        return

    all_data = []
    # Process tickers in bunches with delays
    for i in range(0, len(fetch_list), STOCKS_PER_BUNCH):
        bunch = fetch_list[i:i + STOCKS_PER_BUNCH]
        logging.info(f"\nProcessing bunch: {bunch}")
        for ticker in bunch:
            try:
                # Load and clean OHLCV data
                df = load_and_clean_ohlcv(ticker, fetch_requests[ticker], today_str)
                if not df.empty:
                    # Add moving averages
                    df_with_ma = add_moving_averages(df)
                    # Downcast data types for memory efficiency
                    all_data.append(downcast_ohlcv(df_with_ma))
            except Exception as e:
                logging.error(f"Failed to process {ticker}: {e}")
        
        # Introduce random delay between bunches to respect API limits
        if (i + STOCKS_PER_BUNCH) < len(fetch_list):
            delay = random.randint(MIN_FETCH_DELAY_SECONDS, MAX_FETCH_DELAY_SECONDS)
            logging.info(f"Pausing for {delay} seconds before next bunch...")
            time.sleep(delay)

    if not all_data:
        logging.info("No new data fetched. Exiting.")
        return

    # Concatenate all fetched data and prepare for saving
    final_data = pd.concat(all_data, ignore_index=True)
    # Ensure timestamp is datetime type before strftime
    final_data['timestamp'] = pd.to_datetime(final_data['timestamp'])
    final_data['date_str'] = final_data['timestamp'].dt.strftime('%Y-%m-%d')
    grouped = final_data.groupby('date_str')

    # Save/update data to daily Parquet files
    for date_str, group in grouped:
        path = os.path.join(output_dir, f"{date_str}.parquet")
        existing = pd.DataFrame()
        if os.path.exists(path):
            try:
                existing = pd.read_parquet(path)
            except Exception as e:
                logging.warning(f"Unable to read existing {path} ({e}), treating as empty.")
        
        # Combine existing and new data, remove duplicates, and filter by current tickers
        combined = pd.concat([existing, group.drop(columns=['date_str'])], ignore_index=True)
        combined.drop_duplicates(subset=['timestamp', 'symbol'], keep='last', inplace=True)
        # Filter to only include tickers that are part of the current run's `tickers` list
        combined = combined[combined['symbol'].isin(tickers)].sort_values(by=['timestamp', 'symbol'])
        
        if not combined.empty:
            # Save the combined data to Parquet
            combined.to_parquet(path, engine='pyarrow', compression='snappy', index=False)
            logging.info(f"Saved {path} with {len(combined)} rows.")
        elif os.path.exists(path):
            # Remove empty Parquet files
            os.remove(path)
            logging.info(f"Removed empty {path}.")


if __name__ == '__main__':
    def main():
        parser = argparse.ArgumentParser(description="Download daily OHLCV stock data with dividends and splits, including moving averages.")
        parser.add_argument('--override-tickers', type=str, help='Comma-separated list of tickers to fetch.')
        parser.add_argument('--output-dir', type=str, help=f'Output directory. Defaults to "{DEFAULT_OUTPUT_DIRECTORY}".')
        args = parser.parse_args()

        override = [t.strip().upper() for t in args.override_tickers.split(',')] if args.override_tickers else None
        out_dir = args.output_dir if args.output_dir else DEFAULT_OUTPUT_DIRECTORY

        update_daily_ohlcv_all_at_once([], out_dir, GLOBAL_START_DATE, override_ticker_list=override)
        logging.info("\n--- Update complete ---")

    main()
