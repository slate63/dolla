import yfinance as yf # Import yfinance
import pandas as pd # Import pandas
import os
import pyarrow.parquet as pq # Import pyarrow
import datetime
import requests
import json
import time
import random
import argparse # Import argparse

# --- Configuration ---
SEC_TICKERS_URL = "http://sec.gov/files/company_tickers.json"
# OUTPUT_DIRECTORY will now be determined at runtime, defaulting if not set
DEFAULT_OUTPUT_DIRECTORY = 'data/daily_ohlcv' # Changed to relative path for consistency with volume mount
GLOBAL_START_DATE = '1990-01-01' # The earliest date to consider downloading data from

# Constants for random behavior (still useful for API rate limiting)
MIN_FETCH_DELAY_SECONDS = 15
MAX_FETCH_DELAY_SECONDS = 30
STOCKS_PER_BUNCH = 10 # Number of stocks to fetch in one bunch before pausing


# --- 1. Download, clean, and downcast functions ---

def load_and_clean_ohlcv(ticker: str,
                         start: str,
                         end: str = datetime.datetime.now().strftime('%Y-%m-%d'),
                         interval: str = '1d') -> pd.DataFrame:
    """Download OHLCV from Yahoo, flatten columns, and return clean DataFrame."""
    
    if datetime.datetime.strptime(start, '%Y-%m-%d') > datetime.datetime.strptime(end, '%Y-%m-%d'):
        print(f"Warning: Start date {start} is after end date {end} for {ticker}. Returning empty DataFrame.")
        return pd.DataFrame()

    df = yf.download(
        ticker,
        start=start,
        end=end,
        interval=interval,
        progress=False,
        auto_adjust=False, # Keep False to get 'Dividends' and 'Stock Splits' explicitly
        group_by='column',
        threads=False
    )
    if df.empty:
        print(f"Warning: No data returned for {ticker} within the range {start} to {end}. This might be normal for very new tickers, if the symbol is incorrect, or no new data is available yet.")
        return pd.DataFrame()

    time.sleep(1)

    df.reset_index(inplace=True)

    df.columns = [
        str(col[0]).strip().lower() if isinstance(col, tuple) else str(col).strip().lower()
        for col in df.columns
    ]

    if 'date' in df.columns:
        df.rename(columns={'date': 'timestamp'}, inplace=True)
    else:
        raise ValueError(f"'date' column not found for {ticker}. Check yfinance output format.")

    # Define all required columns including dividends and stock splits
    required_columns = {'timestamp', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock splits'}
    
    # Check for missing essential OHLCV columns first
    ohlcv_core = {'timestamp', 'open', 'high', 'low', 'close', 'volume'}
    missing_ohlcv = ohlcv_core - set(df.columns)
    if missing_ohlcv:
        raise ValueError(f"Missing essential OHLCV columns for {ticker}: {missing_ohlcv}. Columns found: {df.columns.tolist()}")

    # Add 'dividends' and 'stock splits' if they are not present, filling with 0.0
    # They might not be present if no such events occurred in the downloaded period
    if 'dividends' not in df.columns:
        df['dividends'] = 0.0
    if 'stock splits' not in df.columns:
        df['stock splits'] = 0.0

    df['symbol'] = ticker
    
    # Select and return the desired columns in a specific order
    return df[['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock splits']]


def downcast_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast numeric columns for memory efficiency."""
    if df.empty:
        return df
    for col in ['open', 'high', 'low', 'close', 'dividends', 'stock splits']: # Added dividends and stock splits
        df[col] = pd.to_numeric(df[col], downcast='float')
    df['volume'] = pd.to_numeric(df['volume'], downcast='integer', errors='coerce').astype('Int64')
    df['symbol'] = df['symbol'].astype('category')
    return df

# --- Fetch SEC Tickers ---
def get_sec_tickers(url: str) -> list:
    """Fetches and parses the SEC company_tickers.json list."""
    print(f"Attempting to fetch tickers from SEC: {url}")
    try:
        headers = {'User-Agent': 'StockDataLoader/1.0 (contact@example.com)'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        sec_tickers = []
        for cik_data in data.values():
            ticker = cik_data.get('ticker')
            if ticker:
                sec_tickers.append(ticker.upper())
        
        print(f"Successfully fetched {len(sec_tickers)} tickers from SEC.")
        return sec_tickers
    except requests.exceptions.RequestException as e:
        print(f"Error fetching SEC tickers from {url}: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {url}: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred while processing SEC tickers: {e}")
        return []


# --- Main processing: Update all tickers at once ---

def update_daily_ohlcv_all_at_once(user_ticker_list: list, output_dir: str, global_start_date: str, override_ticker_list: list = None):
    os.makedirs(output_dir, exist_ok=True)
    
    if override_ticker_list:
        print(f"Override list provided. Processing only these tickers: {override_ticker_list}")
        combined_tickers = sorted(list(set(override_ticker_list)))
    else:
        # If no override, use the user_ticker_list (which will be empty if not passed via CLI)
        # and combine with SEC tickers.
        sec_tickers = get_sec_tickers(SEC_TICKERS_URL)
        combined_tickers = sorted(list(set(user_ticker_list + sec_tickers)))
    
    if not combined_tickers:
        print("No tickers to process (user list, SEC list, and override list are empty or failed to fetch). Exiting.")
        return

    print(f"\nProcessing a total of {len(combined_tickers)} unique tickers from combined list.")

    # Step 1: Determine the latest date for each ticker from existing files
    ticker_last_dates = {}  # Overall latest date for each ticker

    print("Scanning existing files to determine current data coverage...")
    existing_files = [f for f in os.listdir(output_dir) if f.endswith('.parquet')]
    existing_files.sort()

    today_date = datetime.datetime.now().date() 

    for filename in existing_files:
        filepath = os.path.join(output_dir, filename)
        date_str = filename.replace('.parquet', '')
        
        try:
            file_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date() 

            if file_date > today_date:
                print(f"Skipping future file: {filename}")
                continue
            
            # Only read 'timestamp' and 'symbol' for efficiency
            # Ensure we read all relevant columns for last date determination
            df_metadata = pd.read_parquet(filepath, columns=['timestamp', 'symbol'])
            
            if not df_metadata.empty:
                last_dates_in_file = df_metadata.groupby('symbol')['timestamp'].max().to_dict()
                for symbol, last_ts in last_dates_in_file.items():
                    current_last_ts = ticker_last_dates.get(symbol)
                    if current_last_ts is None or last_ts > current_last_ts:
                        ticker_last_dates[symbol] = last_ts

        except Exception as e:
            print(f"Warning: Could not read metadata from {filepath} due to '{e}'. Skipping this file.")

    printable_ticker_last_dates = {k: v.strftime('%Y-%m-%d') for k, v in ticker_last_dates.items() if isinstance(v, datetime.datetime)}
    print(f"Current overall data coverage (latest dates): {printable_ticker_last_dates}")


    # Step 2: Prepare fetch requests for all combined tickers
    fetch_requests = {} # {ticker: earliest_start_date_to_fetch_from}
    today_str = datetime.datetime.now().strftime('%Y-%m-%d')

    for ticker in combined_tickers:
        if ticker in ticker_last_dates:
            # If ticker exists, fetch from the day after its last recorded date
            last_date_plus_one = (ticker_last_dates[ticker] + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            if last_date_plus_one <= today_str:
                fetch_requests[ticker] = last_date_plus_one
            else:
                print(f"Skipping {ticker}: Already up-to-date or future date. Last recorded: {ticker_last_dates[ticker].strftime('%Y-%m-%d')}")
        else:
            # Completely new ticker, fetch from GLOBAL_START_DATE
            fetch_requests[ticker] = global_start_date
            print(f"Adding new ticker {ticker} to fetch list (from {global_start_date}).")
    
    # Filter out any tickers that don't need fetching (e.g., already up-to-date)
    tickers_to_fetch_in_this_run = sorted([t for t, start_date in fetch_requests.items() if start_date <= today_str])

    if not tickers_to_fetch_in_this_run:
        print("No new data to fetch for any ticker in this run. All data appears complete. Exiting.")
        return

    print(f"\nFetching data for {len(tickers_to_fetch_in_this_run)} tickers in this run:")

    all_fetched_data = []
    total_bunches = (len(tickers_to_fetch_in_this_run) + STOCKS_PER_BUNCH - 1) // STOCKS_PER_BUNCH

    for i in range(0, len(tickers_to_fetch_in_this_run), STOCKS_PER_BUNCH):
        current_bunch_tickers = tickers_to_fetch_in_this_run[i:i + STOCKS_PER_BUNCH]
        current_bunch_number = i // STOCKS_PER_BUNCH + 1
        
        print(f"\n--- Processing bunch {current_bunch_number} of {total_bunches} (Tickers: {', '.join(current_bunch_tickers)}) ---")

        for ticker in current_bunch_tickers:
            download_start_date = fetch_requests[ticker]
            download_end_date = today_str # Always fetch up to today

            print(f"  Fetching {ticker} from {download_start_date} to {download_end_date}...")
            try:
                df = load_and_clean_ohlcv(ticker, start=download_start_date, end=download_end_date)
                if not df.empty:
                    all_fetched_data.append(downcast_ohlcv(df))
            except Exception as e:
                print(f"  Warning: Error fetching or processing {ticker} (from {download_start_date} to {download_end_date}) due to {e}. Skipping this ticker for this run.")
        
        # Pause after each bunch, unless it's the very last bunch
        if (i + STOCKS_PER_BUNCH) < len(tickers_to_fetch_in_this_run):
            random_delay = random.randint(MIN_FETCH_DELAY_SECONDS, MAX_FETCH_DELAY_SECONDS)
            print(f"Pausing for {random_delay} seconds before next bunch of stocks...")
            time.sleep(random_delay)

    if not all_fetched_data:
        print("No new data was successfully fetched in this run. Exiting.")
        return

    # Step 3: Concatenate all newly fetched data and prepare for merging ---
    full_new_data = pd.concat(all_fetched_data, ignore_index=True)
    full_new_data['date_str'] = full_new_data['timestamp'].dt.strftime('%Y-%m-%d')
    full_new_data.sort_values(by=['timestamp', 'symbol'], inplace=True)

    all_dates_to_update = sorted(full_new_data['date_str'].unique())
    grouped_new_data = full_new_data.groupby('date_str')

    # Step 4: Iterate through affected dates and merge new data ---
    for date_str in all_dates_to_update:
        path = os.path.join(output_dir, f"{date_str}.parquet")
        
        new_data_for_this_date = grouped_new_data.get_group(date_str).drop(columns=['date_str'])

        existing_daily_df = pd.DataFrame()

        if os.path.exists(path):
            try:
                existing_daily_df = pd.read_parquet(path)
            except Exception as e:
                print(f"Warning: Could not load existing data for {date_str} from {path} due to {e}. It will be treated as empty for this update.")
                existing_daily_df = pd.DataFrame()

        combined_df = pd.concat([existing_daily_df, new_data_for_this_date], ignore_index=True)
        
        # Drop duplicates, keeping the latest entry for each timestamp/symbol pair
        combined_df.drop_duplicates(subset=['timestamp', 'symbol'], keep='last', inplace=True)

        # Ensure only tickers from the combined_tickers list are kept in the final daily file
        combined_df = combined_df[combined_df['symbol'].isin(combined_tickers)]
        
        combined_df.sort_values(by=['timestamp', 'symbol'], inplace=True)

        if not combined_df.empty:
            combined_df.to_parquet(
                path,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            print(f"Updated/Created {path} with {len(combined_df)} total rows ({len(new_data_for_this_date)} new/updated rows for this date).")
        else:
            # If after merging and filtering, the DataFrame is empty, remove the file if it exists
            if os.path.exists(path):
                os.remove(path)
                print(f"Removed empty file: {path}")


# Example usage
if __name__ == '__main__':
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Download daily OHLCV stock data.")
    parser.add_argument(
        '--override-tickers',
        type=str,
        help='Comma-separated list of tickers to fetch, overriding default and SEC tickers.'
    )
    # Argument for output directory
    parser.add_argument(
        '--output-dir',
        type=str,
        help=f'Path to the output directory for OHLCV data. Defaults to "{DEFAULT_OUTPUT_DIRECTORY}".'
    )
    args = parser.parse_args()

    override_list = None
    if args.override_tickers:
        override_list = [ticker.strip().upper() for ticker in args.override_tickers.split(',')]
        print(f"Received override tickers from command line: {override_list}")

    # Determine the output directory
    output_directory_final = args.output_dir if args.output_dir else DEFAULT_OUTPUT_DIRECTORY
    print(f"Using output directory: {output_directory_final}")

    print(f"--- Full Update Run: Attempting to update all known tickers from their last recorded date or {GLOBAL_START_DATE} ---")
    # Pass an empty list for user_ticker_list as it's now primarily driven by override or SEC
    update_daily_ohlcv_all_at_once([], output_directory_final, GLOBAL_START_DATE, override_ticker_list=override_list)

    print("\n--- Update complete ---")
    print(f"Data saved to: {output_directory_final}")
