import yfinance as yf
import pandas as pd
import os
import pyarrow.parquet as pq
import datetime
import requests
import json
import time
import random # Already imported, keeping it here for clarity

# --- Configuration ---
SEC_TICKERS_URL = "http://sec.gov/files/company_tickers.json"
OUTPUT_DIRECTORY = 'data/daily_ohlcv'
GLOBAL_START_DATE = '1990-01-01' # The earliest date to consider downloading data from

# New: Constants for random behavior
MIN_FETCH_DELAY_SECONDS = 5
MAX_FETCH_DELAY_SECONDS = 30
MIN_NEW_TICKERS_PER_RUN = 1
MAX_NEW_TICKERS_PER_RUN = 10
MAX_MISSING_TICKERS_TO_ADD_PER_RUN = 100 # Limit for historical gaps


# --- 1. Download, clean, and downcast functions (no change) ---

def load_and_clean_ohlcv(ticker: str,
                         start: str,
                         end: str = datetime.datetime.now().strftime('%Y-%m-%d'),
                         interval: str = '1d') -> pd.DataFrame:
    """Download OHLCV from Yahoo, flatten columns, and return clean DataFrame."""
    
    if datetime.datetime.strptime(start, '%Y-%m-%d') > datetime.datetime.strptime(end, '%Y-%m-%d'):
        return pd.DataFrame()

    df = yf.download(
        ticker,
        start=start,
        end=end,
        interval=interval,
        progress=False,
        auto_adjust=False,
        group_by='column',
        threads=False
    )
    if df.empty:
        print(f"Warning: No data returned for {ticker} within the range {start} to {end}. This might be normal for very new tickers, if the symbol is incorrect, or no new data is available yet.")
        return pd.DataFrame()

    df.reset_index(inplace=True)

    df.columns = [
        str(col[0]).strip().lower() if isinstance(col, tuple) else str(col).strip().lower()
        for col in df.columns
    ]

    if 'date' in df.columns:
        df.rename(columns={'date': 'timestamp'}, inplace=True)
    else:
        raise ValueError(f"'date' column not found for {ticker}. Check yfinance output format.")

    required = {'timestamp', 'open', 'high', 'low', 'close', 'volume'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing essential OHLCV columns for {ticker}: {missing}. Columns found: {df.columns.tolist()}")

    df['symbol'] = ticker
    return df[['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']]


def downcast_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast numeric columns for memory efficiency."""
    if df.empty:
        return df
    for col in ['open', 'high', 'low', 'close']:
        df[col] = pd.to_numeric(df[col], downcast='float')
    df['volume'] = pd.to_numeric(df['volume'], downcast='integer', errors='coerce').astype('Int64')
    df['symbol'] = df['symbol'].astype('category')
    return df

# --- Fetch SEC Tickers (no change) ---
def get_sec_tickers(url: str) -> list:
    """Fetches and parses the SEC company_tickers.json list."""
    print(f"Attempting to fetch tickers from SEC: {url}")
    try:
        # IMPORTANT: Replace 'YourAppName/1.0 (your.email@example.com)' with your actual app name and email
        # This is a good practice for respectful API usage and can help Yahoo identify you if there are issues.
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


# --- Main processing: Smartly update per-day files ---

def update_daily_ohlcv_incrementally(user_ticker_list: list, output_dir: str, global_start_date: str):
    os.makedirs(output_dir, exist_ok=True)
    
    sec_tickers = get_sec_tickers(SEC_TICKERS_URL)
    
    combined_tickers = sorted(list(set(user_ticker_list + sec_tickers)))
    
    if not combined_tickers:
        print("No tickers to process (user list and SEC list are empty or failed to fetch). Exiting.")
        return

    print(f"\nProcessing a total of {len(combined_tickers)} unique tickers from combined list.")

    # Step 1: Determine the latest date for each ticker AND identify specific date-ticker gaps
    ticker_last_dates = {} # Overall latest date for each ticker
    specific_missing_data_points = set() 

    print("Scanning existing files to determine current data coverage and identify gaps...")
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
            
            df_metadata = pd.read_parquet(filepath, columns=['timestamp', 'symbol'])
            
            present_tickers_in_file = set(df_metadata['symbol'].unique())
            
            missing_tickers_for_this_date = set(combined_tickers) - present_tickers_in_file
            
            # Add missing (ticker, date) pairs to our set
            for missing_ticker in missing_tickers_for_this_date:
                if file_date <= today_date:
                    specific_missing_data_points.add((missing_ticker, date_str))

            if not df_metadata.empty:
                last_dates_in_file = df_metadata.groupby('symbol')['timestamp'].max().to_dict()
                for symbol, last_ts in last_dates_in_file.items():
                    current_last_ts = ticker_last_dates.get(symbol)
                    if current_last_ts is None or last_ts > current_last_ts:
                        ticker_last_dates[symbol] = last_ts
        except Exception as e:
            print(f"Warning: Could not read metadata from {filepath} due to '{e}'. Skipping this file and marking all expected tickers for this date as potentially missing.")
            file_date_for_error = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            if file_date_for_error <= today_date:
                for ticker in combined_tickers:
                    specific_missing_data_points.add((ticker, date_str))


    printable_ticker_last_dates = {k: v.strftime('%Y-%m-%d') for k, v in ticker_last_dates.items() if isinstance(v, datetime.datetime)}
    print(f"Current overall data coverage (latest dates): {printable_ticker_last_dates}")
    if specific_missing_data_points:
        print(f"Identified {len(specific_missing_data_points)} specific (ticker, date) gaps.")


    # Step 2: Determine exactly what needs to be fetched for this run
    fetch_requests = {} # {ticker: earliest_start_date}

    # Determine the random limit for new tickers for this run
    current_max_new_tickers = random.randint(MIN_NEW_TICKERS_PER_RUN, MAX_NEW_TICKERS_PER_RUN)
    print(f"\nLimiting completely new stock tickers added this run to {current_max_new_tickers}.")

    # Add requests for new tickers (not in ticker_last_dates)
    new_tickers_candidates = sorted([t for t in combined_tickers if t not in ticker_last_dates])
    limited_new_tickers = new_tickers_candidates[:current_max_new_tickers]
    for ticker in limited_new_tickers:
        fetch_requests[ticker] = global_start_date
        print(f"Adding new ticker {ticker} to fetch list (from {global_start_date}).")

    # Add requests for existing tickers that need updates (data not up to today)
    for ticker in combined_tickers:
        if ticker in ticker_last_dates:
            last_date_plus_one = (ticker_last_dates[ticker] + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            today_str = datetime.datetime.now().strftime('%Y-%m-%d') 
            if last_date_plus_one <= today_str:
                if ticker not in fetch_requests or last_date_plus_one < fetch_requests[ticker]:
                    fetch_requests[ticker] = last_date_plus_one
                    print(f"Adding existing ticker {ticker} to fetch list (from {last_date_plus_one}).")
            
    # Add requests for specific missing data points, respecting the limit
    # Convert set to list to allow slicing, then sort for consistent behavior
    sorted_missing_data_points = sorted(list(specific_missing_data_points))
    # Apply limit for missing historical data points
    limited_missing_data_points = sorted_missing_data_points[:MAX_MISSING_TICKERS_TO_ADD_PER_RUN]

    if limited_missing_data_points:
        print(f"Attempting to fill {len(limited_missing_data_points)} historical gaps this run (max {MAX_MISSING_TICKERS_TO_ADD_PER_RUN}).")

    for ticker, date_str in limited_missing_data_points:
        if ticker in fetch_requests:
            current_fetch_start = fetch_requests[ticker]
            if date_str < current_fetch_start:
                fetch_requests[ticker] = date_str
                print(f"Adjusting fetch for {ticker} to start earlier from {date_str} to fill gap.")
        else:
            fetch_requests[ticker] = date_str
            print(f"Adding specific gap fetch for {ticker} on {date_str}.")


    tickers_for_this_run = sorted(fetch_requests.keys())
    
    if not tickers_for_this_run:
        print("No new data to fetch for any ticker in this run. All data appears complete. Exiting.")
        return

    print(f"\nFetching data for {len(tickers_for_this_run)} tickers in this run:")
    print(tickers_for_this_run)

    all_fetched_data = []

    for i, ticker in enumerate(tickers_for_this_run, start=1):
        download_start_date = fetch_requests[ticker]
        download_end_date = datetime.datetime.now().strftime('%Y-%m-%d') 

        print(f"[{i}/{len(tickers_for_this_run)}] Fetching {ticker} from {download_start_date} to {download_end_date}...")
        try:
            df = load_and_clean_ohlcv(ticker, start=download_start_date, end=download_end_date)
            if not df.empty:
                all_fetched_data.append(downcast_ohlcv(df))
            else:
                pass
        except Exception as e:
            print(f"Warning: Error fetching or processing {ticker} (from {download_start_date} to {download_end_date}) due to {e}. Skipping this ticker for this run.")
        
        if i < len(tickers_for_this_run):
            # Random delay between MIN_FETCH_DELAY_SECONDS and MAX_FETCH_DELAY_SECONDS seconds per stock
            random_delay = random.randint(MIN_FETCH_DELAY_SECONDS, MAX_FETCH_DELAY_SECONDS)
            print(f"Pausing for {random_delay} seconds before next ticker...")
            time.sleep(random_delay)

    if not all_fetched_data:
        print("No new data was successfully fetched in this run. Exiting.")
        return

    # Step 3: Concatenate all newly fetched data and prepare for merging (no change) ---
    full_new_data = pd.concat(all_fetched_data, ignore_index=True)
    full_new_data['date_str'] = full_new_data['timestamp'].dt.strftime('%Y-%m-%d')
    full_new_data.sort_values(by=['timestamp', 'symbol'], inplace=True)

    all_dates_to_update = sorted(full_new_data['date_str'].unique())
    grouped_new_data = full_new_data.groupby('date_str')

    # Step 4: Iterate through affected dates and merge new data (no change) ---
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
        
        combined_df.drop_duplicates(subset=['timestamp', 'symbol'], keep='last', inplace=True)

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
            if os.path.exists(path):
                os.remove(path)
                print(f"Removed empty file: {path}")


# Example usage
if __name__ == '__main__':
    my_favorite_tickers = ['AAPL', 'MSFT', 'TSLA', 'GOOGL', 'SPY', 'VOO', 'SMCI', 'AMZN', 'NVDA', 'AMD', 'NFLX']

    print(f"--- First Run: Will check for all gaps and add a random number ({MIN_NEW_TICKERS_PER_RUN}-{MAX_NEW_TICKERS_PER_RUN}) of new tickers and up to {MAX_MISSING_TICKERS_TO_ADD_PER_RUN} missing historical gaps ---")
    update_daily_ohlcv_incrementally(my_favorite_tickers, OUTPUT_DIRECTORY, GLOBAL_START_DATE)

    print("\n--- Update complete ---")
    print(f"Data saved to: {OUTPUT_DIRECTORY}")