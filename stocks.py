import yfinance as yf
import pandas as pd
import os
import pyarrow.parquet as pq
import datetime
import requests
import json
import time
import random

# --- Configuration ---
SEC_TICKERS_URL = "http://sec.gov/files/company_tickers.json"
OUTPUT_DIRECTORY = 'data/daily_ohlcv'
GLOBAL_START_DATE = '1990-01-01' # The earliest date to consider downloading data from

# Constants for random behavior
MIN_FETCH_DELAY_SECONDS = 5
MAX_FETCH_DELAY_SECONDS = 30
MIN_NEW_TICKERS_PER_RUN = 100
MAX_NEW_TICKERS_PER_RUN = 105
MAX_MISSING_TICKERS_TO_ADD_PER_RUN = 100 # Limit for historical gaps


# --- 1. Download, clean, and downcast functions ---

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


# --- Main processing: Smartly update per-day files ---

def update_daily_ohlcv_incrementally(user_ticker_list: list, output_dir: str, global_start_date: str):
    os.makedirs(output_dir, exist_ok=True)
    
    sec_tickers = get_sec_tickers(SEC_TICKERS_URL)
    
    combined_tickers = sorted(list(set(user_ticker_list + sec_tickers)))
    
    if not combined_tickers:
        print("No tickers to process (user list and SEC list are empty or failed to fetch). Exiting.")
        return

    print(f"\nProcessing a total of {len(combined_tickers)} unique tickers from combined list.")

    # Step 1: Determine the latest and earliest date for each ticker, and identify specific date-ticker gaps
    ticker_last_dates = {}  # Overall latest date for each ticker
    ticker_first_dates = {} # Overall earliest date for each ticker (NEW)
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
            
            for missing_ticker in missing_tickers_for_this_date:
                if file_date <= today_date:
                    specific_missing_data_points.add((missing_ticker, date_str))

            if not df_metadata.empty:
                # Update overall latest date for each ticker
                last_dates_in_file = df_metadata.groupby('symbol')['timestamp'].max().to_dict()
                for symbol, last_ts in last_dates_in_file.items():
                    current_last_ts = ticker_last_dates.get(symbol)
                    if current_last_ts is None or last_ts > current_last_ts:
                        ticker_last_dates[symbol] = last_ts
                
                # Update overall earliest date for each ticker (NEW LOGIC)
                first_dates_in_file = df_metadata.groupby('symbol')['timestamp'].min().to_dict()
                for symbol, first_ts in first_dates_in_file.items():
                    current_first_ts = ticker_first_dates.get(symbol)
                    if current_first_ts is None or first_ts < current_first_ts:
                        ticker_first_dates[symbol] = first_ts

        except Exception as e:
            print(f"Warning: Could not read metadata from {filepath} due to '{e}'. Skipping this file and marking all expected tickers for this date as potentially missing.")
            file_date_for_error = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            if file_date_for_error <= today_date:
                for ticker in combined_tickers:
                    specific_missing_data_points.add((ticker, date_str))

    printable_ticker_last_dates = {k: v.strftime('%Y-%m-%d') for k, v in ticker_last_dates.items() if isinstance(v, datetime.datetime)}
    printable_ticker_first_dates = {k: v.strftime('%Y-%m-%d') for k, v in ticker_first_dates.items() if isinstance(v, datetime.datetime)}
    print(f"Current overall data coverage (latest dates): {printable_ticker_last_dates}")
    print(f"Current overall data coverage (earliest dates): {printable_ticker_first_dates}")
    if specific_missing_data_points:
        print(f"Identified {len(specific_missing_data_points)} specific (ticker, date) gaps.")


    # Step 2: Determine exactly what needs to be fetched for this run
    fetch_requests = {} # {ticker: earliest_start_date_to_fetch_from}

    # First, establish a base fetch start for all combined tickers
    # This considers GLOBAL_START_DATE and existing data's earliest date
    for ticker in combined_tickers:
        global_start_dt = datetime.datetime.strptime(global_start_date, '%Y-%m-%d')
        
        if ticker in ticker_first_dates:
            # If ticker exists and GLOBAL_START_DATE is earlier than its current first date,
            # then we need to fetch from GLOBAL_START_DATE to fill the historical gap.
            if global_start_dt < ticker_first_dates[ticker]:
                fetch_requests[ticker] = global_start_date
                print(f"Adding {ticker} for historical re-fetch from {global_start_date} (current earliest: {ticker_first_dates[ticker].strftime('%Y-%m-%d')}).")
            else:
                # Otherwise, it's already covered back to GLOBAL_START_DATE or earlier.
                # Initialize with current latest + 1 day, or no fetch if up-to-date.
                last_date_plus_one = (ticker_last_dates[ticker] + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                today_str = datetime.datetime.now().strftime('%Y-%m-%d') 
                if last_date_plus_one <= today_str:
                    fetch_requests[ticker] = last_date_plus_one
        else:
            # Completely new ticker, fetch from GLOBAL_START_DATE
            fetch_requests[ticker] = global_start_date
            print(f"Adding new ticker {ticker} to fetch list (from {global_start_date}).")
    
    # Randomly select new tickers to actually fetch (if any)
    # Filter out tickers that are already in fetch_requests with a valid start date
    # OR those that have data covering GLOBAL_START_DATE (meaning their fetch_request is not set or later)
    # The `fetch_requests[ticker] == global_start_date` implies they are new or need a full history re-fetch
    new_tickers_candidates_for_run = [
        t for t in combined_tickers 
        if t in fetch_requests and fetch_requests[t] == global_start_date and t not in ticker_first_dates # Only consider truly new ones
    ]
    random.shuffle(new_tickers_candidates_for_run) # Shuffle to pick randomly
    current_max_new_tickers = random.randint(MIN_NEW_TICKERS_PER_RUN, MAX_NEW_TICKERS_PER_RUN)
    print(f"\nLimiting completely new stock tickers added this run to {current_max_new_tickers}.")
    
    # This set will store tickers that are explicitly selected for fetching in this run
    tickers_to_fetch_in_this_run_explicitly = set()

    for i, ticker in enumerate(new_tickers_candidates_for_run):
        if i < current_max_new_tickers:
            tickers_to_fetch_in_this_run_explicitly.add(ticker)
        else:
            # If we don't pick it as a new ticker this run, remove its global_start_date fetch request
            if ticker in fetch_requests and fetch_requests[ticker] == global_start_date:
                del fetch_requests[ticker] # Remove if not selected this run

    # Now, refine fetch_requests by prioritizing existing ticker updates and specific gaps
    # (Existing tickers that need updates or historical re-fetch are already in fetch_requests
    # from the initial loop or will be added from specific_missing_data_points)

    # Add requests for specific missing data points, respecting the limit
    sorted_missing_data_points = sorted(list(specific_missing_data_points))
    limited_missing_data_points = sorted_missing_data_points[:MAX_MISSING_TICKERS_TO_ADD_PER_RUN]

    if limited_missing_data_points:
        print(f"Attempting to fill {len(limited_missing_data_points)} historical gaps this run (max {MAX_MISSING_TICKERS_TO_ADD_PER_RUN}).")

    for ticker, date_str in limited_missing_data_points:
        # Add to explicit fetch list
        tickers_to_fetch_in_this_run_explicitly.add(ticker) 

        # Now, adjust the fetch_request start date if this specific gap is earlier
        if ticker in fetch_requests:
            current_fetch_start = fetch_requests[ticker]
            if date_str < current_fetch_start:
                fetch_requests[ticker] = date_str
                print(f"Adjusting fetch for {ticker} to start earlier from {date_str} to fill gap.")
        else:
            fetch_requests[ticker] = date_str
            print(f"Adding specific gap fetch for {ticker} on {date_str}.")
    
    # Final list of tickers for this specific run
    # Only include tickers that were chosen as new, or had an update/gap explicitly added
    tickers_for_this_run = sorted([t for t in fetch_requests if t in tickers_to_fetch_in_this_run_explicitly or fetch_requests[t] != global_start_date])
    
    # A final check to ensure only tickers within the combined_tickers are processed
    # and that their start date is not after today.
    final_fetch_list = []
    for ticker in tickers_for_this_run:
        if ticker in combined_tickers:
            start_date_obj = datetime.datetime.strptime(fetch_requests[ticker], '%Y-%m-%d').date()
            if start_date_obj <= today_date:
                final_fetch_list.append(ticker)
            else:
                print(f"Skipping {ticker}: Requested start date {fetch_requests[ticker]} is in the future.")
        
    tickers_for_this_run = sorted(final_fetch_list)


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
            random_delay = random.randint(MIN_FETCH_DELAY_SECONDS, MAX_FETCH_DELAY_SECONDS)
            print(f"Pausing for {random_delay} seconds before next ticker...")
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

    # You might want to run this multiple times to see the incremental updates in action
    # For example, to add more new tickers or fill more gaps over time.
    # print(f"\n--- Second Run: Will continue filling gaps and adding another random batch of new tickers ---")
    # update_daily_ohlcv_incrementally(my_favorite_tickers, OUTPUT_DIRECTORY, GLOBAL_START_DATE)

    print("\n--- Update complete ---")
    print(f"Data saved to: {OUTPUT_DIRECTORY}")