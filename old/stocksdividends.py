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
OUTPUT_DIRECTORY_DIVIDENDS = 'data/dividends_by_date' # New directory for dividends, saved by date
GLOBAL_START_DATE = '1990-01-01' # The earliest date to consider downloading data from

# Constants for random behavior (still useful for API rate limiting)
MIN_FETCH_DELAY_SECONDS = 15
MAX_FETCH_DELAY_SECONDS = 30
STOCKS_PER_BUNCH = 10 # Number of stocks to fetch in one bunch before pausing

# --- 1. Download, clean, and downcast functions for dividends ---

def load_and_clean_dividends(ticker: str,
                             start: str,
                             end: str = datetime.datetime.now().strftime('%Y-%m-%d')) -> pd.DataFrame:
    """Download dividend data from Yahoo, flatten columns, and return clean DataFrame."""

    if datetime.datetime.strptime(start, '%Y-%m-%d') > datetime.datetime.strptime(end, '%Y-%m-%d'):
        print(f"Warning: Start date {start} is after end date {end} for {ticker}. Returning empty DataFrame.")
        return pd.DataFrame()

    try:
        stock_ticker = yf.Ticker(ticker)
        df = stock_ticker.dividends
        
        if df.empty:
            print(f"Warning: No dividend data returned for {ticker} within the range {start} to {end}.")
            return pd.DataFrame()

        if isinstance(df, pd.Series):
            df = df.to_frame(name='dividend_amount')
        else:
            df.rename(columns={'Dividends': 'dividend_amount'}, inplace=True)

        df.index.name = 'timestamp'
        df.reset_index(inplace=True)

        # Ensure timestamp is datetime, remove timezone info, and then filter by date range
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None) # Fix 1: Ensure naive datetime
        df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]

        if df.empty:
            print(f"Warning: No dividend data for {ticker} after filtering to range {start} to {end}.")
            return pd.DataFrame()

        df['symbol'] = ticker
        return df[['timestamp', 'symbol', 'dividend_amount']]

    except Exception as e:
        print(f"Error fetching dividend data for {ticker} from {start} to {end}: {e}")
        return pd.DataFrame()
    finally:
        time.sleep(1) # Be polite with API calls


def downcast_dividends(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast numeric columns for memory efficiency for dividend data."""
    if df.empty:
        return df
    df['dividend_amount'] = pd.to_numeric(df['dividend_amount'], downcast='float')
    df['symbol'] = df['symbol'].astype('category')
    return df

# --- Fetch SEC Tickers (re-use from original script) ---
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


# --- Main processing: Update all tickers at once for dividends, saving by date ---

def update_dividend_data_by_date(user_ticker_list: list, output_dir: str, global_start_date: str):
    os.makedirs(output_dir, exist_ok=True)

    sec_tickers = user_ticker_list
    
    combined_tickers = sorted(list(set(user_ticker_list + sec_tickers)))
    
    if not combined_tickers:
        print("No tickers to process (user list and SEC list are empty or failed to fetch). Exiting.")
        return

    print(f"\nProcessing a total of {len(combined_tickers)} unique tickers for dividends from combined list.")

    # Step 1: Determine the latest date for each ticker from existing files (now requires scanning all daily files)
    ticker_last_dates = {}  # Overall latest date for each ticker

    print("Scanning existing dividend files (by date) to determine current data coverage...")
    existing_files = [f for f in os.listdir(output_dir) if f.endswith('.parquet')]
    existing_files.sort() # Process in order

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
            df_metadata = pd.read_parquet(filepath, columns=['timestamp', 'symbol'])
            
            if not df_metadata.empty:
                # Fix 2: Ensure 'symbol' is category after loading for consistent concatenation
                if 'symbol' in df_metadata.columns:
                    df_metadata['symbol'] = df_metadata['symbol'].astype('category')

                last_dates_in_file = df_metadata.groupby('symbol')['timestamp'].max().to_dict()
                for symbol, last_ts in last_dates_in_file.items():
                    current_last_ts = ticker_last_dates.get(symbol)
                    # Use timestamp.tz_localize(None) to remove timezone info if present, for consistent comparison
                    if current_last_ts is None or last_ts.tz_localize(None) > current_last_ts.tz_localize(None):
                        ticker_last_dates[symbol] = last_ts.tz_localize(None) # Store without timezone
        except Exception as e:
            print(f"Warning: Could not read metadata from {filepath} due to '{e}'. Skipping this file.")

    printable_ticker_last_dates = {k: v.strftime('%Y-%m-%d') for k, v in ticker_last_dates.items() if isinstance(v, datetime.datetime)}
    print(f"Current overall dividend data coverage (latest dates): {printable_ticker_last_dates}")


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
                print(f"Skipping {ticker} dividends: Already up-to-date or future date. Last recorded: {ticker_last_dates[ticker].strftime('%Y-%m-%d')}")
        else:
            # Completely new ticker, fetch from GLOBAL_START_DATE
            fetch_requests[ticker] = global_start_date
            print(f"Adding new ticker {ticker} for dividend fetch list (from {global_start_date}).")
            
    tickers_to_fetch_in_this_run = sorted([t for t, start_date in fetch_requests.items() if start_date <= today_str])

    if not tickers_to_fetch_in_this_run:
        print("No new dividend data to fetch for any ticker in this run. All data appears complete. Exiting.")
        return

    print(f"\nFetching dividend data for {len(tickers_to_fetch_in_this_run)} tickers in this run:")

    all_fetched_data = [] # Collect all new dividend data here
    total_bunches = (len(tickers_to_fetch_in_this_run) + STOCKS_PER_BUNCH - 1) // STOCKS_PER_BUNCH

    for i in range(0, len(tickers_to_fetch_in_this_run), STOCKS_PER_BUNCH):
        current_bunch_tickers = tickers_to_fetch_in_this_run[i:i + STOCKS_PER_BUNCH]
        current_bunch_number = i // STOCKS_PER_BUNCH + 1
        
        print(f"\n--- Processing bunch {current_bunch_number} of {total_bunches} (Tickers: {', '.join(current_bunch_tickers)}) ---")

        for ticker in current_bunch_tickers:
            download_start_date = fetch_requests[ticker]
            download_end_date = today_str # Always fetch up to today

            print(f"  Fetching dividend for {ticker} from {download_start_date} to {download_end_date}...")
            try:
                df = load_and_clean_dividends(ticker, start=download_start_date, end=download_end_date)
                if not df.empty:
                    all_fetched_data.append(downcast_dividends(df))
            except Exception as e:
                print(f"  Warning: Error fetching or processing dividend for {ticker} (from {download_start_date} to {download_end_date}) due to {e}. Skipping this ticker for this run.")
        
        # Pause after each bunch, unless it's the very last bunch
        if (i + STOCKS_PER_BUNCH) < len(tickers_to_fetch_in_this_run):
            random_delay = random.randint(MIN_FETCH_DELAY_SECONDS, MAX_FETCH_DELAY_SECONDS)
            print(f"Pausing for {random_delay} seconds before next bunch of stocks...")
            time.sleep(random_delay)

    if not all_fetched_data:
        print("No new dividend data was successfully fetched in this run. Exiting.")
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
                # Fix 2: Ensure 'symbol' is category after loading for consistent concatenation
                if 'symbol' in existing_daily_df.columns:
                    existing_daily_df['symbol'] = existing_daily_df['symbol'].astype('category')
            except Exception as e:
                print(f"Warning: Could not load existing dividend data for {date_str} from {path} due to {e}. It will be treated as empty for this update.")
                existing_daily_df = pd.DataFrame()

        combined_df = pd.concat([existing_daily_df, new_data_for_this_date], ignore_index=True)
        
        # Drop duplicates, keeping the latest entry for each timestamp/symbol pair
        combined_df.drop_duplicates(subset=['timestamp', 'symbol'], keep='last', inplace=True)

        # Ensure only tickers from the combined_tickers list are kept in the final daily file
        # This is important if an old file had data for a ticker no longer in your combined list
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


# Fix 3: Encapsulate main execution logic in a function
def main():
    my_favorite_tickers = ['AAPL', 'MSFT', 'TSLA', 'GOOGL', 'SPY', 'VOO', 'SMCI', 'AMZN', 'NVDA', 'AMD', 'NFLX']

    print(f"--- Full Dividend Data Update Run (by date): Attempting to update all known tickers from their last recorded date or {GLOBAL_START_DATE} ---")
    update_dividend_data_by_date(my_favorite_tickers, OUTPUT_DIRECTORY_DIVIDENDS, GLOBAL_START_DATE)

    print(f"\nDividend data saved to: {OUTPUT_DIRECTORY_DIVIDENDS}")

if __name__ == '__main__':
    main()