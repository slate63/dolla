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
        auto_adjust=False,
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


# --- Main processing: Update all tickers at once (modified for immediate saving) ---

def update_daily_ohlcv_all_at_once(user_ticker_list: list, output_dir: str, global_start_date: str):
    os.makedirs(output_dir, exist_ok=True)
    
    sec_tickers = get_sec_tickers(SEC_TICKERS_URL)
    
    combined_tickers = sorted(list(set(user_ticker_list + sec_tickers)))
    
    if not combined_tickers:
        print("No tickers to process (user list and SEC list are empty or failed to fetch). Exiting.")
        return

    print(f"\nProcessing a total of {len(combined_tickers)} unique tickers from combined list.")

    # Determine the latest date from the most recent file
    earliest_fetch_date = datetime.datetime.strptime(global_start_date, '%Y-%m-%d').date()
    today_date = datetime.datetime.now().date()
    
    existing_files = [f for f in os.listdir(output_dir) if f.endswith('.parquet')]
    existing_files.sort() 

    if existing_files:
        latest_filename = existing_files[-1] 
        try:
            latest_file_date = datetime.datetime.strptime(latest_filename.replace('.parquet', ''), '%Y-%m-%d').date()
            # Start fetching from the day after the date of the latest file
            earliest_fetch_date = latest_file_date + datetime.timedelta(days=1)
            print(f"Determined fetch start date from latest existing file ({latest_filename}): {earliest_fetch_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"Warning: Could not parse date from latest filename {latest_filename} due to '{e}'. Defaulting to GLOBAL_START_DATE.")
            earliest_fetch_date = datetime.datetime.strptime(global_start_date, '%Y-%m-%d').date()
    else:
        print(f"No existing data files found. Starting fetch from GLOBAL_START_DATE: {global_start_date}")

    # Ensure we don't try to fetch from a future date
    if earliest_fetch_date > today_date:
        print(f"Calculated fetch start date {earliest_fetch_date.strftime('%Y-%m-%d')} is in the future. Data is up-to-date. Exiting.")
        return

    download_start_date_str = earliest_fetch_date.strftime('%Y-%m-%d')
    download_end_date_str = today_date.strftime('%Y-%m-%d')

    print(f"\nFetching data for all {len(combined_tickers)} tickers from {download_start_date_str} to {download_end_date_str}.")

    total_bunches = (len(combined_tickers) + STOCKS_PER_BUNCH - 1) // STOCKS_PER_BUNCH

    for i in range(0, len(combined_tickers), STOCKS_PER_BUNCH):
        current_bunch_tickers = combined_tickers[i:i + STOCKS_PER_BUNCH]
        current_bunch_number = i // STOCKS_PER_BUNCH + 1
        
        print(f"\n--- Processing bunch {current_bunch_number} of {total_bunches} (Tickers: {', '.join(current_bunch_tickers)}) ---")

        for ticker in current_bunch_tickers:
            print(f"  Fetching {ticker} from {download_start_date_str} to {download_end_date_str}...")
            try:
                df = load_and_clean_ohlcv(ticker, start=download_start_date_str, end=download_end_date_str)
                
                if not df.empty:
                    df = downcast_ohlcv(df)
                    df['date_str'] = df['timestamp'].dt.strftime('%Y-%m-%d')
                    
                    # Process and save this ticker's data immediately
                    for date_str, daily_data in df.groupby('date_str'):
                        path = os.path.join(output_dir, f"{date_str}.parquet")
                        
                        existing_daily_df = pd.DataFrame()
                        if os.path.exists(path):
                            try:
                                existing_daily_df = pd.read_parquet(path)
                            except Exception as e:
                                print(f"    Warning: Could not load existing data for {date_str} from {path} due to {e}. It will be treated as empty for this update.")
                                existing_daily_df = pd.DataFrame()

                        # Ensure the 'symbol' column in daily_data matches the ticker being processed
                        # This is a safeguard, as the groupby 'date_str' should naturally keep it consistent
                        daily_data_for_this_ticker = daily_data.drop(columns=['date_str'])

                        # Filter existing_daily_df to exclude data for the current ticker, then concatenate
                        # This avoids carrying old data for the current ticker if it's being updated
                        existing_daily_df = existing_daily_df[existing_daily_df['symbol'] != ticker]
                        
                        combined_df = pd.concat([existing_daily_df, daily_data_for_this_ticker], ignore_index=True)
                        
                        # Drop duplicates, keeping the latest entry for each timestamp/symbol pair
                        combined_df.drop_duplicates(subset=['timestamp', 'symbol'], keep='last', inplace=True)

                        # Ensure only tickers from the combined_tickers list are kept in the final daily file
                        # This check is still useful in case an existing file contains old, unwanted tickers
                        combined_df = combined_df[combined_df['symbol'].isin(combined_tickers)]
                        
                        combined_df.sort_values(by=['timestamp', 'symbol'], inplace=True)

                        if not combined_df.empty:
                            combined_df.to_parquet(
                                path,
                                engine='pyarrow',
                                compression='snappy',
                                index=False
                            )
                            print(f"    Updated/Created {path} for {ticker} with {len(combined_df)} total rows.")
                        else:
                            # If after merging and filtering, the DataFrame is empty, remove the file if it exists
                            if os.path.exists(path):
                                os.remove(path)
                                print(f"    Removed empty file: {path}")

                else:
                    print(f"  No new data to save for {ticker}.")

            except Exception as e:
                print(f"  Warning: Error fetching or processing {ticker} (from {download_start_date_str} to {download_end_date_str}) due to {e}. Skipping this ticker for this run.")
        
        # Pause after each bunch, unless it's the very last bunch
        if (i + STOCKS_PER_BUNCH) < len(combined_tickers):
            random_delay = random.randint(MIN_FETCH_DELAY_SECONDS, MAX_FETCH_DELAY_SECONDS)
            print(f"Pausing for {random_delay} seconds before next bunch of stocks...")
            time.sleep(random_delay)

    print("All fetching and saving complete for this run.")


# Example usage
if __name__ == '__main__':
    my_favorite_tickers = ['AAPL', 'MSFT', 'TSLA', 'GOOGL', 'SPY', 'VOO', 'SMCI', 'AMZN', 'NVDA', 'AMD', 'NFLX']

    print(f"--- Full Update Run (immediate saving): Attempting to update all known tickers from the last recorded file date or {GLOBAL_START_DATE} ---")
    update_daily_ohlcv_all_at_once(my_favorite_tickers, OUTPUT_DIRECTORY, GLOBAL_START_DATE)

    print("\n--- Update complete ---")
    print(f"Data saved to: {OUTPUT_DIRECTORY}")