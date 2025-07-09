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

# --- Configuration ---
SEC_TICKERS_URL = "http://sec.gov/files/company_tickers.json"
LOCAL_TICKERS_FILE = "company_tickers.json"
DEFAULT_OUTPUT_DIRECTORY = '/data/daily_ohlcv'
GLOBAL_START_DATE = '1990-01-01'

MIN_FETCH_DELAY_SECONDS = 15
MAX_FETCH_DELAY_SECONDS = 30
STOCKS_PER_BUNCH = 10

#adding for traceablity

def load_and_clean_ohlcv(ticker: str,
                         start: str,
                         end: str = datetime.datetime.now().strftime('%Y-%m-%d'),
                         interval: str = '1d') -> pd.DataFrame:
    if datetime.datetime.strptime(start, '%Y-%m-%d') > datetime.datetime.strptime(end, '%Y-%m-%d'):
        print(f"Warning: Start date {start} is after end date {end} for {ticker}. Returning empty DataFrame.")
        return pd.DataFrame()

    try:
        yf_ticker = yf.Ticker(ticker)
        df = yf_ticker.history(start=start, end=end, interval=interval, actions=True, auto_adjust=False)
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()

    if df.empty:
        print(f"Warning: No data returned for {ticker} within the range {start} to {end}.")
        return pd.DataFrame()

    df.reset_index(inplace=True)
    df.columns = [str(col).strip().lower() for col in df.columns]

    if 'date' in df.columns:
        df.rename(columns={'date': 'timestamp'}, inplace=True)
    elif 'datetime' in df.columns:
        df.rename(columns={'datetime': 'timestamp'}, inplace=True)
    else:
        raise ValueError(f"'date' or 'datetime' column not found for {ticker}. Check yfinance output format.")

    if 'dividends' not in df.columns:
        df['dividends'] = 0.0
    if 'stock splits' not in df.columns:
        df['stock splits'] = 0.0

    df['symbol'] = ticker

    # --- Add Technical Indicators ---
    for period in [5, 20, 50, 100, 200]:
        df[f'sma_{period}'] = df['close'].rolling(window=period).mean()
    for period in [50, 100, 200]:
        df[f'ema_{period}'] = df['close'].ewm(span=period, adjust=False).mean()

    required_columns = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 
                        'dividends', 'stock splits'] + \
                       [f'sma_{p}' for p in [5, 20, 50, 100, 200]] + \
                       [f'ema_{p}' for p in [50, 100, 200]]

    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns for {ticker}: {missing_columns}")

    return df[required_columns]

def downcast_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for col in ['open', 'high', 'low', 'close', 'dividends', 'stock splits'] + \
               [f'sma_{p}' for p in [5, 20, 50, 100, 200]] + \
               [f'ema_{p}' for p in [50, 100, 200]]:
        df[col] = pd.to_numeric(df[col], downcast='float')
    df['volume'] = pd.to_numeric(df['volume'], downcast='integer', errors='coerce').astype('Int64')
    df['symbol'] = df['symbol'].astype('category')
    return df

def get_sec_tickers(url: str, local_file: str) -> list:
    print(f"Attempting to fetch tickers from SEC: {url}")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        print("Successfully fetched tickers from SEC online.")
        return [v.get('ticker').upper() for v in data.values() if v.get('ticker')]
    except Exception as e:
        print(f"Failed to fetch SEC tickers from online source ({e}). Attempting to load from local file: {local_file}")
        if os.path.exists(local_file):
            try:
                with open(local_file, 'r') as f:
                    data = json.load(f)
                print(f"Successfully loaded tickers from local file: {local_file}")
                return [v.get('ticker').upper() for v in data.values() if v.get('ticker')]
            except Exception as e:
                print(f"Failed to load tickers from local file {local_file}: {e}")
                return []
        else:
            print(f"Local ticker file not found: {local_file}")
            return []

def update_daily_ohlcv_all_at_once(user_ticker_list, output_dir, global_start_date, override_ticker_list=None):
    os.makedirs(output_dir, exist_ok=True)
    tickers = sorted(set(override_ticker_list if override_ticker_list else user_ticker_list + get_sec_tickers(SEC_TICKERS_URL, LOCAL_TICKERS_FILE)))
    if not tickers:
        print("No tickers to process. Exiting.")
        return

    print(f"Processing {len(tickers)} tickers")
    ticker_last_dates = {}
    existing_files = sorted([f for f in os.listdir(output_dir) if f.endswith('.parquet')])
    today = datetime.datetime.now().date()

    for fname in existing_files:
        date_str = fname.replace('.parquet', '')
        try:
            fdate = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
            if fdate > today:
                continue
            df_meta = pd.read_parquet(os.path.join(output_dir, fname), columns=['timestamp', 'symbol'])
            if not df_meta.empty:
                for symbol, ts in df_meta.groupby('symbol')['timestamp'].max().to_dict().items():
                    if symbol not in ticker_last_dates or ts > ticker_last_dates[symbol]:
                        ticker_last_dates[symbol] = ts
        except Exception as e:
            print(f"Skipping {fname}: {e}")

    fetch_requests = {}
    today_str = today.strftime('%Y-%m-%d')
    for ticker in tickers:
        last_ts = ticker_last_dates.get(ticker)
        if last_ts:
            next_day = (last_ts + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            if next_day <= today_str:
                fetch_requests[ticker] = next_day
        else:
            fetch_requests[ticker] = global_start_date

    fetch_list = sorted([t for t, s in fetch_requests.items() if s <= today_str])
    if not fetch_list:
        print("All tickers up-to-date. Exiting.")
        return

    all_data = []
    for i in range(0, len(fetch_list), STOCKS_PER_BUNCH):
        bunch = fetch_list[i:i + STOCKS_PER_BUNCH]
        print(f"\nProcessing bunch: {bunch}")
        for ticker in bunch:
            try:
                df = load_and_clean_ohlcv(ticker, fetch_requests[ticker], today_str)
                if not df.empty:
                    all_data.append(downcast_ohlcv(df))
            except Exception as e:
                print(f"Failed {ticker}: {e}")
        if (i + STOCKS_PER_BUNCH) < len(fetch_list):
            time.sleep(random.randint(MIN_FETCH_DELAY_SECONDS, MAX_FETCH_DELAY_SECONDS))

    if not all_data:
        print("No data fetched. Exiting.")
        return

    final_data = pd.concat(all_data, ignore_index=True)
    final_data['date_str'] = final_data['timestamp'].dt.strftime('%Y-%m-%d')
    grouped = final_data.groupby('date_str')

    for date_str, group in grouped:
        path = os.path.join(output_dir, f"{date_str}.parquet")
        existing = pd.DataFrame()
        if os.path.exists(path):
            try:
                existing = pd.read_parquet(path)
            except:
                print(f"Unable to read {path}, treating as empty.")
        combined = pd.concat([existing, group.drop(columns=['date_str'])], ignore_index=True)
        combined.drop_duplicates(subset=['timestamp', 'symbol'], keep='last', inplace=True)
        combined = combined[combined['symbol'].isin(tickers)].sort_values(by=['timestamp', 'symbol'])
        if not combined.empty:
            combined.to_parquet(path, engine='pyarrow', compression='snappy', index=False)
            print(f"Saved {path} with {len(combined)} rows.")
        elif os.path.exists(path):
            os.remove(path)
            print(f"Removed empty {path}.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download daily OHLCV stock data with dividends, splits, and indicators.")
    parser.add_argument('--override-tickers', type=str, help='Comma-separated list of tickers to fetch.')
    parser.add_argument('--output-dir', type=str, help=f'Output directory. Defaults to \"{DEFAULT_OUTPUT_DIRECTORY}\".')
    args = parser.parse_args()

    override = [t.strip().upper() for t in args.override_tickers.split(',')] if args.override_tickers else None
    out_dir = args.output_dir if args.output_dir else DEFAULT_OUTPUT_DIRECTORY

    update_daily_ohlcv_all_at_once(
        user_ticker_list=[],
        output_dir=out_dir,
        global_start_date=GLOBAL_START_DATE,
        override_ticker_list=override
    )

    print("\n--- Update complete ---")
