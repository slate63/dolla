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
LOCAL_TICKERS_FILE = "company_tickers.json"
DEFAULT_OUTPUT_DIRECTORY = '/data/daily_ohlcv'
GLOBAL_START_DATE = '1990-01-01'

MIN_FETCH_DELAY_SECONDS = 15
MAX_FETCH_DELAY_SECONDS = 30
STOCKS_PER_BUNCH = 10

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_and_clean_ohlcv(ticker: str, start: str, end: str = datetime.datetime.now().strftime('%Y-%m-%d'), interval: str = '1d') -> pd.DataFrame:
    if datetime.datetime.strptime(start, '%Y-%m-%d') > datetime.datetime.strptime(end, '%Y-%m-%d'):
        logging.warning(f"Start date {start} is after end date {end} for {ticker}. Returning empty DataFrame.")
        return pd.DataFrame()

    try:
        yf_ticker = yf.Ticker(ticker)
        df = yf_ticker.history(start=start, end=end, interval=interval, actions=True, auto_adjust=False)
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()

    if df.empty:
        logging.warning(f"No data returned for {ticker} within the range {start} to {end}.")
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
    return df

def add_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.sort_values(by='timestamp').copy()
    df['sma_50'] = df['close'].rolling(window=50, min_periods=1).mean()
    df['sma_200'] = df['close'].rolling(window=200, min_periods=1).mean()
    return df

def downcast_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    for col in ['open', 'high', 'low', 'close', 'dividends', 'stock splits', 'sma_50', 'sma_200']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], downcast='float', errors='coerce')
    if 'volume' in df.columns:
        df['volume'] = pd.to_numeric(df['volume'], downcast='integer', errors='coerce').astype('Int64')
    if 'symbol' in df.columns:
        df['symbol'] = df['symbol'].astype('category')
    return df

def get_sec_tickers(url: str, local_file: str) -> list:
    logging.info(f"Attempting to fetch tickers from SEC: {url}")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        logging.info("Successfully fetched tickers from SEC online.")
        return [v.get('ticker').upper() for v in data.values() if v.get('ticker')]
    except requests.exceptions.RequestException as e:
        logging.warning(f"Failed to fetch SEC tickers: {e}. Trying local file {local_file}")
        if os.path.exists(local_file):
            try:
                with open(local_file, 'r') as f:
                    data = json.load(f)
                return [v.get('ticker').upper() for v in data.values() if v.get('ticker')]
            except Exception as e:
                logging.error(f"Local file error: {e}")
                return []
        else:
            return []

def update_daily_ohlcv_all_at_once(user_ticker_list: list, output_dir: str, global_start_date: str, override_ticker_list: list = None):
    os.makedirs(output_dir, exist_ok=True)
    tickers = sorted(set(override_ticker_list if override_ticker_list else user_ticker_list + get_sec_tickers(SEC_TICKERS_URL, LOCAL_TICKERS_FILE)))
    if not tickers:
        logging.info("No tickers to process. Exiting.")
        return

    logging.info(f"Processing {len(tickers)} tickers")
    ticker_last_dates = {}
    existing_files = sorted([f for f in os.listdir(output_dir) if f.endswith('.parquet')])
    today = datetime.datetime.now().date()

    for fname in existing_files:
        path = os.path.join(output_dir, fname)
        try:
            df_meta = pd.read_parquet(path, columns=['timestamp', 'symbol'])
            for symbol, ts in df_meta.groupby('symbol')['timestamp'].max().to_dict().items():
                if symbol not in ticker_last_dates or ts > ticker_last_dates[symbol]:
                    ticker_last_dates[symbol] = ts
        except Exception as e:
            logging.warning(f"Skipping {fname} due to error: {e}")

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
        logging.info("All tickers up-to-date. Exiting.")
        return

    all_data = []
    for i in range(0, len(fetch_list), STOCKS_PER_BUNCH):
        bunch = fetch_list[i:i + STOCKS_PER_BUNCH]
        logging.info(f"\nProcessing bunch: {bunch}")
        for ticker in bunch:
            try:
                df_new = load_and_clean_ohlcv(ticker, fetch_requests[ticker], today_str)
                if not df_new.empty:
                    existing_df = []
                    for fname in existing_files:
                        path = os.path.join(output_dir, fname)
                        try:
                            df_existing = pd.read_parquet(path, columns=['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'dividends', 'stock splits'])
                            df_existing = df_existing[df_existing['symbol'] == ticker]
                            if not df_existing.empty:
                                existing_df.append(df_existing)
                        except:
                            continue

                    if existing_df:
                        df_history = pd.concat(existing_df + [df_new], ignore_index=True)
                    else:
                        df_history = df_new

                    df_history.drop_duplicates(subset=['timestamp', 'symbol'], keep='last', inplace=True)
                    df_history = df_history.sort_values(by='timestamp')

                    df_with_ma = add_moving_averages(df_history)
                    df_with_ma = downcast_ohlcv(df_with_ma)
                    df_to_save = df_with_ma[df_with_ma['timestamp'] >= pd.to_datetime(fetch_requests[ticker])]
                    all_data.append(df_to_save)
            except Exception as e:
                logging.error(f"Failed to process {ticker}: {e}")

        if (i + STOCKS_PER_BUNCH) < len(fetch_list):
            delay = random.randint(MIN_FETCH_DELAY_SECONDS, MAX_FETCH_DELAY_SECONDS)
            logging.info(f"Pausing for {delay} seconds before next bunch...")
            time.sleep(delay)

    if not all_data:
        logging.info("No new data fetched. Exiting.")
        return

    final_data = pd.concat(all_data, ignore_index=True)
    final_data['timestamp'] = pd.to_datetime(final_data['timestamp'])
    final_data['date_str'] = final_data['timestamp'].dt.strftime('%Y-%m-%d')
    grouped = final_data.groupby('date_str')

    for date_str, group in grouped:
        path = os.path.join(output_dir, f"{date_str}.parquet")
        existing = pd.DataFrame()
        if os.path.exists(path):
            try:
                existing = pd.read_parquet(path)
            except:
                pass

        combined = pd.concat([existing, group.drop(columns=['date_str'])], ignore_index=True)
        combined.drop_duplicates(subset=['timestamp', 'symbol'], keep='last', inplace=True)
        combined = combined.sort_values(by=['timestamp', 'symbol'])

        if not combined.empty:
            combined.to_parquet(path, engine='pyarrow', compression='snappy', index=False)
            logging.info(f"Saved {path} with {len(combined)} rows.")
        elif os.path.exists(path):
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
