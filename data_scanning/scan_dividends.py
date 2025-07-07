import os
import pandas as pd
import argparse

def scan_for_dividends(data_dir: str, ticker_filter: str = None):
    if not os.path.exists(data_dir):
        print(f"Directory not found: {data_dir}")
        return

    total_dividends = 0
    files = sorted([f for f in os.listdir(data_dir) if f.endswith('.parquet')])
    if not files:
        print("No Parquet files found.")
        return

    print(f"Scanning {len(files)} files in '{data_dir}' for dividends...")
    
    for file in files:
        path = os.path.join(data_dir, file)
        try:
            df = pd.read_parquet(path, columns=['timestamp', 'symbol', 'dividends'])
            if ticker_filter:
                df = df[df['symbol'] == ticker_filter.upper()]
            df = df[df['dividends'] != 0]
            if not df.empty:
                print(f"\n--- Dividends in {file} ---")
                print(df)
                total_dividends += len(df)
        except Exception as e:
            print(f"Failed to read {file}: {e}")

    print(f"\nScan complete. Found {total_dividends} dividend record(s).")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scan OHLCV Parquet files for dividend data.")
    parser.add_argument('data_dir', help='Path to directory containing daily OHLCV parquet files.')
    parser.add_argument('--ticker', help='Optional: Filter by a specific stock ticker.')
    args = parser.parse_args()

    scan_for_dividends(args.data_dir, args.ticker)
