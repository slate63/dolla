import os
import pandas as pd
import argparse
from tqdm import tqdm  # pip install tqdm

def scan_for_stock_splits(data_dir: str, ticker_filter: str = None):
    if not os.path.exists(data_dir):
        print(f"Directory not found: {data_dir}")
        return

    total_stock_splits = 0
    files = sorted([f for f in os.listdir(data_dir) if f.endswith('.parquet')])
    if not files:
        print("No Parquet files found.")
        return

    print(f"Scanning {len(files)} files in '{data_dir}' for stock splits...")

    results = []

    for file in tqdm(files, desc="Scanning files"):
        path = os.path.join(data_dir, file)
        try:
            df = pd.read_parquet(path, columns=['timestamp', 'symbol', 'stock splits'])
            if ticker_filter:
                df = df[df['symbol'] == ticker_filter.upper()]
            df = df[df['stock splits'] != 0]
            if not df.empty:
                df['file'] = file  # Track which file the data came from
                results.append(df)
                total_stock_splits += len(df)
        except Exception as e:
            print(f"Failed to read {file}: {e}")

    print(f"\nScan complete. Found {total_stock_splits} stock splits record(s).")

    if results:
        final_df = pd.concat(results, ignore_index=True)
        final_df = final_df[['timestamp', 'symbol', 'stock splits', 'file']]
        print("\n--- Stock Splits Summary ---")
        print(final_df.to_string(index=False))
    else:
        print("No dividend records found.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scan OHLCV Parquet files for stock splits data.")
    parser.add_argument('data_dir', help='Path to directory containing daily OHLCV parquet files.')
    parser.add_argument('--ticker', help='Optional: Filter by a specific stock ticker.')
    args = parser.parse_args()

    scan_for_stock_splits(args.data_dir, args.ticker)
