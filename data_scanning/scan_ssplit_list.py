import os
import pandas as pd
import argparse
from tqdm import tqdm
import pyarrow.parquet as pq

def has_required_columns(path, required_columns):
    try:
        schema = pq.read_schema(path)
        return all(col in schema.names for col in required_columns)
    except Exception as e:
        print(f"Could not read schema from {path}: {e}")
        return False

def scan_for_splits_from_files(
    files: list[str],
    ticker_filter: str = None,
    output_file: str = None,
    output_parquet: str = None,
    show_all: bool = False
):
    if not files:
        print("No files provided. Please make sure your pattern matched files.")
        return

    print(f"Received {len(files)} file(s). First 5:")
    for i, f in enumerate(files[:5]):
        print(f"  {i+1}. {f}")
    if len(files) > 5:
        print(f"  ...and {len(files) - 5} more.")

    print(f"\nScanning {len(files)} files for stock splits...")

    total_splits = 0
    results = []
    required_cols = ['timestamp', 'symbol', 'stock splits']
    files_with_splits = 0

    for path in tqdm(files, desc="Scanning files"):
        if not os.path.exists(path):
            print(f"Skipping {path}: file does not exist.")
            continue

        if not has_required_columns(path, required_cols):
            print(f"Skipping {path}: missing one or more required columns.")
            continue

        try:
            df = pd.read_parquet(path, columns=required_cols)
            if ticker_filter:
                df = df[df['symbol'] == ticker_filter.upper()]
            df = df[df['stock splits'] != 0]
            if not df.empty:
                df['file'] = os.path.basename(path)
                results.append(df)
                total_splits += len(df)
                files_with_splits += 1
        except Exception as e:
            print(f"Failed to read {path}: {e}")
            continue

    print(f"\nScan complete.")
    print(f"  Files scanned: {len(files)}")
    print(f"  Files with split records: {files_with_splits}")
    print(f"  Total split entries found: {total_splits}")

    if results:
        final_df = pd.concat(results, ignore_index=True)
        final_df = final_df[['timestamp', 'symbol', 'stock splits', 'file']]

        print("\n--- Stock Splits Summary ---")
        if show_all:
            print(final_df.to_string(index=False))
        else:
            if len(final_df) > 20:
                print(final_df.head(20).to_string(index=False))
                print(f"... (showing 20 of {len(final_df)} rows)")
            else:
                print(final_df.to_string(index=False))

        if output_file:
            final_df.to_csv(output_file, index=False)
            print(f"\nResults written to CSV: {output_file}")

        if output_parquet:
            final_df.to_parquet(output_parquet, index=False)
            print(f"\nResults written to Parquet: {output_parquet}")
    else:
        print("No stock split records found.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scan Parquet files for stock split data (no quotes needed).")
    parser.add_argument('file_list', nargs='+', help='List of Parquet files. Shell expansion like ../data/2025* is supported.')
    parser.add_argument('--ticker', help='Optional: Filter by a specific stock ticker.')
    parser.add_argument('--output', help='Optional: CSV file to save matching split records.')
    parser.add_argument('--output-parquet', help='Optional: Parquet file to save matching split records.')
    parser.add_argument('--show-all', action='store_true', help='Show all split records instead of limiting to 20 rows.')
    args = parser.parse_args()

    scan_for_splits_from_files(
        args.file_list,
        args.ticker,
        args.output,
        args.output_parquet,
        args.show_all
    )
