import pandas as pd
import os
import sys

def read_parquet_file(file_path: str) -> pd.DataFrame:
    """
    Reads a Parquet file and returns a DataFrame.
    """
    if not os.path.isfile(file_path):
        print(f"❌ File not found: {file_path}")
        return pd.DataFrame()

    try:
        df = pd.read_parquet(file_path)
        print(f"\n✅ Successfully read: {file_path}")
        return df
    except Exception as e:
        print(f"\n❌ Error reading Parquet file: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python readstocks.py <path_to_parquet_file>")
        sys.exit(1)

    path = sys.argv[1]
    df = read_parquet_file(path)

    if not df.empty:
        print("\n📄 Preview of the data:")
        print(df)
    else:
        print("\n⚠️ No data loaded.")
