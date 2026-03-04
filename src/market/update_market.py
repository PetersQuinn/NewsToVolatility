import pandas as pd
from datetime import timedelta

from src.config import RAW_MARKET_DIR
from src.utils.io import read_parquet, write_parquet, safe_concat_dedup
from src.market.download_market import download_ohlcv

def main():
    files = list(RAW_MARKET_DIR.glob("*_1d.parquet"))
    if not files:
        raise RuntimeError("No market parquet files found. Run download_market.py first.")

    for f in files:
        existing = read_parquet(f)
        last_ts = pd.to_datetime(existing.index.max())
        # pull a small overlap window to handle revisions/holidays/timezones
        start = (last_ts - pd.Timedelta(days=10)).date().isoformat()

        # infer ticker name back from filename (basic)
        base = f.stem.replace("_1d", "")
        ticker = "^" + base if base in ["GSPC", "VIX", "DJI", "IXIC"] else base

        new = download_ohlcv(ticker=ticker, start=start)
        merged = safe_concat_dedup(existing, new)
        write_parquet(merged, f)
        print(f"Updated {ticker}: {len(existing):,} -> {len(merged):,} rows")

if __name__ == "__main__":
    main()