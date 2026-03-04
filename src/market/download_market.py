import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

from src.config import RAW_MARKET_DIR
from src.utils.io import write_parquet

load_dotenv()

def download_ohlcv(ticker: str, start: str, end: str | None = None, interval: str = "1d") -> pd.DataFrame:
    df = yf.download(
        tickers=ticker,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=False,
        progress=False,
        threads=True,
    )
    if df is None or df.empty:
        raise RuntimeError(f"No data returned for {ticker}")

    # yfinance returns columns sometimes as multiindex if multiple tickers; force flat
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([c for c in col if c]) for col in df.columns]

    df.index = pd.to_datetime(df.index, utc=True)
    return df

def main():
    # Start with 1–3 assets; expand later
    targets = [
        {"ticker": "^GSPC", "start": "2000-01-01"},  # S&P 500 index
        {"ticker": "SPY",   "start": "2000-01-01"},  # ETF (more consistent)
        {"ticker": "^VIX",  "start": "2000-01-01"},  # implied vol proxy
    ]

    for t in targets:
        ticker = t["ticker"]
        df = download_ohlcv(ticker=ticker, start=t["start"])
        out_path = RAW_MARKET_DIR / f"{ticker.replace('^','')}_{'1d'}.parquet"
        write_parquet(df, out_path)
        print(f"Saved {ticker}: {len(df):,} rows -> {out_path}")

if __name__ == "__main__":
    main()