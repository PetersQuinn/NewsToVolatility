import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from src.config import RAW_GDELT_DIR
from src.utils.net import get_with_retries
from src.utils.io import write_parquet

load_dotenv()

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"

def pull_gdelt_docs(query: str, start_dt: str, end_dt: str, mode: str = "ArtList", maxrecords: int = 250):
    """
    start_dt/end_dt format: YYYYMMDDHHMMSS (GDELT style)
    """
    params = {
        "query": query,
        "mode": mode,
        "format": "json",
        "startdatetime": start_dt,
        "enddatetime": end_dt,
        "maxrecords": maxrecords,
        "sort": "HybridRel",
    }
    resp = get_with_retries(GDELT_DOC_API, params=params, timeout=30, max_tries=6, base_sleep=1.0)
    js = resp.json()

    articles = js.get("articles", [])
    if not articles:
        return pd.DataFrame()

    df = pd.DataFrame(articles)
    return df

def main():
    query = '("Federal Reserve" OR FOMC OR Powell) sourceCountry:US lang:English'
    start_dt = "20250101000000"
    end_dt   = "20250108000000"

    df = pull_gdelt_docs(query=query, start_dt=start_dt, end_dt=end_dt)
    out = RAW_GDELT_DIR / f"gdelt_docs_{start_dt}_{end_dt}.parquet"
    write_parquet(df, out)
    print(f"Saved {len(df):,} articles -> {out}")

if __name__ == "__main__":
    main()