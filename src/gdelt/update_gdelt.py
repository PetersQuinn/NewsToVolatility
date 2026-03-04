import pandas as pd
from datetime import datetime, timedelta, timezone
from time import sleep

from src.config import RAW_GDELT_DIR
from src.gdelt.pull_gdelt import pull_gdelt_docs
from src.utils.io import write_parquet

def yyyymmddhhmmss(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")

def main():
    query = '(AI OR "artificial intelligence" OR OpenAI OR Nvidia OR "large language model") lang:English'
    days_back = 14  # start small; expand once stable

    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    for i in range(days_back, 0, -1):
        day_start = (end - timedelta(days=i)).replace(hour=0)
        day_end   = day_start + timedelta(days=1)

        start_dt = yyyymmddhhmmss(day_start)
        end_dt   = yyyymmddhhmmss(day_end)

        out_path = RAW_GDELT_DIR / f"docs_{start_dt}_{end_dt}.parquet"
        if out_path.exists():
            print(f"Skip existing: {out_path.name}")
            continue

        df = pull_gdelt_docs(query=query, start_dt=start_dt, end_dt=end_dt, maxrecords=250)
        write_parquet(df, out_path)
        print(f"Wrote {len(df):,} rows -> {out_path.name}")

        # be nice to the API
        sleep(1.0)

if __name__ == "__main__":
    main()