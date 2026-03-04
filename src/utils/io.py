from pathlib import Path
import pandas as pd

def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=True)

def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)

def safe_concat_dedup(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    # assuming index is DatetimeIndex
    out = pd.concat([existing, new]).sort_index()
    out = out[~out.index.duplicated(keep="last")]
    return out