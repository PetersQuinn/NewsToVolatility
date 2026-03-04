from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

RAW_MARKET_DIR = RAW_DIR / "market"
RAW_GDELT_DIR = RAW_DIR / "gdelt"

for p in [RAW_MARKET_DIR, RAW_GDELT_DIR, PROCESSED_DIR]:
    p.mkdir(parents=True, exist_ok=True)