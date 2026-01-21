from __future__ import annotations
from pathlib import Path

US_INDEX_TICKER = "^GSPC"
JP_INDEX_TICKER = "1306.T"

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
PRICES_DIR = DATA_DIR / "prices"
OUTPUTS_DIR = DATA_DIR / "outputs"
DOCS_DIR = BASE_DIR / "docs"

UNIV_US = DATA_DIR / "universe_us.csv"
UNIV_JP = DATA_DIR / "universe_jp.csv"
EXCLUDE = DATA_DIR / "universe_exclude.csv"
TOO_SHORT = DATA_DIR / "universe_too_short.csv"

MIN_ROWS = 260
BATCH_SIZE = 20
MAX_ROWS_KEEP = 1200

def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PRICES_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
