from __future__ import annotations
import argparse
from datetime import datetime, timezone
import pandas as pd

from src.config import (
    ensure_dirs, PRICES_DIR, OUTPUTS_DIR, DOCS_DIR,
    UNIV_US, UNIV_JP, EXCLUDE, TOO_SHORT,
    US_INDEX_TICKER, JP_INDEX_TICKER,
    MIN_ROWS, BATCH_SIZE, MAX_ROWS_KEEP,
)
from src.universe import build_universe_us_sp500, build_universe_jp_topix_newindex
from src.prices import bulk_update
from src.audit import is_healthy_parquet, delete_empty_parquets, update_exclude_and_shortlists
from src.screen import ScreenParams, market_filter_ok, run_screen
from src.dashboard import build_dashboard

def load_or_build_universe():
    if not UNIV_US.exists():
        build_universe_us_sp500(UNIV_US)
    if not UNIV_JP.exists():
        build_universe_jp_topix_newindex(UNIV_JP)
    us = pd.read_csv(UNIV_US)
    jp = pd.read_csv(UNIV_JP)
    return us, jp

def apply_exclude(tickers: list[str]) -> list[str]:
    if EXCLUDE.exists():
        ex = pd.read_csv(EXCLUDE)
        exclude_set = set(ex["ticker"].astype(str))
        tickers = [t for t in tickers if t not in exclude_set]
    return tickers

def apply_tooshort(univ: pd.DataFrame) -> pd.DataFrame:
    if TOO_SHORT.exists():
        ts = pd.read_csv(TOO_SHORT)
        bad = set(ts["ticker"].astype(str))
        univ = univ[~univ["ticker"].astype(str).isin(bad)].copy()
    return univ

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--initial", action="store_true", help="force initial(600d) build")
    ap.add_argument("--period-initial", default="600d")
    ap.add_argument("--period-daily", default="60d")
    args = ap.parse_args()

    ensure_dirs()

    us, jp = load_or_build_universe()
    us = apply_tooshort(us)
    jp = apply_tooshort(jp)

    tickers_all = us[us["enabled"] == True]["ticker"].astype(str).tolist() \
                + jp[jp["enabled"] == True]["ticker"].astype(str).tolist() \
                + [US_INDEX_TICKER, JP_INDEX_TICKER]
    tickers_all = apply_exclude(tickers_all)

    parquet_count = len(list(PRICES_DIR.glob("*.parquet")))
    do_initial = args.initial or (parquet_count < 50)
    period = args.period_initial if do_initial else args.period_daily

    saved, missing_fetch = bulk_update(
        tickers_all,
        PRICES_DIR,
        period=period,
        batch_size=BATCH_SIZE,
        max_rows=MAX_ROWS_KEEP,
    )

    healthy = [t for t in tickers_all if is_healthy_parquet(PRICES_DIR, t, min_rows=MIN_ROWS)]
    missing_real = [t for t in tickers_all if t not in healthy]

    deleted = delete_empty_parquets(PRICES_DIR, tickers_all)
    miss_df = update_exclude_and_shortlists(
        PRICES_DIR, missing_real,
        exclude_path=EXCLUDE,
        tooshort_path=TOO_SHORT,
        min_rows=MIN_ROWS
    )

    us_ok = market_filter_ok(PRICES_DIR, US_INDEX_TICKER, ma_days=50)
    jp_ok = market_filter_ok(PRICES_DIR, JP_INDEX_TICKER, ma_days=50)

    params = ScreenParams()
    screen_df = run_screen(PRICES_DIR, us, jp, params, us_ok, jp_ok)

    out_csv = OUTPUTS_DIR / "screen_latest.csv"
    out_audit = OUTPUTS_DIR / "audit_latest.csv"
    meta = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "initial" if do_initial else "daily",
        "period": period,
        "saved": len(saved),
        "missing_fetch": len(missing_fetch),
        "healthy": len(healthy),
        "missing_real": len(missing_real),
        "deleted_empty": len(deleted),
        "us_index_ok": bool(us_ok),
        "jp_index_ok": bool(jp_ok),
    }

    screen_df.to_csv(out_csv, index=False, encoding="utf-8")
    miss_df.to_csv(out_audit, index=False, encoding="utf-8")

    docs_csv = DOCS_DIR / "screen_latest.csv"
    docs_html = DOCS_DIR / "index.html"
    screen_df.to_csv(docs_csv, index=False, encoding="utf-8")
    build_dashboard(docs_html, screen_df, meta, csv_rel_path="screen_latest.csv")

    print("meta:", meta)
    print("candidates:", 0 if screen_df.empty else len(screen_df))

if __name__ == "__main__":
    main()
