from __future__ import annotations
from pathlib import Path
import pandas as pd

FIELDS = {"Open", "High", "Low", "Close", "Volume"}

def is_healthy_parquet(prices_dir: Path, ticker: str, min_rows: int) -> bool:
    p = prices_dir / f"{ticker}.parquet"
    if not p.exists():
        return False
    df = pd.read_parquet(p)
    if df is None or df.empty:
        return False
    if not FIELDS.issubset(set(df.columns)):
        return False
    if len(df.dropna()) < min_rows:
        return False
    return True

def classify_missing(prices_dir: Path, tickers: list[str], min_rows: int) -> pd.DataFrame:
    # 空でも列を持たせる（ここが今回の落ち所）
    rows = []
    for t in tickers:
        p = prices_dir / f"{t}.parquet"
        if not p.exists():
            rows.append({"ticker": t, "reason": "missing_file"})
            continue
        df = pd.read_parquet(p)
        if df is None or df.empty:
            rows.append({"ticker": t, "reason": "empty"})
            continue
        if len(df.dropna()) < min_rows:
            rows.append({"ticker": t, "reason": f"too_short<{min_rows}"})
            continue
        rows.append({"ticker": t, "reason": "ok"})
    return pd.DataFrame(rows, columns=["ticker", "reason"])

def update_exclude_and_shortlists(
    prices_dir: Path,
    missing_real: list[str],
    exclude_path: Path,
    tooshort_path: Path,
    min_rows: int,
) -> pd.DataFrame:
    miss_df = classify_missing(prices_dir, missing_real, min_rows=min_rows)

    # 欠損がゼロなら何もすることがない（でも空の監査表は返す）
    if miss_df.empty:
        return miss_df

    to_ex = miss_df[miss_df["reason"].isin(["empty", "missing_file"])].copy()
    if not to_ex.empty:
        to_ex = to_ex.assign(reason="yfinance_no_data_or_file_missing")[["ticker", "reason"]]
        if exclude_path.exists():
            ex = pd.read_csv(exclude_path)
            ex = pd.concat([ex, to_ex], ignore_index=True).drop_duplicates(subset=["ticker"], keep="last")
        else:
            ex = to_ex
        ex.to_csv(exclude_path, index=False, encoding="utf-8")

    to_short = miss_df[miss_df["reason"].str.startswith("too_short")].copy()
    if not to_short.empty:
        to_short.to_csv(tooshort_path, index=False, encoding="utf-8")

    return miss_df

def delete_empty_parquets(prices_dir: Path, tickers: list[str]) -> list[str]:
    deleted = []
    for t in tickers:
        p = prices_dir / f"{t}.parquet"
        if not p.exists():
            continue
        try:
            df = pd.read_parquet(p)
            if df is None or df.empty:
                p.unlink()
                deleted.append(t)
        except Exception:
            p.unlink()
            deleted.append(t)
    return deleted
