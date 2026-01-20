from __future__ import annotations
from pathlib import Path
import pandas as pd
import yfinance as yf

FIELDS = ["Open", "High", "Low", "Close", "Volume"]

def fetch_ohlcv_batch(tickers, period="60d", interval="1d") -> dict[str, pd.DataFrame]:
    if isinstance(tickers, str):
        tickers = [tickers]
    tickers = [t for t in tickers if t]
    if not tickers:
        return {}

    df = yf.download(
        tickers=" ".join(tickers),
        period=period,
        interval=interval,
        auto_adjust=False,
        group_by="ticker",
        threads=True,
        progress=False,
    )
    out: dict[str, pd.DataFrame] = {}
    if df is None or df.empty:
        return out

    if not isinstance(df.columns, pd.MultiIndex):
        sub = df.copy()
        if all(c in sub.columns for c in FIELDS):
            sub = sub[FIELDS].dropna()
            sub.columns.name = None
            out[tickers[0]] = sub
        return out

    lvl0 = set(df.columns.get_level_values(0))
    lvl1 = set(df.columns.get_level_values(1))
    fields = set(FIELDS)

    if any(t in lvl0 for t in tickers) and fields.issubset(lvl1):
        for t in tickers:
            if t not in lvl0:
                continue
            sub = df[t]
            if isinstance(sub, pd.Series) or sub.empty:
                continue
            if not fields.issubset(set(sub.columns)):
                continue
            sub = sub[FIELDS].dropna()
            sub.columns.name = None
            out[t] = sub
        return out

    if any(t in lvl1 for t in tickers) and fields.issubset(lvl0):
        for t in tickers:
            if t not in lvl1:
                continue
            sub = df.xs(t, level=1, axis=1)
            if sub.empty:
                continue
            if not fields.issubset(set(sub.columns)):
                continue
            sub = sub[FIELDS].dropna()
            sub.columns.name = None
            out[t] = sub
        return out

    return out

def upsert_parquet(ticker: str, new_df: pd.DataFrame, prices_dir: Path, max_rows=1200) -> bool:
    prices_dir.mkdir(parents=True, exist_ok=True)
    path = prices_dir / f"{ticker}.parquet"

    new_df = new_df.copy()
    new_df = new_df[FIELDS].dropna()
    new_df.columns.name = None

    if path.exists():
        old = pd.read_parquet(path).sort_index()
        old = old[FIELDS]
        old.columns.name = None
        merged = pd.concat([old, new_df], axis=0)
    else:
        merged = new_df

    merged = merged[~merged.index.duplicated(keep="last")].sort_index()
    if max_rows is not None and len(merged) > max_rows:
        merged = merged.iloc[-max_rows:]

    merged.columns.name = None
    merged.to_parquet(path)
    return True

def bulk_update(tickers: list[str], prices_dir: Path, period: str, batch_size=80, max_rows=1200):
    saved, missing = [], []
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        got = fetch_ohlcv_batch(batch, period=period)
        for t in batch:
            if t not in got:
                missing.append(t)
                continue
            upsert_parquet(t, got[t], prices_dir, max_rows=max_rows)
            saved.append(t)
    return saved, missing
