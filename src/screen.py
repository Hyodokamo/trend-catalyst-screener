from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import math
import pandas as pd

@dataclass
class ScreenParams:
    rvol_min: float = 2.0
    close_loc_min: float = 0.70
    near_high20_ratio: float = 0.98
    require_ma10: bool = True
    require_breakout20: bool = True
    require_ma200: bool = True

def market_filter_ok(prices_dir: Path, index_ticker: str, ma_days: int = 50) -> bool:
    path = prices_dir / f"{index_ticker}.parquet"
    if not path.exists():
        return True
    df = pd.read_parquet(path).sort_index()
    if len(df) < ma_days + 2:
        return True
    close = df["Close"]
    ma_prev = close.rolling(ma_days).mean().shift(1)
    d0 = df.index[-1]
    if pd.isna(ma_prev.loc[d0]):
        return True
    return bool(close.loc[d0] > ma_prev.loc[d0])

def screen_one_ticker(prices_dir: Path, ticker: str, p: ScreenParams) -> dict:
    path = prices_dir / f"{ticker}.parquet"
    if not path.exists():
        return {}
    df = pd.read_parquet(path).sort_index()
    if len(df) < 220:
        return {}

    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    vol = df["Volume"]

    ma10_prev = close.rolling(10).mean().shift(1)
    ma200_prev = close.rolling(200).mean().shift(1)
    vol20_prev = vol.rolling(20).mean().shift(1)
    high20_prev = high.rolling(20).max().shift(1)

    d0 = df.index[-1]
    row = df.loc[d0]

    denom = float(row["High"] - row["Low"])
    close_loc = float((row["Close"] - row["Low"]) / denom) if denom != 0 else 0.0
    v20 = vol20_prev.loc[d0]
    rvol = float(row["Volume"] / v20) if (v20 is not None and not pd.isna(v20) and v20 != 0) else math.nan

    ma10_ok = bool(row["Close"] > ma10_prev.loc[d0]) if not pd.isna(ma10_prev.loc[d0]) else False
    ma200_ok = bool(row["Close"] > ma200_prev.loc[d0]) if not pd.isna(ma200_prev.loc[d0]) else False

    h20 = high20_prev.loc[d0]
    breakout_ok = bool(row["Close"] >= h20) if not pd.isna(h20) else False
    near_breakout_ok = bool(row["Close"] >= p.near_high20_ratio * h20) if not pd.isna(h20) else False

    passed = (
        (not pd.isna(rvol) and rvol >= p.rvol_min) and
        (close_loc >= p.close_loc_min) and
        ((not p.require_ma10) or ma10_ok) and
        ((not p.require_breakout20) or (breakout_ok or near_breakout_ok)) and
        ((not p.require_ma200) or ma200_ok)
    )

    score = 0
    score += 2 if (not pd.isna(rvol) and rvol >= p.rvol_min) else 0
    score += 1 if close_loc >= p.close_loc_min else 0
    score += 1 if ma10_ok else 0
    score += 1 if (breakout_ok or near_breakout_ok) else 0
    score += 1 if ma200_ok else 0

    return {
        "date": str(d0.date()),
        "ticker": ticker,
        "close": float(row["Close"]),
        "rvol": rvol,
        "close_loc": close_loc,
        "ma10_ok": ma10_ok,
        "ma200_ok": ma200_ok,
        "breakout20_ok": breakout_ok,
        "near_breakout_ok": near_breakout_ok,
        "passed": passed,
        "score": score,
    }

def run_screen(
    prices_dir: Path,
    univ_us: pd.DataFrame,
    univ_jp: pd.DataFrame,
    p: ScreenParams,
    us_index_ok: bool,
    jp_index_ok: bool,
) -> pd.DataFrame:
    tickers_us = univ_us[univ_us["enabled"] == True]["ticker"].astype(str).tolist()
    tickers_jp = univ_jp[univ_jp["enabled"] == True]["ticker"].astype(str).tolist()

    rows = []
    if us_index_ok:
        for t in tickers_us:
            r = screen_one_ticker(prices_dir, t, p)
            if r:
                r["market"] = "US"
                rows.append(r)

    if jp_index_ok:
        for t in tickers_jp:
            r = screen_one_ticker(prices_dir, t, p)
            if r:
                r["market"] = "JP"
                rows.append(r)

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out = out[out["passed"] == True].sort_values(["score", "rvol"], ascending=False).reset_index(drop=True)
    return out
