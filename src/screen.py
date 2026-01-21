from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict
import math
import pandas as pd

# config の定数名が揺れても動くようにする
try:
    from .config import PRICES_DIR, US_INDEX_TICKER, JP_INDEX_TICKER
except Exception:
    from .config import PRICES_DIR, US_INDEX as US_INDEX_TICKER, JP_INDEX_PROXY as JP_INDEX_TICKER


@dataclass
class ScreenParams:
    # --- 既存テクニカル（v1） ---
    rvol_min: float = 2.0
    close_loc_min: float = 0.70
    near_high20_ratio: float = 0.98
    require_ma10: bool = True
    require_breakout20: bool = True
    require_ma200: bool = True

    # --- 代理カタリスト（v1.1） ---
    enable_proxy: bool = True

    # ギャップ加点（Open vs 前日Close）
    gap_up_1: float = 0.03     # +3% で +1
    gap_up_2: float = 0.06     # +6% で +2
    gap_overheat: float = 0.12 # +12% 以上は「過熱候補」

    # TR/ATR 加点（値幅異常）
    tr_ratio_1: float = 1.8    # TR/ATR20 >= 1.8 で +1
    tr_ratio_2: float = 2.5    # >= 2.5 で +2
    tr_exhaust: float = 3.0    # >= 3.0 は「失速候補」に使う

    # 加熱・失速除外（Exhaustion回避）
    exclude_exhaust: bool = True
    exhaust_close_loc_max: float = 0.60  # 過熱ギャップなのに引け弱い
    split_suspect_gap_abs: float = 0.25  # ±25%超のギャップは分割/権利等の疑い（参考フラグ）


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


def screen_one_ticker(ticker: str, p: ScreenParams) -> Dict:
    path = PRICES_DIR / f"{ticker}.parquet"
    if not path.exists():
        return {}
    df = pd.read_parquet(path).sort_index()
    if len(df) < 220:
        return {}

    close = df["Close"]
    open_ = df["Open"]
    high  = df["High"]
    low   = df["Low"]
    vol   = df["Volume"]

    ma10_prev   = close.rolling(10).mean().shift(1)
    ma200_prev  = close.rolling(200).mean().shift(1)
    vol20_prev  = vol.rolling(20).mean().shift(1)
    high20_prev = high.rolling(20).max().shift(1)

    d0 = df.index[-1]
    row = df.loc[d0]

    # --- 既存テクニカル ---
    denom = float(row["High"] - row["Low"])
    close_loc = float((row["Close"] - row["Low"]) / denom) if denom != 0 else 0.0

    rvol = float(row["Volume"] / vol20_prev.loc[d0]) if (d0 in vol20_prev.index and not pd.isna(vol20_prev.loc[d0]) and vol20_prev.loc[d0] != 0) else math.nan

    ma10_ok = bool(row["Close"] > ma10_prev.loc[d0]) if not pd.isna(ma10_prev.loc[d0]) else False
    ma200_ok = bool(row["Close"] > ma200_prev.loc[d0]) if not pd.isna(ma200_prev.loc[d0]) else False

    h20 = high20_prev.loc[d0]
    breakout_ok = bool(row["Close"] >= h20) if not pd.isna(h20) else False
    near_breakout_ok = bool(row["Close"] >= p.near_high20_ratio * h20) if not pd.isna(h20) else False

    tech_pass = (
        (not pd.isna(rvol) and rvol >= p.rvol_min) and
        (close_loc >= p.close_loc_min) and
        ((not p.require_ma10) or ma10_ok) and
        ((not p.require_breakout20) or (breakout_ok or near_breakout_ok)) and
        ((not p.require_ma200) or ma200_ok)
    )

    tech_score = 0
    tech_score += 2 if (not pd.isna(rvol) and rvol >= p.rvol_min) else 0
    tech_score += 1 if close_loc >= p.close_loc_min else 0
    tech_score += 1 if ma10_ok else 0
    tech_score += 1 if (breakout_ok or near_breakout_ok) else 0
    tech_score += 1 if ma200_ok else 0

    # --- 代理カタリスト ---
    prev_close = float(close.iloc[-2]) if len(df) >= 2 else math.nan

    # ギャップ率
    gap_pct = math.nan
    if not pd.isna(prev_close) and prev_close != 0:
        gap_pct = float(row["Open"] / prev_close - 1.0)

    # TR/ATR20（ベクトル化で高速）
    prev_c = close.shift(1)
    tr_series = pd.concat([
        (high - low),
        (high - prev_c).abs(),
        (low - prev_c).abs()
    ], axis=1).max(axis=1)

    atr20_prev = tr_series.rolling(20).mean().shift(1)
    atr20_val = float(atr20_prev.loc[d0]) if (d0 in atr20_prev.index and not pd.isna(atr20_prev.loc[d0]) and atr20_prev.loc[d0] != 0) else math.nan

    tr = float(tr_series.loc[d0]) if d0 in tr_series.index and not pd.isna(tr_series.loc[d0]) else math.nan
    tr_ratio = float(tr / atr20_val) if (not pd.isna(tr) and not pd.isna(atr20_val) and atr20_val != 0) else math.nan

    proxy_score = 0
    gap_score = 0
    tr_score = 0

    if p.enable_proxy:
        # ギャップは上方向のみ加点（ロング前提）
        if not pd.isna(gap_pct) and gap_pct >= p.gap_up_1:
            gap_score = 1
            if gap_pct >= p.gap_up_2:
                gap_score = 2

        if not pd.isna(tr_ratio) and tr_ratio >= p.tr_ratio_1:
            tr_score = 1
            if tr_ratio >= p.tr_ratio_2:
                tr_score = 2

        proxy_score = gap_score + tr_score

    # 加熱・失速除外
    exhaust_flag = False
    exhaust_reason = ""

    if (not pd.isna(gap_pct)) and (gap_pct >= p.gap_overheat) and (close_loc < p.exhaust_close_loc_max):
        exhaust_flag = True
        exhaust_reason = "gap_overheat_and_weak_close"

    if (not pd.isna(tr_ratio)) and (tr_ratio >= p.tr_exhaust) and (float(row["Close"]) < float(row["Open"])):
        exhaust_flag = True
        exhaust_reason = exhaust_reason + "|wide_range_red" if exhaust_reason else "wide_range_red"

    split_suspect = False
    if not pd.isna(gap_pct) and abs(gap_pct) >= p.split_suspect_gap_abs:
        split_suspect = True

    passed = tech_pass and (not (p.exclude_exhaust and exhaust_flag))
    score_total = tech_score + proxy_score

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

        # 互換
        "score": tech_score,

        # 新規
        "proxy_score": proxy_score,
        "score_total": score_total,
        "prev_close": prev_close,
        "gap_pct": gap_pct,
        "tr": tr,
        "atr20_prev": atr20_val,
        "tr_ratio": tr_ratio,
        "gap_score": gap_score,
        "tr_score": tr_score,
        "exhaust_flag": exhaust_flag,
        "exhaust_reason": exhaust_reason,
        "split_suspect": split_suspect,
    }


def run_screen(univ_us: pd.DataFrame, univ_jp: pd.DataFrame, p: ScreenParams, limit: int | None = None) -> pd.DataFrame:
    tickers_us = univ_us[univ_us["enabled"] == True]["ticker"].astype(str).tolist()
    tickers_jp = univ_jp[univ_jp["enabled"] == True]["ticker"].astype(str).tolist()

    if limit:
        tickers_us = tickers_us[:limit]
        tickers_jp = tickers_jp[:limit]

    us_ok = market_filter_ok(PRICES_DIR, US_INDEX_TICKER, ma_days=50)
    jp_ok = market_filter_ok(PRICES_DIR, JP_INDEX_TICKER, ma_days=50)

    rows = []
    for t in tickers_us:
        r = screen_one_ticker(t, p)
        if r:
            r["market"] = "US"
            r["index_ok"] = us_ok
            rows.append(r)

    for t in tickers_jp:
        r = screen_one_ticker(t, p)
        if r:
            r["market"] = "JP"
            r["index_ok"] = jp_ok
            rows.append(r)

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out = out[out["index_ok"] == True].copy()
    out = out[out["passed"] == True].copy()

    # 合計点優先 → RVOL → ギャップ → TR比
    sort_cols = ["score_total", "rvol", "gap_pct", "tr_ratio"]
    sort_cols = [c for c in sort_cols if c in out.columns]
    out = out.sort_values(sort_cols, ascending=False).reset_index(drop=True)
    return out
