from __future__ import annotations
import io
from pathlib import Path
import pandas as pd
import requests

DATAHUB_SP500_CSV = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
RAW_GITHUB_SP500_CSV = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
JPX_TOPIX_WEIGHT_URL = "https://www.jpx.co.jp/automation/markets/indices/topix/files/topixweight_j.csv"

UA = "Mozilla/5.0 (compatible; trend-catalyst-screener/1.0)"

def build_universe_us_sp500(save_path: Path) -> pd.DataFrame:
    urls = [DATAHUB_SP500_CSV, RAW_GITHUB_SP500_CSV]
    last_err = None

    for url in urls:
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=30)
            r.raise_for_status()
            df = pd.read_csv(io.StringIO(r.text))
            if "Symbol" not in df.columns:
                raise RuntimeError(f"Symbol列がありません: cols={df.columns.tolist()}")

            out = pd.DataFrame({
                "ticker": df["Symbol"].astype(str).str.strip(),
                "enabled": True,
                "group": "SP500",
                "max_price": 10_000,
                "note": ""
            }).drop_duplicates(subset=["ticker"])

            out["ticker"] = out["ticker"].str.replace(".", "-", regex=False)
            out.to_csv(save_path, index=False, encoding="utf-8")
            return out
        except Exception as e:
            last_err = e

    raise RuntimeError(f"S&P500取得失敗: {last_err}")

def build_universe_jp_topix_newindex(save_path: Path) -> pd.DataFrame:
    r = requests.get(JPX_TOPIX_WEIGHT_URL, headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()

    text = None
    last_err = None
    for enc in ["cp932", "shift_jis", "utf-8-sig", "utf-8"]:
        try:
            text = r.content.decode(enc)
            break
        except UnicodeDecodeError as e:
            last_err = e
    if text is None:
        raise RuntimeError(f"JPX CSV decode失敗: {last_err}")

    df = pd.read_csv(io.StringIO(text))

    def find_col(substr: str) -> str:
        for c in df.columns:
            if substr in str(c):
                return c
        raise RuntimeError(f"列が見つかりません: {substr} / cols={df.columns.tolist()}")

    col_group = find_col("ニューインデックス区分")
    col_code = find_col("コード")

    target_groups = {"TOPIX Core30", "TOPIX Large70", "TOPIX Mid400"}

    d = df.copy()
    d[col_group] = d[col_group].astype(str).str.strip()
    d = d[d[col_group].isin(target_groups)].copy()

    d["code"] = d[col_code].astype(str).str.strip()
    d = d[d["code"].str.match(r"^\d{4}$")].copy()
    d["ticker"] = d["code"].apply(lambda x: f"{x}.T")

    out = pd.DataFrame({
        "code": d["code"],
        "ticker": d["ticker"],
        "enabled": True,
        "group": d[col_group].astype(str),
        "max_price": 200_000,
        "note": ""
    }).drop_duplicates(subset=["ticker"])

    out.to_csv(save_path, index=False, encoding="utf-8")
    return out
