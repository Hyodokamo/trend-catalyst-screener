from __future__ import annotations
from pathlib import Path
import pandas as pd
import json

def build_dashboard(out_html: Path, screen_df: pd.DataFrame, meta: dict, csv_rel_path: str = "screen_latest.csv") -> None:
    out_html.parent.mkdir(parents=True, exist_ok=True)

    meta_json = json.dumps(meta, ensure_ascii=False, indent=2)
    table_html = "<p>No candidates</p>" if screen_df.empty else screen_df.to_html(index=False, escape=False)

    html = f"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Trend×Catalyst Screener</title>
<style>
body {{ font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 20px; }}
h1 {{ margin: 0 0 8px; }}
pre {{ background: #f7f7f7; padding: 10px; overflow-x: auto; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #ddd; padding: 6px 8px; font-size: 13px; }}
th {{ background: #f6f6f6; text-align: left; }}
</style>
</head>
<body>
<h1>Trend×Catalyst Screener</h1>
<p><a href="{csv_rel_path}">Download CSV</a></p>
<pre>{meta_json}</pre>
{table_html}
</body>
</html>
"""
    out_html.write_text(html, encoding="utf-8")
