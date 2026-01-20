from __future__ import annotations
from src.config import ensure_dirs, UNIV_US, UNIV_JP
from src.universe import build_universe_us_sp500, build_universe_jp_topix_newindex

def main():
    ensure_dirs()
    build_universe_us_sp500(UNIV_US)
    build_universe_jp_topix_newindex(UNIV_JP)
    print("ok: universe built")

if __name__ == "__main__":
    main()
