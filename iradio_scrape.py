import argparse
import datetime as dt
import time
from pathlib import Path

import pandas as pd
import requests

BASE_URL = "https://www.bcc.com.tw/news3_search.asp"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}

def fetch_html(params: dict, retries: int = 5, timeout: int = 25) -> str:
    last_err = None
    for i in range(retries):
        try:
            r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=timeout)
            if r.status_code == 200 and r.text:
                return r.text
            last_err = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last_err = e
        time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"Failed to fetch {BASE_URL} params={params}. last_err={last_err}")

def extract_table(html: str) -> pd.DataFrame:
    tables = pd.read_html(html)
    if not tables:
        raise ValueError("No tables found on the page (site layout may have changed).")

    # 優先挑含「歌曲名稱」欄的表（避免抓到版面表）
    for t in tables:
        cols = [str(c) for c in t.columns]
        if any("歌曲名稱" in c for c in cols):
            return t.dropna(how="all")

    # 保底：取最大張表
    t = max(tables, key=lambda x: x.shape[0] * x.shape[1])
    return t.dropna(how="all")

def fetch_dt_all_pages(dt_days_ago: int, max_pages: int = 50) -> pd.DataFrame:
    dfs = []
    for p in range(1, max_pages + 1):
        html = fetch_html({"dt": str(dt_days_ago), "p": str(p)})
        df = extract_table(html)

        if df.shape[0] == 0:
            break

        df["dt_days_ago"] = str(dt_days_ago)
        df["page"] = str(p)
        df["scraped_at"] = dt.datetime.now().isoformat(timespec="seconds")
        dfs.append(df)

        # 通常最後一頁資料會變少，做個保守停止條件
        if df.shape[0] < 5:
            break

        time.sleep(0.6)

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

def merge_dedupe(existing_path: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    if existing_path.exists():
        old_df = pd.read_csv(existing_path, dtype=str, encoding="utf-8-sig")
        combined = pd.concat([old_df, new_df.astype(str)], ignore_index=True)
    else:
        combined = new_df.astype(str)

    key_candidates = ["日期", "播出時間", "歌曲名稱", "演唱(奏)者"]
    keys = [k for k in key_candidates if k in combined.columns]
    if keys:
        combined = combined.drop_duplicates(subset=keys, keep="last")
    else:
        combined = combined.drop_duplicates(keep="last")

    return combined

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", type=int, default=0, help="0=today, 1=yesterday, 2=two days ago...")
    ap.add_argument("--max-pages", type=int, default=50)
    ap.add_argument("--out", type=str, default="data/iradio_today.csv")
    ap.add_argument("--append-dedupe", action="store_true")
    args = ap.parse_args()

    df = fetch_dt_all_pages(args.dt, args.max_pages)
    if df.empty:
        raise SystemExit("No data fetched (site may be down or layout changed).")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.append_dedupe:
        df2 = merge_dedupe(out_path, df)
        df2.to_csv(out_path, index=False, encoding="utf-8-sig")
    else:
        df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"Saved: {out_path} rows={len(df)}")

if __name__ == "__main__":
    main()
