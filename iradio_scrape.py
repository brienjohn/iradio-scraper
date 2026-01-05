import argparse
import datetime as dt
import re
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 用 asP（大寫 P）版本，實務上較穩
BASE_URL = "https://www.bcc.com.tw/news3_search.asP"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

COLS = ["日期", "播出時間", "歌曲名稱", "演唱(奏)者", "專輯", "出版者", "CD編號"]

DATE_RE = re.compile(r"^\d{2}/\d{2}$")   # 例如 01/05
TIME_RE = re.compile(r"^\d{2}:\d{2}$")   # 例如 05:04


def fetch_html(params: dict, verify_ssl: bool = True, retries: int = 6, timeout: int = 30) -> str:
    """
    抓 HTML。若回傳內容不像曲目頁（缺少關鍵字），會重試。
    """
    last_err = None
    for i in range(retries):
        try:
            r = requests.get(
                BASE_URL,
                params=params,
                headers=HEADERS,
                timeout=timeout,
                verify=verify_ssl,
                allow_redirects=True,
            )

            if r.status_code != 200 or not r.text:
                last_err = RuntimeError(f"HTTP {r.status_code}")
                time.sleep(1.5 * (i + 1))
                continue

            html = r.text

            # 基本驗證：曲目頁通常會包含這些字樣（避免抓到空頁/錯誤頁）
            if ("曲目查詢" not in html) or ("播出時間" not in html) or ("歌曲名稱" not in html):
                last_err = RuntimeError("HTML does not look like playlist page (missing keywords)")
                time.sleep(1.5 * (i + 1))
                continue

            return html

        except Exception as e:
            last_err = e
            time.sleep(1.5 * (i + 1))

    # 失敗時把最後一次錯誤帶出去
    raise RuntimeError(f"Failed to fetch {BASE_URL} params={params}. last_err={last_err}")


def extract_rows_from_text(html: str) -> pd.DataFrame:
    """
    不依賴 <table>。把頁面文字拆成 token，再用日期 MM/DD 當 row 起點。
    """
    soup = BeautifulSoup(html, "lxml")
    tokens = [t.strip() for t in soup.get_text("\n").splitlines() if t.strip()]

    # 找欄位標題的開始位置（日期 / 播出時間 / 歌曲名稱 / 演唱(奏)者 ...）
    # 有些頁面會重複出現「日期」，所以找一段連續欄名最可靠
    start_idx = -1
    for i in range(len(tokens) - 4):
        if tokens[i] == "日期" and tokens[i + 1] == "播出時間" and tokens[i + 2] == "歌曲名稱" and tokens[i + 3].startswith("演唱"):
            start_idx = i
            break

    if start_idx == -1:
        # 萬一結構變了，存檔以便 debug
        Path("debug_last.html").write_text(html, encoding="utf-8", errors="ignore")
        raise ValueError("Cannot locate header tokens (日期/播出時間/歌曲名稱/演唱...)")

    data = tokens[start_idx + len(COLS):]

    rows = []
    i = 0
    while i < len(data):
        t = data[i]

        # 遇到頁尾/導覽就停止（避免把「上一頁」等混進資料）
        if ("上一頁" in t) or ("下一頁" in t) or ("本網站內容屬於" in t):
            break

        # 每筆資料以 MM/DD 開頭
        if DATE_RE.match(t):
            row = {c: "" for c in COLS}
            row["日期"] = t
            i += 1

            # 播出時間
            if i < len(data) and TIME_RE.match(data[i]):
                row["播出時間"] = data[i]
                i += 1
            else:
                # 若時間缺失，直接跳過這筆避免錯位
                continue

            # 歌曲名稱
            if i < len(data) and (not DATE_RE.match(data[i])):
                row["歌曲名稱"] = data[i]
                i += 1
            else:
                continue

            # 演唱(奏)者
            if i < len(data) and (not DATE_RE.match(data[i])):
                row["演唱(奏)者"] = data[i]
                i += 1
            else:
                continue

            # 其餘欄位（專輯 / 出版者 / CD編號）是可選，直到下一筆日期出現為止
            extras = []
            while i < len(data) and (not DATE_RE.match(data[i])):
                stop_t = data[i]
                if ("上一頁" in stop_t) or ("下一頁" in stop_t) or ("本網站內容屬於" in stop_t):
                    break
                extras.append(stop_t)
                i += 1

            if len(extras) >= 1:
                row["專輯"] = extras[0]
            if len(extras) >= 2:
                row["出版者"] = extras[1]
            if len(extras) >= 3:
                row["CD編號"] = extras[2]

            rows.append(row)
        else:
            i += 1

    df = pd.DataFrame(rows)

    # 若完全沒解析出資料，把 HTML 存起來方便你看（Actions 也可上傳 artifact）
    if df.empty:
        Path("debug_last.html").write_text(html, encoding="utf-8", errors="ignore")
        raise ValueError("Parsed 0 rows (saved debug_last.html).")

    return df


def fetch_dt_all_pages(dt_days_ago: int, max_pages: int = 50, verify_ssl: bool = True) -> pd.DataFrame:
    dfs = []

    for p in range(1, max_pages + 1):
        params = {"p": str(p)}
        # 今天：不要帶 dt；非今天：帶 dt=1..7
        if dt_days_ago > 0:
            params["dt"] = str(dt_days_ago)

        html = fetch_html(params, verify_ssl=verify_ssl)
        df = extract_rows_from_text(html)

        df["dt_days_ago"] = str(dt_days_ago)
        df["page"] = str(p)
        df["scraped_at"] = dt.datetime.now().isoformat(timespec="seconds")

        dfs.append(df)

        # 如果某頁資料很少，通常接近尾頁
        if len(df) < 5:
            break

        time.sleep(0.6)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def merge_dedupe(existing_path: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    if existing_path.exists():
        old_df = pd.read_csv(existing_path, dtype=str, encoding="utf-8-sig")
        combined = pd.concat([old_df, new_df.astype(str)], ignore_index=True)
    else:
        combined = new_df.astype(str)

    key_candidates = ["日期", "播出時間", "歌曲名稱", "演唱(奏)者"]
    keys = [k for k in key_candidates if k in combined.columns]
    combined = combined.drop_duplicates(subset=keys, keep="last") if keys else combined.drop_duplicates(keep="last")
    return combined


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", type=int, default=0, help="0=today (no dt param), 1..7 = days ago")
    ap.add_argument("--max-pages", type=int, default=50)
    ap.add_argument("--out", type=str, default="data/iradio_today.csv")
    ap.add_argument("--append-dedupe", action="store_true")
    ap.add_argument("--insecure", action="store_true", help="Disable SSL verification (public scraping only)")
    args = ap.parse_args()

    df = fetch_dt_all_pages(args.dt, args.max_pages, verify_ssl=not args.insecure)

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
