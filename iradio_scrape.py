import argparse
import datetime as dt
import re
import time
import traceback
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://www.bcc.com.tw/news3_search.asp"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

DATE_MMDD_RE = re.compile(r"^\d{2}/\d{2}$")   # 例如 01/05
TIME_RE = re.compile(r"^\d{2}:\d{2}$")        # 例如 05:35

NAV_STOP_WORDS = ("上一頁", "下一頁", "官網首頁", "請選擇", "本網站內容屬於")


def fetch_html(params: dict, verify_ssl: bool = True, retries: int = 6, timeout: int = 30) -> str:
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
            if r.status_code == 200 and r.text:
                return r.text
            last_err = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last_err = e
        time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"Failed to fetch {BASE_URL} params={params}. last_err={last_err}")


def mmdd_to_iso(mmdd: str, base_date: dt.date) -> str:
    m, d = mmdd.split("/")
    m, d = int(m), int(d)

    # 先用 base_date 的年份組候選日期
    candidates = [
        dt.date(base_date.year, m, d),
        dt.date(base_date.year - 1, m, d),
        dt.date(base_date.year + 1, m, d),
    ]
    # 選距離 base_date 最近的那個（處理跨年 12/31 ↔ 01/01）
    best = min(candidates, key=lambda x: abs((x - base_date).days))
    return best.isoformat()


def extract_tokens(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    tokens = [t.strip() for t in soup.get_text("\n").splitlines() if t.strip()]

    # 去掉一些常見噪音 token
    cleaned = []
    for t in tokens:
        if t in (";", "；", ">", "＞", "<", "＜"):
            continue
        cleaned.append(t)
    return cleaned


def parse_rows(tokens: list[str], base_date: dt.date) -> pd.DataFrame:
    """
    不靠表頭（播出時間/歌曲名稱/演唱...)。
    規則：
    - 看到 MM/DD 就更新 current_date（處理 dt=1..7 那種頁）
    - 看到 HH:MM 就視為一筆資料開始：HH:MM、歌名、歌手、後面可選欄位直到下一個 HH:MM 或 MM/DD
    """
    rows = []
    current_date = base_date.isoformat()

    def is_stop(x: str) -> bool:
        # 遇到導覽/頁尾文字就停止（避免把網站導覽當資料）
        stop_words = ("上一頁", "下一頁", "官網首頁", "本網站內容屬於", "回到", "TOP")
        return any(w in x for w in stop_words)

    i = 0
    n = len(tokens)

    while i < n:
        t = tokens[i]

        if is_stop(t):
            break

        # dt=1..7 頁面常有 MM/DD
        if DATE_MMDD_RE.match(t):
            current_date = mmdd_to_iso(t, base_date)
            i += 1
            continue

        # 一筆資料的起點：HH:MM
        if TIME_RE.match(t):
            play_time = t

            # 取下一個 token 當歌名
            if i + 1 >= n:
                break
            song = tokens[i + 1]

            # 再下一個 token 當歌手
            if i + 2 >= n:
                break
            artist = tokens[i + 2]

            # 基本防呆：如果歌名/歌手不合理（又是時間/日期），跳過
            if TIME_RE.match(song) or DATE_MMDD_RE.match(song) or TIME_RE.match(artist) or DATE_MMDD_RE.match(artist):
                i += 1
                continue

            # 後面可選欄位（專輯/出版者/CD編號…），直到下一個 HH:MM 或 MM/DD
            extras = []
            j = i + 3
            while j < n:
                nt = tokens[j]
                if is_stop(nt):
                    break
                if TIME_RE.match(nt) or DATE_MMDD_RE.match(nt):
                    break
                extras.append(nt)
                j += 1

            row = {
                "日期": current_date,
                "播出時間": play_time,
                "歌曲名稱": song,
                "演唱(奏)者": artist,
                "專輯": extras[0] if len(extras) >= 1 else "",
                "出版者": extras[1] if len(extras) >= 2 else "",
                "CD編號": extras[2] if len(extras) >= 3 else "",
            }
            rows.append(row)

            i = j
            continue

        i += 1

    df = pd.DataFrame(rows)

    # 若解析不到任何資料，輸出 tokens 方便你下載檢查
    if df.empty:
        Path("debug_tokens.txt").write_text("\n".join(tokens[:500]), encoding="utf-8", errors="ignore")
        raise ValueError("Parsed 0 rows (see debug_last.html and debug_tokens.txt).")

    return df



def parse_rows(tokens: list[str], base_date: dt.date) -> pd.DataFrame:
    has_date, idx = find_header_and_mode(tokens)

    rows = []
    n = len(tokens)

    def is_stop(x: str) -> bool:
        return any(w in x for w in NAV_STOP_WORDS)

    while idx < n:
        t = tokens[idx]

        if is_stop(t):
            break

        # 找到一筆資料的起點
        if has_date:
            if not DATE_MMDD_RE.match(t):
                idx += 1
                continue
            date_iso = mmdd_to_iso(t, base_date)
            idx += 1

            if idx >= n or not TIME_RE.match(tokens[idx]):
                continue
            play_time = tokens[idx]
            idx += 1
        else:
            # 今天頁面：沒有日期欄，用 base_date 當日期
            date_iso = base_date.isoformat()

            if not TIME_RE.match(t):
                idx += 1
                continue
            play_time = t
            idx += 1

        # 之後依序：歌名、歌手，後面是可選欄位直到下一筆起點
        if idx >= n:
            break
        song = tokens[idx]
        idx += 1

        if idx >= n:
            break
        artist = tokens[idx]
        idx += 1

        extras = []
        while idx < n:
            nt = tokens[idx]
            if is_stop(nt):
                break
            if has_date and DATE_MMDD_RE.match(nt):
                break
            if (not has_date) and TIME_RE.match(nt):
                break
            extras.append(nt)
            idx += 1

        row = {
            "日期": date_iso,
            "播出時間": play_time,
            "歌曲名稱": song,
            "演唱(奏)者": artist,
            "專輯": extras[0] if len(extras) >= 1 else "",
            "出版者": extras[1] if len(extras) >= 2 else "",
            "CD編號": extras[2] if len(extras) >= 3 else "",
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    return df


def fetch_dt_all_pages(dt_days_ago: int, max_pages: int = 50, verify_ssl: bool = True) -> pd.DataFrame:
    dfs = []
    base_date = dt.date.today() - dt.timedelta(days=dt_days_ago)

    for p in range(1, max_pages + 1):
        params = {"p": str(p)}
        # 今天：不要帶 dt；非今天：帶 dt=1..7
        if dt_days_ago > 0:
            params["dt"] = str(dt_days_ago)

        html = fetch_html(params, verify_ssl=verify_ssl)

        # 每次都把抓到的 HTML 存下來，便於除錯（最後一次會覆蓋）
        Path("debug_last.html").write_text(html, encoding="utf-8", errors="ignore")

        tokens = extract_tokens(html)
        df = parse_rows(tokens, base_date)

        if df.empty:
            # 沒資料代表可能到尾頁或抓到非預期頁面；保守停止
            break

        df["dt_days_ago"] = str(dt_days_ago)
        df["page"] = str(p)
        df["scraped_at"] = dt.datetime.now().isoformat(timespec="seconds")
        dfs.append(df)

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

    keys = [k for k in ["日期", "播出時間", "歌曲名稱", "演唱(奏)者"] if k in combined.columns]
    return combined.drop_duplicates(subset=keys, keep="last") if keys else combined.drop_duplicates(keep="last")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", type=int, default=0, help="0=today (no dt param), 1..7=days ago")
    ap.add_argument("--max-pages", type=int, default=50)
    ap.add_argument("--out", type=str, default="data/iradio_today.csv")
    ap.add_argument("--append-dedupe", action="store_true")
    ap.add_argument("--insecure", action="store_true", help="Disable SSL verification (public scraping only)")
    args = ap.parse_args()

    try:
        df = fetch_dt_all_pages(args.dt, args.max_pages, verify_ssl=not args.insecure)
        if df.empty:
            raise RuntimeError("Parsed 0 rows from all pages (see debug_last.html).")

        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if args.append-dedupe:
            df2 = merge_dedupe(out_path, df)
            df2.to_csv(out_path, index=False, encoding="utf-8-sig")
        else:
            df.to_csv(out_path, index=False, encoding="utf-8-sig")

        print(f"Saved: {out_path} rows={len(df)}")

    except Exception:
        Path("debug_error.txt").write_text(traceback.format_exc(), encoding="utf-8", errors="ignore")
        raise


if __name__ == "__main__":
    main()
