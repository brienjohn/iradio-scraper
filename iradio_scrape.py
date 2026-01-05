#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

DATE_MMDD_RE = re.compile(r"^\d{2}/\d{2}$")  # 例如 01/05
TIME_RE = re.compile(r"^\d{2}:\d{2}$")       # 例如 16:27


def fetch_content(params: dict, verify_ssl: bool = True, retries: int = 6, timeout: int = 30) -> bytes:
    """
    抓取原始 bytes（避免 encoding 搞亂）。
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
            if r.status_code == 200 and r.content:
                return r.content
            last_err = RuntimeError(f"HTTP {r.status_code}")
        except Exception as e:
            last_err = e

        time.sleep(1.5 * (i + 1))

    raise RuntimeError(f"Failed to fetch {BASE_URL} params={params}. last_err={last_err}")


def mmdd_to_iso(mmdd: str, base_date: dt.date) -> str:
    """
    MM/DD -> YYYY-MM-DD（用 base_date 的年份當中心，挑最近的日期，避免跨年錯年）
    """
    m, d = map(int, mmdd.split("/"))
    candidates = [dt.date(base_date.year + y, m, d) for y in (-1, 0, 1)]
    best = min(candidates, key=lambda x: abs((x - base_date).days))
    return best.isoformat()


def fix_text(s: str) -> str:
    """
    修復網站回傳的中文亂碼（典型 mojibake：例如 'æ²ç®æ¥è©¢' -> '曲目查詢'）
    並處理 &nbsp;（\xa0）等空白。
    """
    if s is None:
        return ""

    s = s.replace("\r", " ").replace("\n", " ")

    # 若本來就含 CJK，通常不用救
    if not re.search(r"[\u4e00-\u9fff]", s) and re.search(r"[ÃÂæåäèéçð]", s):
        try:
            b = s.encode("latin1")  # 保留原 byte 值
            try:
                cand = b.decode("utf-8")
            except UnicodeDecodeError:
                # 常見：尾端混入 \xa0（&nbsp;）導致 UTF-8 decode 失敗
                cand = b.rstrip(b"\xa0").decode("utf-8", errors="strict")

            if re.search(r"[\u4e00-\u9fff]", cand):
                s = cand
        except Exception:
            pass

    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_page(content: bytes, base_date: dt.date, dt_days_ago: int, page: int) -> pd.DataFrame:
    """
    解析頁面：每筆曲目是一個 div.bxa2
    典型順序：
      0 日期(MM/DD)
      1 播出時間(HH:MM)
      2 歌曲名稱
      3 演唱(奏)者
      4 專輯
      5 出版者（可能 hidden/空）
      6 CD編號（可能 hidden/空）
    """
    soup = BeautifulSoup(content, "lxml")

    out = []
    for div in soup.select("div.bxa2"):
        children = div.find_all("div", recursive=False)
        cells = [fix_text(c.get_text(" ", strip=False)) for c in children]
        if len(cells) < 4:
            continue

        mmdd = cells[0]
        tm = cells[1]

        # 若前兩格不是日期/時間（偶發版面多塞一個 div），就用 fallback 掃描
        if not (DATE_MMDD_RE.match(mmdd) and TIME_RE.match(tm)):
            mmdd = next((x for x in cells if DATE_MMDD_RE.match(x)), "")
            if mmdd:
                di = cells.index(mmdd)
                tm = next((x for x in cells[di + 1 :] if TIME_RE.match(x)), "")
            if not (mmdd and tm):
                continue

        date_iso = mmdd_to_iso(mmdd, base_date)

        di = cells.index(mmdd)
        ti = cells.index(tm, di + 1) if tm in cells[di + 1 :] else di + 1

        song = cells[ti + 1] if ti + 1 < len(cells) else ""
        artist = cells[ti + 2] if ti + 2 < len(cells) else ""
        album = cells[ti + 3] if ti + 3 < len(cells) else ""
        publisher = cells[ti + 4] if ti + 4 < len(cells) else ""
        cdno = cells[ti + 5] if ti + 5 < len(cells) else ""

        out.append(
            {
                "日期": date_iso,
                "日期_mmdd": mmdd,
                "播出時間": tm,
                "歌曲名稱": song,
                "演唱(奏)者": artist,
                "專輯": album,
                "出版者": publisher,
                "CD編號": cdno,
                "dt_days_ago": str(dt_days_ago),
                "page": str(page),
            }
        )

    return pd.DataFrame(out)


def fetch_dt_all_pages(dt_days_ago: int, max_pages: int, verify_ssl: bool) -> pd.DataFrame:
    """
    分頁抓取：
      - dt=0（今天）：不要帶 dt 參數，只帶 p
      - dt>0：帶 dt=... 與 p
    """
    base_date = dt.date.today() - dt.timedelta(days=dt_days_ago)
    dfs = []

    for p in range(1, max_pages + 1):
        params = {"p": str(p)}
        if dt_days_ago > 0:
            params["dt"] = str(dt_days_ago)

        content = fetch_content(params, verify_ssl=verify_ssl)

        # 除錯用：永遠保留最後一次抓到的 HTML
        Path("debug_last.html").write_bytes(content)

        df = parse_page(content, base_date, dt_days_ago, p)
        if df.empty:
            if p == 1:
                raise RuntimeError("Parsed 0 rows on page 1 (see debug_last.html).")
            break

        df["scraped_at"] = dt.datetime.now().isoformat(timespec="seconds")
        dfs.append(df)

        # 保守：若這頁資料很少，通常代表已到尾頁
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dt", type=int, default=0, help="0=today, 1..7=days ago")
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

        if args.append_dedupe:
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
