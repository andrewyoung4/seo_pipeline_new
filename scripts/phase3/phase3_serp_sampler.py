#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, time, urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

def serper_search(q: str, country: str="us", device: str="desktop", delay: float=0.0) -> List[Dict]:
    key = os.getenv("SERPER_API_KEY", "").strip()
    if not key:
        raise SystemExit("SERPER_API_KEY is not set. Create a .env or set env var, or pass --allow-ddg to try the fallback.")
    url = "https://google.serper.dev/search"
    payload = {"q": q, "gl": country.lower(), "num": 10}
    headers = {"X-API-KEY": key, "Content-Type": "application/json"}
    # simple retry for 429s
    for attempt in range(3):
        r = requests.post(url, json=payload, headers=headers, timeout=25)
        if r.status_code in (429, 503) and attempt < 2:
            time.sleep(1.5 * (attempt + 1))
            continue
        r.raise_for_status()
        break
    data = r.json()
    out, i = [], 1
    for item in (data.get("organic", []) or [])[:10]:
        out.append({"rank": i, "title": item.get("title",""), "url": item.get("link",""), "source":"serper"})
        i += 1
    time.sleep(delay)
    return out

def ddg_serp(q: str, pages: int=1, delay: float=0.8) -> List[Dict]:
    """HTML fallback. DDG often returns 403 to bots; use only with --allow-ddg."""
    out, i = [], 1
    url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(q)}"
    sess = requests.Session()
    sess.headers.update({"User-Agent": UA, "Accept-Language":"en-US,en;q=0.9"})
    for _ in range(max(1, pages)):
        r = sess.get(url, timeout=25)
        if r.status_code == 403:
            return out  # blocked; return what we have
        r.raise_for_status()
        s = BeautifulSoup(r.text, "html.parser")
        for a in s.select("a.result__a"):
            href = a.get("href")
            title = a.get_text(strip=True)
            if href:
                out.append({"rank": i, "title": title, "url": href, "source":"duckduckgo"})
                i += 1
                if i>10: break
        if i>10: break
        more = s.select_one("a.result--more__btn, a.result__a--more")
        if not more: break
        href = more.get("href")
        if not href: break
        url = urllib.parse.urljoin("https://html.duckduckgo.com/html/", href)
        time.sleep(delay)
    return out

def main():
    ap = argparse.ArgumentParser(description="Collect top-10 SERP results per keyword via Serper.dev; optional DDG fallback.")
    ap.add_argument("--in", dest="inp", required=True, help="CSV with column 'keyword'")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--country", default="us")
    ap.add_argument("--device", default="desktop", choices=["desktop","mobile"])
    ap.add_argument("--sleep", type=float, default=0.2, help="delay between queries (seconds)")
    ap.add_argument("--append", action="store_true", help="append to existing CSV instead of overwriting")
    ap.add_argument("--allow-ddg", action="store_true", help="try a DuckDuckGo HTML fallback if SERPER_API_KEY is missing")
    args = ap.parse_args()

    df = pd.read_csv(args.inp)
    if "keyword" not in df.columns:
        raise SystemExit("Input CSV must have a 'keyword' column")

    rows = []
    for kw in df["keyword"].astype(str):
        kw = kw.strip()
        if not kw:
            continue
        try:
            items = serper_search(kw, country=args.country, device=args.device, delay=args.sleep)
        except SystemExit as e:
            if args.allow-ddg:
                items = ddg_serp(kw, pages=1, delay=args.sleep)
            else:
                raise
        fetched = datetime.now(timezone.utc).isoformat(timespec="seconds")
        for it in items:
            it.update({"keyword": kw, "fetched_at": fetched})
            rows.append(it)

    out_df = pd.DataFrame(rows)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    if args.append and Path(args.out).exists():
        prev = pd.read_csv(args.out)
        out_df = pd.concat([prev, out_df], ignore_index=True).drop_duplicates(subset=["keyword","rank","url","fetched_at"])
    out_df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Wrote {args.out} with {len(out_df)} rows")

if __name__ == "__main__":
    main()
