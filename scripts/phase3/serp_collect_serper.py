#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
serp_collect_serper.py
Fetches SERPs via Serper.dev and writes a normalized CSV for Competitor Parity.

Inputs (choose one):
  --queries-from-gsc  path to GSC CSV (auto-detects 'query' column)
  --queries-file      path to a .txt file with one query per line

Common:
  --out               output CSV path (e.g., .\\data\\outputs\\phase3\\serp_results.csv)
  --per-query         how many organic results per query to keep (default: 10)
  --delay-ms          sleep between requests (default: 250)
  --market            e.g., 'us' (optional)
  --location          free-text location (optional)

Requires env var SERPER_API_KEY.

Writes columns: query,url,domain,rank
"""
import argparse, csv, os, sys, time, json
from urllib.parse import urlparse
from pathlib import Path
import requests

def _read_gsc_queries(path):
    # read any CSV that has a 'query' column
    import pandas as pd
    df = pd.read_csv(path)
    col = next((c for c in df.columns if c.lower()=='query'), None)
    if not col:
        raise SystemExit("Could not find a 'query' column in the GSC CSV.")
    qs = [str(x).strip() for x in df[col].dropna().unique().tolist() if str(x).strip()]
    return qs

def _read_txt(path):
    with open(path, "r", encoding="utf-8") as f:
        qs = [line.strip() for line in f if line.strip()]
    return qs

def _domain(u):
    try:
        host = urlparse(u).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""

def fetch_serp(q, per, market=None, location=None, timeout=20):
    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": os.environ.get("SERPER_API_KEY",""), "Content-Type": "application/json"}
    if not headers["X-API-KEY"]:
        raise SystemExit("Missing SERPER_API_KEY environment variable.")
    payload = {"q": q, "num": max(10, per)}
    if market: payload["gl"] = market
    if location: payload["location"] = location
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
    r.raise_for_status()
    data = r.json()
    results = []
    organic = data.get("organic", []) or []
    rank = 1
    for item in organic:
        u = item.get("link") or item.get("url") or ""
        if not u or not u.startswith("http"):
            continue
        results.append({"url": u, "domain": _domain(u), "rank": rank})
        rank += 1
        if rank > per:
            break
    return results

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries-from-gsc")
    ap.add_argument("--queries-file")
    ap.add_argument("--out", default=r".\data\outputs\phase3\serp_results.csv")
    ap.add_argument("--per-query", type=int, default=10)
    ap.add_argument("--delay-ms", type=int, default=250)
    ap.add_argument("--market")
    ap.add_argument("--location")
    args = ap.parse_args()

    if not args.queries_from_gsc and not args.queries_file:
        raise SystemExit("Provide --queries-from-gsc or --queries-file")
    if args.queries_from_gsc:
        queries = _read_gsc_queries(args.queries_from_gsc)
    else:
        queries = _read_txt(args.queries_file)

    print(f"[info] queries: {len(queries)}")
    outp = args.out
    Path(outp).parent.mkdir(parents=True, exist_ok=True)
    with open(outp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["query","url","domain","rank"])
        w.writeheader()
        for i, q in enumerate(queries, 1):
            try:
                rows = fetch_serp(q, args.per_query, args.market, args.location)
            except Exception as e:
                print(f"[warn] SERP fetch failed for '{q}': {e}", file=sys.stderr)
                rows = []
            for r in rows:
                w.writerow({"query": q, **r})
            if i % 10 == 0:
                print(f"[info] processed {i}/{len(queries)}")
            time.sleep(max(0, args.delay_ms)/1000.0)
    print(f"[done] wrote {outp}")

if __name__ == "__main__":
    main()
