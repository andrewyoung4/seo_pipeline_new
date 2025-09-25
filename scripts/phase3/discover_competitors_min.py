
#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
[Additive] discover_competitors_min.py — produce competitors_serp_hits.csv for Step 7

Purpose
-------
Given a list of queries (keyword_map or keyword_map_expanded), fetch top organic
results via Serper and record which domains show up how often. This creates the
`competitors_serp_hits.csv` your Step 7 Keyword Discovery Expansion expects.

Inputs
------
  --in  data/inputs/phase2/keyword_map.csv    (or ..._expanded.csv)
        Must contain a `query` column.
  --limit-queries 150        Max queries to sample (random or top-N; default top-N)
  --per-query 10             How many organic results per query to record
  --domains-denylist ""      Comma-separated list (e.g., "pinterest.com,amazon.com")

Env
----
  SERPER_API_KEY must be set.

Outputs
-------
  --out data/outputs/phase3/competitors_serp_hits.csv
  Schema: query, position, url, domain, title

Usage
-----
  $env:SERPER_API_KEY = "<key>"
  python .\scripts\phase3\discover_competitors_min.py ^
    --in .\data\inputs\phase2\keyword_map.csv ^
    --out .\data\outputs\phase3\competitors_serp_hits.csv ^
    --limit-queries 150 --per-query 10 ^
    --domains-denylist "pinterest.com,amazon.com,youtube.com,reddit.com"
"""
import argparse, os, re, json, random, time
from urllib.parse import urlparse
import pandas as pd
import requests

def read_queries(p):
    df = pd.read_csv(p)
    qcol = None
    for c in df.columns:
        if re.sub(r"[^a-z]","",c.lower()) == "query":
            qcol = c; break
    if not qcol:
        raise SystemExit("No 'query' column found in input CSV.")
    qs = [str(x).strip() for x in df[qcol].dropna().tolist() if str(x).strip()]
    # keep order but dedupe
    seen, uniq = set(), []
    for q in qs:
        if q not in seen:
            seen.add(q); uniq.append(q)
    return uniq

def serper_search(q, api_key, num=10, timeout=8):
    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    payload = {"q": q, "gl": "us", "hl": "en", "num": int(num)}
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
    if r.status_code != 200:
        return {"organic": []}
    return r.json()

def domain_of(u):
    try:
        h = urlparse(u).netloc.lower()
        return h[4:] if h.startswith("www.") else h
    except Exception:
        return ""

def main():
    ap = argparse.ArgumentParser(description="Minimal competitor discovery via Serper.")
    ap.add_argument("--in", dest="infile", required=True, help="CSV with a 'query' column")
    ap.add_argument("--out", required=True, help="Output CSV path for competitors_serp_hits.csv")
    ap.add_argument("--limit-queries", type=int, default=150)
    ap.add_argument("--per-query", type=int, default=10)
    ap.add_argument("--serper-timeout", type=int, default=8)
    ap.add_argument("--serper-qps", type=float, default=0.8, help="Max queries per second")
    ap.add_argument("--domains-denylist", default="pinterest.com,amazon.com,youtube.com,reddit.com")
    args = ap.parse_args()

    api_key = os.environ.get("SERPER_API_KEY","").strip()
    if not api_key:
        raise SystemExit("Missing SERPER_API_KEY env var.")

    queries = read_queries(args.infile)
    queries = queries[: args.limit_queries]

    deny = [d.strip().lower() for d in (args.domains_denylist or "").split(",") if d.strip()]
    rows = []
    delay = max(0.0, 1.0/float(args.serper_qps)) if args.serper_qps>0 else 0.0

    for qi, q in enumerate(queries, 1):
        data = serper_search(q, api_key, num=args.per_query, timeout=args.serper_timeout)
        org = data.get("organic") or []
        pos = 1
        for item in org:
            url = item.get("link") or item.get("url") or ""
            title = item.get("title") or ""
            dom = domain_of(url)
            if not url or not dom: 
                continue
            if any(dom.endswith(d) or dom==d for d in deny):
                continue
            rows.append({"query": q, "position": pos, "url": url, "domain": dom, "title": title})
            pos += 1
        if delay: time.sleep(delay)

    if not rows:
        # still write an empty file with headers
        pd.DataFrame(columns=["query","position","url","domain","title"]).to_csv(args.out, index=False, encoding="utf-8")
        print(f"Wrote {args.out} (0 rows)")
        return

    df = pd.DataFrame(rows)
    df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Wrote {args.out} with {len(df)} rows across {df['domain'].nunique()} domains")

if __name__ == "__main__":
    main()
