#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, sys
import pandas as pd

ALIASES = {
    "date": ["date","captured_at","serp_date","run_at","timestamp","collected_at","fetched_at","retrieved_at"],
    "query": ["query","keyword","search_term","term","search query"],
    "url": ["url","page","result_url","target_url","landing page","address","link"],
    "position": ["position","rank","pos","result_position","serp_position"]
}

def smart_read(path: str) -> pd.DataFrame:
    for enc in (None,"utf-8","utf-8-sig","cp1252"):
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)

def pick(cols, names):
    low = {str(c).strip().lower(): c for c in cols}
    # exact
    for n in names:
        if n.lower() in low:
            return low[n.lower()]
    # contains
    for c in cols:
        cl = str(c).strip().lower()
        for n in names:
            if n.lower() in cl:
                return c
    return None

def main():
    ap = argparse.ArgumentParser(description="Normalize SERP samples to columns: date, query, url, position")
    ap.add_argument("--in", dest="inp", required=True, help="Input serp_samples.csv")
    ap.add_argument("--out", dest="outp", required=True, help="Output normalized CSV")
    args = ap.parse_args()

    df = smart_read(args.inp)
    cols = list(df.columns)

    d = pick(cols, ALIASES["date"])
    q = pick(cols, ALIASES["query"])
    u = pick(cols, ALIASES["url"])
    p = pick(cols, ALIASES["position"])

    missing = [name for name, val in [("date", d),("query", q),("url", u),("position", p)] if val is None]
    if missing:
        print("[ERROR] Could not find required column(s):", ", ".join(missing))
        print("Available columns:", cols)
        sys.exit(2)

    out = df[[d,q,u,p]].copy()
    out.columns = ["date","query","url","position"]
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["query"] = out["query"].astype(str).str.strip()
    out["url"] = out["url"].astype(str).str.strip()
    out["position"] = pd.to_numeric(out["position"], errors="coerce")

    out = out.dropna(subset=["date","query","url","position"])
    out.to_csv(args.outp, index=False, encoding="utf-8")
    print(f"Wrote normalized SERP samples -> {args.outp} (rows={len(out)})")

if __name__ == "__main__":
    main()
