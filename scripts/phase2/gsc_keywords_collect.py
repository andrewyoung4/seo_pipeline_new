#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patched gsc_keywords_collect.py
- Outputs (query, clicks, impressions, ctr, position) with impressions-weighted position
"""
import argparse, pandas as pd, numpy as np
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gsc-csv", required=True, help="Raw GSC export with at least query, clicks, impressions, ctr, position or position per day")
    ap.add_argument("--out-csv", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.gsc_csv)
    cols = {c.lower(): c for c in df.columns}
    q = cols.get("query") or "query"
    c = cols.get("clicks") or "clicks"
    i = cols.get("impressions") or "impressions"
    p = cols.get("position") or "position"
    if q not in df.columns or i not in df.columns:
        raise SystemExit("GSC CSV must contain query and impressions columns.")

    df[c] = pd.to_numeric(df.get(c, 0), errors="coerce").fillna(0)
    df[i] = pd.to_numeric(df[i], errors="coerce").fillna(0)
    df[p] = pd.to_numeric(df.get(p, np.nan), errors="coerce")

    g = df.groupby(q, as_index=False).agg(
        clicks=(c,"sum"),
        impressions=(i,"sum"),
        ctr=("ctr","mean") if "ctr" in cols else (i, lambda s: np.nan),
        wpos=(p, lambda s: (s*df.loc[s.index, i]).sum() / max(1.0, df.loc[s.index, i].sum()))
    ).rename(columns={"wpos":"position"})
    Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)
    g.to_csv(args.out_csv, index=False)
    print(f"Wrote {args.out_csv} with {len(g)} rows.")

if __name__ == "__main__":
    main()
