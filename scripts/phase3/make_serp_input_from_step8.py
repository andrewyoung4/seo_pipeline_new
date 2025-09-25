#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, sys, re
import pandas as pd

def smart_read_csv(path: str) -> pd.DataFrame:
    for enc in (None, "utf-8", "utf-8-sig", "cp1252"):
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)

def pick(cols, options):
    low = {str(c).strip().lower(): c for c in cols}
    for o in options:
        if o.lower() in low: return low[o.lower()]
    for c in cols:
        cl = str(c).strip().lower()
        for o in options:
            if o.lower() in cl: return c
    return None

def canonicalize_keyword(s: str) -> str:
    if not isinstance(s, str):
        s = "" if pd.isna(s) else str(s)
    s = s.lower()
    s = re.sub(r"[\_\-]+", " ", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def main():
    ap = argparse.ArgumentParser(description="Build Phase-3 SERP sampler input from Step-8 outputs")
    ap.add_argument("--keyword-map", required=False, default="data/inputs/phase2/keyword_map.csv")
    ap.add_argument("--gsc-queries-csv", required=False, default="data/inputs/phase2/gsc_queries_summary.csv")
    ap.add_argument("--out-csv", required=False, default="data/inputs/phase3/serp_input.csv")
    ap.add_argument("--min-impr", type=int, default=0, help="Optional: filter queries by minimum impressions (if GSC present)")
    ap.add_argument("--field", choices=["query","keyword"], default="keyword", help="Which column name to output")
    args = ap.parse_args()

    series = None

    # Prefer keyword_map
    if os.path.exists(args.keyword_map):
        km = smart_read_csv(args.keyword_map)
        qcol = pick(km.columns, ["query","search query"])
        kcol = pick(km.columns, ["keyword","canonical_keyword","group","cluster"])
        use_col = kcol if args.field == "keyword" and kcol is not None else qcol
        if use_col is not None:
            series = km[use_col].astype(str)

    # Fallback: GSC
    if series is None and os.path.exists(args.gsc_queries_csv):
        gsc = smart_read_csv(args.gsc_queries_csv)
        qcol = pick(gsc.columns, ["query","search query","keyword","term"])
        if qcol is None:
            print("[ERROR] Could not find a query column.", file=sys.stderr); sys.exit(2)
        if args.min_impr > 0:
            icol = pick(gsc.columns, ["impressions","impr"])
            if icol is not None:
                gsc = gsc[pd.to_numeric(gsc[icol], errors="coerce").fillna(0) >= args.min_impr]
        series = gsc[qcol].astype(str)
        if args.field == "keyword":
            series = series.map(canonicalize_keyword)

    if series is None:
        print("[ERROR] Neither keyword_map nor GSC file found with usable columns.", file=sys.stderr); sys.exit(2)

    colname = args.field
    out = (
        pd.DataFrame({colname: series})
          .assign(**{colname: lambda d: d[colname].astype(str).str.strip()})
          .query(f"{colname} != ''")
          .drop_duplicates(subset=[colname])
          .sort_values(colname)
    )

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    print(f"Wrote sampler input: {args.out_csv} ({colname}s={len(out)})")

if __name__ == "__main__":
    main()
