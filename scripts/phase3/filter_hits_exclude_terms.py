
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Filter SERP hits CSV by excluding rows whose URL/title/snippet contain banned terms.
Terms are matched case-insensitively and can be provided as a semicolon- or comma-separated list.

Usage:
  python .\scripts\phase3\filter_hits_exclude_terms.py \
    --in .\data\outputs\phase3\competitors_serp_hits.csv \
    --terms "pattern;tutorial;pdf;kit" \
    --out .\data\outputs\phase3\competitors_serp_hits.filtered.csv
"""
import argparse, re, sys
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input hits CSV")
    ap.add_argument("--out", dest="out", required=True, help="Output filtered CSV")
    ap.add_argument("--terms", required=True, help="Semicolon or comma separated terms to exclude (case-insensitive)")
    args = ap.parse_args()

    df = pd.read_csv(args.inp)
    # Detect likely text columns
    cols = [c for c in df.columns]
    lowmap = {c.lower(): c for c in cols}
    url_col = next((lowmap[k] for k in ["url","link","result_url","resultlink"] if k in lowmap), None)
    title_col = next((lowmap[k] for k in ["title","result_title","page_title"] if k in lowmap), None)
    snip_col  = next((lowmap[k] for k in ["snippet","description","result_snippet","summary"] if k in lowmap), None)

    terms = re.split(r"[;,]", args.terms)
    terms = [t.strip() for t in terms if t.strip()]
    if not terms:
        print("No terms provided.", file=sys.stderr)
        sys.exit(2)
    pat = re.compile("|".join([re.escape(t) for t in terms]), flags=re.I)

    mask = pd.Series(False, index=df.index)
    for c in [url_col, title_col, snip_col]:
        if c and c in df.columns:
            mask = mask | df[c].astype(str).str.contains(pat, na=False)

    out_df = df[~mask].copy()
    out_df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Wrote {args.out} (kept {len(out_df)}/{len(df)} rows after excluding: {', '.join(terms)})")

if __name__ == "__main__":
    main()
