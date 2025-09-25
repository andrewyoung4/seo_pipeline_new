#!/usr/bin/env python3
import argparse, os, re
from datetime import datetime
import pandas as pd

def smart_read_csv(path: str) -> pd.DataFrame:
    for enc in (None, "utf-8", "utf-8-sig", "cp1252"):
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)

def norm_map(cols):
    m = {}
    for c in cols:
        n = re.sub(r"\s+", " ", str(c).strip()).lower()
        m[n] = c
    return m

def pick(m, options):
    for o in options:
        if o.lower() in m: return m[o.lower()]
    for k, orig in m.items():
        for o in options:
            if o.lower() in k: return orig
    return None

def infer_url_col(df):
    # pick column with many http(s) matches
    best, score = None, -1.0
    sample = df.head(500)
    for col in sample.columns:
        s = sample[col].astype(str).str.contains(r"^https?://", case=False, na=False).mean()
        if s > score:
            best, score = col, s
    return best if score >= 0.2 else None

def main():
    ap = argparse.ArgumentParser(description="Fallback ranks.csv from GSC positions")
    ap.add_argument("--gsc-queries-csv", required=True)
    ap.add_argument("--out-csv", required=True)
    args = ap.parse_args()

    if not os.path.exists(args.gsc_queries_csv):
        raise FileNotFoundError(args.gsc_queries_csv)

    df = smart_read_csv(args.gsc_queries_csv)
    m = norm_map(df.columns)

    q   = pick(m, ["query","search query","keyword"])
    pos = pick(m, ["position","avg position","average position","rank"])
    url = pick(m, ["page","url","landing page","page url","address","final url","link"])

    if q is None or pos is None:
        raise ValueError("Need at least query and position columns in GSC CSV.")

    if url is None:
        url = infer_url_col(df)

    out = df[[q, pos]].copy()
    out.columns = ["keyword","rank"]
    out["rank"] = pd.to_numeric(out["rank"], errors="coerce").round().clip(lower=1)
    out["captured_at"] = datetime.utcnow().isoformat(timespec="seconds")

    if url and url in df.columns:
        out["url"] = df[url].astype(str)
        cols = ["keyword","url","rank","captured_at"]
        out = out[cols]
    else:
        out["url"] = ""
        out = out[["keyword","url","rank","captured_at"]]

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    print(f"Wrote ranks: {args.out_csv} (rows={len(out)})")

if __name__ == "__main__":
    main()
