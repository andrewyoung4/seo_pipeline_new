#!/usr/bin/env python3
import argparse, os, re, sys
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

def main():
    ap = argparse.ArgumentParser(description="Build keyword_map.csv from gsc_queries_summary.csv")
    ap.add_argument("--gsc-queries-csv", required=True)
    ap.add_argument("--out-csv", required=True)
    args = ap.parse_args()

    if not os.path.exists(args.gsc_queries_csv):
        raise FileNotFoundError(args.gsc_queries_csv)

    df = smart_read_csv(args.gsc_queries_csv)
    m = norm_map(df.columns)

    q = pick(m, ["query","search query","keyword"])
    p = pick(m, ["page","url","landing page","page url","address","final url","link"])

    if q is None:
        print("[ERROR] Could not detect a 'query' column.", file=sys.stderr)
        print(f"[DEBUG] Columns: {list(df.columns)}", file=sys.stderr)
        raise SystemExit(2)

    if p is None:
        # query-only CSV: write placeholders
        out = (
            df[[q]].dropna()
                   .rename(columns={q: "query"})
                   .assign(target_url="")
        )
        os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
        out.to_csv(args.out_csv, index=False, encoding="utf-8")
        print(f"[WARN] No page/url column found. Wrote placeholder keyword_map (blank target_url) -> {args.out_csv} (rows={len(out)})")
        print("       Tip: export GSC with both Query and Page dimensions for automatic page mapping.", file=sys.stderr)
        return

    # standard case: choose best page by clicks if available
    clicks = pick(m, ["clicks","click"])
    work = df[[q, p] + ([clicks] if clicks and clicks in df.columns else [])].copy()
    cols = ["query","page"] + (["clicks"] if clicks and clicks in df.columns else [])
    work.columns = cols

    if "clicks" in work.columns:
        work["q_l"] = work["query"].astype(str).str.lower().str.strip()
        out = (work.sort_values(["q_l","clicks"], ascending=[True, False])
                    .groupby("q_l", as_index=False).first()[["query","page"]])
    else:
        work["q_l"] = work["query"].astype(str).str.lower().str.strip()
        out = work.drop_duplicates(subset=["q_l"])[["query","page"]]

    out = out.rename(columns={"page":"target_url"})
    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    print(f"Wrote keyword map: {args.out_csv} (rows={len(out)})")

if __name__ == "__main__":
    main()
