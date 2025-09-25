#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, re, sys, pandas as pd

COLS_STD = ["domain","links_total","links_follow","first_seen","last_seen","source"]

def read_csv_any(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin-1")

def coerce_domain(x: str) -> str:
    x = str(x).strip().lower()
    x = re.sub(r"^https?://", "", x)
    x = x.split("/",1)[0]
    x = x.replace("www.", "")
    return x

def normalize_one(df: pd.DataFrame, source_hint: str) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    out = pd.DataFrame(columns=COLS_STD)

    # Heuristics for common formats
    # GSC "Top linking sites": columns often like "Top linking sites", "Linking sites", "Links"
    if any("linking site" in c or "top linking" in c for c in cols):
        # Try to detect domain & count
        dom_col = next((cols[c] for c in cols if ("site" in c or "domain" in c) and "link" in c), None)
        cnt_col = next((cols[c] for c in cols if c in ("links","linking sites","external links","count")), None)
        if not dom_col:
            # fallback: first column looks like a domain
            dom_col = df.columns[0]
        if not cnt_col:
            cnt_col = next((c for c in df.columns if df[c].dtype.kind in "if"), df.columns[-1])
        out["domain"] = df[dom_col].map(coerce_domain)
        out["links_total"] = pd.to_numeric(df[cnt_col], errors="coerce").fillna(1).astype(int)
        out["source"] = source_hint or "gsc"
        return out

    # Moz / Ahrefs style exports (commonly "Referring Domain(s)" or "Ref domains" + counts)
    dom_col = next((cols[c] for c in cols if "domain" in c and "refer" in c), None) \
           or next((cols[c] for c in cols if c in ("domain","root domain","source domain")), None)
    cnt_col = next((cols[c] for c in cols if "referring domains" in c or c in ("links","backlinks","count","total links")), None)
    follow_col = next((cols[c] for c in cols if "follow" in c and ("link" in c or "count" in c)), None)
    first_col = next((cols[c] for c in cols if "first seen" in c), None)
    last_col  = next((cols[c] for c in cols if "last seen" in c), None)

    if dom_col:
        out["domain"] = df[dom_col].map(coerce_domain)
        out["links_total"] = pd.to_numeric(df[cnt_col], errors="coerce").fillna(1).astype(int) if cnt_col else 1
        out["links_follow"] = pd.to_numeric(df[follow_col], errors="coerce") if follow_col else None
        out["first_seen"] = df[first_col] if first_col else None
        out["last_seen"] = df[last_col] if last_col else None
        out["source"] = source_hint or "unknown"
        return out

    # Super-simple two-column CSV (domain,count)
    if df.shape[1] >= 1:
        out["domain"] = df.iloc[:,0].map(coerce_domain)
        out["links_total"] = pd.to_numeric(df.iloc[:,1], errors="coerce").fillna(1).astype(int) if df.shape[1] >= 2 else 1
        out["source"] = source_hint or "manual"
        return out

    return pd.DataFrame(columns=COLS_STD)

def main():
    ap = argparse.ArgumentParser(description="Normalize backlinks CSV(s) into referring domains list.")
    ap.add_argument("--in", dest="inputs", nargs="+", required=True, help="One or more CSVs (GSC/Moz/Ahrefs/manual)")
    ap.add_argument("--out", required=True, help="Output CSV path for normalized ref domains")
    ap.add_argument("--source", default="", help="Optional source label applied to all rows (e.g., gsc/moz/ahrefs)")
    args = ap.parse_args()

    frames = []
    for p in args.inputs:
        df = read_csv_any(p)
        frames.append(normalize_one(df, args.source))
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLS_STD)

    # Clean & roll up per-domain
    if not out.empty:
        out["domain"] = out["domain"].fillna("").astype(str)
        out = out[out["domain"]!=""]
        agg = (out.groupby("domain", as_index=False)
                  .agg(links_total=("links_total","sum"),
                       links_follow=("links_follow","sum")))
        agg["first_seen"] = ""
        agg["last_seen"]  = ""
        agg["source"]     = args.source or "mixed"
    else:
        agg = out

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    agg.to_csv(args.out, index=False)
    print(f"Wrote {len(agg)} rows → {args.out}")

if __name__ == "__main__":
    main()
