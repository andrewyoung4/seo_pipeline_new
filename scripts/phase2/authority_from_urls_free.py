#!/usr/bin/env python3
import argparse, os, re
import pandas as pd
def pick(cols, opts):
    low = {c.lower():c for c in cols}
    for o in opts:
        if o.lower() in low: return low[o.lower()]
    for c in cols:
        if any(o.lower() in c.lower() for o in opts):
            return c
    return None
def extract_domains(df, url_col):
    urls = df[url_col].astype(str).dropna().unique().tolist()
    doms=set()
    for u in urls:
        d = re.sub(r"^https?://", "", u.strip(), flags=re.I)
        d = re.sub(r"^www\.", "", d, flags=re.I)
        d = d.split("/")[0].lower()
        if d: doms.add(d)
    return sorted(doms)
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inpath", required=True)
    ap.add_argument("--out", dest="outpath", required=True)
    a = ap.parse_args()
    if not os.path.exists(a.inpath): raise FileNotFoundError(a.inpath)
    ext = os.path.splitext(a.inpath)[1].lower()
    if ext in [".xlsx",".xls"]:
        df = pd.read_excel(a.inpath, engine="openpyxl")
        url_col = pick(df.columns, ["url","page","address","final url","link"])
        if url_col is None: raise ValueError("No URL-like column in Excel")
    else:
        df = pd.read_csv(a.inpath, encoding="utf-8")
        url_col = pick(df.columns, ["page","url","landing page","page url","address","final url","link"])
        if url_col is None: raise ValueError("No URL column in CSV")
    domains = extract_domains(df, url_col)
    out = pd.DataFrame({"domain": domains, "authority": [None]*len(domains)})
    os.makedirs(os.path.dirname(a.outpath), exist_ok=True)
    out.to_csv(a.outpath, index=False, encoding="utf-8")
    print(f"Wrote authority_generic: {a.outpath} (domains={len(out)})")
if __name__ == "__main__":
    main()
