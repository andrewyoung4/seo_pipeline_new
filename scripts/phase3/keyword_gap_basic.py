#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, pandas as pd, re

def norm_kw(s):
    s = str(s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def load_mine(gsc_csv):
    df = pd.read_csv(gsc_csv)
    # Try to detect the query column
    qcol = next((c for c in df.columns if c.lower() in ("query","keyword","search query")), None) or df.columns[0]
    mine = df[[qcol]].rename(columns={qcol:"keyword"}).copy()
    mine["keyword"] = mine["keyword"].map(norm_kw)
    return mine.dropna().drop_duplicates()

def load_serp(serp_csv, origin):
    df = pd.read_csv(serp_csv)
    # expected columns: keyword (or query), url (or link), domain (optional)
    kcol = next((c for c in df.columns if c.lower() in ("keyword","query")), None) or df.columns[0]
    ucol = next((c for c in df.columns if c.lower() in ("url","link","result")), None)
    dcol = next((c for c in df.columns if c.lower() == "domain"), None)

    out = pd.DataFrame({
        "keyword": df[kcol].map(norm_kw),
        "url": df[ucol] if ucol else ""
    })
    # derive domain if needed
    if dcol:
        out["domain"] = df[dcol].str.lower().fillna("")
    else:
        out["domain"] = out["url"].str.extract(r"^(?:https?://)?([^/]+)")[0].str.lower().fillna("")
    out["is_origin"] = out["domain"].str.contains(re.escape(origin.lower()))
    return out.dropna(subset=["keyword"]).drop_duplicates()

def main():
    ap = argparse.ArgumentParser(description="Compute keyword gap: competitors rank but we do not.")
    ap.add_argument("--gsc", required=True, help="GSC queries summary CSV")
    ap.add_argument("--serp", required=True, help="SERP samples CSV from phase 3")
    ap.add_argument("--origin", required=True, help="Your site domain, e.g., silentprincesstt.com")
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    mine = load_mine(args.gsc)
    serp = load_serp(args.serp, args.origin)

    have = set(mine["keyword"].tolist())
    # keywords where competitors appear
    comp_hits = (serp[~serp["is_origin"]]
                 .groupby("keyword", as_index=False)
                 .agg(competitors_count=("domain","nunique"),
                      examples=("domain", lambda s: ", ".join(sorted(s.unique())[:3]))))

    # GAP = competitor keywords not in our GSC queries
    gap = comp_hits[~comp_hits["keyword"].isin(have)].copy()
    gap = gap.sort_values(["competitors_count","keyword"], ascending=[False, True])

    # summary
    total_keywords = int(serp["keyword"].nunique())
    our_keywords    = int(mine["keyword"].nunique())
    gap_keywords    = int(gap["keyword"].nunique())
    top_competitors = (serp[~serp["is_origin"]].groupby("domain", as_index=False)
                          .size().sort_values("size", ascending=False).head(5))

    os.makedirs(args.out_dir, exist_ok=True)
    gap_out = os.path.join(args.out_dir, "keyword_gap_opportunities.csv")
    gap.to_csv(gap_out, index=False)

    summary = pd.DataFrame([
        {"metric":"total_keywords_sampled","value": total_keywords},
        {"metric":"our_keywords_in_gsc","value": our_keywords},
        {"metric":"gap_keywords","value": gap_keywords},
    ])
    summary_out = os.path.join(args.out_dir, "keyword_gap_summary.csv")
    summary.to_csv(summary_out, index=False)

    comps_out = os.path.join(args.out_dir, "keyword_gap_top_competitors.csv")
    top_competitors.to_csv(comps_out, index=False)

    print("Wrote:", gap_out)
    print("Wrote:", summary_out)
    print("Wrote:", comps_out)

if __name__ == "__main__":
    main()
