
import argparse, pandas as pd

def main():
    ap = argparse.ArgumentParser(description="Phase-3 Competitive (FREE) — merges generic authority metrics")
    ap.add_argument("--origin", required=True)
    ap.add_argument("--comp-ranks-csv", required=True)
    ap.add_argument("--comp-backlinks-csv", default="")
    ap.add_argument("--authority-csv", default="", help="Generic authority CSV from authority_free_normalize.py")
    ap.add_argument("--out-xlsx", required=True)
    args = ap.parse_args()

    ranks = pd.read_csv(args.comp_ranks_csv)
    backlinks = pd.read_csv(args.comp_backlinks_csv) if args.comp_backlinks_csv else pd.DataFrame()
    auth = pd.read_csv(args.authority_csv) if args.authority_csv else pd.DataFrame()

    ranks["is_us"] = ranks["domain"].astype(str).str.contains(args.origin.replace("https://","").replace("http://","").split("/")[0], case=False, na=False)
    comp = ranks[~ranks["is_us"]].copy()
    if "hits" in comp.columns:
        score = comp.groupby("domain", as_index=False)["hits"].sum().rename(columns={"hits":"serp_hits"})
    else:
        score = comp.groupby("domain", as_index=False).size().rename(columns={"size":"serp_hits"})

    if not auth.empty and "domain" in auth.columns:
        score = score.merge(auth, on="domain", how="left")

    if not backlinks.empty and "referring_domain" in backlinks.columns:
        ref = backlinks.groupby("referring_domain", as_index=False).size().rename(columns={"size":"backlinks"}).rename(columns={"referring_domain":"domain"})
        score = score.merge(ref, on="domain", how="left")

    score = score.sort_values(by=["serp_hits"], ascending=False)

    with pd.ExcelWriter(args.out_xlsx, engine="xlsxwriter") as xw:
        ranks.to_excel(xw, index=False, sheet_name="Competitor_SERP_Hits")
        score.to_excel(xw, index=False, sheet_name="Competitor_Scores")
        if not backlinks.empty:
            backlinks.to_excel(xw, index=False, sheet_name="Competitor_Backlinks")
        if not auth.empty:
            auth.to_excel(xw, index=False, sheet_name="Authority_Generic")
    print(f"Wrote {args.out_xlsx}")

if __name__ == "__main__":
    main()
