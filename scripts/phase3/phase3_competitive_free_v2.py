import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.share_of_voice import compute_share_of_voice, normalize_domain


def _detect_column(columns, *candidates):
    low = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand in low:
            return low[cand]
    return None


def main():
    ap = argparse.ArgumentParser(
        description="Phase-3 Competitive (FREE) — merges generic authority metrics"
    )
    ap.add_argument("--origin", required=True)
    ap.add_argument("--comp-ranks-csv", required=True)
    ap.add_argument("--comp-backlinks-csv", default="")
    ap.add_argument(
        "--authority-csv",
        default="",
        help="Generic authority CSV from authority_free_normalize.py",
    )
    ap.add_argument("--out-xlsx", required=True)
    args = ap.parse_args()

    ranks = pd.read_csv(args.comp_ranks_csv)
    backlinks = pd.read_csv(args.comp_backlinks_csv) if args.comp_backlinks_csv else pd.DataFrame()
    auth = pd.read_csv(args.authority_csv) if args.authority_csv else pd.DataFrame()

    kw_col = _detect_column(ranks.columns, "keyword", "query", "term")
    rank_col = _detect_column(ranks.columns, "rank", "position", "avg_position")
    domain_col = _detect_column(ranks.columns, "domain", "host")
    url_col = _detect_column(ranks.columns, "url", "page", "landing_page")

    if kw_col is None or rank_col is None:
        raise SystemExit("Competitive ranks CSV must include keyword and rank columns.")

    if domain_col:
        ranks["__domain"] = ranks[domain_col].map(normalize_domain)
    elif url_col:
        ranks["__domain"] = ranks[url_col].map(normalize_domain)
    else:
        ranks["__domain"] = ""

    origin_norm = normalize_domain(args.origin)
    ranks["is_us"] = ranks["__domain"].eq(origin_norm)

    sov_source = (
        ranks.rename(columns={kw_col: "keyword", rank_col: "rank"})
        .assign(domain=ranks["__domain"])
    )
    sov = compute_share_of_voice(sov_source, origin=args.origin)
    sov = sov.rename(
        columns={"Hits": "Keywords", "Top10": "Top10Keywords", "Top3": "Top3Keywords"}
    )
    sov = sov[sov["domain"] != ""]
    sov = sov.sort_values(["SoV%", "Top3Keywords", "Keywords"], ascending=[False, False, False])

    if not auth.empty and "domain" in auth.columns:
        auth = auth.copy()
        auth["domain"] = auth["domain"].map(normalize_domain)
        sov = sov.merge(auth, on="domain", how="left")

    if not backlinks.empty:
        dom_col = _detect_column(backlinks.columns, "referring_domain", "domain", "host")
        if dom_col:
            ref = backlinks.copy()
            ref[dom_col] = ref[dom_col].map(normalize_domain)
            ref = (
                ref.groupby(dom_col, as_index=False)
                .size()
                .rename(columns={"size": "backlinks", dom_col: "domain"})
            )
            sov = sov.merge(ref, on="domain", how="left")

    with pd.ExcelWriter(args.out_xlsx, engine="xlsxwriter") as xw:
        ranks.to_excel(xw, index=False, sheet_name="Competitor_SERP_Hits")
        sov.to_excel(xw, index=False, sheet_name="Competitor_Scores")
        if not backlinks.empty:
            backlinks.to_excel(xw, index=False, sheet_name="Competitor_Backlinks")
        if not auth.empty:
            auth.to_excel(xw, index=False, sheet_name="Authority_Generic")
    print(f"Wrote {args.out_xlsx}")


if __name__ == "__main__":
    main()
