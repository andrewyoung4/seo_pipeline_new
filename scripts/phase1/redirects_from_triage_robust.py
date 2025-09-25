
import argparse
import pandas as pd
from pathlib import Path

def pick_col(df, *cands):
    cols = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in cols:
            return cols[c.lower()]
    return None

def compute_orphan_flag(df):
    if pick_col(df, "orphan_in_audit"):
        c = pick_col(df, "orphan_in_audit")
        return df[c].astype(bool)

    in_audit = pick_col(df, "in_audit", "present_in_audit", "found_in_audit", "in_crawl")
    in_sitemap = pick_col(df, "in_sitemap", "present_in_sitemap")
    status = pick_col(df, "status", "note")

    if in_audit is not None:
        return ~df[in_audit].astype(bool)

    if status is not None:
        return df[status].astype(str).str.contains("orphan", case=False, na=False)

    if in_audit is None and in_sitemap is not None:
        return ~df[in_sitemap].astype(bool)

    return pd.Series(False, index=df.index)

def main():
    ap = argparse.ArgumentParser(description="Build redirects from triage 'Sitemap Diff' sheet; tolerant to column name variations.")
    ap.add_argument("--triage", required=True, help="phase1_triage.xlsx (merged workbook)")
    ap.add_argument("--csv-out", required=True, help="Output CSV of proposed redirects")
    ap.add_argument("--source-col", default=None, help="Override column for source URL if needed")
    ap.add_argument("--target-col", default=None, help="Override column for target URL if available")
    args = ap.parse_args()

    triage = Path(args.triage)
    xls = pd.ExcelFile(triage)
    sheet = None
    for s in xls.sheet_names:
        if "sitemap" in s.lower() and "diff" in s.lower():
            sheet = s
            break
    if not sheet:
        raise SystemExit("Triage workbook missing a 'Sitemap Diff' sheet (case-insensitive).")

    df = pd.read_excel(triage, sheet_name=sheet)

    source_col = args.source_col or pick_col(df, "url", "loc", "page", "source_url")
    if not source_col:
        raise SystemExit("Could not determine a source URL column (try --source-col).")

    target_col = args.target_col or pick_col(df, "target", "canonical", "suggested_redirect", "preferred_url")

    orphan_flag = compute_orphan_flag(df)
    out = df.loc[orphan_flag].copy()
    out = out[[source_col] + ([target_col] if target_col else [])].rename(columns={source_col: "from_url"})
    if target_col:
        out.rename(columns={target_col: "to_url"}, inplace=True)
    else:
        out["to_url"] = ""

    out.to_csv(args.csv_out, index=False)
    print(f"Wrote {args.csv_out} with {len(out)} redirects. (sheet used: '{sheet}')")

if __name__ == "__main__":
    main()
