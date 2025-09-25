import argparse, pathlib, sys, hashlib
try:
    import pandas as pd
except Exception:
    print("Missing deps: pip install pandas")
    sys.exit(1)

def ensure_dirs():
    outdir = pathlib.Path("data/outputs/phase1")
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir

def load_frame(xlsx, sheet=None):
    if sheet:
        return pd.read_excel(xlsx, sheet_name=sheet)
    for sh in ["Internal","All Pages","Pages","Crawl Data"]:
        try:
            return pd.read_excel(xlsx, sheet_name=sh)
        except Exception:
            continue
    raise SystemExit("Could not find a suitable sheet. Try --sheet.")

def norm(s):
    return " ".join(str(s or "").strip().split()).lower()

def h(s):
    return hashlib.sha1(norm(s).encode("utf-8")).hexdigest()

def main():
    ap = argparse.ArgumentParser("Phase1 Duplicate Content detector")
    ap.add_argument("--audit", required=True, help="Path to audit workbook (xlsx)")
    ap.add_argument("--sheet", help="Sheet name to read (auto if omitted)")
    ap.add_argument("--url-col", default=None, help="URL column (default auto)")
    args = ap.parse_args()

    outdir = ensure_dirs()
    df = load_frame(args.audit, args.sheet)

    # columns to check
    cols = []
    title_cols = [c for c in ["Title","Page Title","Title 1"] if c in df.columns]
    meta_cols  = [c for c in ["Meta Description","Description"] if c in df.columns]
    body_cols  = [c for c in ["Text","BodyText","Content","Extracted Text"] if c in df.columns]
    if title_cols: cols.append(("Title", title_cols[0]))
    if meta_cols:  cols.append(("Meta",  meta_cols[0]))
    if body_cols:  cols.append(("Body",  body_cols[0]))
    if not cols:
        raise SystemExit("No Title/Meta/Body columns found to compare.")

    # URL column
    url_col = args.url_col
    if not url_col:
        for c in ["Address","URL","Url","address"]:
            if c in df.columns: url_col=c; break
    if not url_col:
        raise SystemExit("Could not find URL column; try --url-col.")

    out_frames = []
    for label, col in cols:
        g = df.groupby(df[col].apply(h))
        clusters = []
        for key, grp in g:
            urls = grp[url_col].astype(str).tolist()
            if len(urls) > 1:
                sample = (grp[col].astype(str).iloc[0])[:240]
                clusters.append({"Type": label, "Hash": key, "Count": len(urls),
                                 "Sample": sample, "URLs": "\n".join(urls)})
        if clusters:
            out_frames.append((label, pd.DataFrame(clusters).sort_values("Count", ascending=False)))

    out_path = outdir / "duplicate_content.xlsx"
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        if out_frames:
            for label, frame in out_frames:
                frame.to_excel(xw, index=False, sheet_name=f"dup_{label.lower()}")
        else:
            pd.DataFrame({"note":["No duplicate clusters found."]}).to_excel(xw, index=False, sheet_name="summary")
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    main()
