import argparse, pathlib, sys, re
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
    # try common sheets
    for sh in ["Internal", "All Pages", "Pages", "Crawl Data", "Per-URL Checklist"]:
        try:
            return pd.read_excel(xlsx, sheet_name=sh)
        except Exception:
            continue
    raise SystemExit("Could not find a suitable sheet. Try --sheet.")

def infer_wordcount(df):
    # prefer existing columns
    for c in ["Word Count","WordCount","Words","Body Words"]:
        if c in df.columns:
            return df[c].fillna(0).astype(int)
    # fallback: compute from a text column
    text_col = None
    for c in ["Text","BodyText","Content","Extracted Text"]:
        if c in df.columns:
            text_col = c; break
    if text_col is None:
        # last resort: use Title + Meta
        t = df.get("Title 1") if "Title 1" in df.columns else df.get("Title")
        m = df.get("Meta Description")
        combo = (t.fillna("") + " " + (m.fillna("") if m is not None else ""))
        return combo.str.split().apply(len)
    return df[text_col].fillna("").astype(str).str.split().apply(len)

def main():
    ap = argparse.ArgumentParser("Phase1 Thin Content detector")
    ap.add_argument("--audit", required=True, help="Path to audit workbook (xlsx)")
    ap.add_argument("--sheet", help="Sheet name to read (auto if omitted)")
    ap.add_argument("--url-col", default=None, help="URL column (default auto)")
    ap.add_argument("--min-words", type=int, default=120)
    args = ap.parse_args()

    outdir = ensure_dirs()
    df = load_frame(args.audit, args.sheet)

    # URL column
    url_col = args.url_col
    if not url_col:
        for c in ["Address","URL","Url","address"]:
            if c in df.columns: url_col=c; break
    if not url_col:
        raise SystemExit("Could not find URL column; try --url-col.")

    wc = infer_wordcount(df)
    out = df.copy()
    out["WordCount_Est"] = wc
    out["Thin_Flag"] = (wc < args.min_words)

    cols = [url_col, "WordCount_Est", "Thin_Flag"]
    for c in ["Title","Title 1","Meta Description","H1"]:
        if c in out.columns: cols.append(c)
    out = out[cols]

    out_path = outdir / "thin_content.xlsx"
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        out.to_excel(xw, index=False, sheet_name="thin_content")
    print(f"Wrote {out_path}")

if __name__ == "__main__":
    main()
