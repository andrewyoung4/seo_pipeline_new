# prep_phase1p5_input.py — make a single-sheet triage with 'url'
import argparse, pandas as pd, re
from urllib.parse import urlparse

URL_CANDIDATE_COLS = ["url","page","target_url","canonical","product_url","URL","Address","Page URL","Final URL","Link"]

def norm_url(u: str) -> str:
    u = str(u or "").strip()
    if not u:
        return ""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", u):
        u = "https://" + u
    try:
        p = urlparse(u)
    except Exception:
        return ""
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = p.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return f"https://{host}{path}"

def extract_urls(xlsx_path: str) -> pd.DataFrame:
    xl = pd.ExcelFile(xlsx_path)
    urls = []

    # Prefer Schema sheet → products; then Sitemap_Verified; then scan all sheets.
    if "phase1_Schema_Check" in xl.sheet_names:
        df = xl.parse("phase1_Schema_Check")
        col = next((c for c in df.columns if str(c).strip() in URL_CANDIDATE_COLS), None)
        if col:
            pt = df.columns[df.columns.str.lower().str.contains("pagetype")]
            has_schema = df.columns[df.columns.str.lower().str.contains("hasproductschema")]
            if len(pt):
                prod = df[df[pt[0]].astype(str).str.lower().eq("product")]
                urls.extend(prod[col].astype(str).tolist())
            if len(has_schema):
                good = df[df[has_schema[0]].astype(str).isin([True, "True", "true", "1"])]
                urls.extend(good[col].astype(str).tolist())

    if "phase1_Sitemap_Verified" in xl.sheet_names:
        df = xl.parse("phase1_Sitemap_Verified")
        col = next((c for c in df.columns if str(c).strip() in URL_CANDIDATE_COLS), None)
        if col:
            urls.extend(df[col].astype(str).tolist())

    if not urls:
        for s in xl.sheet_names:
            df = xl.parse(s)
            col = next((c for c in df.columns if str(c).strip() in URL_CANDIDATE_COLS), None)
            if col:
                urls.extend(df[col].astype(str).tolist())

    urls_norm = [norm_url(u) for u in urls]
    urls_norm = [u for u in urls_norm if u.startswith("http")]
    prod_urls = [u for u in urls_norm if "/products/" in u]
    final = prod_urls if prod_urls else urls_norm
    final = sorted(set(final))
    return pd.DataFrame({"url": final})

def main():
    ap = argparse.ArgumentParser(description="Prepare a single-sheet triage with 'url' for phase1p5_keywords.py")
    ap.add_argument("--in", dest="inp", required=True, help="Path to original phase1_triage.xlsx")
    ap.add_argument("--out", dest="out", required=True, help="Path to write simplified triage (single sheet 'urls')")
    args = ap.parse_args()

    df = extract_urls(args.inp)
    if df.empty:
        raise SystemExit("No URLs found in triage. Re-run Phase-1 validators or check the workbook.")

    import os
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with pd.ExcelWriter(args.out, engine="xlsxwriter") as xw:
        df.to_excel(xw, sheet_name="urls", index=False)

    print(f"Wrote {len(df)} URLs to {args.out} (sheet 'urls', column 'url').")

if __name__ == "__main__":
    main()
