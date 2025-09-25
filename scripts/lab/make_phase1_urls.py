
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
[Additive] make_phase1_urls.py — generate a Lighthouse URL list from your audit or triage.

Usage (from repo root):
  python .\scripts\lab\make_phase1_urls.py ^
    --audit .\data\outputs\audit\shopify_sf_audit.cleaned.xlsx ^
    --out .\data\outputs\phase1\phase1_urls.txt ^
    --limit 50 --templates home,collections,products,pages

Inputs:
  EITHER --audit <xlsx> OR --triage <xlsx>
Output:
  A newline-delimited URLs file suitable for run_lighthouse_batch.js
"""
import argparse, re, pandas as pd, os
from urllib.parse import urlparse

def guess_sheet_and_urlcol(xlsx_path: str):
    xl = pd.ExcelFile(xlsx_path)
    # Prefer "Internal" if present
    sheet = "Internal" if "Internal" in xl.sheet_names else xl.sheet_names[0]
    df = xl.parse(sheet)
    # Find a URL-like column
    candidates = ["URL","Address","Page URL","Final URL","Link"]
    for c in candidates:
        if c in df.columns:
            return df, sheet, c
    # Case-insensitive fallback
    for c in df.columns:
        if re.search(r"\b(url|address|final url|page url|link)\b", str(c), re.I):
            return df, sheet, c
    raise SystemExit("Could not find a URL column in the workbook.")

def norm_url(u: str) -> str:
    u = (str(u) or "").strip()
    if not u:
        return ""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", u):
        u = "https://" + u.lstrip("/")
    # normalize hostname (strip www)
    p = urlparse(u)
    host = p.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    path = p.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return f"https://{host}{path}"

def template_of(path: str) -> str:
    p = path.lower()
    if p == "/" or p == "":
        return "home"
    if p.startswith("/collections"):
        return "collections"
    if p.startswith("/products"):
        return "products"
    if p.startswith("/blogs") or p.startswith("/blog"):
        return "blog"
    if p.startswith("/pages"):
        return "pages"
    return "other"

def main():
    ap = argparse.ArgumentParser(description="Generate phase1_urls.txt for Lighthouse batch.")
    ap.add_argument("--audit", help="Path to audit cleaned .xlsx")
    ap.add_argument("--triage", help="Path to phase1_triage.xlsx")
    ap.add_argument("--out", required=True, help="Where to write urls.txt")
    ap.add_argument("--limit", type=int, default=50, help="Max URLs to include")
    ap.add_argument("--templates", default="home,collections,products,pages",
                    help="Comma list of templates to include (e.g., home,collections,products,pages,blog,other)")
    args = ap.parse_args()

    if not args.audit and not args.triage:
        raise SystemExit("Provide --audit or --triage")

    if args.audit:
        df, sheet, url_col = guess_sheet_and_urlcol(args.audit)
        urls = [norm_url(u) for u in df[url_col].astype(str).tolist() if str(u).strip()]
    else:
        # triage may not have a single URL column across sheets; try to read a union of known sheets
        xl = pd.ExcelFile(args.triage)
        urls = []
        for s in xl.sheet_names:
            df = xl.parse(s)
            for c in df.columns:
                if re.search(r"\b(url|address|page url|final url|link)\b", str(c), re.I):
                    urls.extend([norm_url(u) for u in df[c].astype(str).tolist() if str(u).strip()])
        urls = list(dict.fromkeys(urls))

    # Filter to included templates
    allowed = set([t.strip().lower() for t in args.templates.split(",") if t.strip()])
    def allowed_url(u):
        from urllib.parse import urlparse
        return template_of(urlparse(u).path) in allowed

    urls = [u for u in urls if u]
    # Deduplicate, preserve order
    urls = list(dict.fromkeys(urls))
    urls = [u for u in urls if allowed_url(u)]

    # Downsample by simple interleave to keep variety by template
    # Group by template first
    from collections import defaultdict
    groups = defaultdict(list)
    from urllib.parse import urlparse
    for u in urls:
        groups[template_of(urlparse(u).path)].append(u)
    # round-robin until limit
    ordered = []
    ptrs = {k:0 for k in groups}
    keys = [k for k in ["home","collections","products","pages","blog","other"] if k in groups]
    while len(ordered) < args.limit and any(ptrs[k] < len(groups[k]) for k in keys):
        for k in keys:
            i = ptrs[k]
            if i < len(groups[k]):
                ordered.append(groups[k][i])
                ptrs[k] += 1
            if len(ordered) >= args.limit:
                break

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        for u in ordered:
            f.write(u + "\n")
    print(f"Wrote {args.out} with {len(ordered)} URLs (templates={','.join(keys)}).")

if __name__ == "__main__":
    main()
