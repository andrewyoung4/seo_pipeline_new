#!/usr/bin/env python3
"""
normalize_urls_from_triage_v2.py
Usage:
  py normalize_urls_from_triage_v2.py --triage ".\data\outputs\phase1\phase1_triage.xlsx" --sheet "Schema Check" --url-col "URL" --out ".\data\inputs\phase2\triage_urls.csv"

Description:
  Reads a Phase-1 triage workbook, finds the URL column you specify (or best-guess),
  normalizes to parent product URLs (no ?variant=, no /variants/<id>), filters to /products/,
  and writes a CSV with a single 'URL' column (unique).
"""
import argparse, re, sys
import pandas as pd
from urllib.parse import urlparse, urlunparse

CANDIDATES = ['url','page','target_url','canonical','product_url','URL','Address','Page URL','Final URL','Link']

def clean_url(url: str) -> str:
    if not isinstance(url, str):
        url = str(url or '')
    url = url.strip()
    if not url:
        return ''
    try:
        p = urlparse(url)
    except Exception:
        return ''
    path = re.sub(r'/variants/\d+(?=/|$)', '', p.path or '')
    clean = urlunparse((p.scheme or 'https', p.netloc.lower(), path, '', '', ''))
    clean = re.sub(r'\?$', '', clean)
    clean = re.sub(r'(?<!:)//+', '/', clean)
    # standardize host without www.
    if clean.startswith('https://www.'):
        clean = 'https://' + clean[12:]
    return clean.rstrip('/')

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--triage', required=True, help='Path to triage .xlsx')
    ap.add_argument('--out', required=True, help='Path to output CSV')
    ap.add_argument('--sheet', default=None, help='Sheet name to read (default: first sheet)')
    ap.add_argument('--url-col', dest='url_col', default=None, help='URL column name (case-insensitive)')
    args = ap.parse_args()

    xl = pd.ExcelFile(args.triage)
    sheet = args.sheet or xl.sheet_names[0]
    if sheet not in xl.sheet_names:
        print(f"[ERROR] Worksheet named '{sheet}' not found. Available: {xl.sheet_names}", file=sys.stderr)
        sys.exit(2)

    df = xl.parse(sheet)
    col = args.url_col
    if not col:
        lower = {str(c).lower(): c for c in df.columns}
        for cand in CANDIDATES:
            if cand.lower() in lower:
                col = lower[cand.lower()]
                break
    else:
        # resolve case-insensitively
        name_map = {str(c).lower(): c for c in df.columns}
        key = str(col).lower()
        if key in name_map:
            col = name_map[key]
        else:
            print(f"[ERROR] URL column '{args.url_col}' not found in sheet '{sheet}'. Available columns: {list(df.columns)}", file=sys.stderr)
            sys.exit(3)

    if not col:
        print(f"[ERROR] Could not find a URL-like column. Available columns: {list(df.columns)}", file=sys.stderr)
        sys.exit(4)

    urls = df[col].astype(str).map(clean_url)
    urls = urls[urls.str.contains(r'^https://', na=False)]
    # keep only product pages
    urls = urls[urls.str.contains(r'/products/', na=False)]
    urls = urls.drop_duplicates().sort_values()

    out = pd.DataFrame({'URL': urls})
    out.to_csv(args.out, index=False, encoding='utf-8')
    print(f"[OK] Wrote {len(out)} URLs to {args.out} (sheet='{sheet}', column='{col}')")

if __name__ == '__main__':
    main()
