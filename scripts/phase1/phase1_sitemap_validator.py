#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase1 Sitemap validator (airtight, products-only by default)

Rules:
- Normalize URLs (https, strip www, drop query/fragment, trim trailing '/').
- Scope defaults to products only; add --scope products,collections to include collections.
- A product is "missing" only if ALL are true:
  * HTTP 200 (from Audit 'Status' if present, else assume 200)
  * Not 'noindex' (from Audit 'Meta Robots' if present)
  * Canonical (from Audit 'Canonical' if present; else self) == self (after normalization)
  * Not present in sitemap set (after normalization)
- If Canonical != self and THAT canonical is in sitemap -> NOT missing.

Outputs: data/outputs/phase1/phase1_Sitemap_Diff.csv with:
  url,type,reason
"""

import argparse, csv, io, os, gzip
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
import requests
import pandas as pd

INDEX_IGNORE = {'/', '/collections', '/collections/all'}

def norm(u: str) -> str:
    p = urlparse(str(u))
    host = (p.netloc or '').lower()
    if host.startswith('www.'): host = host[4:]
    path = p.path or '/'
    if path != '/': path = path.rstrip('/')
    return f"https://{host}{path}"

def ptype(path: str) -> str:
    if path.startswith('/products/'): return 'product'
    if path.startswith('/collections/'): return 'collection'
    if path in INDEX_IGNORE: return 'index'
    return 'other'

def _get_bytes(url: str, timeout=20):
    r = requests.get(url, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    data = r.content
    # handle gzipped sitemaps
    if url.endswith('.gz') or r.headers.get('Content-Type','').endswith('gzip'):
        try:
            data = gzip.decompress(data)
        except Exception:
            pass
    return data

def fetch_sitemap_urls(store: str) -> set[str]:
    """Follow sitemap index and collect all <loc> values, normalized."""
    urls = set()
    seen = set()
    def pull(u):
        if u in seen: return
        seen.add(u)
        try:
            raw = _get_bytes(u)
            root = ET.fromstring(raw)
        except Exception:
            return
        # Namespace-insensitive search
        for loc in root.findall('.//{*}loc'):
            loc_text = (loc.text or '').strip()
            if not loc_text: continue
            if loc_text.endswith('.xml') or loc_text.endswith('.xml.gz'):
                pull(loc_text)
            else:
                urls.add(norm(loc_text))
    pull(f'https://{store}/sitemap.xml')
    return urls

def pick_url_col(df):
    for c in ['URL','Address','Page URL','Final URL','Link']:
        if c in df.columns: return c
    return None

def main():
    ap = argparse.ArgumentParser("Phase1 Sitemap validator (airtight)")
    ap.add_argument("--audit", required=True, help="Path to audit Excel (.xlsx)")
    ap.add_argument("--store", required=True, help="Domain, e.g., silentprincesstt.com")
    ap.add_argument("--sheet", default=None, help="Audit sheet (default first)")
    ap.add_argument("--url-col", dest="url_col", default=None, help="URL column header (e.g., URL)")
    ap.add_argument("--scope", default="products", help="Comma list: products,collections (default products)")
    args = ap.parse_args()

    scope = {s.strip().lower() for s in args.scope.split(',') if s.strip()}
    include_products = 'products' in scope
    include_collections = 'collections' in scope

    xl = pd.ExcelFile(args.audit)
    sheet = args.sheet or xl.sheet_names[0]
    df = xl.parse(sheet_name=sheet)

    url_col = args.url_col or pick_url_col(df)
    if not url_col or url_col not in df.columns:
        raise SystemExit("Could not find URL column; try --sheet and --url-col.")

    # Optional audit columns
    status_col  = 'Status'        if 'Status' in df.columns else None
    robots_col  = None
    for c in df.columns:
        if 'robots' in str(c).lower(): robots_col = c; break
    canonical_col = 'Canonical'    if 'Canonical' in df.columns else None

    df['_u'] = df[url_col].astype(str).map(norm)
    df['_path'] = df['_u'].map(lambda u: urlparse(u).path or '/')
    df['_type'] = df['_path'].map(ptype)

    # Live sitemap set
    site_urls = fetch_sitemap_urls(args.store)

    out = []
    for _, row in df.iterrows():
        u     = row['_u']
        path  = row['_path']
        typ   = row['_type']

        # Scope/exclusions
        if typ == 'index' or typ == 'other': 
            continue
        if typ == 'product' and not include_products: 
            continue
        if typ == 'collection' and not include_collections: 
            continue

        # Already present in sitemap?
        if u in site_urls:
            continue

        # Audit hints: status / robots / canonical
        status = str(row.get(status_col, '')).strip() if status_col else ''
        if status and not status.startswith('2'):
            # Non-200 -> do not count missing (not eligible)
            continue

        robots = str(row.get(robots_col, '')).lower() if robots_col else ''
        if 'noindex' in robots:
            # Intentionally excluded
            continue

        canon = str(row.get(canonical_col, '')).strip() if canonical_col else ''
        canon_norm = norm(canon) if canon else u  # default to self if blank

        # If canonical points elsewhere and that is in sitemap, it's aligned
        if canon_norm != u and canon_norm in site_urls:
            continue

        # At this point, it's eligible and not represented
        reason = 'not in sitemap'
        if canon_norm != u and canon_norm != '':
            reason = 'canonicalized elsewhere (target not in sitemap)'
        out.append((u, typ, reason))

    os.makedirs(os.path.join('data','outputs','phase1'), exist_ok=True)
    out_path = os.path.join('data','outputs','phase1','phase1_Sitemap_Diff.csv')
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f); w.writerow(['url','type','reason']); w.writerows(out)
    print(f"Wrote {out_path} (missing: {len(out)})")

if __name__ == "__main__":
    main()
