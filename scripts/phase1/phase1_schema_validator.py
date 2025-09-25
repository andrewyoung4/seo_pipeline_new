#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase1 Schema validator (basic Product JSON-LD presence)

Usage:
  python scripts/phase1/phase1_schema_validator.py \
    --audit .\data\outputs\audits\shopify_sf_audit.cleaned.xlsx \
    [--sheet SHEET] [--url-col URL_COL] [--live] [--timeout 15]

Outputs:
  data/outputs/phase1/phase1_Schema_Check.csv

Notes:
  - Only checks /products/* URLs (homepage/collections marked N/A).
  - Detection is whitespace-tolerant: r'"@type"\s*:\s*"Product"'
"""

import argparse
import csv
import os
import re
import sys
import time
from urllib.parse import urlparse

import requests

# pandas is used to read Excel reliably (sheet + header inference)
try:
    import pandas as pd
except Exception as e:
    print("ERROR: pandas is required for this script (pip install pandas openpyxl)", file=sys.stderr)
    raise

PRODUCT_TYPE_REGEX = re.compile(r'"@type"\s*:\s*"Product"', flags=re.IGNORECASE)

COMMON_URL_HEADERS = [
    "URL", "Url", "PageURL", "Page Url", "PageURLNormalized",
    "Link", "Address", "Final URL", "Product URL"
]

DEFAULT_TIMEOUT = 15
USER_AGENT = "Phase1-SchemaValidator/1.0 (+https://silentprincesstt.com)"

def is_product_path(u: str) -> bool:
    try:
        p = urlparse(u).path or "/"
    except Exception:
        return False
    return p.startswith("/products/")

def fetch_html(url: str, timeout: int) -> tuple[int, str, str]:
    """
    Returns (status_code, html_text, err_msg)
    """
    try:
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        text = resp.text or ""
        return resp.status_code, text, ""
    except requests.RequestException as e:
        return 0, "", str(e)

def detect_product_schema(html: str) -> bool:
    # whitespace-tolerant detection of Product JSON-LD
    return bool(PRODUCT_TYPE_REGEX.search(html or ""))

def infer_url_column(df: "pd.DataFrame") -> str | None:
    lower_map = {c.lower().strip(): c for c in df.columns}
    for cand in COMMON_URL_HEADERS:
        if cand in df.columns:
            return cand
        if cand.lower().strip() in lower_map:
            return lower_map[cand.lower().strip()]
    # Fallback: the first column that looks like URLs
    for c in df.columns:
        sample = str(df[c].astype(str).head(20).dropna().tolist())
        if "http://" in sample or "https://" in sample:
            return c
    return None

def main() -> int:
    ap = argparse.ArgumentParser("Phase1 Schema validator (basic Product JSON-LD presence)")
    ap.add_argument("--audit", required=True, help="Path to audit Excel (.xlsx)")
    ap.add_argument("--sheet", default=None, help="Worksheet name (defaults to first sheet)")
    ap.add_argument("--url-col", dest="url_col", default=None, help="Column header containing URLs")
    ap.add_argument("--live", action="store_true", help="Fetch live HTML (recommended)")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help=f"HTTP timeout seconds (default {DEFAULT_TIMEOUT})")
    args = ap.parse_args()

    audit_path = args.audit
    if not os.path.isfile(audit_path):
        print(f"ERROR: audit file not found: {audit_path}", file=sys.stderr)
        return 2

    # Load Excel
    try:
        xl = pd.ExcelFile(audit_path)
        sheet_name = args.sheet or xl.sheet_names[0]
        df = xl.parse(sheet_name=sheet_name)
    except Exception as e:
        print(f"ERROR: failed reading Excel '{audit_path}': {e}", file=sys.stderr)
        return 3

    if df.empty:
        print("ERROR: audit worksheet is empty.", file=sys.stderr)
        return 4

    url_col = args.url_col or infer_url_column(df)
    if not url_col or url_col not in df.columns:
        print("ERROR: Could not find URL column; try --sheet and --url-col.", file=sys.stderr)
        print(f"Detected headers: {list(df.columns)}", file=sys.stderr)
        return 5

    out_dir = os.path.join("data", "outputs", "phase1")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "phase1_Schema_Check.csv")

    rows_out = []
    for idx, row in df.iterrows():
        raw_url = str(row.get(url_col, "")).strip()
        if not raw_url:
            rows_out.append({
                "URL": "",
                "HTTPStatus": "",
                "PageType": "",
                "HasProductSchema": "",
                "NeedsFix": "",
                "Notes": "Empty URL cell"
            })
            continue

        page_type = "product" if is_product_path(raw_url) else "non-product"

        # Only evaluate products for Phase-1 Product schema
        if page_type != "product":
            rows_out.append({
                "URL": raw_url,
                "HTTPStatus": "",
                "PageType": page_type,
                "HasProductSchema": "",
                "NeedsFix": "",
                "Notes": "Non-product URL skipped for Product schema"
            })
            continue

        status, html, err = (0, "", "")
        if args.live:
            status, html, err = fetch_html(raw_url, timeout=args.timeout)
        else:
            # If not live, we still fetch (no cache layer in this drop-in). Keep flag for CLI compatibility.
            status, html, err = fetch_html(raw_url, timeout=args.timeout)

        if err:
            rows_out.append({
                "URL": raw_url,
                "HTTPStatus": status,
                "PageType": page_type,
                "HasProductSchema": False,
                "NeedsFix": "Yes",
                "Notes": f"Fetch error: {err}"
            })
            continue

        has_schema = detect_product_schema(html)
        rows_out.append({
            "URL": raw_url,
            "HTTPStatus": status,
            "PageType": page_type,
            "HasProductSchema": has_schema,
            "NeedsFix": "No" if has_schema else "Yes",
            "Notes": "" if has_schema else "Product JSON-LD not detected"
        })

        # Be polite to the site
        time.sleep(0.15)

    # Write CSV
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["URL", "HTTPStatus", "PageType", "HasProductSchema", "NeedsFix", "Notes"]
        )
        writer.writeheader()
        for r in rows_out:
            writer.writerow(r)

    print(f"Wrote: {out_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
