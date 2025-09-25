#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
filter_serp_by_domains.py
Focus SOV on *actual competitors* by filtering SERP rows to an allow-list of domains.

Usage example:
  python .\scripts\phase3\filter_serp_by_domains.py ^
    --serp .\data\outputs\phase3\serp_results.csv ^
    --allow-from-csv .\data\inputs\phase3\competitors_urls.csv ^
    --out .\data\outputs\phase3\serp_results_allowed.csv
"""
import argparse, csv, sys
from pathlib import Path
from urllib.parse import urlparse

def _root(u_or_domain: str) -> str:
    if not u_or_domain: return ""
    u_or_domain = u_or_domain.strip()
    if u_or_domain.startswith("http"):
        try:
            host = urlparse(u_or_domain).netloc.lower()
        except Exception:
            host = ""
    else:
        host = u_or_domain.lower()
    if host.startswith("www."):
        host = host[4:]
    return host

def _read_allow_csv(path):
    # accept either a 'domain' column or a 'url' column with full URLs
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        fields = {c.lower(): c for c in (r.fieldnames or [])}
        dom_key = fields.get("domain")
        url_key = fields.get("url")
        allowed = set()
        for row in r:
            src = row.get(dom_key) if dom_key else None
            if not src and url_key:
                src = row.get(url_key)
            host = _root(src or "")
            if host:
                allowed.add(host)
    return allowed

def _read_serp(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        fields = {c.lower(): c for c in (r.fieldnames or [])}
        qk = fields.get("query","query")
        uk = fields.get("url","url")
        dk = fields.get("domain") or "domain"
        rk = fields.get("rank") or "rank"
        for row in r:
            rows.append({"query": row.get(qk,""), "url": row.get(uk,""), "domain": row.get(dk,""), "rank": row.get(rk,"")})
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--serp", required=True)
    ap.add_argument("--allow-from-csv", required=True)
    ap.add_argument("--out", default=r".\data\outputs\phase3\serp_results_allowed.csv")
    ap.add_argument("--write-allow-txt", default=r".\data\inputs\phase3\allow_domains.txt")
    args = ap.parse_args()

    allow = _read_allow_csv(args.allow_from_csv)
    if not allow:
        raise SystemExit("No allow-list domains found in the CSV. Expect a 'domain' or 'url' column.")
    rows = _read_serp(args.serp)
    kept = [r for r in rows if _root(r.get("domain") or r.get("url")) in allow]

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["query","url","domain","rank"])
        w.writeheader()
        for r in kept:
            w.writerow(r)

    # also write a normalized allow-list file for reuse
    Path(args.write_allow_txt).parent.mkdir(parents=True, exist_ok=True)
    with open(args.write_allow_txt, "w", encoding="utf-8") as f:
        for d in sorted(allow):
            f.write(d + "\n")

    print(f"[done] wrote {args.out} with {len(kept)} rows")
    print(f"[done] wrote allow-list -> {args.write_allow_txt}")

if __name__ == "__main__":
    main()
