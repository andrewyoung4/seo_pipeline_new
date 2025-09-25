#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backlinks (Free) via Serper Mentions + HTML Verification — FIXED
- Adds missing `from pathlib import Path`
- Uses timezone-aware UTC datetimes (datetime.now(datetime.UTC))
"""

import os, sys, csv, time, json, re
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

USER_AGENT = "Mozilla/5.0 (compatible; SilentPrincessBacklinkBot/1.0)"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "text/html,application/json;q=0.9,*/*;q=0.8"}

def domain_only(u: str) -> str:
    try:
        host = urlparse(u).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""

def unique(seq):
    seen = set(); out = []
    for x in seq:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def derive_domains(df: pd.DataFrame, domain_col: str) -> List[str]:
    if domain_col in df.columns:
        vals = [str(x).strip().lower() for x in df[domain_col].dropna().tolist()]
        return unique([v for v in vals if v and "." in v])
    url_candidates = [c for c in df.columns if "url" in c.lower() or "link" in c.lower()]
    for col in url_candidates:
        ds = [domain_only(str(u)) for u in df[col].dropna().astype(str).tolist()]
        ds = [d for d in ds if d]
        if ds:
            return unique(ds)
    return []

def serper_search(q: str, api_key: str, timeout: int = 10):
    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    payload = {"q": q, "hl": "en", "num": 20}
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return data.get("organic", []) or []

def fetch_and_check(url: str, target_domain: str, timeout: int = 10):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        status = r.status_code
        if status != 200 or not r.text:
            return status, []
        soup = BeautifulSoup(r.text, "html.parser")
        out = []
        for a in soup.find_all("a", href=True):
            href = a.get("href") or ""
            if target_domain in href:
                text = (a.get_text(strip=True) or "")[:300]
                rel = a.get("rel") or []
                nofollow = any("nofollow" in str(x).lower() for x in rel)
                out.append((href, text, nofollow))
        return status, out
    except Exception:
        return 0, []

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Backlinks (Free) via Serper mentions + HTML verification")
    ap.add_argument("--domains-csv", type=str, required=True, help="CSV with domains")
    ap.add_argument("--domain-col", type=str, default="domain", help="Column containing domains (default: domain)")
    ap.add_argument("--include-self", action="store_true", help="Include your own domain if present in the CSV")
    ap.add_argument("--brand-map-csv", type=str, default="", help="Optional CSV with columns domain,brand")
    ap.add_argument("--out-csv", type=str, required=True, help="Output CSV path")
    ap.add_argument("--limit-per-domain", type=int, default=50, help="Max SERP results per query type")
    ap.add_argument("--qps", type=float, default=0.8, help="Queries per second to Serper")
    ap.add_argument("--timeout", type=int, default=10, help="Per-request timeout (seconds)")
    args = ap.parse_args()

    api_key = os.environ.get("SERPER_API_KEY", "").strip()
    if not api_key:
        print("ERROR: SERPER_API_KEY not set.", file=sys.stderr)
        sys.exit(2)

    df = pd.read_csv(args.domains_csv)
    domains = derive_domains(df, args.domain_col)

    if not args.include_self:
        domains = [d for d in domains if d not in ("silentprincesstt.com","www.silentprincesstt.com")]

    brands = {}
    if args.brand_map_csv and os.path.isfile(args.brand_map_csv):
        bdf = pd.read_csv(args.brand_map_csv)
        cols = {c.lower(): c for c in bdf.columns}
        if "domain" in cols and "brand" in cols:
            for _, row in bdf.iterrows():
                d = str(row[cols["domain"]]).strip().lower()
                b = str(row[cols["brand"]]).strip()
                if d and b:
                    brands[d] = b

    rows = []

    def qsleep():
        time.sleep(1.0/max(0.1, args.qps))

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for d in domains:
        base_queries = [
            (f'"{d}" -site:{d}', "domain_quoted"),
            (f'intext:"{d}" -site:{d}', "domain_intext"),
        ]
        if d in brands:
            b = brands[d]
            base_queries.append((f'"{b}" -site:{d}', "brand_quoted"))

        seen_referrers = set()
        results_processed = 0

        for q, qtype in base_queries:
            try:
                organics = serper_search(q, api_key, timeout=args.timeout)
            except requests.HTTPError as e:
                if getattr(e, "response", None) is not None and e.response.status_code == 429:
                    time.sleep(2.5); organics = []
                else:
                    organics = []
            except Exception:
                organics = []

            for rank, item in enumerate(organics, start=1):
                if results_processed >= args.limit_per_domain:
                    break
                url = item.get("link") or item.get("url") or ""
                if not url:
                    continue
                ref_dom = domain_only(url)
                if not ref_dom or ref_dom == d or ref_dom.endswith(("google.com","bing.com","yahoo.com")):
                    continue
                if url in seen_referrers:
                    continue

                status, links = fetch_and_check(url, d, timeout=args.timeout)
                seen_referrers.add(url)
                results_processed += 1

                if not links:
                    rows.append({
                        "target_domain": d,
                        "referring_url": url,
                        "referring_domain": ref_dom,
                        "anchor_href": "",
                        "anchor_text": "",
                        "rel_nofollow": "",
                        "query_type": qtype,
                        "serp_rank": rank,
                        "http_status": status,
                        "captured_at": now
                    })
                else:
                    for href, text, nofollow in links:
                        rows.append({
                            "target_domain": d,
                            "referring_url": url,
                            "referring_domain": ref_dom,
                            "anchor_href": href,
                            "anchor_text": text,
                            "rel_nofollow": "yes" if nofollow else "no",
                            "query_type": qtype,
                            "serp_rank": rank,
                            "http_status": status,
                            "captured_at": now
                        })
                qsleep()

    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False, encoding="utf-8")
    print(f"[OK] Wrote {out_path} with {len(rows)} rows.")

if __name__ == "__main__":
    main()
