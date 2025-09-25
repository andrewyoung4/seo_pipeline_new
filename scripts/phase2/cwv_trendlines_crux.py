#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Collect Core Web Vitals **field** trendlines from the **Chrome UX Report (CrUX) API**.

- Primary key:  CRUX_API_KEY   (recommended)
- Fallback key: PSI_API_KEY    (only if you want to reuse an existing key)

Outputs a tidy CSV: data/outputs/phase2/cwv_trendlines.csv
Columns: template, month, lcp_p75_ms, cls_p75, inp_p75_ms, sample_size

Usage (PowerShell):
$env:CRUX_API_KEY="YOUR_CRUX_API_KEY"
python .\scripts\phase2\cwv_trendlines_crux.py `
  --site https://silentprincesstt.com `
  --triage .\data\inputs\phase2\triage_urls.csv `
  --out .\data\outputs\phase2\cwv_trendlines.csv `
  --months 6
"""
import os, argparse, re, collections, requests, sys
import pandas as pd

CRUX_ENDPOINT = "https://chromeuxreport.googleapis.com/v1/records:query"  # POST JSON

def load_triage(path: str) -> list[str]:
    df = pd.read_csv(path)
    col = next((c for c in df.columns if c.lower() in ("url","address","page url","final url","link")), df.columns[0])
    return df[col].dropna().astype(str).tolist()

def to_template(u: str) -> str:
    u = u.lower()
    if "/products/" in u: return "Products"
    if "/collections/" in u: return "Collections"
    if "/blogs/" in u or "/blog/" in u or "/articles/" in u: return "Blog"
    return "Other"

def pick_representatives(urls: list[str], per_template: int = 8) -> dict[str, list[str]]:
    buckets = collections.defaultdict(list)
    for u in urls:
        buckets[to_template(u)].append(u)
    reps = {}
    for t, arr in buckets.items():
        reps[t] = arr[:per_template]
    # Ensure all standard tabs exist even if empty
    for t in ("Products","Collections","Blog","Other"):
        reps.setdefault(t, [])
    return reps

def crux_query(api_key: str, *, origin: str | None = None, url: str | None = None, months: int = 6) -> dict | None:
    """Call CrUX API for monthly history of LCP/CLS/INP (P75)."""
    payload = {
        "metrics": ["LCP","CLS","INP"],
        "formFactor": "PHONE",
        "history": {"collectionPeriod": {"months": int(months)}}
    }
    if url:
        payload["url"] = url
    elif origin:
        payload["origin"] = origin
    else:
        raise ValueError("Need url or origin")

    r = requests.post(f"{CRUX_ENDPOINT}?key={api_key}", json=payload, timeout=30)
    if r.status_code != 200:
        # Helpful message, but keep the pipeline flowing
        print(f"[warn] CrUX API {r.status_code} for {'url' if url else 'origin'}={url or origin}: {r.text[:250]}", file=sys.stderr)
        return None
    return r.json()

def _get_list(d: dict, path: list[str]):
    """Safely walk nested dicts/lists; return None if missing."""
    cur = d
    for p in path:
        if cur is None: return None
        cur = cur.get(p) if isinstance(cur, dict) else None
    return cur

def extract_timeseries(js: dict) -> list[dict]:
    """
    Handle both response shapes:
      - { "record": {... "metrics": { "LCP": {"percentilesTimeseries": {"p75":{"ms":[...]}} ...}}}}
      - { "records": [ { "collectionPeriod": {...}, "metrics": {...} }, ... ] }  (less common)
    """
    if not js:
        return []

    def to_rows_from_record(rec: dict) -> list[dict]:
        out = []
        # Preferred: percentilesTimeseries
        lcp_ts = _get_list(rec, ["metrics","LCP","percentilesTimeseries","p75"])
        cls_ts = _get_list(rec, ["metrics","CLS","percentilesTimeseries","p75"])
        inp_ts = _get_list(rec, ["metrics","INP","percentilesTimeseries","p75"])
        # Fallback: single percentiles (no history)
        lcp_p75 = _get_list(rec, ["metrics","LCP","percentiles","p75"])
        cls_p75 = _get_list(rec, ["metrics","CLS","percentiles","p75"])
        inp_p75 = _get_list(rec, ["metrics","INP","percentiles","p75"])

        # Timeseries labels
        months = _get_list(rec, ["collectionPeriod","months"]) or _get_list(rec, ["key","collectionPeriod","months"])
        # Some responses instead expose "collectionDate" lists or implicit monthly buckets;
        # if labels are missing, we synthesize 1..N and let the UI list 'Months: N points'.
        n = 0
        def as_list(x):
            if x is None: return []
            if isinstance(x, dict) and "ms" in x:  # e.g., {"ms":[...]}, {"value":[...]}
                return x.get("ms") or x.get("value") or []
            if isinstance(x, dict) and "value" in x:
                return x.get("value") or []
            return x if isinstance(x, list) else [x]

        lcp_list = as_list(lcp_ts) or (as_list(lcp_p75))
        cls_list = as_list(cls_ts) or (as_list(cls_p75))
        inp_list = as_list(inp_ts) or (as_list(inp_p75))
        n = max(len(lcp_list), len(cls_list), len(inp_list))

        if n == 0:
            # No history available; return single-point if present so the card can still render a row
            if any(x is not None for x in (lcp_p75, cls_p75, inp_p75)):
                out.append({
                    "month": "latest",
                    "lcp_p75_ms": int(lcp_p75.get("ms") if isinstance(lcp_p75, dict) else lcp_p75) if lcp_p75 is not None else None,
                    "cls_p75": float(cls_p75.get("value") if isinstance(cls_p75, dict) else cls_p75) if cls_p75 is not None else None,
                    "inp_p75_ms": int(inp_p75.get("ms") if isinstance(inp_p75, dict) else inp_p75) if inp_p75 is not None else None,
                    "sample_size": ""
                })
            return out

        # Build synthetic month labels if none are provided
        labels = months if isinstance(months, list) and len(months) == n else [f"m-{i+1}" for i in range(n)]

        for i in range(n):
            out.append({
                "month": labels[i],
                "lcp_p75_ms": int(lcp_list[i]) if i < len(lcp_list) and lcp_list[i] is not None else None,
                "cls_p75": float(cls_list[i]) if i < len(cls_list) and cls_list[i] is not None else None,
                "inp_p75_ms": int(inp_list[i]) if i < len(inp_list) and inp_list[i] is not None else None,
                "sample_size": ""
            })
        return out

    rows: list[dict] = []
    if "record" in js and isinstance(js["record"], dict):
        rows.extend(to_rows_from_record(js["record"]))
    elif "records" in js and isinstance(js["records"], list):
        for rec in js["records"]:
            rows.extend(to_rows_from_record(rec))
    return rows

def main():
    ap = argparse.ArgumentParser(description="CrUX API trendlines → CSV")
    ap.add_argument("--site", required=True, help="Origin like https://example.com")
    ap.add_argument("--triage", required=True, help="CSV of site URLs (triage list)")
    ap.add_argument("--out", required=True, help="Output CSV for trendlines")
    ap.add_argument("--months", type=int, default=6, help="Months of history to request (e.g., 6 or 12)")
    ap.add_argument("--per-template", type=int, default=8, help="Representative URLs per template to try")
    args = ap.parse_args()

    # ---- API key: prefer CRUX_API_KEY, fallback PSI_API_KEY only if present
    api_key = (os.getenv("CRUX_API_KEY") or os.getenv("PSI_API_KEY") or "").strip()
    if not api_key:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        pd.DataFrame(columns=["template","month","lcp_p75_ms","cls_p75","inp_p75_ms","sample_size"]).to_csv(args.out, index=False)
        print("[warn] No CRUX_API_KEY/PSI_API_KEY found. Wrote empty trendlines CSV so the report shows a friendly 'no data yet' state.")
        sys.exit(0)

    urls = load_triage(args.triage)
    reps = pick_representatives(urls, per_template=args.per_template)
    origin = args.site.rstrip("/") + "/"

    all_rows: list[dict] = []
    for tpl, sample_urls in reps.items():
        got = False
        # Try page-level CrUX first (some pages won’t have it)
        for u in sample_urls:
            js = crux_query(api_key, url=u, months=args.months)
            ts = extract_timeseries(js)
            if ts:
                for r in ts:
                    r2 = dict(r); r2["template"] = tpl; all_rows.append(r2)
                got = True
                break
        # Fallback to origin-level trend if no page had data
        if not got:
            js = crux_query(api_key, origin=origin, months=args.months)
            ts = extract_timeseries(js)
            for r in ts:
                r2 = dict(r); r2["template"] = tpl; all_rows.append(r2)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    pd.DataFrame(all_rows, columns=["template","month","lcp_p75_ms","cls_p75","inp_p75_ms","sample_size"]).to_csv(args.out, index=False)
    print(f"Wrote {len(all_rows)} rows → {args.out}")
    print("[info] Source: Chrome UX Report (CrUX) API")

if __name__ == "__main__":
    main()
