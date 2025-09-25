
#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
[Replacement] phase1p5_keywords.py — Build Phase-2 keyword_map.csv from triage/audit + Serper

Purpose
-------
Generate a lightweight keyword map (seed -> suggested queries) to feed Phase-2.
You can derive seeds from your Phase-1 triage/audit and/or pass a seeds file.

Env
----
Requires SERPER_API_KEY in your environment.

Examples (from repo root)
-------------------------
# From triage only
python .\scripts\phase1\phase1p5_keywords.py ^
  --triage .\data\outputs\phase1\phase1_triage.xlsx ^
  --out .\data\inputs\phase2\keyword_map.csv ^
  --per-seed 5 --use-related --max-queries 500 --sleep 0.3

# From audit + a custom seeds file (combined & de-duped)
python .\scripts\phase1\phase1p5_keywords.py ^
  --audit .\data\outputs\audit\shopify_sf_audit.cleaned.xlsx ^
  --seeds .\data\inputs\seeds\keywords.txt ^
  --out .\data\inputs\phase2\keyword_map.csv ^
  --per-seed 8 --sleep 0.25
"""

import argparse, os, re, time, json
import pandas as pd
import requests
from urllib.parse import urlparse

def norm_url(u: str) -> str:
    u = (str(u) or "").strip()
    if not u:
        return ""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", u):
        u = "https://" + u.lstrip("/")
    p = urlparse(u)
    host = p.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    path = p.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return f"https://{host}{path}"

def words_from_slug(path: str):
    slug = re.sub(r"^/+|/+$", "", path or "").split("/")[-1]
    slug = re.sub(r"[\W_]+", " ", slug)
    slug = re.sub(r"\s+", " ", slug).strip()
    return slug

def harvest_seeds_from_xlsx(xlsx_path: str) -> list:
    try:
        xl = pd.ExcelFile(xlsx_path)
    except Exception:
        return []
    seeds = []
    for sheet in xl.sheet_names:
        df = xl.parse(sheet)
        cols = [c for c in df.columns if re.search(r"\b(url|address|page url|final url|link|title)\b", str(c), re.I)]
        if not cols:
            continue
        for c in cols:
            if re.search(r"title", str(c), re.I):
                for v in df[c].dropna().astype(str).tolist():
                    v = re.sub(r"\s+", " ", v).strip()
                    if v: seeds.append(v.lower())
            else:
                # URL-like
                for u in df[c].dropna().astype(str).tolist():
                    u = norm_url(u)
                    if not u: 
                        continue
                    p = urlparse(u)
                    txt = words_from_slug(p.path)
                    if txt:
                        seeds.append(txt.lower())
    # clean & dedupe (preserve order)
    cleaned, seen, uniq = [], set(), []
    for s in seeds:
        s = s.strip(" -_/|")
        s = re.sub(r"\s+", " ", s)
        if 2 <= len(s) <= 60:
            cleaned.append(s)
    for s in cleaned:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq

def load_seed_file(path: str) -> list:
    if not path or not os.path.isfile(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = re.sub(r"\s+", " ", line.strip())
            if s:
                out.append(s.lower())
    # dedupe preserve order
    seen, uniq = set(), []
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq

def serper_search(q: str, api_key: str, gl="us", hl="en"):
    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    payload = {"q": q, "gl": gl, "hl": hl, "num": 10}
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Serper error {r.status_code}: {r.text[:160]}")
    return r.json()

def expand_seed(seed: str, api_key: str, use_related=True, per_seed=5, sleep=0.3):
    data = serper_search(seed, api_key)
    results = []
    # Related searches first
    if use_related:
        rel = data.get("relatedSearches") or []
        for item in rel[:per_seed]:
            q = (item.get("query") or "").strip()
            if q:
                results.append(("related", q, ""))
                if len(results) >= per_seed:
                    break
    # If still short, use organic titles
    if len(results) < per_seed:
        org = data.get("organic") or []
        for item in org:
            title = (item.get("title") or "").strip()
            if not title:
                continue
            q = re.sub(r"[\|\-–—]+.*$", "", title).strip()
            if q and len(q) > 2:
                results.append(("organic", q, title))
                if len(results) >= per_seed:
                    break
    time.sleep(max(0.0, float(sleep)))
    return results

def main():
    ap = argparse.ArgumentParser(description="Generate Phase-2 keyword_map.csv via Serper expansions.")
    ap.add_argument("--triage", help="Path to phase1_triage.xlsx")
    ap.add_argument("--audit", help="Path to audit cleaned .xlsx")
    ap.add_argument("--seeds", help="Optional seeds .txt (one per line)")
    ap.add_argument("--out", required=True, help="Output CSV (e.g., data/inputs/phase2/keyword_map.csv)")
    ap.add_argument("--per-seed", type=int, default=5)
    ap.add_argument("--use-related", action="store_true", help="Prefer related searches first")
    ap.add_argument("--max-queries", type=int, default=500, help="Hard cap on total suggested queries")
    ap.add_argument("--sleep", type=float, default=0.3, help="Delay between API calls (seconds)")
    args = ap.parse_args()

    api_key = os.environ.get("SERPER_API_KEY","").strip()
    if not api_key:
        raise SystemExit("Missing SERPER_API_KEY in env. Set it and re-run.")

    seeds = []
    if args.triage:
        seeds.extend(harvest_seeds_from_xlsx(args.triage))
    if args.audit:
        seeds.extend(harvest_seeds_from_xlsx(args.audit))
    seeds.extend(load_seed_file(args.seeds))

    # de-dupe
    seen, uniq_seeds = set(), []
    for s in seeds:
        if s and s not in seen:
            seen.add(s)
            uniq_seeds.append(s)

    if not uniq_seeds:
        raise SystemExit("No seeds found. Provide --seeds or a valid --triage/--audit.")

    rows, total = [], 0
    for s in uniq_seeds:
        try:
            ex = expand_seed(s, api_key, use_related=args.use_related, per_seed=args.per_seed, sleep=args.sleep)
            for src, q, note in ex:
                rows.append({"seed": s, "query": q, "source": src, "notes": note})
                total += 1
                if total >= args.max_queries:
                    break
        except Exception as e:
            rows.append({"seed": s, "query": "", "source": "error", "notes": str(e)[:200]})
        if total >= args.max_queries:
            break

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df = pd.DataFrame(rows)
    df = df[df["query"].astype(str).str.len() > 0]
    df = df.drop_duplicates(subset=["query"])
    df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Wrote {args.out} with {len(df)} rows from {len(uniq_seeds)} seeds (cap={args.max_queries}).")

if __name__ == "__main__":
    main()
