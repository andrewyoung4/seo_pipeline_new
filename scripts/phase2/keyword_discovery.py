
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Keyword Discovery (Niche-Focused) — v1b (Resilient)
- Adds: --no-serper, --serper-limit, --serper-timeout, --serper-qps, retries with backoff
- Compatible with your original CLI; just extra switches.

Usage (Windows PowerShell):
  python .\scripts\phase2\keyword_discovery_v1b.py `
    --keyword-map .\data\inputs\phase2\keyword_map.csv `
    --gsc-csv .\data\inputs\phase2\gsc_search_analytics.csv `
    --competitors-serp-hits .\data\outputs\phase3\competitors_serp_hits.csv `
    --origin https://silentprincesstt.com `
    --out .\data\inputs\phase2\keyword_map_expanded.csv `
    --no-serper

Drop-in path suggestion: scripts/phase2/keyword_discovery_v1b.py
"""

import argparse, csv, os, re, sys, time, json, math, random
from collections import defaultdict
from urllib.parse import urlparse
from typing import Iterable, List, Dict, Tuple, Optional, Set

import requests
import pandas as pd
try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None
try:
    from pytrends.request import TrendReq
except Exception:
    TrendReq = None
try:
    import tldextract
except Exception:
    tldextract = None

USER_AGENT = "Mozilla/5.0 (compatible; SilentPrincessKeywordBot/1.0)"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "text/html,application/json;q=0.9,*/*;q=0.8"}

def clean_kw(s: str) -> str:
    s = re.sub(r"\s+", " ", str(s or "")).strip()
    s = re.sub(r"\b(pattern|tutorial|pdf|kit|printable)s?\b", "", s, flags=re.I).strip()
    return s.lower()

def load_seed_keywords(path: Optional[str]) -> Set[str]:
    seeds = set()
    if not path or not os.path.isfile(path): return seeds
    df = pd.read_csv(path)
    cols = [c for c in df.columns if c.lower() in ("keyword","query","term","search_term")]
    if not cols: cols = [df.columns[0]]
    for col in cols:
        for v in df[col].astype(str).tolist():
            v = clean_kw(v)
            if v: seeds.add(v)
    return seeds

def load_gsc_keywords(path: Optional[str]) -> Tuple[Set[str], Dict[str, float]]:
    kws, imp = set(), {}
    if not path or not os.path.isfile(path): return kws, imp
    df = pd.read_csv(path)
    q_col = next((c for c in df.columns if c.lower() in ("query","queries","search query")), None) or df.columns[0]
    i_col = next((c for c in df.columns if "impression" in c.lower()), None)
    for _, row in df.iterrows():
        q = clean_kw(str(row.get(q_col, "")).strip())
        if not q: continue
        kws.add(q)
        if i_col:
            try: imp[q] = imp.get(q, 0.0) + float(row.get(i_col, 0) or 0)
            except: pass
    return kws, imp

def google_autocomplete(seed: str, max_suggestions: int = 10) -> List[str]:
    url = "https://suggestqueries.google.com/complete/search"
    params = {"client": "firefox", "hl": "en", "q": seed}
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and len(data) >= 2 and isinstance(data[1], list):
            sugg = [clean_kw(s) for s in data[1][:max_suggestions]]
            return [s for s in sugg if s]
    except Exception:
        return []
    return []

def serper_related(seed: str, api_key: Optional[str], timeout: int = 8, retries: int = 3, qps: float = 1.0) -> Tuple[List[str], List[str]]:
    if not api_key:
        return [], []
    url = "https://google.serper.dev/search"
    payload = {"q": seed, "hl": "en", "num": 10}
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    backoff = 0.8
    for attempt in range(retries):
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=timeout)
            if r.status_code == 429:
                # rate limited → backoff
                time.sleep(max(1.0, qps) * (attempt + 1) * 2)
                continue
            r.raise_for_status()
            data = r.json()
            related = [clean_kw(x.get("query","")) for x in (data.get("relatedSearches") or []) if x.get("query")]
            paa = [clean_kw(x.get("question","")) for x in (data.get("peopleAlsoAsk") or []) if x.get("question")]
            time.sleep(1.0/qps if qps>0 else 0)
            return [k for k in related if k], [k for k in paa if k]
        except KeyboardInterrupt:
            raise
        except Exception:
            time.sleep((attempt+1) * backoff)
    return [], []

def domain_of(u: str) -> str:
    try:
        if tldextract:
            ext = tldextract.extract(u)
            return ".".join([p for p in [ext.domain, ext.suffix] if p])
        host = urlparse(u).netloc.lower()
        if host.startswith("www."): host = host[4:]
        return host
    except Exception:
        return ""

def unique_keep_order(items: Iterable[str]) -> List[str]:
    seen, out = set(), []
    for x in items:
        if x and x not in seen:
            seen.add(x); out.append(x)
    return out

def load_competitor_urls(txt_path: Optional[str], serp_hits_csv: Optional[str], origin: Optional[str]) -> List[str]:
    urls = []
    if txt_path and os.path.isfile(txt_path):
        with open(txt_path, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u: urls.append(u)
    if serp_hits_csv and os.path.isfile(serp_hits_csv):
        try:
            df = pd.read_csv(serp_hits_csv)
            url_col = next((c for c in df.columns if c.lower() in ("url","link","result_url","resultlink")), None) or df.columns[0]
            urls += [str(u) for u in df[url_col].dropna().astype(str).tolist()]
        except Exception:
            pass
    urls = unique_keep_order(urls)
    if origin:
        self_dom = domain_of(origin)
        urls = [u for u in urls if domain_of(u) != self_dom]
    urls = [u for u in urls if re.search(r"/product|/shop|/listing|/collections|/item|/products/", u, re.I)]
    return urls[:150]

def derive_keywords_from_text(text: str) -> List[str]:
    if not text: return []
    t = re.sub(r"[\|\-–—_/·•:]+", " ", text)
    t = re.sub(r"[^a-zA-Z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip().lower()
    words = t.split()
    stop = set("""a an the and or of for with to from in on by at as is are be your my our their
                  & plus new sale buy shop official 2025 2024 cheap best top free pattern tutorial pdf kit printable"""
                .split())
    out = []
    for n in (2,3,4):
        for i in range(len(words)-n+1):
            chunk = words[i:i+n]
            if any(w in stop for w in chunk): continue
            phrase = " ".join(chunk).strip()
            if len(phrase) < 4: continue
            out.append(phrase)
    return out[:40]

def try_trend_score(kw_list: List[str]) -> Dict[str, float]:
    scores = {k: 0.0 for k in kw_list}
    if TrendReq is None: return scores
    try:
        pytrends = TrendReq(hl="en-US", tz=0)
        CHUNK = 5
        for i in range(0, len(kw_list), CHUNK):
            chunk = kw_list[i:i+CHUNK]
            if not chunk: continue
            pytrends.build_payload(chunk, timeframe="today 12-m")
            df = pytrends.interest_over_time()
            if df is not None and not df.empty:
                recent = df.tail(4).mean()
                for k in chunk:
                    try: scores[k] = max(scores.get(k, 0.0), float(recent.get(k, 0.0)))
                    except Exception: pass
            time.sleep(1.0)
    except Exception:
        pass
    return scores

def main():
    ap = argparse.ArgumentParser(description="Niche Keyword Discovery (resilient)")
    ap.add_argument("--keyword-map", type=str, default="", help="Existing seed keyword_map.csv")
    ap.add_argument("--gsc-csv", type=str, default="", help="GSC Search Analytics export CSV")
    ap.add_argument("--competitor-urls", type=str, default="", help="TXT file with competitor URLs (one per line)")
    ap.add_argument("--competitors-serp-hits", type=str, default="", help="Phase-3 competitors_serp_hits.csv")
    ap.add_argument("--origin", type=str, default="", help="Your site origin (e.g., https://silentprincesstt.com)")
    ap.add_argument("--max-per-seed", type=int, default=10, help="Autocomplete suggestions per seed")
    ap.add_argument("--crawl-competitors", action="store_true", help="Fetch competitor titles/H1s (optional)")
    ap.add_argument("--out", type=str, required=True, help="Path to write keyword_map_expanded.csv")
    # New resilience flags
    ap.add_argument("--no-serper", action="store_true", help="Disable Serper related/PAA even if env key is set")
    ap.add_argument("--serper-limit", type=int, default=120, help="Max seeds to hit Serper with")
    ap.add_argument("--serper-timeout", type=int, default=8, help="Per-request timeout (seconds)")
    ap.add_argument("--serper-qps", type=float, default=0.8, help="Queries per second (<=1.0 recommended)")

    args = ap.parse_args()
    serper_key = None if args.no_serper else (os.environ.get("SERPER_API_KEY", "").strip() or None)

    # Seeds
    seeds = set()
    seeds |= load_seed_keywords(args.keyword_map)
    gsc_kw, gsc_imp = load_gsc_keywords(args.gsc_csv)
    seeds |= gsc_kw
    seeds = [k for k in seeds if k]
    if not seeds:
        print("WARN: No seeds found. Provide --keyword-map and/or --gsc-csv.", file=sys.stderr)

    # Expansion
    candidates = defaultdict(lambda: {"sources": set(), "autocomplete_hits": 0, "competitor_hits": 0, "paa_hits": 0, "related_hits": 0})

    # Autocomplete
    for seed in seeds[:500]:
        sugg = google_autocomplete(seed, max_suggestions=args.max_per_seed)
        for s in sugg:
            c = candidates[s]
            c["sources"].add("autocomplete")
            c["autocomplete_hits"] += 1
        time.sleep(0.2 + random.random()*0.3)

    # Serper related & PAA (optional + limited + retries)
    if serper_key:
        for seed in seeds[:max(0, args.serper_limit)]:
            related, paa = serper_related(seed, serper_key, timeout=args.serper_timeout, qps=max(0.2, args.serper_qps))
            for s in related:
                c = candidates[s]; c["sources"].add("related"); c["related_hits"] += 1
            for q in paa:
                c = candidates[q]; c["sources"].add("paa"); c["paa_hits"] += 1

    # Competitor titles/H1s (optional)
    comp_urls = load_competitor_urls(args.competitor_urls, args.competitors_serp_hits, args.origin)
    if args.crawl_competitors and comp_urls and BeautifulSoup is not None:
        for u in comp_urls:
            try:
                r = requests.get(u, headers=HEADERS, timeout=12)
                r.raise_for_status()
                soup = BeautifulSoup(r.text, "html.parser")
                title = soup.title.get_text(strip=True) if (soup.title and soup.title.string) else None
                h1 = soup.find("h1")
                h1 = h1.get_text(strip=True) if h1 else None
            except Exception:
                title = h1 = None
            for text in [title, h1]:
                if not text: continue
                for phrase in derive_keywords_from_text(text or ""):
                    c = candidates[phrase]; c["sources"].add("competitor"); c["competitor_hits"] += 1
            time.sleep(0.8 + random.random()*0.6)

    # Scoring
    all_kw = list(candidates.keys())
    trend_scores = try_trend_score(all_kw) if all_kw else {}
    rows = []
    for kw, meta in candidates.items():
        gi = float(gsc_imp.get(kw, 0.0))
        comp = int(meta["competitor_hits"]); ac=int(meta["autocomplete_hits"]); paa=int(meta["paa_hits"]); rel=int(meta["related_hits"])
        trend = float(trend_scores.get(kw, 0.0))
        score = (gi * 0.5) + (comp * 2.0) + (ac * 1.0) + (paa * 1.0) + (rel * 1.0) + (trend * 0.5)
        rows.append({
            "keyword": kw,
            "score": round(score, 3),
            "gsc_impressions": gi,
            "competitors": comp,
            "autocomplete_hits": ac,
            "paa_hits": paa,
            "related_hits": rel,
            "trend_score": round(trend, 2),
            "sources": ",".join(sorted(meta["sources"])) or ""
        })
    out_df = pd.DataFrame(rows).sort_values(["score","gsc_impressions","competitors"], ascending=[False, False, False])
    out_df = out_df.drop_duplicates(subset=["keyword"], keep="first")

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    out_df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Wrote {args.out} with {len(out_df)} keywords")
    if not serper_key:
        print("NOTE: Serper disabled (no key or --no-serper). 'related' and 'paa' columns may be 0.", file=sys.stderr)
    if TrendReq is None:
        print("NOTE: pytrends not installed; trend_score=0.", file=sys.stderr)

if __name__ == "__main__":
    main()
