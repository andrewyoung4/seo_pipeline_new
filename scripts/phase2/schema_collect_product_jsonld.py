#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
schema_collect_product_jsonld.py
Collect Product JSON-LD, score completeness & rich-results eligibility, and write a CSV.
Now with:
- tolerant host match (handles www vs non-www)
- optional --include-pattern (regex) to pick what "looks like product"
- Shopify fallbacks: try /sitemap_products_*.xml if main filter yields 0

Usage (PowerShell):
  python .\scripts\phase2\schema_collect_product_jsonld.py `
    --origin "https://silentprincesstt.com" `
    --out .\data\outputs\phase2\schema_product_report.csv
"""
import argparse, csv, json, re, sys, time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup

DEFAULT_RULES = {
  "required": ["name","image","offers.price","offers.priceCurrency"],
  "recommended": ["offers.availability","sku","brand.name|brand","gtin|gtin8|gtin12|gtin13|gtin14","aggregateRating.ratingValue","aggregateRating.reviewCount","review"],
  "eligibility_logic": "has('name') and has('image') and has('offers.price') and has('offers.priceCurrency')"
}

@dataclass
class ProductDoc:
    url: str
    raw: Dict[str, Any]

def _norm_host(h: str) -> str:
    h = h.lower()
    return h[4:] if h.startswith("www.") else h

def _load_rules(path: Optional[str]) -> Dict[str, Any]:
    if path:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[warn] Failed to load rules from {path}: {e}. Using defaults.", file=sys.stderr)
    return DEFAULT_RULES

def _read_urls_from_csv(path: str, maxn: Optional[int]) -> List[str]:
    urls = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            u = (row.get("url") or "").strip()
            if u:
                urls.append(u)
                if maxn and len(urls) >= maxn:
                    break
    return urls

def _fetch(url: str, timeout: int, ua: str) -> Optional[requests.Response]:
    try:
        return requests.get(url, timeout=timeout, headers={"User-Agent": ua})
    except Exception as e:
        print(f"[warn] fetch error {url}: {e}", file=sys.stderr)
        return None

def _discover_sitemap_urls(origin: str, timeout: int, ua: str) -> List[str]:
    candidates = [urljoin(origin.rstrip('/')+'/', "sitemap.xml"), origin]
    seen = set()
    urls = []

    for cand in candidates:
        if cand in seen:
            continue
        seen.add(cand)
        resp = _fetch(cand, timeout, ua)
        if not resp or resp.status_code != 200:
            continue
        ct = resp.headers.get("Content-Type","")
        text = resp.text or ""
        if "xml" in ct or text.strip().startswith("<?xml"):
            try:
                root = ET.fromstring(text)
                ns = {"sm": root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
                for loc in root.iterfind(".//sm:loc", ns) if ns else root.iter("loc"):
                    u = (loc.text or "").strip()
                    if u:
                        urls.append(u)
            except Exception as e:
                print(f"[warn] sitemap parse {cand}: {e}", file=sys.stderr)
        else:
            try:
                soup = BeautifulSoup(text, "html.parser")
                for ln in soup.find_all("link", rel=lambda v: v and "sitemap" in v):
                    href = ln.get("href")
                    if href:
                        urls.append(urljoin(origin, href))
            except Exception as e:
                print(f"[warn] html parse for sitemap {cand}: {e}", file=sys.stderr)

    # fetch nested sitemaps if index
    final_urls = []
    for u in urls:
        if not u.lower().endswith(".xml"):
            continue
        resp = _fetch(u, timeout, ua)
        if not resp or resp.status_code != 200:
            continue
        try:
            root = ET.fromstring(resp.text)
            ns = {"sm": root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
            locs = [el.text.strip() for el in (root.iterfind(".//sm:loc", ns) if ns else root.iter("loc")) if (el.text or "").strip()]
            if locs:
                final_urls.extend(locs)
        except Exception as e:
            print(f"[warn] nested sitemap parse {u}: {e}", file=sys.stderr)

    # keep same registrable host (tolerate www diff)
    want = _norm_host(urlparse(origin).netloc)
    final_urls = [u for u in final_urls if _norm_host(urlparse(u).netloc) == want]
    return list(dict.fromkeys(final_urls))

def _looks_like_product(u: str, include_pattern: Optional[str]) -> bool:
    u = u.lower()
    if include_pattern:
        try:
            return re.search(include_pattern, u) is not None
        except re.error:
            pass
    # defaults: Shopify common patterns
    return ("/products/" in u) or ("/product/" in u) or (re.search(r"/p/\d+", u) is not None)

def _extract_product_ld_json(html: str) -> List[Dict[str, Any]]:
    out = []
    soup = BeautifulSoup(html, "html.parser")
    for s in soup.find_all("script", {"type": "application/ld+json"}):
        txt = s.string or s.text
        if not txt:
            continue
        try:
            data = json.loads(txt.strip())
        except Exception:
            try:
                data = json.loads(re.sub(r",\s*}", "}", re.sub(r",\s*]", "]", txt.strip())))
            except Exception:
                continue
        def flatten(obj):
            if isinstance(obj, list):
                for it in obj:
                    yield from flatten(it)
            else:
                yield obj
        for node in flatten(data):
            if isinstance(node, dict):
                types = node.get("@type") or node.get("type")
                if isinstance(types, list):
                    types_lower = [str(t).lower() for t in types]
                else:
                    types_lower = [str(types).lower()] if types else []
                if any(t in ("product",) for t in types_lower):
                    out.append(node)
    return out

def _dig(d, path):
    for segment in path.split("|"):
        cur = d
        ok = True
        for key in segment.split("."):
            if isinstance(cur, list):
                cur = cur[0] if cur else None
            if not isinstance(cur, dict) or key not in cur:
                ok = False
                break
            cur = cur[key]
        if ok and cur not in (None, "", []):
            return cur
    return None

def _has(d, path): return _dig(d, path) is not None

def _eval_eligibility(prod, rules):
    expr = rules.get("eligibility_logic") or ""
    def has(path: str) -> bool: return _has(prod, path)
    try:
        return bool(eval(expr, {"__builtins__": {}}, {"has": has}))
    except Exception:
        return _has(prod, "name") and _has(prod, "image") and _has(prod, "offers.price") and _has(prod, "offers.priceCurrency")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--origin", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--urls")
    ap.add_argument("--rules")
    ap.add_argument("--timeout", type=int, default=12)
    ap.add_argument("--max", type=int, default=600)
    ap.add_argument("--user-agent", default="SchemaCollector/1.1 (+https://)")
    ap.add_argument("--include-pattern", help=r"Regex to select product-like URLs (overrides defaults)")
    args = ap.parse_args()

    # rules
    rules = DEFAULT_RULES
    if args.rules:
        try:
            with open(args.rules, "r", encoding="utf-8") as f:
                rules = json.load(f)
        except Exception as e:
            print(f"[warn] Could not load rules: {e}; using defaults.", file=sys.stderr)

    # URLs
    if args.urls:
        urls = _read_urls_from_csv(args.urls, args.max)
    else:
        site_urls = _discover_sitemap_urls(args.origin, args.timeout, args.user_agent)
        urls = [u for u in site_urls if _looks_like_product(u, args.include_pattern)]
        if not urls:
            # Shopify fallback: if we saw sitemaps, prefer product sitemaps explicitly
            shopify_products = [u for u in site_urls if "/sitemap_products_" in u.lower()]
            keep = []
            for sm in shopify_products:
                resp = _fetch(sm, args.timeout, args.user_agent)
                if not resp or resp.status_code != 200: 
                    continue
                try:
                    root = ET.fromstring(resp.text)
                    ns = {"sm": root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
                    locs = [el.text.strip() for el in (root.iterfind(".//sm:loc", ns) if ns else root.iter("loc")) if (el.text or "").strip()]
                    keep.extend(locs)
                except Exception as e:
                    print(f"[warn] product-sitemap parse {sm}: {e}", file=sys.stderr)
            urls = [u for u in keep if _looks_like_product(u, args.include_pattern)]
        if args.max:
            urls = urls[: args.max]

    print(f"[info] Product-like URLs: {len(urls)}", file=sys.stderr)

    # Crawl
    rows = []
    for i, url in enumerate(urls, 1):
        resp = _fetch(url, args.timeout, args.user_agent)
        if not resp or resp.status_code != 200:
            print(f"[warn] skip {url} status={getattr(resp,'status_code',None)}", file=sys.stderr)
            continue
        prods = _extract_product_ld_json(resp.text)
        if not prods:
            rows.append({
                "url": url,
                "has_product_jsonld": 0,
                "eligibile_rich_results": 0,
                "completeness_pct": 0,
                "missing_required": ";".join(rules["required"]),
                "missing_recommended": ";".join(rules["recommended"]),
                "name": "",
                "sku": "",
                "brand": "",
                "price": "",
                "priceCurrency": "",
                "availability": "",
            })
            continue

        prod = prods[0]
        missing_req = [p for p in rules["required"] if not _has(prod, p)]
        missing_rec = [p for p in rules["recommended"] if not _has(prod, p)]
        total = len(rules["required"]) + len(rules["recommended"])
        have = (len(rules["required"]) - len(missing_req)) + (len(rules["recommended"]) - len(missing_rec))
        completeness = int(round(100.0 * have / total)) if total else 0
        elig = _eval_eligibility(prod, rules)

        def pick(path):
            v = _dig(prod, path)
            if isinstance(v, (dict, list)):
                return json.dumps(v, ensure_ascii=False)
            return v if v is not None else ""

        rows.append({
            "url": url,
            "has_product_jsonld": 1,
            "eligibile_rich_results": 1 if elig else 0,
            "completeness_pct": completeness,
            "missing_required": ";".join(missing_req),
            "missing_recommended": ";".join(missing_rec),
            "name": pick("name"),
            "sku": pick("sku"),
            "brand": pick("brand.name|brand"),
            "price": pick("offers.price"),
            "priceCurrency": pick("offers.priceCurrency"),
            "availability": pick("offers.availability"),
        })
        if i % 10 == 0:
            print(f"[info] processed {i}/{len(urls)}", file=sys.stderr)
        time.sleep(0.25)

    # Write CSV
    out_path = args.out
    fieldnames = ["url","has_product_jsonld","eligibile_rich_results","completeness_pct",
                  "missing_required","missing_recommended","name","sku","brand","price","priceCurrency","availability"]
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[done] wrote {out_path} ({len(rows)} rows)")

if __name__ == "__main__":
    main()
