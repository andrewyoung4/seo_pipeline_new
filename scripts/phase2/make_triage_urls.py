#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Builds data/inputs/phase2/triage_urls.csv by reading sitemaps (and a tiny crawl fallback).

Usage (PowerShell):
python .\scripts\phase2\make_triage_urls.py `
  --site https://silentprincesstt.com `
  --out .\data\inputs\phase2\triage_urls.csv `
  --include "/products/,/collections/" `
  --exclude "/cart/,/account/,/search?" `
  --max 1000
"""
import argparse, csv, os, re, sys, urllib.parse, collections
from typing import List, Set, Iterable
import requests
from xml.etree import ElementTree as ET
from bs4 import BeautifulSoup  # lightweight HTML fallback; ok for crawl

REQ_TIMEOUT = 20

def same_host(a: str, b: str) -> bool:
    pa, pb = urllib.parse.urlparse(a), urllib.parse.urlparse(b)
    return (pa.netloc.lower() == pb.netloc.lower())

def normalize_url(u: str) -> str:
    u = u.strip()
    if not u: return u
    # remove fragments; keep query as-is (Shopify options can matter)
    pu = urllib.parse.urlsplit(u)
    pu = pu._replace(fragment="")
    # collapse multiple slashes in path
    path = re.sub(r"/{2,}", "/", pu.path)
    return urllib.parse.urlunsplit((pu.scheme.lower(), pu.netloc.lower(), path, pu.query, ""))

def fetch_text(url: str) -> str | None:
    try:
        r = requests.get(url, timeout=REQ_TIMEOUT, headers={"User-Agent": "seo-pipeline/triage"})
        if r.status_code == 200:
            return r.text
    except Exception:
        return None
    return None

def discover_sitemaps(site: str) -> List[str]:
    roots = []
    # robots.txt
    robots = site.rstrip("/") + "/robots.txt"
    txt = fetch_text(robots)
    if txt:
        for line in txt.splitlines():
            m = re.match(r"(?i)Sitemap:\s*(\S+)", line.strip())
            if m:
                roots.append(m.group(1).strip())
    # common fallbacks
    for cand in ("/sitemap.xml", "/sitemap_index.xml"):
        roots.append(site.rstrip("/") + cand)
    # dedupe while preserving order
    seen = set(); out = []
    for s in roots:
        if s not in seen:
            out.append(s); seen.add(s)
    return out

def parse_sitemap_urls(url: str) -> tuple[list[str], list[str]]:
    """Returns (sub_sitemaps, page_urls)"""
    txt = fetch_text(url)
    if not txt: return [], []
    try:
        root = ET.fromstring(txt)
    except ET.ParseError:
        return [], []
    tag = lambda x: x.rsplit("}",1)[-1] if "}" in x else x
    submaps, pages = [], []
    for el in root.iter():
        t = tag(el.tag)
        if t == "sitemap":
            loc = el.findtext(".//{*}loc")
            if loc: submaps.append(loc.strip())
        elif t == "url":
            loc = el.findtext(".//{*}loc")
            if loc: pages.append(loc.strip())
    return submaps, pages

def walk_sitemaps(roots: List[str], limit: int|None=None) -> List[str]:
    urls: list[str] = []
    q = collections.deque(roots)
    seen_maps: Set[str] = set()
    while q:
        sm = q.popleft()
        if sm in seen_maps: continue
        seen_maps.add(sm)
        subs, pages = parse_sitemap_urls(sm)
        for s in subs:
            if s not in seen_maps:
                q.append(s)
        for p in pages:
            urls.append(p)
            if limit and len(urls) >= limit:
                return urls
    return urls

def apply_filters(urls: Iterable[str], site: str, include: list[str], exclude: list[str]) -> List[str]:
    out = []
    for u in urls:
        n = normalize_url(u)
        if not n or not same_host(n, site): continue
        if include:
            if not any(s.lower() in n.lower() for s in include):
                continue
        if exclude:
            if any(s.lower() in n.lower() for s in exclude):
                continue
        out.append(n)
    # stable dedupe
    seen = set(); deduped = []
    for u in out:
        if u not in seen:
            deduped.append(u); seen.add(u)
    return deduped

def tiny_crawl_seed(site: str, start: str, max_urls: int) -> List[str]:
    """Very light same-host crawl, for when no sitemap is available."""
    start = normalize_url(start)
    q = collections.deque([start])
    seen = set([start])
    out = [start]
    while q and len(out) < max_urls:
        u = q.popleft()
        html = fetch_text(u)
        if not html: continue
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = urllib.parse.urljoin(u, a["href"])
            href = normalize_url(href)
            if not href or href in seen: continue
            if not same_host(href, site): continue
            seen.add(href)
            out.append(href)
            q.append(href)
            if len(out) >= max_urls:
                break
    return out

def main():
    ap = argparse.ArgumentParser(description="Generate triage_urls.csv from sitemaps (with small crawl fallback).")
    ap.add_argument("--site", required=True, help="Origin like https://example.com")
    ap.add_argument("--out", required=True, help="Path to CSV to write (url column).")
    ap.add_argument("--include", default="", help="Comma list of substrings to include (optional).")
    ap.add_argument("--exclude", default="", help="Comma list of substrings to exclude (optional).")
    ap.add_argument("--max", type=int, default=1000, help="Max URLs to keep (after filtering).")
    args = ap.parse_args()

    site = args.site.rstrip("/") + "/"
    include = [s.strip() for s in args.include.split(",") if s.strip()]
    exclude = [s.strip() for s in args.exclude.split(",") if s.strip()]

    roots = discover_sitemaps(site)
    urls: list[str] = []
    if roots:
        urls = walk_sitemaps(roots, limit=None)  # gather all, we’ll cap later
    if not urls:
        # fallback crawl seed
        urls = tiny_crawl_seed(site, site, max_urls=args.max * 2)

    urls = apply_filters(urls, site, include, exclude)
    if args.max and len(urls) > args.max:
        urls = urls[: args.max]

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["url"])
        for u in urls:
            w.writerow([u])
    print(f"Wrote {len(urls)} URLs → {args.out}")

if __name__ == "__main__":
    # Soft requirements note for BeautifulSoup
    try:
        import bs4  # noqa: F401
    except Exception:
        print("[hint] pip install beautifulsoup4", file=sys.stderr)
    main()
