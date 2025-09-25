#!/usr/bin/env python3
"""
Shopify SF-like Auditor (Patched Full Script)
- Crawls a Shopify site and exports a Screaming-Frog-style workbook
- Adds Quality sheet aligned to your n8n LIMITS
- Collects per-image alt/src/filename and flags alt length + filename hygiene
- Exports TWO files if you call export_workbook twice with different 'outfile'

Usage (PowerShell, single line):
python .\shopify_sf_like_audit.py --store "https://silentprincesstt.com" --out "C:\\path\\to\\shopify_audit_out" --noise-policy label --psi-key "REDACTED" --psi-max 40 --psi-delay 0.25

Notes:
- PSI is optional; omit --psi-key to skip.
- To include URL parameters in crawl graph, pass --include-params
- Exports:
   * raw:    use outfile="shopify_sf_audit.raw.xlsx"
   * cleaned:use outfile="shopify_sf_audit.cleaned.xlsx" (noise excluded)
"""
from __future__ import annotations

import argparse
import collections
import dataclasses
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Set
import hashlib
import json
import os
import queue
import re
import sys
import time
from urllib.parse import urlparse, urljoin, urlunparse, urlencode, parse_qsl

import requests
from bs4 import BeautifulSoup
import pandas as pd

# =========================
# LIMITS (keep in sync with n8n)
# =========================
LIMITS = {
    "TITLE_MIN": 50, "TITLE_MAX": 60,
    "META_MIN": 140, "META_MAX": 155,
    "H1_MIN": 50, "H1_MAX": 70,
    "SLUG_MIN": 40, "SLUG_MAX": 60,
    "ALT_MIN": 100, "ALT_MAX": 125,   # guidance for images
}

# =========================
# Models
# =========================
@dataclass
class Edge:
    source: str
    target: str
    anchor: Optional[str] = None
    rel: Optional[str] = None

@dataclass
class ImageInfo:
    src: str
    alt: str
    filename: str

@dataclass
class PageRecord:
    url: str
    final_url: Optional[str] = None
    status: Optional[int] = None
    content_type: Optional[str] = None
    content_length: Optional[int] = None
    title: Optional[str] = None
    meta_description: Optional[str] = None
    canonical: Optional[str] = None
    meta_robots: Optional[str] = None
    x_robots_tag: Optional[str] = None
    h1: Optional[str] = None
    h2: Optional[str] = None
    word_count: Optional[int] = None
    image_count: Optional[int] = None
    images_missing_alt: Optional[int] = None
    inlinks: int = 0
    outlinks: int = 0
    hreflang_count: int = 0
    jsonld_types: Optional[str] = None
    https: Optional[bool] = None
    mixed_content: int = 0
    hsts: Optional[bool] = None
    cache_control: Optional[str] = None
    vary: Optional[str] = None
    charset: Optional[str] = None
    x_content_type_options: Optional[str] = None
    param_risk: Optional[str] = None
    psi_mobile_score: Optional[int] = None
    psi_desktop_score: Optional[int] = None
    cwv_lcp_ms: Optional[int] = None
    cwv_inp_ms: Optional[int] = None
    cwv_cls: Optional[float] = None
    body_hash: Optional[str] = None
    images: List[ImageInfo] = field(default_factory=list)

# =========================
# Helpers
# =========================
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

NOFOLLOW_RELS = {"nofollow", "ugc", "sponsored"}

PARAM_NOISE_PREFIXES = ("utm_", "fbclid", "gclid", "mc_", "mkevt", "mkcid", "mkrid")


def normalize_url(href: str, base: str) -> str:
    """Resolve and normalize a URL against a base URL; drop fragment."""
    if not href:
        return base
    absu = urljoin(base, href)
    p = urlparse(absu)
    # strip fragment
    p = p._replace(fragment="")
    return urlunparse(p)


def strip_params(u: str) -> str:
    p = urlparse(u)
    return urlunparse(p._replace(query=""))


def host_of(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""


def path_slug(u: str) -> str:
    parts = [x for x in urlparse(u).path.split("/") if x]
    return parts[-1] if parts else ""


def param_risk_str(u: str) -> str:
    q = dict(parse_qsl(urlparse(u).query))
    if not q:
        return "None"
    noisy = [k for k in q if k.lower().startswith(PARAM_NOISE_PREFIXES)]
    return "Noisy params" if noisy else f"Params: {len(q)}"


def should_skip(u: str, meta_robots: str = "", x_robots: str = "") -> Tuple[bool, str]:
    """Label/skip common noise endpoints.
    Returns (skip, reason).
    """
    if not u:
        return False, ""
    p = urlparse(u)
    path = p.path or "/"
    low = path.lower()
    # Shopify/system areas
    if low.startswith("/cart") or low.startswith("/checkout"):
        return True, "Cart/Checkout"
    if low.startswith("/account") or low.startswith("/orders"):
        return True, "Account"
    if low.startswith("/apps/") or low.startswith("/admin") or low.startswith("/tools/"):
        return True, "Admin/Apps"
    if low.startswith("/customer_authentication/"):
        return True, "Auth redirect"
    if low.startswith("/policies/"):
        return True, "Policies"
    # blog index pages often noisy, keep posts
    parts = [x for x in low.split("/") if x]
    if len(parts) == 2 and parts[0] == "blogs":
        return True, "Blog index"
    # robots meta
    mr = (meta_robots or "").lower()
    xr = (x_robots or "").lower()
    if "noindex" in mr or "noindex" in xr:
        return True, "Noindex"
    return False, ""


def hash_text(txt: str) -> str:
    return hashlib.sha1((txt or "").encode("utf-8")).hexdigest()

# =========================
# Crawler / Auditor
# =========================
class Auditor:
    def __init__(self, start_url: str, out_dir: str, timeout: int = 20, sleep: float = 0.0,
                 ua: Optional[str] = None, max_pages: int = 2000, max_depth: int = 6, include_params: bool = False):
        self.start_url = start_url.rstrip("/")
        self.out_dir = out_dir
        self.timeout = timeout
        self.sleep = sleep
        self.ua = ua or DEFAULT_HEADERS["User-Agent"]
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.include_params = include_params

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.ua})

        self.host = host_of(self.start_url)
        self.pages: Dict[str, PageRecord] = {}
        self.edges: List[Edge] = []
        self.hreflang_map: Dict[str, List[Tuple[str, str]]] = collections.defaultdict(list)
        self.exact_text: Dict[str, List[str]] = collections.defaultdict(list)

    def crawl(self):
        seen: Set[str] = set()
        q = queue.Queue()
        # seed
        q.put((self.start_url, 0))
        seen.add(strip_params(self.start_url) if not self.include_params else self.start_url)

        while not q.empty() and len(self.pages) < self.max_pages:
            u, depth = q.get()
            try:
                rec, links = self.fetch_page(u)
            except Exception as e:
                # record minimal error page
                rec = PageRecord(url=u, final_url=u, status=None)
                links = []
            self.pages[rec.final_url or rec.url] = rec

            # queue links
            if depth < self.max_depth:
                for href, anchor, rel in links:
                    absu = normalize_url(href, rec.final_url or rec.url)
                    if host_of(absu) != self.host:
                        continue
                    key = absu if self.include_params else strip_params(absu)
                    if key not in seen and not should_skip(absu)[0]:
                        seen.add(key)
                        q.put((absu, depth + 1))
                    self.edges.append(Edge(source=rec.final_url or rec.url, target=absu, anchor=anchor, rel=rel))

            if self.sleep:
                time.sleep(self.sleep)

        # compute in/out link counts
        out_map = collections.Counter([e.source for e in self.edges])
        in_map = collections.Counter([e.target for e in self.edges])
        for u, r in self.pages.items():
            r.outlinks = out_map.get(u, 0)
            r.inlinks = in_map.get(u, 0)

    def fetch_page(self, u: str) -> Tuple[PageRecord, List[Tuple[str, Optional[str], Optional[str]]]]:
        r = self.session.get(u, allow_redirects=True, timeout=self.timeout)
        final_u = r.url
        rec = PageRecord(url=u, final_url=final_u, status=r.status_code)
        rec.content_type = r.headers.get("Content-Type")
        rec.content_length = int(r.headers.get("Content-Length") or 0)
        rec.cache_control = r.headers.get("Cache-Control")
        rec.vary = r.headers.get("Vary")
        rec.charset = r.apparent_encoding
        rec.x_content_type_options = r.headers.get("X-Content-Type-Options")
        rec.x_robots_tag = r.headers.get("X-Robots-Tag")
        rec.https = final_u.startswith("https://")
        rec.hsts = bool(r.headers.get("Strict-Transport-Security"))
        rec.param_risk = param_risk_str(final_u)

        links: List[Tuple[str, Optional[str], Optional[str]]] = []

        ctype = (rec.content_type or "").lower()
        if "text/html" not in ctype:
            return rec, links

        soup = BeautifulSoup(r.text, "lxml")

        # title
        title_tag = soup.find("title")
        rec.title = (title_tag.get_text(" ", strip=True) if title_tag else "")

        # meta description & robots
        mdesc = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
        rec.meta_description = (mdesc.get("content", "").strip() if mdesc else "")

        mrobots = soup.find("meta", attrs={"name": re.compile(r"^robots$", re.I)})
        rec.meta_robots = (mrobots.get("content", "").strip() if mrobots else "")

        # canonical
        lcanon = soup.find("link", attrs={"rel": re.compile(r"^canonical$", re.I)})
        rec.canonical = normalize_url(lcanon.get("href"), final_u) if lcanon and lcanon.get("href") else ""

        # headings
        h1 = soup.find("h1")
        rec.h1 = (h1.get_text(" ", strip=True) if h1 else "")
        h2 = soup.find("h2")
        rec.h2 = (h2.get_text(" ", strip=True) if h2 else "")

        # hreflang
        for link in soup.find_all("link", attrs={"rel": re.compile(r"alternate", re.I)}):
            lang = (link.get("hreflang") or link.get("lang") or "").strip()
            href = link.get("href")
            if lang and href:
                href_abs = normalize_url(href, final_u)
                self.hreflang_map[final_u].append((lang, href_abs))
        rec.hreflang_count = len(self.hreflang_map.get(final_u, []))

        # JSON-LD types
        types = []
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(s.string or "{}")
            except Exception:
                continue
            def collect_types(obj):
                if isinstance(obj, dict):
                    t = obj.get("@type")
                    if isinstance(t, str): types.append(t)
                    elif isinstance(t, list):
                        for x in t:
                            if isinstance(x, str): types.append(x)
                    for v in obj.values():
                        collect_types(v)
                elif isinstance(obj, list):
                    for it in obj:
                        collect_types(it)
            collect_types(data)
        rec.jsonld_types = ",".join(sorted(set(types))) if types else ""

        # body text for duplicate detection
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        body_text = soup.get_text(" ", strip=True)
        rec.word_count = len(body_text.split()) if body_text else 0
        rec.body_hash = hash_text(body_text)
        self.exact_text[rec.body_hash].append(final_u)

        # images (collect alt/src/filename)
        images: List[ImageInfo] = []
        missing_alt = 0
        mixed_count = 0
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or img.get("data-image")
            if not src:
                continue
            src_abs = normalize_url(src, final_u)
            if src_abs.startswith("http://") and final_u.startswith("https://"):
                mixed_count += 1
            alt = (img.get("alt") or "").strip()
            if not alt:
                missing_alt += 1
            # filename
            fn = path_slug(src_abs)
            images.append(ImageInfo(src=src_abs, alt=alt, filename=fn))
        rec.image_count = len(images)
        rec.images_missing_alt = missing_alt
        rec.mixed_content = mixed_count
        rec.images = images

        # links
        for a in soup.find_all("a"):
            href = a.get("href")
            if not href:
                continue
            rel = (a.get("rel") or [])
            rel_join = ",".join(rel) if isinstance(rel, list) else str(rel)
            anchor = a.get_text(" ", strip=True)[:200]
            links.append((href, anchor, rel_join))

        return rec, links

# =========================
# PSI (optional)
# =========================
def collect_psi(pages: Dict[str, PageRecord], strategies: List[str], api_key: str, max_urls: int = 50, delay: float = 0.25):
    """Populate PSI scores for top pages by inlinks. Uses Google's PSI API v5."""
    base = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    # pick top pages by inlinks
    top = sorted(pages.values(), key=lambda r: (r.inlinks or 0), reverse=True)[:max_urls]
    s = requests.Session()
    for rec in top:
        for strat in strategies:
            try:
                resp = s.get(base, params={"url": rec.final_url or rec.url, "strategy": strat, "key": api_key}, timeout=30)
                if resp.status_code != 200:
                    time.sleep(delay)
                    continue
                data = resp.json()
                lhr = data.get("lighthouseResult", {})
                cat = lhr.get("categories", {}).get("performance", {}).get("score")
                score = int(round((cat or 0) * 100)) if cat is not None else None
                audits = lhr.get("audits", {})
                lcp = audits.get("largest-contentful-paint", {}).get("numericValue")
                inp = audits.get("interactive", {}).get("numericValue")  # proxy for INP isn't in PSI yet; adjust if available
                cls = audits.get("cumulative-layout-shift", {}).get("numericValue")
                if strat == "mobile":
                    rec.psi_mobile_score = score
                else:
                    rec.psi_desktop_score = score
                rec.cwv_lcp_ms = int(lcp) if lcp else rec.cwv_lcp_ms
                rec.cwv_inp_ms = int(inp) if inp else rec.cwv_inp_ms
                rec.cwv_cls = float(cls) if cls is not None else rec.cwv_cls
            except Exception:
                pass
            time.sleep(delay)

# =========================
# Issues builder (adds quality heuristics)
# =========================
def build_issues(pages: Dict[str, PageRecord]) -> Dict[str, List[Dict[str, str]]]:
    TITLE_MIN, TITLE_MAX = LIMITS["TITLE_MIN"], LIMITS["TITLE_MAX"]
    DESC_MIN,  DESC_MAX  = LIMITS["META_MIN"], LIMITS["META_MAX"]

    # Duplicates (case-insensitive)
    title_to_urls: Dict[str, List[str]] = {}
    desc_to_urls: Dict[str, List[str]] = {}
    for u, r in pages.items():
        t = (r.title or "").strip().lower()
        d = (r.meta_description or "").strip().lower()
        if t:
            title_to_urls.setdefault(t, []).append(u)
        if d:
            desc_to_urls.setdefault(d, []).append(u)
    dup_titles = {t for t, urls in title_to_urls.items() if len(urls) > 1}
    dup_descs  = {d for d, urls in desc_to_urls.items()  if len(urls) > 1}

    def sev_rank(s: str) -> int:
        return {"Info":0, "Minor":1, "Moderate":2, "Major":3, "Critical":4}.get(s, 0)

    issues: Dict[str, List[Dict[str, str]]] = {}

    for u, r in pages.items():
        items: List[Dict[str, str]] = []
        title = (r.title or "").strip()
        desc  = (r.meta_description or "").strip()

        # Title length
        tlen = len(title)
        if 0 < tlen < TITLE_MIN:
            items.append({"Reason": "Title too short", "Details": f"{tlen} chars (<{TITLE_MIN})", "Severity": "Minor"})
        elif tlen > TITLE_MAX:
            items.append({"Reason": "Title too long", "Details": f"{tlen} chars (>{TITLE_MAX})", "Severity": "Minor"})

        # Title trailing separator
        if re.search(r"\s*[-–—|:]+\s*$", title):
            items.append({"Reason": "Title ends with separator", "Details": title[-6:], "Severity": "Minor"})

        # Description length
        dlen = len(desc)
        if 0 < dlen < DESC_MIN:
            items.append({"Reason": "Meta description too short", "Details": f"{dlen} chars (<{DESC_MAX})", "Severity": "Minor"})
        elif dlen > DESC_MAX:
            items.append({"Reason": "Meta description too long", "Details": f"{dlen} chars (>{DESC_MAX})", "Severity": "Minor"})

        # Duplicates
        if title.strip().lower() in dup_titles and title:
            items.append({"Reason": "Duplicate title", "Details": "Appears on multiple URLs", "Severity": "Moderate"})
        if desc.strip().lower() in dup_descs and desc:
            items.append({"Reason": "Duplicate meta description", "Details": "Appears on multiple URLs", "Severity": "Minor"})

        if items:
            top = max(items, key=lambda x: sev_rank(x["Severity"]))["Severity"]
            issues[u] = [{"Top Severity": top}] + items

    return issues

# =========================
# Export workbook (with Quality + Image details)
# =========================
def export_workbook(
    out_dir: str,
    pages: Dict[str, PageRecord],
    edges: List[Edge],
    exact_text: Dict[str, List[str]],
    hreflang_map: Dict[str, List[Tuple[str, str]]],
    noise_policy: str,
    outfile: Optional[str] = None,
):
    import pandas as pd
    out_dir = os.path.abspath(out_dir)
    os.makedirs(out_dir, exist_ok=True)
    if not outfile:
        outfile = "shopify_sf_audit.cleaned.xlsx" if noise_policy == "exclude" else "shopify_sf_audit.raw.xlsx"
    xlsx_path = os.path.join(out_dir, outfile)

    def to_df(rows):
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def _get(r, name, default=None):
        try:
            return getattr(r, name)
        except Exception:
            return default

    def _maybe_should_skip(u, meta_robots, xrobots):
        try:
            return should_skip(u, meta_robots, xrobots)
        except Exception:
            return (False, "")

    def _norm_canonical(canonical, url):
        try:
            return normalize_url(canonical, url)
        except Exception:
            return canonical

    internal_rows, resp_rows, directives_rows = [], [], []
    canon_rows, head_rows, title_rows, meta_rows = [], [], [], []
    img_rows, in_rows, out_rows, dup_exact_rows, hreflang_rows, issue_rows = [], [], [], [], [], []

    # Per-page rows
    for u, r in pages.items():
        url = r.final_url or r.url
        mr, xr = (r.meta_robots or ""), (r.x_robots_tag or "")
        skip, reason = _maybe_should_skip(url, mr, xr)
        if skip and noise_policy == "exclude":
            continue

        base_common = {
            "URL": url,
            "Status": r.status,
            "Content-Type": r.content_type,
            "Content-Length": r.content_length,
            "Title": r.title,
            "Meta Description": r.meta_description,
            "Canonical": r.canonical,
            "Meta Robots": r.meta_robots,
            "X-Robots-Tag": r.x_robots_tag,
            "H1": r.h1,
            "H2": r.h2,
            "Word Count": r.word_count,
            "Image Count": r.image_count,
            "Images Missing Alt": r.images_missing_alt,
            "Inlinks": r.inlinks,
            "Outlinks": r.outlinks,
            "Hreflang Count": r.hreflang_count,
            "JSON-LD Types": r.jsonld_types,
            "HTTPS": r.https,
            "Mixed Content Count": r.mixed_content,
            "HSTS": r.hsts,
            "Cache-Control": r.cache_control,
            "Vary": r.vary,
            "Charset": r.charset,
            "X-Content-Type-Options": r.x_content_type_options,
            "Param Risk": r.param_risk,
            "PSI Mobile": r.psi_mobile_score,
            "PSI Desktop": r.psi_desktop_score,
            "LCP (ms)": r.cwv_lcp_ms,
            "INP (ms)": r.cwv_inp_ms,
            "CLS": r.cwv_cls,
        }
        if skip and noise_policy == "label":
            base_common["Noise"] = reason

        internal_rows.append(base_common.copy())

        resp_rows.append({
            "URL": r.url,
            "Final URL": url,
            "Status": r.status,
            "Content-Type": r.content_type,
            "Content-Length": r.content_length,
            **({"Noise": reason} if (skip and noise_policy == "label") else {}),
        })

        directives_rows.append({
            "URL": url,
            "Robots.txt Allowed": "Unknown",
            "Meta Robots": (r.meta_robots or "").lower(),
            "X-Robots-Tag": r.x_robots_tag,
            "Followable": ("nofollow" not in (r.meta_robots or "").lower() and "nofollow" not in (r.x_robots_tag or "").lower()),
            **({"Noise": reason} if (skip and noise_policy == "label") else {}),
        })

        canon_rows.append({
            "URL": url,
            "Canonical": r.canonical,
            "Has Canonical": bool(r.canonical),
            "Self Canonical": (_norm_canonical(r.canonical, url) == url) if r.canonical else False,
            **({"Noise": reason} if (skip and noise_policy == "label") else {}),
        })

        head_rows.append({"URL": url, "H1": r.h1, "H2": r.h2, **({"Noise": reason} if (skip and noise_policy == "label") else {})})
        title_rows.append({"URL": url, "Title": r.title, **({"Noise": reason} if (skip and noise_policy == "label") else {})})
        meta_rows.append({"URL": url, "Meta Description": r.meta_description, **({"Noise": reason} if (skip and noise_policy == "label") else {})})

        img_rows.append({
            "URL": url,
            "Image Count": r.image_count,
            "Images Missing Alt": r.images_missing_alt,
            **({"Noise": reason} if (skip and noise_policy == "label") else {})
        })

    # Links
    def _keep_link(u):
        s, _ = _maybe_should_skip(u, "", "")
        return not (s and noise_policy == "exclude")

    inlinks_map = collections.defaultdict(list)
    outlinks_map = collections.defaultdict(list)
    for e in edges:
        if not (_keep_link(e.source) and _keep_link(e.target)):
            continue
        outlinks_map[e.source].append({"Target": e.target, "Anchor": e.anchor, "Rel": e.rel})
        inlinks_map[e.target].append({"Source": e.source, "Anchor": e.anchor, "Rel": e.rel})
    for t, lst in inlinks_map.items():
        for it in lst:
            in_rows.append({"Target": t, **it})
    for s, lst in outlinks_map.items():
        for it in lst:
            out_rows.append({"Source": s, **it})

    # Duplicates Exact (by body hash)
    for h, urls in (exact_text or {}).items():
        kept = []
        for u in urls:
            s, _ = _maybe_should_skip(u, "", "")
            if s and noise_policy == "exclude":
                continue
            kept.append(u)
        if len(kept) > 1:
            for u in kept:
                dup_exact_rows.append({"Content Hash": h, "URL": u})

    # Hreflang
    for src, lst in (hreflang_map or {}).items():
        sskip, _ = _maybe_should_skip(src, "", "")
        if sskip and noise_policy == "exclude":
            continue
        for lang, href in lst:
            hskip, _ = _maybe_should_skip(href, "", "")
            if hskip and noise_policy == "exclude":
                continue
            hreflang_rows.append({"Source": src, "Lang": lang, "Href": href})

    # Issues
    try:
        issues_map = build_issues(pages)
    except NameError:
        issues_map = {}
    for u, lst in issues_map.items():
        if u not in pages:
            continue
        r = pages[u]
        skip, reason = _maybe_should_skip(u, r.meta_robots or "", r.x_robots_tag or "")
        if skip and noise_policy == "exclude":
            continue
        top = lst[0].get("Top Severity") if lst else ""
        for item in lst[1:]:
            row = {"URL": u, "Top Severity": top, **item}
            if skip and noise_policy == "label":
                row["Noise"] = reason
                row.setdefault("Severity", "Info")
            issue_rows.append(row)

    # QUALITY SHEET (includes image alt + filename hygiene)
    TITLE_MIN, TITLE_MAX = LIMITS["TITLE_MIN"], LIMITS["TITLE_MAX"]
    DESC_MIN,  DESC_MAX  = LIMITS["META_MIN"], LIMITS["META_MAX"]

    title_to_urls, desc_to_urls = {}, {}
    for u, r in pages.items():
        url = r.final_url or r.url
        s, _ = _maybe_should_skip(url, r.meta_robots or "", r.x_robots_tag or "")
        if s and noise_policy == "exclude":
            continue
        t = (r.title or "").strip().lower()
        d = (r.meta_description or "").strip().lower()
        if t: title_to_urls.setdefault(t, []).append(url)
        if d: desc_to_urls.setdefault(d, []).append(url)
    dup_titles = {t for t, urls in title_to_urls.items() if len(urls) > 1}
    dup_descs  = {d for d, urls in desc_to_urls.items()  if len(urls) > 1}

    quality_rows = []
    images_detail_rows = []

    for u, r in pages.items():
        url = r.final_url or r.url
        s, _ = _maybe_should_skip(url, r.meta_robots or "", r.x_robots_tag or "")
        if s and noise_policy == "exclude":
            continue
        title = (r.title or "").strip()
        desc  = (r.meta_description or "").strip()
        tlen, dlen = len(title), len(desc)

        # Per-image checks
        alt_too_short = 0
        alt_too_long  = 0
        file_spaces   = 0
        file_upper    = 0
        file_underscore = 0
        file_bad_ext  = 0

        for im in r.images or []:
            alt = im.alt or ""
            fn  = im.filename or ""
            alen = len(alt)
            if alt and alen < LIMITS["ALT_MIN"]:
                alt_too_short += 1
            if alen > LIMITS["ALT_MAX"]:
                alt_too_long += 1
            if " " in fn:
                file_spaces += 1
            if fn != fn.lower():
                file_upper += 1
            if "_" in fn:
                file_underscore += 1
            if not re.search(r"\.(jpg|jpeg|png|webp|gif|avif)$", fn, re.I):
                file_bad_ext += 1

            images_detail_rows.append({
                "URL": url,
                "IMG_SRC": im.src,
                "ALT": alt,
                "ALT Length": alen,
                "FILENAME": fn,
                "Has Spaces": (" " in fn),
                "Has Uppercase": (fn != fn.lower()),
                "Has Underscore": ("_" in fn),
                "Bad Ext": bool(file_bad_ext),
                "Alt <100": (0 < alen < LIMITS["ALT_MIN"]),
                "Alt >125": (alen > LIMITS["ALT_MAX"]),
            })

        quality_rows.append({
            "URL": url,
            "Title": title,
            "Title Length": tlen,
            "Title Too Short": (0 < tlen < TITLE_MIN),
            "Title Too Long": (tlen > TITLE_MAX),
            "Title Trailing Sep": bool(re.search(r"\s*[-–—|:]+\s*$", title)),
            "Title Duplicate": title.strip().lower() in dup_titles if title else False,

            "Meta Description": desc,
            "Description Length": dlen,
            "Desc Too Short": (0 < dlen < DESC_MIN),
            "Desc Too Long": (dlen > DESC_MAX),
            "Desc Duplicate": desc.strip().lower() in dup_descs if desc else False,

            "Image Count": r.image_count,
            "Images Missing Alt": r.images_missing_alt,
            "Alts <100": alt_too_short,
            "Alts >125": alt_too_long,
            "Filenames with spaces": file_spaces,
            "Filenames with uppercase": file_upper,
            "Filenames with underscores": file_underscore,
            "Files bad ext": file_bad_ext,
        })

    # write
    print(f"[export] ({noise_policy}) Writing: {xlsx_path}")
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        to_df(internal_rows).to_excel(writer, sheet_name="Internal", index=False)
        to_df(resp_rows).to_excel(writer, sheet_name="Response Codes", index=False)
        to_df(directives_rows).to_excel(writer, sheet_name="Directives", index=False)
        to_df(canon_rows).to_excel(writer, sheet_name="Canonicals", index=False)
        to_df(head_rows).to_excel(writer, sheet_name="Headings", index=False)
        to_df(title_rows).to_excel(writer, sheet_name="Page Titles", index=False)
        to_df(meta_rows).to_excel(writer, sheet_name="Meta Descriptions", index=False)
        to_df(img_rows).to_excel(writer, sheet_name="Images", index=False)
        to_df(in_rows).to_excel(writer, sheet_name="Inlinks", index=False)
        to_df(out_rows).to_excel(writer, sheet_name="Outlinks", index=False)
        to_df(dup_exact_rows).to_excel(writer, sheet_name="Duplicates Exact", index=False)
        to_df(hreflang_rows).to_excel(writer, sheet_name="Hreflang", index=False)
        to_df(issue_rows).to_excel(writer, sheet_name="Issues", index=False)
        to_df(quality_rows).to_excel(writer, sheet_name="Quality", index=False)
        to_df(images_detail_rows).to_excel(writer, sheet_name="Images Detail", index=False)
    print(f"[\u2713] ({noise_policy}) Excel written: {xlsx_path}")

# =========================
# CLI & Main
# =========================

def parse_args():
    ap = argparse.ArgumentParser("Shopify SF-like Auditor (patched)")
    ap.add_argument("--store", required=True, help="https://domain.tld or domain.tld")
    ap.add_argument("--out", required=True, help="Output directory")
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--user-agent", default=DEFAULT_HEADERS["User-Agent"])
    ap.add_argument("--max-pages", type=int, default=10000)
    ap.add_argument("--max-depth", type=int, default=10)
    ap.add_argument("--include-params", action="store_true")
    ap.add_argument("--noise-policy", choices=["label","exclude"], default="label")
    # PSI
    ap.add_argument("--psi-key", default="")
    ap.add_argument("--psi-max", type=int, default=50)
    ap.add_argument("--psi-delay", type=float, default=0.25)
    ap.add_argument("--psi-strategies", default="mobile,desktop")
    return ap.parse_args()


def main():
    args = parse_args()
    start = args.store.strip().rstrip("/")
    if not start.startswith(("http://","https://")):
        start = "https://" + start

    print(f"[1/5] Starting crawl: {start}")
    auditor = Auditor(
        start_url=start,
        out_dir=args.out,
        timeout=args.timeout,
        sleep=args.sleep,
        ua=args.user_agent,
        max_pages=args.max_pages,
        max_depth=args.max_depth,
        include_params=args.include_params,
    )
    auditor.crawl()
    print(f"[2/5] Pages collected: {len(auditor.pages)} | Edges: {len(auditor.edges)}")

    # PSI optional
    strategies = [s.strip().lower() for s in args.psi_strategies.split(",") if s.strip()]
    strategies = [s for s in strategies if s in ("mobile","desktop")] or ["mobile","desktop"]
    if args.psi_key:
        print(f"[3/5] Collecting PSI for up to {args.psi_max} URLs (strategies: {', '.join(strategies)})")
        collect_psi(auditor.pages, strategies, api_key=args.psi_key, max_urls=args.psi_max, delay=args.psi_delay)
    else:
        print("[3/5] PSI disabled (no --psi-key)")

    # Export both
    print(f"[4/5] Export (raw/labeled) with noise policy = {args.noise_policy}")
    export_workbook(
        out_dir=args.out,
        pages=auditor.pages,
        edges=auditor.edges,
        exact_text=auditor.exact_text,
        hreflang_map=auditor.hreflang_map,
        noise_policy=args.noise_policy,
        outfile="shopify_sf_audit.raw.xlsx",
    )

    print("[5/5] Export cleaned workbook (noise excluded)")
    export_workbook(
        out_dir=args.out,
        pages=auditor.pages,
        edges=auditor.edges,
        exact_text=auditor.exact_text,
        hreflang_map=auditor.hreflang_map,
        noise_policy="exclude",
        outfile="shopify_sf_audit.cleaned.xlsx",
    )


if __name__ == "__main__":
    main()
