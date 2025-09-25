
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse, re
import pandas as pd
from urllib.parse import urlparse, urlunparse, parse_qsl

def norm_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    # add https if missing
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", u):
        u = "https://" + u.lstrip("/")
    p = urlparse(u)
    scheme = "https"
    netloc = p.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = p.path or "/"
    # unify case, strip trailing slash (except root)
    path = re.sub(r"/{2,}", "/", path)
    if path != "/":
        path = path.rstrip("/")
    # strip common tracking params
    q = ""
    return urlunparse((scheme, netloc, path, "", q, ""))

def strip_params(u: str) -> str:
    p = urlparse(u)
    return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def shopify_product_path(u: str) -> str:
    # Map /collections/<c>/products/<h> -> /products/<h> ; also singular /product/ -> /products/
    p = urlparse(u)
    segs = [s for s in (p.path or "/").split("/") if s]
    new = []
    i = 0
    while i < len(segs):
        s = segs[i]
        if s.lower() == "collections" and i + 2 < len(segs) and segs[i+2].lower() == "products":
            # skip 'collections/<c>' and keep 'products/<h>'
            i += 2
            continue
        elif s.lower() == "product":
            new.append("products")
        else:
            new.append(s)
        i += 1
    path = "/" + "/".join(new)
    return urlunparse((p.scheme, p.netloc, path, "", "", ""))

def choose_canonical(urls):
    # Pick the shortest path length; tie-break lexicographically.
    def key(u):
        from urllib.parse import urlparse
        return (len(urlparse(u).path), u)
    urls = [norm_url(u) for u in urls if u]
    urls = list(dict.fromkeys(urls))
    if not urls:
        return ""
    return sorted(urls, key=key)[0]

def try_guess_target(u, site_urls_set=None):
    # Heuristics to guess a better target within same host. site_urls_set is optional.
    cand = norm_url(u)
    # 1) drop params
    c1 = strip_params(cand)
    # 2) map to product canonical form
    c2 = shopify_product_path(c1)
    # 3) lowercase path
    p = urlparse(c2)
    c3 = urlunparse((p.scheme, p.netloc, p.path.lower(), "", "", ""))
    # check against known set if provided
    for c in [c3, c2, c1]:
        if site_urls_set is None or c in site_urls_set:
            return c
    return c3  # best-effort

def load_sheet(xlsx, name_candidates):
    xl = pd.ExcelFile(xlsx)
    for name in name_candidates:
        if name in xl.sheet_names:
            return xl.parse(name)
    # case-insensitive fallback
    for s in xl.sheet_names:
        if any(s.lower() == n.lower() for n in name_candidates):
            return xl.parse(s)
    return None

def build_redirects(triage_path: str, known_site_urls=None):
    redirects = []  # dicts with from_url,to_url,reason,confidence
    site_set = set(known_site_urls or [])

    # 1) From "Sitemap Diff"
    df_diff = load_sheet(triage_path, ["Sitemap Diff", "Sitemap Check", "phase1_Sitemap_Diff"])
    if df_diff is not None and not df_diff.empty:
        # accept either a single 'URL' column OR rows already filtered to missing ones
        url_col = None
        for c in df_diff.columns:
            if str(c).strip().lower() in ("url", "address", "page url", "final url"):
                url_col = c
                break
        rows = df_diff.to_dict("records")
        for r in rows:
            src = r.get(url_col) if url_col else r.get("URL")
            if not isinstance(src, str) or not src.strip():
                continue
            src_n = norm_url(src)
            # If sheet includes an explicit flag, skip ones that are in sitemap
            in_flag = None
            for k in r.keys():
                if str(k).strip().lower() in ("in_sitemap", "in sitemap", "insitemap"):
                    in_flag = bool(r[k])
                    break
            if in_flag is True:
                continue
            tgt = try_guess_target(src_n, site_set if site_set else None)
            if tgt and tgt != src_n:
                redirects.append({
                    "from_url": src_n, "to_url": tgt,
                    "reason": "Sitemap-missing canonicalization",
                    "confidence": 0.6 if site_set else 0.5
                })

    # 2) From "Duplicate Content": pick canonical and redirect others
    df_dup = load_sheet(triage_path, ["Duplicate Content", "Duplicate Titles"])
    if df_dup is not None and not df_dup.empty:
        # expect columns: value,count,urls (semicolon-separated)
        if "urls" in df_dup.columns:
            for _, r in df_dup.iterrows():
                try:
                    urls = [u.strip() for u in str(r["urls"]).split(";") if u.strip()]
                except Exception:
                    urls = []
                if len(urls) < 2:
                    continue
                canon = choose_canonical(urls)
                for u in urls:
                    u_n = norm_url(u)
                    if u_n != canon:
                        redirects.append({
                            "from_url": u_n, "to_url": canon,
                            "reason": "Duplicate content canonicalization",
                            "confidence": 0.8
                        })

    # de-dup identical pairs
    seen = set()
    unique = []
    for d in redirects:
        key = (d["from_url"], d["to_url"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(d)
    return pd.DataFrame(unique)

def main():
    ap = argparse.ArgumentParser(description="Suggest 301 redirects from phase1 triage (no extra deps).")
    ap.add_argument("--triage", required=True, help="Path to phase1_triage.xlsx")
    ap.add_argument("--csv-out", required=True, help="Output CSV path")
    args = ap.parse_args()

    df = build_redirects(args.triage)
    if df.empty:
        print(f"Wrote {args.csv_out} with 0 suggestions.")
        pd.DataFrame(columns=["from_url","to_url","reason","confidence"]).to_csv(args.csv_out, index=False)
    else:
        df.sort_values(by=["confidence","reason"], ascending=[False, True]).to_csv(args.csv_out, index=False)
        print(f"Wrote {args.csv_out} with {len(df)} suggestions.")

if __name__ == "__main__":
    main()
