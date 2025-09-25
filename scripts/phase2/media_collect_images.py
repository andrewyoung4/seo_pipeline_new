#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
media_collect_images.py (responsive-aware)
Collects images and audits responsive attributes + CSS backgrounds.

Outputs:
  - data/outputs/phase2/media_images.csv   (unique images: image_url,bytes,mime,next_gen,mime_source,pages_one,pages_count)
  - data/outputs/phase2/media_onpage.csv   (per occurrence: page_url,image_url,tag_type,has_srcset,has_sizes,has_wh,decl_w,decl_h,shopify_size_hint)

Usage:
  python .\scripts\phase2\media_collect_images.py ^
    --origin https://silentprincesstt.com ^
    --urls .\data\inputs\phase2\triage_urls.csv ^
    --out-dir .\data\outputs\phase2
"""
import argparse, csv, sys, re, time
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

ACCEPT = "image/avif,image/webp,image/*;q=0.9,*/*;q=0.8"

def _read_urls(path):
    import pandas as pd
    df = pd.read_csv(path)
    for name in ("url","final_url","normalized_url","loc"):
        col = next((c for c in df.columns if c.lower()==name), None)
        if col:
            return [u for u in df[col].astype(str) if u.startswith("http")]
    for c in df.columns:
        if df[c].astype(str).str.contains(r"^https?://", na=False).any():
            return [u for u in df[c].astype(str) if u.startswith("http")]
    raise SystemExit("Could not find a URL column in the CSV.")

def _abs(origin, page_url, src):
    if not src or src.startswith("data:"): return None
    if src.startswith("//"):
        scheme = urlparse(origin).scheme or "https"
        return scheme + ":" + src
    return urljoin(page_url, src)

def _mime_from_ext(u):
    u = u.lower().split("?")[0]
    if u.endswith(".webp"): return "image/webp"
    if u.endswith(".avif"): return "image/avif"
    if u.endswith(".jpg") or u.endswith(".jpeg"): return "image/jpeg"
    if u.endswith(".png"): return "image/png"
    if u.endswith(".gif"): return "image/gif"
    if u.endswith(".svg"): return "image/svg+xml"
    return ""

def _head_size(u, timeout, ua):
    try:
        r = requests.head(u, timeout=timeout, allow_redirects=True,
                          headers={"User-Agent": ua, "Accept": ACCEPT})
        cl = r.headers.get("Content-Length")
        if cl and cl.isdigit():
            return int(cl), (r.headers.get("Content-Type") or "").lower()
        return None, (r.headers.get("Content-Type") or "").lower()
    except Exception:
        return None, ""

def _peek_get(u, timeout, ua, max_bytes=65536):
    try:
        r = requests.get(u, timeout=timeout, stream=True,
                         headers={"User-Agent": ua, "Accept": ACCEPT})
        total = 0
        for chunk in r.iter_content(8192):
            if not chunk: break
            total += len(chunk)
            if total >= max_bytes: break
        cl = r.headers.get("Content-Length")
        if cl and cl.isdigit():
            return int(cl), (r.headers.get("Content-Type") or "").lower()
        return total, (r.headers.get("Content-Type") or "").lower()
    except Exception:
        return None, ""

_re_shopify_size = re.compile(r'[_-](\d+)[xX](\d+)?(?=[_.])')
_re_shopify_q = re.compile(r'[?&](width|w|h|height)=(\d+)', re.I)

def _shopify_hint(url):
    u = url.split("?")[0]
    m = _re_shopify_size.search(u)
    if m:
        w = m.group(1)
        h = m.group(2) or ""
        return f"{w}x{h}" if h else f"{w}w"
    m2 = _re_shopify_q.findall(url)
    if m2:
        dims = {k.lower(): v for k, v in m2}
        w = dims.get("width") or dims.get("w")
        h = dims.get("height") or dims.get("h")
        if w and h:
            return f"{w}x{h}"
        if w:
            return f"{w}w"
        if h:
            return f"x{h}"
    return ""

def _extract_css_images(html_text, base_url):
    urls = set()
    soup = BeautifulSoup(html_text, "html.parser")
    # inline <style>
    for st in soup.find_all("style"):
        if not st.string: continue
        for u in re.findall(r'url\(([^)]+)\)', st.string):
            u = u.strip('\'"')
            absu = _abs(base_url, base_url, u)
            if absu and absu.startswith("http"):
                urls.add(("css_bg", absu))
    # external stylesheets
    for ln in soup.find_all("link", rel=lambda v: v and "stylesheet" in v):
        href = ln.get("href")
        if not href: continue
        css_url = _abs(base_url, base_url, href)
        if not css_url: continue
        try:
            r = requests.get(css_url, timeout=10)
            if r.status_code != 200 or not r.text: continue
            for u in re.findall(r'url\(([^)]+)\)', r.text):
                u = u.strip('\'"')
                absu = _abs(base_url, css_url, u)
                if absu and absu.startswith("http"):
                    urls.add(("css_bg", absu))
        except Exception:
            continue
    return list(urls)

def collect(origin, urls, out_dir, timeout=10, ua="MediaCollector/1.2 (+https://)", trust_extension=True):
    # unique images
    seen = {}  # image_url -> {bytes,mime,next_gen,pages:set(),mime_source}
    # per-occurrence
    onpage_rows = []  # {page_url,image_url,tag_type,has_srcset,has_sizes,has_wh,decl_w,decl_h,shopify_size_hint}

    for i, page in enumerate(urls, 1):
        try:
            pr = requests.get(page, timeout=timeout, headers={"User-Agent": ua})
        except Exception as e:
            print(f"[warn] page fetch fail: {page} ({e})", file=sys.stderr); continue
        if pr.status_code != 200 or not pr.text:
            print(f"[warn] page status {pr.status_code}: {page}", file=sys.stderr); continue
        soup = BeautifulSoup(pr.text, "html.parser")

        # IMG/SOURCE
        pics = []
        for img in soup.find_all("img"):
            pics.append(("img", img))
        for pic in soup.find_all("picture"):
            # capture <img> inside picture as well
            img = pic.find("img")
            if img:
                pics.append(("img", img))

        for tag_type, el in pics:
            src = el.get("src") or el.get("data-src") or el.get("data-original") or el.get("data-lazy")
            if not src:
                # try first source[srcset]
                srcset = (el.get("srcset") or "")
                if srcset:
                    cand = srcset.split(",")[0].strip().split(" ")[0]
                    src = cand or None
            absu = _abs(origin, page, src) if src else None
            if not absu: 
                continue
            has_srcset = 1 if (el.get("srcset")) else 0
            has_sizes = 1 if (el.get("sizes")) else 0
            wh = (1 if (el.get("width") and el.get("height")) else 0)
            decl_w = el.get("width") or ""
            decl_h = el.get("height") or ""
            hint = _shopify_hint(absu)

            onpage_rows.append({
                "page_url": page,
                "image_url": absu,
                "tag_type": tag_type,
                "has_srcset": has_srcset,
                "has_sizes": has_sizes,
                "has_wh": wh,
                "decl_w": decl_w, "decl_h": decl_h,
                "shopify_size_hint": hint
            })

        # CSS backgrounds
        for tag_type, absu in _extract_css_images(pr.text, page):
            onpage_rows.append({
                "page_url": page,
                "image_url": absu,
                "tag_type": tag_type,
                "has_srcset": 0,
                "has_sizes": 0,
                "has_wh": 0,
                "decl_w": "", "decl_h": "",
                "shopify_size_hint": _shopify_hint(absu)
            })

        # now gather unique images for sizes/mime
        for row in [r for r in onpage_rows if r["page_url"] == page]:
            img = row["image_url"]
            mime_ext = _mime_from_ext(img)
            size, mime_head = _head_size(img, timeout, ua)
            mime_head = (mime_head or "").lower()
            mime = mime_head or mime_ext
            mime_source = "header" if mime_head else ("ext" if mime_ext else "")
            if size is None:
                size, mime_get = _peek_get(img, timeout, ua)
                mime_get = (mime_get or "").lower()
                if mime_get:
                    mime = mime_get
                    mime_source = "header"
                elif not mime and mime_ext:
                    mime = mime_ext
                    mime_source = "ext"
            if trust_extension and mime_ext and ("webp" in mime_ext or "avif" in mime_ext):
                mime = mime_ext
                mime_source = "header+ext" if mime_source=="header" else "ext"
            if size is None:
                continue

            is_next = (("webp" in (mime or "")) or ("avif" in (mime or "")) or ("webp" in (mime_ext or "")) or ("avif" in (mime_ext or "")))

            ent = seen.get(img)
            if not ent:
                ent = {"bytes": size, "mime": mime, "next_gen": 1 if is_next else 0, "pages": set(), "mime_source": mime_source}
                seen[img] = ent
            else:
                ent["bytes"] = max(ent["bytes"], size)
                ent["mime"] = mime or ent["mime"]
                ent["next_gen"] = 1 if (ent["next_gen"] or is_next) else 0
                if ent.get("mime_source") != "header+ext" and mime_source == "header+ext":
                    ent["mime_source"] = "header+ext"
            ent["pages"].add(page)

        if i % 5 == 0:
            print(f"[info] processed {i}/{len(urls)}", file=sys.stderr)
        time.sleep(0.2)

    # write unique images
    out_images = out_dir.rstrip("/\\") + "/media_images.csv"
    with open(out_images, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["image_url","bytes","mime","next_gen","mime_source","pages_one","pages_count"])
        for img, meta in sorted(seen.items(), key=lambda kv: kv[1]["bytes"], reverse=True):
            pages = list(meta["pages"])
            w.writerow([img, meta["bytes"], meta["mime"], meta["next_gen"], meta.get("mime_source",""), pages[0] if pages else "", len(pages)])

    # write onpage audit
    out_onpage = out_dir.rstrip("/\\") + "/media_onpage.csv"
    with open(out_onpage, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["page_url","image_url","tag_type","has_srcset","has_sizes","has_wh","decl_w","decl_h","shopify_size_hint"])
        for r in onpage_rows:
            w.writerow([r["page_url"], r["image_url"], r["tag_type"], r["has_srcset"], r["has_sizes"], r["has_wh"], r["decl_w"], r["decl_h"], r["shopify_size_hint"]])

    print(f"[done] wrote {out_images} and {out_onpage}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--origin", required=True)
    ap.add_argument("--urls", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--timeout", type=int, default=10)
    ap.add_argument("--no-trust-extension", action="store_true")
    args = ap.parse_args()
    urls = _read_urls(args.urls)
    collect(args.origin, urls, args.out_dir, timeout=args.timeout, trust_extension=not args.no_trust_extension)

if __name__ == "__main__":
    main()
