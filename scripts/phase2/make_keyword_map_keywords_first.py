#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, re, sys
import pandas as pd

BASE_STOP = {
    "for","in","to","of","and","or","a","an","the","on","with","your","my","our","at","by","from",
    "shop","store","buy","online","website","site","page","pages","near","me","price","prices"
}

RE_URL = re.compile(r"^https?://", re.I)
RE_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
RE_EMOJI = re.compile(r"[\U00010000-\U0010ffff]", flags=re.UNICODE)
RE_SKUISH = re.compile(r"^[a-z]*\d{3,}[a-z\d\-]*$", re.I)

def smart_read_csv(path: str) -> pd.DataFrame:
    for enc in (None, "utf-8", "utf-8-sig", "cp1252"):
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def clean_keyword(q: str, brand_tokens, extra_stop, allow_numbers: bool) -> str:
    if not isinstance(q, str): q = "" if pd.isna(q) else str(q)
    s = q.lower()
    if RE_URL.search(s) or RE_EMAIL.match(s):
        return ""
    s = RE_EMOJI.sub("", s)
    s = re.sub(r"[^\w\s]", " ", s)
    s = s.replace("_", " ")
    s = norm_space(s)
    for bt in brand_tokens:
        s = re.sub(rf"\b{re.escape(bt)}\b", " ", s)
    tokens = [t for t in s.split(" ") if t]
    kept = []
    for t in tokens:
        tl = t.lower()
        if tl in BASE_STOP or tl in extra_stop:
            continue
        if not allow_numbers and tl.isdigit():
            continue
        if RE_SKUISH.match(tl):
            continue
        kept.append(tl)
    s2 = " ".join(kept)
    s2 = norm_space(s2)
    s2 = " ".join([t for t in s2.split(" ") if len(t) > 1])
    return s2

def pick(mapping, options):
    for o in options:
        if o.lower() in mapping:
            return mapping[o.lower()]
    for k, v in mapping.items():
        for o in options:
            if o.lower() in k: return v
    return None

def main():
    ap = argparse.ArgumentParser(description="Build a clean keyword_map.csv (query, keyword, target_url) from GSC")
    ap.add_argument("--gsc-queries-csv", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--brand", required=False, default="")
    ap.add_argument("--min-impr", required=False, type=int, default=10)
    ap.add_argument("--min-len", required=False, type=int, default=3)
    ap.add_argument("--stop-list", required=False, default="")
    ap.add_argument("--allow-numbers", action="store_true")
    ap.add_argument("--write-diag", required=False, default=None)
    args = ap.parse_args()

    df = smart_read_csv(args.gsc_queries_csv)
    mapping = {str(c).strip().lower(): c for c in df.columns}
    q = pick(mapping, ["query","search query","keyword","term"])
    impr = pick(mapping, ["impressions","impr"])

    if q is None or impr is None:
        print("[ERROR] Need columns for query and impressions.", file=sys.stderr)
        print("Columns detected:", list(df.columns), file=sys.stderr)
        sys.exit(2)

    brand_tokens = [t.strip().lower() for t in args.brand.split(",") if t.strip()]
    extra_stop = set([t.strip().lower() for t in args.stop_list.split(",") if t.strip()])

    work = df[[q,impr]].copy()
    work.columns = ["query","impressions"]
    work["impressions"] = pd.to_numeric(work["impressions"], errors="coerce").fillna(0).astype(int)

    work = work[work["impressions"] >= args.min_impr].copy()

    work["keyword"] = work["query"].astype(str).map(lambda s: clean_keyword(s, brand_tokens, extra_stop, args.allow_numbers))

    work = work[work["keyword"].str.len() >= args.min_len].copy()

    work = work.sort_values(["keyword","impressions"], ascending=[True, False])                .drop_duplicates(subset=["query"], keep="first")

    if args.write_diag:
        diag = work.groupby("keyword", as_index=False)["impressions"].sum().sort_values("impressions", ascending=False)
        diag.to_csv(args.write_diag, index=False, encoding="utf-8")

    out = work[["query","keyword"]].copy()
    out["target_url"] = ""
    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
    out.to_csv(args.out_csv, index=False, encoding="utf-8")
    print(f"Wrote keyword_map: {args.out_csv} (rows={len(out)})")

if __name__ == "__main__":
    main()
