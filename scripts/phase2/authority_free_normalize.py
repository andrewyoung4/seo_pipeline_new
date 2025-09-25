#!/usr/bin/env python3
r"""
authority_free_normalize.py

Goal: Normalize various "authority" / SEO metrics CSVs from different providers
      (e.g., Ubersuggest, OpenLinkProfiler, Moz-like exports) into a single
      canonical CSV that Phase-2 can ingest.

Input:  --in <csv>   (required)  Path to vendor CSV
Output: --out <csv>  (required)  Path to normalized CSV
Options:
  --source <name>    (optional)  Hint for vendor ("ubersuggest","olp","moz","ahrefs","semrush").
  --domain-col <col> (optional)  Force domain/URL column name
  --encoding <enc>   (optional)  Default "utf-8", fallbacks tried automatically
  --minify           (flag)      Keep only canonical columns

Canonical columns emitted (superset; some may be blank):
  domain, metric_source, authority, authority_scale, traffic, backlinks, referring_domains,
  rank_keywords, spam_score, notes

Rules:
- We parse a domain from URL when necessary.
- We auto-detect columns case-insensitively and via fuzzy aliases.
- We coerce numeric strings safely (commas, percents, "N/A").
- Unknown columns are preserved unless --minify is set.
- Output rows with empty "domain" are dropped.

Usage (Windows PowerShell):
  py .\scripts\phase2\authority_free_normalize.py `
    --in .\data\inputs\phase2\ubersuggest_or_olp.csv `
    --out .\data\inputs\phase2\authority_generic.csv `
    --source ubersuggest --minify

Author: Silent Princess SEO Pipeline
"""
from __future__ import annotations

import argparse
import math
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
from urllib.parse import urlparse

# ---------- Helpers ----------

def try_read_csv(path: Path, enc_guess: str = "utf-8") -> pd.DataFrame:
    encs = [enc_guess, "utf-8-sig", "latin-1"]
    last_err = None
    for enc in encs:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    raise last_err  # type: ignore[misc]

def to_domain(value: str) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    # If already looks like a domain
    if re.fullmatch(r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}", s):
        return s.lower().lstrip(".")
    # If URL
    try:
        u = urlparse(s if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", s) else f"https://{s}")
        host = (u.netloc or "").lower()
        return host.lstrip("www.") if host else ""
    except Exception:
        return ""

def to_number(x) -> Optional[float]:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    s = str(x).strip()
    if s == "" or s.lower() in {"n/a", "na", "null", "none", "-"}:
        return None
    s = s.replace(",", "")
    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100.0
        except Exception:
            return None
    try:
        return float(s)
    except Exception:
        # Some vendors ship like "1.2K" "3.4M"
        m = re.match(r"^([0-9]*\.?[0-9]+)\s*([KkMm])$", s)
        if m:
            val = float(m.group(1))
            mul = 1_000 if m.group(2).lower() == "k" else 1_000_000
            return val * mul
        return None

def pick(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    cols = {str(c).strip().lower(): c for c in df.columns}
    for a in aliases:
        if a.lower() in cols:
            return cols[a.lower()]
    # fuzzy contains
    for a in aliases:
        for key in cols:
            if a.lower() in key:
                return cols[key]
    return None

# ---------- Vendor normalizers ----------

def norm_ubersuggest(df: pd.DataFrame) -> pd.DataFrame:
    # Example aliases (may vary by export version)
    dom_col = pick(df, ["domain", "website", "url", "root domain", "site"])
    auth_col = pick(df, ["domain authority", "authority score", "ds", "da"])
    traf_col = pick(df, ["traffic", "organic traffic", "monthly traffic", "visits"])
    back_col = pick(df, ["backlinks", "total backlinks"])
    refd_col = pick(df, ["referring domains", "ref domains", "domains"])
    kw_col   = pick(df, ["keywords", "ranked keywords"])
    spam_col = pick(df, ["spam score"])

    out = pd.DataFrame()
    out["domain"] = df[dom_col].map(to_domain) if dom_col else ""
    out["metric_source"] = "ubersuggest"
    out["authority"] = df[auth_col].map(to_number) if auth_col else None
    out["authority_scale"] = 100.0  # typically 0-100
    out["traffic"] = df[traf_col].map(to_number) if traf_col else None
    out["backlinks"] = df[back_col].map(to_number) if back_col else None
    out["referring_domains"] = df[refd_col].map(to_number) if refd_col else None
    out["rank_keywords"] = df[kw_col].map(to_number) if kw_col else None
    out["spam_score"] = df[spam_col].map(to_number) if spam_col else None
    return out

def norm_olp(df: pd.DataFrame) -> pd.DataFrame:
    # OpenLinkProfiler typical fields
    dom_col = pick(df, ["domain", "website", "url", "site", "root domain"])
    auth_col = pick(df, ["domain rating", "domain authority", "link influence score","lis"])
    back_col = pick(df, ["backlinks", "links total", "total links", "all backlinks"])
    refd_col = pick(df, ["referring domains", "linking sites", "domains"])
    spam_col = pick(df, ["spam score"])

    out = pd.DataFrame()
    out["domain"] = df[dom_col].map(to_domain) if dom_col else ""
    out["metric_source"] = "olp"
    out["authority"] = df[auth_col].map(to_number) if auth_col else None
    out["authority_scale"] = 100.0
    out["traffic"] = None
    out["backlinks"] = df[back_col].map(to_number) if back_col else None
    out["referring_domains"] = df[refd_col].map(to_number) if refd_col else None
    out["rank_keywords"] = None
    out["spam_score"] = df[spam_col].map(to_number) if spam_col else None
    return out

def norm_moz(df: pd.DataFrame) -> pd.DataFrame:
    dom_col = pick(df, ["root_domain", "root domain", "domain", "url"])
    da_col  = pick(df, ["domain_authority", "domain authority", "da"])
    traf_col = pick(df, ["est_monthly_visits", "traffic"])
    back_col = pick(df, ["external_links", "backlinks", "linking_root_domains"])
    refd_col = pick(df, ["linking_root_domains", "referring domains"])
    spam_col = pick(df, ["spam_score"])

    out = pd.DataFrame()
    out["domain"] = df[dom_col].map(to_domain) if dom_col else ""
    out["metric_source"] = "moz"
    out["authority"] = df[da_col].map(to_number) if da_col else None
    out["authority_scale"] = 100.0
    out["traffic"] = df[traf_col].map(to_number) if traf_col else None
    out["backlinks"] = df[back_col].map(to_number) if back_col else None
    out["referring_domains"] = df[refd_col].map(to_number) if refd_col else None
    out["rank_keywords"] = None
    out["spam_score"] = df[spam_col].map(to_number) if spam_col else None
    return out

def norm_generic(df: pd.DataFrame, source_hint: Optional[str]) -> pd.DataFrame:
    # Try a general best-guess mapping
    dom_col = pick(df, ["domain", "website", "url", "root domain", "site"])
    auth_col = pick(df, ["domain authority", "authority score", "domain rating", "dr", "da"])
    traf_col = pick(df, ["traffic", "organic traffic", "monthly traffic", "visits"])
    back_col = pick(df, ["backlinks", "links total", "total links", "external links"])
    refd_col = pick(df, ["referring domains", "linking root domains", "domains", "linking sites"])
    kw_col   = pick(df, ["keywords", "ranked keywords"])
    spam_col = pick(df, ["spam score"])

    out = pd.DataFrame()
    out["domain"] = df[dom_col].map(to_domain) if dom_col else ""
    out["metric_source"] = (source_hint or "generic").lower()
    out["authority"] = df[auth_col].map(to_number) if auth_col else None
    out["authority_scale"] = 100.0
    out["traffic"] = df[traf_col].map(to_number) if traf_col else None
    out["backlinks"] = df[back_col].map(to_number) if back_col else None
    out["referring_domains"] = df[refd_col].map(to_number) if refd_col else None
    out["rank_keywords"] = df[kw_col].map(to_number) if kw_col else None
    out["spam_score"] = df[spam_col].map(to_number) if spam_col else None
    return out

# ---------- Main ----------

def main() -> int:
    ap = argparse.ArgumentParser(description="Normalize various authority CSVs to a canonical schema.")
    ap.add_argument("--in", dest="inp", required=True, help="Input CSV file")
    ap.add_argument("--out", dest="out", required=True, help="Output normalized CSV")
    ap.add_argument("--source", dest="source", default=None, help="Source hint: ubersuggest|olp|moz|ahrefs|semrush")
    ap.add_argument("--domain-col", dest="domain_col", default=None, help="Force domain/URL column name")
    ap.add_argument("--encoding", dest="encoding", default="utf-8", help="Input encoding guess (default utf-8)")
    ap.add_argument("--minify", action="store_true", help="Keep only canonical columns")
    args = ap.parse_args()

    inp = Path(args.inp)
    if not inp.exists():
        print(f"[ERROR] Input not found: {inp}", file=sys.stderr)
        return 2

    df = try_read_csv(inp, args.encoding)

    # If user forced the domain column, inject a parsed domain series
    forced_domain = None
    if args.domain_col:
        col = None
        for c in df.columns:
            if str(c).strip().lower() == str(args.domain_col).strip().lower():
                col = c; break
        if col is None:
            print(f"[ERROR] --domain-col '{args.domain_col}' not in CSV. Available: {list(df.columns)}", file=sys.stderr)
            return 3
        forced_domain = df[col].map(to_domain)

    source = (args.source or "").strip().lower()
    if source == "ubersuggest":
        out = norm_ubersuggest(df)
    elif source in {"olp", "openlinkprofiler", "open-link-profiler"}:
        out = norm_olp(df)
    elif source == "moz":
        out = norm_moz(df)
    else:
        out = norm_generic(df, source if source else None)

    if forced_domain is not None:
        out["domain"] = forced_domain

    # Drop rows with empty domains
    out["domain"] = out["domain"].fillna("").map(lambda s: str(s).strip().lower().lstrip("."))
    out = out[out["domain"] != ""]

    # Ensure canonical column ordering / presence
    canonical_order = [
        "domain", "metric_source", "authority", "authority_scale",
        "traffic", "backlinks", "referring_domains", "rank_keywords", "spam_score", "notes"
    ]
    for col in canonical_order:
        if col not in out.columns:
            out[col] = None
    out = out[canonical_order + [c for c in out.columns if c not in canonical_order]]

    # Write
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[OK] Wrote {len(out)} rows → {out_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
