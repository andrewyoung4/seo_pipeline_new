#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
phase3_relevance_filter.py — per-domain relevance filtering
- Reads competitors_serp_hits.csv
- Filters rows using include/exclude terms from a per-domain YAML/JSON config (or CLI overrides)
- Backs up original to .unfiltered.csv (once) and overwrites the live CSV
"""
from __future__ import annotations

import argparse
import json
import re
from urllib.parse import urlsplit
from pathlib import Path
import pandas as pd

def _domain_from_origin(origin: str) -> str:
    netloc = urlsplit(origin).netloc or origin.split("://")[-1]
    return netloc.split("/")[0].lower()

def _read_config_file(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".json"}:
        return json.loads(text)
    try:
        import yaml  # optional; install with: python -m pip install pyyaml
        return yaml.safe_load(text) or {}
    except Exception:
        return json.loads(text)

def load_relevance(origin: str, cfg_path: str) -> tuple[set[str], set[str]]:
    inc, exc = set(), set()
    if cfg_path:
        p = Path(cfg_path)
        if p.exists():
            d = _read_config_file(p)
            inc |= set(d.get("include_terms") or [])
            exc |= set(d.get("exclude_terms") or [])
            return inc, exc
    dom = _domain_from_origin(origin)
    for cand in [Path(f"data/inputs/phase3/relevance/{dom}.yml"),
                 Path(f"data/inputs/phase3/relevance/{dom}.json")]:
        if cand.exists():
            d = _read_config_file(cand)
            inc |= set(d.get("include_terms") or [])
            exc |= set(d.get("exclude_terms") or [])
            return inc, exc
    return inc, exc

def make_predicate(include_csv: str, exclude_csv: str, inc_set: set[str], exc_set: set[str]):
    inc = {t.strip().lower() for t in include_csv.split(",") if t.strip()} or {t.lower() for t in inc_set}
    exc = {t.strip().lower() for t in exclude_csv.split(",") if t.strip()} or {t.lower() for t in exc_set}
    def is_relevant(keyword: str, title: str = "", url: str = "") -> bool:
        text = f"{keyword} {title} {url}".lower()
        if any(re.search(rf"\b{re.escape(t)}\b", text) for t in exc):
            return False
        return (not inc) or any(re.search(rf"\b{re.escape(t)}\b", text) for t in inc)
    return is_relevant

def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Filter Phase-3 SERP hits by relevance.")
    ap.add_argument("--origin", required=True)
    ap.add_argument("--hits-csv", required=True)
    ap.add_argument("--relevance-yaml", default="")
    ap.add_argument("--include-terms", default="")
    ap.add_argument("--exclude-terms", default="")
    args = ap.parse_args(argv)

    hits_path = Path(args.hits_csv)
    if not hits_path.exists():
        print(f"[WARN] hits CSV not found: {hits_path}; nothing to filter.")
        return 0

    inc_set, exc_set = load_relevance(args.origin, args.relevance_yaml)
    pred = make_predicate(args.include_terms, args.exclude_terms, inc_set, exc_set)

    df = pd.read_csv(hits_path, encoding="utf-8", on_bad_lines="skip")
    if df.empty:
        print(f"[INFO] No rows in {hits_path}; skipping filter.")
        return 0

    cols = {c.lower(): c for c in df.columns}
    kcol = cols.get("keyword") or cols.get("query") or cols.get("kw") or list(cols.values())[0]
    tcol = cols.get("title") or ""
    ucol = cols.get("url") or cols.get("link") or ""

    def _ok(row) -> bool:
        kw = str(row.get(kcol, ""))
        ti = str(row.get(tcol, "")) if tcol else ""
        ur = str(row.get(ucol, "")) if ucol else ""
        return pred(kw, ti, ur)

    before = len(df)
    df2 = df[df.apply(_ok, axis=1)].copy()
    after = len(df2)

    # Backup original once
    backup = hits_path.with_suffix(".unfiltered.csv")
    if not backup.exists():
        try:
            df.to_csv(backup, index=False, encoding="utf-8")
        except Exception:
            pass

    df2.to_csv(hits_path, index=False, encoding="utf-8")
    print(f"[INFO] Filtered SERP hits: {before} -> {after} rows written to {hits_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
