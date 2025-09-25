#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build Phase-1 remediation TODO CSVs from triage (and, optionally, audit).

Inputs:
  --triage  data/outputs/phase1/phase1_triage.xlsx
  [--audit] data/outputs/audits/shopify_sf_audit.cleaned.xlsx  (optional; used for Canonical/Robots hints)
  --outdir  data/outputs/previews

Outputs (any missing set is skipped):
  redirects_todo.csv          (from 'Sitemap Diff')
  canonicals_todo.csv         (from 'dup_body' cluster sheet)
  robots_todo.csv             (from audit's 'Internal' 'Meta Robots' flags)
  broken_links_todo.csv       (from 'Broken Internal Links' or non-200 'Outlinks')

Notes:
- We only *suggest* targets; you review/edit before running the remediator.
"""

import argparse, os, re
from urllib.parse import urlparse
import pandas as pd

URL_CAND = ["URL","Address","Page URL","Final URL","Link"]
CLUSTER_CAND = ["cluster","Cluster","group","Group","duplicate_set","Duplicate Set"]

def norm(u:str)->str:
    try:
        p = urlparse(str(u))
    except Exception:
        return ""
    host = (p.netloc or "").lower()
    if host.startswith("www."): host = host[4:]
    path = p.path or "/"
    if path != "/": path = path.rstrip("/")
    return f"https://{host}{path}"

def pick_url_col(df):
    for c in URL_CAND:
        if c in df.columns: return c
    return None

def pick_cluster_col(df):
    for c in CLUSTER_CAND:
        if c in df.columns: return c
    # some dup sheets have 'Hash' per body instead of a cluster id
    for c in df.columns:
        if str(c).lower().startswith("hash"): return c
    return None

def load_sheet(xl, name):
    return xl.parse(sheet_name=name)

def main():
    ap = argparse.ArgumentParser("Build Phase-1 remediation TODOs from triage")
    ap.add_argument("--triage", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--audit", default=None, help="Optional audit workbook for Canonical/Robots suggestions")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    tri = pd.ExcelFile(args.triage)

    # Optional audit (for Canonical / Robots suggestions)
    canon_map = {}
    robots_map = {}
    if args.audit and os.path.isfile(args.audit):
        aud = pd.ExcelFile(args.audit)
        # Prefer 'Internal'
        sheet = "Internal" if "Internal" in aud.sheet_names else aud.sheet_names[0]
        df_int = aud.parse(sheet_name=sheet)
        url_col = pick_url_col(df_int)
        if url_col:
            df_int["_u"] = df_int[url_col].astype(str).map(norm)
            if "Canonical" in df_int.columns:
                df_int["_canon"] = df_int["Canonical"].astype(str).map(norm)
                canon_map = dict(zip(df_int["_u"], df_int["_canon"]))
            # Robots
            rob_col = None
            for c in df_int.columns:
                if "robots" in str(c).lower():
                    rob_col = c; break
            if rob_col:
                robots_map = dict(zip(df_int["_u"], df_int[rob_col].astype(str)))

    # 1) Redirects from 'Sitemap Diff'
    if "Sitemap Diff" in tri.sheet_names:
        sd = load_sheet(tri, "Sitemap Diff")
        miss_col = next((c for c in sd.columns if "missing" in c.lower()), None)
        if miss_col:
            redir = pd.DataFrame({
                "from_url": sd[miss_col].astype(str).map(norm)
            }).dropna()
            # Suggest canonical target if available and different
            def suggest(u):
                cu = canon_map.get(u, "")
                return cu if (cu and cu != u) else ""
            redir["to_url"] = redir["from_url"].map(suggest)
            redir["reason"] = redir.apply(lambda r: "sitemap alignment / canonicalize" if r["to_url"] else "sitemap alignment (choose target)", axis=1)
            # Drop blanks/dupes
            redir = redir[redir["from_url"] != ""].drop_duplicates()
            out = os.path.join(args.outdir, "redirects_todo.csv")
            redir.to_csv(out, index=False, encoding="utf-8")
            print("Wrote", out)

    # 2) Canonicals from 'dup_body'
    if "dup_body" in tri.sheet_names:
        db = load_sheet(tri, "dup_body")
        url_col = pick_url_col(db)
        cl_col  = pick_cluster_col(db)
        if url_col and cl_col:
            work = db[[cl_col, url_col]].rename(columns={cl_col:"cluster", url_col:"url"}).copy()
            work["url"] = work["url"].astype(str).map(norm)
            # pick a suggested canonical per cluster: prefer a URL that appears as someone else's canonical; else first URL
            sugg = []
            for cid, grp in work.groupby("cluster"):
                urls = [u for u in grp["url"].tolist() if u]
                if not urls: continue
                # heuristic: prefer one with inbound canonicals (from audit), else first
                ranked = sorted(urls, key=lambda u: sum(1 for v in urls if canon_map.get(v,"")==u), reverse=True)
                winner = ranked[0]
                for u in urls:
                    if u != winner:
                        sugg.append((u, winner, f"duplicate body cluster {cid}"))
            if sugg:
                can = pd.DataFrame(sugg, columns=["url","canonical_to","notes"]).drop_duplicates()
                out = os.path.join(args.outdir, "canonicals_todo.csv")
                can.to_csv(out, index=False, encoding="utf-8")
                print("Wrote", out)

    # 3) Robots from audit (flip unintended noindex)
    if robots_map:
        rob = []
        for u, flag in robots_map.items():
            f = str(flag).lower()
            if "noindex" in f:
                rob.append((u, "", "contains noindex — set false to allow indexing?"))
        if rob:
            rdf = pd.DataFrame(rob, columns=["url","set_noindex(true|false)","notes"]).drop_duplicates()
            out = os.path.join(args.outdir, "robots_todo.csv")
            rdf.to_csv(out, index=False, encoding="utf-8")
            print("Wrote", out)

    # 4) Broken internal links
    if "Broken Internal Links" in tri.sheet_names:
        bl = load_sheet(tri, "Broken Internal Links")
        src = next((c for c in bl.columns if ("source" in c.lower() and "url" in c.lower())), None)
        dst = next((c for c in bl.columns if (("link" in c.lower() or "url" in c.lower()) and (src is None or c != src))), None)
        if src and dst:
            outdf = bl[[src, dst]].copy()
            outdf.columns = ["source_url","broken_link"]
            outdf["fix_to"] = ""
            outdf["source_url"] = outdf["source_url"].astype(str).map(norm)
            outdf["broken_link"] = outdf["broken_link"].astype(str).map(norm)
            outdf = outdf[(outdf["source_url"]!="") & (outdf["broken_link"]!="")].drop_duplicates()
            out = os.path.join(args.outdir, "broken_links_todo.csv")
            outdf.to_csv(out, index=False, encoding="utf-8")
            print("Wrote", out)
    elif "Outlinks" in tri.sheet_names:
        ol = load_sheet(tri, "Outlinks")
        src = next((c for c in ol.columns if ("from" in c.lower() and "url" in c.lower())), None)
        dst = next((c for c in ol.columns if (("to" in c.lower() or "destination" in c.lower()) and "url" in c.lower())), None)
        st  = next((c for c in ol.columns if "status" in c.lower()), None)
        if src and dst and st:
            mask = ~ol[st].astype(str).str.startswith("2")
            sub = ol.loc[mask, [src, dst, st]].copy()
            sub.columns = ["source_url","broken_link","status"]
            sub["fix_to"] = ""
            for col in ["source_url","broken_link"]:
                sub[col] = sub[col].astype(str).map(norm)
            sub = sub[(sub["source_url"]!="") & (sub["broken_link"]!="")].drop_duplicates()
            out = os.path.join(args.outdir, "broken_links_todo.csv")
            sub[["source_url","broken_link","fix_to"]].to_csv(out, index=False, encoding="utf-8")
            print("Wrote", out)

    print("Done.")
if __name__ == "__main__":
    main()
