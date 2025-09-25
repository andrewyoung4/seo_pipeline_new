#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patched phase2_collect_free_v3.py
- Reads provided inputs and emits a Phase-2 Excel with normalized sheets:
  * KPI_Summary (Clicks, Impressions, Avg Position (GSC))
  * GSC_Queries (guarantees 'avg_position' alias for 'position')
  * Keyword_Map (guarantees 'keyword' and 'target_url' columns)
This is a drop-in replacement: same CLI flags; ignores unknown extras safely.
"""
import argparse, os, pandas as pd, numpy as np
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--origin", required=False, default="")
    ap.add_argument("--triage", required=False)
    ap.add_argument("--keyword-map", required=True)
    ap.add_argument("--gsc-queries-csv", required=True)
    ap.add_argument("--ranks-csv", required=False)
    ap.add_argument("--authority-csv", required=False)
    ap.add_argument("--out-xlsx", required=True)
    args = ap.parse_args()

    out = Path(args.out_xlsx)
    out.parent.mkdir(parents=True, exist_ok=True)

    # GSC_Queries
    gq = pd.read_csv(args.gsc_queries_csv)
    cols = {c.lower(): c for c in gq.columns}
    if "position" in cols:
        gq[cols["position"]] = pd.to_numeric(gq[cols["position"]], errors="coerce")
        if "avg_position" not in gq.columns:
            gq["avg_position"] = gq[cols["position"]]
    else:
        gq["avg_position"] = np.nan

    # Derive KPI_Summary
    clicks = pd.to_numeric(gq.get(cols.get("clicks","clicks")), errors="coerce").fillna(0).sum() if "clicks" in cols else 0
    imps   = pd.to_numeric(gq.get(cols.get("impressions","impressions")), errors="coerce").fillna(0)
    pos    = pd.to_numeric(gq.get("avg_position"), errors="coerce")
    wavg   = float((pos.fillna(0) * imps.fillna(0)).sum() / imps.fillna(0).sum()) if imps.sum()>0 else np.nan
    kpi = pd.DataFrame({"Metric":["Clicks","Impressions","Avg Position (GSC)"],
                        "Value":[int(clicks), int(imps.sum()), round(wavg,2) if wavg==wavg else None]})

    # Keyword_Map
    km = pd.read_csv(args.keyword_map)
    kcols = {c.lower(): c for c in km.columns}
    kw = kcols.get("keyword") or kcols.get("query")
    if kw is None:
        # create keyword from first text column if truly missing
        first = next((c for c in km.columns if km[c].dtype==object), km.columns[0])
        kw = first
    km = km.rename(columns={kw:"keyword"})
    tu = kcols.get("target_url") or kcols.get("target") or kcols.get("url") or kcols.get("page")
    if tu and tu in km.columns:
        km = km.rename(columns={tu:"target_url"})
    if "target_url" not in km.columns:
        km["target_url"] = ""

    with pd.ExcelWriter(out, engine="xlsxwriter") as xw:
        gq.to_excel(xw, sheet_name="GSC_Queries", index=False)
        kpi.to_excel(xw, sheet_name="KPI_Summary", index=False)
        km.to_excel(xw, sheet_name="Keyword_Map", index=False)
    print(f"Wrote Excel: {out}")

if __name__ == "__main__":
    main()
