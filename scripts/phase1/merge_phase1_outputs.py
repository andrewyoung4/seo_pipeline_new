
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse, os, re, sys
from pathlib import Path
import pandas as pd

def main():
    ap = argparse.ArgumentParser(description="Safe merge Phase-1 outputs (CSV and XLSX) into triage XLSX with normalized sheet names.")
    ap.add_argument("--phase1-dir", required=True, help="Folder with Phase-1 outputs (CSV/XLSX)")
    ap.add_argument("--out", required=True, help="Output triage XLSX path, e.g., data/outputs/phase1/phase1_triage.xlsx")
    args = ap.parse_args()

    src = Path(args.phase1_dir)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    files = list(src.glob("*.csv")) + list(src.glob("*.xlsx"))
    if not files:
        raise SystemExit(f"No CSV/XLSX found in {src}")

    def norm_name(p: Path) -> str:
        name = p.stem
        friendly = {
            "phase1_Schema_Check": "Schema Check",
            "phase1_Sitemap_Diff": "Sitemap Diff",
            "thin_content": "Thin Content",
            "duplicate_content": "Duplicate Content",
            "phase1_schema_validator": "Schema Check",
            "phase1_sitemap_validator": "Sitemap Check",
            "phase1_thin_content": "Thin Content",
            "phase1_duplicate_content": "Duplicate Content",
        }
        if name in friendly:
            return friendly[name]
        name = re.sub(r"[_\-]+", " ", name).strip().title()
        return name[:31] if name else "Sheet"

    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        for p in files:
            sheet = norm_name(p)
            try:
                if p.suffix.lower() == ".csv":
                    df = pd.read_csv(p)
                else:
                    df = pd.read_excel(p, sheet_name=0, engine="openpyxl")
                df.to_excel(xw, index=False, sheet_name=sheet)
            except Exception as e:
                print(f"Skip {p.name}: {e}", file=sys.stderr)
    print(f"Wrote {out} with {len(files)} sheet(s).")

if __name__ == "__main__":
    main()
