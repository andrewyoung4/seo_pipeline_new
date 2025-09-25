
import argparse, pandas as pd

def main():
    ap = argparse.ArgumentParser(description="Generate Phase-4 Runbook v2 (consolidated workbook)")
    ap.add_argument("--audit-xlsx", required=True)
    ap.add_argument("--phase1-xlsx", required=True)
    ap.add_argument("--phase2-xlsx", required=True)
    ap.add_argument("--phase3-xlsx", required=True)
    ap.add_argument("--out-xlsx", required=True)
    args = ap.parse_args()

    inputs = {
        "Audit": args.audit_xlsx,
        "Phase1": args.phase1_xlsx,
        "Phase2": args.phase2_xlsx,
        "Phase3": args.phase3_xlsx,
    }

    with pd.ExcelWriter(args.out_xlsx, engine="xlsxwriter") as xw:
        for name, path in inputs.items():
            xl = pd.ExcelFile(path)
            for sheet in xl.sheet_names:
                df = xl.parse(sheet)
                sname = f"{name} — {sheet}"
                if len(sname) > 31: sname = sname[:31]
                df.to_excel(xw, index=False, sheet_name=sname)
    print(f"Wrote {args.out_xlsx}")

if __name__ == "__main__":
    main()
