
import argparse, pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch

def main():
    ap = argparse.ArgumentParser(description="Generate an Executive Summary PDF (KPIs, deltas, competitor snapshot)")
    ap.add_argument("--phase2-xlsx", required=True)
    ap.add_argument("--phase3-xlsx", required=True)
    ap.add_argument("--out-pdf", required=True)
    args = ap.parse_args()

    kpis = {}
    try:
        kpi_df = pd.read_excel(args.phase2_xlsx, sheet_name="KPI_Summary")
        if not kpi_df.empty:
            kpis = kpi_df.iloc[0].to_dict()
    except Exception:
        pass

    comp_df = pd.read_excel(args.phase3_xlsx, sheet_name="Competitor_Scores")
    top = comp_df.head(8).fillna("")

    c = canvas.Canvas(args.out_pdf, pagesize=letter)
    width, height = letter
    y = height - 1*inch

    def write_line(text, size=12, dy=14):
        nonlocal y
        c.setFont("Helvetica-Bold" if size>=14 else "Helvetica", size)
        c.drawString(1*inch, y, text)
        y -= dy

    write_line("SEO Executive Summary", size=18, dy=22)
    write_line("KPIs", size=14, dy=18)
    for k in ["gsc_clicks","gsc_impressions","avg_position","total_backlinks","ref_domains","moz_da_median","moz_linking_domains_sum"]:
        if k in kpis:
            write_line(f"• {k}: {kpis[k]}", size=11, dy=14)

    write_line("", size=12, dy=10)
    write_line("Top Competitors", size=14, dy=18)
    cols = [c for c in ["domain","serp_hits","domain_authority","linking_domains","backlinks"] if c in top.columns]
    for _, row in top.iterrows():
        bits = [str(row.get(col,"")) for col in cols]
        write_line("• " + " | ".join(bits), size=11, dy=14)

    c.showPage()
    c.save()
    print(f"Wrote {args.out_pdf}")

if __name__ == "__main__":
    main()
