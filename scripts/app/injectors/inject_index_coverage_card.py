#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, re, html as HTML, pandas as pd

def load_summary(csv_path, xlsx_path):
    if csv_path and os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    if xlsx_path and os.path.exists(xlsx_path):
        xls = pd.ExcelFile(xlsx_path)
        sheet = next((s for s in xls.sheet_names if s.lower().strip()=="gsc index summary"), 0)
        return pd.read_excel(xlsx_path, sheet_name=sheet)
    raise SystemExit("Could not find summary data.")

def _i(x): 
    try: return int(x)
    except: return 0

def build_card_html(df, title, section_class, kpi_class, grid3_class, list_class):
    base = df[df.get("metric","").notna()] if "metric" in df.columns else pd.DataFrame()
    total  = _i(base.loc[base["metric"]=="total_sampled_urls","value"].max()) if not base.empty else 0
    idxyes = _i(base.loc[base["metric"]=="indexed_estimate","value"].max()) if not base.empty else 0
    idxno  = max(total-idxyes, 0)
    pcti   = (idxyes/total*100.0) if total else 0.0
    pctn   = 100.0 - pcti if total else 0.0

    cov = df[(df.get("section","")== "coverage_state")].copy() if "section" in df.columns else pd.DataFrame()
    if not cov.empty and "name" not in cov.columns:
        name_col = next((c for c in cov.columns if "coverage" in c and "state" in c), None)
        if name_col: cov = cov.rename(columns={name_col:"name"})
    cov = cov[["name","count"]].dropna() if not cov.empty else pd.DataFrame(columns=["name","count"])
    cov["count"] = cov["count"].apply(_i)
    cov = cov.sort_values("count", ascending=False).head(6)

    li = "".join(f"<li><span>{HTML.escape(str(r['name']))}</span><strong>{_i(r['count'])}</strong></li>" for _, r in cov.iterrows()) or "<li><span>No coverage breakdown available.</span></li>"

    return f"""
<section class="{section_class}" data-injected="gsc-index-coverage">
  <h2>{HTML.escape(title)}</h2>
  <div class="{grid3_class}">
    <div class="{kpi_class}"><div class="label">Sampled URLs</div><div class="value">{total:,}</div></div>
    <div class="{kpi_class}"><div class="label">Indexed (est.)</div><div class="value">{idxyes:,} <span class="sub">({pcti:.1f}%)</span></div></div>
    <div class="{kpi_class}"><div class="label">Not Indexed (est.)</div><div class="value">{idxno:,} <span class="sub">({pctn:.1f}%)</span></div></div>
  </div>
  <div class="sub label" style="margin-top:6px">Top coverage states</div>
  <ul class="{list_class}">{li}</ul>
</section>
""".strip()

def derive_classes(around_html):
    # Try to copy the parent <section> class used by nearby cards
    m = re.search(r"(<section[^>]+>\s*<h2[^>]*>\s*Keyword Information\s*</h2[^>]*>)", around_html, flags=re.I)
    if not m:
        m = re.search(r"(<section[^>]+>\s*<h2[^>]*>\s*Query Cannibalization\s*</h2[^>]*>)", around_html, flags=re.I)
    section_class = "sp-card"
    if m:
        open_tag = re.search(r"<section[^>]+", m.group(1), flags=re.I).group(0)
        cm = re.search(r'class="([^"]+)"', open_tag, flags=re.I)
        if cm: section_class = cm.group(1)

    # Heuristics for inner classes (reuse if present in doc)
    kpi_class   = "sp-kpi"      if re.search(r"\.sp-kpi\b", around_html)   else "kpi"
    grid3_class = "sp-grid-3"   if re.search(r"\.sp-grid-3\b", around_html) else "grid-3"
    list_class  = "sp-list"     if re.search(r"\.sp-list\b", around_html)  else "list-plain"
    return section_class, kpi_class, grid3_class, list_class

def inject(doc, card):
    anchors = [
        (r"(<h2[^>]*>\s*Keyword Information\s*</h2[^>]*>)","before"),
        (r"(<h2[^>]*>\s*Query Cannibalization\s*</h2[^>]*>)","after"),
        (r"(</h1\s*>)","after"),
        (r"(</body\s*>)","before"),
    ]
    for pat, mode in anchors:
        m = re.search(pat, doc, flags=re.I)
        if not m: continue
        return (doc[:m.start(1)] + card + doc[m.start(1):]) if mode=="before" else (doc[:m.end(1)] + card + doc[m.end(1):])
    return doc + card

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--html-in", required=True)
    ap.add_argument("--html-out", required=True)
    ap.add_argument("--summary-csv")
    ap.add_argument("--phase2-xlsx")
    ap.add_argument("--title", default="GSC Index Coverage")
    args = ap.parse_args()

    df = load_summary(args.summary_csv, args.phase2_xlsx)
    doc = open(args.html_in, "r", encoding="utf-8", errors="ignore").read()
    section_class, kpi_class, grid3_class, list_class = derive_classes(doc)
    card = build_card_html(df, args.title, section_class, kpi_class, grid3_class, list_class)
    out = inject(doc, "\n"+card+"\n")
    os.makedirs(os.path.dirname(os.path.abspath(args.html_out)), exist_ok=True)
    open(args.html_out, "w", encoding="utf-8").write(out)
    print("Injected Index Coverage card →", args.html_out)

if __name__ == "__main__":
    main()
