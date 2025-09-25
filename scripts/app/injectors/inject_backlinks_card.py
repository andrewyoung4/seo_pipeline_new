#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, re, html as HTML, pandas as pd

def derive_classes(doc: str):
    m = re.search(r"(<section[^>]+>\s*<h2[^>]*>\s*Keyword Information\s*</h2[^>]*>)", doc, flags=re.I) \
        or re.search(r"(<section[^>]+>\s*<h2[^>]*>\s*Query Cannibalization\s*</h2[^>]*>)", doc, flags=re.I)
    section_class = "sp-card"
    if m:
        open_tag = re.search(r"<section[^>]+", m.group(1), flags=re.I).group(0)
        cm = re.search(r'class="([^"]+)"', open_tag, flags=re.I)
        if cm: section_class = cm.group(1)
    kpi_class   = "sp-kpi"    if re.search(r"\.sp-kpi\b", doc) else "kpi"
    grid3_class = "sp-grid-3" if re.search(r"\.sp-grid-3\b", doc) else "grid-3"
    list_class  = "sp-list"   if re.search(r"\.sp-list\b", doc) else "list-plain"
    return section_class, kpi_class, grid3_class, list_class

def build_card(df: pd.DataFrame, title: str, classes):
    section_class, kpi_class, grid3_class, list_class = classes
    empty = df.empty or df["domain"].dropna().nunique() == 0

    if empty:
        body = (
            f'<div class="sub label" style="margin-top:4px">'
            f'No backlink data available yet for this property. '
            f'Add a CSV later (GSC “Top linking sites”, Moz, Ahrefs, or a simple domain,count file) and this card will populate automatically.'
            f'</div>'
        )
        kpis = ""
        items = "<li><span>—</span></li>"
        total_domains = 0
        total_links = 0
    else:
        df2 = df.copy()
        if "links_total" in df2.columns:
            df2["links_total"] = pd.to_numeric(df2["links_total"], errors="coerce").fillna(0).astype(int)
        else:
            df2["links_total"] = 1
        total_domains = int(df2["domain"].nunique())
        total_links = int(df2["links_total"].sum())
        top = (df2.groupby("domain", as_index=False)["links_total"].sum()
                    .sort_values("links_total", ascending=False).head(5))
        items = "".join(
            f"<li><span>{HTML.escape(r.domain)}</span><strong>{int(r.links_total):,}</strong></li>"
            for r in top.itertuples()
        )
        body = f'<div class="sub label" style="margin-top:6px">Top linking domains</div><ul class="{list_class}">{items}</ul>'

    kpis_html = "" if empty else f"""
  <div class="{grid3_class}">
    <div class="{kpi_class}"><div class="label">Referring domains</div><div class="value">{total_domains:,}</div></div>
    <div class="{kpi_class}"><div class="label">Total links (sum)</div><div class="value">{total_links:,}</div></div>
    <div class="{kpi_class}"><div class="label">Quality signal</div><div class="value">—</div></div>
  </div>
"""

    return f"""
<section class="{section_class}" data-injected="backlinks-snapshot">
  <h2>{HTML.escape(title)}</h2>
  {kpis_html}
  {body}
  <div class="footnote">Source: normalized CSV (GSC/Moz/Ahrefs/manual).</div>
</section>
""".strip()

def inject(doc: str, card_html: str) -> str:
    anchors = [
        (r"(<h2[^>]*>\s*Keyword Information\s*</h2[^>]*>)","before"),
        (r"(<h2[^>]*>\s*Query Cannibalization\s*</h2[^>]*>)","after"),
        (r"(</h1\s*>)","after"),
        (r"(</body\s*>)","before"),
    ]
    for pat, mode in anchors:
        m = re.search(pat, doc, flags=re.I)
        if not m: continue
        return (doc[:m.start(1)] + card_html + doc[m.start(1):]) if mode=="before" else (doc[:m.end(1)] + card_html + doc[m.end(1):])
    return doc + card_html

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--html-in", required=True)
    ap.add_argument("--html-out", required=True)
    ap.add_argument("--refdomains-csv", required=True)
    ap.add_argument("--title", default="Backlinks Snapshot")
    args = ap.parse_args()

    df = pd.read_csv(args.refdomains_csv) if os.path.exists(args.refdomains_csv) else pd.DataFrame(columns=["domain","links_total"])
    with open(args.html_in, "r", encoding="utf-8", errors="ignore") as f:
        doc = f.read()
    classes = derive_classes(doc)
    card = build_card(df, args.title, classes)
    out = inject(doc, "\n"+card+"\n")
    os.makedirs(os.path.dirname(os.path.abspath(args.html_out)), exist_ok=True)
    with open(args.html_out, "w", encoding="utf-8") as f:
        f.write(out)
    print("Injected Backlinks Snapshot →", args.html_out)

if __name__ == "__main__":
    main()
