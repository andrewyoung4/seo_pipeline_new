#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, re, html as HTML, pandas as pd

def derive_classes(doc: str):
    m = re.search(r"(<section[^>]+>\s*<h2[^>]*>\s*Competitor Parity\s*</h2[^>]*>)", doc, flags=re.I) \
        or re.search(r"(<section[^>]+>\s*<h2[^>]*>\s*Keyword Information\s*</h2[^>]*>)", doc, flags=re.I)
    section_class = "sp-card"
    if m:
        open_tag = re.search(r"<section[^>]+", m.group(1), flags=re.I).group(0)
        cm = re.search(r'class="([^"]+)"', open_tag, flags=re.I)
        if cm: section_class = cm.group(1)
    kpi_class   = "sp-kpi"    if re.search(r"\.sp-kpi\b", doc) else "kpi"
    grid3_class = "sp-grid-3" if re.search(r"\.sp-grid-3\b", doc) else "grid-3"
    list_class  = "sp-list"   if re.search(r"\.sp-list\b", doc) else "list-plain"
    return section_class, kpi_class, grid3_class, list_class

def build_card(sum_csv, opp_csv, title, classes):
    section_class, kpi_class, grid3_class, list_class = classes
    s = pd.read_csv(sum_csv) if os.path.exists(sum_csv) else pd.DataFrame(columns=["metric","value"])
    get = lambda k: int(s.loc[s["metric"]==k,"value"].max()) if (not s.empty and (s["metric"]==k).any()) else 0
    total_sampled = get("total_keywords_sampled")
    ours          = get("our_keywords_in_gsc")
    gap           = get("gap_keywords")

    opp = pd.read_csv(opp_csv) if os.path.exists(opp_csv) else pd.DataFrame(columns=["keyword","competitors_count","examples"])
    opp = opp.sort_values(["competitors_count","keyword"], ascending=[False, True]).head(8)

    items = "".join(
        f"<li><span>{HTML.escape(r.keyword)}</span>"
        f"<strong>×{int(r.competitors_count)}</strong> <em style='opacity:.7'>({HTML.escape(str(r.examples))})</em></li>"
        for r in opp.itertuples()
    ) or "<li><span>No gaps detected in this sample.</span></li>"

    return f"""
<section class="{section_class}" data-injected="keyword-gap">
  <h2>{HTML.escape(title)}</h2>
  <div class="{grid3_class}">
    <div class="{kpi_class}"><div class="label">Sampled keywords</div><div class="value">{total_sampled:,}</div></div>
    <div class="{kpi_class}"><div class="label">We rank for</div><div class="value">{ours:,}</div></div>
    <div class="{kpi_class}"><div class="label">Gap keywords</div><div class="value">{gap:,}</div></div>
  </div>
  <div class="sub label" style="margin-top:6px">Top gap opportunities</div>
  <ul class="{list_class}">{items}</ul>
  <div class="footnote">Source: competitor SERP sample vs our GSC queries.</div>
</section>
""".strip()

def inject(doc: str, card_html: str) -> str:
    anchors = [
        (r"(<h2[^>]*>\s*Competitor Parity\s*</h2[^>]*>)","after"),
        (r"(<h2[^>]*>\s*Keyword Information\s*</h2[^>]*>)","before"),
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
    ap.add_argument("--summary-csv", required=True)
    ap.add_argument("--opps-csv", required=True)
    ap.add_argument("--title", default="Keyword Gap")
    args = ap.parse_args()

    with open(args.html_in, "r", encoding="utf-8", errors="ignore") as f:
        doc = f.read()
    classes = derive_classes(doc)
    card = build_card(args.summary_csv, args.opps_csv, args.title, classes)
    out = inject(doc, "\n"+card+"\n")
    os.makedirs(os.path.dirname(os.path.abspath(args.html_out)), exist_ok=True)
    with open(args.html_out, "w", encoding="utf-8") as f:
        f.write(out)
    print("Injected Keyword Gap →", args.html_out)

if __name__ == "__main__":
    main()
