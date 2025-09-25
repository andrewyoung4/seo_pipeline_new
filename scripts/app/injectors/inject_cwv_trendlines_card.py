#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, re, html as HTML, pandas as pd

def derive_classes(doc: str):
    # Borrow classes from nearby cards so it looks native
    m = re.search(r"(<section[^>]+>\s*<h2[^>]*>\s*Keyword Information\s*</h2[^>]*>)", doc, flags=re.I) \
        or re.search(r"(<section[^>]+>\s*<h2[^>]*>\s*Query Cannibalization\s*</h2[^>]*>)", doc, flags=re.I)
    section_class = "sp-card"
    if m:
        open_tag = re.search(r"<section[^>]+", m.group(1), flags=re.I).group(0)
        cm = re.search(r'class="([^"]+)"', open_tag, flags=re.I)
        if cm: section_class = cm.group(1)
    kpi_class   = "sp-kpi"    if re.search(r"\.sp-kpi\b", doc) else "kpi"
    list_class  = "sp-list"   if re.search(r"\.sp-list\b", doc) else "list-plain"
    return section_class, kpi_class, list_class

def build_svg_series(xs, ys, width=280, height=48, pad=6, invert=False):
    if not xs or not ys or len(xs)!=len(ys): return ""
    # normalize x to 0..1 by index
    n = len(xs)
    xs_n = [i/(n-1 if n>1 else 1) for i in range(n)]
    # normalize y to 0..1
    y_min = min(ys); y_max = max(ys)
    if y_min == y_max: y_max = y_min + (1 if y_min==0 else abs(y_min)*0.1)
    def sx(x): return pad + x*(width-2*pad)
    def sy(y):
        t = (y - y_min) / (y_max - y_min)
        if invert: t = 1-t
        return pad + t*(height-2*pad)
    pts = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x,y in zip(xs_n, ys))
    return f'<svg width="{width}" height="{height}" preserveAspectRatio="xMidYMid meet"><polyline fill="none" stroke="currentColor" stroke-opacity="0.6" stroke-width="2" points="{pts}"/></svg>'

def build_card(df: pd.DataFrame, title: str, classes):
    section_class, kpi_class, list_class = classes
    if df.empty:
        body = '<div class="sub label">No CrUX field data available yet. Add PSI_API_KEY and re-run the collector.</div>'
        return f'<section class="{section_class}" data-injected="cwv-trendlines"><h2>{HTML.escape(title)}</h2>{body}</section>'

    tabs = []
    for tpl, sub in df.groupby("template"):
        sub = sub.sort_values("month")
        months = sub["month"].tolist()
        lcp = [int(x) if pd.notna(x) else None for x in sub["lcp_p75_ms"].tolist()]
        cls = [float(x) if pd.notna(x) else None for x in sub["cls_p75"].tolist()]
        inp = [int(x) if pd.notna(x) else None for x in sub["inp_p75_ms"].tolist()]
        # filter Nones
        def filt(arr): 
            return [a for a in arr if a is not None]
        lcp_svg = build_svg_series(months, filt(lcp)) if any(x is not None for x in lcp) else ""
        cls_svg = build_svg_series(months, filt(cls), invert=True) if any(x is not None for x in cls) else ""
        inp_svg = build_svg_series(months, filt(inp)) if any(x is not None for x in inp) else ""
        tab = f"""
<div class="cwv-tab" data-template="{HTML.escape(tpl)}">
  <div class="cwv-row">
    <div class="{kpi_class}"><div class="label">LCP p75</div><div class="value">{(lcp[-1] if lcp and lcp[-1] is not None else '—')}</div>{lcp_svg}</div>
    <div class="{kpi_class}"><div class="label">CLS p75</div><div class="value">{(f"{cls[-1]:.2f}" if cls and cls[-1] is not None else '—')}</div>{cls_svg}</div>
    <div class="{kpi_class}"><div class="label">INP p75</div><div class="value">{(inp[-1] if inp and inp[-1] is not None else '—')}</div>{inp_svg}</div>
  </div>
  <div class="footnote">Months: {", ".join(months)}</div>
</div>""".strip()
        tabs.append(tab)
    tabs_html = "\n".join(tabs)

    # simple tabs (CSS-only; first tab visible)
    css = """
<style>
.cwv-tabs{margin-top:6px}
.cwv-tab{display:none}
.cwv-tab:first-of-type{display:block}
.cwv-tab .value{font-size:20px;font-weight:600;margin-bottom:4px}
.cwv-row{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;align-items:end}
.cwv-buttons{display:flex;gap:8px;margin-bottom:6px;flex-wrap:wrap}
.cwv-buttons button{border:1px solid rgba(0,0,0,.15);border-radius:10px;padding:4px 8px;background:transparent;cursor:pointer}
@media (prefers-color-scheme: dark){
  .cwv-buttons button{border-color:rgba(255,255,255,.18)}
}
</style>
<script>
document.addEventListener('click', function(e){
  const b = e.target.closest('[data-cwv-tab]');
  if(!b) return;
  const wrap = b.closest('[data-cwv-wrap]');
  wrap.querySelectorAll('.cwv-tab').forEach(el=>el.style.display='none');
  const name = b.getAttribute('data-cwv-tab');
  wrap.querySelector(`.cwv-tab[data-template="${name}"]`).style.display='block';
});
</script>
""".strip()

    buttons = "".join(f'<button data-cwv-tab="{HTML.escape(t)}">{HTML.escape(t)}</button>' for t in df["template"].unique())
    return f"""
<section class="{section_class}" data-injected="cwv-trendlines" data-cwv-wrap>
  <h2>{HTML.escape(title)}</h2>
  {css}
  <div class="cwv-buttons">{buttons}</div>
  <div class="cwv-tabs">
    {tabs_html}
  </div>
</section>""".strip()

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
    ap.add_argument("--trendlines-csv", required=True)
    ap.add_argument("--title", default="Core Web Vitals — Trendlines (Field)")
    args = ap.parse_args()

    df = pd.read_csv(args.trendlines_csv) if os.path.exists(args.trendlines_csv) else pd.DataFrame()
    with open(args.html_in, "r", encoding="utf-8", errors="ignore") as f: doc = f.read()
    card = build_card(df, args.title, derive_classes(doc))
    out = inject(doc, "\n"+card+"\n")
    os.makedirs(os.path.dirname(os.path.abspath(args.html_out)), exist_ok=True)
    with open(args.html_out, "w", encoding="utf-8") as f: f.write(out)
    print("Injected CWV Trendlines →", args.html_out)

if __name__ == "__main__":
    main()
