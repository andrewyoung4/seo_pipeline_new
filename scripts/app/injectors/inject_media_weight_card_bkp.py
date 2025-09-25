#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
inject_media_weight_card.py (with Responsive Coverage)
Adds KPIs:
- % IMG with srcset
- % IMG with sizes
- % IMG with width & height
- Count of CSS background images found

Also keeps the original weight/format KPIs + table of heaviest legacy images.
"""
import argparse, csv, re, sys, statistics, html
from pathlib import Path

CARD_START = "<!--[MEDIA_CARD_BEGIN]-->"
CARD_END = "<!--[MEDIA_CARD_END]-->"

def _read_csv(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows

def _fmt_kb(b): return f"{round(b/1024.0)} KB"
def _fmt_mb(b): return f"{round(b/1024.0/1024.0, 2)} MB"

def _build_table(rows, limit=10):
    head = "<tr><th>Image</th><th>Size</th><th>Type</th><th>Example page</th></tr>"
    body = []
    for r in rows[:limit]:
        img = html.escape(r["image_url"])
        size = _fmt_kb(int(r["bytes"] or 0))
        m = html.escape(r["mime"] or "")
        page = html.escape(r["pages_one"] or "")
        body.append(f"<tr><td class='cell-url'><a href='{img}' target='_blank'>{img}</a></td><td>{size}</td><td>{m}</td><td class='cell-url'><a href='{page}' target='_blank'>{page}</a></td></tr>")
    return f"<table class='sp-table'>{head}{''.join(body)}</table>"

def _kpis_images(rows):
    if not rows: return 0,0,0.0,0.0
    unique = len(rows)
    total_bytes = sum(int(r.get("bytes") or 0) for r in rows)
    sizes = [int(r.get("bytes") or 0) for r in rows if r.get("bytes")]
    median_kb = round(statistics.median(sizes)/1024.0,1) if sizes else 0.0
    nextgen = sum(1 for r in rows if str(r.get("next_gen","0")) in ("1", "True", "true"))
    rate = round((nextgen/unique)*100.0,1) if unique else 0.0
    return unique, total_bytes, median_kb, rate

def _responsive_kpis(onpage_rows):
    imgs = [r for r in onpage_rows if r.get("tag_type")=="img"]
    css = [r for r in onpage_rows if r.get("tag_type")=="css_bg"]
    n = len(imgs)
    if n == 0: return 0,0,0,0,0
    srcset = sum(1 for r in imgs if str(r.get("has_srcset")) in ("1","True","true"))
    sizes = sum(1 for r in imgs if str(r.get("has_sizes")) in ("1","True","true"))
    wh = sum(1 for r in imgs if str(r.get("has_wh")) in ("1","True","true"))
    return n, round(100*srcset/n,1), round(100*sizes/n,1), round(100*wh/n,1), len(css)

def build_card(img_rows, onpage_rows):
    # heaviest legacy images (jpeg/png/gif)
    legacy = [r for r in img_rows if (r.get("mime","") in ("image/jpeg","image/png","image/gif"))]
    legacy.sort(key=lambda r: int(r.get("bytes") or 0), reverse=True)
    table_html = _build_table(legacy, limit=10)

    unique, total_bytes, median_kb, rate = _kpis_images(img_rows)
    n_img, p_srcset, p_sizes, p_wh, css_count = _responsive_kpis(onpage_rows)

    css = (
        "<style>"
        ".sp-card{border-radius:16px;padding:16px;background:var(--sp-bg,#F2F1F4);box-shadow:0 1px 3px rgba(0,0,0,.06)}"
        ".sp-card .kpis{display:grid;grid-template-columns:repeat(6,minmax(120px,1fr));gap:12px;margin-bottom:12px}"
        ".kpi-title{font-size:.85rem;color:#666}.kpi-value{font-size:1.25rem;font-weight:700}"
        ".table-wrap{overflow:auto;max-width:100%}"
        ".sp-table{width:100%;border-collapse:collapse;table-layout:fixed}"
        ".sp-table th,.sp-table td{border-bottom:1px solid #e5e7eb;padding:8px 10px;text-align:left;font-size:.9rem;vertical-align:top}"
        ".cell-url{word-break:break-word;overflow-wrap:anywhere;white-space:normal}"
        "@media print{.table-wrap{overflow:visible;max-width:none} a[href]:after{content:'' !important}}"
        "</style>"
    )

    return f"""{CARD_START}
<div class="card sp-card">
  <div class="card-header">
    <h2>Media Weight, Formats &amp; Responsive Coverage</h2>
    <p class="muted">Largest images, next‑gen rate, and responsive markup audit</p>
  </div>
  <div class="card-body">
    <div class="kpis">
      <div class="kpi"><div class="kpi-title">Images (unique)</div><div class="kpi-value">{unique}</div></div>
      <div class="kpi"><div class="kpi-title">Total bytes</div><div class="kpi-value">{_fmt_mb(total_bytes)}</div></div>
      <div class="kpi"><div class="kpi-title">Median size</div><div class="kpi-value">{median_kb} KB</div></div>
      <div class="kpi"><div class="kpi-title">WebP+AVIF rate</div><div class="kpi-value">{rate}%</div></div>
      <div class="kpi"><div class="kpi-title">IMG with srcset</div><div class="kpi-value">{p_srcset}%</div></div>
      <div class="kpi"><div class="kpi-title">IMG with sizes</div><div class="kpi-value">{p_sizes}%</div></div>
      <div class="kpi"><div class="kpi-title">IMG with width/height</div><div class="kpi-value">{p_wh}%</div></div>
      <div class="kpi"><div class="kpi-title">CSS background images</div><div class="kpi-value">{css_count}</div></div>
    </div>
    <div class="table-wrap">{table_html}</div>
    <details class="muted"><summary>What counts as “responsive” here?</summary>
      <ul>
        <li><b>srcset</b>: &lt;img srcset="..."> present</li>
        <li><b>sizes</b>: &lt;img sizes="..."> present (recommended with srcset)</li>
        <li><b>width/height</b>: intrinsic dimensions set on &lt;img> to prevent CLS</li>
        <li>Shopify size hints (e.g., <code>_600x</code> or <code>?width=600</code>) are captured in <code>media_onpage.csv</code></li>
      </ul>
    </details>
  </div>
</div>
{CARD_END}
{css}
"""

def _inject(html_text, card_html):
    html_text = re.sub(r"<!--\[MEDIA_CARD_BEGIN\]-->.*?<!--\[MEDIA_CARD_END\]-->", "", html_text, flags=re.S)
    m = re.search(r"</h1>", html_text, flags=re.I)
    if m:
        idx = m.end()
        return html_text[:idx] + "\n" + card_html + "\n" + html_text[idx:]
    m2 = re.search(r"<body[^>]*>", html_text, flags=re.I)
    if m2:
        idx = m2.end()
        return html_text[:idx] + "\n" + card_html + "\n" + html_text[idx:]
    return html_text + "\n" + card_html

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True)
    ap.add_argument("--media-csv", required=True, help="media_images.csv")
    ap.add_argument("--onpage-csv", required=True, help="media_onpage.csv")
    args = ap.parse_args()

    img_rows = _read_csv(args.media_csv)
    onpage_rows = _read_csv(args.onpage_csv)
    if not img_rows:
        print("[warn] no media rows; nothing to inject", file=sys.stderr); return

    html_path = Path(args.report)
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    new_html = _inject(html, build_card(img_rows, onpage_rows))
    html_path.write_text(new_html, encoding="utf-8")
    print("[done] Media card injected.")

if __name__ == "__main__":
    main()
