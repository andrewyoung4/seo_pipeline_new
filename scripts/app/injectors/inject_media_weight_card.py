#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
inject_media_weight_card.py
Adds an explicit empty-state when there are no legacy offenders.
"""
import argparse, csv, statistics, html, re
from pathlib import Path

CARD_START = "<!--[MEDIA_CARD_BEGIN]-->"
CARD_END = "<!--[MEDIA_CARD_END]-->"

def _read_rows(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                r["bytes"] = int(r.get("bytes") or 0)
            except Exception:
                r["bytes"] = 0
            r["next_gen"] = 1 if str(r.get("next_gen", "0")).lower() in ("1","true","yes") else 0
            r["mime"] = (r.get("mime") or "").lower()
            r["image_url"] = r.get("image_url") or ""
            r["pages_one"] = r.get("pages_one") or ""
            rows.append(r)
    return rows

def _fmt_kb(b): return f"{round(b/1024.0)} KB"
def _fmt_mb(b): return f"{round(b/1024.0/1024.0, 2)} MB"
def _empty_state(msg="No action necessary."): return f"<p class='muted' style='margin:8px 0 0'>{html.escape(msg)}</p>"

def _build_table(rows, limit=10):
    head = "<tr><th>Image</th><th>Size</th><th>Type</th><th>Example page</th></tr>"
    body = []
    for r in rows[:limit]:
        img = html.escape(r["image_url"])
        size = _fmt_kb(r["bytes"])
        m = html.escape(r["mime"])
        page = html.escape(r["pages_one"])
        body.append(f"<tr><td class='cell-url'><a href='{img}' target='_blank'>{img}</a></td><td>{size}</td><td>{m}</td><td class='cell-url'><a href='{page}' target='_blank'>{page}</a></td></tr>")
    return f"<table class='sp-table'>{head}{''.join(body)}</table>"

def _kpis(rows):
    if not rows:
        return 0,0,0.0,0.0
    unique_imgs = len(rows)
    total_bytes = sum(r["bytes"] for r in rows)
    sizes = [r["bytes"] for r in rows if r["bytes"]>0]
    median_kb = round(statistics.median(sizes)/1024.0,1) if sizes else 0.0
    nextgen = sum(1 for r in rows if r["next_gen"]==1)
    rate = round((nextgen/unique_imgs)*100.0,1) if unique_imgs else 0.0
    return unique_imgs, total_bytes, median_kb, rate

def build_card(rows):
    legacy = [r for r in rows if r["mime"] in ("image/jpeg","image/png","image/gif")]
    legacy.sort(key=lambda r: r["bytes"], reverse=True)
    unique_imgs, total_bytes, median_kb, rate = _kpis(rows)
    css = (
        "<style>"
        ".sp-card{border-radius:16px;padding:16px;background:#F2F1F4;box-shadow:0 1px 3px rgba(0,0,0,.06)}"
        ".sp-card .kpis{display:grid;grid-template-columns:repeat(4,minmax(120px,1fr));gap:12px;margin-bottom:12px}"
        ".kpi-title{font-size:.85rem;color:#666}.kpi-value{font-size:1.25rem;font-weight:700}"
        ".table-wrap{overflow:auto;max-width:100%}"
        ".sp-table{width:100%;border-collapse:collapse;table-layout:fixed}"
        ".sp-table th,.sp-table td{border-bottom:1px solid #e5e7eb;padding:8px 10px;text-align:left;font-size:.9rem;vertical-align:top}"
        ".cell-url{word-break:break-word;overflow-wrap:anywhere;white-space:normal}"
        "@media print{.table-wrap{overflow:visible;max-width:none} a[href]:after{content:'' !important}}"
        "</style>"
    )
    offenders_html = _build_table(legacy, limit=10) if legacy else _empty_state("All images are next-gen (WebP/AVIF); no action necessary.")
    return f"""{CARD_START}
<div class="card sp-card">
  <div class="card-header">
    <h2>Media Weight & Formats</h2>
    <p class="muted">Largest images and next-gen format coverage</p>
  </div>
  <div class="card-body">
    <div class="kpis">
      <div class="kpi"><div class="kpi-title">Images (unique)</div><div class="kpi-value">{unique_imgs}</div></div>
      <div class="kpi"><div class="kpi-title">Total bytes</div><div class="kpi-value">{_fmt_mb(total_bytes)}</div></div>
      <div class="kpi"><div class="kpi-title">Median size</div><div class="kpi-value">{median_kb} KB</div></div>
      <div class="kpi"><div class="kpi-title">WebP+AVIF rate</div><div class="kpi-value">{rate}%</div></div>
    </div>
    <div class="table-wrap">{offenders_html}</div>
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
    ap.add_argument("--media-csv", required=True)
    args = ap.parse_args()
    rows = _read_rows(args.media_csv)
    html_path = Path(args.report)
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    new_html = _inject(html, build_card(rows))
    html_path.write_text(new_html, encoding="utf-8")
    print("[done] Media card injected.")

if __name__ == "__main__":
    main()
