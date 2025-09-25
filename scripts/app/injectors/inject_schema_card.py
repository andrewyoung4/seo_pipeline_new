#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
inject_schema_card.py
Adds empty-state when there are no product rows or no fixes needed.
"""
import argparse, csv, html, re
from pathlib import Path

CARD_START = "<!--[SCHEMA_CARD_BEGIN]-->"
CARD_END = "<!--[SCHEMA_CARD_END]-->"

def _read_rows(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows

def _mk_table(rows, limit=12):
    head = "<tr><th>URL</th><th>Eligible</th><th>Completeness</th><th>Missing (required)</th><th>Missing (recommended)</th></tr>"
    body = []
    for r in rows[:limit]:
        url = html.escape(r.get("url",""))
        elig = "Yes" if str(r.get("eligibile_rich_results","0")).lower() in ("1","true") else "No"
        try:
            comp = f"{int(float(r.get('completeness_pct',0)))}%"
        except Exception:
            comp = "0%"
        miss_req = html.escape(r.get("missing_required","") or "")
        miss_rec = html.escape(r.get("missing_recommended","") or "")
        body.append("<tr>"
            f"<td class='cell-url'><a href='{url}' target='_blank'>{url}</a></td>"
            f"<td>{elig}</td><td>{comp}</td>"
            f"<td class='cell-wrap'>{miss_req}</td>"
            f"<td class='cell-wrap'>{miss_rec}</td></tr>")
    return f"<table class='sp-table'>{head}{''.join(body)}</table>"

def build_card(rows, table_rows, limit):
    css = (
        "<style>"
        ".sp-card{border-radius:16px;padding:16px;background:#F2F1F4;box-shadow:0 1px 3px rgba(0,0,0,.06)}"
        ".sp-card .kpis{display:grid;grid-template-columns:repeat(5,minmax(120px,1fr));gap:12px;margin-bottom:12px}"
        ".kpi-title{font-size:.85rem;color:#666}.kpi-value{font-size:1.25rem;font-weight:700}"
        ".table-wrap{overflow:auto;max-width:100%}"
        ".sp-table{width:100%;border-collapse:collapse;table-layout:fixed}"
        ".sp-table th,.sp-table td{border-bottom:1px solid #e5e7eb;padding:8px 10px;text-align:left;font-size:.9rem;vertical-align:top}"
        ".sp-table .cell-url,.sp-table .cell-wrap{word-break:break-word;overflow-wrap:anywhere;white-space:normal}"
        ".sp-table a{word-break:break-word;overflow-wrap:anywhere}"
        "@media print{.table-wrap{overflow:visible;max-width:none} a[href]:after{content:'' !important}}"
        "</style>"
    )
    if not rows:
        return f"""{CARD_START}
<div class="card sp-card"><div class="card-header"><h2>Schema Completeness & Rich Results</h2></div>
<div class="card-body"><p class='muted'>No product pages detected in the dataset; no action necessary.</p></div></div>
{CARD_END}
{css}"""
    table_html = _mk_table(table_rows, limit=limit) if table_rows else "<p class='muted'>Nothing to fix — all listed products meet requirements; no action necessary.</p>"
    return f"""{CARD_START}
<div class="card sp-card"><div class="card-header"><h2>Schema Completeness & Rich Results</h2><p class="muted">Product JSON-LD coverage and eligibility snapshot</p></div>
<div class="card-body"><div class="table-wrap">{table_html}</div></div></div>
{CARD_END}
{css}"""

def _inject(html_text, card_html):
    html_text = re.sub(r"<!--\[SCHEMA_CARD_BEGIN\]-->.*?<!--\[SCHEMA_CARD_END\]-->", "", html_text, flags=re.S)
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
    ap.add_argument("--schema-csv", required=True)
    ap.add_argument("--table-limit", type=int, default=12)
    ap.add_argument("--products-only", action="store_true")
    ap.add_argument("--include-eligible", type=int, default=1)
    ap.add_argument("--include-ineligible", type=int, default=1)
    args = ap.parse_args()

    rows = _read_rows(args.schema_csv)
    table_rows = rows
    if args.products_only:
        table_rows = [r for r in table_rows if str(r.get("has_product_jsonld","0")).lower() in ("1","true")]
    if not args.include_eligible:
        table_rows = [r for r in table_rows if str(r.get("eligibile_rich_results","0")).lower() not in ("1","true")]
    if not args.include_ineligible:
        table_rows = [r for r in table_rows if str(r.get("eligibile_rich_results","0")).lower() in ("1","true")]
    html_path = Path(args.report)
    html_text = html_path.read_text(encoding="utf-8", errors="ignore")
    new_html = _inject(html_text, build_card(rows, table_rows, args.table_limit))
    html_path.write_text(new_html, encoding="utf-8")
    print("[done] Schema card injected.")

if __name__ == "__main__":
    main()
