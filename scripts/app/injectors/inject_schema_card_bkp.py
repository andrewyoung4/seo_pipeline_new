#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
scripts/app/injectors/inject_schema_card.py (+flags)
Adds CLI flags to control what the table shows:
  --table-limit N         # default 12
  --products-only         # only rows with has_product_jsonld == 1 (hides collections)
  --include-eligible 0/1  # default 1
  --include-ineligible 0/1# default 1
"""
import argparse, csv, re, sys, statistics, html
from pathlib import Path

CARD_START = "<!--[SCHEMA_CARD_BEGIN]-->"
CARD_END = "<!--[SCHEMA_CARD_END]-->"

def _read_rows(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            def to_int(x, default=0):
                try:
                    return int(float(x))
                except Exception:
                    return default
            r["completeness_pct"] = to_int(r.get("completeness_pct", 0))
            r["has_product_jsonld"] = to_int(r.get("has_product_jsonld", 0))
            r["eligibile_rich_results"] = to_int(r.get("eligibile_rich_results", 0))
            rows.append(r)
    return rows

def _mk_table(rows, limit=12):
    head = "<tr><th>URL</th><th>Eligible</th><th>Completeness</th><th>Missing (required)</th><th>Missing (recommended)</th></tr>"
    body = []
    for r in rows[:limit]:
        url = html.escape(r.get("url",""))
        elig = "Yes" if int(r.get("eligibile_rich_results",0)) else "No"
        comp = f"{int(r.get('completeness_pct',0))}%"
        miss_req = html.escape(r.get("missing_required",""))
        miss_rec = html.escape(r.get("missing_recommended",""))
        body.append(
            "<tr>"
            f"<td class='cell-url'><a href='{url}' target='_blank'>{url}</a></td>"
            f"<td>{elig}</td>"
            f"<td>{comp}</td>"
            f"<td class='cell-wrap'>{miss_req}</td>"
            f"<td class='cell-wrap'>{miss_rec}</td>"
            "</tr>"
        )
    return f"<table class='sp-table'>{head}{''.join(body)}</table>"

def _compute_summary(rows):
    n = len(rows)
    with_jsonld = sum(1 for r in rows if int(r.get("has_product_jsonld",0)))
    eligible = sum(1 for r in rows if int(r.get("eligibile_rich_results",0)))
    compl = [int(r.get("completeness_pct",0)) for r in rows if r.get("completeness_pct") is not None]
    avg = round(sum(compl)/len(compl),1) if compl else 0
    if len(compl) >= 4:
        q = statistics.quantiles(compl, n=4)
        p25, p75 = int(q[0]), int(q[-1])
    else:
        p25 = int(min(compl)) if compl else 0
        p75 = int(max(compl)) if compl else 0
    return n, with_jsonld, eligible, avg, p25, p75

def build_schema_card(rows, table_rows, limit):
    n, with_jsonld, eligible, avg, p25, p75 = _compute_summary(rows)
    table_html = _mk_table(table_rows, limit=limit)
    css_block = (
        "<style>"
        ".sp-card{border-radius:16px;padding:16px;background:var(--sp-bg,#F2F1F4);box-shadow:0 1px 3px rgba(0,0,0,.06)}"
        ".sp-card .kpis{display:grid;grid-template-columns:repeat(5,minmax(120px,1fr));gap:12px;margin-bottom:12px}"
        ".kpi-title{font-size:.85rem;color:#666}"
        ".kpi-value{font-size:1.25rem;font-weight:700}"
        ".table-wrap{overflow:auto;max-width:100%}"
        ".sp-table{width:100%;border-collapse:collapse;table-layout:fixed}"
        ".sp-table th,.sp-table td{border-bottom:1px solid #e5e7eb;padding:8px 10px;text-align:left;font-size:.9rem;vertical-align:top}"
        ".sp-table .cell-url,.sp-table .cell-wrap{word-break:break-word;overflow-wrap:anywhere;white-space:normal}"
        ".sp-table a{word-break:break-word;overflow-wrap:anywhere}"
        "@media print{"
        "  .table-wrap{overflow:visible;max-width:none}"
        "  a[href]:after{content:'' !important}"
        "  .sp-table th,.sp-table td{white-space:normal;word-break:break-word;overflow-wrap:anywhere}"
        "}"
        "</style>"
    )
    parts = [
        CARD_START,
        "<div class=\"card sp-card\">",
        "  <div class=\"card-header\">",
        "    <h2>Schema Completeness &amp; Rich&nbsp;Results</h2>",
        "    <p class=\"muted\">Product JSON-LD coverage and eligibility snapshot</p>",
        "  </div>",
        "  <div class=\"card-body\">",
        "    <div class=\"kpis\">",
        f"      <div class=\"kpi\"><div class=\"kpi-title\">Products scanned</div><div class=\"kpi-value\">{n}</div></div>",
        f"      <div class=\"kpi\"><div class=\"kpi-title\">Has Product JSON-LD</div><div class=\"kpi-value\">{with_jsonld}</div></div>",
        f"      <div class=\"kpi\"><div class=\"kpi-title\">Eligible for Rich Results</div><div class=\"kpi-value\">{eligible}</div></div>",
        f"      <div class=\"kpi\"><div class=\"kpi-title\">Completeness Avg</div><div class=\"kpi-value\">{avg}%</div></div>",
        f"      <div class=\"kpi\"><div class=\"kpi-title\">P25 / P75</div><div class=\"kpi-value\">{p25}% / {p75}%</div></div>",
        "    </div>",
        "    <div class=\"table-wrap\">",
        f"      {table_html}",
        "    </div>",
        "    <details class=\"muted\"><summary>How this is scored</summary>",
        "      <p>Required: name, image, offers.price, offers.priceCurrency. Recommended: availability, sku, brand, GTIN, rating/reviews. Eligibility = has name + image + price + currency.</p>",
        "    </details>",
        "  </div>",
        "</div>",
        CARD_END,
        css_block
    ]
    return "\n".join(parts)

def inject_schema_card(html_text: str, rows, table_rows, limit) -> str:
    card_html = build_schema_card(rows, table_rows, limit)
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
    ap.add_argument("--report", required=True, help="Path to report HTML")
    ap.add_argument("--schema-csv", required=True, help="Path to schema_product_report.csv")
    ap.add_argument("--table-limit", type=int, default=12)
    ap.add_argument("--products-only", action="store_true")
    ap.add_argument("--include-eligible", type=int, default=1)
    ap.add_argument("--include-ineligible", type=int, default=1)
    args = ap.parse_args()

    rows = _read_rows(args.schema_csv)
    if not rows:
        print("[warn] no rows to inject", file=sys.stderr)
        return

    # Filter for table view
    table_rows = rows
    if args.products_only:
        table_rows = [r for r in table_rows if int(r.get("has_product_jsonld",0)) == 1]
    # include eligibility filters
    if not args.include_eligible:
        table_rows = [r for r in table_rows if int(r.get("eligibile_rich_results",0)) == 0]
    if not args.include_ineligible:
        table_rows = [r for r in table_rows if int(r.get("eligibile_rich_results",0)) == 1]

    # sort by worst first
    table_rows = sorted(table_rows, key=lambda r: (r.get("eligibile_rich_results",0), r.get("completeness_pct",0)))

    html_path = Path(args.report)
    html_text = html_path.read_text(encoding="utf-8", errors="ignore")
    new_html = inject_schema_card(html_text, rows, table_rows, args.table_limit)
    html_path.write_text(new_html, encoding="utf-8")
    print("[done] Schema card injected.")

if __name__ == "__main__":
    main()
