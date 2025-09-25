#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
inject_variant_duplicates_card.py
"Variant URL Duplicates" — Shopify-specific hygiene
Reads Phase-1 triage CSV and injects a card near Other Mismatches.
"""
import argparse, csv, re, sys, html
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

CARD_START = "<!--[VARIANT_CARD_BEGIN]-->"
CARD_END = "<!--[VARIANT_CARD_END]-->"

PARAM_DROP = set([
    "variant","variant_id","view","size","color","colour","option","options","option1","option2","option3",
    "utm_source","utm_medium","utm_campaign","utm_term","utm_content","gclid","fbclid","mc_eid","mc_cid","ref","referral","code"
])

def _detect_columns(header):
    cols = {c.lower(): c for c in header}
    def get(*names):
        for n in names:
            if n in cols:
                return cols[n]
        return None
    return {
        "url": get("url","final_url","normalized_url","loc","address"),
        "canonical": get("canonical","canonical_url","rel=canonical","rel_canonical","declared_canonical"),
        "cluster": get("cluster_id","dupe_cluster","duplicate_key","group_id")
    }

def _norm_host(host):
    h = host.lower()
    return h[4:] if h.startswith("www.") else h

def _strip_params(query):
    if not query:
        return ""
    kept = []
    for k,v in parse_qsl(query, keep_blank_values=True):
        kl = k.lower()
        if kl in PARAM_DROP or kl.startswith("utm_"):
            continue
        kept.append((k,v))
    return urlencode(kept, doseq=True)

def _base_product(url):
    sp = urlsplit(url)
    path = sp.path
    path = re.sub(r"/variants/\d+", "", path, flags=re.I)
    m = re.search(r"(^|/)(products|product)/([^/]+)", path, flags=re.I)
    if m:
        path = f"/products/{m.group(3)}"
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    norm = urlunsplit((sp.scheme.lower(), _norm_host(sp.netloc), path, _strip_params(sp.query), ""))
    return norm

def _is_variantish(url):
    if "?variant=" in url.lower() or "/variants/" in url.lower():
        return True
    sp = urlsplit(url)
    q = dict(parse_qsl(sp.query, keep_blank_values=True))
    keys = {k.lower() for k in q.keys()}
    return bool(keys & PARAM_DROP)

def _status_for_cluster(urls, canonicals, base):
    cands = [c for c in canonicals if c]
    if not cands:
        return "Needs canonical", "No rel=canonical present in cluster"
    canon_norm = [_base_product(c) for c in cands]
    unique_targets = sorted(set(canon_norm))
    if len(unique_targets) > 1:
        return "Mismatch", "Mixed canonical targets in cluster"
    target = unique_targets[0]
    if target == _base_product(base):
        return "OK", "All canonicalize to base product"
    return "Mismatch", "Canonical points to non-base URL"

def _read_rows(path):
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        cols = _detect_columns(reader.fieldnames or [])
    if not cols["url"]:
        raise SystemExit("Could not find a URL-like column in triage CSV (looked for url/final_url/normalized_url/loc).")
    return rows, cols

def _group_clusters(rows, cols):
    groups = {}
    for r in rows:
        u = r.get(cols["url"],"").strip()
        if not u.startswith("http"):
            continue
        cluster_id = r.get(cols["cluster"]) if cols["cluster"] else ""
        base = _base_product(u)
        key = (cluster_id or "") + "||" + base
        entry = groups.get(key)
        if not entry:
            entry = {"base": base, "rows": []}
            groups[key] = entry
        entry["rows"].append(r)
    return list(groups.values())

def _exemplar_row(base, rows, cols):
    urls = [r.get(cols["url"],"") for r in rows]
    cvals = [r.get(cols["canonical"] or "", "") for r in rows]
    status, reason = _status_for_cluster(urls, cvals, base)
    canon_target = ""
    for c in cvals:
        if c:
            canon_target = _base_product(c)
            break
    if not canon_target:
        canon_target = base
    ex = ""
    for u in urls:
        if _is_variantish(u) and _base_product(u) == base:
            ex = u; break
    if not ex and urls:
        ex = urls[0]
    return {
        "base": base,
        "canonical_target": canon_target,
        "example_dupe": ex,
        "status": status,
        "reason": reason,
        "count": len(urls)
    }

def _build_card(exemplars, totals):
    css = (
        "<style>"
        ".sp-card{border-radius:16px;padding:16px;background:var(--sp-bg,#F2F1F4);box-shadow:0 1px 3px rgba(0,0,0,.06)}"
        ".sp-card .kpis{display:grid;grid-template-columns:repeat(4,minmax(120px,1fr));gap:12px;margin-bottom:12px}"
        ".kpi-title{font-size:.85rem;color:#666}.kpi-value{font-size:1.25rem;font-weight:700}"
        ".table-wrap{overflow:auto;max-width:100%}"
        ".sp-table{width:100%;border-collapse:collapse;table-layout:fixed}"
        ".sp-table th,.sp-table td{border-bottom:1px solid #e5e7eb;padding:8px 10px;text-align:left;font-size:.9rem;vertical-align:top}"
        ".cell-url{word-break:break-word;overflow-wrap:anywhere;white-space:normal}"
        ".status-ok{color:#0a7b38;font-weight:600}.status-mismatch{color:#b91c1c;font-weight:600}.status-needs{color:#92400e;font-weight:600}"
        "@media print{.table-wrap{overflow:visible;max-width:none} a[href]:after{content:'' !important}}"
        "</style>"
    )
    head = "<tr><th>Base product</th><th>Canonical target</th><th>Example dupe</th><th>Status</th><th>Reason</th><th>URLs</th></tr>"
    body = []
    for ex in exemplars:
        status = ex["status"]
        cls = "status-ok" if status=="OK" else ("status-needs" if status.startswith("Needs") else "status-mismatch")
        body.append(
            "<tr>"
            f"<td class='cell-url'><a href='{html.escape(ex['base'])}' target='_blank'>{html.escape(ex['base'])}</a></td>"
            f"<td class='cell-url'><a href='{html.escape(ex['canonical_target'])}' target='_blank'>{html.escape(ex['canonical_target'])}</a></td>"
            f"<td class='cell-url'><a href='{html.escape(ex['example_dupe'])}' target='_blank'>{html.escape(ex['example_dupe'])}</a></td>"
            f"<td class='{cls}'>{html.escape(status)}</td>"
            f"<td>{html.escape(ex['reason'])}</td>"
            f"<td>{ex['count']}</td>"
            "</tr>"
        )
    table = f"<table class='sp-table'>{head}{''.join(body)}</table>"
    return f"""{CARD_START}
<div class="card sp-card">
  <div class="card-header">
    <h2>Variant URL Duplicates</h2>
    <p class="muted">Shopify variant/parameter duplicates and canonical consistency</p>
  </div>
  <div class="card-body">
    <div class="kpis">
      <div class="kpi"><div class="kpi-title">Duplicate clusters</div><div class="kpi-value">{totals['clusters']}</div></div>
      <div class="kpi"><div class="kpi-title">URLs in clusters</div><div class="kpi-value">{totals['urls_in_clusters']}</div></div>
      <div class="kpi"><div class="kpi-title">All OK</div><div class="kpi-value">{totals['ok_clusters']}</div></div>
      <div class="kpi"><div class="kpi-title">With issues</div><div class="kpi-value">{totals['issue_clusters']}</div></div>
    </div>
    <div class="table-wrap">{table}</div>
    <details class="muted"><summary>What this checks</summary>
      <ul>
        <li>Variant/query duplicates: <code>?variant=</code>, <code>/variants/123</code>, <code>?view=</code>, option params, and UTM clutter.</li>
        <li>Canonical consistency: all URLs in a cluster should declare the same <code>rel=canonical</code>, ideally the base product (no variant params).</li>
      </ul>
    </details>
  </div>
</div>
{CARD_END}
{css}
"""

def _inject(html_text, card_html):
    import re
    html_text = re.sub(r"<!--\[VARIANT_CARD_BEGIN\]-->.*?<!--\[VARIANT_CARD_END\]-->", "", html_text, flags=re.S)
    m = re.search(r"(Other Mismatches|Other&nbsp;Mismatches)</h2>", html_text, flags=re.I)
    if m:
        idx = m.end()
        return html_text[:idx] + "\n" + card_html + "\n" + html_text[idx:]
    m2 = re.search(r"</h1>", html_text, flags=re.I)
    if m2:
        idx = m2.end()
        return html_text[:idx] + "\n" + card_html + "\n" + html_text[idx:]
    return html_text + "\n" + card_html

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True)
    ap.add_argument("--triage-csv", required=True)
    ap.add_argument("--limit", type=int, default=10)
    args = ap.parse_args()

    rows, cols = _read_rows(args.triage_csv)
    groups = _group_clusters(rows, cols)

    exemplars = []
    totals = {"clusters":0, "urls_in_clusters":0, "ok_clusters":0, "issue_clusters":0}
    for g in groups:
        urls = [r.get(cols["url"],"") for r in g["rows"]]
        if len(urls) <= 1 or not any(_is_variantish(u) for u in urls):
            continue
        totals["clusters"] += 1
        totals["urls_in_clusters"] += len(urls)
        ex = _exemplar_row(g["base"], g["rows"], cols)
        if ex["status"] == "OK":
            totals["ok_clusters"] += 1
        else:
            totals["issue_clusters"] += 1
        exemplars.append(ex)

    exemplars.sort(key=lambda e: (0 if e["status"]!="OK" else 1, -e["count"]))
    exemplars = exemplars[: args.limit]

    html_path = Path(args.report)
    html_text = html_path.read_text(encoding="utf-8", errors="ignore")
    new_html = _inject(html_text, _build_card(exemplars, totals))
    html_path.write_text(new_html, encoding="utf-8")
    print("[done] Variant URL Duplicates card injected.")

if __name__ == "__main__":
    main()
