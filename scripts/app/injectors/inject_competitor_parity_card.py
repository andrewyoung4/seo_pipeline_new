#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
inject_competitor_parity_card.py (strict v2)
"""
import argparse, csv, re, html
from pathlib import Path
from urllib.parse import urlparse
from collections import defaultdict

CARD_START = "<!--[PARITY_CARD_BEGIN]-->"
CARD_END = "<!--[PARITY_CARD_END]-->"

DEFAULT_EXCLUDES = {
    "pinterest.com","www.pinterest.com",
    "facebook.com","www.facebook.com","m.facebook.com",
    "youtube.com","www.youtube.com","m.youtube.com",
    "twitter.com","x.com","www.twitter.com","www.x.com",
    "instagram.com","www.instagram.com",
    "amazon.com","www.amazon.com",
    "etsy.com","www.etsy.com",
    "wikipedia.org","en.wikipedia.org","www.wikipedia.org",
    "reddit.com","www.reddit.com",
    "ebay.com","www.ebay.com"
}

def _det_cols(cols):
    low = {c.lower(): c for c in cols}
    def get(*names):
        for n in names:
            if n in low:
                return low[n]
        return None
    return {
        "query": get("query","q","keyword","search_term"),
        "url": get("url","result_url","link","final_url"),
        "domain": get("domain","host"),
        "rank": get("rank","position","pos")
    }

def _domain_from_url(u):
    try:
        host = urlparse(u).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""

def _read_serp(path):
    rows = []
    with open(path,"r",encoding="utf-8") as f:
        r = csv.DictReader(f)
        cols = _det_cols(r.fieldnames or [])
        for row in r:
            url = (row.get(cols["url"] or "", "") or "").strip()
            if not url.startswith("http"):
                continue
            dom = (row.get(cols["domain"] or "", "") or "").strip().lower() or _domain_from_url(url)
            rank_raw = (row.get(cols["rank"] or "", "") or "").strip()
            try:
                rank = int(float(rank_raw)) if rank_raw != "" else None
            except Exception:
                rank = None
            rows.append({
                "query": (row.get(cols["query"] or "", "") or "").strip(),
                "url": url, "domain": dom, "rank": rank
            })
    return rows

def _read_domains_txt(path):
    s = set()
    if not path: return s
    try:
        with open(path,"r",encoding="utf-8") as f:
            for line in f:
                line=line.strip().lower()
                if line and not line.startswith("#"):
                    if line.startswith("www."):
                        line = line[4:]
                    s.add(line)
    except Exception:
        pass
    return s

def _empty_state(msg="No action necessary."):
    return f"<p class='muted' style='margin:8px 0 0'>{html.escape(msg)}</p>"

def _bars_table(items):
    head = "<tr><th>Domain</th><th>SoV%</th><th>Top-3 SoV%</th></tr>"
    body = []
    for d, s, s3 in items:
        bar_w = max(0, min(100, s))
        bar_w3 = max(0, min(100, s3))
        body.append(
            "<tr>"
            f"<td>{html.escape(d)}</td>"
            f"<td><div style='background:#eee;border-radius:6px;overflow:hidden'><div style='height:10px;width:{bar_w}%;background:#3b82f6'></div></div> {s:.1f}%</td>"
            f"<td><div style='background:#eee;border-radius:6px;overflow:hidden'><div style='height:10px;width:{bar_w3}%;background:#8b5cf6'></div></div> {s3:.1f}%</td>"
            "</tr>"
        )
    return f"<table class='sp-table'>{head}{''.join(body)}</table>"

def _apply_query_excludes(rows, substrs):
    if not substrs:
        return rows
    out = []
    lowers = [s.lower() for s in substrs]
    for r in rows:
        ql = (r["query"] or "").lower()
        if any(s in ql for s in lowers):
            continue
        out.append(r)
    return out

def _cap_per_domain_per_query(rows, cap):
    if cap <= 0:
        return rows
    out = []
    per_qd = {}
    for r in rows:
        key = (r["query"], r["domain"])
        c = per_qd.get(key, 0)
        if c >= cap:
            continue
        per_qd[key] = c + 1
        out.append(r)
    return out

def build_card(rows, top_n, exclude_platforms, extra_excludes, allowlist, min_hits, max_per_q, exclude_q_substr):
    if not rows:
        return f"""{CARD_START}
<div class="card sp-card"><div class="card-header"><h2>Competitor Parity</h2></div>
<div class="card-body">{_empty_state("No SERP data available; no action necessary.")}</div></div>
{CARD_END}"""

    rows = _apply_query_excludes(rows, exclude_q_substr)

    excluded = set()
    if exclude_platforms:
        excluded |= DEFAULT_EXCLUDES
    excluded |= extra_excludes
    if allowlist:
        rows = [r for r in rows if r["domain"] in allowlist]
    rows = [r for r in rows if r["domain"] and r["domain"] not in excluded]

    rows = _cap_per_domain_per_query(rows, max_per_q)

    if not rows:
        return f"""{CARD_START}
<div class="card sp-card"><div class="card-header"><h2>Competitor Parity</h2></div>
<div class="card-body">{_empty_state("No rows after filters; adjust allow/exclude lists or query filters.")}</div></div>
{CARD_END}"""

    hits = defaultdict(int)
    top3_hits = defaultdict(int)
    queries = set()
    for r in rows:
        d = r["domain"]
        hits[d] += 1
        if r["rank"] is not None and r["rank"] <= 3:
            top3_hits[d] += 1
        if r["query"]:
            queries.add(r["query"])

    keep_domains = {d for d, c in hits.items() if c >= min_hits}
    rows = [r for r in rows if r["domain"] in keep_domains]
    if not rows:
        return f"""{CARD_START}
<div class="card sp-card"><div class="card-header"><h2>Competitor Parity</h2></div>
<div class="card-body">{_empty_state("All domains fell below the minimum presence threshold; try --min-hits 1.")}</div></div>
{CARD_END}"""

    hits = defaultdict(int)
    top3_hits = defaultdict(int)
    for r in rows:
        d = r["domain"]
        hits[d] += 1
        if r["rank"] is not None and r["rank"] <= 3:
            top3_hits[d] += 1

    total = sum(hits.values())
    total3 = sum(top3_hits.values()) or 1
    sov = {d: 100.0 * hits[d]/total for d in hits}
    sov3 = {d: 100.0 * top3_hits[d]/total3 for d in hits}
    top = sorted(hits.keys(), key=lambda d: sov[d], reverse=True)[:top_n]
    items = [(d, sov[d], sov3.get(d, 0.0)) for d in top]

    css = (
        "<style>"
        ".sp-card{border-radius:16px;padding:16px;background:#F2F1F4;box-shadow:0 1px 3px rgba(0,0,0,.06)}"
        ".sp-table{width:100%;border-collapse:collapse;table-layout:fixed;margin-top:8px}"
        ".sp-table th,.sp-table td{border-bottom:1px solid #e5e7eb;padding:8px 10px;text-align:left;font-size:.9rem;vertical-align:middle}"
        ".kpis{display:grid;grid-template-columns:repeat(4,minmax(120px,1fr));gap:12px;margin-bottom:8px}"
        ".kpi-title{font-size:.85rem;color:#666}.kpi-value{font-size:1.1rem;font-weight:700}"
        "</style>"
    )
    settings = []
    if allowlist: settings.append("allow-list ON")
    if exclude_platforms: settings.append("large platforms excluded")
    if extra_excludes: settings.append(f"{len(extra_excludes)} extra excludes")
    if max_per_q: settings.append(f"max {max_per_q}/domain/query")
    if min_hits: settings.append(f"min-hits ≥{min_hits}")
    if exclude_q_substr: settings.append("brand filters ON")

    kpis = (
        "<div class='kpis'>"
        f"<div><div class='kpi-title'>Queries</div><div class='kpi-value'>{len(queries)}</div></div>"
        f"<div><div class='kpi-title'>Domains kept</div><div class='kpi-value'>{len(hits)}</div></div>"
        f"<div><div class='kpi-title'>Rows</div><div class='kpi-value'>{sum(hits.values())}</div></div>"
        f"<div><div class='kpi-title'>Filters</div><div class='kpi-value'>{', '.join(settings) if settings else 'none'}</div></div>"
        "</div>"
    )
    table = _bars_table(items)
    note = "<p class='muted'>SoV% is computed over <b>all kept domains</b> after filters. Display shows top results only.</p>"
    return f"""{CARD_START}
<div class="card sp-card">
  <div class="card-header"><h2>Competitor Parity</h2>
    <p class="muted">Share of Voice across filtered competitors + Top-3 SoV</p>
  </div>
  <div class="card-body">{kpis}{table}{note}</div>
</div>
{CARD_END}{css}
"""

def _inject(html_text, card_html):
    html_text = re.sub(r"<!--\[PARITY_CARD_BEGIN\]-->.*?<!--\[PARITY_CARD_END\]-->", "", html_text, flags=re.S)
    m = re.search(r"(Competitor Parity|Parity)</h2>", html_text, flags=re.I)
    if m:
        idx = m.end(); return html_text[:idx] + "\n" + card_html + "\n" + html_text[idx:]
    m2 = re.search(r"</h1>", html_text, flags=re.I)
    if m2:
        idx = m2.end(); return html_text[:idx] + "\n" + card_html + "\n" + html_text[idx:]
    return html_text + "\n" + card_html

def _read_excludes(path):
    s = set()
    if not path: return s
    try:
        with open(path,"r",encoding="utf-8") as f:
            for line in f:
                line=line.strip().lower()
                if line and not line.startswith("#"):
                    if line.startswith("www."):
                        line = line[4:]
                    s.add(line)
    except Exception: pass
    return s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True)
    ap.add_argument("--serp-csv", required=True)
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--exclude-large-platforms", type=int, default=1)
    ap.add_argument("--exclude-domains-file")
    ap.add_argument("--allow-domains-file")
    ap.add_argument("--min-hits", type=int, default=2)
    ap.add_argument("--max-per-domain-per-query", type=int, default=1)
    ap.add_argument("--exclude-query-substr", action="append", default=[])
    args = ap.parse_args()

    rows = _read_serp(args.serp_csv)
    excludes = _read_excludes(args.exclude_domains_file)
    allow = _read_excludes(args.allow_domains_file)

    html_path = Path(args.report)
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    new_html = _inject(html, build_card(
        rows=rows,
        top_n=args.top,
        exclude_platforms=bool(args.exclude_large_platforms),
        extra_excludes=excludes,
        allowlist=allow,
        min_hits=max(1, args.min_hits),
        max_per_q=max(0, args.max_per_domain_per_query),
        exclude_q_substr=args.exclude_query_substr
    ))
    html_path.write_text(new_html, encoding="utf-8")
    print("[done] Competitor parity card injected.")
if __name__ == "__main__":
    main()
