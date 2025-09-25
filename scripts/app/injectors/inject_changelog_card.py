#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
inject_changelog_card.py
Client-friendly change log (Δ since previous report).
"""
import argparse, csv, json, re, sys, statistics, html
from pathlib import Path

CARD_START = "<!--[CHANGELOG_CARD_BEGIN]-->"
CARD_END = "<!--[CHANGELOG_CARD_END]-->"

def _read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _try_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _calc_schema_cov(schema_csv):
    if not schema_csv: return None
    try:
        with open(schema_csv, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            rows = list(r)
        if not rows: return 0.0
        total = len(rows)
        with_jsonld = sum(1 for row in rows if str(row.get("has_product_jsonld","0")).lower() in ("1","true","yes"))
        return round(100.0 * with_jsonld / total, 1)
    except Exception:
        return None

def _calc_refdomains(ref_csv):
    if not ref_csv: return None
    try:
        with open(ref_csv, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            rows = list(r)
        if not rows: return 0
        # summary field?
        lower = {k.lower(): k for k in rows[0].keys()}
        if "domains_count" in lower:
            k = lower["domains_count"]
            v = _try_float(rows[0].get(k, ""))
            return int(v) if v is not None else None
        # else unique domain count
        for cand in ("domain","refdomain","referring_domain","root_domain"):
            if cand in lower:
                k = lower[cand]
                return len({(row.get(k) or "").strip().lower() for row in rows if row.get(k)})
        return len(rows)
    except Exception:
        return None

def _calc_indexed(index_csv):
    if not index_csv: return None
    try:
        with open(index_csv, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            rows = list(r)
        if not rows: return 0
        lower = {k.lower(): k for k in rows[0].keys()}
        if "indexed_count" in lower:
            k = lower["indexed_count"]
            v = _try_float(rows[0].get(k, ""))
            return int(v) if v is not None else None
        return len(rows)
    except Exception:
        return None

def _calc_cwv_strict(cwv_csv):
    if not cwv_csv: return None
    try:
        with open(cwv_csv, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            rows = list(r)
        if not rows: return 0.0
        passes = 0
        for row in rows:
            low = {k.lower(): k for k in row.keys()}
            key = None
            for cand in ("pass","passes","strict_pass","status","all_good"):
                if cand in low:
                    key = low[cand]; break
            val = (row.get(key,"") if key else "").strip().lower()
            if val in ("pass","passed","1","true","yes"):
                passes += 1
        return round(100.0 * passes / len(rows), 1)
    except Exception:
        return None

def _calc_lighthouse(lh_json):
    if not lh_json: return None
    data = _read_json(lh_json)
    if not data: return None
    if isinstance(data, dict):
        if "performance" in data and isinstance(data["performance"], (int,float)):
            v = float(data["performance"])
            return round(v if v<=100 else v*100, 1) if v<=1 else round(v,1)
        try:
            v = float(data["categories"]["performance"]["score"])
            return round(v*100,1) if v<=1 else round(v,1)
        except Exception:
            return None
    return None

def _load_prev_kpis(prev_snapshot, prev_report):
    if prev_snapshot:
        js = _read_json(prev_snapshot) or {}
        return js
    # Light HTML fallback: deltas will be "—" if not provided.
    return {}

def _delta(cur, prev):
    if cur is None or prev is None: return "—", ""
    diff = round(cur - prev, 1)
    sign = "▲" if diff > 0 else ("▼" if diff < 0 else "—")
    s = f"{'+' if diff>0 else ''}{diff}"
    return sign, s

def _build_card(cur, prev):
    css = (
        "<style>"
        ".sp-card{border-radius:16px;padding:16px;background:var(--sp-bg,#F2F1F4);box-shadow:0 1px 3px rgba(0,0,0,.06)}"
        ".sp-grid{display:grid;grid-template-columns:repeat(5,minmax(160px,1fr));gap:12px}"
        ".kpi{background:#fff;border-radius:12px;padding:12px;box-shadow:0 1px 2px rgba(0,0,0,.05)}"
        ".kpi-title{font-size:.85rem;color:#555} .kpi-value{font-size:1.25rem;font-weight:700} .kpi-delta{font-size:.85rem;color:#555}"
        ".up{color:#0a7b38}.down{color:#b91c1c}.flat{color:#6b7280}"
        "@media print{a[href]:after{content:'' !important}}"
        "</style>"
    )
    def kpi(label, cur_v, prev_v, suffix=""):
        sign, diff = _delta(cur_v, prev_v)
        cls = "up" if sign=="▲" else ("down" if sign=="▼" else "flat")
        cur_txt = "—" if cur_v is None else (f"{cur_v}{suffix}" if suffix and isinstance(cur_v,(int,float)) else f"{cur_v}")
        prev_txt = "—" if prev_v is None else (f"{prev_v}{suffix}" if suffix and isinstance(prev_v,(int,float)) else f"{prev_v}")
        delta_txt = f"<span class='{cls}'>{sign} {diff}{suffix}</span>" if sign!="—" else f"<span class='{cls}'>—</span>"
        return f"<div class='kpi'><div class='kpi-title'>{html.escape(label)}</div><div class='kpi-value'>{cur_txt}</div><div class='kpi-delta'>Prev: {prev_txt}&nbsp;&nbsp;{delta_txt}</div></div>"
    grid = "".join([
        kpi("Lighthouse (perf)", cur.get("lighthouse"), prev.get("lighthouse"), "%"),
        kpi("CWV Strict pass", cur.get("cwv_strict_pct"), prev.get("cwv_strict_pct"), "%"),
        kpi("JSON‑LD coverage", cur.get("jsonld_cov_pct"), prev.get("jsonld_cov_pct"), "%"),
        kpi("Referring domains", cur.get("ref_domains"), prev.get("ref_domains"), ""),
        kpi("Indexed pages", cur.get("indexed_pages"), prev.get("indexed_pages"), ""),
    ])
    return f"""{CARD_START}
<div class="card sp-card">
  <div class="card-header">
    <h2>Change Log since last report</h2>
    <p class="muted">KPIs compared with the previous run</p>
  </div>
  <div class="card-body">
    <div class="sp-grid">{grid}</div>
  </div>
</div>
{CARD_END}
{css}
"""

def _inject(html_text, card_html):
    html_text = re.sub(r"<!--\[CHANGELOG_CARD_BEGIN\]-->.*?<!--\[CHANGELOG_CARD_END\]-->", "", html_text, flags=re.S)
    m = re.search(r"</body>", html_text, flags=re.I)
    if m:
        idx = m.start()
        return html_text[:idx] + card_html + html_text[idx:]
    m2 = re.search(r"</h1>", html_text, flags=re.I)
    if m2:
        idx = m2.end()
        return html_text[:idx] + "\n" + card_html + "\n" + html_text[idx:]
    return html_text + "\n" + card_html

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True)
    ap.add_argument("--prev-snapshot")
    ap.add_argument("--schema-csv")
    ap.add_argument("--refdomains-csv")
    ap.add_argument("--indexed-csv")
    ap.add_argument("--cwv-csv")
    ap.add_argument("--lh-json")
    ap.add_argument("--snapshot-out", default=r".\data\outputs\phase4\changelog_snapshot.json")
    args = ap.parse_args()

    cur = {
        "lighthouse": _calc_lighthouse(args.lh_json),
        "cwv_strict_pct": _calc_cwv_strict(args.cwv_csv),
        "jsonld_cov_pct": _calc_schema_cov(args.schema_csv),
        "ref_domains": _calc_refdomains(args.refdomains_csv),
        "indexed_pages": _calc_indexed(args.indexed_csv),
    }
    prev = _read_json(args.prev_snapshot) or {}

    html_path = Path(args.report)
    html_text = html_path.read_text(encoding="utf-8", errors="ignore")
    new_html = _inject(html_text, _build_card(cur, prev))
    html_path.write_text(new_html, encoding="utf-8")

    out = Path(args.snapshot_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(cur, f, ensure_ascii=False, indent=2)
    print("[done] Change Log card injected; snapshot written:", out)

if __name__ == "__main__":
    main()
