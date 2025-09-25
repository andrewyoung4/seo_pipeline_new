#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import re, html, argparse

def read_csv_safe(p: Path) -> pd.DataFrame:
    if not p or not p.exists():
        return pd.DataFrame()
    for enc in ("utf-8","utf-8-sig","latin1"):
        try:
            return pd.read_csv(p, encoding=enc)
        except Exception:
            continue
    return pd.DataFrame()

def norm_serp(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame()
    cols = {c.lower(): c for c in df.columns}
    q = cols.get("query") or cols.get("keyword") or cols.get("term")
    url = cols.get("url") or cols.get("page") or cols.get("landing_page")
    pos = cols.get("position") or cols.get("rank") or cols.get("avg_position") or cols.get("serp_position")
    date = cols.get("date") or cols.get("snapshot") or cols.get("dt")
    vol = cols.get("volume") or cols.get("search_volume") or cols.get("sv")
    out = df.copy()
    if q: out = out.rename(columns={q:"query"})
    if url: out = out.rename(columns={url:"url"})
    if pos: out = out.rename(columns={pos:"position"})
    if date: out = out.rename(columns={date:"date"})
    if vol: out = out.rename(columns={vol:"volume"})
    if "position" in out: out["position"] = pd.to_numeric(out["position"], errors="coerce")
    if "volume" in out: out["volume"] = pd.to_numeric(out["volume"], errors="coerce")
    return out

def norm_gsc(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame()
    cols = {c.lower(): c for c in df.columns}
    q = cols.get("query") or cols.get("keyword") or cols.get("term")
    clicks = cols.get("clicks") or cols.get("click")
    imps = cols.get("impressions") or cols.get("impr") or cols.get("impressions_sum")
    ctr = cols.get("ctr") or cols.get("click_through_rate")
    pos = cols.get("position") or cols.get("avg_position")
    out = df.copy()
    if q: out = out.rename(columns={q:"query"})
    if clicks: out = out.rename(columns={clicks:"clicks"})
    if imps: out = out.rename(columns={imps:"impressions"})
    if ctr: out = out.rename(columns={ctr:"ctr"})
    if pos: out = out.rename(columns={pos:"position"})
    for c in ["clicks","impressions","ctr","position"]:
        if c in out: out[c] = pd.to_numeric(out[c], errors="coerce")
    return out

def fmt(x):
    if x is None: return "–"
    try:
        xf = float(x)
        if xf.is_integer():
            return f"{int(xf):,}"
        return f"{xf:,.1f}"
    except Exception:
        return html.escape(str(x))

def html_table(df: pd.DataFrame, headers: dict=None) -> str:
    if df is None or df.empty:
        return '<div class="mini">No data available.</div>'
    safe = df.copy()
    for c in safe.columns:
        if safe[c].dtype == object:
            safe[c] = safe[c].astype(str).map(lambda v: html.escape(v))
    if headers:
        safe = safe.rename(columns=headers)
    cols = list(safe.columns)
    thead = "".join(f"<th>{html.escape(h)}</th>" for h in cols)
    rows = []
    for _, r in safe.iterrows():
        tds = "".join(f"<td>{'' if pd.isna(r[c]) else r[c]}</td>" for c in cols)
        rows.append(f"<tr>{tds}</tr>")
    return (
        '<table class="tbl">'
        '<thead><tr>' + thead + '</tr></thead>'
        '<tbody>' + "".join(rows) + '</tbody>'
        '</table>'
    )

def build_block(serp: pd.DataFrame, gsc: pd.DataFrame, origin: str) -> str:
    serp = norm_serp(serp)
    gsc = norm_gsc(gsc)

    mapped_keywords = int(serp["query"].nunique()) if "query" in serp else 0
    with_pos = int(serp.dropna(subset=["position"]).shape[0]) if "position" in serp else 0
    avg_rank = float(serp["position"].mean()) if "position" in serp and serp["position"].notna().any() else None

    clicks = int(gsc["clicks"].sum()) if "clicks" in gsc else 0
    impressions = int(gsc["impressions"].sum()) if "impressions" in gsc else 0
    avg_gsc_pos = float(gsc["position"].mean()) if "position" in gsc and gsc["position"].notna().any() else None

    top_gsc = pd.DataFrame()
    if not gsc.empty:
        keep = [c for c in ["query","clicks","impressions","ctr","position"] if c in gsc.columns]
        sort_cols = [c for c in ["impressions","clicks"] if c in keep]
        top_gsc = gsc[keep]
        if sort_cols:
            top_gsc = top_gsc.sort_values(by=sort_cols, ascending=False).head(10)
        else:
            top_gsc = top_gsc.head(10)

    top_serp = pd.DataFrame()
    if not serp.empty and "query" in serp.columns:
        tmp = serp.dropna(subset=["position"]) if "position" in serp else serp
        if not tmp.empty:
            aggs = {"position":"min"}
            if "url" in tmp.columns: aggs["url"] = "first"
            top_serp = (tmp.groupby("query", as_index=False)
                        .agg(aggs)
                        .sort_values("position", ascending=True)
                        .head(10))

    overview = (
        '<div class="grid" style="margin-top:4px">'
        f'<div class="kpi"><div class="label">Mapped Keywords</div><div class="value">{fmt(mapped_keywords)}</div></div>'
        f'<div class="kpi"><div class="label">SERP Rows</div><div class="value">{fmt(with_pos)}</div></div>'
        f'<div class="kpi"><div class="label">Avg Rank (SERP)</div><div class="value">{fmt(avg_rank)}</div></div>'
        f'<div class="kpi"><div class="label">Clicks (GSC)</div><div class="value">{fmt(clicks)}</div></div>'
        f'<div class="kpi"><div class="label">Impressions (GSC)</div><div class="value">{fmt(impressions)}</div></div>'
        f'<div class="kpi"><div class="label">Avg Pos (GSC)</div><div class="value">{fmt(avg_gsc_pos)}</div></div>'
        '</div>'
    )

    block = (
        '<div class="card" style="margin-top:12px">'
        '<h2>Keyword Information</h2>'
        + overview +
        '<div class="grid2" style="margin-top:8px">'
          '<div>'
            '<h3>Top Opportunities (GSC)</h3>'
            + html_table(top_gsc, headers={"query":"Query","clicks":"Clicks","impressions":"Impr.","ctr":"CTR","position":"Avg Pos"}) +
          '</div>'
          '<div>'
            '<h3>Best Rankings (SERP)</h3>'
            + html_table(top_serp, headers={"query":"Keyword","position":"Best Rank","url":"URL"}) +
          '</div>'
        '</div>'
        '</div>'
    )
    return block

def _insert_block(html_text: str, block: str) -> str:
    # 1) Prefer after subtitle if present
    m = re.search(r'(?is)(<p class="sub">.*?</p>)', html_text)
    if m:
        return html_text[:m.end()] + "\n" + block + html_text[m.end():]
    # 2) Else after the first </h1>
    m = re.search(r'(?is)(</h1>)', html_text)
    if m:
        return html_text[:m.end()] + "\n" + block + html_text[m.end():]
    # 3) Else right after <body>
    m = re.search(r'(?is)(<body[^>]*>)', html_text)
    if m:
        return html_text[:m.end()] + "\n" + block + html_text[m.end():]
    # 4) Else just before </body> (order intact)
    m = re.search(r'(?is)(</body>)', html_text)
    if m:
        return html_text[:m.start()] + block + "\n" + html_text[m.start():]
    # 5) Fallback: append at end (never before DOCTYPE)
    return html_text + "\n" + block

def _sanitize_final_html(html_text: str) -> str:
    # Ensure nothing precedes DOCTYPE
    m = re.search(r'(?is)<!DOCTYPE html', html_text)
    if m and m.start() > 0:
        html_text = html_text[m.start():]
    # Remove any legacy Keyword Tracking (v6) card
    html_text = re.sub(r'(?is)<div\\s+class="card"[^>]*>\\s*<h2>\\s*Keyword\\s+Tracking.*?</h2>.*?</div>', '', html_text)
    # Replace internal subtitle with client copy (only if a subtitle exists)
    html_text = re.sub(r'(?is)\\s*<p class="sub">.*?</p>',
                       '<p class="sub">Monthly SEO performance overview</p>',
                       html_text, count=1)
    # Remove "Generated by ..." footer if present
    html_text = re.sub(r'(?is)\\s*<p class="mini"[^>]*>\\s*Generated by[^<]*</p>', '', html_text, count=1)
    return html_text

def inject(html_text: str, block: str) -> str:
    # Remove any existing 'Keyword Information' card to avoid duplicates
    html_text = re.sub(r'(?is)<div\\s+class="card"[^>]*>\\s*<h2>\\s*Keyword\\s+Information\\s*</h2>.*?</div>', '', html_text)
    return _insert_block(html_text, block)

def main():
    ap = argparse.ArgumentParser(description="Inject v6 Keyword data into existing report HTML (robust insertion + cleanup)")
    ap.add_argument("--html", required=True, help="Path to existing client_report_pro.html")
    ap.add_argument("--serp-samples", required=True, help="Path to serp_samples.csv")
    ap.add_argument("--gsc", required=False, default=None, help="Path to gsc_queries.csv (optional)")
    ap.add_argument("--origin", required=False, default="", help="Origin (for copy)")
    args = ap.parse_args()

    html_path = Path(args.html)
    if not html_path.exists():
        raise SystemExit(f"HTML not found: {html_path}")

    serp_df = read_csv_safe(Path(args.serp_samples))
    gsc_df = read_csv_safe(Path(args.gsc)) if args.gsc else pd.DataFrame()

    base_html = html_path.read_text(encoding="utf-8", errors="ignore")
    block = build_block(serp_df, gsc_df, args.origin)
    final_html = inject(base_html, block)
    final_html = re.sub(r"(<title>Silent Princess — Client Report \\()v5_6k(\\))", r"\\1v5_6k+v6\\2", final_html, count=1)
    final_html = _sanitize_final_html(final_html)
    html_path.write_text(final_html, encoding="utf-8")
    print("Injected v6 Keyword Information into:", str(html_path))

if __name__ == "__main__":
    main()
