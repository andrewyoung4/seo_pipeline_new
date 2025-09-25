#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_client_report_pro_v5_6k.py — self-contained generator (brand + compact fixes)

Based on v5_6i. Adds:
- Brand skin (Sour Gummy + #323E28 / #2F95B1 / #F2F1F4 / #B6C6A9).
- CWV: cap INP culprits to top 6 with "+N more" note; compact p75 table.
- Auto full-width card when a grid has only one child (fixes 'Other Mismatches' whitespace).
- Print CSS cleanups.

Usage:
  python make_client_report_pro_v5_6k.py --phase4 path\\phase4_dashboard.xlsx --phase3 path\\phase3_report.xlsx --phase2 path\\phase2_report.xlsx --origin example.com --out out.html --debug
"""
import argparse, os, re, json, math
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from string import Template


# === Keyword Tracking helpers (auto-injected) ===
import pandas as _pd, numpy as _np

_MEGA_ = set("youtube.com reddit.com amazon.com pinterest.com facebook.com wikipedia.org instagram.com tiktok.com x.com twitter.com linkedin.com etsy.com ebay.com quora.com medium.com".split())

def _kt_norm(u: str) -> str:
    try:
        u = str(u or "")
        u = u.split("//",1)[-1]
        d = u.split("/",1)[0].lower()
        return d[4:] if d.startswith("www.") else d
    except: return ""

def _kt_load_serp(path: str):
    try:
        if not path: return _pd.DataFrame()
        p = Path(path)
        if not p.exists(): return _pd.DataFrame()
        df = _pd.read_csv(p, dtype=str)
        cols = {c.lower(): c for c in df.columns}
        need = {"keyword","rank","url"}
        if not need.issubset(set(cols.keys())): return _pd.DataFrame()
        df = df.rename(columns={cols.get("keyword"):"keyword", cols.get("rank"):"rank", cols.get("url"):"url"})
        df["rank"] = _pd.to_numeric(df["rank"], errors="coerce")
        fa = next((c for c in df.columns if c.lower()=="fetched_at"), None)
        df["fetched_at"] = _pd.to_datetime(df[fa], errors="coerce") if fa else _pd.NaT
        df["domain"] = df["url"].map(_kt_norm)
        return df.dropna(subset=["keyword","rank","domain"])
    except Exception:
        return _pd.DataFrame()

def _kt_load_gsc(path: str):
    try:
        if not path: return _pd.DataFrame()
        p = Path(path)
        if not p.exists(): return _pd.DataFrame()
        df = _pd.read_csv(p)
        lo = {c.lower(): c for c in df.columns}
        q = lo.get("query") or lo.get("top queries") or lo.get("search query") or lo.get("queries")
        i = lo.get("impressions") or lo.get("impr.") or lo.get("impr") or lo.get("total impressions")
        if not q or not i: return _pd.DataFrame()
        out = _pd.DataFrame({"query": df[q].astype(str),
                             "impressions": _pd.to_numeric(df[i], errors="coerce").fillna(0.0)})
        out["query_norm"] = out["query"].str.lower()
        return out
    except Exception:
        return _pd.DataFrame()

def _kt_fmt_int(v):
    try:
        if v is None or (isinstance(v,float) and (_np.isnan(v) or _np.isinf(v))): return "–"
        return f"{int(round(float(v))):,}"
    except: return "–"

def _kt_fmt_float(v, n=1):
    try:
        if v is None or (isinstance(v,float) and (_np.isnan(v) or _np.isinf(v))): return "–"
        return f"{float(v):.{n}f}"
    except: return "–"

def _kt_blocks_html(serp_df, gsc_df, origin: str) -> str:
    if serp_df is None: serp_df = _pd.DataFrame()
    if gsc_df is None: gsc_df = _pd.DataFrame()
    kt = _kt_block_tracking(serp_df, origin)
    gaps = _kt_block_gaps(serp_df, origin)
    sov = _kt_block_sov(serp_df, origin)
    top = _kt_block_opps(serp_df, gsc_df, origin)
    row1 = "<div class='grid'>" + "".join([b for b in [kt, gaps, sov] if b]) + "</div>"
    row2 = ("<div class='grid' style='margin-top:10px'>" + top + "</div>") if top else ""
    return row1 + row2

def _kt_block_tracking(df, origin: str) -> str:
    if df.empty: return ""
    you = _kt_norm(origin)
    d = df.copy()
    if not d["fetched_at"].notna().any():
        d["fetched_at"] = _pd.Timestamp.utcnow().normalize()
    latest = d["fetched_at"].max()
    d7  = latest - _pd.Timedelta(days=7)
    d30 = latest - _pd.Timedelta(days=30)
    cur = (d[d["domain"]==you]
           .sort_values(["keyword","rank","fetched_at"], ascending=[True,True,False])
           .groupby("keyword", as_index=False).first()[["keyword","rank"]]
           .rename(columns={"rank":"rank_now"}))
    def at_or_before(t0):
        w = d[(d["domain"]==you) & (d["fetched_at"]<=t0)]
        if w.empty: return _pd.DataFrame(columns=["keyword","rank"])
        w = (w.sort_values(["keyword","fetched_at","rank"], ascending=[True,False,True])
               .groupby("keyword", as_index=False).first()[["keyword","rank"]])
        return w
    r7  = at_or_before(d7).rename(columns={"rank":"rank_7d"})
    r30 = at_or_before(d30).rename(columns={"rank":"rank_30d"})
    out = cur.merge(r7, on="keyword", how="left").merge(r30, on="keyword", how="left")
    out["d7"]  = out["rank_now"] - out["rank_7d"]
    out["d30"] = out["rank_now"] - out["rank_30d"]
    out = out.sort_values(["rank_now","d7","d30"], na_position="last").head(25)
    def arrow(v):
        if _pd.isna(v): return ""
        v = float(v)
        return "↑" if v>0 else ("↓" if v<0 else "→")
    rows = []
    for _,r in out.iterrows():
        d7  = "" if _pd.isna(r["d7"])  else f"{arrow(r['d7'])}{abs(int(r['d7']))}"
        d30 = "" if _pd.isna(r["d30"]) else f"{arrow(r['d30'])}{abs(int(r['d30']))}"
        rows.append(f"<tr><td>{r['keyword']}</td><td>{_kt_fmt_int(r['rank_now'])}</td><td>{d7}</td><td>{d30}</td></tr>")
    table = "<table class='tbl'><tr><th>Keyword</th><th>Rank</th><th>Δ7d</th><th>Δ30d</th></tr>"+ "".join(rows) + "</table>"
    return f"<div class='card span6'><h2>Keyword Tracking</h2><div class='mini'>Latest: {latest.date() if _pd.notna(latest) else 'n/a'} (top 25)</div>{table}</div>"

def _kt_block_gaps(df, origin: str) -> str:
    if df.empty: return ""
    you = _kt_norm(origin)
    latest = df.sort_values("fetched_at").groupby("keyword", as_index=False).tail(10) if df["fetched_at"].notna().any() else df
    pres = latest.assign(is_you=lambda x: x["domain"]==you).groupby("keyword")["is_you"].any()
    gaps = pres[~pres].index.tolist()
    if not gaps: return ""
    rows = "".join([f"<tr><td>{k}</td></tr>" for k in gaps[:40]])
    more = f"<div class='mini'>+{len(gaps)-40} more</div>" if len(gaps)>40 else ""
    table = "<table class='tbl'><tr><th>Keyword</th></tr>"+rows+"</table>"
    return f"<div class='card span3'><h2>Coverage Gaps</h2>{table}{more}</div>"

def _kt_block_sov(df, origin: str, hide_megasites: bool=True) -> str:
    if df.empty: return ""
    you = _kt_norm(origin)
    latest_date = df["fetched_at"].max() if df["fetched_at"].notna().any() else None
    latest = df[df["fetched_at"]==latest_date] if latest_date is not None else df
    dom_counts = latest["domain"].value_counts()
    dom_counts = dom_counts[dom_counts.index.map(lambda d: d!=you)]
    total = int(dom_counts.sum()) or 1
    rows = []
    for d, c in dom_counts.items():
        if hide_megasites and d in _MEGA_: continue
        sov = 100.0 * c / total
        rows.append((d, c, sov))
    if not rows: return ""
    rows = rows[:12]
    table_rows = "".join([f"<tr><td>{d}</td><td>{_kt_fmt_int(c)}</td><td>{_kt_fmt_float(s,1)}%</td></tr>" for d,c,s in rows])
    table = "<table class='tbl'><tr><th>Domain</th><th>Hits</th><th>SoV%</th></tr>"+table_rows+"</table>"
    return f"<div class='card span3'><h2>Competitor SoV (SMB)</h2>{table}<div class='mini'>Latest{f' {latest_date.date()}' if latest_date else ''}; megasites hidden.</div></div>"

def _kt_block_opps(df_serp, df_gsc, origin: str) -> str:
    if df_serp.empty or df_gsc.empty: return ""
    you = _kt_norm(origin)
    s = df_serp.copy()
    s["domain"] = s["url"].map(_kt_norm)
    s["keyword_norm"] = s["keyword"].astype(str).str.lower()
    s["is_big"] = s["domain"].isin(_MEGA_)
    s["is_you"] = s["domain"].eq(you)
    agg = s.groupby("keyword_norm").agg(
        you_rank=("rank", lambda x: int(_pd.to_numeric(x, errors="coerce").min()) if (s.loc[x.index,'is_you']).any() else 99),
        big_share=("is_big","mean"),
        kw=("keyword","first")
    ).reset_index()
    agg["smb_share"] = 1.0 - agg["big_share"]
    g = df_gsc[["query_norm","impressions"]].copy()
    df = _pd.merge(agg, g, left_on="keyword_norm", right_on="query_norm", how="left").fillna({"impressions":0})
    def opp(row):
        imp = float(row["impressions"])
        rank_penalty = (100 - min(100, int(row["you_rank"]))) / 100.0
        smb_bonus = float(row["smb_share"])
        big_pen = 1.0 - float(row["big_share"])
        return imp * (0.4 * rank_penalty + 0.4 * smb_bonus + 0.2 * big_pen)
    df["OpportunityScore"] = df.apply(opp, axis=1)
    keep = df.sort_values("OpportunityScore", ascending=False).head(12)
    if keep.empty: return ""
    rows = "".join([
        f"<tr><td>{r.kw}</td><td>{_kt_fmt_int(r.impressions)}</td><td>{_kt_fmt_int(r.you_rank)}</td><td>{_kt_fmt_float(100*r.smb_share,0)}%</td><td>{_kt_fmt_float(r.OpportunityScore,1)}</td></tr>"
        for r in keep.itertuples(index=False)
    ])
    table = "<table class='tbl'><tr><th>Keyword</th><th>Impr.</th><th>Your Rank</th><th>SMB Share</th><th>Opp.</th></tr>"+rows+"</table>"
    return f"<div class='card span6'><h2>Top Opportunities</h2><div class='mini'>Demand (GSC) × Rank gap × SMB-friendliness</div>{table}</div>"

def _kt_inject(html: str, serp_samples_path: str|None, gsc_path: str|None, origin: str) -> str:
    serp = _kt_load_serp(serp_samples_path) if serp_samples_path else _pd.DataFrame()
    gsc  = _kt_load_gsc(gsc_path) if gsc_path else _pd.DataFrame()
    block = _kt_blocks_html(serp, gsc, origin)
    if not block: return html
    try:
        return re.sub(r'(?is)(<h1[^>]*>.*?</h1>)', r'\\1' + block, html, count=1)
    except Exception:
        return block + html
# === End Keyword Tracking helpers ===

# ---------- helpers (same as v5_6i) ----------
def _exists(p): return bool(p) and os.path.isfile(str(p))
def _num(s): return pd.to_numeric(s, errors="coerce")
def _fmt_int(v):
    try: return f"{int(round(float(v))):,}"
    except: return "–"
def _fmt_float(v, n=1):
    try: return f"{float(v):.{n}f}"
    except: return "–"
def _pct_text(v):
    try:
        x = float(v); 
        if x<=1.0: x*=100.0
        return f"{int(round(max(0,min(100,x))))}%"
    except: return "–"
def _pct_num(v):
    try:
        x = float(v); 
        if x<=1.0: x*=100.0
        return int(round(max(0,min(100,x))))
    except: return 0
def _truthy(series: pd.Series) -> pd.Series:
    if series.dtype == bool: return series.fillna(False)
    s = series.astype(str).str.strip().str.lower()
    return s.isin(["true","1","yes","y","t"])
def _top_segment(url: str) -> str:
    try:
        m = re.match(r'^https?://[^/]+/([^/?#]+)', str(url))
        seg = (m.group(1).lower() if m else "/")
        return "/" if seg in ("", "/") else seg
    except: return "/"
def _percentile(s: pd.Series, q: float) -> Optional[float]:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if not len(s): return None
    return float(np.percentile(s, q))
def _normalize_name(n: str) -> str:
    n = (n or "").lower().strip()
    n = n.replace("phase2 — ","").replace("phase2—","").replace("phase 2 — ","")
    n = re.sub(r'\s+', ' ', n)
    return n

def _load_excel(path: str, ns: str="") -> Dict[str,pd.DataFrame]:
    out = {}
    try:
        xl = pd.ExcelFile(path)
        for s in xl.sheet_names:
            try:
                df = xl.parse(s)
                df.columns = [str(c).strip() for c in df.columns]
                out[s] = df
            except Exception:
                pass
    except Exception:
        pass
    return out

def _get_sheet(d: Dict[str,pd.DataFrame], prefer: List[str]=None, contains: str=None) -> Optional[pd.DataFrame]:
    if not d: return None
    if prefer:
        targets = [_normalize_name(x) for x in prefer]
        for k,df in d.items():
            if _normalize_name(k) in targets:
                return df
    if contains:
        c = contains.lower()
        for k,df in d.items():
            if c in _normalize_name(k):
                return df
    return None

def brand_heuristic(origin: str) -> list:
    origin = (origin or "").lower()
    toks = re.sub(r'[^a-z0-9]+',' ', origin).split()
    bad = set(["com","net","org","co","uk","tt","www"])
    return [t for t in toks if t not in bad]

def _norm_url_basic(u: str) -> str:
    s = str(u or "")
    s = s.split("#",1)[0]
    s = s.split("?",1)[0]
    return s.rstrip("/")

# ---------- site metrics (same as v5_6i) ----------
def derive_site_metrics(ph4: Dict[str,pd.DataFrame]) -> dict:
    out = {
        "pages": None, "https_pct": None, "jsonld_pct": None, "avg_inlinks": None,
        "perf_lh": None, "cwv_strict_pct": None, "cwv_legacy_pct": None, "inp_good_pct": None,
        "lcp_p75": None, "inp_p75": None, "cls_p75": None,
        "issues": {"critical":0,"high":0,"medium":0,"low":0},
        "generated": None,
        "img_with_alt_pct": None,
        "status": {"2xx":0,"3xx":0,"4xx":0,"5xx":0},
    }
    dash = ph4.get("Dashboard")
    if dash is not None and not dash.empty:
        try:
            m = dash.stack().map(lambda x: isinstance(x,str) and "last generated" in x.lower())
            if m.any():
                row_idx = int(m[m].index[0][0])
                if row_idx+1 < len(dash):
                    val = dash.iloc[row_idx+1].dropna().tolist()[-1]
                    out["generated"] = str(val)
        except Exception:
            pass

    dk = ph4.get("Derived_KPIs")
    if dk is not None and not dk.empty:
        if "Performance (LH)" in dk.columns:
            out["perf_lh"] = float(_num(dk["Performance (LH)"]).dropna().mean())
        if "CWV Pass Rate" in dk.columns:
            out["cwv_strict_pct"] = float(_num(dk["CWV Pass Rate"]).dropna().mean())

    ai = ph4.get("Audit — Internal")
    if ai is not None and not ai.empty:
        out["pages"] = int(len(ai))
        if "HTTPS" in ai.columns:
            https = ai["HTTPS"].astype(str).str.lower().isin(["true","1","yes","y","t"])
            out["https_pct"] = float(https.mean())*100.0 if https.size else None
        jl_col = next((c for c in ai.columns if "json-ld" in c.lower()), None)
        if jl_col:
            jl = ai[jl_col].astype(str).str.strip()
            out["jsonld_pct"] = float((jl != "").mean())*100.0 if jl.size else None
        inl_col = next((c for c in ai.columns if c.lower()=="inlinks"), None)
        if inl_col:
            inl = _num(ai[inl_col])
            out["avg_inlinks"] = float(inl.dropna().mean()) if inl.notna().any() else None
        if out["perf_lh"] is None and "PSI Mobile" in ai.columns:
            psi = _num(ai["PSI Mobile"])
            out["perf_lh"] = float(psi.dropna().mean()) if psi.notna().any() else None
        lcp_col = next((c for c in ai.columns if c.lower().startswith("lcp")), None)
        inp_col = next((c for c in ai.columns if c.lower().startswith("inp")), None)
        cls_col = next((c for c in ai.columns if c.strip().lower()=="cls"), None)
        lcp = _num(ai[lcp_col]) if lcp_col else pd.Series([], dtype=float)
        inp = _num(ai[inp_col]) if inp_col else pd.Series([], dtype=float)
        cls = _num(ai[cls_col]) if cls_col else pd.Series([], dtype=float)
        if len(lcp): out["lcp_p75"] = _percentile(lcp, 75)
        if len(inp): out["inp_p75"] = _percentile(inp, 75)
        if len(cls): out["cls_p75"] = _percentile(cls, 75)
        if len(lcp) and len(inp) and len(cls):
            strict_mask = (lcp<=2500) & (inp<=200) & (cls<=0.1)
            out["cwv_strict_pct"] = float(strict_mask.mean()*100.0)
            out["inp_good_pct"] = float((inp<=200).mean()*100.0)
        if len(lcp) and len(cls):
            legacy_mask = (lcp<=2500) & (cls<=0.1)
            out["cwv_legacy_pct"] = float(legacy_mask.mean()*100.0)
        st_col = next((c for c in ai.columns if c.strip().lower()=="status"), None)
        if st_col:
            s = _num(ai[st_col])
            out["status"]["2xx"] = int(((s>=200)&(s<300)).sum())
            out["status"]["3xx"] = int(((s>=300)&(s<400)).sum())
            out["status"]["4xx"] = int(((s>=400)&(s<500)).sum())
            out["status"]["5xx"] = int(((s>=500)&(s<600)).sum())
        if "Images Missing Alt" in ai.columns:
            miss = _num(ai["Images Missing Alt"]).fillna(0)
            try:
                total_rows = len(ai.index)
                with_alt_pct = max(0.0, 100.0 * (1.0 - (miss>0).sum()/max(1,total_rows)))
                out["img_with_alt_pct"] = with_alt_pct
            except Exception:
                pass
    return out

# ---------- small charts (same as v5_6i) ----------
def svg_status_bar(d: dict) -> str:
    total = sum(d.values()) or 1
    w = 1000; h = 38; x=10; y=10
    segments = []; cur = x
    for idx,label in enumerate(["2xx","3xx","4xx","5xx"]):
        val = d.get(label, 0)
        ww = int(round((val/total)*(w-20)))
        segments.append(f'<rect x="{cur}" y="{y}" width="{ww}" height="18" rx="6" ry="6" fill="var(--primary)" opacity="{0.2 + 0.15*idx}"><title>{label}: {val}</title></rect>')
        cur += ww
    ticks = ''.join([f'<text x="{x+i*(w-20)/4}" y="{y+34}" font-size="11" fill="#666">{lab}: {_fmt_int(d.get(lab,0))}</text>' for i,lab in enumerate(["2xx","3xx","4xx","5xx"])])
    return f'<svg viewBox="0 0 {w} 50">{ "".join(segments) }{ticks}</svg>'

def svg_bar(pairs: List[Tuple[str,float]], title: str) -> str:
    if not pairs:
        return f'<div class="chart"><svg viewBox="0 0 1000 200"><text x="500" y="100" font-size="14" text-anchor="middle" fill="#666">No data for {title}</text></svg></div>'
    maxv = max(v for _,v in pairs) or 1
    x0, x1, y0 = 160, 970, 20
    span = x1-x0
    lines = [f'<div class="chart"><svg viewBox="0 0 1000 {len(pairs)*30+60}">']
    for i in range(6):
        gx = x0 + int(span*i/5); val = maxv*i/5
        lines.append(f'<line x1="{gx}" y1="{y0}" x2="{gx}" y2="{len(pairs)*30+20}" stroke="#eee" stroke-width="1"/><text x="{gx}" y="{len(pairs)*30+50}" font-size="11" text-anchor="middle" fill="#666">{val:.0f}</text>')
    for idx,(name,val) in enumerate(pairs):
        bw = int(round((val/maxv)*span)); bw = max(0, min(bw, span)); y = y0 + idx*30
        safe_name = (name or "").replace("&","&amp;")
        lines.append(f'<text x="152" y="{y+16}" font-size="12" text-anchor="end" fill="#333">{safe_name}</text>')
        lines.append(f'<rect x="{x0}" y="{y}" width="{bw}" height="22" rx="6" ry="6" fill="var(--primary)" opacity="0.9"/><text x="{x0+bw+6}" y="{y+16}" font-size="12" fill="#333">{val:.1f}</text>')
    lines.append('</svg></div>')
    return ''.join(lines)

# ---------- competitors (same as v5_6i robust logic) ----------
def derive_competitors(ph3: Dict[str,pd.DataFrame]) -> pd.DataFrame:
    if not ph3: return pd.DataFrame()
    hits = None
    for name,df in ph3.items():
        if "Competitor_SERP_Hits" in name or "competitor_serp_hits" in name.lower():
            hits = df.copy(); break
    if hits is None:
        for name,df in ph3.items():
            if "Competitor_Scores" in name or "competitor_scores" in name.lower():
                hits = df.copy(); break
    if hits is None: return pd.DataFrame()
    dom = next((c for c in hits.columns if c.lower()=="domain" or "domain" in c.lower() or "host" in c.lower()), None)
    if dom is None: return pd.DataFrame()
    df = pd.DataFrame({"domain": hits[dom].astype(str)})
    def getn(label, *alts):
        c = next((k for k in hits.columns if k.lower()==label.lower()), None)
        if c is None:
            for a in alts:
                c = next((k for k in hits.columns if k.lower()==a.lower()), None)
                if c: break
        return _num(hits[c]).fillna(0).astype(int) if c else pd.Series([0]*len(df))
    df["hits"] = getn("hits","serp_hits","count","score")
    df["top10"] = getn("top10")
    df["top3"]  = getn("top3")
    c_us = next((c for c in hits.columns if "us" in c.lower()), None)
    if c_us: df["is_us"] = (hits[c_us].astype(str).str.lower().isin(["true","1","yes","y","t"]))
    tot_hits = int(df["hits"].sum()) or 1
    tot_top3 = int(df["top3"].sum()) or 1
    tot_top10= int(df["top10"].sum()) or 1
    df["sov"] = df["hits"]/tot_hits*100.0
    df["sov_top3"] = df["top3"]/tot_top3*100.0
    df["sov_top10"] = df["top10"]/tot_top10*100.0
    return df.sort_values(["hits","top3","top10"], ascending=False).reset_index(drop=True)

def parity_summary(df: pd.DataFrame, origin_host: str) -> dict:
    if df is None or df.empty: 
        return {"rank_sov":None,"median_sov":None,"max_sov":None,"your_sov":None}
    oh = origin_host.lower().lstrip("www.")
    def norm(d): return str(d).lower().lstrip("www.")
    x = df.copy()
    x["_norm"] = x["domain"].map(norm)
    x["rank_sov"] = x["sov"].rank(ascending=False, method="min")
    row = x[x["_norm"]==oh].head(1)
    your_sov = float(row["sov"].iloc[0]) if not row.empty else None
    your_rsov= int(row["rank_sov"].iloc[0]) if not row.empty else None
    med_sov = float(x["sov"].median())
    max_sov = float(x["sov"].max())
    return {"rank_sov":your_rsov,"median_sov":med_sov,"max_sov":max_sov,"your_sov":your_sov}

# ---------- CWV by template (same) ----------
def derive_template_cwv(ai: pd.DataFrame) -> pd.DataFrame:
    if ai is None or ai.empty or "URL" not in ai.columns: return pd.DataFrame()
    seg = ai["URL"].astype(str).map(_top_segment)
    lcp_col = next((c for c in ai.columns if c.lower().startswith("lcp")), None)
    inp_col = next((c for c in ai.columns if c.lower().startswith("inp")), None)
    cls_col = next((c for c in ai.columns if c.strip().lower()=="cls"), None)
    if not (lcp_col and inp_col and cls_col): return pd.DataFrame()
    g = ai.copy(); g["__seg"] = seg
    rows = []
    for k,df in g.groupby("__seg", dropna=False):
        lcp = _num(df[lcp_col]); inp=_num(df[inp_col]); cls=_num(df[cls_col])
        n = len(df)
        if n==0: continue
        strict = ((lcp<=2500)&(inp<=200)&(cls<=0.1)).mean()*100.0
        rows.append({"segment": k, "pages": n, "lcp_p75": _percentile(lcp,75), "inp_p75": _percentile(inp,75), "cls_p75": _percentile(cls,75), "cwv_strict_pct": strict})
    out = pd.DataFrame(rows).sort_values("cwv_strict_pct", ascending=True)
    return out

def derive_canonical_indexing(canon: pd.DataFrame, directives: pd.DataFrame) -> dict:
    out = {"has_canonical":None,"self_canonical":None,"followable":None}
    if canon is not None and not canon.empty:
        if "Has Canonical" in canon.columns:
            out["has_canonical"] = float(_truthy(canon["Has Canonical"]).mean()*100.0)
        if "Self Canonical" in canon.columns:
            out["self_canonical"] = float(_truthy(canon["Self Canonical"]).mean()*100.0)
    if directives is not None and not directives.empty:
        if "Followable" in directives.columns:
            out["followable"] = float(_truthy(directives["Followable"]).mean()*100.0)
    return out

def derive_duplicates(dups: pd.DataFrame) -> dict:
    out = {"clusters":0,"largest_cluster":0}
    if dups is None or dups.empty: return out
    key = next((c for c in dups.columns if "content hash" in c.lower()), None)
    if not key: return out
    vc = dups[key].astype(str).value_counts()
    out["clusters"] = int((vc>=2).sum())
    out["largest_cluster"] = int((vc.max() if len(vc) else 0))
    return out

def derive_images(images: Optional[pd.DataFrame]) -> dict:
    out = {"with_alt_pct":None,"top_missing_rows":0}
    if images is None or images.empty: return out
    if "ALT" not in images.columns: return out
    mask = images["ALT"].astype(str).str.strip() != ""
    out["with_alt_pct"] = float(mask.mean())*100.0 if mask.size else None
    out["top_missing_rows"] = int((~mask).sum())
    return out

def derive_sitemap_diff(sd: pd.DataFrame) -> dict:
    out = {"added":0,"removed":0,"mismatch":0}
    if sd is None or sd.empty: return out
    tcol = next((c for c in sd.columns if c.lower()=="type"), None)
    rcol = next((c for c in sd.columns if c.lower()=="reason"), None)
    if not (tcol and rcol): return out
    td = sd[tcol].astype(str).str.lower()
    rd = sd[rcol].astype(str).str.lower()
    out["added"] = int(((td=="sitemap") & rd.str.contains("only")).sum())
    out["removed"] = int(((td=="crawl") & rd.str.contains("only")).sum())
    out["mismatch"] = int((~rd.str.contains("only")).sum())
    return out

def derive_inp_culprits(ai: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    if ai is None or ai.empty: return pd.DataFrame()
    url_col = next((c for c in ai.columns if c.lower()=="url"), None)
    inp_col = next((c for c in ai.columns if c.lower().startswith("inp")), None)
    if not (url_col and inp_col): return pd.DataFrame()
    df = ai[[url_col, inp_col]].copy()
    df.columns = ["url","inp"]
    df["inp"] = _num(df["inp"])
    df = df.dropna(subset=["inp"]).sort_values("inp", ascending=False)  # no head() yet
    df["segment"] = df["url"].astype(str).map(_top_segment)
    return df[["url","segment","inp"]]

def derive_inp_culprits_table(ai: pd.DataFrame, show_n: int = 6) -> Tuple[str,int]:
    culprits = derive_inp_culprits(ai if ai is not None and not ai.empty else pd.DataFrame(), top_n=9999)
    if culprits is None or culprits.empty:
        return '<tr><td colspan="3">No INP data found</td></tr>', 0
    extra = max(0, len(culprits) - show_n)
    culprits = culprits.head(show_n)
    rows = []
    for _,r in culprits.iterrows():
        rows.append(f"<tr><td>/{r['segment']}</td><td><a href='{r['url']}' target='_blank' rel='noopener'>{r['url']}</a></td><td>{_fmt_int(r['inp'])}</td></tr>")
    return "\n".join(rows), extra

# ---------- offsite & coverage (same as v5_6i) ----------
def build_offsite_block(ph2: Dict[str,pd.DataFrame], debug=False) -> Tuple[str, dict]:
    info = {"has_sheet": False, "columns": []}
    df = _get_sheet(ph2, prefer=["Phase2 — KPI_Summary","KPI_Summary"])
    if df is None or df.empty:
        empty_card = """
  <div class="card" style="margin-top:12px">
    <h2>Off‑site KPIs</h2>
    <div class="mini">No KPI_Summary sheet found in Phase2 workbook.</div>
  </div>"""
        return empty_card, info
    info["has_sheet"] = True
    info["columns"] = list(df.columns)
    cols = {c.lower(): c for c in df.columns}
    tb = df[cols.get("total_backlinks")] if "total_backlinks" in cols else None
    rd = df[cols.get("ref_domains")] if "ref_domains" in cols else None
    da = df[cols.get("moz_da_median")] if "moz_da_median" in cols else None
    ld = df[cols.get("moz_linking_domains_sum")] if "moz_linking_domains_sum" in cols else None
    def val(s):
        if s is None: return "–"
        v = _num(s).dropna()
        if not len(v): return "–"
        return _fmt_int(v.iloc[0])
    row = f"""
  <div class="card" style="margin-top:12px">
    <h2>Off‑site KPIs</h2>
    <div class="grid" style="margin-top:4px">
      <div class="kpi"><div class="label">Total Backlinks</div><div class="value">{val(tb)}</div></div>
      <div class="kpi"><div class="label">Referring Domains</div><div class="value">{val(rd)}</div></div>
      <div class="kpi"><div class="label">Domain Authority (median)</div><div class="value">{val(da)}</div></div>
      <div class="kpi"><div class="label">Linking Domains (sum)</div><div class="value">{val(ld)}</div></div>
    </div>
  </div>
"""
    return row, info

def build_keyword_coverage(ph2: Dict[str,pd.DataFrame]) -> Tuple[str, dict]:
    info = {"has_km": False, "has_gsc": False}
    km = _get_sheet(ph2, prefer=["Phase2 — keyword_map","keyword_map"])
    gsc = _get_sheet(ph2, contains="gsc")
    if km is None or km.empty:
        info["has_km"] = False
        return """
  <div class="card" style="margin-top:12px">
    <h2>Keyword Coverage</h2>
    <div class="mini">No keyword_map sheet found in Phase2 workbook.</div>
  </div>""", info
    info["has_km"] = True
    if gsc is None or gsc.empty:
        info["has_gsc"] = False
        return """
  <div class="card" style="margin-top:12px">
    <h2>Keyword Coverage</h2>
    <div class="mini">No GSC sheet found in Phase2 workbook.</div>
  </div>""", info
    info["has_gsc"] = True
    if "keyword" not in km.columns: 
        return """
  <div class="card" style="margin-top:12px">
    <h2>Keyword Coverage</h2>
    <div class="mini">Phase2 keyword_map has no 'keyword' column.</div>
  </div>""", info
    has_query = "query" in [c.lower() for c in gsc.columns]
    if not has_query:
        return """
  <div class="card" style="margin-top:12px">
    <h2>Keyword Coverage</h2>
    <div class="mini">Phase2 GSC sheet has no 'query' column.</div>
  </div>""", info
    col_query = [c for c in gsc.columns if c.lower()=="query"][0]
    mapped = set(km["keyword"].astype(str).str.lower())
    g = gsc.copy()
    g["__q"] = g[col_query].astype(str).str.lower()
    clicks = _num(g.get("clicks")).fillna(0)
    impr   = _num(g.get("impressions")).fillna(0)
    mapped_total = len(mapped) or 1
    mapped_with_impr = len(set(g.loc[impr>0,"__q"]) & mapped)
    mapped_with_clicks = len(set(g.loc[clicks>0,"__q"]) & mapped)
    return f"""
  <div class="card" style="margin-top:12px">
    <h2>Keyword Coverage</h2>
    <div class="grid" style="margin-top:4px">
      <div class="kpi"><div class="label">Mapped Keywords</div><div class="value">{_fmt_int(mapped_total)}</div></div>
      <div class="kpi"><div class="label">Mapped w/ Impressions</div><div class="value">{_fmt_int(mapped_with_impr)} ({int(round(mapped_with_impr*100/mapped_total))}%)</div></div>
      <div class="kpi"><div class="label">Mapped w/ Clicks</div><div class="value">{_fmt_int(mapped_with_clicks)} ({int(round(mapped_with_clicks*100/mapped_total))}%)</div></div>
    </div>
  </div>
""", info

# ---------- internal link opportunities (same) ----------
def _token_set(s: str) -> set:
    return set(t for t in re.sub(r'[^a-z0-9]+',' ', str(s).lower()).split() if t)

def build_internal_link_opps(ph4: Dict[str,pd.DataFrame], ph2: Dict[str,pd.DataFrame], top_n: int=10) -> Tuple[str, dict]:
    info = {"has_ai":False,"has_inlinks":False,"has_km":False,"has_gsc":False,"matched_keywords":0}
    ai = ph4.get("Audit — Internal")
    inlinks = ph4.get("Audit — Inlinks")
    gsc = _get_sheet(ph2, contains="gsc")
    if ai is None or ai.empty or "URL" not in ai.columns:
        return """
  <div class="card" style="margin-top:12px">
    <h2>Internal Link Opportunities</h2>
    <div class="mini">No 'Audit — Internal' sheet with URL column.</div>
  </div>""", info
    info["has_ai"] = True
    if inlinks is None or inlinks.empty or "Target" not in inlinks.columns:
        return """
  <div class="card" style="margin-top:12px">
    <h2>Internal Link Opportunities</h2>
    <div class="mini">No 'Audit — Inlinks' sheet with Target column.</div>
  </div>""", info
    info["has_inlinks"] = True
    if gsc is None or gsc.empty:
        return """
  <div class="card" style="margin-top:12px">
    <h2>Internal Link Opportunities</h2>
    <div class="mini">No GSC sheet found in Phase2 workbook.</div>
  </div>""", info
    info["has_gsc"] = True
    km = _get_sheet(ph2, prefer=["Phase2 — keyword_map","keyword_map"])
    if km is None or km.empty or "keyword" not in km.columns or "target_url" not in km.columns:
        return """
  <div class="card" style="margin-top:12px">
    <h2>Internal Link Opportunities</h2>
    <div class="mini">No keyword→URL mapping available (keyword_map with 'keyword' and 'target_url').</div>
  </div>""", info
    info["has_km"] = True
    if "query" not in gsc.columns or "impressions" not in gsc.columns:
        return """
  <div class="card" style="margin-top:12px">
    <h2>Internal Link Opportunities</h2>
    <div class="mini">GSC is missing 'query' or 'impressions' columns.</div>
  </div>""", info

    km2 = km[["keyword","target_url"]].dropna().copy()
    km2["kw_tokens"] = km2["keyword"].map(_token_set)
    g = pd.DataFrame({"query": gsc["query"].astype(str), "impressions": _num(gsc["impressions"]).fillna(0)})
    g["q_tokens"] = g["query"].map(_token_set)
    agg = {}; matched=0
    for _, kr in km2.iterrows():
        kw_set = kr["kw_tokens"]
        if not kw_set: continue
        best_imp = 0.0
        for _, qr in g.iterrows():
            qset = qr["q_tokens"]
            if not qset: continue
            inter = len(kw_set & qset); uni = len(kw_set | qset)
            jacc = inter/uni if uni else 0.0
            if jacc >= 0.5:
                matched += 1
                best_imp += float(qr["impressions"])
        if best_imp>0:
            agg[kr["target_url"]] = agg.get(kr["target_url"], 0.0) + best_imp
    info["matched_keywords"] = matched
    counts = inlinks["Target"].astype(str).value_counts()
    rows = []
    for url, impr in agg.items():
        il = int(counts.get(url, 0))
        rows.append((url, il, float(impr)))
    if not rows:
        return """
  <div class="card" style="margin-top:12px">
    <h2>Internal Link Opportunities</h2>
    <div class="mini">No matches between GSC queries and mapped keywords (after fuzzy matching).</div>
  </div>""", info
    df = pd.DataFrame(rows, columns=["url","inlinks","impressions"]).copy()
    df["score"] = df["impressions"]/(df["inlinks"]+1)
    df = df.sort_values(["score","impressions","inlinks"], ascending=[False,False,True]).head(top_n)
    trs = "\n".join([f"<tr><td><a href='{r.url}' target='_blank' rel='noopener'>{r.url}</a></td><td>{_fmt_int(r.inlinks)}</td><td>{_fmt_int(r.impressions)}</td><td>{_fmt_float(r.score,1)}</td></tr>" for r in df.itertuples()])
    return f"""
  <div class="card" style="margin-top:12px">
    <h2>Internal Link Opportunities</h2>
    <div class="mini">Pages with high demand (GSC impressions) but few internal links.</div>
    <table class="tbl">
      <tr><th>URL</th><th>Inlinks</th><th>Impressions</th><th>Priority</th></tr>
      {trs}
    </table>
  </div>
""", info

# ---------- quick wins / structured / sitemap (same as v5_6i) ----------
def _dedupe_and_cap(df, url_col, sort_col, ascending, cap=5):
    if df is None or df.empty: return df, 0
    tmp = df.copy()
    tmp["__norm"] = tmp[url_col].map(_norm_url_basic)
    tmp = tmp.sort_values(sort_col, ascending=ascending)
    before = len(tmp)
    tmp = tmp.drop_duplicates("__norm", keep="first")
    extra = max(0, before - min(cap, len(tmp)))
    return tmp.head(cap), extra

def build_quick_wins_block(ph4: Dict[str,pd.DataFrame]) -> str:
    qual = ph4.get("Audit — Quality")
    ai = ph4.get("Audit — Internal")
    inlinks = ph4.get("Audit — Inlinks")
    dup = ph4.get("Audit — Duplicates Exact")
    cards = []

    if qual is not None and not qual.empty:
        # Long titles
        if set(["URL","Title Length","Title Too Long"]).issubset(qual.columns):
            q_full = qual[qual["Title Too Long"].fillna(0).astype(float)>0][["URL","Title Length"]]
            q, extra_q = _dedupe_and_cap(q_full, "URL", "Title Length", ascending=False, cap=5)
            if q is not None and not q.empty:
                rows = "\n".join([f"<tr><td><a href='{u}' target='_blank' rel='noopener'>{u}</a></td><td>{int(l)}</td></tr>" for u,l in zip(q["URL"], q["Title Length"])])
                more = f"<div class='mini'>+{extra_q} more</div>" if extra_q>0 else ""
                cards.append(f"<div class='card span3'><h3>Titles Too Long</h3><table class='tbl'><tr><th>URL</th><th>Chars</th></tr>{rows}</table>{more}</div>")
        # Long descriptions
        desc_len_col = "Description Length" if "Description Length" in qual.columns else ("Desc Length" if "Desc Length" in qual.columns else None)
        desc_flag_col = "Desc Too Long" if "Desc Too Long" in qual.columns else ("Description Too Long" if "Description Too Long" in qual.columns else None)
        if desc_len_col and desc_flag_col and "URL" in qual.columns:
            d_full = qual[qual[desc_flag_col].fillna(0).astype(float)>0][["URL",desc_len_col]]
            d, extra_d = _dedupe_and_cap(d_full, "URL", desc_len_col, ascending=False, cap=5)
            if d is not None and not d.empty:
                rows = "\n".join([f"<tr><td><a href='{u}' target='_blank' rel='noopener'>{u}</a></td><td>{int(l)}</td></tr>" for u,l in zip(d["URL"], d[desc_len_col])])
                more = f"<div class='mini'>+{extra_d} more</div>" if extra_d>0 else ""
                cards.append(f"<div class='card span3'><h3>Descriptions Too Long</h3><table class='tbl'><tr><th>URL</th><th>Chars</th></tr>{rows}</table>{more}</div>")

    # Zero/Low inlinks
    if ai is not None and not ai.empty and inlinks is not None and not inlinks.empty:
        if "URL" in ai.columns and "Target" in inlinks.columns:
            counts = inlinks["Target"].astype(str).value_counts()
            df = ai[["URL"]].copy()
            df["inlinks"] = df["URL"].map(counts).fillna(0).astype(int)
            low_full = df.sort_values("inlinks", ascending=True)
            low, extra_low = _dedupe_and_cap(low_full, "URL", "inlinks", ascending=True, cap=5)
            if low is not None and not low.empty:
                rows = "\n".join([f"<tr><td><a href='{u}' target='_blank' rel='noopener'>{u}</a></td><td>{int(l)}</td></tr>" for u,l in zip(low["URL"], low["inlinks"])])
                more = f"<div class='mini'>+{extra_low} more</div>" if extra_low>0 else ""
                cards.append(f"<div class='card span3'><h3>Low/Zero Inlinks</h3><table class='tbl'><tr><th>URL</th><th>Inlinks</th></tr>{rows}</table>{more}</div>")

    # Largest duplicate clusters (example pairs)
    dup_df = ph4.get("Audit — Duplicates Exact")
    if dup_df is not None and not dup_df.empty and "Content Hash" in dup_df.columns and "URL" in dup_df.columns:
        vc = dup_df["Content Hash"].astype(str).value_counts()
        clusters = vc[vc>=2].sort_values(ascending=False).head(5).index.tolist()
        examples = []
        for h in clusters:
            urls = dup_df[dup_df["Content Hash"].astype(str)==h]["URL"].astype(str)
            urls = urls.map(_norm_url_basic).drop_duplicates().head(2).tolist()
            if len(urls)>=2:
                examples.append((urls[0], urls[1]))
        if examples:
            rows = "\n".join([f"<tr><td><a href='{a}' target='_blank' rel='noopener'>{a}</a></td><td><a href='{b}' target='_blank' rel='noopener'>{b}</a></td></tr>" for a,b in examples])
            cards.append(f"<div class='card span3'><h3>Duplicate Clusters (Examples)</h3><table class='tbl'><tr><th>URL A</th><th>URL B</th></tr>{rows}</table></div>")

    if not cards: 
        return ""
    return "<div class='grid' style='margin-top:12px'>" + "".join(cards) + "</div>"

def build_structured_data_block(ph4: Dict[str,pd.DataFrame]) -> str:
    ai = ph4.get("Audit — Internal")
    if ai is None or ai.empty:
        return ""
    col = next((c for c in ai.columns if "json-ld" in c.lower()), None)
    if not col:
        return ""
    types = []
    for v in ai[col].astype(str).fillna("").tolist():
        for t in re.split(r'[;,|]+', v):
            t = t.strip()
            if t: types.append(t)
    if not types:
        return ""
    vc = pd.Series(types).value_counts().head(12)
    rows = "\n".join([f"<tr><td>{k}</td><td>{_fmt_int(v)}</td></tr>" for k,v in vc.items()])
    return f"""
  <div class="card" style="margin-top:12px">
    <h2>Structured Data Types</h2>
    <table class="tbl">
      <tr><th>Type</th><th>Pages</th></tr>
      {rows}
    </table>
  </div>
"""

def build_sitemap_examples_block(ph4: Dict[str,pd.DataFrame]) -> str:
    sd = ph4.get("Phase1 — Sitemap Diff")
    if sd is None or sd.empty or not set(["url","type","reason"]).issubset({c.lower() for c in sd.columns}):
        return ""
    df = sd.rename(columns={c:c.lower() for c in sd.columns})
    def sample(kind, cond):
        samp = df[cond].head(5)
        if samp.empty: return ""
        rows = "\n".join([f"<tr><td><a href='{u}' target='_blank' rel='noopener'>{u}</a></td><td>{r}</td></tr>" for u,r in zip(samp['url'],samp['reason'])])
        return f"<div class='card span3'><h3>{kind}</h3><table class='tbl'><tr><th>URL</th><th>Reason</th></tr>{rows}</table></div>"
    block_a = sample("Sitemap‑only URLs", (df["type"].str.lower()=="sitemap") & df["reason"].str.contains("only", case=False, na=False))
    block_b = sample("Crawl‑only URLs", (df["type"].str.lower()=="crawl") & df["reason"].str.contains("only", case=False, na=False))
    block_c = sample("Other Mismatches", ~(df["reason"].str.contains("only", case=False, na=False)))
    blocks = "".join([b for b in [block_a,block_b,block_c] if b])
    if not blocks: return ""
    return "<div class='grid' style='margin-top:12px'>" + blocks + "</div>"

# ---------- issues & GSC (same as v5_6i) ----------
def build_issue_counts(ph4: Dict[str,pd.DataFrame]) -> dict:
    out = {"critical":0,"high":0,"medium":0,"low":0}
    iss = ph4.get("Audit — Issues")
    if iss is not None and not iss.empty and "Severity" in iss.columns:
        s = iss["Severity"].astype(str).str.lower()
        out["critical"] = int((s=="critical").sum())
        out["high"] = int((s=="high").sum())
        out["medium"] = int((s=="medium").sum())
        out["low"] = int((s=="low").sum())
        return out
    qual = ph4.get("Audit — Quality")
    internal = ph4.get("Audit — Internal")
    directives = ph4.get("Audit — Directives")
    canon = ph4.get("Audit — Canonicals")

    if internal is not None and not internal.empty and "Status" in internal.columns:
        codes = _num(internal["Status"])
        out["high"] += int(((codes>=500)&(codes<600)).sum())
        out["high"] += int(((codes>=400)&(codes<500)).sum())
    if directives is not None and not directives.empty:
        mr = directives.get("Meta Robots")
        if mr is not None:
            out["high"] += int(mr.astype(str).str.contains("noindex", case=False, na=False).sum())
    if qual is not None and not qual.empty:
        for c in ["Title Duplicate","Desc Duplicate","Images Missing Alt"]:
            if c in qual.columns:
                out["medium"] += int(pd.to_numeric(qual[c], errors="coerce").fillna(0).astype(int).sum())
        for c in ["Title Too Long","Desc Too Long","Description Too Long"]:
            if c in qual.columns:
                out["low"] += int(pd.to_numeric(qual[c], errors="coerce").fillna(0).astype(int).sum())
    if canon is not None and not canon.empty and "Has Canonical" in canon.columns:
        has_c = canon["Has Canonical"].astype(str).str.lower().isin(["true","1","yes","y","t"])
        out["low"] += int((~has_c).sum())
    if internal is not None and not internal.empty and "Mixed Content Count" in internal.columns:
        mc = pd.to_numeric(internal["Mixed Content Count"], errors="coerce").fillna(0)
        out["medium"] += int((mc>0).sum())
    return out

def build_gsc_rows(ph2: Dict[str,pd.DataFrame]) -> Tuple[str, str]:
    gsc = _get_sheet(ph2, contains="gsc")
    if gsc is None or gsc.empty:
        return '<tr><td colspan="5">No GSC data</td></tr>', ""
    cols = {c.lower(): c for c in gsc.columns}
    if not {"query","clicks","impressions"}.issubset(cols.keys()):
        return '<tr><td colspan="5">Missing GSC columns</td></tr>', ""
    q = gsc[cols["query"]].astype(str)
    c = _num(gsc[cols["clicks"]]); i = _num(gsc[cols["impressions"]])
    pos = _num(gsc.get(cols.get("avg_position","avg_position"))) if "avg_position" in cols else None
    df = pd.DataFrame({"query":q, "clicks":c, "impressions":i})
    if pos is not None: df["avg_position"] = pos
    df = df.dropna(subset=["clicks","impressions"])
    df["ctr"] = np.where(df["impressions"]>0, df["clicks"]/df["impressions"], np.nan)
    df = df.sort_values(["clicks","impressions"], ascending=False).head(15)
    rows = []
    for _,r in df.iterrows():
        rows.append(f"<tr><td>{r['query']}</td><td>{_fmt_int(r['clicks'])}</td><td>{_fmt_int(r['impressions'])}</td><td>{_pct_text(r['ctr'])}</td><td>{_fmt_float(r.get('avg_position',''),1) if 'avg_position' in df.columns else '–'}</td></tr>")
    gsc_window = ""
    if "date" in cols:
        try:
            d = pd.to_datetime(gsc[cols["date"]], errors="coerce")
            dmin = d.min(); dmax = d.max()
            if pd.notna(dmin) and pd.notna(dmax):
                gsc_window = f"<span class='mini'>( {dmin.date()} → {dmax.date()} )</span>"
        except Exception:
            pass
    return "\n".join(rows) if rows else '<tr><td colspan="5">No queries</td></tr>', gsc_window

def build_brand_rows(ph2: Dict[str,pd.DataFrame], origin: str) -> str:
    gsc = _get_sheet(ph2, contains="gsc")
    if gsc is None or gsc.empty or "query" not in [c.lower() for c in gsc.columns]:
        return ""
    col_query = [c for c in gsc.columns if c.lower()=="query"][0]
    clicks = _num(gsc.get("clicks")); impr = _num(gsc.get("impressions"))
    if clicks is None or impr is None: return ""
    km = _get_sheet(ph2, prefer=["Phase2 — keyword_map","keyword_map"])
    branded_terms = set()
    if km is not None and not km.empty and "source" in km.columns and "keyword" in km.columns:
        branded_terms = set(km[km["source"].astype(str).str.lower()=="brand"]["keyword"].astype(str).str.lower().tolist())
    else:
        branded_terms = set(brand_heuristic(origin))
    def is_branded(q: str) -> bool:
        ql = str(q or "").lower()
        return any(t in ql for t in branded_terms) if branded_terms else False
    mask_b = gsc[col_query].astype(str).map(is_branded)
    b_clicks = int(clicks[mask_b].sum()) if len(clicks) else 0
    nb_clicks = int(clicks[~mask_b].sum()) if len(clicks) else 0
    b_impr = int(impr[mask_b].sum()) if len(impr) else 0
    nb_impr = int(impr[~mask_b].sum()) if len(impr) else 0
    row = f"<tr><td>Branded Clicks</td><td>{_fmt_int(b_clicks)}</td></tr><tr><td>Non‑Branded Clicks</td><td>{_fmt_int(nb_clicks)}</td></tr><tr><td>Branded Impr.</td><td>{_fmt_int(b_impr)}</td></tr><tr><td>Non‑Branded Impr.</td><td>{_fmt_int(nb_impr)}</td></tr>"
    return row

# ---------- HTML template (brand + compact) ----------
HTML = Template(r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Silent Princess — Client Report (v5_6k)</title>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Sour+Gummy:wght@400;600;700&display=swap" rel="stylesheet">
<style>
  :root{ --bg:#B6C6A9; --fg:#111; --muted:#B6C6A9; --primary:#323E28; --accent:#2F95B1; }
  html,body{margin:0;padding:0;background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,"Helvetica Neue",Arial}
  h1,h2,h3{font-family:"Sour Gummy", system-ui, sans-serif;letter-spacing:.2px}
  .wrap{max-width:1200px;margin:18px auto;padding:0 12px}
  h1{font-size:26px;margin:8px 0 4px}
  .sub{color:#555;margin:0 0 10px}
  .grid{display:grid;grid-template-columns:repeat(6,1fr); gap:10px}
  .grid2{display:grid;grid-template-columns:1.8fr 2.2fr; gap:12px}
  @media(max-width: 1000px){ .grid{grid-template-columns:repeat(4,1fr)} .grid2{grid-template-columns:1fr} }
  @media(max-width: 720px){ .grid{grid-template-columns:repeat(2,1fr)} .grid2{grid-template-columns:1fr} }
  .span2{grid-column:span 2} .span3{grid-column:span 3} .span6{grid-column:span 6}
  .card{background:#fff;border-radius:14px;box-shadow:0 3px 10px rgba(0,0,0,.05);padding:12px; overflow:hidden}
  .kpi{background:#fff;border:1px solid #eee;border-radius:12px;padding:8px; position:relative; min-height:auto}
  .kpi .label{color:#666;font-size:12px}
  .kpi .value{font-size:18px;font-weight:700;line-height:1.1}
  .bar{height:8px;background:#e8e8ef;border-radius:999px;overflow:hidden;margin-top:6px}
  .bar>i{display:block;height:8px;background:var(--primary)}
  .badge{position:relative;display:inline-block;background:#EAF8EA;border:1px solid #BCE2BC;color:#1E6B1E;font-size:11px;padding:2px 6px;border-radius:999px;margin:0 0 6px 0}
  .note{background:#FFFBEA;border:1px solid #F6E5A3;color:#6A5A00;padding:8px 10px;border-radius:10px;margin:10px 0;font-size:13px}
  .tbl{width:100%;border-collapse:collapse; table-layout:fixed}
  .tbl th,.tbl td{padding:6px 8px;border-bottom:1px solid #eee;text-align:left;vertical-align:top; word-break:break-word; overflow-wrap:anywhere; white-space:normal}
  .mini{color:#666;font-size:12px}
  .pill{display:inline-block;padding:2px 8px;border-radius:999px;background:rgba(47,149,177,.10);border:1px solid rgba(47,149,177,.35);color:var(--accent);margin-left:8px;font-size:12px}

  /* Compact layout & overflow */
  h2{margin:4px 0 6px}
  .tbl a{display:inline-block; max-width:100%; text-decoration:none}
  .kpi .badge{display:block; margin-bottom:4px}

  /* CWV compact table + INP list cap */
  .tbl.cwv th,.tbl.cwv td{padding:3px 6px;font-size:13px}
  .cwv-urls .tbl tr:nth-child(n+7){display:none} /* show top 6 rows */
  .cwv-urls .more{margin-top:6px;font-size:12px;color:#666}

  /* Auto full-width when grid has single card */
  .grid:has(> .card:only-child) > .card{grid-column:1 / -1}

  /* Print improvements */
  @media print{
    .pill{display:none}
    a[href]::after{content:""}
    .card{box-shadow:none;border:1px solid #ddd}
  }
</style>
</head>
<body>
<div class="wrap">
  <h1>Silent Princess — Client Report <span class="pill">${origin}</span></h1>
                
  ${generated_block}
  ${cwv_note}

  <!-- KPI STRIP -->
  <div class="grid">
    <div class="kpi"><div class="label">Performance (LH)</div><div class="value">${perf_txt}</div><div class="bar"><i style="width:${perf_w}%"></i></div></div>
    <div class="kpi"><div class="label">CWV (Strict)</div><div class="value">${cwv_strict_txt}</div><div class="bar"><i style="width:${cwv_strict_w}%"></i></div><div class="mini">LCP+INP+CLS</div></div>
    <div class="kpi"><div class="label">CWV (Legacy)</div><div class="value">${cwv_legacy_txt}</div><div class="bar"><i style="width:${cwv_legacy_w}%"></i></div><div class="mini">LCP+CLS</div></div>
    <div class="kpi"><div class="label">INP Good %</div><div class="value">${inp_good_txt}</div><div class="bar"><i style="width:${inp_good_w}%"></i></div><div class="mini">≤200 ms</div></div>
    <div class="kpi"><div class="label">JSON‑LD Coverage</div><div class="value">${jsonld_txt}</div><div class="bar"><i style="width:${jsonld_w}%"></i></div></div>
    <div class="kpi">${access_badge}<div class="label">Pages Crawled</div><div class="value">${pages}</div></div>
  </div>

  ${offsite_block}
  ${kw_coverage_block}

  <div class="card" style="margin-top:12px">
    <h2>Crawl Health</h2>
    ${status_svg}
  </div>

  ${comp_block}

  <!-- CORE WEB VITALS + INP CULPRITS -->
  <div class="grid2" style="margin-top:12px">
    <div class="card">
      <h2>Core Web Vitals — p75</h2>
      <table class="tbl cwv">
        <tr><th>Metric</th><th>p75</th><th>Target</th></tr>
        <tr><td>LCP (ms)</td><td>${lcp_p75}</td><td>≤ 2500</td></tr>
        <tr><td>INP (ms)</td><td>${inp_p75}</td><td>≤ 200</td></tr>
        <tr><td>CLS</td><td>${cls_p75}</td><td>≤ 0.1</td></tr>
      </table>
    </div>
    <div class="card">
      <h2>INP Culprits (Worst URLs)</h2>
      <div class="cwv-urls">
        <table class="tbl">
          <tr><th>Segment</th><th>URL</th><th>INP (ms)</th></tr>
          ${inp_rows}
        </table>
        ${inp_more}
      </div>
      <div class="mini">Source: Audit — Internal</div>
    </div>
  </div>

  <!-- ISSUES & GSC -->
  <div class="grid" style="margin-top:12px">
    <div class="card span3">
      <h2>Top Issues (Severity)</h2>
      <table class="tbl">
        <tr><th>Critical</th><th>High</th><th>Medium</th><th>Low</th></tr>
        <tr><td>${crit}</td><td>${high}</td><td>${med}</td><td>${low}</td></tr>
      </table>
      <div class="mini">${issues_source}</div>
    </div>
    <div class="card span3">
      <h2>Search Console Snapshot ${gsc_window}</h2>
      <table class="tbl">
        <tr><td>Clicks</td><td>${gsc_clicks}</td></tr>
        <tr><td>Impressions</td><td>${gsc_impr}</td></tr>
        <tr><td>Avg Position</td><td>${gsc_pos}</td></tr>
        ${brand_rows}
      </table>
    </div>
  </div>

  <div class="card" style="margin-top:12px">
    <h2>Top Queries (GSC)</h2>
    <table class="tbl">
      <tr><th>Query</th><th>Clicks</th><th>Impressions</th><th>CTR</th><th>Avg Pos</th></tr>
      ${gsc_rows}
    </table>
  </div>

  ${link_opps_block}

  ${structured_block}
  ${sitemap_examples}
  <div class="card" style="margin-top:12px">
    <h2>Quick Wins</h2>
    <div class="mini">Prioritized fixes your dev/content team can ship right away.</div>
    ${quick_wins}
  </div>

  ${adv_block}

</div>
</body>
</html>
""")

# ---------- advanced block (same as v5_6i) ----------
def derive_inlinks(inlinks: pd.DataFrame) -> dict:
    out = {"p50":None,"p90":None,"by_target":None}
    if inlinks is None or inlinks.empty: return out
    if "Target" in inlinks.columns:
        counts = inlinks["Target"].astype(str).value_counts()
        if len(counts):
            out["p50"] = float(np.percentile(counts.values, 50))
            out["p90"] = float(np.percentile(counts.values, 90))
            out["by_target"] = counts
    return out

def derive_security(ai: pd.DataFrame) -> dict:
    out = {"hsts":None,"xcto":None,"mixed":None}
    if ai is None or ai.empty: return out
    if "HSTS" in ai.columns:
        out["hsts"] = float(_truthy(ai["HSTS"]).mean()*100.0)
    if "X-Content-Type-Options" in ai.columns:
        out["xcto"] = float(_truthy(ai["X-Content-Type-Options"]).mean()*100.0)
    if "Mixed Content Count" in ai.columns:
        mc = _num(ai["Mixed Content Count"])
        out["mixed"] = float((mc>0).mean()*100.0) if mc.notna().any() else None
    return out

def build_advanced_block(ph4: Dict[str,pd.DataFrame], img_with_alt_pct: Optional[float]) -> str:
    ai = ph4.get("Audit — Internal")
    canon = ph4.get("Audit — Canonicals")
    directives = ph4.get("Audit — Directives")
    dups = ph4.get("Audit — Duplicates Exact")
    images = ph4.get("Audit — Images Detail")
    if images is None or images.empty:
        images = ph4.get("Audit — Images")
    sitemap = ph4.get("Phase1 — Sitemap Diff")
    inlinks = ph4.get("Audit — Inlinks")

    tmpl = derive_template_cwv(ai if ai is not None and not ai.empty else pd.DataFrame())
    hyg = derive_canonical_indexing(canon if canon is not None and not canon.empty else pd.DataFrame(),
                                    directives if directives is not None and not directives.empty else pd.DataFrame())
    dup = derive_duplicates(dups if dups is not None and not dups.empty else pd.DataFrame())
    img = derive_images(images if images is not None and not images.empty else None)
    inl = derive_inlinks(inlinks if inlinks is not None and not inlinks.empty else pd.DataFrame())
    sec = derive_security(ai if ai is not None and not ai.empty else pd.DataFrame())
    sm  = derive_sitemap_diff(sitemap if sitemap is not None and not sitemap.empty else pd.DataFrame())

    def rows_template(df: pd.DataFrame):
        if df is None or df.empty: return '<tr><td colspan="6">No URL data</td></tr>'
        lines = []
        for _,r in df.iterrows():
            seg = r['segment']
            if seg == "//": seg = "/"
            lines.append(f"<tr><td>/{seg}</td><td>{_fmt_int(r['pages'])}</td><td>{_fmt_int(r['lcp_p75'])}</td><td>{_fmt_int(r['inp_p75'])}</td><td>{_fmt_float(r['cls_p75'],2)}</td><td>{_pct_text(r['cwv_strict_pct'])}</td></tr>")
        return "\n".join(lines)

    img_pct_display = _pct_text(img_with_alt_pct) if img_with_alt_pct is not None else _pct_text(img.get("with_alt_pct"))

    block = f"""
  <div class="card" style="margin-top:12px">
    <h2>Advanced Technical Insights</h2>
    <div class="mini">Deeper diagnostics to guide engineering sprints.</div>

    <h3 style="margin-top:6px">Template CWV — by top‑level path</h3>
    <table class="tbl">
      <tr><th>Segment</th><th>Pages</th><th>LCP p75</th><th>INP p75</th><th>CLS p75</th><th>CWV Strict %</th></tr>
      {rows_template(tmpl)}
    </table>

    <div class="grid" style="margin-top:8px">
      <div class="kpi"><div class="label">Has Canonical</div><div class="value">{_pct_text(hyg.get('has_canonical'))}</div></div>
      <div class="kpi"><div class="label">Self Canonical</div><div class="value">{_pct_text(hyg.get('self_canonical'))}</div></div>
      <div class="kpi"><div class="label">Followable</div><div class="value">{_pct_text(hyg.get('followable'))}</div></div>
      <div class="kpi"><div class="label">Dup Clusters</div><div class="value">{_fmt_int(dup.get('clusters'))}</div></div>
      <div class="kpi"><div class="label">Largest Dup Cluster</div><div class="value">{_fmt_int(dup.get('largest_cluster'))}</div></div>
      <div class="kpi"><div class="label">Images With Alt</div><div class="value">{img_pct_display}</div></div>
      <div class="kpi"><div class="label">HSTS</div><div class="value">{_pct_text(sec.get('hsts'))}</div></div>
      <div class="kpi"><div class="label">X‑Content‑Type‑Options</div><div class="value">{_pct_text(sec.get('xcto'))}</div></div>
      <div class="kpi"><div class="label">Mixed Content Present</div><div class="value">{_pct_text(sec.get('mixed'))}</div></div>
      <div class="kpi"><div class="label">Sitemap‑Only URLs</div><div class="value">{_fmt_int(sm.get('added'))}</div></div>
      <div class="kpi"><div class="label">Crawl‑Only URLs</div><div class="value">{_fmt_int(sm.get('removed'))}</div></div>
      <div class="kpi"><div class="label">Other Mismatches</div><div class="value">{_fmt_int(sm.get('mismatch'))}</div></div>
    </div>
  </div>
"""
    return block

# ---------- main ----------

# === Search Visibility + SMB Parity (locked v2 with DOM scan) ===
from pathlib import Path
import pandas as pd, re, html
from urllib.parse import urlparse

# --- Wildcard platform filter (subdomains + ccTLDs) ---
import re as _re_plat
from urllib.parse import urlparse as _urlparse_plat

PLATFORM_FAMILIES = [
    r"amazon", r"youtube", r"youtu", r"pinterest", r"reddit",
    r"facebook", r"instagram", r"tiktok",
    r"etsy", r"walmart", r"ebay",
    r"wikipedia", r"linkedin", r"quora", r"medium"
]
PLATFORM_RE = _re_plat.compile(
    r"(^|\.)(" + "|".join(PLATFORM_FAMILIES) + r")\.[a-z]{2,}(?:\.[a-z]{2})?$",
    _re_plat.IGNORECASE
)
def _norm_host(u: str) -> str:
    if not isinstance(u, str) or not u:
        return ""
    if not u.startswith(("http://","https://")):
        u = "http://" + u
    host = _urlparse_plat(u).netloc.lower()
    return host[4:] if host.startswith("www.") else host

def _is_platform(host: str) -> bool:
    return bool(PLATFORM_RE.search(host or ""))
# --- end wildcard platform filter ---


MEGASITES = {
  "youtube.com","m.youtube.com","youtu.be",
  "amazon.com","smile.amazon.com","amazon.ca","amazon.co.uk",
  "pinterest.com","www.pinterest.com",
  "reddit.com","www.reddit.com",
  "facebook.com","www.facebook.com","instagram.com","www.instagram.com","tiktok.com","www.tiktok.com",
  "etsy.com","www.etsy.com","walmart.com","www.walmart.com","ebay.com","www.ebay.com",
  "wikipedia.org","en.wikipedia.org",
  "linkedin.com","www.linkedin.com","quora.com","www.quora.com","medium.com","www.medium.com"
}

def _sv_read_csv(p):
    try:
        if not p: return pd.DataFrame()
        pp = Path(p)
        if not pp.exists(): return pd.DataFrame()
        for enc in ("utf-8","utf-8-sig","latin1"):
            try: return pd.read_csv(pp, encoding=enc)
            except Exception: continue
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def _sv_norm_serp(df):
    if df is None or df.empty: return pd.DataFrame()
    cols = {c.lower(): c for c in df.columns}
    q = cols.get("query") or cols.get("keyword") or cols.get("term")
    url = cols.get("url") or cols.get("page") or cols.get("landing_page")
    pos = cols.get("position") or cols.get("rank") or cols.get("avg_position") or cols.get("serp_position")
    out = df.copy()
    if q: out = out.rename(columns={q:"query"})
    if url: out = out.rename(columns={url:"url"})
    if pos: out = out.rename(columns={pos:"position"})
    if "position" in out: out["position"] = pd.to_numeric(out["position"], errors="coerce")
    return out

def _fmt(x):
    if x is None: return "–"
    try:
        xf = float(x);  return f"{int(xf):,}" if xf.is_integer() else f"{xf:,.1f}"
    except Exception:
        return html.escape(str(x))

def _build_parity_smb(serp_path, origin):
    df = _sv_norm_serp(_sv_read_csv(serp_path))
    if df.empty or "url" not in df.columns: return ""
    def _domain(u):
        try:
            u = u if isinstance(u,str) else ""
            if not u.startswith("http"): u = "http://" + u
            host = urlparse(u).netloc.lower()
            return host[4:] if host.startswith("www.") else host
        except Exception:
            return None
    df = df.copy()
    df['domain'] = df['url'].map(_norm_host)
    df = df.dropna(subset=["domain"])
    df = df[~df['domain'].apply(_is_platform)]
    if "position" in df.columns:
        df["w_all"] = 1.0 / df["position"].clip(lower=1)
        df["w_top3"] = (df["position"] <= 3).astype(float)
    else:
        df["w_all"] = 0.1; df["w_top3"] = 0.0
    agg = df.groupby("domain", as_index=False).agg(Hits=("domain","size"), W=("w_all","sum"), Top3=("w_top3","sum"))
    agg = agg.sort_values(["W","Top3","Hits"], ascending=[False, False, False]).head(12)
    tot_w = agg["W"].sum() or 1.0
    tot_w3 = agg["Top3"].sum() or 1.0
    agg["SoV%"] = (agg["W"]/tot_w*100).round(1)
    agg["Top-3 SoV%"] = (agg["Top3"]/tot_w3*100).round(1)
    # charts
    def chart(values, ticks):
        max_val = max([v for _,v in values] + [0.1])
        x0,x1=160,970
        def xw(v): return int(x0 + (v/max_val)*(x1-x0)) if max_val else x0
        lines=[]
        for t in ticks:
            x=int(x0+(t/max(ticks))*(x1-x0))
            lines.append(f"<line x1='{x}' y1='20' x2='{x}' y2='470' stroke='#eee' stroke-width='1'/>")
            lines.append(f"<text x='{x}' y='500' font-size='11' text-anchor='middle' fill='#666'>{t}</text>")
        y=20
        for lab,val in values:
            lines.append(f"<text x='152' y='{y+16}' font-size='12' text-anchor='end' fill='#333'>{html.escape(lab)}</text>")
            lines.append(f"<rect x='{x0}' y='{y}' width='{xw(val)-x0}' height='22' rx='6' ry='6' fill='var(--primary)' opacity='0.9'/>")
            lines.append(f"<text x='{xw(val)+6}' y='{y+16}' font-size='12' fill='#333'>{val}</text>")
            y+=30
        return "<div class='chart'><svg viewBox='0 0 1000 510'>" + "".join(lines) + "</svg></div>"
    sov_vals=[(r["domain"], r["SoV%"]) for _,r in agg.iterrows()]
    top3_vals=[(r["domain"], r["Top-3 SoV%"]) for _,r in agg.iterrows()]
    c1 = chart(sov_vals, list(range(0,11,2)))
    c2 = chart(top3_vals, list(range(0,9,2)))
    pill = '<div style="display:flex;gap:.5rem;align-items:center;margin-bottom:.5rem"><span style="font:500 .8rem/1.8 ui-sans-serif,system-ui; background:#eef6ff; color:#1e6bb8; border:1px solid #d7eaff; border-radius:999px; padding:.1rem .55rem;">Hiding large platforms</span></div>'
    mini1 = "<div class='mini'>SoV% is each domain's share of total SERP hits.</div>"
    mini2 = "<div class='mini'>Top‑3 SoV% is share of top‑3 placements.</div>"
    thead = "<tr><th>Domain</th><th>Hits</th><th>Top‑3</th><th>SoV%</th><th>Top‑3 SoV%</th></tr>"
    rows = "".join(f"<tr><td>{html.escape(str(r['domain']))}</td><td>{int(r['Hits'])}</td><td>{int(r['Top3'])}</td><td>{r['SoV%']}</td><td>{r['Top-3 SoV%']}</td></tr>" for _,r in agg.iterrows())
    tbl = f"<div style='margin-top:8px'><table class='tbl'>{thead}{rows}</table></div>"
    return "<div class='card' style='margin-top:12px'><h2>Competitor Parity</h2>" + pill + "<div class='grid' style='margin-top:8px'><div class='card span3'>" + c1 + mini1 + "</div><div class='card span3'>" + c2 + mini2 + "</div></div>" + tbl + "</div>"

def _find_card_bounds(html, heading_text):
    # Find <h2> heading instance
    m = re.search(r'(?is)<h2>\\s*'+re.escape(heading_text)+r'\\s*</h2>', html)
    if not m: return None
    i = m.start()
    # Walk backwards to the nearest "<div" that starts the card
    start = html.rfind('<div', 0, i)
    if start == -1: return None
    # Now scan forward counting <div ...> and </div>
    depth = 0
    j = start
    while j < len(html):
        # find next tag
        nxt = html.find('<', j)
        if nxt == -1: break
        if html.startswith('</div', nxt):
            depth -= 1
            j = html.find('>', nxt) + 1
            if depth <= 0:
                end = j
                return (start, end)
            continue
        elif html.startswith('<div', nxt):
            depth += 1
            j = html.find('>', nxt) + 1
            continue
        else:
            j = nxt + 1
    return None

def _apply_locked_sections(html, serp_path, gsc_path, origin):
    # 1) Ensure single Search Visibility card (rebuild real one)
    html = re.sub(r'(?is)<div\s+class="card"\s+id="search-visibility".*?</div>', '', html)
    vis = _build_visibility_card(serp_path, gsc_path, origin)
    m = re.search(r'(?is)(<p class="sub">.*?</p>)', html)
    if m:
        html = html[:m.end()] + "\n  " + vis + html[m.end():]
    else:
        html = vis + html
    # 2) Rebuild Competitor Parity (SMB-only, wildcard) and replace to next HTML comment
    try:
        new_card = rebuild_parity_smb(serp_path, origin)
    except Exception:
        new_card = ""
    if new_card:
        pat = re.compile(r'(?is)<div\s+class="card"[^>]*>\s*<h2>\s*Competitor\s+Parity\s*</h2>.*?(?=\n\s*<!--|\Z)')
        html = pat.sub(new_card, html, count=1)
    return html
# === end locked v2 ===



def _build_visibility_card(serp_path, gsc_path, origin):
    import pandas as pd, html
    # Readers & normalizers may already exist in this file; fall back minimally
    def _sv_read_csv(p):
        from pathlib import Path
        if not p: return pd.DataFrame()
        pp = Path(p)
        if not pp.exists(): return pd.DataFrame()
        for enc in ("utf-8","utf-8-sig","latin1"):
            try: return pd.read_csv(pp, encoding=enc)
            except Exception: continue
        return pd.DataFrame()
    def _sv_norm_serp(df):
        if df is None or df.empty: return pd.DataFrame()
        cols = {c.lower(): c for c in df.columns}
        q = cols.get("query") or cols.get("keyword") or cols.get("term")
        url = cols.get("url") or cols.get("page") or cols.get("landing_page")
        pos = cols.get("position") or cols.get("rank") or cols.get("avg_position") or cols.get("serp_position")
        out = df.copy()
        if q: out = out.rename(columns={q:"query"})
        if url: out = out.rename(columns={url:"url"})
        if pos: out = out.rename(columns={pos:"position"})
        if "position" in out: out["position"] = pd.to_numeric(out["position"], errors="coerce")
        return out
    def _sv_norm_gsc(df):
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
    serp = _sv_norm_serp(_sv_read_csv(serp_path))
    gsc  = _sv_norm_gsc(_sv_read_csv(gsc_path)) if gsc_path else pd.DataFrame()
    def _fmt(x):
        if x is None: return "–"
        try:
            xf = float(x);  return f"{int(xf):,}" if xf.is_integer() else f"{xf:,.1f}"
        except Exception:
            return html.escape(str(x))
    mapped = int(serp["query"].nunique()) if "query" in serp else 0
    pct_top10 = 0.0
    if "position" in serp and serp["position"].notna().any():
        total_with_pos = (serp["position"].notna()).sum()
        in10 = (serp["position"] <= 10).sum()
        pct_top10 = (in10 / total_with_pos * 100.0) if total_with_pos else 0.0
    avg_rank = float(serp["position"].mean()) if "position" in serp and serp["position"].notna().any() else None
    clicks = int(gsc["clicks"].sum()) if "clicks" in gsc else 0
    imps   = int(gsc["impressions"].sum()) if "impressions" in gsc else 0
    avg_pos = float(gsc["position"].mean()) if "position" in gsc and gsc["position"].notna().any() else None
    # High-Potential Queries
    high_p = pd.DataFrame()
    if not gsc.empty:
        keep = [c for c in ["query","clicks","impressions","ctr","position"] if c in gsc.columns]
        sort_cols = [c for c in ["impressions","clicks"] if c in keep]
        high_p = gsc[keep]
        if sort_cols: high_p = high_p.sort_values(by=sort_cols, ascending=False)
        if "position" in high_p.columns: high_p = high_p.sort_values(by=["impressions","position"], ascending=[False, True])
        high_p = high_p.head(10)
    # Best-Performing Keywords
    best_kw = pd.DataFrame()
    if not serp.empty and "query" in serp.columns:
        tmp = serp.dropna(subset=["position"]) if "position" in serp else serp
        if not tmp.empty:
            aggs = {"position":"min"}
            if "url" in tmp.columns: aggs["url"] = "first"
            best_kw = (tmp.groupby("query", as_index=False).agg(aggs).sort_values("position", ascending=True).head(10))
    # Coverage Gaps
    cov_gaps = pd.DataFrame()
    if "query" in gsc.columns:
        serp_best = pd.DataFrame()
        if "query" in serp.columns and "position" in serp.columns:
            serp_best = serp.groupby("query", as_index=False)["position"].min()
        cov_gaps = gsc[["query","impressions"]].copy() if "impressions" in gsc.columns else gsc[["query"]].copy()
        if not serp_best.empty: cov_gaps = cov_gaps.merge(serp_best, on="query", how="left")
        if "impressions" in cov_gaps.columns:
            cov_gaps = cov_gaps[(cov_gaps["impressions"] > 0) & (cov_gaps["position"].isna())].sort_values("impressions", ascending=False).head(10)
        else:
            cov_gaps = cov_gaps[cov_gaps["position"].isna()].head(10)
        cov_gaps = cov_gaps.drop(columns=[c for c in ["position"] if c in cov_gaps.columns])
    def _tbl(df, headers=None, max_rows=12):
        if df is None or df.empty:
            return '<div class="mini">No data available.</div>'
        safe = df.copy().reset_index(drop=True).head(max_rows)
        for c in safe.columns:
            if safe[c].dtype == object:
                safe[c] = safe[c].astype(str).map(lambda v: html.escape(v))
        if headers: safe = safe.rename(columns=headers)
        cols = list(safe.columns)
        thead = "".join(f"<th>{html.escape(str(h))}</th>" for h in cols)
        rows = []
        for _, r in safe.iterrows():
            tds = "".join(f"<td>{'' if pd.isna(r[c]) else r[c]}</td>" for c in cols)
            rows.append(f"<tr>{tds}</tr>")
        return '<div class="table-wrap"><table class="tbl"><thead><tr>'+thead+'</tr></thead><tbody>'+''.join(rows)+'</tbody></table></div>'
    kpis = (
        '<div class="kpi-grid">'
        f'<div class="kpi"><div class="kpi-label">Mapped Keywords</div><div class="kpi-value">{_fmt(mapped)}</div></div>'
        f'<div class="kpi"><div class="kpi-label">% in Top 10</div><div class="kpi-value">{_fmt(pct_top10)}%</div></div>'
        f'<div class="kpi"><div class="kpi-label">Avg Rank (SERP)</div><div class="kpi-value">{_fmt(avg_rank)}</div></div>'
        f'<div class="kpi"><div class="kpi-label">Impressions</div><div class="kpi-value">{_fmt(imps)}</div></div>'
        f'<div class="kpi"><div class="kpi-label">Clicks</div><div class="kpi-value">{_fmt(clicks)}</div></div>'
        f'<div class="kpi"><div class="kpi-label">Avg Pos (GSC)</div><div class="kpi-value">{_fmt(avg_pos)}</div></div>'
        '</div>'
    )
    return (
        '<div class="card" id="search-visibility" style="margin-top:12px">'
        '<h2>Search Visibility</h2>'
        + kpis +
        '<div class="grid-2" style="margin-top:8px">'
          '<div><h3>High-Potential Queries</h3>' + _tbl(high_p, headers={"query":"Query","clicks":"Clicks","impressions":"Impr.","ctr":"CTR","position":"Avg Pos"}) + '</div>'
          '<div><h3>Best-Performing Keywords</h3>' + _tbl(best_kw, headers={"query":"Keyword","position":"Best Rank","url":"URL"}) + '</div>'
        '</div>'
        '<div style="margin-top:10px"><h3>Coverage Gaps</h3>' + _tbl(cov_gaps, headers={"query":"Query","impressions":"Impr."}) + '</div>'
        '</div>'
    )



# === Baked-in platform stripper for Competitor Parity (charts + table) ===
import re as _re_strip

_PLATFORM_PATTERNS = [
    r"amazon", r"youtube", r"youtu", r"pinterest", r"reddit",
    r"facebook", r"instagram", r"tiktok",
    r"etsy", r"walmart", r"ebay",
    r"wikipedia", r"linkedin", r"quora", r"medium"
]
_PLAT_RE = _re_strip.compile(r"(?i)\b(" + "|".join(_PLATFORM_PATTERNS) + r")\b")

def _extract_parity_block(html: str):
    m = _re_strip.search(r'(?is)<h2>\s*Competitor Parity\s*</h2>', html)
    if not m: return None, None, None
    start = html.rfind('<div', 0, m.start())
    if start == -1: return None, None, None
    endm = _re_strip.search(r'(?is)\n\s*<!--', html[m.end():])
    end = (m.end() + endm.start()) if endm else len(html)
    return html[start:end], start, end

def _scrub_svg(svg_html: str) -> str:
    parts = _re_strip.split(r'(<text\b[^>]*>.*?</text>|<rect\b[^>]*>|<line\b[^>]*?/>)', svg_html)
    out = []
    i = 0
    while i < len(parts):
        frag = parts[i] or ""
        if frag.startswith('<text') and _PLAT_RE.search(frag):
            i += 1  # skip label
            skip = 0
            while i < len(parts) and skip < 2:
                if (parts[i] or "").strip():
                    skip += 1
                i += 1
            continue
        out.append(frag)
        i += 1
    return ''.join(out)

def _scrub_table(tbl_html: str) -> str:
    def repl_row(m):
        row = m.group(0)
        first_td = _re_strip.search(r'<td>(.*?)</td>', row)
        if first_td and _PLAT_RE.search(first_td.group(1)):
            return ''
        return row
    return _re_strip.sub(r'(?is)<tr>\s*<td>.*?</tr>', repl_row, tbl_html)

def _strip_platforms_inplace_html(html: str) -> str:
    blk, s, e = _extract_parity_block(html)
    if blk is None:
        return html
    blk2 = _re_strip.sub(r'(?is)(<svg\b.*?</svg>)', lambda m: _scrub_svg(m.group(1)), blk, count=2)
    blk3 = _re_strip.sub(r'(?is)(<table\b[^>]*>.*?</table>)', lambda m: _scrub_table(m.group(1)), blk2, count=1)
    if "Hiding large platforms" not in blk3:
        pill = '<div style="display:flex;gap:.5rem;align-items:center;margin:6px 0 6px"><span style="font:500 .8rem/1.8 ui-sans-serif,system-ui; background:#eef6ff; color:#1e6bb8; border:1px solid #d7eaff; border-radius:999px; padding:.1rem .55rem;">Hiding large platforms</span></div>'
        blk3 = blk3.replace("<h2>Competitor Parity</h2>", "<h2>Competitor Parity</h2>" + pill)
    return html[:s] + blk3 + html[e:]
# === end stripper ===



# === Rebuild Competitor Parity (SMB-only, wildcard) for clean charts ===
from urllib.parse import urlparse as _urlparse
import pandas as _pd, re as _re, html as _html

_WFAMILIES = [
  r"amazon", r"youtube", r"youtu", r"pinterest", r"reddit",
  r"facebook", r"instagram", r"tiktok",
  r"etsy", r"walmart", r"ebay",
  r"wikipedia", r"linkedin", r"quora", r"medium"
]
_WRE = _re.compile(r"(^|\.)(" + "|".join(_WFAMILIES) + r")\.[a-z]{2,}(?:\.[a-z]{2})?$", _re.I)

def _w_norm_host(u: str) -> str:
    if not isinstance(u, str) or not u: return ""
    if not u.startswith(("http://","https://")): u = "http://" + u
    host = _urlparse(u).netloc.lower()
    return host[4:] if host.startswith("www.") else host

def _w_is_platform(host: str) -> bool:
    return bool(_WRE.search(host or ""))

def _w_read_csv(p):
    from pathlib import Path as _P
    if not p: return _pd.DataFrame()
    pp = _P(p)
    if not pp.exists(): return _pd.DataFrame()
    for enc in ("utf-8","utf-8-sig","latin1"):
        try: return _pd.read_csv(pp, encoding=enc)
        except Exception: continue
    return _pd.DataFrame()

def _w_norm_serp(df):
    if df is None or df.empty: return _pd.DataFrame()
    cols = {c.lower(): c for c in df.columns}
    q = cols.get("query") or cols.get("keyword") or cols.get("term")
    url = cols.get("url") or cols.get("page") or cols.get("landing_page")
    pos = cols.get("position") or cols.get("rank") or cols.get("avg_position") or cols.get("serp_position")
    out = df.copy()
    if q: out = out.rename(columns={q:"query"})
    if url: out = out.rename(columns={url:"url"})
    if pos: out = out.rename(columns={pos:"position"})
    if "position" in out: out["position"] = _pd.to_numeric(out["position"], errors="coerce")
    return out

def _w_build_parity_smb_clean(serp_csv_path, origin):
    df = _w_norm_serp(_w_read_csv(serp_csv_path))
    if df.empty or "url" not in df.columns: return ""
    df = df.copy()
    df["domain"] = df["url"].map(_w_norm_host)
    df = df.dropna(subset=["domain"])
    df = df[~df["domain"].apply(_w_is_platform)]
    if "position" in df.columns:
        df["w_all"] = 1.0 / df["position"].clip(lower=1)
        df["w_top3"] = (df["position"] <= 3).astype(float)
    else:
        df["w_all"] = 0.1; df["w_top3"] = 0.0
    agg = df.groupby("domain", as_index=False).agg(Hits=("domain","size"), W=("w_all","sum"), Top3=("w_top3","sum"))
    agg = agg.sort_values(["W","Top3","Hits"], ascending=[False, False, False]).head(10)

    if agg.empty:
        return "<div class='card' style='margin-top:12px'><h2>Competitor Parity</h2><div class='mini'>No SMB competitors detected in SERP sample.</div></div>"

    tot_w = agg["W"].sum() or 1.0
    tot_w3 = agg["Top3"].sum() or 1.0
    agg["SoV%"] = (agg["W"]/tot_w*100).round(1)
    agg["Top-3 SoV%"] = (agg["Top3"]/tot_w3*100).round(1)

    # Clean, sequential chart rendering (no gaps)
    def chart(values):
        max_val = max([v for _,v in values] + [0.1])
        # heuristic ticks around actual max
        tick_max = max(10.0, (int(max_val) // 2 + 1) * 2)  # even tick, >=10
        ticks = list(range(0, int(tick_max)+1, 2))
        x0, x1 = 160, 970
        def xw(v): return int(x0 + (v / tick_max) * (x1 - x0))
        lines = []
        for t in ticks:
            x = int(x0 + (t / tick_max) * (x1 - x0))
            lines.append(f"<line x1='{x}' y1='20' x2='{x}' y2='470' stroke='#eee' stroke-width='1'/>")
            lines.append(f"<text x='{x}' y='500' font-size='11' text-anchor='middle' fill='#666'>{t}</text>")
        y = 20
        for label,val in values:
            lines.append(f"<text x='152' y='{y+16}' font-size='12' text-anchor='end' fill='#333'>{_html.escape(label)}</text>")
            bar_w = max(2, xw(val) - x0)  # min width so small bars are still visible
            lines.append(f"<rect x='{x0}' y='{y}' width='{bar_w}' height='22' rx='6' ry='6' fill='var(--primary)' opacity='0.9'/>")
            lines.append(f"<text x='{x0 + bar_w + 6}' y='{y+16}' font-size='12' fill='#333'>{val}</text>")
            y += 30
        return "<div class='chart'><svg viewBox='0 0 1000 510'>" + "".join(lines) + "</svg></div>"

    sov_vals = list(zip(agg["domain"].tolist(), agg["SoV%"].tolist()))
    top3_vals = list(zip(agg["domain"].tolist(), agg["Top-3 SoV%"].tolist()))
    c1 = chart(sov_vals)
    c2 = chart(top3_vals)
    pill = '<div style="display:flex;gap:.5rem;align-items:center;margin:6px 0 6px"><span style="font:500 .8rem/1.8 ui-sans-serif,system-ui; background:#eef6ff; color:#1e6bb8; border:1px solid #d7eaff; border-radius:999px; padding:.1rem .55rem;">Hiding large platforms</span></div>'
    mini1 = "<div class='mini'>SoV% is each domain's share of total SERP hits.</div>"
    mini2 = "<div class='mini'>Top‑3 SoV% is share of top‑3 placements.</div>"
    thead = "<tr><th>Domain</th><th>Hits</th><th>Top‑3</th><th>SoV%</th><th>Top‑3 SoV%</th></tr>"
    rows = "".join(f"<tr><td>{_html.escape(str(r['domain']))}</td><td>{int(r['Hits'])}</td><td>{int(r['Top3'])}</td><td>{r['SoV%']}</td><td>{r['Top-3 SoV%']}</td></tr>" for _,r in agg.iterrows())
    tbl = f"<div style='margin-top:8px'><table class='tbl'>{thead}{rows}</table></div>"
    return "<div class='card' style='margin-top:12px'><h2>Competitor Parity</h2>" + pill + "<div class='grid' style='margin-top:8px'><div class='card span3'>" + c1 + mini1 + "</div><div class='card span3'>" + c2 + mini2 + "</div></div>" + tbl + "</div>"

def _w_replace_parity(html, serp_csv_path, origin):
    # replace from <h2>Competitor Parity</h2> to next HTML comment or EOF
    new_card = _w_build_parity_smb_clean(serp_csv_path, origin)
    if not new_card: return html
    pat = _re.compile(r'(?is)<div\s+class="card"[^>]*>\s*<h2>\s*Competitor\s+Parity\s*</h2>.*?(?=\n\s*<!--|\Z)')
    return pat.sub(new_card, html, count=1)
# === end rebuild ===



# === Client copy cleaner (subtitle/footer) ===
import re as _re_clean

def _clean_client_copy(html: str) -> str:
    # Remove the dashboard subtitle if it's the stock internal line
    html = _re_clean.sub(r'(?is)\s*<p class="sub">.*?</p>', '', html, count=1)
    # Remove "Generated by ..." footer line
    html = _re_clean.sub(r'(?is)\s*<p class="mini"[^>]*>\s*Generated by[^<]*</p>', '', html, count=1)
    return html
# === end cleaner ===



# === Client-friendly subtitle + footer removal (baked-in) ===
import re as _re_cc

def _set_client_subtitle(html_text: str, subtitle: str) -> str:
    # Normalize whitespace in subtitle
    sub_html = f'<p class="sub">{subtitle.strip()}</p>' if subtitle.strip() else ''
    if not sub_html:
        return _re_cc.sub(r'(?is)\s*<p class="sub">.*?</p>', '', html_text, count=1)

    # If a subtitle exists, replace its contents; else insert after <h1>
    if _re_cc.search(r'(?is)<p class="sub">.*?</p>', html_text):
        return _re_cc.sub(r'(?is)<p class="sub">.*?</p>', sub_html, html_text, count=1)
    # Insert right after the H1
    m = _re_cc.search(r'(?is)(<h1[^>]*>.*?</h1>)', html_text)
    if m:
        cut = m.end()
        return html_text[:cut] + "\n  " + sub_html + html_text[cut:]
    # Fallback: prepend
    return sub_html + html_text

def _remove_generated_footer(html_text: str) -> str:
    return _re_cc.sub(r'(?is)\s*<p class="mini"[^>]*>\s*Generated by[^<]*</p>', '', html_text, count=1)
# === end client-friendly subtitle ===

def _strip_keyword_tracking_card(html_text: str) -> str:
    """
    Remove any top-level card whose <h2> contains 'Keyword Tracking' (e.g., 'Keyword Tracking (v6)')
    including the surrounding <div class="card"> ... </div> block.
    Then, as a fallback, rename any stray <h2>Keyword Tracking</h2> headings to a client-friendly copy.
    """
    pat = re.compile(r'(?is)<div\s+class="card"[^>]*>\s*<h2>\s*Keyword\s+Tracking.*?</h2>.*?</div>')
    html_text = pat.sub("", html_text)
    # Soft rename if any stray headings survived (nested or malformed cases)
    html_text = re.sub(
        r'(?is)<h2>\s*Keyword\s+Tracking(?:\s*\(.*?\))?\s*</h2>',
        "<h2>Search Visibility — Rankings &amp; Opportunities</h2>",
        html_text
    )
    return html_text



def main():
    ap = argparse.ArgumentParser(description="One-HTML Client Report (v5_6k)")
    ap.add_argument("--phase4", required=True, help="path to phase4_dashboard.xlsx")
    ap.add_argument("--phase3", help="path to phase3_report.xlsx")
    ap.add_argument("--phase2", help="path to phase2_report.xlsx")
    ap.add_argument("--origin", default="silentprincesstt.com", help="origin host for parity matching")
    ap.add_argument("--out", required=True, help="output HTML path")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--serp-samples", help="CSV from rank sampler (keyword,rank,url[,fetched_at])")
    ap.add_argument("--gsc", help="(optional) GSC queries CSV (query + impressions; common variants ok)")
    args = ap.parse_args()


    ph4 = _load_excel(args.phase4, ns="phase4")
    ph3 = _load_excel(args.phase3, ns="phase3") if _exists(args.phase3) else {}
    ph2 = _load_excel(args.phase2, ns="phase2") if _exists(args.phase2) else {}

    # site metrics
    m = derive_site_metrics(ph4)

    # KPI strip
    perf_txt = _pct_text(m["perf_lh"]); perf_w = _pct_num(m["perf_lh"])
    cwv_strict_txt = _pct_text(m["cwv_strict_pct"]); cwv_strict_w = _pct_num(m["cwv_strict_pct"])
    cwv_legacy_txt = _pct_text(m["cwv_legacy_pct"]); cwv_legacy_w = _pct_num(m["cwv_legacy_pct"])
    inp_good_txt = _pct_text(m["inp_good_pct"]); inp_good_w = _pct_num(m["inp_good_pct"])
    jsonld_txt = _pct_text(m["jsonld_pct"]); jsonld_w = _pct_num(m["jsonld_pct"])

    access_badge = ""
    img_with_alt_pct = m.get("img_with_alt_pct")
    if isinstance(img_with_alt_pct, (int,float)) and img_with_alt_pct >= 95:
        access_badge = '<div class="badge">Accessibility ✓ Alt text ≥95%</div>'

    # Offsite KPIs & Coverage
    offsite_block, offsite_info = build_offsite_block(ph2, debug=args.debug)
    kw_coverage_block, cov_info = build_keyword_coverage(ph2)

    # status svg
    status_svg = svg_status_bar(m["status"])

    # competitors
    cdf = derive_competitors(ph3) if ph3 else pd.DataFrame()
    if cdf is not None and not cdf.empty:
        par = parity_summary(cdf, args.origin)
        svg_sov = svg_bar(list(zip(cdf["domain"].tolist(), cdf["sov"].astype(float).tolist()))[:15], "Share of Voice (All)")
        svg_top3 = svg_bar(list(zip(cdf["domain"].tolist(), cdf["sov_top3"].astype(float).tolist()))[:15], "Top‑3 Share (All)")
        rows = []
        for _,r in cdf.head(20).iterrows():
            rows.append(f"<tr><td>{r['domain']}</td><td>{_fmt_int(r['hits'])}</td><td>{_fmt_int(r['top10'])}</td><td>{_fmt_int(r['top3'])}</td><td>{_fmt_float(r['sov'],1)}</td><td>{_fmt_float(r['sov_top10'],1)}</td><td>{_fmt_float(r['sov_top3'],1)}</td></tr>")
        table = f"<table class='tbl'><tr><th>Domain</th><th>Hits</th><th>Top‑10</th><th>Top‑3</th><th>SoV%</th><th>Top‑10 SoV%</th><th>Top‑3 SoV%</th></tr>{''.join(rows)}</table>"
        comp_block = f"""
  <div class="card" style="margin-top:12px">
    <h2>Competitor Parity</h2>
    <div class="grid" style="margin-top:8px">
      <div class="card span3">{svg_sov}<div class="mini">SoV% is each domain's share of total SERP hits.</div></div>
      <div class="card span3">{svg_top3}<div class="mini">Top‑3 SoV% is share of top‑3 placements.</div></div>
    </div>
    <div style="margin-top:8px">{table}</div>
  </div>"""
    else:
        comp_block = '<div class="card" style="margin-top:12px"><h2>Competitor Parity</h2><div class="mini">No competitor data available.</div></div>'

    # GSC snapshot + window + top queries + brand split
    gsc_rows, gsc_window = build_gsc_rows(ph2)
    gsc_df = _get_sheet(ph2, contains="gsc")
    gsc_clicks=gsc_impr=gsc_pos=None
    if gsc_df is not None:
        clicks=_num(gsc_df.get("clicks")); impr=_num(gsc_df.get("impressions")); pos=_num(gsc_df.get("avg_position"))
        gsc_clicks = float(clicks.dropna().sum()) if clicks is not None else None
        gsc_impr   = float(impr.dropna().sum()) if impr is not None else None
        gsc_pos = (
    float(pd.to_numeric(pd.Series(pos), errors="coerce").dropna().mean())
    if pos is not None else None
)
    brand_rows = build_brand_rows(ph2, args.origin)

    # INP culprits table (cap to 6 + more note)
    ai = ph4.get("Audit — Internal")
    inp_rows, inp_extra = derive_inp_culprits_table(ai if ai is not None and not ai.empty else pd.DataFrame(), show_n=6)
    inp_more = f"<div class='more'>+{inp_extra} more in full crawl</div>" if inp_extra>0 else ""

    # issue counts & internal link opps
    issues = build_issue_counts(ph4)
    link_opps_block, link_info = build_internal_link_opps(ph4, ph2, top_n=10)

    # advanced
    adv_block = build_advanced_block(ph4, m.get("img_with_alt_pct"))

    # new parity blocks
    structured_block = build_structured_data_block(ph4)
    sitemap_examples = build_sitemap_examples_block(ph4)
    quick_wins = build_quick_wins_block(ph4)

    # CWV explanation banner
    cwv_note = ""
    try:
        strict0 = (m["cwv_strict_pct"] is not None and float(m["cwv_strict_pct"]) == 0.0)
        inp_bad = (m["inp_p75"] is not None and float(m["inp_p75"]) > 200.0)
        if strict0 and inp_bad:
            cwv_note = f"<div class='note'>Core Web Vitals Strict is 0% primarily due to high INP (p75 ≈ {_fmt_int(m['inp_p75'])} ms). LCP/CLS may be passing; focus on input latency to lift this score.</div>"
    except Exception:
        pass

    gen_block = f'<p class="mini">Generated (UTC): {m["generated"]}</p>' if m.get("generated") else ""

    html = HTML.substitute(
        origin=args.origin,
        generated_block=gen_block,
        cwv_note=cwv_note,
        perf_txt=_pct_text(m["perf_lh"]), perf_w=_pct_num(m["perf_lh"]),
        cwv_strict_txt=_pct_text(m["cwv_strict_pct"]), cwv_strict_w=_pct_num(m["cwv_strict_pct"]),
        cwv_legacy_txt=_pct_text(m["cwv_legacy_pct"]), cwv_legacy_w=_pct_num(m["cwv_legacy_pct"]),
        inp_good_txt=_pct_text(m["inp_good_pct"]), inp_good_w=_pct_num(m["inp_good_pct"]),
        jsonld_txt=_pct_text(m["jsonld_pct"]), jsonld_w=_pct_num(m["jsonld_pct"]),
        pages=_fmt_int(m["pages"]),
        access_badge=('<div class="badge">Accessibility ✓ Alt text ≥95%</div>' if (m.get("img_with_alt_pct") is not None and m.get("img_with_alt_pct")>=95) else ""),
        offsite_block=offsite_block,
        kw_coverage_block=kw_coverage_block,
        status_svg=svg_status_bar(m["status"]),
        comp_block=comp_block,
        crit=_fmt_int(issues["critical"]), high=_fmt_int(issues["high"]), med=_fmt_int(issues["medium"]), low=_fmt_int(issues["low"]),
        issues_source=("Source: Audit — Issues" if (("Audit — Issues" in ph4) and (ph4["Audit — Issues"] is not None) and (not ph4["Audit — Issues"].empty)) else "Synthesized from Quality/Internal/Directives/Canonicals"),
        lcp_p75=(_fmt_int(m["lcp_p75"]) if m["lcp_p75"] is not None else "–"),
        inp_p75=(_fmt_int(m["inp_p75"]) if m["inp_p75"] is not None else "–"),
        cls_p75=(f"{m['cls_p75']:.2f}" if m["cls_p75"] is not None else "–"),
        gsc_clicks=_fmt_int(gsc_clicks), gsc_impr=_fmt_int(gsc_impr), gsc_pos=(f"{gsc_pos:.1f}" if gsc_pos is not None else "–"),
        gsc_window=gsc_window,
        brand_rows=brand_rows,
        inp_rows=inp_rows,
        inp_more=inp_more,
        gsc_rows=gsc_rows,
        structured_block=structured_block,
        sitemap_examples=sitemap_examples,
        quick_wins=quick_wins,
        adv_block=adv_block,
        link_opps_block=link_opps_block,
    )
    html = _apply_locked_sections(html, args.serp_samples, args.gsc, args.origin)


    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    html = _w_replace_parity(html, args.serp_samples, args.origin)
    html = _strip_keyword_tracking_card(html)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)
    print("Wrote", args.out)
    if args.debug:
        debug = {
            "phase2_sheets": list(ph2.keys()),
            "phase3_sheets": list(ph3.keys()),
        }
        print(json.dumps(debug, indent=2))

if __name__ == "__main__":
    main()
