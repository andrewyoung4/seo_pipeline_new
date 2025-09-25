
#!/usr/bin/env python3
"""
update_report_cards_v3.py
One script to rule them all:
1) (Optional) Run your Keyword Information injector (inject_v6_into_report_PATCHED.py).
2) Compute Rank Trends (7/28d) and Query Cannibalization (from SERP or GSC).
3) Strip any prior injected blocks & normalize stray "\n" artifacts.
4) Inject the two cards as sibling sections after the "Keyword Information" card.

Usage examples:
  # SERP samples (preferred)
  python update_report_cards_v3.py ^
    --html .\data\outputs\phase4\client_report_pro.html ^
    --serp-samples .\data\outputs\phase3\serp_samples.csv ^
    --origin example.com

  # GSC daily queries
  python update_report_cards_v3.py ^
    --html .\data\outputs\phase4\client_report_pro.html ^
    --gsc-csv .\data\inputs\phase2\gsc_queries_daily.csv

Optional flags:
  --skip-v6                # don't run the v6 injector
  --v6-script PATH         # explicit path to inject_v6_into_report_PATCHED.py
  --no-rank-trends         # skip Rank Trends card
  --no-cannibalization     # skip Cannibalization card
  --out-dir PATH           # where to write JSON/CSVs (default: HTML folder)

Requires: pandas
"""

import argparse, sys, re, json, csv, runpy, os
from pathlib import Path
from datetime import timedelta

# ---------------- CSV readers ----------------
def _read_csv_generic(path):
    import pandas as pd
    df = pd.read_csv(path)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    col_date = next((c for c in df.columns if c in ("date","day","fetched_at")), None)
    col_query = next((c for c in df.columns if c in ("query","keyword","search_query")), None)
    col_url = next((c for c in df.columns if c in ("url","page","landing_page")), None)
    col_pos = None
    for c in ("position","rank","avg_position","average_position","current_position"):
        if c in df.columns:
            col_pos = c; break
    if not (col_date and col_query and col_pos):
        raise ValueError(f"Could not detect required columns. Found: {df.columns.tolist()}")
    import pandas as pd
    df[col_date] = pd.to_datetime(df[col_date], errors="coerce").dt.date
    df[col_pos] = pd.to_numeric(df[col_pos], errors="coerce")
    df = df.dropna(subset=[col_date, col_query, col_pos])
    df = df[(df[col_pos] > 0) & (df[col_pos] <= 100)]
    df = df.rename(columns={col_date:"date", col_query:"query", col_pos:"position"})
    if col_url:
        df = df.rename(columns={col_url:"url"})
    else:
        df["url"] = ""
    return df[["date","query","position","url"]]

def _read_serp_filtered(path, origin):
    import pandas as pd
    df = pd.read_csv(path)
    c_keyword = next((c for c in df.columns if c.lower() in ("keyword","query")), None)
    c_rank = next((c for c in df.columns if c.lower() in ("rank","position")), None)
    c_url = next((c for c in df.columns if c.lower() == "url"), None)
    c_date = next((c for c in df.columns if c.lower() in ("fetched_at","date","day")), None)
    if not all([c_keyword, c_rank, c_url, c_date]):
        return _read_csv_generic(path)
    m = df[c_url].astype(str).str.contains(origin or "", case=False, na=False) if origin else (df[c_url].astype(str).str.len()>0)
    df = df[m].copy()
    if df.empty:
        import pandas as pd
        return pd.DataFrame(columns=["date","query","position","url"])
    df["date"] = pd.to_datetime(df[c_date], errors="coerce").dt.date
    df["query"] = df[c_keyword].astype(str)
    df["url"] = df[c_url].astype(str)
    df["position"] = pd.to_numeric(df[c_rank], errors="coerce")
    df = df.dropna(subset=["date","query","position"])
    df = df[(df["position"] > 0) & (df["position"] <= 100)]
    return df[["date","query","position","url"]]

# ---------------- Rank Trends ----------------
def compute_rank_trends(df):
    import pandas as pd
    from datetime import timedelta
    if df.empty:
        raise ValueError("No rows available for Rank Trends. Check origin/domain filtering and input CSV.")
    agg = (df.groupby(["query","date"])["position"].min().reset_index())
    D0 = agg["date"].max()
    prior7 = D0 - timedelta(days=7)
    prior28 = D0 - timedelta(days=28)

    latest = agg[agg["date"]==D0][["query","position"]].rename(columns={"position":"pos_0"})
    def get_prior(pos_df, day, label):
        subset = pos_df[pos_df["date"]<=day].sort_values(["query","date"])
        prior = subset.groupby("query").tail(1)[["query","position"]].rename(columns={"position":label})
        return prior
    prev7 = get_prior(agg, prior7, "pos_7")
    prev28 = get_prior(agg, prior28, "pos_28")
    cur = latest.merge(prev7, on="query", how="left").merge(prev28, on="query", how="left")

    def classify(row, col_prev):
        p0 = row["pos_0"]; pprev = row[col_prev]
        if pd.isna(pprev): return "new"
        d = pprev - p0
        if d > 0.5: return "up"
        if d < -0.5: return "down"
        return "flat"
    cur["status_7"] = cur.apply(lambda r: classify(r,"pos_7"), axis=1)
    cur["status_28"] = cur.apply(lambda r: classify(r,"pos_28"), axis=1)
    cur["is_new"] = cur["pos_7"].isna() & cur["pos_28"].isna()

    had_prior = agg[agg["date"]<=prior7]["query"].unique().tolist()
    current_queries = latest["query"].unique().tolist()
    lost_queries = sorted(set(had_prior) - set(current_queries))
    lost_df = (agg[agg["query"].isin(lost_queries)].sort_values(["query","date"]).groupby("query").tail(1)[["query","position"]]
               .rename(columns={"position":"pos_prior"}))
    lost_df["lost_on"] = str(D0)

    def delta_best(row):
        import math
        p0 = row["pos_0"]; p7 = row["pos_7"]; p28 = row["pos_28"]
        ref = p7 if not (isinstance(p7,float) and (p7!=p7)) else p28
        if ref is None or (isinstance(ref,float) and (ref!=ref)): return None
        return ref - p0
    cur["delta"] = cur.apply(delta_best, axis=1)

    movers_up = cur.dropna(subset=["delta"]).sort_values("delta", ascending=False).head(50)
    movers_down = cur.dropna(subset=["delta"]).sort_values("delta", ascending=True).head(50)

    striking = cur[(cur["pos_0"]>=8) & (cur["pos_0"]<=20)].copy()
    striking["improve_to_top3"] = (striking["pos_0"] - 3).clip(lower=0)

    summary = {"as_of": str(D0),
               "counts": {"up_7d": int((cur["status_7"]=="up").sum()),
                          "down_7d": int((cur["status_7"]=="down").sum()),
                          "flat_7d": int((cur["status_7"]=="flat").sum()),
                          "new": int(cur["is_new"].sum()),
                          "lost": int(len(lost_df)),
                          "striking_distance": int(len(striking))}}
    return summary, striking, movers_up, movers_down, cur[cur["is_new"]], lost_df

# ---------------- Cannibalization ----------------
def compute_cannibalization(df):
    import pandas as pd
    if df.empty:
        raise ValueError("No rows available for Cannibalization. Check origin/domain filtering and input CSV.")
    D0 = df["date"].max()
    today = df[df["date"]==D0].copy()
    best = (today.sort_values(["query","url","position"])
                 .groupby(["query","url"], as_index=False)["position"].min())
    counts = best.groupby("query")["url"].nunique().reset_index(name="url_count")
    conflicts = counts[counts["url_count"]>=2]["query"]
    cann = best[best["query"].isin(conflicts)].copy()
    if cann.empty:
        import pandas as pd
        return D0, pd.DataFrame(columns=["query","winner_url","winner_pos","losers_count","worst_loser_pos","visibility_split","note"]), cann

    cann["rank_order"] = cann.groupby("query")["position"].rank(method="first")

    def vis(pos):
        try: return 1.0/float(pos)
        except: return 0.0

    rows = []
    for q, group in cann.groupby("query"):
        w = group.sort_values("position").iloc[0]
        loser_rows = group.sort_values("position").iloc[1:]
        losers_count = len(loser_rows)
        worst_loser_pos = loser_rows["position"].max() if losers_count else None
        total_vis = sum(vis(x) for x in group["position"])
        win_vis = vis(w["position"])
        vis_pct = (win_vis/total_vis*100.0) if total_vis>0 else 0.0
        note = ""
        try:
            from urllib.parse import urlparse
            wpath = urlparse(w["url"]).path
            shared = 0
            for _, lr in loser_rows.iterrows():
                if urlparse(lr["url"]).path.split("/")[1:3] == wpath.split("/")[1:3]:
                    shared += 1
            if shared >= 1:
                note = "Likely variants/siblings — consider canonical or merge signals."
        except: pass
        rows.append({"query": q, "winner_url": w["url"], "winner_pos": int(w["position"]),
                     "losers_count": int(loser_rows.shape[0]),
                     "worst_loser_pos": int(worst_loser_pos) if worst_loser_pos is not None else "",
                     "visibility_split": round(vis_pct,1), "note": note})
    import pandas as pd
    summary = pd.DataFrame(rows).sort_values(["losers_count","winner_pos"], ascending=[False, True])
    long = cann.sort_values(["query","position"]).copy()
    long["is_winner"] = long.groupby("query")["position"].rank(method="first")==1
    return D0, summary, long

# ---------------- Injection helpers ----------------
RANK_BEGIN = "<!-- BEGIN: Rank Movements Card -->"
RANK_END   = "<!-- END: Rank Movements Card -->"
CANN_BEGIN = "<!-- BEGIN: Query Cannibalization Card -->"
CANN_END   = "<!-- END: Query Cannibalization Card -->"

RANK_CARD = """\
<!-- BEGIN: Rank Movements Card -->
<section class="card" id="rank-movements-card">
  <h2>Rank Movements</h2>
  <p class="muted">7/28-day movement summary with “striking distance” (positions 8–20).</p>
  <div class="kpi-row">
    <div class="kpi"><div class="kpi-label">Up (7d)</div><div class="kpi-value" id="rm-up-7d">–</div></div>
    <div class="kpi"><div class="kpi-label">Down (7d)</div><div class="kpi-value" id="rm-down-7d">–</div></div>
    <div class="kpi"><div class="kpi-label">New</div><div class="kpi-value" id="rm-new">–</div></div>
    <div class="kpi"><div class="kpi-label">Lost</div><div class="kpi-value" id="rm-lost">–</div></div>
    <div class="kpi"><div class="kpi-label">Striking Dist.</div><div class="kpi-value" id="rm-striking">–</div></div>
  </div>
  <div class="grid">
    <div class="card span3">
      <h3>Striking Distance</h3>
      <table class="tbl" id="rm-striking-table"><thead><tr><th>Query</th><th>Pos</th></tr></thead><tbody></tbody></table>
    </div>
    <div class="card span3">
      <h3>Biggest Movers (7/28d)</h3>
      <table class="tbl" id="rm-movers-up"><thead><tr><th>Query</th><th>Now</th><th>v7</th><th>v28</th><th>Δ</th></tr></thead><tbody></tbody></table>
      <div class="spacer"></div>
      <table class="tbl" id="rm-movers-down"><thead><tr><th>Query</th><th>Now</th><th>v7</th><th>v28</th><th>Δ</th></tr></thead><tbody></tbody></table>
    </div>
  </div>
</section>
<!-- END: Rank Movements Card -->
<script>
(async function(){
  try{
    const resp = await fetch('rank_movements.json', {cache:'no-store'});
    const data = await resp.json();
    const fmt = n => (n==null || isNaN(n)) ? '–' : new Intl.NumberFormat().format(n);
    document.getElementById('rm-up-7d').textContent = fmt(data.counts.up_7d);
    document.getElementById('rm-down-7d').textContent = fmt(data.counts.down_7d);
    document.getElementById('rm-new').textContent = fmt(data.counts.new);
    document.getElementById('rm-lost').textContent = fmt(data.counts.lost);
    document.getElementById('rm-striking').textContent = fmt(data.counts.striking_distance);
  }catch(e){
    console.warn('rank_movements.json missing', e);
  }
  async function loadCSV(name){
    try{
      const r = await fetch(name, {cache:'no-store'});
      const t = await r.text();
      const lines = t.split(/\\r?\\n/).filter(x => x.length);
      const header = (lines.shift() || '').split(',');
      return lines.map(line => {
        const cols = line.split(',');
        const obj = {}; header.forEach((h,i)=> obj[h] = cols[i]);
        return obj;
      });
    }catch(e){
      return [];
    }
  }
  function fillTable(id, rows, cols){
    const tb = document.querySelector(id + ' tbody');
    if(!tb) return;
    tb.innerHTML = rows.slice(0,20).map(r => '<tr>' + cols.map(c => '<td>'+ (r[c]??'') +'</td>').join('') + '</tr>').join('');
  }
  const striking = await loadCSV('rank_movements_striking_distance.csv');
  fillTable('#rm-striking-table', striking, ['query','pos_0']);
  const up = await loadCSV('rank_movements_movers_up.csv');
  fillTable('#rm-movers-up', up, ['query','pos_0','pos_7','pos_28','delta']);
  const down = await loadCSV('rank_movements_movers_down.csv');
  fillTable('#rm-movers-down', down, ['query','pos_0','pos_7','pos_28','delta']);
})();
</script>
"""

CANN_CARD = """\
<!-- BEGIN: Query Cannibalization Card -->
<section class="card" id="query-cannibalization-card">
  <h2>Query Cannibalization</h2>
  <p class="muted">Queries where multiple URLs compete. Focus on consolidating or clarifying intent.</p>
  <div class="kpi-row">
    <div class="kpi"><div class="kpi-label">Total Conflicts</div><div class="kpi-value" id="qc-total">–</div></div>
  </div>
  <div class="card" style="margin-top:8px">
    <h3>Top Conflicts</h3>
    <table class="tbl" id="qc-table"><thead><tr><th>Query</th><th>Winner URL</th><th>Winner Pos</th><th>Losers</th><th>Worst Loser</th><th>Win %</th><th>Note</th></tr></thead><tbody></tbody></table>
    <p class="mini">Full exports: <code>cannibalization_summary.csv</code>, <code>cannibalization_long.csv</code></p>
  </div>
</section>
<!-- END: Query Cannibalization Card -->
<script>
(async function(){
  function fmt(n){ return (n==null || isNaN(n)) ? '–' : new Intl.NumberFormat().format(n); }
  function esc(s){ return (s||'').replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
  try {
    const r = await fetch('cannibalization_card.json', {cache:'no-store'});
    const data = await r.json();
    document.getElementById('qc-total').textContent = fmt(data.total_conflicts);
    const rows = (data.top_conflicts||[]).map(x => {
      return '<tr>' +
        '<td>'+esc(x.query)+'</td>' +
        '<td class="truncate">'+esc(x.winner_url)+'</td>' +
        '<td>'+fmt(x.winner_pos)+'</td>' +
        '<td>'+fmt(x.losers_count)+'</td>' +
        '<td>'+fmt(x.worst_loser_pos)+'</td>' +
        '<td>'+fmt(x.visibility_split)+'</td>' +
        '<td>'+esc(x.note||"")+'</td>' +
      '</tr>';
    }).join('');
    const tb = document.querySelector('#qc-table tbody');
    if (tb) tb.innerHTML = rows;
  } catch(e) {
    console.warn('cannibalization_card.json missing', e);
  }
})();
</script>
"""

def strip_blocks_and_artifacts(html):
    def remove_block(h, begin, end):
        while True:
            i = h.find(begin)
            if i == -1: break
            j = h.find(end, i)
            if j == -1: j = i + len(begin)
            else: j += len(end)
            h = h[:i] + h[j:]
        return h
    html = remove_block(html, RANK_BEGIN, RANK_END)
    html = remove_block(html, CANN_BEGIN, CANN_END)
    # Normalize literal "\n" artifacts
    html = re.sub(r'(?m)^[ \t]*\\n', '\n', html)
    html = re.sub(r'\\n(?=\s*<!--)', '\n', html)
    html = re.sub(r'\\n(?=\s*<section\b)', '\n', html)
    html = re.sub(r'\\n(?=\s*<div\b)', '\n', html)
    return html

def find_keyword_card_close(html):
    anchor = "<h2>Keyword Information</h2>"
    k = html.find(anchor)
    if k == -1:
        k = html.lower().find("<h2>keyword information</h2>")
        if k == -1:
            return -1
    open_idx = html.rfind('<div class="card', 0, k)
    if open_idx == -1:
        return -1
    i = open_idx
    depth = 0
    while i < len(html):
        m = re.search(r'<(/?)div\b', html[i:], flags=re.I)
        if not m: break
        tag_pos = i + m.start()
        is_close = (m.group(1) == '/')
        if not is_close:
            depth += 1
        else:
            depth -= 1
            if depth == 0:
                end_tag = re.search(r'>', html[tag_pos:])
                if end_tag:
                    return tag_pos + end_tag.end()
                else:
                    return tag_pos + 6
        i = tag_pos + 5
    return -1

def inject_cards(html):
    html = strip_blocks_and_artifacts(html)
    ins = find_keyword_card_close(html)
    if ins == -1:
        pivot = html.find("<!-- KPI STRIP -->")
        ins = pivot if pivot != -1 else html.lower().rfind("</body>")
        if ins == -1:
            ins = len(html)
    insertion = "\n" + RANK_CARD + "\n" + CANN_CARD + "\n"
    return html[:ins] + insertion + html[ins:]

def maybe_run_v6(v6_path, html_path):
    if not v6_path:  # try to auto-discover
        guesses = [
            Path("scripts/app/inject_v6_into_report_PATCHED.py"),
            Path("scripts/app/inject_v6_into_report.py"),
            Path("inject_v6_into_report_PATCHED.py"),
        ]
        for g in guesses:
            if g.exists():
                v6_path = g; break
    if not v6_path or not Path(v6_path).exists():
        print("[v6] injector not found; skipping Keyword Information injection.")
        return
    print("[v6] running", v6_path)
    # Run in a clean globals with __name__ == "__main__" semantics if script expects CLI args.
    # If that script expects CLI args, prefer to let it parse the same --html path via env var.
    # We'll set an env var the script can optionally read; if it ignores, no harm.
    os.environ["REPORT_HTML_PATH"] = str(html_path)
    try:
        runpy.run_path(str(v6_path), run_name="__main__")
    except SystemExit:
        # some scripts call sys.exit(); that's fine
        pass
    except Exception as e:
        print(f"[v6] Warning: injector raised {e}; continuing.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--html", required=True)
    ap.add_argument("--serp-samples", default=None)
    ap.add_argument("--gsc-csv", default=None)
    ap.add_argument("--origin", default=None)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--skip-v6", action="store_true")
    ap.add_argument("--v6-script", default=None)
    ap.add_argument("--no-rank-trends", action="store_true")
    ap.add_argument("--no-cannibalization", action="store_true")
    args = ap.parse_args()

    html_path = Path(args.html)
    if not html_path.exists():
        raise SystemExit(f"HTML not found: {html_path}")
    out_dir = Path(args.out_dir) if args.out_dir else html_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Run v6 injector first (optional)
    if not args.skip_v6:
        maybe_run_v6(args.v6_script, html_path)

    # 2) Prepare dataframe
    df = None
    if args.serp_samples or args.gsc_csv:
        if args.serp_samples:
            df = _read_serp_filtered(args.serp_samples, args.origin or "")
            if df.empty and not args.origin:
                df = _read_csv_generic(args.serp_samples)
        else:
            df = _read_csv_generic(args.gsc_csv)

    # 3) Compute + write files
    if df is not None:
        import pandas as pd
        if not args.no_rank_trends:
            summary, striking, movers_up, movers_down, new_df, lost_df = compute_rank_trends(df)
            (out_dir / "rank_movements.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
            striking.to_csv(out_dir / "rank_movements_striking_distance.csv", index=False, quoting=csv.QUOTE_MINIMAL)
            movers_up[["query","pos_0","pos_7","pos_28","delta"]].to_csv(out_dir / "rank_movements_movers_up.csv", index=False, quoting=csv.QUOTE_MINIMAL)
            movers_down[["query","pos_0","pos_7","pos_28","delta"]].to_csv(out_dir / "rank_movements_movers_down.csv", index=False, quoting=csv.QUOTE_MINIMAL)
            new_df[["query","pos_0","pos_7","pos_28"]].to_csv(out_dir / "rank_movements_new.csv", index=False, quoting=csv.QUOTE_MINIMAL)
            lost_df.to_csv(out_dir / "rank_movements_lost.csv", index=False, quoting=csv.QUOTE_MINIMAL)
        if not args.no_cannibalization:
            D0, summary_c, long_c = compute_cannibalization(df)
            summary_c.to_csv(out_dir / "cannibalization_summary.csv", index=False, quoting=csv.QUOTE_MINIMAL)
            long_c.to_csv(out_dir / "cannibalization_long.csv", index=False, quoting=csv.QUOTE_MINIMAL)
            top = summary_c.head(20).to_dict(orient="records")
            card = {"as_of": str(D0), "total_conflicts": int(summary_c.shape[0]), "top_conflicts": top}
            (out_dir / "cannibalization_card.json").write_text(json.dumps(card, indent=2), encoding="utf-8")

    # 4) Inject Rank + Cannibalization (idempotent)
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    bak = html_path.with_suffix(html_path.suffix + ".bak")
    bak.write_text(html, encoding="utf-8")
    new_html = inject_cards(html)
    html_path.write_text(new_html, encoding="utf-8")

    print("Report updated. Backup at:", bak)
    print("Outputs written to:", out_dir)

if __name__ == "__main__":
    main()
