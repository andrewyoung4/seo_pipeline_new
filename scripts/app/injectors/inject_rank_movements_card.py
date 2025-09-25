
#!/usr/bin/env python3
"""
inject_rank_movements_card.py
Inject a "Rank Movements" card under the "Keyword Information" section in client_report_pro.html.

It expects the CSV/JSON outputs from rank_trends.py to exist in --data-dir.
If files are missing, it will still inject an empty state with a helpful message.

Usage:
  python inject_rank_movements_card.py `
    --html .\data\outputs\phase4\client_report_pro.html `
    --data-dir .\data\outputs\phase4

This script edits the HTML in-place (backs up to *.bak before writing).
"""
import argparse
from pathlib import Path
import json

CARD_HTML = """\
<!-- BEGIN: Rank Movements Card -->
<section class="card" id="rank-movements-card">
  <h2>Rank Movements</h2>
  <p class="muted">7/28‑day movement summary with “striking distance” (positions 8–20).</p>
  <div class="kpi-row">
    <div class="kpi"><div class="kpi-label">Up (7d)</div><div class="kpi-value" id="rm-up-7d">–</div></div>
    <div class="kpi"><div class="kpi-label">Down (7d)</div><div class="kpi-value" id="rm-down-7d">–</div></div>
    <div class="kpi"><div class="kpi-label">New</div><div class="kpi-value" id="rm-new">–</div></div>
    <div class="kpi"><div class="kpi-label">Lost</div><div class="kpi-value" id="rm-lost">–</div></div>
    <div class="kpi"><div class="kpi-label">Striking Dist.</div><div class="kpi-value" id="rm-striking">–</div></div>
  </div>
  <div class="grid two">
    <div>
      <h3>Striking Distance</h3>
      <table id="rm-striking-table"><thead><tr><th>Query</th><th>Pos</th><th>Δ v7/28</th></tr></thead><tbody></tbody></table>
    </div>
    <div>
      <h3>Biggest Movers (7/28d)</h3>
      <table id="rm-movers-up"><thead><tr><th>Query</th><th>Now</th><th>v7</th><th>v28</th><th>Δ</th></tr></thead><tbody></tbody></table>
      <div class="spacer"></div>
      <table id="rm-movers-down"><thead><tr><th>Query</th><th>Now</th><th>v7</th><th>v28</th><th>Δ</th></tr></thead><tbody></tbody></table>
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
      const rows = t.trim().split(/\r?\n/).map(l => l.split(','));
      const header = rows.shift() || [];
      return rows.map(r => Object.fromEntries(r.map((v,i)=>[header[i], v])));
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
  striking.forEach(r => r["Δ v7/28"] = ( (r.pos_7||'') && (r.pos_28||'') ) ? '' : '');
  fillTable('#rm-striking-table', striking, ['query','pos_0','Δ v7/28']);
  const up = await loadCSV('rank_movements_movers_up.csv');
  fillTable('#rm-movers-up', up, ['query','pos_0','pos_7','pos_28','delta']);
  const down = await loadCSV('rank_movements_movers_down.csv');
  fillTable('#rm-movers-down', down, ['query','pos_0','pos_7','pos_28','delta']);
})();
</script>
"""

def inject_card(html_path: Path, data_rel_dir: str):
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    # Place after "Keyword Information" section if present; otherwise after first <h2>
    anchors = [
        "<h2>Keyword Information</h2>",
        "<h2>Keyword information</h2>",
    ]
    insert_idx = -1
    for a in anchors:
        idx = html.find(a)
        if idx != -1:
            # insert after the closing tag occurrence
            insert_idx = idx + len(a)
            break
    if insert_idx == -1:
        # fallback: after first <h2>
        idx = html.lower().find("<h2")
        if idx != -1:
            end = html.find(">", idx)
            insert_idx = end + 1
        else:
            # fallback: at end of body
            idxb = html.lower().rfind("</body>")
            insert_idx = idxb if idxb != -1 else len(html)

    # Make resources relative: we expect the JSON/CSVs to be next to the HTML file; no path changes needed.
    new_html = html[:insert_idx] + "\n" + CARD_HTML + "\n" + html[insert_idx:]
    # Backup then write
    bak = html_path.with_suffix(html_path.suffix + ".bak")
    bak.write_text(html, encoding="utf-8")
    html_path.write_text(new_html, encoding="utf-8")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--html", required=True, help="Path to client_report_pro.html")
    ap.add_argument("--data-dir", required=False, default=".", help="Directory where rank_movements.* files live")
    args = ap.parse_args()
    p = Path(args.html)
    if not p.exists():
        raise SystemExit(f"HTML not found: {p}")
    inject_card(p, args.data_dir)
    print(f"Injected Rank Movements card into: {p}")
    print(f"Backup created at: {p.with_suffix(p.suffix + '.bak')}")

if __name__ == "__main__":
    main()
