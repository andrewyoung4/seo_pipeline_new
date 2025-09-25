#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
inject_cannibalization_card.py (patched)
---------------------------------------
- Inserts BOTH Rank Movements + Query Cannibalization cards as TOP-LEVEL siblings.
- Avoids nesting inside "Search Visibility" (previous issue).
- De-duplicates: removes previous injected blocks (by id) before inserting.
- Appends JS at end of <body> (not mid-card), and adds minimal CSS for the cannibalization table.
- Works with: --html <path>  (same CLI you used)
- Data files expected to sit alongside the HTML: rank_movements.json and related CSVs, cannibalization_card.json.
"""
import argparse, re, sys
from pathlib import Path

RANK_CARD = """<!-- BEGIN: Rank Movements Card -->
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
"""

CANNIBAL_CARD = """<!-- BEGIN: Query Cannibalization Card -->
<section class="card" id="query-cannibalization-card">
  <h2>Query Cannibalization</h2>
  <p class="muted">Queries where multiple URLs compete. Consolidate or clarify intent.</p>
  <div class="kpi-row">
    <div class="kpi"><div class="label">Total Conflicts</div><div class="value" id="qc-total">–</div></div>
  </div>
  <div class="grid one">
    <div>
      <h3>Top Conflicts</h3>
      <table id="qc-table"><thead>
        <tr>
          <th>Query</th>
          <th>Winner URL</th>
          <th>Winner&nbsp;Pos</th>
          <th>Losers</th>
          <th>Worst&nbsp;Loser</th>
          <th>Win&nbsp;%</th>
          <th>Note</th>
        </tr>
      </thead><tbody></tbody></table>
      <div id="qc-empty" class="muted" style="display:none">No query cannibalization detected in this dataset.</div>
    </div>
  </div>
</section>
<!-- END: Query Cannibalization Card -->
"""

CANNIBAL_CSS = r"""
  /* Cannibalization card polish (no vertical letters, sensible widths) */
  #query-cannibalization-card table{width:100%;border-collapse:collapse;table-layout:auto}
  #query-cannibalization-card th,#query-cannibalization-card td{
    padding:6px 8px;border-bottom:1px solid #e9e9e9;vertical-align:middle;white-space:nowrap
  }
  #query-cannibalization-card th{text-align:left;font-weight:600}
  #query-cannibalization-card td.url{max-width:520px;overflow:hidden;text-overflow:ellipsis}
  #query-cannibalization-card td, #query-cannibalization-card th { word-break:normal; overflow-wrap:normal; }
  #query-cannibalization-card th:nth-child(1){width:24%}
  #query-cannibalization-card th:nth-child(2){width:42%}
  #query-cannibalization-card th:nth-child(3){width:8%}
  #query-cannibalization-card th:nth-child(4){width:8%}
  #query-cannibalization-card th:nth-child(5){width:10%}
  #query-cannibalization-card th:nth-child(6){width:8%}
"""

SCRIPTS = r"""<script>
(async function(){
  try{
    const resp = await fetch('rank_movements.json', {cache:'no-store'});
    if(!resp.ok) throw 0;
    const data = await resp.json();
    const fmt = n => (n==null || isNaN(n)) ? '–' : new Intl.NumberFormat().format(n);
    const set = (id, v) => { const el = document.getElementById(id); if(el) el.textContent = fmt(v); };
    set('rm-up-7d', data.counts && data.counts.up_7d);
    set('rm-down-7d', data.counts && data.counts.down_7d);
    set('rm-new', data.counts && data.counts.new);
    set('rm-lost', data.counts && data.counts.lost);
    set('rm-striking', data.counts && data.counts.striking_distance);
  }catch(e){ console.warn('rank_movements.json missing', e); }

  async function loadCSV(name){
    try{
      const r = await fetch(name, {cache:'no-store'});
      if(!r.ok) return [];
      const t = await r.text();
      const lines = t.trim().split(/\r?\n/);
      const header = (lines.shift() || '').split(',');
      return lines.map(line => {
        const cells = line.split(',');
        const obj = {};
        header.forEach((h,i)=>{ obj[h]=cells[i]||''; });
        return obj;
      });
    }catch(e){ return []; }
  }
  function fillTable(id, rows, cols){
    const tb = document.querySelector(id + ' tbody');
    if(!tb) return;
    tb.innerHTML = rows.slice(0,20).map(r => '<tr>' + cols.map(c => '<td>'+ (r[c]??'') +'</td>').join('') + '</tr>').join('');
  }
  const striking = await loadCSV('rank_movements_striking_distance.csv');
  // Δ column currently computed offline; leave empty if not present
  fillTable('#rm-striking-table', striking, ['query','pos_0','delta'].map(c=> striking.length && (c in striking[0]) ? c : (c=='delta'?'Δ v7/28':'pos_0')));
  const up = await loadCSV('rank_movements_movers_up.csv');
  fillTable('#rm-movers-up', up, ['query','pos_0','pos_7','pos_28','delta']);
  const down = await loadCSV('rank_movements_movers_down.csv');
  fillTable('#rm-movers-down', down, ['query','pos_0','pos_7','pos_28','delta']);
})();
</script>

<script>
(async function(){
  function fmtNum(n){ return (n==null || isNaN(n)) ? '–' : new Intl.NumberFormat().format(n); }
  function fmtPct(n){ return (n==null || isNaN(n)) ? '–' : (Number(n).toFixed(1).replace(/\.0$/,'') + '%'); }
  function esc(s){ return String(s||'').replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
  try{
    const r = await fetch('cannibalization_card.json', {cache:'no-store'});
    if(!r.ok) throw 0;
    const data = await r.json();
    const rows = Array.isArray(data.top_conflicts) ? data.top_conflicts : [];
    const total = (typeof data.total_conflicts==='number') ? data.total_conflicts : rows.length;
    const totalEl = document.getElementById('qc-total'); if(totalEl) totalEl.textContent = fmtNum(total);
    const table = document.getElementById('qc-table');
    const empty = document.getElementById('qc-empty');
    if(!rows.length){
      if(table) table.style.display='none';
      if(empty) empty.style.display='block';
      return;
    }
    const tb = table.querySelector('tbody');
    tb.innerHTML = rows.slice(0,20).map(x => {
      const url = esc(x.winner_url||'');
      const link = url ? ('<a href=\"'+url+'\" target=\"_blank\" rel=\"noopener\">'+url+'</a>') : '–';
      return '<tr>'+
        '<td>'+esc(x.query)+'</td>'+
        '<td class=\"url\">'+link+'</td>'+
        '<td>'+fmtNum(x.winner_pos)+'</td>'+
        '<td>'+fmtNum(x.losers_count)+'</td>'+
        '<td>'+fmtNum(x.worst_loser_pos)+'</td>'+
        '<td>'+fmtPct(x.visibility_split)+'</td>'+
        '<td>'+esc(x.note||\"\")+'</td>'+
      '</tr>';
    }).join('');
  }catch(e){ console.warn('cannibalization_card.json missing', e); }
})();
</script>
"""

def strip_block(html: str, block_id: str) -> str:
    # Remove a previously injected section by id (and its surrounding comments if present)
    pattern = re.compile(
        rf"<!-- BEGIN:[^\\n]*?{re.escape(block_id)}.*?-->(.*?)<!-- END:[^\\n]*?{re.escape(block_id)}.*?-->",
        re.S
    )
    html2 = re.sub(pattern, "", html)
    # If comment wrappers are missing, remove a bare <section id="...">...</section>
    if html2 == html:
        bare = re.compile(rf"<section[^>]+id=[\"']{re.escape(block_id)}[\"'][\s\S]*?</section>", re.I)
        html2 = re.sub(bare, "", html2)
    return html2

def ensure_cannibal_css(head_html: str) -> str:
    if "Cannibalization card polish" in head_html:
        return head_html
    return re.sub(r"</style>\s*</head>", CANNIBAL_CSS + "\n</style>\n</head>", head_html, count=1, flags=re.I)

def insert_cards_as_siblings(html: str) -> str:
    # 1) Clean old blocks
    html = strip_block(html, "rank-movements-card")
    html = strip_block(html, "query-cannibalization-card")

    # 2) Add CSS in <head>
    html = re.sub(r"<head>(.*?)</head>", lambda m: "<head>"+ensure_cannibal_css(m.group(0))+"</head>", html, flags=re.S|re.I)

    # 3) Choose insertion point: before Search Visibility card if present; else after <div class="wrap">
    anchor = re.search(r'<div\s+class="card"\s+id="search-visibility"[^>]*>', html, flags=re.I)
    if anchor:
        pos = anchor.start()
        html = html[:pos] + RANK_CARD + "\n" + CANNIBAL_CARD + "\n" + html[pos:]
    else:
        wrap = re.search(r'<div\s+class="wrap"[^>]*>', html, flags=re.I)
        if wrap:
            pos = wrap.end()
            html = html[:pos] + "\n" + RANK_CARD + "\n" + CANNIBAL_CARD + "\n" + html[pos:]
        else:
            # as a last resort, before </body>
            html = re.sub(r"</body>", RANK_CARD + "\n" + CANNIBAL_CARD + "\n</body>", html, count=1, flags=re.I)

    # 4) Append scripts before </body> (dedupe by removing previous copies)
    html = re.sub(r"<script>\s*\(async function\(\)\{[\s\S]*?cannibalization_card\.json[\s\S]*?\}\)\(\);\s*</script>", "", html, flags=re.I)
    html = re.sub(r"<script>\s*\(async function\(\)\{[\s\S]*?rank_movements\.json[\s\S]*?\}\)\(\);\s*</script>", "", html, flags=re.I)
    html = re.sub(r"</body>", SCRIPTS + "\n</body>", html, count=1, flags=re.I)
    return html

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--html", required=True, help="Path to client_report_pro.html to inject")
    args = ap.parse_args()

    p = Path(args.html)
    html = p.read_text(encoding="utf-8", errors="ignore")
    new_html = insert_cards_as_siblings(html)

    p.write_text(new_html, encoding="utf-8")
    print(f"[ok] Injected cards into {p}")

if __name__ == "__main__":
    main()
