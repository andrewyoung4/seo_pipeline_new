#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, re, json
from datetime import timedelta
import pandas as pd
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

TRACKING_KEYS = ("utm_", "srsltid", "gclid", "fbclid", "_ga")

def read_csv(path):
    for enc in (None, "utf-8", "utf-8-sig", "cp1252"):
        try: return pd.read_csv(path, sep=None, engine="python", encoding=enc)
        except Exception: pass
    return pd.read_csv(path)

def infer(df, prefer=None, contains=None):
    prefer = prefer or []
    cols = [c.lower() for c in df.columns]
    if contains:
        for c in cols:
            if contains in c: return df.columns[cols.index(c)]
    for p in prefer:
        if p in cols: return df.columns[cols.index(p)]
    return None

def clean_domain(u):
    m = re.match(r"https?://([^/]+)", str(u), re.I)
    host = (m.group(1) if m else str(u)).lower()
    return re.sub(r"^www\\.", "", host)

def strip_tracking(u: str) -> str:
    try:
        pr = urlsplit(u)
        qs = [(k,v) for k,v in parse_qsl(pr.query, keep_blank_values=True)
              if not (k.lower().startswith(TRACKING_KEYS) or any(k.lower()==x for x in TRACKING_KEYS))]
        return urlunsplit((pr.scheme, pr.netloc, pr.path, urlencode(qs, doseq=True), pr.fragment))
    except Exception:
        return u

def main():
    ap = argparse.ArgumentParser(description="Cannibalization analyzer")
    ap.add_argument("--serp-samples", required=True)
    ap.add_argument("--origin", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--lookback-days", type=int, default=30)
    ap.add_argument("--top-n", type=int, default=20)
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    df = read_csv(args.serp_samples)

    # Infer columns
    qcol = infer(df, ["query","keyword"], None) or infer(df, [], "query")
    ucol = infer(df, ["url","page","landing_page"], None) or infer(df, [], "url")
    pcol = infer(df, ["position","rank"], None)
    dcol = infer(df, ["date"], "date")

    if not (qcol and ucol and dcol):
        raise SystemExit(f"Need at least date/query/url columns. Got: {list(df.columns)}")
    if pcol: df[pcol] = pd.to_numeric(df[pcol], errors="coerce")

    # Normalize + filter to origin domain
    df = df.rename(columns={qcol:"query", ucol:"url", dcol:"date", **({pcol:"position"} if pcol else {})})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date","query","url"])
    df = df[df["url"].map(lambda u: clean_domain(u)==args.origin.lower())]

    # Window
    if not df.empty:
        end = df["date"].max()
        start = end - timedelta(days=max(0, args.lookback_days-1))
        df = df[(df["date"]>=start) & (df["date"]<=end)]

    # LONG export
    cols = ["date","query","url"] + (["position"] if "position" in df.columns else [])
    canni_long = df[cols].sort_values(["date","query","position"]).copy()
    canni_long.to_csv(os.path.join(args.out_dir, "cannibalization_long.csv"), index=False)

    # SUMMARY (+ card rows)
    rows = []
    for (d,q), g in canni_long.groupby(["date","query"], dropna=True):
        urls = g["url"].tolist()
        if len(set(urls)) < 2:
            continue
        if "position" in g and g["position"].notna().any():
            pos = g.groupby("url")["position"].mean()
            winner = pos.idxmin()
            wpos   = float(pos.min())
            losers = [u for u in pos.index if u!=winner]
            worst  = float(pos.loc[losers].max()) if losers else None
            # 1/pos weighting -> winner share %
            weights = (1.0 / pos.clip(lower=1.0))
            win_pct = float(weights.loc[winner] / weights.sum() * 100.0) if weights.sum() else 100.0
        else:
            winner = g["url"].value_counts().idxmax()
            wpos   = None
            losers = [u for u in set(urls) if u!=winner]
            worst  = None
            win_pct= 50.0
        rows.append({
            "date": d.date().isoformat(), "query": q,
            "winner_url": strip_tracking(winner),
            "winner_pos": wpos,
            "losers_count": len(losers),
            "worst_loser_pos": worst,
            "visibility_split": round(win_pct,1),
            "note": ""
        })

    summary = pd.DataFrame(rows, columns=["date","query","winner_url","winner_pos","losers_count","worst_loser_pos","visibility_split","note"])
    summary = summary.sort_values(["losers_count","winner_pos"], ascending=[False, True])
    summary.to_csv(os.path.join(args.out_dir, "cannibalization_summary.csv"), index=False)

    # CARD JSON (no exports line; cleaner UI handles empty case)
    card_rows = summary.drop(columns=["date"]).head(args.top_n).to_dict(orient="records")
    card = {"total_conflicts": int(summary.shape[0]), "top_conflicts": card_rows}
    with open(os.path.join(args.out_dir, "cannibalization_card.json"), "w", encoding="utf-8") as f:
        json.dump(card, f, ensure_ascii=False, indent=2)

    print(f"[ok] cannibalization_* written to {args.out_dir} (conflicts={summary.shape[0]})")

if __name__ == "__main__":
    main()
