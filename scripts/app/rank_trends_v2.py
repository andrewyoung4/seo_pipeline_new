
#!/usr/bin/env python3
"""
rank_trends_v2.py
Compute rank movements (7/28d) and "striking distance" (positions 8–20)
from either:
  A) Full SERP samples (e.g., phase3_serp_sampler.py output with columns: keyword, rank, url, fetched_at)
     -> requires --origin to filter rows for your site (url contains origin)
  B) GSC query export with per-day positions
     -> columns like date, query, avg_position (or position)

Usage examples:
  python rank_trends_v2.py --serp-samples .\data\outputs\phase3\serp_samples.csv --origin silentprincesstt.com --out-dir .\data\outputs\phase4
  python rank_trends_v2.py --gsc-csv .\data\inputs\phase2\gsc_queries_daily.csv --out-dir .\data\outputs\phase4

Outputs:
  rank_movements.json
  rank_movements_striking_distance.csv
  rank_movements_movers_up.csv
  rank_movements_movers_down.csv
  rank_movements_new.csv
  rank_movements_lost.csv
"""
import argparse
import csv
import json
from pathlib import Path
from datetime import datetime, timedelta
import sys

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
            col_pos = c
            break
    if not (col_date and col_query and col_pos):
        raise ValueError(f"Could not detect required columns. Found: {df.columns.tolist()}")
    # Coerce date
    df[col_date] = pd.to_datetime(df[col_date], errors="coerce").dt.date
    df = df.dropna(subset=[col_date, col_query, col_pos])
    # Coerce numeric position
    df[col_pos] = pd.to_numeric(df[col_pos], errors="coerce")
    df = df.dropna(subset=[col_pos])
    # Bound positions
    df = df[(df[col_pos] > 0) & (df[col_pos] <= 100)]
    # Normalize
    df = df.rename(columns={col_date:"date", col_query:"query", col_pos:"position"})
    if col_url:
        df = df.rename(columns={col_url:"url"})
    else:
        df["url"] = ""
    return df

def _read_serp_samples_filtered(path, origin):
    """
    Read phase3 serp_samples.csv and keep only rows where url contains origin.
    Expected columns: keyword, rank, url, fetched_at
    """
    import pandas as pd
    df = pd.read_csv(path)
    cols = [c.lower() for c in df.columns]
    # Loose detection
    c_keyword = next((c for c in df.columns if c.lower() in ("keyword","query")), None)
    c_rank = next((c for c in df.columns if c.lower() in ("rank","position")), None)
    c_url = next((c for c in df.columns if c.lower() == "url"), None)
    c_date = next((c for c in df.columns if c.lower() in ("fetched_at","date","day")), None)
    if not all([c_keyword, c_rank, c_url, c_date]):
        # fallback to generic
        return _read_csv_generic(path)
    # Filter to our domain
    m = df[c_url].astype(str).str.contains(origin, case=False, na=False)
    df = df[m].copy()
    if df.empty:
        # No matches -> return empty with required columns to avoid crashes
        out = pd.DataFrame(columns=["date","query","position","url"])
        return out
    # Normalize
    df["date"] = pd.to_datetime(df[c_date], errors="coerce").dt.date
    df["query"] = df[c_keyword].astype(str)
    df["position"] = pd.to_numeric(df[c_rank], errors="coerce")
    df["url"] = df[c_url].astype(str)
    df = df.dropna(subset=["date","query","position"])
    df = df[(df["position"] > 0) & (df["position"] <= 100)]
    return df[["date","query","position","url"]]

def compute_movements(df):
    import pandas as pd
    if df.empty:
        raise ValueError("No rows for your site were found in the input. Ensure --origin matches your domain and your SERP/GSC files include it.")
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
        p0 = row["pos_0"]
        pprev = row[col_prev]
        if pd.isna(pprev):
            return "new"
        delta = pprev - p0
        if delta > 0.5:
            return "up"
        if delta < -0.5:
            return "down"
        return "flat"

    cur["status_7"] = cur.apply(lambda r: classify(r,"pos_7"), axis=1)
    cur["status_28"] = cur.apply(lambda r: classify(r,"pos_28"), axis=1)
    cur["is_new"] = cur["pos_7"].isna() & cur["pos_28"].isna()

    had_prior = agg[agg["date"]<=prior7]["query"].unique().tolist()
    current_queries = latest["query"].unique().tolist()
    lost_queries = sorted(set(had_prior) - set(current_queries))
    lost_df = (agg[agg["query"].isin(lost_queries)]
               .sort_values(["query","date"])
               .groupby("query").tail(1)[["query","position"]]
               .rename(columns={"position":"pos_prior"}))
    lost_df["lost_on"] = str(D0)

    import math
    def delta_best(row):
        p0 = row["pos_0"]
        p7 = row["pos_7"]
        p28 = row["pos_28"]
        ref = p7 if not (p7 is None or (isinstance(p7,float) and math.isnan(p7))) else p28
        if ref is None or (isinstance(ref,float) and (ref!=ref)):
            return None
        return ref - p0
    cur["delta"] = cur.apply(delta_best, axis=1)

    movers_up = cur.dropna(subset=["delta"]).sort_values("delta", ascending=False).head(50)
    movers_down = cur.dropna(subset=["delta"]).sort_values("delta", ascending=True).head(50)

    striking = cur[(cur["pos_0"]>=8) & (cur["pos_0"]<=20)].copy()
    striking["improve_to_top3"] = (striking["pos_0"] - 3).clip(lower=0)

    summary = {
        "as_of": str(D0),
        "counts": {
            "up_7d": int((cur["status_7"]=="up").sum()),
            "down_7d": int((cur["status_7"]=="down").sum()),
            "flat_7d": int((cur["status_7"]=="flat").sum()),
            "new": int(cur["is_new"].sum()),
            "lost": int(len(lost_df)),
            "striking_distance": int(len(striking)),
        }
    }
    return summary, striking, movers_up, movers_down, cur[cur["is_new"]], lost_df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--serp-samples", help="Path to serp_samples.csv", default=None)
    parser.add_argument("--gsc-csv", help="Path to GSC query export CSV (must include a date column)", default=None)
    parser.add_argument("--origin", help="Your domain (e.g., silentprincesstt.com) for filtering SERP samples", default=None)
    parser.add_argument("--out-dir", required=True, help="Output directory for JSON/CSVs")
    args = parser.parse_args()

    if not (args.serp_samples or args.gsc_csv):
        print("Provide --serp-samples or --gsc-csv", file=sys.stderr)
        sys.exit(2)

    import pandas as pd
    if args.serp_samples:
        df = _read_serp_samples_filtered(args.serp_samples, args.origin or "")
        if args.origin and df.empty:
            raise SystemExit(f"No rows for origin '{args.origin}' found in {args.serp_samples}.")
        if df.empty and not args.origin:
            # Fall back to generic parse if origin not provided
            df = _read_csv_generic(args.serp_samples)
    else:
        df = _read_csv_generic(args.gsc_csv)

    summary, striking, movers_up, movers_down, new_df, lost_df = compute_movements(df)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "rank_movements.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    def to_csv_safe(frame, name):
        import csv
        frame.to_csv(out_dir / name, index=False, quoting=csv.QUOTE_MINIMAL)

    to_csv_safe(striking, "rank_movements_striking_distance.csv")
    to_csv_safe(movers_up[["query","pos_0","pos_7","pos_28","delta"]], "rank_movements_movers_up.csv")
    to_csv_safe(movers_down[["query","pos_0","pos_7","pos_28","delta"]], "rank_movements_movers_down.csv")
    to_csv_safe(new_df[["query","pos_0","pos_7","pos_28"]], "rank_movements_new.csv")
    to_csv_safe(lost_df, "rank_movements_lost.csv")

    print("Wrote rank movement files to:", out_dir)

if __name__ == "__main__":
    main()
