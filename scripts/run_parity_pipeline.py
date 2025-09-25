#!/usr/bin/env python3
"""One-click runner for the competitor parity / share-of-voice pipeline.

The script optionally fetches fresh SERP samples (phase3_serp_sampler),
computes unbiased share-of-voice metrics with the shared helper, and
saves a CSV summary ready for reporting or injector scripts.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from scripts.lib.share_of_voice import compute_share_of_voice, normalize_domain

PLATFORM_EXCLUDES = {
    "amazon.com",
    "youtube.com",
    "youtu.be",
    "pinterest.com",
    "reddit.com",
    "facebook.com",
    "instagram.com",
    "tiktok.com",
    "etsy.com",
    "ebay.com",
    "walmart.com",
    "wikipedia.org",
    "linkedin.com",
    "quora.com",
    "medium.com",
}


def run_sampler(args) -> None:
    if not args.keywords:
        return
    serp_path = Path(args.serp_csv)
    if serp_path.exists() and not args.refresh_serp:
        print(f"[run_parity_pipeline] Reusing SERP sample at {serp_path}")
        return
    cmd = [
        sys.executable,
        str(ROOT / "phase3" / "phase3_serp_sampler.py"),
        "--in",
        str(args.keywords),
        "--out",
        str(serp_path),
        "--country",
        args.country,
        "--device",
        args.device,
        "--sleep",
        str(args.sleep),
    ]
    if args.allow_ddg:
        cmd.append("--allow-ddg")
    print("[run_parity_pipeline] Collecting SERP data…")
    subprocess.run(cmd, check=True)


def load_serp(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"SERP CSV not found: {path}")
    df = pd.read_csv(path)
    if df.empty:
        raise SystemExit(f"SERP CSV {path} has no rows.")
    cols = {c.lower(): c for c in df.columns}
    kw_col = cols.get("keyword") or cols.get("query") or cols.get("term")
    rank_col = cols.get("rank") or cols.get("position")
    url_col = cols.get("url") or cols.get("page")
    if not (kw_col and rank_col and url_col):
        raise SystemExit("SERP CSV must include keyword/query, rank/position, and url columns.")
    df = df.rename(columns={kw_col: "keyword", rank_col: "rank", url_col: "url"})
    df["domain"] = df["url"].map(normalize_domain)
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df = df.dropna(subset=["keyword", "domain", "rank"])
    return df


def compute_sov(df: pd.DataFrame, args) -> pd.DataFrame:
    excludes = None if args.keep_platforms else PLATFORM_EXCLUDES
    sov = compute_share_of_voice(
        df,
        keyword_col="keyword",
        domain_col="domain",
        rank_col="rank",
        origin=args.origin,
        exclude_domains=excludes,
    )
    sov = sov.rename(
        columns={"Hits": "Keywords", "Top10": "Top10Keywords", "Top3": "Top3Keywords"}
    )
    if args.min_keywords > 1:
        sov = sov[sov["Keywords"] >= args.min_keywords]
    sov = sov.sort_values(["SoV%", "Top3Keywords", "Keywords"], ascending=[False, False, False])
    return sov.reset_index(drop=True)


def format_preview(sov: pd.DataFrame, top: int) -> str:
    head = sov.head(top)
    lines = ["domain,SoV%,Top-3 SoV%,Keywords"]
    for _, row in head.iterrows():
        lines.append(
            f"{row['domain']},{row['SoV%']:.1f},{row['Top-3 SoV%']:.1f},{int(row['Keywords'])}"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SERP sampling + share-of-voice aggregation.")
    parser.add_argument("--origin", required=True, help="Origin site URL or domain")
    parser.add_argument("--keywords", type=Path, help="CSV with keyword column (optional)")
    parser.add_argument(
        "--serp-csv",
        type=Path,
        default=Path("data/outputs/phase3/serp_samples.csv"),
        help="Path to store/read sampled SERP rows",
    )
    parser.add_argument(
        "--out-csv",
        type=Path,
        default=Path("data/outputs/phase3/share_of_voice.csv"),
        help="Where to write the aggregated SoV table",
    )
    parser.add_argument("--country", default="us")
    parser.add_argument("--device", default="desktop", choices=["desktop", "mobile"])
    parser.add_argument("--sleep", type=float, default=0.2)
    parser.add_argument("--allow-ddg", action="store_true", help="Allow DuckDuckGo fallback in sampler")
    parser.add_argument("--refresh-serp", action="store_true", help="Force re-sampling even if CSV exists")
    parser.add_argument("--keep-platforms", action="store_true", help="Do not exclude large platforms")
    parser.add_argument("--min-keywords", type=int, default=1, help="Minimum keywords per domain to keep")
    parser.add_argument("--top", type=int, default=10, help="Preview top N rows in stdout")
    args = parser.parse_args()

    run_sampler(args)
    df = load_serp(args.serp_csv)
    sov = compute_sov(df, args)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    sov.to_csv(args.out_csv, index=False)
    print(f"[run_parity_pipeline] Saved share-of-voice table to {args.out_csv}")
    print(format_preview(sov, args.top))


if __name__ == "__main__":
    main()
