#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
GSC Index Coverage sampler via URL Inspection API.
Writes per-URL details + rolled-up summary, and can append to phase2_report.xlsx.

Usage (Windows PowerShell):
python .\scripts\phase2\gsc_index_coverage.py `
  --client .\secrets\gsc_oauth_client.json `
  --token  .\secrets\gsc_oauth_token.json `
  --site   https://silentprincesstt.com/ `
  --urls-csv .\data\inputs\phase2\triage_urls.csv `
  --out-dir .\data\outputs\phase2 `
  --phase2-xlsx .\data\outputs\phase2\phase2_report.xlsx `
  --max 250 --filter "/products/" --delay 0.25
"""
import os, re, time, json, argparse, csv
from typing import List, Dict, Any
import pandas as pd

# Optional deps only when appending to Excel
try:
    import openpyxl  # noqa: F401
except Exception:
    pass

# Google auth / API client
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]

def load_urls(path: str, pattern: str | None, max_n: int | None) -> List[str]:
    df = pd.read_csv(path)
    url_col = next((c for c in df.columns if c.lower() in ("url","address","page url","final url","link")), None)
    if not url_col:
        # try 'urls' or generic first column
        url_col = "url" if "url" in df.columns else df.columns[0]
    urls = df[url_col].astype(str).str.strip()
    if pattern:
        urls = urls[urls.str.contains(pattern, case=False, na=False)]
    urls = urls.dropna().drop_duplicates()
    if max_n:
        urls = urls.head(int(max_n))
    return urls.tolist()

def get_creds(client: str, token: str) -> Credentials:
    creds = None
    if os.path.exists(token):
        creds = Credentials.from_authorized_user_file(token, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())  # type: ignore
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client, SCOPES)
            creds = flow.run_local_server(port=0, prompt="consent")
        with open(token, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return creds

def inspect_batch(service, site: str, urls: List[str], delay: float) -> List[Dict[str, Any]]:
    out = []
    for i, u in enumerate(urls, 1):
        body = {"inspectionUrl": u, "siteUrl": site}
        try:
            r = service.urlInspection().index().inspect(body=body).execute()
        except HttpError as e:
            # Capture error payload if possible
            try:
                err = json.loads(e.content.decode("utf-8"))
            except Exception:
                err = {"error": str(e)}
            out.append({"url": u, "_error": json.dumps(err, ensure_ascii=False)})
            time.sleep(max(delay, 0.25))
            continue

        ir = r.get("inspectionResult", {})
        idx = ir.get("indexStatusResult", {}) or {}
        mob = ir.get("mobileUsabilityResult", {}) or {}
        rich = ir.get("richResultsResult", {}) or {}
        amp = ir.get("ampResult", {}) or {}

        row = {
            "url": u,
            "verdict": idx.get("verdict"),
            "coverage_state": idx.get("coverageState"),
            "indexing_state": idx.get("indexingState"),
            "robots_txt_state": idx.get("robotsTxtState"),
            "page_fetch_state": idx.get("pageFetchState"),
            "last_crawl_time": idx.get("lastCrawlTime"),
            "user_canonical": idx.get("userCanonical"),
            "google_canonical": idx.get("googleCanonical"),
            "sitemaps": ";".join(idx.get("sitemap", []) if isinstance(idx.get("sitemap"), list) else [idx.get("sitemap")] if idx.get("sitemap") else []),
            "referring_urls_count": len(idx.get("referringUrls", []) or []),
            # Quick flags
            "is_indexed_guess": _is_indexed(idx),
            "has_canonical_mismatch": _canon_mismatch(idx),
            # Mobile/Rich/AMP headlines (booleans/strings)
            "mobile_verdict": mob.get("verdict"),
            "rich_results_verdict": rich.get("verdict"),
            "amp_verdict": amp.get("verdict"),
        }
        out.append(row)
        time.sleep(delay)
    return out

def _is_indexed(idx: Dict[str, Any]) -> bool:
    cs = (idx.get("coverageState") or "").lower()
    # heuristics: states with 'indexed' or explicit 'URL is on Google' texts
    return ("indexed" in cs) or (cs == "url is on google")

def _canon_mismatch(idx: Dict[str, Any]) -> bool:
    u = (idx.get("userCanonical") or "").strip()
    g = (idx.get("googleCanonical") or "").strip()
    if not u or not g:
        return False
    # Compare host+path ignoring trailing slash & scheme
    def norm(x: str) -> str:
        x = x.strip()
        x = re.sub(r"^https?://", "", x, flags=re.I)
        x = re.sub(r"/+$", "", x)
        return x.lower()
    return norm(u) != norm(g)

def summarize(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame()

    df["coverage_state_norm"] = df["coverage_state"].fillna("").str.strip().replace("", "Unknown")
    df["indexing_state"] = df["indexing_state"].fillna("UNKNOWN")
    df["robots_txt_state"] = df["robots_txt_state"].fillna("UNKNOWN")
    df["page_fetch_state"] = df["page_fetch_state"].fillna("UNKNOWN")

    # rollups
    totals = len(df)
    idx_yes = int(df["is_indexed_guess"].sum())
    idx_no = totals - idx_yes
    canon_mismatch = int(df["has_canonical_mismatch"].sum())

    by_cov = df.groupby("coverage_state_norm", dropna=False, as_index=False)["url"].count().rename(columns={"url":"count"}).sort_values("count", ascending=False)
    by_idx_state = df.groupby("indexing_state", dropna=False, as_index=False)["url"].count().rename(columns={"url":"count"}).sort_values("count", ascending=False)
    by_robots = df.groupby("robots_txt_state", dropna=False, as_index=False)["url"].count().rename(columns={"url":"count"}).sort_values("count", ascending=False)
    by_fetch = df.groupby("page_fetch_state", dropna=False, as_index=False)["url"].count().rename(columns={"url":"count"}).sort_values("count", ascending=False)

    # Flatten into a tidy multi-section summary
    frames = []
    frames.append(pd.DataFrame([{
        "metric": "total_sampled_urls", "value": totals
    }, {
        "metric": "indexed_estimate", "value": idx_yes
    }, {
        "metric": "non_indexed_estimate", "value": idx_no
    }, {
        "metric": "canonical_mismatch_count", "value": canon_mismatch
    }]))
    by_cov.insert(0, "section", "coverage_state")
    by_idx_state.insert(0, "section", "indexing_state")
    by_robots.insert(0, "section", "robots_txt_state")
    by_fetch.insert(0, "section", "page_fetch_state")

    frames.extend([by_cov.rename(columns={"coverage_state_norm":"name"}),
                   by_idx_state.rename(columns={"indexing_state":"name"}),
                   by_robots.rename(columns={"robots_txt_state":"name"}),
                   by_fetch.rename(columns={"page_fetch_state":"name"})])
    summary = pd.concat(frames, ignore_index=True)
    return summary

def append_to_phase2_xlsx(details: pd.DataFrame, summary: pd.DataFrame, xlsx_path: str):
    mode = "a" if os.path.exists(xlsx_path) else "w"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl", mode="a" if mode=="a" else "w") as xw:
        if not details.empty:
            details.to_excel(xw, sheet_name="GSC Index Details", index=False)
        if not summary.empty:
            summary.to_excel(xw, sheet_name="GSC Index Summary", index=False)

def main():
    ap = argparse.ArgumentParser(description="Summarize GSC Index Coverage via URL Inspection API.")
    ap.add_argument("--client", required=True, help="Path to OAuth client JSON")
    ap.add_argument("--token", required=True, help="Path to cached token JSON")
    ap.add_argument("--site", required=True, help="GSC property URL (e.g., https://example.com/)")
    ap.add_argument("--urls-csv", required=True, help="CSV of URLs to inspect")
    ap.add_argument("--out-dir", required=True, help="Output directory for CSVs")
    ap.add_argument("--phase2-xlsx", help="Optional Phase-2 Excel workbook to append")
    ap.add_argument("--max", type=int, default=250, help="Max URLs to inspect")
    ap.add_argument("--filter", default=None, help="Substring/regex filter for URLs (e.g., /products/)")
    ap.add_argument("--delay", type=float, default=0.2, help="Delay between calls (seconds)")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    urls = load_urls(args.urls_csv, args.filter, args.max)
    if not urls:
        raise SystemExit("No URLs to inspect after filtering.")

    creds = get_creds(args.client, args.token)
    svc = build("searchconsole", "v1", credentials=creds, cache_discovery=False)

    rows = inspect_batch(svc, args.site, urls, max(args.delay, 0.1))
    details_df = pd.DataFrame(rows)
    details_csv = os.path.join(args.out_dir, "gsc_index_inspections.csv")
    details_df.to_csv(details_csv, index=False, quoting=csv.QUOTE_MINIMAL)

    summary_df = summarize(rows)
    summary_csv = os.path.join(args.out_dir, "gsc_index_summary.csv")
    summary_df.to_csv(summary_csv, index=False, quoting=csv.QUOTE_MINIMAL)

    if args.phase2_xlsx:
        try:
            append_to_phase2_xlsx(details_df, summary_df, args.phase2_xlsx)
        except Exception as e:
            print(f"[warn] Could not append to Excel: {e}")

    print("Wrote:", details_csv)
    print("Wrote:", summary_csv)
    if args.phase2_xlsx:
        print("Appended tabs to:", args.phase2_xlsx)

if __name__ == "__main__":
    main()
