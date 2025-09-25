#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
gsc_pull_oauth.py — Pull GSC Search Analytics with user OAuth (no service account keys)

First run opens a browser for consent; token is cached for reuse.

Deps:
  pip install google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2 pandas
"""
import argparse, time
from typing import List
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]

def get_oauth_creds(client_secret_path: str, token_path: str) -> Credentials:
    creds = None
    try:
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    except Exception:
        creds = None
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())  # noqa: F821 (Request is optional here; omitted for brevity)
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
            # This spins up a local server and opens your browser
            creds = flow.run_local_server(port=0)
        with open(token_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return creds

def build_sc(creds):
    # Try new API first
    try:
        return ("searchconsole", build("searchconsole", "v1", credentials=creds, cache_discovery=False))
    except Exception:
        return ("webmasters", build("webmasters", "v3", credentials=creds, cache_discovery=False))

def query_batch(svc, site, start, end, dims: List[str], start_row: int, row_limit: int, device=None, country=None):
    body = {
        "startDate": start,
        "endDate": end,
        "dimensions": dims,
        "rowLimit": row_limit,
        "startRow": start_row,
        "dataState": "all",
        "type": "web",
    }
    dim_filters = []
    if device:
        dim_filters.append({"dimension": "device", "operator": "equals", "expression": device})
    if country:
        dim_filters.append({"dimension": "country", "operator": "equals", "expression": country.lower()})
    if dim_filters:
        body["dimensionFilterGroups"] = [{"filters": dim_filters}]
    return svc.searchanalytics().query(siteUrl=site, body=body).execute()

def main():
    ap = argparse.ArgumentParser(description="Pull GSC Search Analytics via OAuth (user sign-in).")
    ap.add_argument("--site", required=True, help="Exact GSC property URL, e.g. https://example.com/")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--client", default=r".\secrets\gsc_oauth_client.json", help="OAuth client file")
    ap.add_argument("--token",  default=r".\secrets\gsc_oauth_token.json",  help="Token cache file")
    ap.add_argument("--dimensions", default="query", help="Comma list: query,page,device,country,date")
    ap.add_argument("--row-limit", type=int, default=25000)
    ap.add_argument("--sleep", type=float, default=0.2)
    ap.add_argument("--device", choices=["DESKTOP","MOBILE","TABLET"])
    ap.add_argument("--country")
    args = ap.parse_args()

    dims = [d.strip().lower() for d in args.dimensions.split(",") if d.strip()]
    creds = get_oauth_creds(args.client, args.token)
    api_name, svc = build_sc(creds)

    rows, start_row = [], 0
    while True:
        try:
            resp = query_batch(svc, args.site, args.start, args.end, dims, start_row, args.row_limit, args.device, args.country)
        except HttpError as e:
            raise SystemExit(f"GSC API error: {e}")
        data = resp.get("rows", [])
        if not data: break
        for r in data:
            entry = {"clicks": r.get("clicks",0), "impressions": r.get("impressions",0),
                     "ctr": r.get("ctr",0), "position": r.get("position",0)}
            keys = r.get("keys", [])
            for i, d in enumerate(dims):
                entry[d] = keys[i] if i < len(keys) else None
            rows.append(entry)
        if len(data) < args.row_limit: break
        start_row += args.row_limit
        if args.sleep: time.sleep(args.sleep)

    if not rows:
        pd.DataFrame(columns=dims+["clicks","impressions","ctr","position"]).to_csv(args.out, index=False, encoding="utf-8")
        print(f"Wrote {args.out} (0 rows)."); return

    df = pd.DataFrame(rows)
    ordered = dims + [c for c in ["clicks","impressions","ctr","position"] if c in df.columns]
    df = df[ordered]
    for c in ["clicks","impressions","ctr","position"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Wrote {args.out} with {len(df)} rows via {api_name}.")

if __name__ == "__main__":
    main()
