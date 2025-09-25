import os, time, json, argparse, sys
import requests
import pandas as pd

# add scripts/common to sys.path so we can import csv_normalizer without a package
HERE = os.path.dirname(__file__)
COMMON = os.path.abspath(os.path.join(HERE, "..", "common"))
if COMMON not in sys.path:
    sys.path.insert(0, COMMON)

ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from scripts.lib.share_of_voice import compute_share_of_voice, normalize_domain

def serper_search(query, per_keyword, gl, hl, device, api_key, retries=4, base_delay=0.8):
    url = "https://google.serper.dev/search"
    payload = {"q": query, "num": max(10, min(int(per_keyword or 20), 100)), "gl": gl, "hl": hl, "autocorrect": True}
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    delay = base_delay
    for attempt in range(1, retries+1):
        try:
            r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt < retries:
                time.sleep(delay); delay *= 2
                continue
            return None

def to_domain(u):
    return normalize_domain(u)

def extract_hits(serp_json):
    hits = []
    if not serp_json: return hits
    blocks = {"organic": "organic", "peopleAlsoAsk": "paa", "shopping": "shopping", "local": "local"}
    for key, btype in blocks.items():
        for i, item in enumerate(serp_json.get(key, []) or [], start=1):
            link = item.get("link") or item.get("url") or item.get("linkUrl") or ""
            title = item.get("title") or item.get("name") or ""
            if not link: continue
            hits.append({"rank": i, "type": btype, "title": title, "url": link, "domain": to_domain(link)})
    return hits

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--origin", required=True)
    ap.add_argument("--keyword-map", required=True)
    ap.add_argument("--region", default="us")
    ap.add_argument("--lang", default="en")
    ap.add_argument("--device", default="desktop")
    ap.add_argument("--per-keyword", type=int, default=20)
    ap.add_argument("--keywords-limit", type=int, default=150)
    ap.add_argument("--out", default="data/outputs/phase3/competitors_ranked.csv")
    ap.add_argument("--out-hits", default="data/outputs/phase3/competitors_serp_hits.csv")
    ap.add_argument("--log", default="data/outputs/phase3/rate_usage_log.jsonl")
    args = ap.parse_args()

    api_key = os.getenv("SERPER_API_KEY")
    if not api_key: raise SystemExit("Missing SERPER_API_KEY in environment.")

    df_kw = pd.read_csv(args.keyword_map)
    kw_col = next((c for c in df_kw.columns if c.lower() in ("keyword","query","term")), df_kw.columns[0])
    keywords = [str(x).strip() for x in df_kw[kw_col].dropna().tolist() if str(x).strip()]
    if args.keywords_limit: keywords = keywords[:args.keywords_limit]

    max_q = int(os.getenv("SERPER_MAX_QUERIES", "400"))
    sleep_s = float(os.getenv("SERPER_SLEEP_S", "0.25"))

    qcount, all_hits = 0, []
    os.makedirs(os.path.dirname(args.log), exist_ok=True)
    with open(args.log, "a", encoding="utf-8") as logf:
        for kw in keywords:
            if qcount >= max_q: break
            resp = serper_search(kw, args.per_keyword, args.region, args.lang, args.device, api_key)
            logf.write(json.dumps({"kw": kw, "status": bool(resp), "ts": time.time()}) + "\n")
            qcount += 1
            time.sleep(sleep_s)
            if not resp: continue
            hits = extract_hits(resp)
            for h in hits:
                h.update({"keyword": kw, "fetched_at": pd.Timestamp.utcnow().isoformat(), "source": "serper:search"})
            all_hits.extend(hits)

    if not all_hits:
        print("No SERP hits extracted.")
        pd.DataFrame(columns=["keyword","rank","type","title","url","domain","fetched_at","source"]).to_csv(args.out_hits, index=False)
        pd.DataFrame(columns=["domain","hits","top3","top10","score"]).to_csv(args.out, index=False)
        return

    df_hits = pd.DataFrame(all_hits)
    os.makedirs(os.path.dirname(args.out_hits), exist_ok=True)
    df_hits.to_csv(args.out_hits, index=False)

    sov = compute_share_of_voice(df_hits, origin=args.origin)
    sov = sov.rename(columns={"Hits": "keywords", "Top10": "top10_keywords", "Top3": "top3_keywords"})
    sov["score"] = sov["hits"] + 2 * sov["top3"] + sov["top10"]
    df_ranked = sov.sort_values(["sov", "top3", "hits"], ascending=[False, False, False])
    df_ranked = df_ranked[
        ["domain", "hits", "top3", "top10", "sov", "sov_top3", "sov_top10", "score"]
    ]
    df_ranked.to_csv(args.out, index=False)
    print(f"Wrote {args.out} and {args.out_hits}")

if __name__ == "__main__":
    main()
