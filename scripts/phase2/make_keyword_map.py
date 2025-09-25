
#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
[Replacement] make_keyword_map.py — canonical builder with --profile support

Now supports a JSON profile so you can swap niches without changing code.
Command-line flags override the profile values.

Profile JSON keys (all optional):
{
  "triage": "data/outputs/phase1/phase1_triage.xlsx",
  "audit": "data/outputs/audit/shopify_sf_audit.cleaned.xlsx",
  "gsc": "data/outputs/phase2/gsc_queries.csv",
  "serp": "data/outputs/phase3/serp_samples.csv",
  "catalog": "data/inputs/catalog/products.csv",
  "seeds": "data/inputs/seeds/keywords.txt",
  "expand": true,
  "per_seed": 4,
  "max_queries": 200,
  "must": ["crochet","amigurumi","plush","stuffed","pattern","handmade","keychain"],
  "include": ["frog","mushroom","axolotl","bee","turtle","octopus","cat","dog","bunny","cow","duck","flower"],
  "exclude": ["amazon","pinterest","reddit","youtube","free","download","pdf","svg","template","clipart","wallpaper"],
  "out": "data/inputs/phase2/keyword_map.csv"
}

Usage examples:
  # Use a profile only
  python .\scripts\phase2\make_keyword_map.py --profile .\data\config\niches\crochet_amigurumi.json

  # Profile + override the output path
  python .\scripts\phase2\make_keyword_map.py --profile .\data\config\niches\crochet_amigurumi.json --out .\data\inputs\phase2\keyword_map.csv

  # No profile (explicit flags)
  python .\scripts\phase2\make_keyword_map.py --triage ... --gsc ... --seeds ... --expand --per-seed 4 --max-queries 200 --out ...
"""
import argparse, os, re, json, time
import pandas as pd
import requests
from urllib.parse import urlparse

def _read_csv_safe(p):
    try:
        return pd.read_csv(p)
    except Exception:
        return pd.DataFrame()

def _read_xlsx_union(p):
    try:
        xl = pd.ExcelFile(p)
        return pd.concat([xl.parse(s) for s in xl.sheet_names], ignore_index=True)
    except Exception:
        try:
            return pd.read_excel(p, engine="openpyxl")
        except Exception:
            return pd.DataFrame()

def _slug_words(path):
    import re as _re
    slug = _re.sub(r"^/+|/+$", "", (path or "")).split("/")[-1]
    slug = _re.sub(r"[\W_]+", " ", slug)
    return _re.sub(r"\s+", " ", slug).strip()

def harvest_triage_audit(xlsx_path):
    df = _read_xlsx_union(xlsx_path)
    if df.empty: return []
    import re as _re
    seeds = []
    for c in df.columns:
        cn = str(c).lower()
        if _re.search(r"title", cn):
            seeds.extend(df[c].dropna().astype(str).tolist())
        if _re.search(r"\b(url|address|page url|final url|link)\b", cn):
            for u in df[c].dropna().astype(str).tolist():
                u = str(u).strip()
                if not u: continue
                if not _re.match(r"^[a-z]+://", u):
                    u = "https://" + u.lstrip("/")
                try:
                    p = urlparse(u)
                    w = _slug_words(p.path)
                    if w:
                        seeds.append(w)
                except Exception:
                    pass
    out = []
    for s in seeds:
        s = re.sub(r"\s+", " ", str(s)).strip().lower()
        if 2 <= len(s) <= 60:
            out.append(s)
    seen, uniq = set(), []
    for s in out:
        if s not in seen: seen.add(s); uniq.append(s)
    return uniq

def harvest_gsc(p):
    df = _read_csv_safe(p)
    if df.empty: return []
    qcol = None
    for c in df.columns:
        if re.sub(r"[^a-z]","",c.lower()) == "query":
            qcol = c; break
    if not qcol: return []
    qs = [re.sub(r"\s+"," ",str(v)).strip().lower() for v in df[qcol].dropna().tolist()]
    return list(dict.fromkeys(qs))

def harvest_serp(p):
    df = _read_csv_safe(p); 
    if df.empty: return []
    qcol = None
    for c in df.columns:
        if re.sub(r"[^a-z]","",c.lower()) == "query":
            qcol = c; break
    if not qcol: return []
    qs = [re.sub(r"\s+"," ",str(v)).strip().lower() for v in df[qcol].dropna().tolist()]
    return list(dict.fromkeys(qs))

def harvest_catalog(p):
    df = _read_csv_safe(p); 
    if df.empty: return []
    cols = [c for c in df.columns if re.sub(r"[^a-z]","",c.lower()) in ("title","handle")]
    seeds = []
    for c in cols:
        for v in df[c].dropna().astype(str).tolist():
            s = re.sub(r"[-_/]+", " ", v)
            s = re.sub(r"\s+", " ", s).strip().lower()
            if 2 <= len(s) <= 60:
                seeds.append(s)
    return list(dict.fromkeys(seeds))

def harvest_seedfile(p):
    if not p or not os.path.isfile(p): return []
    out = []
    with open(p,"r",encoding="utf-8") as f:
        for line in f:
            s = re.sub(r"\s+"," ", line.strip()).lower()
            if s: out.append(s)
    return list(dict.fromkeys(out))

def serper_expand(seed, api_key, per_seed=5):
    url = "https://google.serper.dev/search"
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    payload = {"q": seed, "gl":"us", "hl":"en", "num": 10}
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
        if r.status_code != 200: return []
        data = r.json()
    except Exception:
        return []
    out = []
    rel = data.get("relatedSearches") or []
    for item in rel[:per_seed]:
        q = (item.get("query") or "").strip().lower()
        if q:
            out.append(("related", q, ""))
            if len(out) >= per_seed: return out
    org = data.get("organic") or []
    for item in org:
        title = (item.get("title") or "").strip()
        if not title: continue
        q = re.sub(r"[\|\-–—]+.*$", "", title).strip().lower()
        if q:
            out.append(("organic", q, title))
            if len(out) >= per_seed: break
    return out

def compile_list(x):
    if x is None: return []
    if isinstance(x, list): return [str(i).strip().lower() for i in x if str(i).strip()]
    return [i.strip().lower() for i in str(x).split(",") if i.strip()]

def keep_query(q, must, include, exclude):
    ql = q.lower().strip()
    if len(ql) < 2 or len(ql) > 70: return False
    if any(x and x in ql for x in exclude): return False
    if must and not any(x and x in ql for x in must): return False
    if include and not any(x and x in ql for x in (include + must)): return False
    return True

def score_query(q, from_seed=False):
    score = 0
    ql = q.lower()
    # lightweight: reward short-ish, on-niche, product terms
    if any(x in ql for x in ["crochet","amigurumi","plush","stuffed","pattern","handmade","keychain"]): score += 3
    if any(x in ql for x in ["frog","mushroom","axolotl","bee","turtle","octopus","bunny","cow","duck","flower","ghost","heart","bouquet"]): score += 2
    if len(ql) <= 20: score += 1
    if from_seed: score += 1
    return score

def load_profile(pth):
    try:
        with open(pth, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise SystemExit(f"Profile read error: {e}")

def main():
    ap = argparse.ArgumentParser(description="Build keyword_map.csv (canonical, profile-aware).")
    ap.add_argument("--profile", help="Path to profile JSON")
    ap.add_argument("--triage"); ap.add_argument("--audit"); ap.add_argument("--gsc"); ap.add_argument("--serp"); ap.add_argument("--catalog"); ap.add_argument("--seeds")
    ap.add_argument("--expand", action="store_true"); ap.add_argument("--per-seed", type=int); ap.add_argument("--max-queries", type=int)
    ap.add_argument("--must"); ap.add_argument("--include"); ap.add_argument("--exclude")
    ap.add_argument("--out")
    args = ap.parse_args()

    cfg = {}
    if args.profile:
        cfg = load_profile(args.profile)

    # merge: CLI overrides profile
    def pick(k, default=None):
        v_cli = getattr(args, k.replace("-","_"))
        if v_cli is not None:
            return v_cli
        return cfg.get(k, default)

    triage  = pick("triage");  audit = pick("audit");  gsc = pick("gsc");  serp = pick("serp");  catalog = pick("catalog"); seeds_file = pick("seeds")
    expand  = bool(pick("expand", False))
    per_seed = int(pick("per_seed", 5))
    max_queries = int(pick("max_queries", 500))
    must = compile_list(pick("must", ""))
    include = compile_list(pick("include", ""))
    exclude = compile_list(pick("exclude", ""))
    out = pick("out", "data/inputs/phase2/keyword_map.csv")

    seeds = []
    if triage:  seeds += harvest_triage_audit(triage)
    if audit:   seeds += harvest_triage_audit(audit)
    if gsc:     seeds += harvest_gsc(gsc)
    if serp:    seeds += harvest_serp(serp)
    if catalog: seeds += harvest_catalog(catalog)
    seeds += harvest_seedfile(seeds_file)

    seen, uniq = set(), []
    for s in seeds:
        if s and s not in seen: seen.add(s); uniq.append(s)

    rows = [{"seed": s, "query": s, "source": "seed", "notes": ""} for s in uniq]

    if expand and uniq:
        api_key = os.environ.get("SERPER_API_KEY","").strip()
        if not api_key:
            print("WARN: --expand/expand=true but SERPER_API_KEY missing; skipping expansion.")
        else:
            total = 0
            for s in uniq:
                ex = serper_expand(s, api_key, per_seed=per_seed)
                for src, q, note in ex:
                    rows.append({"seed": s, "query": q, "source": src, "notes": note})
                    total += 1
                    if total >= max_queries: break
                if total >= max_queries: break

    df = pd.DataFrame(rows)
    if df.empty:
        os.makedirs(os.path.dirname(out), exist_ok=True)
        pd.DataFrame(columns=["seed","query","score","source","notes"]).to_csv(out, index=False)
        print(f"Wrote {out} (empty)."); return

    df["keep"] = df["query"].map(lambda q: keep_query(str(q), must, include, exclude))
    df = df[df["keep"]].drop(columns=["keep"])
    df["score"] = df.apply(lambda r: score_query(str(r["query"]), from_seed=(r["source"]=="seed")), axis=1)
    df = df.sort_values(by=["score"], ascending=False).drop_duplicates(subset=["query"], keep="first")
    if len(df) > max_queries: df = df.head(max_queries)

    os.makedirs(os.path.dirname(out), exist_ok=True)
    df[["seed","query","score","source","notes"]].to_csv(out, index=False, encoding="utf-8")
    print(f"Wrote {out} with {len(df)} queries (seeds={len(uniq)}, expand={expand}).")

if __name__ == "__main__":
    main()
