"""Share-of-voice aggregation helpers.

These utilities normalise SERP samples and compute unbiased share of
voice metrics by keyword. They collapse duplicate URLs per keyword,
weight results by rank, and expose helper functions that downstream
scripts can reuse to keep the business logic consistent.
"""
from __future__ import annotations

from typing import Callable, Iterable, Optional, Sequence
from urllib.parse import urlparse

import pandas as pd

__all__ = ["normalize_domain", "compute_share_of_voice", "share_of_voice_from_rows"]


def normalize_domain(value: str | None) -> str:
    """Normalise a URL or hostname to a bare domain.

    Parameters
    ----------
    value:
        Raw URL or hostname. "None" or empty values return an empty
        string.
    """
    if not value:
        return ""
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        netloc = urlparse(value).netloc
    else:
        netloc = value
    netloc = netloc.split("@")[-1].split(":")[0]
    netloc = netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc


def _prepare_dataframe(
    df: pd.DataFrame,
    keyword_col: str,
    domain_col: str,
    rank_col: str,
    origin: str | None,
    exclude_domains: Optional[Iterable[str]],
    exclude_predicate: Optional[Callable[[str], bool]],
) -> pd.DataFrame:
    data = df[[keyword_col, domain_col, rank_col]].copy()
    data[keyword_col] = data[keyword_col].astype(str).str.strip()
    data[domain_col] = data[domain_col].astype(str).str.strip()
    data[rank_col] = pd.to_numeric(data[rank_col], errors="coerce")

    data = data.dropna(subset=[keyword_col, domain_col, rank_col])
    data = data[(data[keyword_col] != "") & (data[domain_col] != "")]
    if data.empty:
        return pd.DataFrame(columns=["keyword", "domain", "rank"])

    data["keyword"] = data[keyword_col].str.lower()
    data["domain"] = data[domain_col].map(normalize_domain)
    data["rank"] = data[rank_col].clip(lower=1)

    if origin:
        origin_norm = normalize_domain(origin)
        if origin_norm:
            data = data[data["domain"] != origin_norm]

    if exclude_domains:
        excludes = {normalize_domain(d) for d in exclude_domains if d}
        if excludes:
            data = data[~data["domain"].isin(excludes)]

    if exclude_predicate:
        data = data[~data["domain"].map(exclude_predicate)]

    data = data.dropna(subset=["keyword", "domain", "rank"])
    data = data[(data["keyword"] != "") & (data["domain"] != "")]
    return data


def compute_share_of_voice(
    df: pd.DataFrame,
    keyword_col: str = "keyword",
    domain_col: str = "domain",
    rank_col: str = "rank",
    origin: str | None = None,
    exclude_domains: Optional[Iterable[str]] = None,
    exclude_predicate: Optional[Callable[[str], bool]] = None,
) -> pd.DataFrame:
    """Aggregate share-of-voice metrics from a SERP sample.

    The function collapses each domain to its best rank per keyword,
    applies inverse-rank weighting, and computes share percentages for
    all, top-10 and top-3 presence. Results are sorted by overall share
    descending.
    """
    if df is None or df.empty:
        return pd.DataFrame(
            columns=["domain", "Hits", "Top10", "Top3", "SoV%", "Top-10 SoV%", "Top-3 SoV%"]
        )

    data = _prepare_dataframe(
        df, keyword_col, domain_col, rank_col, origin, exclude_domains, exclude_predicate
    )
    if data.empty:
        return pd.DataFrame(
            columns=["domain", "Hits", "Top10", "Top3", "SoV%", "Top-10 SoV%", "Top-3 SoV%"]
        )

    best = (
        data.sort_values(["keyword", "domain", "rank"])
        .groupby(["keyword", "domain"], as_index=False)["rank"]
        .min()
        .rename(columns={"rank": "best_rank"})
    )
    best["Hits"] = 1
    best["w_all"] = 1.0 / best["best_rank"]
    best["w_top10"] = (best["best_rank"] <= 10).astype(float)
    best["w_top3"] = (best["best_rank"] <= 3).astype(float)

    agg = (
        best.groupby("domain", as_index=False)
        .agg(
            Hits=("Hits", "sum"),
            Weight=("w_all", "sum"),
            Top10Weight=("w_top10", "sum"),
            Top3Weight=("w_top3", "sum"),
        )
        .reset_index(drop=True)
    )

    if agg.empty:
        return pd.DataFrame(
            columns=["domain", "Hits", "Top10", "Top3", "SoV%", "Top-10 SoV%", "Top-3 SoV%"]
        )

    agg["Hits"] = agg["Hits"].astype(int)
    agg["Top10"] = agg["Top10Weight"].round().astype(int)
    agg["Top3"] = agg["Top3Weight"].round().astype(int)

    total_weight = float(agg["Weight"].sum()) or 1.0
    total_top10 = float(agg["Top10Weight"].sum()) or 1.0
    total_top3 = float(agg["Top3Weight"].sum()) or 1.0

    agg["SoV%"] = (agg["Weight"] / total_weight * 100.0).round(1)
    agg["Top-10 SoV%"] = (agg["Top10Weight"] / total_top10 * 100.0).round(1)
    agg["Top-3 SoV%"] = (agg["Top3Weight"] / total_top3 * 100.0).round(1)

    agg = agg.sort_values(["SoV%", "Top3", "Top10", "Hits"], ascending=[False, False, False, False])
    agg = agg.reset_index(drop=True)

    # legacy column aliases for downstream compatibility
    agg["hits"] = agg["Hits"]
    agg["top10"] = agg["Top10"]
    agg["top3"] = agg["Top3"]
    agg["sov"] = agg["SoV%"]
    agg["sov_top10"] = agg["Top-10 SoV%"]
    agg["sov_top3"] = agg["Top-3 SoV%"]

    return agg[
        [
            "domain",
            "Hits",
            "Top10",
            "Top3",
            "SoV%",
            "Top-10 SoV%",
            "Top-3 SoV%",
            "hits",
            "top10",
            "top3",
            "sov",
            "sov_top10",
            "sov_top3",
        ]
    ]


def share_of_voice_from_rows(
    rows: Sequence[dict],
    origin: str | None = None,
    exclude_domains: Optional[Iterable[str]] = None,
    exclude_predicate: Optional[Callable[[str], bool]] = None,
) -> pd.DataFrame:
    """Convenience wrapper to compute share-of-voice from dict rows."""
    if not rows:
        return pd.DataFrame(
            columns=["domain", "Hits", "Top10", "Top3", "SoV%", "Top-10 SoV%", "Top-3 SoV%"]
        )
    df = pd.DataFrame(rows)
    cols = {c.lower(): c for c in df.columns}
    k = cols.get("keyword") or cols.get("query") or cols.get("term")
    d = cols.get("domain") or cols.get("host")
    r = cols.get("rank") or cols.get("position") or cols.get("pos")
    if not (k and d and r):
        return pd.DataFrame(
            columns=["domain", "Hits", "Top10", "Top3", "SoV%", "Top-10 SoV%", "Top-3 SoV%"]
        )
    return compute_share_of_voice(
        df,
        keyword_col=k,
        domain_col=d,
        rank_col=r,
        origin=origin,
        exclude_domains=exclude_domains,
        exclude_predicate=exclude_predicate,
    )
