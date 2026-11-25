#!/usr/bin/env python3
"""TAAT BM25 検索エンジン（roundtable_brain.py から分離した純粋モジュール）。

- 元の DAAT ループと IEEE-754 ビット単位で同一のスコアを返す（terms 順に加算）。
- HAY_CACHE の現行版に紐づく遅延転置キャッシュ（term -> ((doc_idx, contrib), ...)）を持つ。
- 版更新時は未変更文書の (doc_idx, tf) を増分引き継ぎし、変更文書のみ再走査する（IEEE-754 ビット完全同一）。
- 検索順位は1ミリも変えない。
"""
from __future__ import annotations

import math

BM25_K1 = 1.2
BM25_B = 0.75
BM25_MAX_CACHED_TERMS = 4096


def build_hay_norms(hays: list[str], avgdl: float) -> list[float]:
    denom = max(1.0, float(avgdl))
    return [BM25_K1 * (1.0 - BM25_B + BM25_B * len(h) / denom) for h in hays]


def build_incremental_bm25_carry(
    prev_hays: list[str] | None,
    prev_entry: dict | None,
    new_hays: list[str],
    max_change_ratio: float = 0.25,
) -> dict | None:
    """前世代の term_raw_hits から現行版へ増分引き継ぎするための対応表を作る。

    haystacks_of の _HAY_ROWS 再利用により未変更文書は同一 str オブジェクト（同一 id）になる。
    同一文字列が複数文書に出る場合も全インデックスを厳密対応させ、変更行のみ再走査対象とする。
    """
    if not prev_hays or not prev_entry or not new_hays:
        return None
    prev_raw_hits = prev_entry.get("term_raw_hits")
    if not prev_raw_hits:
        return None
    # 前世代の id(str) -> 代表旧インデックス (同一文字列はどの旧インデックスでも tf が完全に同じ)
    old_id_to_idx: dict[int, int] = {}
    for oi, h in enumerate(prev_hays):
        hid = id(h)
        if hid not in old_id_to_idx:
            old_id_to_idx[hid] = oi
    # 各代表旧インデックス -> 新世代で同一 str を持つ全インデックス群
    old_to_new: dict[int, list[int]] = {}
    changed_new: list[int] = []
    for ni, h in enumerate(new_hays):
        oi = old_id_to_idx.get(id(h))
        if oi is not None:
            bucket = old_to_new.get(oi)
            if bucket is None:
                old_to_new[oi] = [ni]
            else:
                bucket.append(ni)
        else:
            changed_new.append(ni)
    if len(changed_new) > max(16, int(len(new_hays) * max_change_ratio)):
        return None
    rep_old_indices = set(old_to_new.keys())
    return {
        "prev_raw_hits": prev_raw_hits,
        "old_to_new": {oi: tuple(nis) for oi, nis in old_to_new.items()},
        "rep_old_indices": rep_old_indices,
        "changed_new": tuple(changed_new),
    }


def bm25_taat_scores(
    hays: list[str],
    avgdl: float,
    terms: list[str],
    total: int,
    idx_cache: dict | None = None,
) -> dict[int, float]:
    """Term-at-a-time BM25 with lazy per-term posting cache and cross-version incremental carry.
    Evaluates terms in exact `terms` order for bit-identical IEEE-754 scores vs DAAT loop."""
    scores: dict[int, float] = {}
    if not terms or not hays:
        return scores
    k1_p1 = BM25_K1 + 1.0
    use_cache = idx_cache is not None and len(hays) == total
    if use_cache:
        norms = idx_cache["norms"]
        postings_cache = idx_cache["term_postings"]
        raw_hits_cache = idx_cache.setdefault("term_raw_hits", {})
        carry = idx_cache.get("prev_carry")
        for term in terms:
            postings = postings_cache.get(term)
            if postings is None:
                raw_hits: list[tuple[int, int]] = []
                used_carry = False
                if carry is not None:
                    prev_term_hits = carry["prev_raw_hits"].get(term)
                    if prev_term_hits is not None:
                        old_to_new = carry["old_to_new"]
                        rep_old = carry["rep_old_indices"]
                        for oi, f in prev_term_hits:
                            if oi in rep_old:
                                for ni in old_to_new[oi]:
                                    raw_hits.append((ni, f))
                        for ni in carry["changed_new"]:
                            h = hays[ni]
                            if h and term in h:
                                f = h.count(term)
                                if f:
                                    raw_hits.append((ni, f))
                        if len(raw_hits) > 1:
                            raw_hits.sort(key=lambda pair: pair[0])
                        used_carry = True
                if not used_carry:
                    for i, h in enumerate(hays):
                        if h and term in h:
                            f = h.count(term)
                            if f:
                                raw_hits.append((i, f))
                df = len(raw_hits)
                if df:
                    idf_t = math.log((total - df + 0.5) / (df + 0.5) + 1.0)
                    postings = tuple(
                        (i, idf_t * (f * k1_p1) / (f + norms[i]))
                        for i, f in raw_hits
                    )
                else:
                    postings = ()
                if len(postings_cache) < BM25_MAX_CACHED_TERMS:
                    postings_cache[term] = postings
                    raw_hits_cache[term] = tuple(raw_hits)
            for i, contrib in postings:
                prev = scores.get(i)
                scores[i] = contrib if prev is None else prev + contrib
        return scores

    norms = build_hay_norms(hays, avgdl)
    for term in terms:
        raw_hits = []
        for i, h in enumerate(hays):
            if h and term in h:
                f = h.count(term)
                if f:
                    raw_hits.append((i, f))
        df = len(raw_hits)
        if not df:
            continue
        idf_t = math.log((total - df + 0.5) / (df + 0.5) + 1.0)
        for i, f in raw_hits:
            contrib = idf_t * (f * k1_p1) / (f + norms[i])
            prev = scores.get(i)
            scores[i] = contrib if prev is None else prev + contrib
    return scores
