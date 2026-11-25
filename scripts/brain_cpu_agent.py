#!/usr/bin/env python3
"""ニライBrain 二本柱「cpu_agent モード（ローカルAIなし・純CPU広め拾い＋エージェント判断）」
および第18波特異トークン被覆率ガードの純粋モジュール（Python 標準ライブラリのみ）。

- GPU・ローカルAI・ネットワーク・外部パッケージ依存ゼロ。
- roundtable_brain.py から分離し、本体肥大化を防ぎつつ頭脳単体貸し出し時の可読性・再利用性を高める。
- クエリ文字列やトークン本文を diagnostics に一切残さない非リーク設計。
"""
from __future__ import annotations

import contextlib
import contextvars
import math
import os
import re
import sys

from brain_text_utils import item_key, normalize

# ---------------------------------------------------------------------------
# 第19波: cpu_agent モード定数・コンテキストガード
# ---------------------------------------------------------------------------

BRAIN_MODE_ENV = "KINGDOM_BRAIN_MODE"
CPU_AGENT_DEFAULT_LIMIT = 12
AGENT_CONFIDENCE_SCHEMA = "kingdom.roundtable.shared_brain.agent_confidence.v1"

_CPU_AGENT_ACTIVE: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "_cpu_agent_active", default=False
)
_CPU_AGENT_ALIASES = frozenset({"cpu_agent", "cpu", "no_local_ai", "cpu_only"})

_AGENT_CONFIDENCE_GUIDANCE = {
    "high": "上位候補に高い語彙・核心語一致があります。本文と日付・訂正履歴を確認して根拠を特定してください。",
    "medium": "上位候補群に関連記録が含まれる可能性が高いです。上位候補を読み比べて最終判断してください。",
    "low": "語彙一致が薄いため、上位候補の本文精査と別表現・関連語での再検索を推奨します。",
    "likely_absent": "該当する記録が共有脳に存在しない可能性が高いです。候補本文を鵜呑みにせず、別キーワードでの再検索または『記録なし』判断を優先してください。",
}


def resolve_brain_mode(brain_mode: str = "") -> str:
    """検索モードを解決する。既定は 'auto'（現行GPU最大精度モード）。'cpu_agent' でローカルAI完全非依存モード。"""
    raw = str(brain_mode or os.environ.get(BRAIN_MODE_ENV, "")).strip().lower()
    if raw in _CPU_AGENT_ALIASES:
        return "cpu_agent"
    return "auto"


@contextlib.contextmanager
def cpu_agent_scope(active: bool):
    """cpu_agent 実行中のみ ContextVar を立て、子関数からの間接ローカルAI呼出をスレッドセーフに遮断する。"""
    if not active:
        yield
        return
    tok = _CPU_AGENT_ACTIVE.set(True)
    try:
        yield
    finally:
        _CPU_AGENT_ACTIVE.reset(tok)


def is_cpu_agent_active() -> bool:
    return bool(_CPU_AGENT_ACTIVE.get())


# ---------------------------------------------------------------------------
# 第18波: 特異トークン Top-3 被覆率ガード（低 rerank ハードネガティブ対策）
# ---------------------------------------------------------------------------

LOW_RR_SPEC_ABSTAIN_KITEI = 0.020
LOW_RR_SPEC_COV_KITEI = 0.50

_SPEC_TOKEN_RE = re.compile(
    r"[A-Za-z][A-Za-z0-9_.\-/+#]{1,}|[0-9]+(?:\.[0-9]+)?[A-Za-z%]+|[ァ-ヶー]{3,}"
)
_SPEC_TOKEN_STOP = frozenset({
    "システム", "データ", "ファイル", "エラー", "チェック", "テスト", "モデル",
    "ルール", "ガイド", "リスト", "レベル", "ステータス", "バージョン", "ログ",
    "スコア", "ツール", "タスク", "コード", "サーバ", "サーバー", "メッセージ",
    "コマンド", "スクリプト", "メモリ", "プロセス", "ベース", "ライン", "タイム",
    "サイズ", "タイプ", "モード", "ゲート", "ベンチ", "ガード", "フラグ",
})


def low_rr_spec_abstain_shikii() -> float:
    """低 rerank × 特異トークン不在の公開棄権しきい値。0.0 以下で無効（即時ロールバック）。"""
    try:
        v = float(os.environ.get("KINGDOM_LOW_RR_SPEC_ABSTAIN", str(LOW_RR_SPEC_ABSTAIN_KITEI)))
    except (TypeError, ValueError):
        return LOW_RR_SPEC_ABSTAIN_KITEI
    if not math.isfinite(v):
        return LOW_RR_SPEC_ABSTAIN_KITEI
    return v


def extract_spec_tokens(query: str) -> list[str]:
    """クエリから固有名詞・技術名・型番・専門カタカナ等の特異トークンを抽出する。"""
    norm = normalize(str(query or ""))
    seen: set[str] = set()
    out: list[str] = []
    for raw in _SPEC_TOKEN_RE.findall(norm):
        tok = raw.strip(".-_/+")
        if len(tok) < 2 or tok.isdigit() or tok in _SPEC_TOKEN_STOP:
            continue
        if tok not in seen:
            seen.add(tok)
            out.append(tok)
    return out


def spec_token_matches_text(tok: str, norm_text: str) -> bool:
    if tok in norm_text:
        return True
    # カタカナ末尾長音ゆれ（例: サーバー/サーバ）の陽性誤棄権を防ぐ
    if len(tok) >= 4 and tok.endswith("ー") and tok[:-1] in norm_text:
        return True
    return False


def _lookup_cached_by_key(items: list[dict] | None) -> dict[str, dict] | None:
    """roundtable_brain._HAY_INDEX_OBOE に同一 items オブジェクトの by_key 辞書があれば O(1) 再利用する。"""
    if items is None:
        return None
    for mod_name in ("roundtable_brain", "__main__"):
        mod = sys.modules.get(mod_name)
        oboe = getattr(mod, "_HAY_INDEX_OBOE", None) if mod is not None else None
        if not isinstance(oboe, dict):
            continue
        for entry in oboe.values():
            if isinstance(entry, dict) and entry.get("items_ref") is items:
                bk = entry.get("by_key")
                if isinstance(bk, dict):
                    return bk
    return None


def _resolve_top3_items(
    results: list[dict],
    items: list[dict] | None,
    by_key: dict[str, dict] | None = None,
) -> tuple[list[dict], list[str], dict[str, dict]]:
    top_rows = [r for r in results[:3] if isinstance(r, dict)]
    top_keys = [item_key(r) for r in top_rows]
    wanted = set(top_keys)
    found_items: dict[str, dict] = {}
    if not wanted:
        return top_rows, top_keys, found_items
    bk = by_key if isinstance(by_key, dict) else _lookup_cached_by_key(items)
    if bk is not None:
        for k in wanted:
            it = bk.get(k)
            if isinstance(it, dict):
                found_items[k] = it
        return top_rows, top_keys, found_items
    if items:
        for it in items:
            if not isinstance(it, dict):
                continue
            k = item_key(it)
            if k in wanted and k not in found_items:
                found_items[k] = it
                if len(found_items) == len(wanted):
                    break
    return top_rows, top_keys, found_items


def _row_flat_norm(row: dict, item: dict | None) -> str:
    src = item or row
    parts = [
        str(src.get(fld))
        for fld in ("topic", "title", "summary", "content", "text", "body", "snippet")
        if src.get(fld)
    ]
    return normalize("\n".join(parts))


def attach_spec_coverage_diag(
    query: str,
    results: list[dict],
    items: list[dict] | None,
    diagnostics: dict | None,
) -> None:
    """上位3候補の本文に対するクエリ特異トークン被覆率(数値のみ)を diagnostics へ記録する。"""
    if diagnostics is None or not isinstance(diagnostics, dict) or not results:
        return
    spec_tokens = extract_spec_tokens(query)
    if not spec_tokens:
        diagnostics["spec_coverage"] = {
            "spec_count": 0,
            "top3_hit": 0,
            "top3_coverage": 1.0,
            "top_checked": 0,
        }
        return
    top_rows, top_keys, found_items = _resolve_top3_items(results, items)
    top3_norm = "\n".join(_row_flat_norm(r, found_items.get(k)) for r, k in zip(top_rows, top_keys))
    hit = sum(1 for tok in spec_tokens if spec_token_matches_text(tok, top3_norm))
    diagnostics["spec_coverage"] = {
        "spec_count": len(spec_tokens),
        "top3_hit": hit,
        "top3_coverage": round(hit / len(spec_tokens), 4),
        "top_checked": len(top_rows),
    }


# ---------------------------------------------------------------------------
# 第19波: 純CPU スケール不変 [0..1] 4段階 agent_confidence 算出
# ---------------------------------------------------------------------------

def cpu_agent_idf_coverage(
    terms: list[str],
    norm_text: str,
    total_docs: int,
    idx_cache: dict | None,
) -> float:
    """BM25 転置キャッシュの DF を O(1) 参照し、完全スケール不変 [0..1] の IDF 加重語彙被覆率を計算する。"""
    qt = [t for t in terms if len(t) >= 2]
    if not qt or not norm_text:
        return 0.0
    n_docs = max(1, int(total_docs))
    raw_hits = idx_cache.get("term_raw_hits") if isinstance(idx_cache, dict) else None
    postings = idx_cache.get("term_postings") if isinstance(idx_cache, dict) else None
    w_sum = 0.0
    hit_w = 0.0
    for t in qt:
        df = 0
        if isinstance(raw_hits, dict) and t in raw_hits:
            df = len(raw_hits[t])
        elif isinstance(postings, dict) and t in postings:
            df = len(postings[t])
        hit = (t in norm_text)
        if hit and df == 0:
            df = 1
        w = math.log((n_docs + 1.0) / (df + 1.0)) + 1.0
        w_sum += w
        if hit:
            hit_w += w
    return round(hit_w / w_sum, 4) if w_sum > 0.0 else 0.0


def attach_cpu_agent_confidence_diag(
    query: str,
    terms: list[str],
    results: list[dict],
    items: list[dict] | None,
    total_docs: int,
    idx_cache: dict | None,
    diagnostics: dict | None,
) -> None:
    """cpu_agent モード用の4段階信頼度サイン（数値シグナルのみ・クエリ文字列非記録）を diagnostics へ記録する。"""
    if diagnostics is None or not isinstance(diagnostics, dict):
        return
    if not results:
        diagnostics["agent_confidence"] = {
            "schema": AGENT_CONFIDENCE_SCHEMA,
            "mode": "cpu_agent",
            "level": "likely_absent",
            "guidance": _AGENT_CONFIDENCE_GUIDANCE["likely_absent"],
            "signals": {
                "t1_idf_coverage": 0.0,
                "t3_idf_coverage": 0.0,
                "spec_count": len(extract_spec_tokens(query)),
                "spec_top3_coverage": 0.0,
                "candidate_count": 0,
            },
        }
        return

    by_key = idx_cache.get("by_key") if isinstance(idx_cache, dict) else None
    top_rows, top_keys, found_items = _resolve_top3_items(results, items, by_key=by_key)
    norms = [_row_flat_norm(r, found_items.get(k)) for r, k in zip(top_rows, top_keys)]
    t1_norm = norms[0] if norms else ""
    t3_norm = "\n".join(norms)
    t1_cov = cpu_agent_idf_coverage(terms, t1_norm, total_docs, idx_cache)
    t3_cov = cpu_agent_idf_coverage(terms, t3_norm, total_docs, idx_cache)

    sc = diagnostics.get("spec_coverage") if isinstance(diagnostics.get("spec_coverage"), dict) else {}
    spec_count = int(sc.get("spec_count") or 0)
    spec_cov = float(sc.get("top3_coverage") if sc.get("top3_coverage") is not None else 1.0)
    sp_miss = (spec_count >= 1 and spec_cov < LOW_RR_SPEC_COV_KITEI)

    if (sp_miss and t1_cov < 0.45) or (t1_cov < 0.18 and t3_cov < 0.28):
        level = "likely_absent"
    elif sp_miss or t1_cov < 0.28 or t3_cov < 0.40:
        level = "low"
    elif t1_cov >= 0.45 and (spec_count == 0 or spec_cov >= 1.0):
        level = "high"
    else:
        level = "medium"

    diagnostics["agent_confidence"] = {
        "schema": AGENT_CONFIDENCE_SCHEMA,
        "mode": "cpu_agent",
        "level": level,
        "guidance": _AGENT_CONFIDENCE_GUIDANCE[level],
        "signals": {
            "t1_idf_coverage": t1_cov,
            "t3_idf_coverage": t3_cov,
            "spec_count": spec_count,
            "spec_top3_coverage": round(spec_cov, 4),
            "candidate_count": len(results),
        },
    }
