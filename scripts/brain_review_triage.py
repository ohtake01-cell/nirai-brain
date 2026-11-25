#!/usr/bin/env python3
"""review_queue トリアージ純粋モジュール（roundtable_brain.py から分離）。

14,500件超のフラットな review_queue から、雑談・経過・機械通知を背景層へ分離し、
verified_episode_candidates.py の L3 独立再現待ち候補と、決定・実測主張・停止項目を優先抽出する。
"""
from __future__ import annotations

from pathlib import Path

REVIEW_NOISE_TOPICS = {"雑談", "仕事台帳"}
REVIEW_NOISE_TYPES = {"progress", "question"}
REVIEW_MECHANICAL_PREFIXES = (
    "【仕事台帳】", "【自動再読込み", "【投影未反映】",
    "【稼働コード一致】", "【loop_alignment", "【心拍", "[状態変化",
)


def l3_ready_event_ids(root: Path) -> set[str]:
    """verified_episode_candidates.py の L2合格・L3独立再現待ちID集合を fail-open で取得する。"""
    try:
        import verified_episode_candidates as _vec
        cands = _vec.select_candidates(root, limit=200, include_high=True)
        return {str(c.get("event_id") or "") for c in cands if c.get("event_id")}
    except Exception:
        return set()


def triage_review_row(row: dict, l3_ready_ids: set[str] | None = None) -> tuple[str, int, str]:
    """review_queue 1行をトリアージ分類する: (tier, score, reason)。
    tier は l3_ready / priority / background の3段階。"""
    eid = str(row.get("event_id") or "")
    if l3_ready_ids and eid in l3_ready_ids:
        return ("l3_ready", 100, "L2証拠合格済み・L3独立再現待ち")
    topic = str(row.get("topic") or "").strip()
    rtype = str(row.get("type") or "").strip()
    text = str(row.get("text") or "").lstrip()
    if topic in REVIEW_NOISE_TOPICS or rtype in REVIEW_NOISE_TYPES or text.startswith(REVIEW_MECHANICAL_PREFIXES):
        return ("background", 10, "雑談・経過・機械通知")
    if rtype == "blocked":
        return ("priority", 85, "停止理由と復活条件の確認")
    if row.get("trust") == "measured_claim":
        return ("priority", 80, "実測主張・独立証拠連結候補")
    if rtype in {"instruction", "complete", "proposal"}:
        return ("priority", 70, "決定・完了・提案の確認")
    return ("background", 20, "一般メモ・通常回答")
