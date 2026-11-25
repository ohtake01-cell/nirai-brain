#!/usr/bin/env python3
"""純粋な文字列正規化・クエリ語分解・キー生成・文脈切り出しモジュール（roundtable_brain.py から分離）。

- 状態・ファイルI/O・モック依存を持たない純粋関数のみを格納する。
- roundtable_brain.py から同名で再エクスポートされ、既存呼び出し・試験と100%後方互換を保つ。
"""
from __future__ import annotations

import json
import re
import unicodedata

KANA_ONLY = re.compile(r"^[ぁ-んー]{2}$")   # ひらがなだけの2文字(助詞のつながり)
KATAMARI = re.compile(r"[a-z0-9][a-z0-9._\-]*[a-z0-9]|[a-z0-9]|[ァ-ヴー]{2,}|[一-龥]{2,}")
GYOU_KIRI_CHUKI = "\n（ここから先は roundtable_digest で全部読める）"


def normalize(text: str) -> str:
    return unicodedata.normalize("NFKC", text).casefold()


def query_terms(query: str) -> list[str]:
    normalized = normalize(query).strip()
    terms = [part for part in re.split(r"[\s,、。/]+", normalized) if len(part) >= 2]
    for m in KATAMARI.finditer(normalized):
        word = m.group()
        if len(word) >= 2:
            terms.append(word)
    if normalized and normalized not in terms:
        terms.append(normalized)
    compact = re.sub(r"\s+", "", normalized)
    if len(compact) >= 4:
        terms.extend(
            compact[index:index + 2]
            for index in range(len(compact) - 1)
            if not KANA_ONLY.match(compact[index:index + 2])
        )
    return list(dict.fromkeys(terms))


def item_key(item: dict) -> str:
    """記録を一意に指す鍵。意味の索引と突き合わせる時に使う。"""
    return "{}|{}".format(item.get("memory_type") or "", item.get("id") or item.get("event_id") or "")


def _nagasa(value) -> int:
    return len(json.dumps(value, ensure_ascii=False))


def midashi_de_kiru(digest: str, yosan_ji: int) -> str:
    """今の状態を予算に合わせて切る。見出し(◆)の切れ目で切り、文の途中で切らない。"""
    digest = str(digest or "")
    if yosan_ji <= 2 or not digest:
        return ""
    if _nagasa(digest) <= yosan_ji:
        return digest
    for ichi in reversed([m.start() for m in re.finditer(r"\n◆", digest)]):
        kouho = digest[:ichi]
        if _nagasa(kouho) <= yosan_ji:
            return kouho
    ji = len(digest)
    while ji > 0 and _nagasa(digest[:ji]) > yosan_ji:
        ji -= max(1, (_nagasa(digest[:ji]) - yosan_ji) // 2)
    return digest[:max(0, ji)]


def gyou_de_kiru(digest: str, ji: int) -> str:
    """起動束の digest 要点を行の切れ目で切る。"""
    digest = str(digest or "")
    ji = int(ji)
    if ji <= 0 or len(digest) <= ji:
        return digest
    yochi = ji - len(GYOU_KIRI_CHUKI)
    if yochi <= 0:
        return digest[:ji]
    kiri = digest[:yochi]
    ato = kiri.rfind("\n")
    return (kiri[:ato] if ato > 0 else kiri) + GYOU_KIRI_CHUKI
